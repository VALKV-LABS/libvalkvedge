//! Change subscriptions on the local copy. See [`Database::watch`].

use crate::local::Changed;
use crate::{Database, Open, Result, ValkvEdgeError};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::sync::broadcast::Receiver;

/// One change reported by a [`Watch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    /// `key` may read differently now. `value` is what it reads as at the
    /// moment the event is delivered (`None`: no value), so it always agrees
    /// with [`Database::get`]. Several quick writes to one key can arrive as
    /// several events that all carry the latest value.
    Changed {
        /// The key.
        key: Vec<u8>,
        /// Its value now, decrypted.
        value: Option<Vec<u8>>,
    },
    /// Any key may have changed: the local copy was replaced from a snapshot,
    /// or this watcher fell too far behind to be told key by key. Read again
    /// whatever you show.
    Reset,
}

/// A subscription to changes in one database under a key prefix, from
/// [`Database::watch`]. Keeps the database open until dropped.
pub struct Watch {
    db: Database,
    // Holding the store open keeps it from being closed as idle, so the
    // background task goes on following the remote copy.
    _open: Arc<Open>,
    prefix: Vec<u8>,
    feed: Receiver<Arc<Changed>>,
    queue: VecDeque<Vec<u8>>,
}

/// What one message from the feed means to this watcher.
enum Step {
    Event(WatchEvent),
    /// Queued keys to report, or nothing under the prefix.
    Continue,
}

impl Watch {
    pub(crate) fn new(
        db: Database,
        open: Arc<Open>,
        prefix: Vec<u8>,
        feed: Receiver<Arc<Changed>>,
    ) -> Self {
        Watch {
            db,
            _open: open,
            prefix,
            feed,
            queue: VecDeque::new(),
        }
    }

    /// The prefix this watches.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// Waits for the next change. Safe to cancel, for example in
    /// `tokio::select!`: no change is lost.
    pub async fn next(&mut self) -> Result<WatchEvent> {
        loop {
            if let Some(event) = self.pop()? {
                return Ok(event);
            }
            let received = self.feed.recv().await;
            if let Step::Event(event) = self.take(received)? {
                return Ok(event);
            }
        }
    }

    /// The next change if one is already waiting, without waiting.
    pub fn try_next(&mut self) -> Result<Option<WatchEvent>> {
        loop {
            if let Some(event) = self.pop()? {
                return Ok(Some(event));
            }
            let received = match self.feed.try_recv() {
                Ok(changed) => Ok(changed),
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Lagged(n)) => Err(RecvError::Lagged(n)),
                Err(TryRecvError::Closed) => Err(RecvError::Closed),
            };
            if let Step::Event(event) = self.take(received)? {
                return Ok(Some(event));
            }
        }
    }

    fn pop(&mut self) -> Result<Option<WatchEvent>> {
        match self.queue.pop_front() {
            None => Ok(None),
            Some(key) => {
                let value = self.db.get(&key)?;
                Ok(Some(WatchEvent::Changed { key, value }))
            }
        }
    }

    fn take(&mut self, received: std::result::Result<Arc<Changed>, RecvError>) -> Result<Step> {
        match received {
            Ok(changed) => match changed.as_ref() {
                Changed::Reset => {
                    self.queue.clear();
                    Ok(Step::Event(WatchEvent::Reset))
                }
                Changed::Keys(keys) => {
                    let mut seen = HashSet::new();
                    for key in keys {
                        if key.starts_with(&self.prefix) && seen.insert(key) {
                            self.queue.push_back(key.clone());
                        }
                    }
                    Ok(Step::Continue)
                }
            },
            Err(RecvError::Lagged(_)) => {
                self.queue.clear();
                Ok(Step::Event(WatchEvent::Reset))
            }
            // The sender lives as long as the database's handle, which this
            // watch holds.
            Err(RecvError::Closed) => Err(ValkvEdgeError::local("the database was closed")),
        }
    }
}
