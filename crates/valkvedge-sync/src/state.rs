//! The interface between the sync logic and whatever holds a database's
//! materialized state.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use valkvedge_log::{ChainPos, Frame, Hlc, OpKind, Record, ReplicaId, Result};

/// The materialized state of one database: every key's latest value and
/// timestamp, the per-replica push watermarks, and the log position that state
/// corresponds to.
///
/// Implementations must make [`StateStore::apply`] atomic: after a crash the
/// store reflects either all of a call or none of it, including the position.
pub trait StateStore: Send + Sync + 'static {
    /// The log position the state corresponds to.
    fn position(&self) -> Result<ChainPos>;

    /// True if a snapshot load was started and never finished. The state must
    /// then be rebuilt from a snapshot before it is used.
    fn needs_bootstrap(&self) -> Result<bool>;

    /// Applies `frames` and records `pos` in one atomic step. Frames at or
    /// below the current applied LSN are skipped, so replaying is harmless.
    fn apply(&self, frames: &[Frame], pos: ChainPos) -> Result<()>;

    /// Timestamp of the latest operation on `key`, including a delete.
    fn version_of(&self, key: &[u8]) -> Result<Option<Hlc>>;

    /// Timestamp of the value `key` holds now: `None` when the key was never
    /// written or its latest operation is a delete. Conditional writes are
    /// checked against this.
    fn live_version(&self, key: &[u8]) -> Result<Option<Hlc>>;

    /// Highest outbox sequence number applied for `replica`, 0 if none.
    fn watermark(&self, replica: &ReplicaId) -> Result<u64>;

    /// Discards all replicated state and marks a snapshot load as in progress.
    fn begin_reset(&self) -> Result<()>;

    /// Loads snapshot records. Called between `begin_reset` and `finish_reset`,
    /// with keys in ascending order across calls.
    fn load(&self, records: &[Record]) -> Result<()>;

    /// Records the position of the loaded snapshot and clears the in-progress mark.
    fn finish_reset(&self, pos: ChainPos) -> Result<()>;

    /// Visits every entry (including tombstones) in key order, then every
    /// watermark. Stops at the first error.
    fn scan(&self, visit: &mut dyn FnMut(Record) -> Result<()>) -> Result<()>;

    /// Removes tombstones whose timestamp is older than `older_than_ms`.
    /// Returns how many were removed.
    fn purge_tombstones(&self, older_than_ms: u64) -> Result<u64>;
}

#[derive(Debug, Default)]
struct MemInner {
    pos: ChainPos,
    resetting: bool,
    data: BTreeMap<Vec<u8>, (Vec<u8>, Hlc, bool)>,
    watermarks: HashMap<ReplicaId, u64>,
}

/// An in-memory [`StateStore`], for tests and examples.
#[derive(Debug, Default)]
pub struct MemState {
    inner: Mutex<MemInner>,
}

impl MemState {
    /// Creates an empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// The live value of `key`, if any.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let inner = self.inner.lock().expect("mem state lock");
        match inner.data.get(key) {
            Some((value, _, false)) => Some(value.clone()),
            _ => None,
        }
    }

    /// Number of entries, counting tombstones.
    pub fn len_with_tombstones(&self) -> usize {
        self.inner.lock().expect("mem state lock").data.len()
    }
}

impl StateStore for MemState {
    fn position(&self) -> Result<ChainPos> {
        Ok(self.inner.lock().expect("mem state lock").pos)
    }

    fn needs_bootstrap(&self) -> Result<bool> {
        Ok(self.inner.lock().expect("mem state lock").resetting)
    }

    fn apply(&self, frames: &[Frame], pos: ChainPos) -> Result<()> {
        let mut inner = self.inner.lock().expect("mem state lock");
        for frame in frames {
            if frame.lsn <= inner.pos.applied_lsn {
                continue;
            }
            for op in &frame.ops {
                let tombstone = op.kind == OpKind::Delete;
                inner
                    .data
                    .insert(op.key.clone(), (op.value.clone(), op.hlc, tombstone));
            }
            if let Some(origin) = frame.origin {
                let mark = inner.watermarks.entry(origin.replica).or_insert(0);
                *mark = (*mark).max(origin.last_seq);
            }
        }
        inner.pos = pos;
        Ok(())
    }

    fn version_of(&self, key: &[u8]) -> Result<Option<Hlc>> {
        let inner = self.inner.lock().expect("mem state lock");
        Ok(inner.data.get(key).map(|(_, hlc, _)| *hlc))
    }

    fn live_version(&self, key: &[u8]) -> Result<Option<Hlc>> {
        let inner = self.inner.lock().expect("mem state lock");
        Ok(match inner.data.get(key) {
            Some((_, hlc, false)) => Some(*hlc),
            _ => None,
        })
    }

    fn watermark(&self, replica: &ReplicaId) -> Result<u64> {
        let inner = self.inner.lock().expect("mem state lock");
        Ok(inner.watermarks.get(replica).copied().unwrap_or(0))
    }

    fn begin_reset(&self) -> Result<()> {
        let mut inner = self.inner.lock().expect("mem state lock");
        *inner = MemInner {
            resetting: true,
            ..MemInner::default()
        };
        Ok(())
    }

    fn load(&self, records: &[Record]) -> Result<()> {
        let mut inner = self.inner.lock().expect("mem state lock");
        for record in records {
            match record {
                Record::Entry {
                    key,
                    value,
                    hlc,
                    tombstone,
                } => {
                    inner
                        .data
                        .insert(key.clone(), (value.clone(), *hlc, *tombstone));
                }
                Record::Watermark { replica, seq } => {
                    inner.watermarks.insert(*replica, *seq);
                }
            }
        }
        Ok(())
    }

    fn finish_reset(&self, pos: ChainPos) -> Result<()> {
        let mut inner = self.inner.lock().expect("mem state lock");
        inner.pos = pos;
        inner.resetting = false;
        Ok(())
    }

    fn scan(&self, visit: &mut dyn FnMut(Record) -> Result<()>) -> Result<()> {
        let (entries, marks) = {
            let inner = self.inner.lock().expect("mem state lock");
            (inner.data.clone(), inner.watermarks.clone())
        };
        for (key, (value, hlc, tombstone)) in entries {
            visit(Record::Entry {
                key,
                value,
                hlc,
                tombstone,
            })?;
        }
        for (replica, seq) in marks {
            visit(Record::Watermark { replica, seq })?;
        }
        Ok(())
    }

    fn purge_tombstones(&self, older_than_ms: u64) -> Result<u64> {
        let mut inner = self.inner.lock().expect("mem state lock");
        let before = inner.data.len();
        inner
            .data
            .retain(|_, (_, hlc, tombstone)| !(*tombstone && hlc.physical_ms < older_than_ms));
        Ok((before - inner.data.len()) as u64)
    }
}
