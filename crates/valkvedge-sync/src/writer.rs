//! The writer role: orders sync writes, resolves pushed writes with
//! last-write-wins, and compacts the log.

use crate::pull::pull;
use crate::state::StateStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use valkvedge_log::{
    now_ms, ChainPos, Error, Frame, Hlc, HlcClock, Layout, LogWriter, Lsn, Node, Op, OpKind,
    Origin, Record, RejectReason, ReplicaId, Result, Store,
};

/// Tunables for a [`Writer`].
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// How long deletes are remembered. Pushed operations older than this are refused.
    pub tombstone_retention: Duration,
    /// How far ahead of the writer's clock a pushed operation may be stamped.
    pub max_clock_skew: Duration,
    /// Compact when the current generation has this many segments.
    pub compact_after_segments: u64,
    /// Compact when the current generation's log reaches this many bytes.
    pub compact_after_bytes: u64,
    /// How long a superseded generation is kept for replicas that are behind.
    pub generation_grace: Duration,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            tombstone_retention: Duration::from_secs(30 * 24 * 3600),
            max_clock_skew: Duration::from_secs(5 * 60),
            compact_after_segments: 512,
            compact_after_bytes: 64 * 1024 * 1024,
            generation_grace: Duration::from_secs(24 * 3600),
        }
    }
}

/// One unstamped change, as given to [`Writer::commit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// Put or delete.
    pub kind: OpKind,
    /// The key.
    pub key: Vec<u8>,
    /// The stored value. Empty for deletes.
    pub value: Vec<u8>,
}

/// A requirement on one key for [`Writer::commit_if`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    /// The key.
    pub key: Vec<u8>,
    /// The timestamp its live value must have, or `None` for "has no live
    /// value": never written, or deleted.
    pub version: Option<Hlc>,
}

/// One entry from a replica's outbox: an atomic batch with its sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEntry {
    /// The replica's sequence number for this batch.
    pub seq: u64,
    /// The operations, already stamped by the replica.
    pub ops: Vec<Op>,
}

/// What happened to one pushed operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpOutcome {
    /// It won and is now in the log.
    Applied,
    /// A newer write to the same key already existed.
    Superseded,
    /// It was refused.
    Rejected(RejectReason),
    /// Its entry had already been applied by an earlier push.
    Duplicate,
}

/// Result of [`Writer::push`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushReport {
    /// The replica's watermark after the push. Entries up to here are settled.
    pub watermark: u64,
    /// Log position after the push.
    pub lsn: Lsn,
    /// Per entry: its sequence number and one outcome per operation.
    pub entries: Vec<(u64, Vec<OpOutcome>)>,
}

/// The single writer of one database, holding its lease.
pub struct Writer<S: StateStore> {
    store: Store,
    layout: Layout,
    state: Arc<S>,
    clock: Arc<HlcClock>,
    config: WriterConfig,
    log: Mutex<Option<LogWriter>>,
}

impl<S: StateStore> std::fmt::Debug for Writer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("layout", &self.layout)
            .finish_non_exhaustive()
    }
}

impl<S: StateStore> Writer<S> {
    /// Takes ownership of the database (creating it if needed) and brings
    /// `state` up to the tail of the log.
    ///
    /// `wait` controls what happens when another node holds the lease: see
    /// [`valkvedge_log::lease::acquire`].
    pub async fn open(
        store: Store,
        layout: Layout,
        node: Node,
        state: Arc<S>,
        clock: Arc<HlcClock>,
        config: WriterConfig,
        wait: bool,
    ) -> Result<Self> {
        let log = LogWriter::open(store.clone(), layout.clone(), node, wait).await?;
        pull(&store, &layout, state.as_ref()).await?;
        let (have, want) = (state.position()?, log.position());
        if have != want {
            return Err(Error::corrupt(format!(
                "state did not reach the log tail: {have:?} vs {want:?}"
            )));
        }
        Ok(Writer {
            store,
            layout,
            state,
            clock,
            config,
            log: Mutex::new(Some(log)),
        })
    }

    /// The state this writer maintains.
    pub fn state(&self) -> &Arc<S> {
        &self.state
    }

    /// Current tail of the log.
    pub async fn position(&self) -> Result<ChainPos> {
        let guard = self.log.lock().await;
        Ok(guard.as_ref().ok_or(Error::Fenced)?.position())
    }

    /// The manifest's encryption marker, if the database is encrypted.
    pub async fn encryption_check(&self) -> Result<Option<String>> {
        let guard = self.log.lock().await;
        Ok(guard
            .as_ref()
            .ok_or(Error::Fenced)?
            .manifest()
            .encryption_check
            .clone())
    }

    /// Records the encryption marker in the manifest.
    pub async fn set_encryption_check(&self, check: Option<String>) -> Result<()> {
        let mut guard = self.log.lock().await;
        guard
            .as_mut()
            .ok_or(Error::Fenced)?
            .set_encryption_check(check)
            .await
    }

    async fn append(&self, log: &mut LogWriter, frames: Vec<Frame>) -> Result<Lsn> {
        if frames.is_empty() {
            return Ok(log.position().applied_lsn);
        }
        let pos = log.append(frames.clone()).await?;
        // If an earlier append reached the log but not the state, the state is
        // behind: catch it up from the log, which also covers these frames.
        if self.state.position()?.applied_lsn + frames.len() as u64 != pos.applied_lsn {
            pull(&self.store, &self.layout, self.state.as_ref()).await?;
            return Ok(pos.applied_lsn);
        }
        self.state.apply(&frames, pos)?;
        Ok(pos.applied_lsn)
    }

    /// Commits one atomic batch. Returns its LSN once the frame is stored.
    ///
    /// The writer stamps the operations itself, later than anything it has
    /// seen, so a sync write always wins over writes that preceded it.
    pub async fn commit(&self, changes: Vec<Change>) -> Result<Lsn> {
        Ok(self.commit_if(changes, &[]).await?.0)
    }

    /// Commits one atomic batch if every condition holds. Returns its LSN
    /// and the timestamp each change was stamped with, in order.
    ///
    /// The conditions are checked under the same lock that orders commits,
    /// against state that already holds every earlier write, so nothing can
    /// change a key between the check and the write. If any condition fails,
    /// nothing is written and the error is `CONDITION_FAILED`.
    pub async fn commit_if(
        &self,
        changes: Vec<Change>,
        conditions: &[Condition],
    ) -> Result<(Lsn, Vec<Hlc>)> {
        let mut guard = self.log.lock().await;
        let log = guard.as_mut().ok_or(Error::Fenced)?;
        for condition in conditions {
            if self.state.live_version(&condition.key)? != condition.version {
                return Err(Error::ConditionFailed {
                    key: condition.key.clone(),
                });
            }
        }
        for change in &changes {
            if let Some(current) = self.state.version_of(&change.key)? {
                self.clock.observe(&current);
            }
        }
        let ops: Vec<Op> = changes
            .into_iter()
            .map(|c| Op {
                kind: c.kind,
                key: c.key,
                value: c.value,
                hlc: self.clock.now(),
            })
            .collect();
        let stamps = ops.iter().map(|op| op.hlc).collect();
        let frame = Frame {
            lsn: log.next_lsn(),
            commit_hlc: self.clock.now(),
            origin: None,
            ops,
        };
        Ok((self.append(log, vec![frame]).await?, stamps))
    }

    /// Applies entries from a replica's outbox.
    ///
    /// Each entry becomes one frame holding the operations that won. An entry
    /// at or below the replica's watermark is reported as a duplicate and not
    /// applied again, so a retried push is safe.
    pub async fn push(
        &self,
        replica: ReplicaId,
        mut entries: Vec<OutboxEntry>,
    ) -> Result<PushReport> {
        let mut guard = self.log.lock().await;
        let log = guard.as_mut().ok_or(Error::Fenced)?;
        entries.sort_by_key(|e| e.seq);
        let mut watermark = self.state.watermark(&replica)?;
        let now = now_ms();
        let too_new = now + self.config.max_clock_skew.as_millis() as u64;
        let too_old = now.saturating_sub(self.config.tombstone_retention.as_millis() as u64);

        let mut seen: HashMap<Vec<u8>, Hlc> = HashMap::new();
        let mut frames = Vec::new();
        let mut report = Vec::with_capacity(entries.len());
        let mut lsn = log.next_lsn();
        for entry in entries {
            if entry.seq <= watermark {
                report.push((entry.seq, vec![OpOutcome::Duplicate; entry.ops.len()]));
                continue;
            }
            let mut outcomes = Vec::with_capacity(entry.ops.len());
            let mut winners = Vec::new();
            for op in entry.ops {
                if op.hlc.physical_ms > too_new {
                    outcomes.push(OpOutcome::Rejected(RejectReason::ClockSkew));
                    continue;
                }
                if op.hlc.physical_ms < too_old {
                    outcomes.push(OpOutcome::Rejected(RejectReason::StaleWrite));
                    continue;
                }
                let current = match seen.get(&op.key) {
                    Some(hlc) => Some(*hlc),
                    None => self.state.version_of(&op.key)?,
                };
                if current.is_some_and(|c| c >= op.hlc) {
                    outcomes.push(OpOutcome::Superseded);
                    continue;
                }
                seen.insert(op.key.clone(), op.hlc);
                self.clock.observe(&op.hlc);
                outcomes.push(OpOutcome::Applied);
                winners.push(op);
            }
            // A frame is written even when nothing won, so the watermark is durable.
            frames.push(Frame {
                lsn,
                commit_hlc: self.clock.now(),
                origin: Some(Origin {
                    replica,
                    first_seq: entry.seq,
                    last_seq: entry.seq,
                }),
                ops: winners,
            });
            lsn += 1;
            watermark = entry.seq;
            report.push((entry.seq, outcomes));
        }
        let lsn = self.append(log, frames).await?;
        Ok(PushReport {
            watermark,
            lsn,
            entries: report,
        })
    }

    /// True when the current generation is large enough to be worth compacting.
    pub async fn should_compact(&self) -> bool {
        let guard = self.log.lock().await;
        guard.as_ref().is_some_and(|log| {
            log.segment_count() >= self.config.compact_after_segments
                || log.log_bytes() >= self.config.compact_after_bytes
        })
    }

    /// Writes a snapshot of the current state and starts a new generation.
    ///
    /// Tombstones older than the retention window are left out of the snapshot
    /// and removed from the state. Superseded generations older than the grace
    /// period are deleted. Writes wait while this runs.
    pub async fn compact(&self) -> Result<ChainPos> {
        let mut guard = self.log.lock().await;
        let log = guard.as_mut().ok_or(Error::Fenced)?;
        let cutoff = now_ms().saturating_sub(self.config.tombstone_retention.as_millis() as u64);

        let mut upload = log.begin_snapshot();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Record>(1024);
        let state = self.state.clone();
        let scan = tokio::task::spawn_blocking(move || {
            state.scan(&mut |record| {
                tx.blocking_send(record)
                    .map_err(|_| Error::local("snapshot upload stopped"))
            })
        });
        let mut upload_result = Ok(());
        while let Some(record) = rx.recv().await {
            let expired = matches!(
                &record,
                Record::Entry { tombstone: true, hlc, .. } if hlc.physical_ms < cutoff
            );
            if expired {
                continue;
            }
            if let Err(e) = upload.push(record).await {
                upload_result = Err(e);
                break;
            }
        }
        drop(rx);
        let scan_result = scan
            .await
            .map_err(|e| Error::local(format!("snapshot scan panicked: {e}")))?;
        upload_result?;
        scan_result?;

        let pos = log.install_snapshot(upload).await?;
        self.state.apply(&[], pos)?;
        self.state.purge_tombstones(cutoff)?;
        log.delete_old_generations(self.config.generation_grace.as_millis() as u64)
            .await?;
        Ok(pos)
    }

    /// Gives up ownership cleanly. The writer cannot be used afterwards.
    pub async fn release(&self) -> Result<()> {
        let mut guard = self.log.lock().await;
        match guard.take() {
            Some(log) => log.release().await,
            None => Ok(()),
        }
    }

    /// The object store and layout this writer uses.
    pub fn location(&self) -> (&Store, &Layout) {
        (&self.store, &self.layout)
    }
}
