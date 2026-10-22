//! Reading and writing a database's log in the object store.
//!
//! [`LogWriter`] is the single writer: it holds the lease, appends segments
//! with create-only writes, and installs snapshots. The free functions read
//! the same objects and need no lease.

use crate::error::{Error, Result};
use crate::format::{
    decode_snapshot_part, encode_snapshot_part, Layout, Manifest, PartRef, Record, Segment,
    Superseded,
};
use crate::lease::{acquire, Lease, Node};
use crate::store::{PutOutcome, Store};
use crate::types::{now_ms, Frame, Lsn};

/// Where a reader or writer stands in a database's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChainPos {
    /// Generation the position refers to.
    pub generation: u64,
    /// Sequence number of the next segment to read or write.
    pub next_seq: u64,
    /// Checksum of the segment before `next_seq`, 0 at the start of a generation.
    pub prev_checksum: u32,
    /// LSN of the last frame covered.
    pub applied_lsn: Lsn,
}

/// Reads the manifest with its entity tag. `None` if the database does not exist.
pub async fn read_manifest(store: &Store, layout: &Layout) -> Result<Option<(Manifest, String)>> {
    match store.get(&layout.manifest()).await? {
        None => Ok(None),
        Some(v) => Ok(Some((Manifest::from_bytes(&v.bytes)?, v.etag))),
    }
}

/// One segment read from the store.
#[derive(Debug, Clone)]
pub struct ReadSegment {
    /// Sequence number within the generation.
    pub seq: u64,
    /// The decoded segment.
    pub segment: Segment,
    /// Its checksum, which the next segment must reference.
    pub checksum: u32,
    /// Encoded size in bytes.
    pub size: u64,
}

/// Reads the segments of `generation` starting at `pos.next_seq`, verifying
/// that each one chains to the one before. Returns at most `limit` segments.
///
/// A gap in sequence numbers or a checksum that does not match the chain is
/// [`Error::Corrupt`].
pub async fn read_segments(
    store: &Store,
    layout: &Layout,
    generation: u64,
    mut next_seq: u64,
    mut prev_checksum: u32,
    limit: usize,
) -> Result<Vec<ReadSegment>> {
    let keys = store.list(&layout.log_prefix(generation)).await?;
    let mut out = Vec::new();
    for key in keys {
        let Some(seq) = Layout::parse_segment_seq(&key) else {
            continue;
        };
        if seq < next_seq {
            continue;
        }
        if out.len() >= limit {
            break;
        }
        if seq != next_seq {
            return Err(Error::corrupt(format!(
                "log gap: expected segment {next_seq}, found {seq}"
            )));
        }
        let object = store
            .get(&key)
            .await?
            .ok_or_else(|| Error::NotFound(key.clone()))?;
        let (segment, checksum) = Segment::decode(&object.bytes)?;
        if segment.prev_checksum != prev_checksum {
            return Err(Error::corrupt(format!(
                "segment {seq} does not chain to its predecessor"
            )));
        }
        out.push(ReadSegment {
            seq,
            segment,
            checksum,
            size: object.bytes.len() as u64,
        });
        next_seq += 1;
        prev_checksum = checksum;
    }
    Ok(out)
}

/// Reads and verifies one snapshot part.
pub async fn read_snapshot_part(store: &Store, part: &PartRef) -> Result<Vec<Record>> {
    let object = store
        .get(&part.key)
        .await?
        .ok_or_else(|| Error::NotFound(part.key.clone()))?;
    decode_snapshot_part(&object.bytes, part.checksum)
}

/// Target size of one snapshot part.
pub const SNAPSHOT_PART_BYTES: usize = 8 * 1024 * 1024;

/// Uploads a snapshot for the next generation, one bounded part at a time.
#[derive(Debug)]
pub struct SnapshotUpload {
    store: Store,
    layout: Layout,
    generation: u64,
    parts: Vec<PartRef>,
    buffer: Vec<Record>,
    buffered: usize,
}

impl SnapshotUpload {
    /// Adds a record, uploading a part when the buffer is full.
    pub async fn push(&mut self, record: Record) -> Result<()> {
        self.buffered += record.encoded_len();
        self.buffer.push(record);
        if self.buffered >= SNAPSHOT_PART_BYTES {
            self.flush().await?;
        }
        Ok(())
    }

    /// Uploads what is buffered and returns the parts, in order. For a
    /// snapshot started with [`LogWriter::begin_base`].
    pub async fn finish(mut self) -> Result<Vec<PartRef>> {
        self.flush().await?;
        Ok(self.parts)
    }

    async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let part = self.parts.len() as u32;
        let (bytes, checksum) = encode_snapshot_part(part, &self.buffer);
        let key = self.layout.snapshot_part(self.generation, part);
        let size = bytes.len() as u64;
        self.store.put(&key, bytes).await?;
        self.parts.push(PartRef {
            key,
            size,
            checksum,
        });
        self.buffer.clear();
        self.buffered = 0;
        Ok(())
    }
}

/// The single writer of a database's log.
#[derive(Debug)]
pub struct LogWriter {
    store: Store,
    layout: Layout,
    node: Node,
    lease: Option<Lease>,
    manifest: Manifest,
    manifest_etag: String,
    pos: ChainPos,
    log_bytes: u64,
    invalid: bool,
}

impl LogWriter {
    /// Takes ownership of the database at `layout`, creating it if needed.
    ///
    /// After this returns, any previous owner's next append or compaction
    /// fails: the manifest has a new entity tag and a marker segment occupies
    /// the next log position.
    pub async fn open(store: Store, layout: Layout, node: Node, wait: bool) -> Result<Self> {
        let lease = acquire(&store, &layout, &node, wait).await?;
        for _ in 0..8 {
            let (mut manifest, etag) = match read_manifest(&store, &layout).await? {
                Some(found) => found,
                None => {
                    let fresh = Manifest::empty();
                    store
                        .put_if_absent(&layout.manifest(), fresh.to_bytes())
                        .await?;
                    continue;
                }
            };
            manifest.owner_epoch = lease.record.lease_epoch;
            let manifest_etag = match store
                .put_if_match(&layout.manifest(), manifest.to_bytes(), &etag)
                .await?
            {
                PutOutcome::Stored(_) => match read_manifest(&store, &layout).await? {
                    Some((m, e)) if m == manifest => e,
                    _ => continue,
                },
                PutOutcome::ConditionFailed => continue,
            };

            let generation = manifest.generation;
            let segments = read_segments(&store, &layout, generation, 0, 0, usize::MAX).await?;
            let mut pos = ChainPos {
                generation,
                next_seq: 0,
                prev_checksum: 0,
                applied_lsn: manifest.snapshot_lsn,
            };
            let mut log_bytes = 0;
            for s in &segments {
                pos.next_seq = s.seq + 1;
                pos.prev_checksum = s.checksum;
                pos.applied_lsn = pos.applied_lsn.max(s.segment.last_lsn);
                log_bytes += s.size;
            }
            let marker = Segment::marker(pos.prev_checksum, pos.applied_lsn + 1);
            let (bytes, checksum) = marker.encode();
            let key = layout.segment(generation, pos.next_seq);
            match store.put_if_absent(&key, bytes).await? {
                PutOutcome::Stored(_) => {
                    pos.next_seq += 1;
                    pos.prev_checksum = checksum;
                    return Ok(LogWriter {
                        store,
                        layout,
                        node,
                        lease: Some(lease),
                        manifest,
                        manifest_etag,
                        pos,
                        log_bytes,
                        invalid: false,
                    });
                }
                // Someone appended while we scanned: look again.
                PutOutcome::ConditionFailed => continue,
            }
        }
        Err(Error::Unavailable(
            "could not claim the log: too much contention".into(),
        ))
    }

    /// Current tail position. `applied_lsn` is the last LSN in the log.
    pub fn position(&self) -> ChainPos {
        self.pos
    }

    /// The LSN the next appended frame must carry.
    pub fn next_lsn(&self) -> Lsn {
        self.pos.applied_lsn + 1
    }

    /// The manifest as last written by this writer.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Number of segments in the current generation.
    pub fn segment_count(&self) -> u64 {
        self.pos.next_seq
    }

    /// Bytes of log in the current generation, as seen by this writer.
    pub fn log_bytes(&self) -> u64 {
        self.log_bytes
    }

    fn check_usable(&self) -> Result<()> {
        if self.invalid {
            return Err(Error::Fenced);
        }
        if !self.node.is_live() {
            return Err(Error::Unavailable(
                "node heartbeat is overdue; refusing to write".into(),
            ));
        }
        Ok(())
    }

    /// Appends `frames` as one segment. The frames must start at
    /// [`LogWriter::next_lsn`] and be consecutive.
    ///
    /// Returns only after the segment is stored. On [`Error::Fenced`] another
    /// node owns the database and this writer must be dropped. On any other
    /// error the outcome is unknown and the writer is also unusable: reopen it
    /// to learn what the log contains.
    pub async fn append(&mut self, frames: Vec<Frame>) -> Result<ChainPos> {
        self.check_usable()?;
        if frames.is_empty() {
            return Ok(self.pos);
        }
        if frames[0].lsn != self.next_lsn() {
            return Err(Error::local("frames do not start at the next LSN"));
        }
        let segment = Segment::new(self.pos.prev_checksum, frames);
        let last_lsn = segment.last_lsn;
        let (bytes, checksum) = segment.encode();
        let size = bytes.len() as u64;
        let key = self.layout.segment(self.pos.generation, self.pos.next_seq);
        let stored = match self.store.put_if_absent(&key, bytes.clone()).await {
            Ok(PutOutcome::Stored(_)) => true,
            // Either our own earlier attempt landed, or another node wrote here.
            Ok(PutOutcome::ConditionFailed) | Err(_) => match self.store.get(&key).await {
                Ok(Some(existing)) => existing.bytes[..] == bytes[..],
                Ok(None) => {
                    self.invalid = true;
                    return Err(Error::Unavailable(
                        "segment write failed with unknown outcome".into(),
                    ));
                }
                Err(e) => {
                    self.invalid = true;
                    return Err(e);
                }
            },
        };
        if !stored {
            self.invalid = true;
            return Err(Error::Fenced);
        }
        // Checked again after the write: if the heartbeat went overdue while
        // the upload was in flight, the write must not be acknowledged.
        if !self.node.is_live() {
            self.invalid = true;
            return Err(Error::Unavailable(
                "node heartbeat went overdue during the write".into(),
            ));
        }
        self.pos.next_seq += 1;
        self.pos.prev_checksum = checksum;
        self.pos.applied_lsn = last_lsn;
        self.log_bytes += size;
        Ok(self.pos)
    }

    /// Starts uploading a snapshot for the next generation.
    pub fn begin_snapshot(&self) -> SnapshotUpload {
        SnapshotUpload {
            store: self.store.clone(),
            layout: self.layout.clone(),
            generation: self.pos.generation + 1,
            parts: Vec::new(),
            buffer: Vec::new(),
            buffered: 0,
        }
    }

    /// Starts uploading a snapshot for `generation`, an older generation
    /// whose own snapshot is no longer kept: the state at its snapshot LSN,
    /// rebuilt from the history, to become the history's base.
    pub fn begin_base(&self, generation: u64) -> SnapshotUpload {
        SnapshotUpload {
            store: self.store.clone(),
            layout: self.layout.clone(),
            generation,
            parts: Vec::new(),
            buffer: Vec::new(),
            buffered: 0,
        }
    }

    /// Makes an uploaded snapshot the current generation. The snapshot must
    /// describe the state at the writer's current tail.
    pub async fn install_snapshot(&mut self, upload: SnapshotUpload) -> Result<ChainPos> {
        let lsn = self.pos.applied_lsn;
        self.install(upload, lsn, false).await
    }

    /// Makes an uploaded snapshot of another state the current generation,
    /// one LSN past the tail: a restore, or a branch's first state. No log
    /// leads to it, so replicas reload it rather than catching up, and it is
    /// kept as long as its generation is.
    pub async fn install_restore(&mut self, upload: SnapshotUpload) -> Result<ChainPos> {
        let lsn = self.pos.applied_lsn + 1;
        self.install(upload, lsn, true).await
    }

    async fn install(
        &mut self,
        mut upload: SnapshotUpload,
        lsn: Lsn,
        restored: bool,
    ) -> Result<ChainPos> {
        self.check_usable()?;
        upload.flush().await?;
        let now = now_ms();
        let mut next = self.manifest.clone();
        next.superseded.push(Superseded {
            generation: self.manifest.generation,
            at_ms: now,
            snapshot_lsn: Some(self.manifest.snapshot_lsn),
            snapshot_ms: self.manifest.snapshot_ms,
            snapshot_parts: Some(self.manifest.snapshot_parts.clone()),
            restored: self.manifest.restored,
        });
        next.generation = upload.generation;
        next.snapshot_lsn = lsn;
        next.snapshot_parts = upload.parts;
        next.snapshot_ms = Some(now);
        next.restored = restored;
        self.write_manifest(next).await?;
        self.pos.generation = self.manifest.generation;
        self.pos.next_seq = 0;
        self.pos.prev_checksum = 0;
        self.pos.applied_lsn = lsn;
        self.log_bytes = 0;
        Ok(self.pos)
    }

    /// Sets or clears the manifest's encryption marker.
    pub async fn set_encryption_check(&mut self, check: Option<String>) -> Result<()> {
        self.check_usable()?;
        let mut next = self.manifest.clone();
        next.encryption_check = check;
        self.write_manifest(next).await
    }

    async fn write_manifest(&mut self, next: Manifest) -> Result<()> {
        let key = self.layout.manifest();
        match self
            .store
            .put_if_match(&key, next.to_bytes(), &self.manifest_etag)
            .await?
        {
            PutOutcome::Stored(etag) if !etag.is_empty() => self.manifest_etag = etag,
            PutOutcome::Stored(_) => {
                self.manifest_etag = self
                    .store
                    .get(&key)
                    .await?
                    .map(|v| v.etag)
                    .ok_or_else(|| Error::Unavailable("manifest vanished".into()))?;
            }
            PutOutcome::ConditionFailed => {
                self.invalid = true;
                return Err(Error::Fenced);
            }
        }
        self.manifest = next;
        Ok(())
    }

    /// Deletes generations that were superseded more than `grace_ms` ago.
    pub async fn delete_old_generations(&mut self, grace_ms: u64) -> Result<usize> {
        self.prune(grace_ms, false).await
    }

    /// Deletes what neither replicas nor restores need any more. Returns how
    /// many generations were deleted.
    ///
    /// Without history, a generation is deleted once it was replaced more
    /// than `grace_ms` ago. With history, the snapshots of replaced
    /// generations are deleted at once, except the base (the oldest one
    /// kept) and those of restores; generations older than the base are
    /// deleted once past `grace_ms`. [`LogWriter::advance_base`] moves the
    /// base forward. Replicas only ever need the current snapshot and the
    /// logs.
    pub async fn prune(&mut self, grace_ms: u64, history: bool) -> Result<usize> {
        self.check_usable()?;
        let now = now_ms();
        let aged = |s: &Superseded| now.saturating_sub(s.at_ms) >= grace_ms;
        let base = self
            .manifest
            .superseded
            .iter()
            .filter(|s| s.snapshot_lsn.is_some() && s.snapshot_parts.is_some())
            .map(|s| s.generation)
            .min();
        let mut next = self.manifest.clone();
        let mut whole = Vec::new();
        let mut parts = Vec::new();
        let mut keep = Vec::new();
        for mut old in next.superseded.drain(..) {
            let outside = !history
                || old.snapshot_lsn.is_none()
                || base.is_none_or(|base| old.generation < base);
            if outside && aged(&old) {
                whole.push(old.generation);
                continue;
            }
            if history && Some(old.generation) != base && !old.restored {
                parts.extend(old.snapshot_parts.take().into_iter().flatten());
            }
            keep.push(old);
        }
        if whole.is_empty() && parts.is_empty() {
            return Ok(0);
        }
        next.superseded = keep;
        self.drop_objects(next, &whole, &parts).await?;
        Ok(whole.len())
    }

    /// Makes `generation` the base of the history. `parts` is its snapshot,
    /// uploaded with [`LogWriter::begin_base`], when it had none kept. The
    /// snapshots of older generations are deleted, and so, once past grace,
    /// are those generations at the next [`LogWriter::prune`].
    pub async fn advance_base(
        &mut self,
        generation: u64,
        parts: Option<Vec<PartRef>>,
    ) -> Result<()> {
        self.check_usable()?;
        let mut next = self.manifest.clone();
        let mut dropped = Vec::new();
        for old in &mut next.superseded {
            if old.generation < generation {
                dropped.extend(old.snapshot_parts.take().into_iter().flatten());
            } else if old.generation == generation {
                if let Some(parts) = &parts {
                    old.snapshot_parts = Some(parts.clone());
                }
                if old.snapshot_parts.is_none() {
                    return Err(Error::local(format!(
                        "generation {generation} has no snapshot to be the base"
                    )));
                }
            }
        }
        self.drop_objects(next, &[], &dropped).await
    }

    /// Writes `next`, then deletes whole generations and snapshot parts it no
    /// longer names. The manifest goes first, so no reader is pointed at
    /// something that is being deleted.
    async fn drop_objects(
        &mut self,
        next: Manifest,
        generations: &[u64],
        parts: &[PartRef],
    ) -> Result<()> {
        self.write_manifest(next).await?;
        for generation in generations {
            for key in self
                .store
                .list(&self.layout.generation_prefix(*generation))
                .await?
            {
                self.store.delete(&key).await?;
            }
        }
        for part in parts {
            self.store.delete(&part.key).await?;
        }
        Ok(())
    }

    /// Gives up ownership cleanly so another node can take over at once.
    pub async fn release(mut self) -> Result<()> {
        match self.lease.take() {
            Some(lease) => lease.release(&self.store).await,
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::LeaseConfig;
    use crate::types::{Hlc, Op, OpKind, ReplicaId};
    use std::time::Duration;

    fn fast() -> LeaseConfig {
        LeaseConfig {
            heartbeat_interval: Duration::from_millis(20),
            lease_ttl: Duration::from_millis(200),
            safety_margin: Duration::from_millis(50),
        }
    }

    fn frame(lsn: Lsn) -> Frame {
        Frame {
            lsn,
            commit_hlc: Hlc::default(),
            origin: None,
            ops: vec![Op {
                kind: OpKind::Put,
                key: format!("k{lsn}").into_bytes(),
                value: vec![7; 16],
                hlc: Hlc {
                    physical_ms: lsn,
                    counter: 0,
                    replica: ReplicaId([1; 16]),
                },
                expires_ms: None,
            }],
        }
    }

    async fn node(store: &Store, id: &str) -> Node {
        Node::start(store.clone(), id, "addr", fast())
            .await
            .unwrap()
    }

    async fn read_all(store: &Store, layout: &Layout) -> Vec<Lsn> {
        let (manifest, _) = read_manifest(store, layout).await.unwrap().unwrap();
        read_segments(store, layout, manifest.generation, 0, 0, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|s| s.segment.frames.into_iter().map(|f| f.lsn))
            .collect()
    }

    #[tokio::test]
    async fn appended_frames_are_readable_in_order() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let mut w = LogWriter::open(
            store.clone(),
            layout.clone(),
            node(&store, "a").await,
            false,
        )
        .await
        .unwrap();
        assert_eq!(w.next_lsn(), 1);
        w.append(vec![frame(1), frame(2)]).await.unwrap();
        w.append(vec![frame(3)]).await.unwrap();
        assert_eq!(read_all(&store, &layout).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn reopening_resumes_at_the_tail() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let a = node(&store, "a").await;
        let mut w = LogWriter::open(store.clone(), layout.clone(), a.clone(), false)
            .await
            .unwrap();
        w.append(vec![frame(1), frame(2)]).await.unwrap();
        w.release().await.unwrap();

        let mut w = LogWriter::open(store.clone(), layout.clone(), a, false)
            .await
            .unwrap();
        assert_eq!(w.next_lsn(), 3);
        w.append(vec![frame(3)]).await.unwrap();
        assert_eq!(read_all(&store, &layout).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn previous_owner_is_fenced_after_takeover() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let a = node(&store, "a").await;
        let mut old = LogWriter::open(store.clone(), layout.clone(), a.clone(), false)
            .await
            .unwrap();
        old.append(vec![frame(1)]).await.unwrap();

        // Node a stops renewing but its process keeps running and still
        // believes it owns the database.
        a.stop_heartbeat();
        let mut new = LogWriter::open(store.clone(), layout.clone(), node(&store, "b").await, true)
            .await
            .unwrap();
        new.append(vec![frame(2)]).await.unwrap();

        // The stale owner cannot append: its heartbeat is overdue, and even if
        // it were not, the log position is taken.
        assert!(old.append(vec![frame(2)]).await.is_err());
        assert_eq!(read_all(&store, &layout).await, vec![1, 2]);
    }

    #[tokio::test]
    async fn stale_writer_with_a_live_heartbeat_is_fenced_by_the_log_itself() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let a = node(&store, "a").await;
        let mut old = LogWriter::open(store.clone(), layout.clone(), a.clone(), false)
            .await
            .unwrap();
        old.append(vec![frame(1)]).await.unwrap();

        // Simulate a wrongful takeover while a is still alive by releasing the
        // lease out from under it: fencing must not depend on a noticing.
        let stolen = old.lease.take().unwrap();
        stolen.release(&store).await.unwrap();
        let mut new = LogWriter::open(
            store.clone(),
            layout.clone(),
            node(&store, "b").await,
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            old.append(vec![frame(2)]).await.unwrap_err().code(),
            "FENCED"
        );
        assert_eq!(
            old.append(vec![frame(2)]).await.unwrap_err().code(),
            "FENCED"
        );
        let upload = old.begin_snapshot();
        assert!(old.install_snapshot(upload).await.is_err());

        new.append(vec![frame(2)]).await.unwrap();
        assert_eq!(read_all(&store, &layout).await, vec![1, 2]);
    }

    #[tokio::test]
    async fn snapshot_starts_a_new_generation_and_old_one_is_deleted_after_grace() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let mut w = LogWriter::open(
            store.clone(),
            layout.clone(),
            node(&store, "a").await,
            false,
        )
        .await
        .unwrap();
        w.append(vec![frame(1), frame(2)]).await.unwrap();

        let mut upload = w.begin_snapshot();
        upload
            .push(Record::Entry {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                hlc: Hlc::default(),
                tombstone: false,
                expires_ms: None,
            })
            .await
            .unwrap();
        let pos = w.install_snapshot(upload).await.unwrap();
        assert_eq!((pos.generation, pos.next_seq, pos.applied_lsn), (1, 0, 2));
        w.append(vec![frame(3)]).await.unwrap();

        let (manifest, _) = read_manifest(&store, &layout).await.unwrap().unwrap();
        assert_eq!((manifest.generation, manifest.snapshot_lsn), (1, 2));
        let records = read_snapshot_part(&store, &manifest.snapshot_parts[0])
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(read_all(&store, &layout).await, vec![3]);

        assert!(!store
            .list(&layout.generation_prefix(0))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(w.delete_old_generations(60_000).await.unwrap(), 0);
        assert_eq!(w.delete_old_generations(0).await.unwrap(), 1);
        assert!(store
            .list(&layout.generation_prefix(0))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn a_gap_or_broken_chain_is_reported_as_corrupt() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let mut w = LogWriter::open(
            store.clone(),
            layout.clone(),
            node(&store, "a").await,
            false,
        )
        .await
        .unwrap();
        w.append(vec![frame(1)]).await.unwrap();
        w.append(vec![frame(2)]).await.unwrap();

        // Segment 0 is the marker, 1 and 2 hold the frames.
        let wrong_prev = Segment::new(12345, vec![frame(2)]).encode().0;
        store.put(&layout.segment(0, 2), wrong_prev).await.unwrap();
        let err = read_segments(&store, &layout, 0, 0, 0, usize::MAX)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "CORRUPT");

        store.delete(&layout.segment(0, 1)).await.unwrap();
        let err = read_segments(&store, &layout, 0, 0, 0, usize::MAX)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "CORRUPT");
    }
}
