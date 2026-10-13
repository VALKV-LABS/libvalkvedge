//! One database's local store: replicated data, the outbox of unconfirmed
//! buffered writes, and the overlay that makes those writes readable.

use crate::crypto::Cipher;
use fjall::{Keyspace, KeyspaceCreateOptions, PersistMode};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Mutex};
use valkvedge_log::types::{decode_ops, encode_ops, Dec};
use valkvedge_log::{ChainPos, DeviceId, Error, Frame, Hlc, Lsn, Op, OpKind, Record, Result};
use valkvedge_sync::{Change, OutboxEntry, StateStore};

const POS: &[u8] = b"pos";
const RESETTING: &[u8] = b"resetting";
const DEVICE: &[u8] = b"device";
const OUTBOX_STAT: &[u8] = b"outbox";
const ENC_CHECK: &[u8] = b"enc";

fn local(e: fjall::Error) -> Error {
    Error::local(e)
}

/// Counters describing the outbox.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OutboxStat {
    pub next_seq: u64,
    pub entries: u64,
    pub bytes: u64,
}

pub(crate) struct LocalStore {
    db: fjall::Database,
    data: Keyspace,
    meta: Keyspace,
    devices: Keyspace,
    outbox: Keyspace,
    overlay: Keyspace,
    persist: Option<PersistMode>,
    // Serializes read-modify-write sequences. Fjall itself is thread-safe.
    write: Mutex<()>,
}

fn encode_entry(tombstone: bool, hlc: &Hlc, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + Hlc::SIZE + value.len());
    out.push(u8::from(tombstone));
    hlc.encode(&mut out);
    out.extend_from_slice(value);
    out
}

fn decode_entry(bytes: &[u8]) -> Result<(bool, Hlc, &[u8])> {
    let mut dec = Dec::new(bytes);
    let tombstone = dec.u8()? != 0;
    let hlc = Hlc::decode(&mut dec)?;
    Ok((tombstone, hlc, &bytes[dec.position()..]))
}

fn encode_pos(pos: &ChainPos) -> Vec<u8> {
    let mut out = Vec::with_capacity(28);
    out.extend_from_slice(&pos.generation.to_le_bytes());
    out.extend_from_slice(&pos.next_seq.to_le_bytes());
    out.extend_from_slice(&pos.prev_checksum.to_le_bytes());
    out.extend_from_slice(&pos.applied_lsn.to_le_bytes());
    out
}

impl LocalStore {
    pub(crate) fn open(path: &Path, cache_bytes: u64, fsync: bool) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(Error::local)?;
        let db = fjall::Database::builder(path)
            .worker_threads(1)
            .cache_size(cache_bytes)
            .open()
            .map_err(local)?;
        let ks = |name: &str| {
            db.keyspace(name, KeyspaceCreateOptions::default)
                .map_err(local)
        };
        Ok(LocalStore {
            data: ks("data")?,
            meta: ks("meta")?,
            devices: ks("devices")?,
            outbox: ks("outbox")?,
            overlay: ks("overlay")?,
            persist: fsync.then_some(PersistMode::SyncAll),
            write: Mutex::new(()),
            db,
        })
    }

    fn batch(&self) -> fjall::OwnedWriteBatch {
        self.db.batch().durability(self.persist)
    }

    fn meta_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.meta.get(key).map_err(local)?.map(|v| v.to_vec()))
    }

    /// This installation's id for this database, created on first use.
    pub(crate) fn device_id(&self) -> Result<DeviceId> {
        let _guard = self.write.lock().expect("write lock");
        if let Some(bytes) = self.meta_get(DEVICE)? {
            let id: [u8; 16] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| Error::corrupt("device id"))?;
            return Ok(DeviceId(id));
        }
        let id = DeviceId::random();
        self.meta.insert(DEVICE, id.0.to_vec()).map_err(local)?;
        Ok(id)
    }

    pub(crate) fn encryption_check(&self) -> Result<Option<String>> {
        Ok(self
            .meta_get(ENC_CHECK)?
            .map(|b| String::from_utf8_lossy(&b).into_owned()))
    }

    pub(crate) fn set_encryption_check(&self, check: &str) -> Result<()> {
        self.meta.insert(ENC_CHECK, check.as_bytes()).map_err(local)
    }

    pub(crate) fn outbox_stat(&self) -> Result<OutboxStat> {
        match self.meta_get(OUTBOX_STAT)? {
            None => Ok(OutboxStat {
                next_seq: 1,
                ..OutboxStat::default()
            }),
            Some(bytes) => {
                let mut dec = Dec::new(&bytes);
                Ok(OutboxStat {
                    next_seq: dec.u64()?,
                    entries: dec.u64()?,
                    bytes: dec.u64()?,
                })
            }
        }
    }

    fn put_outbox_stat(&self, batch: &mut fjall::OwnedWriteBatch, stat: &OutboxStat) {
        let mut out = Vec::with_capacity(24);
        out.extend_from_slice(&stat.next_seq.to_le_bytes());
        out.extend_from_slice(&stat.entries.to_le_bytes());
        out.extend_from_slice(&stat.bytes.to_le_bytes());
        batch.insert(&self.meta, OUTBOX_STAT, out);
    }

    /// Stores a buffered batch in the outbox and makes it readable through
    /// the overlay, atomically. Returns its outbox sequence number.
    pub(crate) fn buffer(&self, ops: &[Op], limit_bytes: u64) -> Result<u64> {
        let _guard = self.write.lock().expect("write lock");
        let mut stat = self.outbox_stat()?;
        let encoded = encode_ops(ops);
        if stat.bytes + encoded.len() as u64 > limit_bytes {
            return Err(Error::OutboxFull { limit: limit_bytes });
        }
        let seq = stat.next_seq;
        let mut batch = self.batch();
        for op in ops {
            let mut value = seq.to_le_bytes().to_vec();
            value.extend_from_slice(&encode_entry(op.kind == OpKind::Delete, &op.hlc, &op.value));
            batch.insert(&self.overlay, op.key.as_slice(), value);
        }
        stat.next_seq += 1;
        stat.entries += 1;
        stat.bytes += encoded.len() as u64;
        batch.insert(&self.outbox, seq.to_be_bytes().to_vec(), encoded);
        self.put_outbox_stat(&mut batch, &stat);
        batch.commit().map_err(local)?;
        Ok(seq)
    }

    /// The oldest outbox entries, up to about `max_bytes` (always at least one).
    pub(crate) fn outbox_peek(&self, max_bytes: u64) -> Result<Vec<OutboxEntry>> {
        let mut out = Vec::new();
        let mut total = 0u64;
        for guard in self.outbox.iter() {
            let (key, value) = guard.into_inner().map_err(local)?;
            let seq = u64::from_be_bytes(
                key.as_ref()
                    .try_into()
                    .map_err(|_| Error::corrupt("outbox key"))?,
            );
            total += value.len() as u64;
            out.push(OutboxEntry {
                seq,
                ops: decode_ops(&value)?,
            });
            if total >= max_bytes {
                break;
            }
        }
        Ok(out)
    }

    /// Removes settled entries from the outbox, and their overlay entries
    /// unless a later buffered write to the same key replaced them.
    pub(crate) fn outbox_settle(&self, entries: &[OutboxEntry]) -> Result<OutboxStat> {
        let _guard = self.write.lock().expect("write lock");
        let mut stat = self.outbox_stat()?;
        let mut batch = self.batch();
        for entry in entries {
            let key = entry.seq.to_be_bytes();
            let Some(stored) = self.outbox.get(key).map_err(local)? else {
                continue;
            };
            stat.entries = stat.entries.saturating_sub(1);
            stat.bytes = stat.bytes.saturating_sub(stored.len() as u64);
            batch.remove(&self.outbox, key.to_vec());
            for op in &entry.ops {
                if let Some(pending) = self.overlay.get(&op.key).map_err(local)? {
                    if pending.len() >= 8 && pending[..8] == entry.seq.to_le_bytes() {
                        batch.remove(&self.overlay, op.key.as_slice());
                    }
                }
            }
        }
        self.put_outbox_stat(&mut batch, &stat);
        batch.commit().map_err(local)?;
        Ok(stat)
    }

    /// Applies a batch directly, for databases with no remote log.
    pub(crate) fn apply_local(&self, changes: Vec<Change>, hlc: impl Fn() -> Hlc) -> Result<Lsn> {
        let mut pos = self.position()?;
        pos.applied_lsn += 1;
        let frame = Frame {
            lsn: pos.applied_lsn,
            commit_hlc: hlc(),
            origin: None,
            ops: changes
                .into_iter()
                .map(|c| Op {
                    kind: c.kind,
                    key: c.key,
                    value: c.value,
                    hlc: hlc(),
                })
                .collect(),
        };
        self.apply(&[frame], pos)?;
        Ok(pos.applied_lsn)
    }

    /// The stored (possibly encrypted) value of `key`: a pending buffered
    /// write if there is one, otherwise the replicated value.
    pub(crate) fn get_stored(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(pending) = self.overlay.get(key).map_err(local)? {
            if pending.len() < 8 {
                return Err(Error::corrupt("overlay entry"));
            }
            let (tombstone, _, value) = decode_entry(&pending[8..])?;
            return Ok((!tombstone).then(|| value.to_vec()));
        }
        match self.data.get(key).map_err(local)? {
            None => Ok(None),
            Some(stored) => {
                let (tombstone, _, value) = decode_entry(&stored)?;
                Ok((!tombstone).then(|| value.to_vec()))
            }
        }
    }

    pub(crate) fn scan_range(
        &self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> (fjall::Iter, fjall::Iter) {
        (self.overlay.range(range.clone()), self.data.range(range))
    }

    pub(crate) fn scan_prefix(&self, prefix: &[u8]) -> (fjall::Iter, fjall::Iter) {
        (self.overlay.prefix(prefix), self.data.prefix(prefix))
    }

    pub(crate) fn disk_bytes(&self) -> u64 {
        self.db.disk_space().unwrap_or(0)
    }
}

impl StateStore for LocalStore {
    fn position(&self) -> Result<ChainPos> {
        match self.meta_get(POS)? {
            None => Ok(ChainPos::default()),
            Some(bytes) => {
                let mut dec = Dec::new(&bytes);
                Ok(ChainPos {
                    generation: dec.u64()?,
                    next_seq: dec.u64()?,
                    prev_checksum: dec.u32()?,
                    applied_lsn: dec.u64()?,
                })
            }
        }
    }

    fn needs_bootstrap(&self) -> Result<bool> {
        self.meta.contains_key(RESETTING).map_err(local)
    }

    fn apply(&self, frames: &[Frame], pos: ChainPos) -> Result<()> {
        let _guard = self.write.lock().expect("write lock");
        let applied = self.position()?.applied_lsn;
        let mut batch = self.batch();
        let mut marks: HashMap<DeviceId, u64> = HashMap::new();
        for frame in frames.iter().filter(|f| f.lsn > applied) {
            for op in &frame.ops {
                let stored = encode_entry(op.kind == OpKind::Delete, &op.hlc, &op.value);
                batch.insert(&self.data, op.key.as_slice(), stored);
            }
            if let Some(origin) = frame.origin {
                let mark = match marks.get(&origin.device) {
                    Some(m) => *m,
                    None => self.watermark(&origin.device)?,
                };
                marks.insert(origin.device, mark.max(origin.last_seq));
            }
        }
        for (device, seq) in marks {
            batch.insert(&self.devices, device.0.to_vec(), seq.to_le_bytes().to_vec());
        }
        batch.insert(&self.meta, POS, encode_pos(&pos));
        batch.commit().map_err(local)
    }

    fn version_of(&self, key: &[u8]) -> Result<Option<Hlc>> {
        match self.data.get(key).map_err(local)? {
            None => Ok(None),
            Some(stored) => Ok(Some(decode_entry(&stored)?.1)),
        }
    }

    fn watermark(&self, device: &DeviceId) -> Result<u64> {
        match self.devices.get(device.0).map_err(local)? {
            None => Ok(0),
            Some(v) => Ok(u64::from_le_bytes(
                v.as_ref()
                    .try_into()
                    .map_err(|_| Error::corrupt("watermark"))?,
            )),
        }
    }

    fn begin_reset(&self) -> Result<()> {
        let _guard = self.write.lock().expect("write lock");
        self.meta.insert(RESETTING, [1u8]).map_err(local)?;
        self.db.persist(PersistMode::SyncAll).map_err(local)?;
        self.data.clear().map_err(local)?;
        self.devices.clear().map_err(local)
    }

    fn load(&self, records: &[Record]) -> Result<()> {
        let mut batch = self.batch();
        for record in records {
            match record {
                Record::Entry {
                    key,
                    value,
                    hlc,
                    tombstone,
                } => batch.insert(
                    &self.data,
                    key.as_slice(),
                    encode_entry(*tombstone, hlc, value),
                ),
                Record::Watermark { device, seq } => {
                    batch.insert(&self.devices, device.0.to_vec(), seq.to_le_bytes().to_vec())
                }
            }
        }
        batch.commit().map_err(local)
    }

    fn finish_reset(&self, pos: ChainPos) -> Result<()> {
        let _guard = self.write.lock().expect("write lock");
        let mut batch = self.batch();
        batch.insert(&self.meta, POS, encode_pos(&pos));
        batch.remove(&self.meta, RESETTING);
        batch.commit().map_err(local)?;
        self.db.persist(PersistMode::SyncAll).map_err(local)
    }

    fn scan(&self, visit: &mut dyn FnMut(Record) -> Result<()>) -> Result<()> {
        for guard in self.data.iter() {
            let (key, stored) = guard.into_inner().map_err(local)?;
            let (tombstone, hlc, value) = decode_entry(&stored)?;
            visit(Record::Entry {
                key: key.to_vec(),
                value: value.to_vec(),
                hlc,
                tombstone,
            })?;
        }
        for guard in self.devices.iter() {
            let (key, value) = guard.into_inner().map_err(local)?;
            visit(Record::Watermark {
                device: DeviceId(
                    key.as_ref()
                        .try_into()
                        .map_err(|_| Error::corrupt("device key"))?,
                ),
                seq: u64::from_le_bytes(
                    value
                        .as_ref()
                        .try_into()
                        .map_err(|_| Error::corrupt("watermark"))?,
                ),
            })?;
        }
        Ok(())
    }

    fn purge_tombstones(&self, older_than_ms: u64) -> Result<u64> {
        let _guard = self.write.lock().expect("write lock");
        let mut expired = Vec::new();
        for guard in self.data.iter() {
            let (key, stored) = guard.into_inner().map_err(local)?;
            let (tombstone, hlc, _) = decode_entry(&stored)?;
            if tombstone && hlc.physical_ms < older_than_ms {
                expired.push(key.to_vec());
            }
        }
        for chunk in expired.chunks(1024) {
            let mut batch = self.batch();
            for key in chunk {
                batch.remove(&self.data, key.as_slice());
            }
            batch.commit().map_err(local)?;
        }
        Ok(expired.len() as u64)
    }
}

/// Merges pending buffered writes over replicated data, in key order,
/// skipping deleted keys and decrypting values.
pub struct Iter {
    overlay: Side,
    data: Side,
    cipher: Option<Arc<Cipher>>,
    // Keeps the database open while the iterator lives.
    _open: Arc<dyn std::any::Any + Send + Sync>,
}

struct Side {
    iter: fjall::Iter,
    head: Option<(Vec<u8>, Vec<u8>)>,
    skip: usize,
}

impl Side {
    fn new(iter: fjall::Iter, skip: usize) -> Self {
        Side {
            iter,
            head: None,
            skip,
        }
    }

    fn fill(&mut self) -> Result<()> {
        if self.head.is_none() {
            if let Some(guard) = self.iter.next() {
                let (k, v) = guard.into_inner().map_err(local)?;
                self.head = Some((k.to_vec(), v.to_vec()));
            }
        }
        Ok(())
    }
}

impl Iter {
    pub(crate) fn new(
        iters: (fjall::Iter, fjall::Iter),
        cipher: Option<Arc<Cipher>>,
        open: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        Iter {
            overlay: Side::new(iters.0, 8),
            data: Side::new(iters.1, 0),
            cipher,
            _open: open,
        }
    }

    fn step(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            self.overlay.fill()?;
            self.data.fill()?;
            let order = match (&self.overlay.head, &self.data.head) {
                (None, None) => return Ok(None),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((a, _)), Some((b, _))) => a.cmp(b),
            };
            if order == Ordering::Equal {
                // The pending write hides the replicated value.
                self.data.head = None;
            }
            let side = if order != Ordering::Greater {
                &mut self.overlay
            } else {
                &mut self.data
            };
            let (key, stored) = side.head.take().expect("head present");
            if stored.len() < side.skip {
                return Err(Error::corrupt("overlay entry"));
            }
            let (tombstone, _, value) = decode_entry(&stored[side.skip..])?;
            if tombstone {
                continue;
            }
            let value = match &self.cipher {
                Some(cipher) => cipher.open(&key, value)?,
                None => value.to_vec(),
            };
            return Ok(Some((key, value)));
        }
    }
}

impl Iterator for Iter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.step().transpose()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn store(dir: &tempfile::TempDir) -> LocalStore {
        LocalStore::open(dir.path(), 8 * 1024 * 1024, false).unwrap()
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc {
            physical_ms: ms,
            counter: 0,
            device: DeviceId([7; 16]),
        }
    }

    fn entry(key: &str, value: &str) -> Record {
        Record::Entry {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            hlc: hlc(1),
            tombstone: false,
        }
    }

    fn pos(lsn: u64) -> ChainPos {
        ChainPos {
            generation: 1,
            next_seq: 0,
            prev_checksum: 0,
            applied_lsn: lsn,
        }
    }

    fn read(store: &LocalStore, key: &str) -> Option<String> {
        store
            .get_stored(key.as_bytes())
            .unwrap()
            .map(|v| String::from_utf8(v).unwrap())
    }

    fn all(store: &LocalStore) -> Vec<Record> {
        let mut out = Vec::new();
        store
            .scan(&mut |record| {
                out.push(record);
                Ok(())
            })
            .unwrap();
        out
    }

    #[test]
    fn a_snapshot_loaded_in_parts_reads_back_exactly_including_tombstones_and_watermarks() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let first = vec![entry("a", "1"), entry("b", "2"), entry("c", "3")];
        let second = vec![
            entry("d", "4"),
            Record::Entry {
                key: b"e".to_vec(),
                value: Vec::new(),
                hlc: hlc(9),
                tombstone: true,
            },
            Record::Watermark {
                device: DeviceId([3; 16]),
                seq: 42,
            },
        ];
        store.begin_reset().unwrap();
        store.load(&first).unwrap();
        store.load(&second).unwrap();
        assert!(
            store.needs_bootstrap().unwrap(),
            "not usable until finished"
        );
        store.finish_reset(pos(10)).unwrap();
        assert!(!store.needs_bootstrap().unwrap());
        assert_eq!(store.position().unwrap(), pos(10));
        assert_eq!(read(&store, "a").as_deref(), Some("1"));
        assert_eq!(read(&store, "d").as_deref(), Some("4"));
        assert_eq!(read(&store, "e"), None, "a tombstone reads as absent");
        assert_eq!(store.version_of(b"e").unwrap(), Some(hlc(9)));
        assert_eq!(store.watermark(&DeviceId([3; 16])).unwrap(), 42);
        let mut expected = first;
        expected.extend(second);
        assert_eq!(all(&store), expected);
        // A loaded snapshot is the only copy this device has of that data:
        // it must still be there, complete, after a restart.
        drop(store);
        let reopened = self::store(&dir);
        assert!(!reopened.needs_bootstrap().unwrap());
        assert_eq!(reopened.position().unwrap(), pos(10));
        assert_eq!(all(&reopened), expected);
    }

    #[test]
    fn a_first_load_that_was_cut_short_is_started_again_and_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let full = vec![entry("a", "1"), entry("b", "2"), entry("c", "3")];
        {
            let store = store(&dir);
            store.begin_reset().unwrap();
            store.load(&full[..2]).unwrap();
            // The process dies here: no finish_reset.
        }
        let store = self::store(&dir);
        assert!(
            store.needs_bootstrap().unwrap(),
            "the half-loaded copy is not trusted"
        );
        store.begin_reset().unwrap();
        store.load(&full).unwrap();
        store.finish_reset(pos(3)).unwrap();
        assert_eq!(all(&store), full);
        drop(store);
        let reopened = self::store(&dir);
        assert_eq!(all(&reopened), full);
        assert_eq!(reopened.position().unwrap(), pos(3));
    }

    #[test]
    fn loading_replaces_what_was_there_and_later_writes_win_over_loaded_values() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let frame = |lsn: u64, key: &str, value: &str, kind: OpKind| Frame {
            lsn,
            commit_hlc: hlc(lsn),
            origin: None,
            ops: vec![Op {
                kind,
                key: key.as_bytes().to_vec(),
                value: value.as_bytes().to_vec(),
                hlc: hlc(lsn),
            }],
        };
        store
            .apply(&[frame(1, "old", "gone", OpKind::Put)], pos(1))
            .unwrap();
        store
            .apply(&[frame(2, "b", "before", OpKind::Put)], pos(2))
            .unwrap();
        store.begin_reset().unwrap();
        store
            .load(&[entry("a", "1"), entry("b", "loaded"), entry("c", "3")])
            .unwrap();
        store.finish_reset(pos(5)).unwrap();
        assert_eq!(read(&store, "old"), None, "nothing from before survives");
        assert_eq!(read(&store, "b").as_deref(), Some("loaded"));
        // Writes applied after the load must be seen instead of what was loaded.
        store
            .apply(&[frame(6, "b", "newer", OpKind::Put)], pos(6))
            .unwrap();
        store
            .apply(&[frame(7, "c", "", OpKind::Delete)], pos(7))
            .unwrap();
        assert_eq!(read(&store, "b").as_deref(), Some("newer"));
        assert_eq!(read(&store, "c"), None);
        assert_eq!(read(&store, "a").as_deref(), Some("1"));
        // And they are still there after the files are closed and reopened.
        drop(store);
        let reopened = self::store(&dir);
        assert_eq!(read(&reopened, "a").as_deref(), Some("1"));
        assert_eq!(read(&reopened, "b").as_deref(), Some("newer"));
        assert_eq!(read(&reopened, "c"), None);
        assert_eq!(reopened.position().unwrap(), pos(7));
    }

    #[test]
    fn records_that_are_not_in_key_order_are_still_loaded_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.begin_reset().unwrap();
        // Out of order, and a key twice: the later record wins, as with writes.
        store
            .load(&[
                entry("m", "1"),
                entry("a", "2"),
                entry("z", "3"),
                entry("a", "4"),
            ])
            .unwrap();
        // A second part may reach back before the first.
        store.load(&[entry("b", "5"), entry("c", "6")]).unwrap();
        store.load(&[]).unwrap();
        store.finish_reset(pos(1)).unwrap();
        for (key, value) in [("a", "4"), ("b", "5"), ("c", "6"), ("m", "1"), ("z", "3")] {
            assert_eq!(read(&store, key).as_deref(), Some(value), "{key}");
        }
        assert_eq!(all(&store).len(), 5);
    }
}
