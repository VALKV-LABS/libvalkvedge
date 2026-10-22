//! On-store object formats: segments, snapshot parts, the manifest, and the
//! key layout. This is the public, versioned contract described in
//! `docs/format.md`.

use crate::error::{Error, Result};
use crate::types::{put_bytes, Dec, Frame, Hlc, Lsn, ReplicaId};
use serde::{Deserialize, Serialize};

/// Highest format version this build reads.
pub const FORMAT_VERSION: u16 = 2;

/// The version written, unless an object holds something version 1 cannot
/// express: a value that expires (version 2). Objects without one stay
/// readable by builds that know only version 1.
pub const BASE_VERSION: u16 = 1;

/// The version of an object that holds an expiring value or not.
fn version_for(expiring: bool) -> u16 {
    if expiring {
        2
    } else {
        BASE_VERSION
    }
}

const SEGMENT_MAGIC: &[u8; 4] = b"VKSG";
const SNAPSHOT_MAGIC: &[u8; 4] = b"VKSN";

fn crc(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

fn check_version(found: u16) -> Result<()> {
    if found > FORMAT_VERSION {
        return Err(Error::UnsupportedVersion {
            found,
            supported: FORMAT_VERSION,
        });
    }
    Ok(())
}

/// A contiguous run of frames stored as one object.
///
/// A segment with no frames is a *marker*: a new owner writes one to claim the
/// next log position and fence the previous owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    /// LSN of the first frame, or the next LSN for a marker.
    pub first_lsn: Lsn,
    /// LSN of the last frame, or `first_lsn - 1` for a marker.
    pub last_lsn: Lsn,
    /// Checksum of the previous segment in the generation, 0 for the first.
    pub prev_checksum: u32,
    /// The frames, in LSN order.
    pub frames: Vec<Frame>,
}

impl Segment {
    /// Builds a segment from frames. `frames` must be consecutive.
    pub fn new(prev_checksum: u32, frames: Vec<Frame>) -> Self {
        let first_lsn = frames.first().map(|f| f.lsn).unwrap_or(0);
        let last_lsn = frames.last().map(|f| f.lsn).unwrap_or(0);
        Segment {
            first_lsn,
            last_lsn,
            prev_checksum,
            frames,
        }
    }

    /// Builds a marker segment claiming the position before `next_lsn`.
    pub fn marker(prev_checksum: u32, next_lsn: Lsn) -> Self {
        Segment {
            first_lsn: next_lsn,
            last_lsn: next_lsn - 1,
            prev_checksum,
            frames: Vec::new(),
        }
    }

    /// Encodes the segment. Returns the bytes and the segment checksum that the
    /// next segment must carry as `prev_checksum`.
    pub fn encode(&self) -> (Vec<u8>, u32) {
        let mut out = Vec::new();
        out.extend_from_slice(SEGMENT_MAGIC);
        let expiring = self
            .frames
            .iter()
            .any(|f| f.ops.iter().any(|op| op.expires_ms.is_some()));
        out.extend_from_slice(&version_for(expiring).to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&self.first_lsn.to_le_bytes());
        out.extend_from_slice(&self.last_lsn.to_le_bytes());
        out.extend_from_slice(&self.prev_checksum.to_le_bytes());
        out.extend_from_slice(&(self.frames.len() as u32).to_le_bytes());
        let mut payload = Vec::new();
        for frame in &self.frames {
            payload.clear();
            frame.encode(&mut payload);
            out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            out.extend_from_slice(&crc(&payload).to_le_bytes());
            out.extend_from_slice(&payload);
        }
        let checksum = crc(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        (out, checksum)
    }

    /// Decodes and verifies a segment. Returns it with its checksum.
    pub fn decode(bytes: &[u8]) -> Result<(Self, u32)> {
        if bytes.len() < 4 {
            return Err(Error::corrupt("segment too short"));
        }
        let (body, tail) = bytes.split_at(bytes.len() - 4);
        let checksum = u32::from_le_bytes(tail.try_into().expect("len 4"));
        let mut dec = Dec::new(body);
        if dec.take(4)? != SEGMENT_MAGIC {
            return Err(Error::corrupt("not a segment"));
        }
        check_version(dec.u16()?)?;
        let _flags = dec.u16()?;
        if crc(body) != checksum {
            return Err(Error::corrupt("segment checksum mismatch"));
        }
        let first_lsn = dec.u64()?;
        let last_lsn = dec.u64()?;
        let prev_checksum = dec.u32()?;
        let count = dec.u32()? as usize;
        let mut frames = Vec::with_capacity(count.min(4096));
        for i in 0..count {
            let len = dec.u32()? as usize;
            let want = dec.u32()?;
            let payload = dec.take(len)?;
            if crc(payload) != want {
                return Err(Error::corrupt("frame checksum mismatch"));
            }
            let frame = Frame::decode(&mut Dec::new(payload))?;
            if frame.lsn != first_lsn + i as u64 {
                return Err(Error::corrupt("frames are not consecutive"));
            }
            frames.push(frame);
        }
        if dec.remaining() != 0 {
            return Err(Error::corrupt("trailing bytes in segment"));
        }
        if count > 0 && last_lsn != first_lsn + count as u64 - 1 {
            return Err(Error::corrupt(
                "segment LSN range does not match its frames",
            ));
        }
        Ok((
            Segment {
                first_lsn,
                last_lsn,
                prev_checksum,
                frames,
            },
            checksum,
        ))
    }
}

/// One entry of a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    /// A key with its current value, or a tombstone.
    Entry {
        /// The key.
        key: Vec<u8>,
        /// The stored value. Empty for tombstones.
        value: Vec<u8>,
        /// Timestamp of the operation that produced this state.
        hlc: Hlc,
        /// True when the key was deleted.
        tombstone: bool,
        /// When the value expires, in milliseconds since the Unix epoch.
        /// `None`: never.
        expires_ms: Option<u64>,
    },
    /// The highest outbox sequence number applied for a replica.
    Watermark {
        /// The replica.
        replica: ReplicaId,
        /// Highest applied outbox sequence number.
        seq: u64,
    },
}

impl Record {
    /// Appends the encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Record::Entry {
                key,
                value,
                hlc,
                tombstone,
                expires_ms,
            } => {
                out.push(if expires_ms.is_some() { 2 } else { 0 });
                out.push(u8::from(*tombstone));
                hlc.encode(out);
                put_bytes(out, key);
                put_bytes(out, value);
                if let Some(ms) = expires_ms {
                    out.extend_from_slice(&ms.to_le_bytes());
                }
            }
            Record::Watermark { replica, seq } => {
                out.push(1);
                out.extend_from_slice(&replica.0);
                out.extend_from_slice(&seq.to_le_bytes());
            }
        }
    }

    fn decode(dec: &mut Dec<'_>) -> Result<Self> {
        match dec.u8()? {
            tag @ (0 | 2) => {
                let tombstone = dec.u8()? != 0;
                let hlc = Hlc::decode(dec)?;
                let key = dec.bytes()?.to_vec();
                let value = dec.bytes()?.to_vec();
                let expires_ms = if tag == 2 { Some(dec.u64()?) } else { None };
                Ok(Record::Entry {
                    key,
                    value,
                    hlc,
                    tombstone,
                    expires_ms,
                })
            }
            1 => Ok(Record::Watermark {
                replica: ReplicaId(dec.array16()?),
                seq: dec.u64()?,
            }),
            other => Err(Error::corrupt(format!("unknown record tag {other}"))),
        }
    }

    /// Approximate encoded size, used to decide when a part is full.
    pub fn encoded_len(&self) -> usize {
        match self {
            Record::Entry { key, value, .. } => 2 + Hlc::SIZE + 8 + key.len() + value.len(),
            Record::Watermark { .. } => 25,
        }
    }
}

/// Encodes one snapshot part. Returns the bytes and their checksum.
pub fn encode_snapshot_part(part: u32, records: &[Record]) -> (Vec<u8>, u32) {
    let mut out = Vec::new();
    out.extend_from_slice(SNAPSHOT_MAGIC);
    let expiring = records.iter().any(|r| {
        matches!(
            r,
            Record::Entry {
                expires_ms: Some(_),
                ..
            }
        )
    });
    out.extend_from_slice(&version_for(expiring).to_le_bytes());
    out.extend_from_slice(&part.to_le_bytes());
    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for record in records {
        record.encode(&mut out);
    }
    let checksum = crc(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    (out, checksum)
}

/// Decodes and verifies one snapshot part.
pub fn decode_snapshot_part(bytes: &[u8], expect_checksum: u32) -> Result<Vec<Record>> {
    if bytes.len() < 4 {
        return Err(Error::corrupt("snapshot part too short"));
    }
    let (body, tail) = bytes.split_at(bytes.len() - 4);
    let checksum = u32::from_le_bytes(tail.try_into().expect("len 4"));
    let mut dec = Dec::new(body);
    if dec.take(4)? != SNAPSHOT_MAGIC {
        return Err(Error::corrupt("not a snapshot part"));
    }
    check_version(dec.u16()?)?;
    if crc(body) != checksum || checksum != expect_checksum {
        return Err(Error::corrupt("snapshot part checksum mismatch"));
    }
    let _part = dec.u32()?;
    let count = dec.u32()? as usize;
    let mut records = Vec::with_capacity(count.min(65_536));
    for _ in 0..count {
        records.push(Record::decode(&mut dec)?);
    }
    if dec.remaining() != 0 {
        return Err(Error::corrupt("trailing bytes in snapshot part"));
    }
    Ok(records)
}

/// Reference to one snapshot part object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartRef {
    /// Full object key.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// Checksum the part must have.
    pub checksum: u32,
}

/// One key that may open an encrypted database: the data key, sealed with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeySlot {
    /// Names the key without revealing it: hex of a tag the key computes.
    pub id: String,
    /// The data key sealed with this key: hex of nonce, ciphertext and tag.
    pub wrapped: String,
}

/// A generation that has been replaced. It is kept while replicas that are
/// behind may still read its log, and while it is part of the history that
/// restores are made from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Superseded {
    /// The old generation number.
    pub generation: u64,
    /// When it was replaced, in milliseconds since the Unix epoch.
    pub at_ms: u64,
    /// The LSN its snapshot was taken at. Absent in manifests written before
    /// history was kept; such a generation is not part of the history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_lsn: Option<Lsn>,
    /// When its snapshot was installed, in milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_ms: Option<u64>,
    /// Its snapshot's parts, while the snapshot is kept for restores.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_parts: Option<Vec<PartRef>>,
    /// Its snapshot was installed by a restore: no log leads to it, so it is
    /// kept as long as the generation is.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub restored: bool,
}

/// `manifest.json`: names the current generation of a database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version of the database's objects.
    pub format_version: u16,
    /// Current generation.
    pub generation: u64,
    /// LSN the current generation's snapshot was taken at. 0 means no snapshot.
    pub snapshot_lsn: Lsn,
    /// Parts of the current snapshot, in order.
    #[serde(default)]
    pub snapshot_parts: Vec<PartRef>,
    /// Bumped every time an owner claims the database.
    #[serde(default)]
    pub owner_epoch: u64,
    /// Older generations not yet deleted.
    #[serde(default)]
    pub superseded: Vec<Superseded>,
    /// Hex-encoded sealed marker, present when values are encrypted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption_check: Option<String>,
    /// The key values are encrypted with (the data key), wrapped by each key
    /// that may open the database. Empty for a database encrypted before key
    /// slots existed: its one key is its data key.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_slots: Vec<KeySlot>,
    /// When the current snapshot was installed, in milliseconds since the
    /// Unix epoch. Absent in older manifests and for generation 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_ms: Option<u64>,
    /// The current snapshot was installed by a restore or a branch, not
    /// taken at the tail of the previous generation's log.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub restored: bool,
}

impl Manifest {
    /// The manifest of a database that has just been created.
    pub fn empty() -> Self {
        Manifest {
            format_version: BASE_VERSION,
            generation: 0,
            snapshot_lsn: 0,
            snapshot_parts: Vec::new(),
            owner_epoch: 0,
            superseded: Vec::new(),
            encryption_check: None,
            key_slots: Vec::new(),
            snapshot_ms: None,
            restored: false,
        }
    }

    /// Serializes to JSON.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec_pretty(self).expect("manifest serializes")
    }

    /// Parses JSON and checks the format version.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let manifest: Manifest =
            serde_json::from_slice(bytes).map_err(|e| Error::corrupt(format!("manifest: {e}")))?;
        check_version(manifest.format_version)?;
        Ok(manifest)
    }
}

/// Object keys for one database, under a caller-chosen prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    prefix: String,
}

impl Layout {
    /// Creates a layout rooted at `prefix` (no leading or trailing slash needed).
    pub fn new(prefix: impl Into<String>) -> Self {
        Layout {
            prefix: prefix.into().trim_matches('/').to_string(),
        }
    }

    /// The database prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Key of the lease object.
    pub fn lease(&self) -> String {
        format!("{}/lease.json", self.prefix)
    }

    /// Key of the manifest.
    pub fn manifest(&self) -> String {
        format!("{}/manifest.json", self.prefix)
    }

    /// Prefix holding everything in one generation.
    pub fn generation_prefix(&self, generation: u64) -> String {
        format!("{}/gen/{generation:010}/", self.prefix)
    }

    /// Prefix holding a generation's log segments.
    pub fn log_prefix(&self, generation: u64) -> String {
        format!("{}/gen/{generation:010}/log/", self.prefix)
    }

    /// Key of one log segment.
    pub fn segment(&self, generation: u64, seq: u64) -> String {
        format!("{}{seq:020}", self.log_prefix(generation))
    }

    /// Key of one snapshot part.
    pub fn snapshot_part(&self, generation: u64, part: u32) -> String {
        format!("{}/gen/{generation:010}/snapshot/{part:06}", self.prefix)
    }

    /// Parses the sequence number out of a segment key.
    pub fn parse_segment_seq(key: &str) -> Option<u64> {
        key.rsplit('/').next()?.parse().ok()
    }
}

/// Key of a node's heartbeat object. Lives at the store root, outside any database.
pub fn node_key(node_id: &str) -> String {
    format!("_nodes/{node_id}.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Op, OpKind};

    fn frame(lsn: Lsn) -> Frame {
        Frame {
            lsn,
            commit_hlc: Hlc {
                physical_ms: lsn,
                counter: 0,
                replica: ReplicaId([1; 16]),
            },
            origin: None,
            ops: vec![Op {
                kind: OpKind::Put,
                key: format!("k{lsn}").into_bytes(),
                value: b"v".to_vec(),
                hlc: Hlc::default(),
                expires_ms: None,
            }],
        }
    }

    #[test]
    fn segment_round_trips_and_chains() {
        let seg = Segment::new(0xABCD, vec![frame(5), frame(6), frame(7)]);
        let (bytes, checksum) = seg.encode();
        let (decoded, decoded_checksum) = Segment::decode(&bytes).unwrap();
        assert_eq!(decoded, seg);
        assert_eq!(decoded_checksum, checksum);
        assert_eq!((decoded.first_lsn, decoded.last_lsn), (5, 7));
    }

    #[test]
    fn marker_segment_round_trips() {
        let seg = Segment::marker(7, 10);
        let (bytes, _) = seg.encode();
        let (decoded, _) = Segment::decode(&bytes).unwrap();
        assert!(decoded.frames.is_empty());
        assert_eq!((decoded.first_lsn, decoded.last_lsn), (10, 9));
    }

    #[test]
    fn any_flipped_bit_is_detected() {
        let (bytes, _) = Segment::new(0, vec![frame(1), frame(2)]).encode();
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x01;
            assert!(Segment::decode(&bad).is_err(), "byte {i} not detected");
        }
    }

    #[test]
    fn newer_version_is_refused() {
        let (mut bytes, _) = Segment::new(0, vec![frame(1)]).encode();
        bytes[4..6].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        assert!(matches!(
            Segment::decode(&bytes),
            Err(Error::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn version_2_is_written_only_for_values_that_expire() {
        let version = |bytes: &[u8]| u16::from_le_bytes([bytes[4], bytes[5]]);
        let (plain, _) = Segment::new(0, vec![frame(1)]).encode();
        assert_eq!(version(&plain), 1, "readable by builds that know only 1");
        let mut expiring = frame(1);
        expiring.ops[0].expires_ms = Some(1_700_000_000_000);
        let seg = Segment::new(0, vec![expiring]);
        let (bytes, _) = seg.encode();
        assert_eq!(version(&bytes), 2);
        assert_eq!(Segment::decode(&bytes).unwrap().0, seg);

        let entry = |expires_ms| Record::Entry {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            hlc: Hlc::default(),
            tombstone: false,
            expires_ms,
        };
        assert_eq!(version(&encode_snapshot_part(0, &[entry(None)]).0), 1);
        assert_eq!(version(&encode_snapshot_part(0, &[entry(Some(5))]).0), 2);
        assert_eq!(Manifest::empty().format_version, 1);
    }

    #[test]
    fn snapshot_part_round_trips_and_checks_expected_checksum() {
        let records = vec![
            Record::Entry {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                hlc: Hlc::default(),
                tombstone: false,
                expires_ms: Some(1_700_000_000_000),
            },
            Record::Entry {
                key: b"b".to_vec(),
                value: vec![],
                hlc: Hlc::default(),
                tombstone: true,
                expires_ms: None,
            },
            Record::Watermark {
                replica: ReplicaId([3; 16]),
                seq: 99,
            },
        ];
        let (bytes, checksum) = encode_snapshot_part(0, &records);
        assert_eq!(decode_snapshot_part(&bytes, checksum).unwrap(), records);
        assert!(decode_snapshot_part(&bytes, checksum ^ 1).is_err());
    }

    #[test]
    fn manifest_round_trips_and_rejects_newer_version() {
        let mut manifest = Manifest::empty();
        manifest.generation = 3;
        manifest.snapshot_lsn = 77;
        assert_eq!(
            Manifest::from_bytes(&manifest.to_bytes()).unwrap(),
            manifest
        );
        manifest.format_version = FORMAT_VERSION + 1;
        assert!(Manifest::from_bytes(&manifest.to_bytes()).is_err());
    }

    #[test]
    fn layout_keys_sort_in_sequence_order() {
        let layout = Layout::new("/acct/db/");
        assert_eq!(layout.manifest(), "acct/db/manifest.json");
        let a = layout.segment(1, 9);
        let b = layout.segment(1, 10);
        assert!(a < b);
        assert_eq!(Layout::parse_segment_seq(&b), Some(10));
    }
}
