//! Core value types and their binary encoding: [`Hlc`], [`Op`], [`Frame`].
//!
//! All integers are little-endian. Byte strings are a `u32` length followed by
//! the bytes.

use crate::error::{Error, Result};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Log sequence number. Frame *n* has LSN *n*; the first frame is 1.
pub type Lsn = u64;

/// Identifies one installation of the library.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DeviceId(pub [u8; 16]);

impl DeviceId {
    /// Generates a random device id.
    pub fn random() -> Self {
        DeviceId(*uuid::Uuid::new_v4().as_bytes())
    }
}

impl std::fmt::Display for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", uuid::Uuid::from_bytes(self.0))
    }
}

/// Hybrid logical clock timestamp.
///
/// Ordered by physical time, then counter, then device id, so two different
/// devices can never produce equal timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Hlc {
    /// Milliseconds since the Unix epoch.
    pub physical_ms: u64,
    /// Tie-breaker for events in the same millisecond.
    pub counter: u32,
    /// The device that produced the timestamp.
    pub device: DeviceId,
}

impl Hlc {
    /// Encoded size in bytes.
    pub const SIZE: usize = 28;

    /// Appends the encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.physical_ms.to_le_bytes());
        out.extend_from_slice(&self.counter.to_le_bytes());
        out.extend_from_slice(&self.device.0);
    }

    /// Reads one timestamp from `dec`.
    pub fn decode(dec: &mut Dec<'_>) -> Result<Self> {
        Ok(Hlc {
            physical_ms: dec.u64()?,
            counter: dec.u32()?,
            device: DeviceId(dec.array16()?),
        })
    }
}

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Produces strictly increasing [`Hlc`] timestamps for one device.
#[derive(Debug)]
pub struct HlcClock {
    device: DeviceId,
    last: Mutex<(u64, u32)>,
}

impl HlcClock {
    /// Creates a clock for `device`.
    pub fn new(device: DeviceId) -> Self {
        HlcClock {
            device,
            last: Mutex::new((0, 0)),
        }
    }

    /// The device this clock stamps for.
    pub fn device(&self) -> DeviceId {
        self.device
    }

    /// Returns a timestamp greater than every one returned or observed before.
    pub fn now(&self) -> Hlc {
        let wall = now_ms();
        let mut last = self.last.lock().expect("hlc lock");
        if wall > last.0 {
            *last = (wall, 0);
        } else {
            last.1 += 1;
        }
        Hlc {
            physical_ms: last.0,
            counter: last.1,
            device: self.device,
        }
    }

    /// Moves the clock forward so later timestamps exceed `seen`.
    pub fn observe(&self, seen: &Hlc) {
        let mut last = self.last.lock().expect("hlc lock");
        if (seen.physical_ms, seen.counter) > *last {
            *last = (seen.physical_ms, seen.counter);
        }
    }
}

/// What an operation does to its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    /// Store a value.
    Put,
    /// Remove the key, leaving a tombstone.
    Delete,
}

/// One change to one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op {
    /// Put or delete.
    pub kind: OpKind,
    /// The key.
    pub key: Vec<u8>,
    /// The stored value (ciphertext when the database is encrypted). Empty for deletes.
    pub value: Vec<u8>,
    /// When the operation happened.
    pub hlc: Hlc,
}

impl Op {
    /// Appends the encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(match self.kind {
            OpKind::Put => 0,
            OpKind::Delete => 1,
        });
        put_bytes(out, &self.key);
        put_bytes(out, &self.value);
        self.hlc.encode(out);
    }

    /// Reads one operation from `dec`.
    pub fn decode(dec: &mut Dec<'_>) -> Result<Self> {
        let kind = match dec.u8()? {
            0 => OpKind::Put,
            1 => OpKind::Delete,
            other => return Err(Error::corrupt(format!("unknown op kind {other}"))),
        };
        Ok(Op {
            kind,
            key: dec.bytes()?.to_vec(),
            value: dec.bytes()?.to_vec(),
            hlc: Hlc::decode(dec)?,
        })
    }

    /// Encoded size in bytes.
    pub fn encoded_len(&self) -> usize {
        1 + 4 + self.key.len() + 4 + self.value.len() + Hlc::SIZE
    }
}

/// Encodes a list of operations (used for outbox entries).
pub fn encode_ops(ops: &[Op]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + ops.iter().map(Op::encoded_len).sum::<usize>());
    out.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    for op in ops {
        op.encode(&mut out);
    }
    out
}

/// Decodes a list written by [`encode_ops`].
pub fn decode_ops(bytes: &[u8]) -> Result<Vec<Op>> {
    let mut dec = Dec::new(bytes);
    let n = dec.u32()? as usize;
    let mut ops = Vec::with_capacity(n.min(4096));
    for _ in 0..n {
        ops.push(Op::decode(&mut dec)?);
    }
    Ok(ops)
}

/// Where a frame's operations came from, when they came from a device outbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Origin {
    /// The pushing device.
    pub device: DeviceId,
    /// First outbox sequence number covered by the frame.
    pub first_seq: u64,
    /// Last outbox sequence number covered by the frame.
    pub last_seq: u64,
}

/// One atomic batch in a database's log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Position in the log.
    pub lsn: Lsn,
    /// When the writer committed the frame.
    pub commit_hlc: Hlc,
    /// Set for frames built from a device's outbox.
    pub origin: Option<Origin>,
    /// The operations. May be empty (a frame can exist only to record `origin`).
    pub ops: Vec<Op>,
}

impl Frame {
    /// Appends the encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.lsn.to_le_bytes());
        self.commit_hlc.encode(out);
        match &self.origin {
            None => out.push(0),
            Some(o) => {
                out.push(1);
                out.extend_from_slice(&o.device.0);
                out.extend_from_slice(&o.first_seq.to_le_bytes());
                out.extend_from_slice(&o.last_seq.to_le_bytes());
            }
        }
        out.extend_from_slice(&(self.ops.len() as u32).to_le_bytes());
        for op in &self.ops {
            op.encode(out);
        }
    }

    /// Reads one frame from `dec`.
    pub fn decode(dec: &mut Dec<'_>) -> Result<Self> {
        let lsn = dec.u64()?;
        let commit_hlc = Hlc::decode(dec)?;
        let origin = match dec.u8()? {
            0 => None,
            1 => Some(Origin {
                device: DeviceId(dec.array16()?),
                first_seq: dec.u64()?,
                last_seq: dec.u64()?,
            }),
            other => return Err(Error::corrupt(format!("unknown origin tag {other}"))),
        };
        let n = dec.u32()? as usize;
        let mut ops = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            ops.push(Op::decode(dec)?);
        }
        Ok(Frame {
            lsn,
            commit_hlc,
            origin,
            ops,
        })
    }
}

/// Appends a length-prefixed byte string.
pub fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Cursor over encoded bytes. Every read is bounds-checked.
#[derive(Debug)]
pub struct Dec<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Dec<'a> {
    /// Starts reading at the beginning of `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        Dec { buf, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Current offset.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Takes the next `n` bytes.
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::corrupt("unexpected end of data"));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    /// Reads a `u8`.
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// Reads a little-endian `u16`.
    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("len 2")))
    }

    /// Reads a little-endian `u32`.
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("len 4")))
    }

    /// Reads a little-endian `u64`.
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("len 8")))
    }

    /// Reads 16 raw bytes.
    pub fn array16(&mut self) -> Result<[u8; 16]> {
        Ok(self.take(16)?.try_into().expect("len 16"))
    }

    /// Reads a length-prefixed byte string.
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(ms: u64, counter: u32, dev: u8) -> Hlc {
        Hlc {
            physical_ms: ms,
            counter,
            device: DeviceId([dev; 16]),
        }
    }

    #[test]
    fn hlc_orders_by_time_then_counter_then_device() {
        assert!(hlc(2, 0, 0) > hlc(1, 9, 9));
        assert!(hlc(1, 2, 0) > hlc(1, 1, 9));
        assert!(hlc(1, 1, 2) > hlc(1, 1, 1));
    }

    #[test]
    fn clock_is_strictly_increasing_and_follows_observed() {
        let clock = HlcClock::new(DeviceId([1; 16]));
        let a = clock.now();
        let b = clock.now();
        assert!(b > a);
        let future = hlc(a.physical_ms + 60_000, 7, 2);
        clock.observe(&future);
        let c = clock.now();
        assert!((c.physical_ms, c.counter) > (future.physical_ms, future.counter));
    }

    #[test]
    fn frame_round_trips() {
        let frame = Frame {
            lsn: 42,
            commit_hlc: hlc(1000, 3, 7),
            origin: Some(Origin {
                device: DeviceId([9; 16]),
                first_seq: 5,
                last_seq: 6,
            }),
            ops: vec![
                Op {
                    kind: OpKind::Put,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    hlc: hlc(999, 0, 9),
                },
                Op {
                    kind: OpKind::Delete,
                    key: b"k2".to_vec(),
                    value: vec![],
                    hlc: hlc(999, 1, 9),
                },
            ],
        };
        let mut buf = Vec::new();
        frame.encode(&mut buf);
        let mut dec = Dec::new(&buf);
        assert_eq!(Frame::decode(&mut dec).unwrap(), frame);
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn truncated_input_is_an_error_not_a_panic() {
        let frame = Frame {
            lsn: 1,
            commit_hlc: hlc(1, 0, 1),
            origin: None,
            ops: vec![Op {
                kind: OpKind::Put,
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                hlc: hlc(1, 0, 1),
            }],
        };
        let mut buf = Vec::new();
        frame.encode(&mut buf);
        for cut in 0..buf.len() {
            assert!(Frame::decode(&mut Dec::new(&buf[..cut])).is_err());
        }
    }

    #[test]
    fn ops_round_trip() {
        let ops = vec![Op {
            kind: OpKind::Put,
            key: b"a".to_vec(),
            value: vec![0u8; 100],
            hlc: hlc(5, 5, 5),
        }];
        assert_eq!(decode_ops(&encode_ops(&ops)).unwrap(), ops);
    }
}
