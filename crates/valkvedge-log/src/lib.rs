//! Log format, object-store access, leases and fencing for valkvedge.
//!
//! A database is an append-only log of [`Frame`]s kept in an object store.
//! Exactly one process at a time may append to it; that is enforced with
//! conditional writes, not with a lock service:
//!
//! - ownership is a lease object, taken and handed over by compare-and-swap
//!   ([`lease`]);
//! - every log segment is written create-only to a key that is its sequence
//!   number, so two writers can never both occupy a position ([`LogWriter`]);
//! - a new owner rewrites the manifest and writes a marker segment, which makes
//!   a previous owner's next append and next compaction fail.
//!
//! The object formats are described in `docs/format.md`.

#![forbid(unsafe_code)]

pub mod error;
pub mod format;
pub mod lease;
pub mod log;
pub mod store;
pub mod types;

pub use error::{Error, RejectReason, Result};
pub use format::{Layout, Manifest, PartRef, Record, Segment, FORMAT_VERSION};
pub use lease::{Lease, LeaseConfig, LeaseRecord, LeaseState, Node};
pub use log::{
    read_manifest, read_segments, read_snapshot_part, ChainPos, LogWriter, ReadSegment,
    SnapshotUpload,
};
pub use store::{PutOutcome, Store, Versioned};
pub use types::{has_expired, now_ms, Frame, Hlc, HlcClock, Lsn, Op, OpKind, Origin, ReplicaId};
