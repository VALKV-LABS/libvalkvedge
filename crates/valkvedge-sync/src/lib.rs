//! Replication for valkvedge: keeping a local state in step with a database's
//! log, and the writer role that produces that log.
//!
//! - [`pull`] brings any [`StateStore`] up to date, from frames when it can
//!   and from a snapshot when it must.
//! - [`Writer`] owns a database: it orders sync writes, grouping those that
//!   arrive together into one segment, resolves pushed (buffered) writes with
//!   last-write-wins, and compacts.
//!
//! This crate knows nothing about the local storage engine. The `valkvedge`
//! crate implements [`StateStore`] on top of it.

#![forbid(unsafe_code)]

mod pull;
mod state;
mod writer;

pub use pull::{pull, remote_head, PullOutcome};
pub use state::{MemState, StateStore};
pub use writer::{
    Change, CommitCounters, Condition, OpOutcome, OutboxEntry, PushReport, Writer, WriterConfig,
};
