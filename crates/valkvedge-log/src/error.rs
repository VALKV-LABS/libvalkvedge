//! The error type shared by every crate in this workspace.

use std::fmt;

/// Result alias used across the workspace.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a pushed operation was not applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The operation's timestamp is too far ahead of the writer's clock.
    ClockSkew,
    /// The operation is older than the tombstone retention window.
    StaleWrite,
}

/// Every failure the library can report.
///
/// [`Error::code`] returns a stable, machine-readable code. The message is for
/// humans and may change.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The object store returned an error or could not be reached.
    #[error("object store unavailable: {0}")]
    Unavailable(String),
    /// Stored data failed a checksum or could not be decoded.
    #[error("corrupt data: {0}")]
    Corrupt(String),
    /// An object was written by a newer, incompatible format version.
    #[error("unsupported format version {found} (this build supports up to {supported})")]
    UnsupportedVersion {
        /// Version found in the object.
        found: u16,
        /// Highest version this build can read.
        supported: u16,
    },
    /// Another live node owns the database.
    #[error("lease is held by node {owner}")]
    LeaseHeld {
        /// Node id of the current owner.
        owner: String,
    },
    /// This writer lost ownership of the database and must reopen it.
    #[error("writer was fenced: another node took over this database")]
    Fenced,
    /// The database, object, or key does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// The local store failed.
    #[error("local store error: {0}")]
    Local(String),
    /// The database was opened read-only, or the handle cannot write.
    #[error("database is read-only: {0}")]
    ReadOnly(String),
    /// A database name does not match `^[a-z][a-z0-9-]{1,47}$`.
    #[error("invalid database name: {0:?}")]
    InvalidName(String),
    /// A key is empty or longer than the limit.
    #[error("key is empty or larger than {limit} bytes")]
    KeyTooLarge {
        /// Maximum key size in bytes.
        limit: usize,
    },
    /// A value is longer than the limit.
    #[error("value is larger than {limit} bytes")]
    ValueTooLarge {
        /// Maximum value size in bytes.
        limit: usize,
    },
    /// A batch is larger than the limit once encoded.
    #[error("batch is larger than {limit} bytes")]
    BatchTooLarge {
        /// Maximum encoded batch size in bytes.
        limit: usize,
    },
    /// The outbox reached its configured size.
    #[error("outbox is full ({limit} bytes)")]
    OutboxFull {
        /// Configured outbox limit in bytes.
        limit: u64,
    },
    /// Every open database was busy and none could be closed in time.
    #[error("too many open databases (limit {limit})")]
    TooManyOpenDatabases {
        /// Configured limit.
        limit: usize,
    },
    /// The encryption key is missing, wrong, or was supplied for a plaintext database.
    #[error("invalid encryption key: {0}")]
    InvalidEncryptionKey(String),
    /// A pushed operation was refused. See [`RejectReason`].
    #[error("write rejected: {0:?}")]
    Rejected(RejectReason),
    /// The requested feature is not available in this build or mode.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl Error {
    /// Stable code for this error.
    pub fn code(&self) -> &'static str {
        match self {
            Error::Unavailable(_) => "UNAVAILABLE",
            Error::Corrupt(_) => "CORRUPT",
            Error::UnsupportedVersion { .. } => "UNSUPPORTED_VERSION",
            Error::LeaseHeld { .. } => "LEASE_HELD",
            Error::Fenced => "FENCED",
            Error::NotFound(_) => "NOT_FOUND",
            Error::Local(_) => "LOCAL_STORE",
            Error::ReadOnly(_) => "READ_ONLY",
            Error::InvalidName(_) => "INVALID_NAME",
            Error::KeyTooLarge { .. } => "KEY_TOO_LARGE",
            Error::ValueTooLarge { .. } => "VALUE_TOO_LARGE",
            Error::BatchTooLarge { .. } => "BATCH_TOO_LARGE",
            Error::OutboxFull { .. } => "OUTBOX_FULL",
            Error::TooManyOpenDatabases { .. } => "TOO_MANY_OPEN_DATABASES",
            Error::InvalidEncryptionKey(_) => "INVALID_ENCRYPTION_KEY",
            Error::Rejected(RejectReason::ClockSkew) => "CLOCK_SKEW",
            Error::Rejected(RejectReason::StaleWrite) => "STALE_WRITE",
            Error::Unsupported(_) => "UNSUPPORTED",
        }
    }

    /// Builds a [`Error::Corrupt`] from anything printable.
    pub fn corrupt(msg: impl fmt::Display) -> Self {
        Error::Corrupt(msg.to_string())
    }

    /// Builds a [`Error::Local`] from anything printable.
    pub fn local(msg: impl fmt::Display) -> Self {
        Error::Local(msg.to_string())
    }
}
