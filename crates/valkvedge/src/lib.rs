//! valkvedge: a local-first embedded key-value store.
//!
//! Reads come from a local store and never touch the network. The durable
//! copy of a database is an append-only log in an object store such as S3,
//! with exactly one writer at a time.
//!
//! ```no_run
//! # async fn demo() -> Result<(), valkvedge::ValkvEdgeError> {
//! use valkvedge::{Durability, ValkvEdge};
//!
//! // Local only. Add `.object_store(store, "prefix")` to keep the durable
//! // copy in S3.
//! let kv = ValkvEdge::builder("/var/lib/myapp/kv").build()?;
//! let db = kv.database("telemetry").open().await?;
//!
//! db.put(b"sensor/1", b"21.5").await?;
//! assert_eq!(db.get(b"sensor/1")?.as_deref(), Some(&b"21.5"[..]));
//!
//! let mut batch = db.batch();
//! batch.put(b"a", b"1").delete(b"b");
//! batch.commit_with(Durability::Sync).await?;
//! # Ok(()) }
//! ```
//!
//! # Write modes
//!
//! - [`Durability::Sync`]: the write returns after its frame is stored in the
//!   object store. Nothing acknowledged is lost if the process or machine dies.
//! - [`Durability::Buffered`]: the write returns after it is in the local
//!   outbox. It is readable at once and uploaded in batches. It works offline.
//!   It can be lost if the replica is lost before the upload, and it can lose a
//!   conflict to a newer write to the same key (see [`Database::take_events`]).
//!
//! # Conditional writes
//!
//! Every value has a [`Version`] that changes with each write to its key.
//! A sync batch can require keys to be at a version, or to have no value,
//! and is then written only if they are. Use it for counters, stock levels and
//! anything else where last write wins would lose an update:
//!
//! ```no_run
//! # async fn demo(db: valkvedge::Database) -> Result<(), valkvedge::ValkvEdgeError> {
//! // Read, change and write back, retried if another writer got there first.
//! db.update(b"stock/sku-1", |current| {
//!     let n: u64 = current.map_or(0, |v| String::from_utf8_lossy(v).parse().unwrap_or(0));
//!     Some((n - 1).to_string().into_bytes())
//! })
//! .await?;
//! # Ok(()) }
//! ```
//!
//! # Watching for changes
//!
//! [`Database::watch`] reports every change to keys under a prefix as it
//! reaches the local copy, whoever made it: this process, a replica pull, or
//! another replica's write arriving from a hosted service.

#![forbid(unsafe_code)]

mod crypto;
mod local;
mod prefixes;
mod remote;
mod watch;

use crypto::{Cipher, KeyBytes};
use local::{ChangeFeed, LocalStore};
use prefixes::Prefixes;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use valkvedge_log::{Frame, HlcClock, Layout, Node, Store};
use valkvedge_sync::{Change, CommitCounters, Condition, StateStore, Writer};
use zeroize::Zeroizing;

pub use crypto::{key_from_hex, key_id, key_to_hex};
pub use local::Iter;
pub use prefixes::MAX_PREFIXES;
pub use valkvedge_log::{Error as ValkvEdgeError, LeaseConfig, Lsn, OpKind, RejectReason, Result};
pub use valkvedge_log::{Hlc, KeySlot, Op, ReplicaId};
pub use valkvedge_sync::{History, OpOutcome, OutboxEntry, Point, PushReport, WriterConfig};

/// Where scratch stores for rebuilding a past state are made, under the root.
/// Database names cannot start with a dot.
const SCRATCH_DIR: &str = ".scratch";

/// Names the database's key slots in a `CONDITION_FAILED` when they changed.
const KEY_SLOTS_KEY: &[u8] = b"valkvedge/key-slots";

/// What a restore or a branch did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Restored {
    /// The point that was restored, as an LSN of the source's history.
    pub from: Lsn,
    /// The LSN the restored state has now: one past the tail when it was
    /// installed, so replicas see it as new.
    pub lsn: Lsn,
}

/// A local store for rebuilding a past state, removed when dropped.
struct Scratch {
    /// Always present until dropped.
    store: Option<Arc<LocalStore>>,
    path: PathBuf,
}

impl Scratch {
    fn store(&self) -> &Arc<LocalStore> {
        self.store.as_ref().expect("present until dropped")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Closed first, so its files are not written as they are removed.
        drop(self.store.take());
        // Best effort: another holder may still be closing it. A leftover
        // is only disk.
        if let Err(e) = std::fs::remove_dir_all(&self.path) {
            tracing::debug!(path = %self.path.display(), error = %e, "scratch store not removed");
        }
    }
}
pub use watch::{Watch, WatchEvent};

/// Re-export of the object store crate, for building a store to pass to
/// [`Builder::object_store`].
pub use object_store;

/// Largest key, in bytes.
pub const MAX_KEY_BYTES: usize = 1024;
/// Largest value, in bytes.
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
/// Largest batch, in bytes of keys and values.
pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// When a write is acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// After the write is stored in the object store.
    #[default]
    Sync,
    /// After the write is in the local outbox.
    Buffered,
}

/// What this process does for the databases it opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Role {
    /// Owns the databases: the only process that appends to their logs.
    #[default]
    Writer,
    /// Follows the log read-only.
    Replica,
}

/// Tunables. The defaults follow the specification.
#[derive(Debug, Clone)]
pub struct Options {
    /// Most databases kept open at once.
    pub max_open_databases: usize,
    /// A database unused for this long is closed.
    pub idle_close_after: Duration,
    /// How long an open waits for a busy database to become closable.
    pub open_wait_timeout: Duration,
    /// Block cache per open database, in bytes.
    pub cache_bytes_per_database: u64,
    /// Flush every local write to disk before returning. Off by default: the
    /// operating system writes back shortly after, and a power failure can
    /// lose the last moments of buffered writes.
    pub local_fsync: bool,
    /// Buffered writes fail once the outbox holds this many bytes.
    pub outbox_max_bytes: u64,
    /// The outbox is pushed at least this often.
    pub push_interval: Duration,
    /// The outbox is pushed once it holds this many bytes.
    pub push_bytes: u64,
    /// How often a replica pulls. A hosted replica asks to be told of changes
    /// instead (`follow_wait`), and polls this often only when the service
    /// does not hold its requests.
    pub pull_interval: Duration,
    /// How long a hosted service may hold a replica's request for changes
    /// that have not happened yet. It answers as soon as there are some, so
    /// changes reach the replica at once rather than at its next poll. Zero
    /// turns this off: the replica polls every `pull_interval`.
    pub follow_wait: Duration,
    /// When another live node owns a database, watch it for one lease TTL and
    /// take over if it is dead, instead of failing immediately.
    pub wait_for_lease: bool,
    /// Heartbeat and lease timing.
    pub lease: LeaseConfig,
    /// Conflict-resolution and compaction settings for the writer role.
    pub writer: WriterConfig,
    /// Identity of this process among those sharing the object store.
    /// Random when not set.
    pub node_id: Option<String>,
    /// An address written into the leases this process takes, so other
    /// processes sharing the object store can tell where the owner of a
    /// database can be reached. Empty when not set. The library does not
    /// connect to it.
    pub node_addr: String,
    /// Run the background task that pushes outboxes, pulls replicas and
    /// closes idle databases.
    pub background: bool,
    /// Longest a sync write waits. After this it fails with `UNAVAILABLE`; the
    /// write may or may not have been stored. Reads and buffered writes never
    /// wait on the object store.
    pub sync_write_timeout: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            max_open_databases: 200,
            idle_close_after: Duration::from_secs(15 * 60),
            open_wait_timeout: Duration::from_secs(5),
            cache_bytes_per_database: 8 * 1024 * 1024,
            local_fsync: false,
            outbox_max_bytes: 256 * 1024 * 1024,
            push_interval: Duration::from_secs(5),
            push_bytes: 5 * 1024 * 1024,
            pull_interval: Duration::from_secs(1),
            follow_wait: Duration::from_secs(25),
            wait_for_lease: true,
            lease: LeaseConfig::default(),
            writer: WriterConfig::default(),
            node_id: None,
            node_addr: String::new(),
            background: true,
            sync_write_timeout: Duration::from_secs(10),
        }
    }
}

enum Backend {
    Local,
    Object {
        store: Store,
        prefix: String,
        role: Role,
    },
    /// A service owns the databases; this process reaches them over HTTP.
    Remote {
        url: String,
        auth: Auth,
    },
}

/// How this process proves itself to a hosted service.
#[derive(Clone)]
pub(crate) enum Auth {
    /// An account API key, the same for every database.
    ApiKey(String),
    /// Short-lived tokens, one per database, fetched when needed.
    Tokens(Arc<dyn TokenSource>),
}

/// The future a [`TokenSource`] returns.
pub type TokenFuture = Pin<Box<dyn Future<Output = Result<String>> + Send>>;

/// Supplies short-lived access tokens for a hosted service, one database at
/// a time. See [`Builder::remote_with_tokens`].
///
/// Usually this asks your own backend, which holds the API key and mints
/// tokens for the user signed in on this replica. It is called when a
/// database is first used, shortly before its token expires, and when the
/// service refuses a token. Any closure `Fn(String) -> impl Future<Output =
/// Result<String>>` is a token source; it receives the database name.
pub trait TokenSource: Send + Sync + 'static {
    /// A token for `database`.
    fn token(&self, database: &str) -> TokenFuture;
}

impl<F, Fut> TokenSource for F
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String>> + Send + 'static,
{
    fn token(&self, database: &str) -> TokenFuture {
        Box::pin(self(database.to_string()))
    }
}

/// Configures and creates a [`ValkvEdge`].
pub struct Builder {
    root: PathBuf,
    options: Options,
    object: Option<(Store, String)>,
    remote: Option<(String, Auth)>,
    role: Role,
}

impl Builder {
    /// Replaces every option at once.
    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// Sets the most databases kept open at once.
    pub fn max_open_databases(mut self, n: usize) -> Self {
        self.options.max_open_databases = n.max(1);
        self
    }

    /// Keeps the durable copy of every database in `store`, under `prefix`.
    ///
    /// For Amazon S3 the store must be built with conditional puts enabled.
    pub fn object_store(
        mut self,
        store: Arc<dyn object_store::ObjectStore>,
        prefix: impl Into<String>,
    ) -> Self {
        self.object = Some((Store::new(store), prefix.into()));
        self
    }

    /// Sets whether this process writes or only follows. Default: writer.
    pub fn role(mut self, role: Role) -> Self {
        self.role = role;
        self
    }

    /// Connects to a hosted service instead of an object store of your own.
    ///
    /// The replica needs no storage credentials, only the service's URL and an
    /// API key. Sync writes are confirmed by the service, buffered writes are
    /// pushed to it, and the local copy follows it. Reads stay local.
    /// Databases are addressed by the names they have in the service. See
    /// `docs/hosted-api.md`.
    pub fn remote(mut self, url: impl Into<String>, api_key: impl Into<String>) -> Self {
        self.remote = Some((url.into(), Auth::ApiKey(api_key.into())));
        self
    }

    /// Connects to a hosted service with short-lived tokens instead of an
    /// API key.
    ///
    /// Use this on replicas you do not control, such as an app on a user's
    /// phone: the API key stays on your backend, which mints a token for one
    /// database with read or write access, and the replica never holds more
    /// than that. `tokens` is asked for a token per database and asked again
    /// before one expires or when the service refuses it. Everything else
    /// works as with [`Builder::remote`].
    pub fn remote_with_tokens(mut self, url: impl Into<String>, tokens: impl TokenSource) -> Self {
        self.remote = Some((url.into(), Auth::Tokens(Arc::new(tokens))));
        self
    }

    /// Creates the client. Does not touch the network.
    pub fn build(self) -> Result<ValkvEdge> {
        std::fs::create_dir_all(&self.root).map_err(ValkvEdgeError::local)?;
        let backend = match (self.object, self.remote) {
            (Some(_), Some(_)) => {
                return Err(ValkvEdgeError::Unsupported(
                    "choose either an object store or a hosted service, not both".into(),
                ))
            }
            (None, None) => Backend::Local,
            (Some((store, prefix)), None) => Backend::Object {
                store,
                prefix: prefix.trim_matches('/').to_string(),
                role: self.role,
            },
            (None, Some((url, auth))) => Backend::Remote { url, auth },
        };
        let inner = Arc::new(Inner {
            root: self.root,
            options: self.options,
            backend,
            dbs: Mutex::new(HashMap::new()),
            open_lock: Mutex::new(()),
            open_count: AtomicUsize::new(0),
            evictions: AtomicU64::new(0),
            commits: Arc::default(),
            started: Instant::now(),
            node: tokio::sync::OnceCell::new(),
        });
        if inner.options.background {
            let tick = match &inner.backend {
                // Only to delete expired values.
                Backend::Local => Some(inner.options.push_interval),
                Backend::Object {
                    role: Role::Writer, ..
                } => Some(inner.options.push_interval),
                Backend::Object { .. } | Backend::Remote { .. } => {
                    Some(inner.options.pull_interval)
                }
            };
            if let (Some(tick), Ok(handle)) = (tick, tokio::runtime::Handle::try_current()) {
                let weak = Arc::downgrade(&inner);
                handle.spawn(async move {
                    loop {
                        tokio::time::sleep(tick).await;
                        match weak.upgrade() {
                            Some(inner) => inner.tick().await,
                            None => break,
                        }
                    }
                });
            }
        }
        Ok(ValkvEdge { inner })
    }
}

struct Inner {
    root: PathBuf,
    options: Options,
    backend: Backend,
    dbs: Mutex<HashMap<String, Arc<Shared>>>,
    open_lock: Mutex<()>,
    open_count: AtomicUsize,
    evictions: AtomicU64,
    /// Sync commits written by this process's writers, and in how many segments.
    commits: Arc<CommitCounters>,
    started: Instant,
    node: tokio::sync::OnceCell<Node>,
}

struct Shared {
    name: String,
    path: PathBuf,
    slot: Mutex<Option<Arc<Open>>>,
    last_used_ms: AtomicU64,
    has_outbox: AtomicBool,
    cipher: OnceLock<Option<Arc<Cipher>>>,
    /// Set by the first open: true when the database was opened for
    /// ingestion ([`DatabaseBuilder::ingest`]) and keeps no local copy.
    ingest: OnceLock<bool>,
    flush_lock: tokio::sync::Mutex<()>,
    events: Mutex<VecDeque<WriteEvent>>,
    /// Changes to the local copy, for [`Database::watch`].
    feed: ChangeFeed,
    /// A task is keeping this hosted database's local copy current.
    following: AtomicBool,
    /// Set by the first open: for a partial copy, the prefixes it holds
    /// ([`DatabaseBuilder::prefixes`]).
    prefixes: OnceLock<Option<Prefixes>>,
    /// How far back this database's history reaches, when set for it
    /// ([`DatabaseBuilder::history`]); used by the next writer opened.
    history: Mutex<Option<Duration>>,
    /// The history's base is being moved forward.
    maintaining: AtomicBool,
    /// The recovery key made when this process encrypted the database, until
    /// it is taken ([`Database::take_recovery_key`]).
    recovery: Mutex<Option<Zeroizing<String>>>,
}

/// What a database records about its keys: its key slots, its encryption
/// marker, and whether it holds anything yet.
struct KeyState {
    slots: Vec<KeySlot>,
    check: Option<String>,
    empty: bool,
}

struct Open {
    store: Arc<LocalStore>,
    // One clock per database, shared with the writer, so a buffered write
    // made after a sync write is always stamped later.
    clock: Arc<HlcClock>,
    // Replaced after a failed write, so the next one starts from the log.
    session: tokio::sync::Mutex<Option<Arc<Session>>>,
}

enum Session {
    Local,
    Replica,
    Writer(Box<Writer<LocalStore>>),
    Remote(Box<remote::Client>),
}

impl Inner {
    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn layout(&self, name: &str) -> Option<(Store, Layout)> {
        match &self.backend {
            Backend::Local | Backend::Remote { .. } => None,
            Backend::Object { store, prefix, .. } => {
                Some((store.clone(), Layout::new(format!("{prefix}/{name}"))))
            }
        }
    }

    fn role(&self) -> Option<Role> {
        match &self.backend {
            Backend::Local | Backend::Remote { .. } => None,
            Backend::Object { role, .. } => Some(*role),
        }
    }

    fn is_remote(&self) -> bool {
        matches!(self.backend, Backend::Remote { .. })
    }

    async fn node(&self, store: &Store) -> Result<Node> {
        let node =
            self.node
                .get_or_try_init(|| async {
                    let id = self.options.node_id.clone().unwrap_or_else(|| {
                        format!("direct-{}", valkvedge_log::ReplicaId::random())
                    });
                    Node::start(
                        store.clone(),
                        id,
                        self.options.node_addr.clone(),
                        self.options.lease.clone(),
                    )
                    .await
                })
                .await?;
        Ok(node.clone())
    }

    /// Returns the open store for `shared`, opening it (and closing another
    /// database if the limit is reached) when needed.
    fn open(&self, shared: &Arc<Shared>, touch: bool) -> Result<Arc<Open>> {
        if touch {
            shared.last_used_ms.store(self.now_ms(), Ordering::Relaxed);
        }
        if let Some(open) = shared.slot.lock().expect("slot lock").clone() {
            return Ok(open);
        }
        let _opening = self.open_lock.lock().expect("open lock");
        if let Some(open) = shared.slot.lock().expect("slot lock").clone() {
            return Ok(open);
        }
        self.make_room()?;
        let store = Arc::new(self.open_store(shared)?);
        if let Some(Some(cipher)) = shared.cipher.get() {
            if let Some(check) = store.encryption_check()? {
                cipher.verify_check(&check)?;
            }
        }
        shared
            .has_outbox
            .store(store.outbox_stat()?.entries > 0, Ordering::Relaxed);
        let open = Arc::new(Open {
            clock: Arc::new(HlcClock::new(store.replica_id()?)),
            store,
            session: tokio::sync::Mutex::new(None),
        });
        *shared.slot.lock().expect("slot lock") = Some(open.clone());
        self.open_count.fetch_add(1, Ordering::SeqCst);
        Ok(open)
    }

    fn open_store(&self, shared: &Shared) -> Result<LocalStore> {
        // A database closed a moment ago may still be releasing its files.
        let deadline = Instant::now() + self.options.open_wait_timeout;
        loop {
            match LocalStore::open(
                &shared.path,
                self.options.cache_bytes_per_database,
                self.options.local_fsync,
                shared.feed.clone(),
                shared.prefixes(),
            ) {
                Ok(store) => return Ok(store),
                Err(e) if Instant::now() >= deadline => return Err(e),
                Err(_) => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    }

    fn make_room(&self) -> Result<()> {
        let limit = self.options.max_open_databases;
        let deadline = Instant::now() + self.options.open_wait_timeout;
        while self.open_count.load(Ordering::SeqCst) >= limit {
            if self.evict_least_recently_used() {
                continue;
            }
            if Instant::now() >= deadline {
                return Err(ValkvEdgeError::TooManyOpenDatabases { limit });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    }

    fn evict_least_recently_used(&self) -> bool {
        let mut candidates: Vec<Arc<Shared>> = self
            .dbs
            .lock()
            .expect("dbs lock")
            .values()
            .filter(|s| s.is_idle())
            .cloned()
            .collect();
        candidates.sort_by_key(|s| s.last_used_ms.load(Ordering::Relaxed));
        candidates.iter().any(|s| self.close_if_idle(s))
    }

    /// Closes the database unless an operation or iterator is using it.
    fn close_if_idle(&self, shared: &Shared) -> bool {
        let mut slot = shared.slot.lock().expect("slot lock");
        match slot.as_ref() {
            Some(open) if Arc::strong_count(open) == 1 => {
                // Dropping the only reference flushes and closes the store.
                // A held lease stays held by this process, which is still
                // alive and renewing its heartbeat.
                *slot = None;
                self.open_count.fetch_sub(1, Ordering::SeqCst);
                self.evictions.fetch_add(1, Ordering::Relaxed);
                true
            }
            _ => false,
        }
    }

    async fn tick(self: Arc<Self>) {
        let all: Vec<Arc<Shared>> = self
            .dbs
            .lock()
            .expect("dbs lock")
            .values()
            .cloned()
            .collect();
        let idle_ms = self.options.idle_close_after.as_millis() as u64;
        for shared in all {
            let db = Database {
                kv: self.clone(),
                shared: shared.clone(),
                durability: Durability::Sync,
                touch: false,
            };
            // The writer deletes what has expired.
            if !self.is_remote() && self.role() != Some(Role::Replica) && shared.is_open() {
                if let Err(e) = db.expire_if_writing().await {
                    tracing::warn!(database = %shared.name, error = %e, "could not delete expired values");
                }
            }
            let result = match self.role() {
                Some(Role::Writer) if shared.has_outbox.load(Ordering::Relaxed) => {
                    db.flush().await.map(|_| ())
                }
                Some(Role::Replica) if shared.is_open() => db.sync().await.map(|_| ()),
                // Hosted: push what is pending. A follower keeps each open
                // copy current, told of changes as they happen.
                None if self.is_remote() => {
                    if shared.is_open() && !shared.ingest() {
                        self.follow(&shared);
                    }
                    if shared.has_outbox.load(Ordering::Relaxed) {
                        db.flush().await.map(|_| ())
                    } else {
                        Ok(())
                    }
                }
                _ => Ok(()),
            };
            if let Err(e) = result {
                tracing::warn!(database = %shared.name, error = %e, "background sync failed");
            }
            drop(db);
            let unused = self
                .now_ms()
                .saturating_sub(shared.last_used_ms.load(Ordering::Relaxed));
            if unused >= idle_ms && !shared.has_outbox.load(Ordering::Relaxed) {
                self.close_if_idle(&shared);
            }
        }
    }
}

impl Inner {
    /// Starts the task that keeps a hosted database's local copy current,
    /// unless one is running. It ends when the database closes.
    fn follow(self: &Arc<Self>, shared: &Arc<Shared>) {
        if shared.following.swap(true, Ordering::SeqCst) {
            return;
        }
        let (weak, shared) = (Arc::downgrade(self), shared.clone());
        tokio::spawn(async move {
            let mut failures = 0u32;
            loop {
                let Some(kv) = weak.upgrade() else { break };
                let pull_interval = kv.options.pull_interval;
                let db = Database {
                    kv,
                    shared: shared.clone(),
                    durability: Durability::Sync,
                    touch: false,
                };
                let step = db.follow_step().await;
                drop(db);
                match step {
                    Ok(Follow::Closed) => break,
                    Ok(Follow::Changed | Follow::Waited) => failures = 0,
                    Ok(Follow::Polled) => {
                        failures = 0;
                        tokio::time::sleep(pull_interval).await;
                    }
                    Err(e) => {
                        tracing::warn!(database = %shared.name, error = %e, "could not follow the database");
                        failures = failures.saturating_add(1);
                        let backoff = pull_interval
                            .saturating_mul(1 << failures.min(5))
                            .min(Duration::from_secs(30));
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
            shared.following.store(false, Ordering::SeqCst);
        });
    }
}

/// What one step of following a hosted database did.
enum Follow {
    /// The database is closed: stop.
    Closed,
    /// The local copy moved forward.
    Changed,
    /// The service held the request and nothing changed: ask again at once.
    Waited,
    /// The service answered at once with nothing new: poll again later.
    Polled,
}

impl Shared {
    fn ingest(&self) -> bool {
        self.ingest.get().copied().unwrap_or(false)
    }

    /// For a partial copy, the prefixes it holds.
    fn prefixes(&self) -> Option<Prefixes> {
        self.prefixes.get().cloned().flatten()
    }

    /// The open database, without opening it.
    fn current(&self) -> Option<Arc<Open>> {
        self.slot.lock().expect("slot lock").clone()
    }

    fn is_open(&self) -> bool {
        self.slot.lock().expect("slot lock").is_some()
    }

    fn is_idle(&self) -> bool {
        matches!(self.slot.lock().expect("slot lock").as_ref(), Some(o) if Arc::strong_count(o) == 1)
    }
}

/// Entry point: a directory of local databases, optionally backed by an
/// object store. Cheap to clone.
#[derive(Clone)]
pub struct ValkvEdge {
    inner: Arc<Inner>,
}

impl ValkvEdge {
    /// Starts configuring a client that keeps its local files under `root`.
    pub fn builder(root: impl Into<PathBuf>) -> Builder {
        Builder {
            root: root.into(),
            options: Options::default(),
            object: None,
            remote: None,
            role: Role::default(),
        }
    }

    /// Starts opening the database called `name`.
    ///
    /// Names are 2 to 64 characters: lowercase letters, digits and `-`,
    /// starting with a letter or digit.
    pub fn database(&self, name: &str) -> DatabaseBuilder {
        DatabaseBuilder {
            kv: self.inner.clone(),
            // A slash is only valid through `namespace`.
            name: name.replace('/', "\0"),
            durability: Durability::default(),
            key: None,
            ingest: false,
            prefixes: None,
            history: None,
        }
    }

    /// A group of databases kept under their own directory locally and their
    /// own prefix in the object store.
    ///
    /// Databases in different namespaces are unrelated even when they share a
    /// name. Use it to keep one application's or one customer's databases
    /// together, for example to delete them as a unit. Namespace names follow
    /// the same rules as database names.
    pub fn namespace(&self, namespace: &str) -> Namespace {
        Namespace {
            kv: self.inner.clone(),
            namespace: namespace.replace('/', "\0"),
        }
    }

    /// Number of databases currently open.
    pub fn open_databases(&self) -> usize {
        self.inner.open_count.load(Ordering::SeqCst)
    }

    /// Number of times a database was closed to make room or for being idle.
    pub fn evictions(&self) -> u64 {
        self.inner.evictions.load(Ordering::Relaxed)
    }

    /// Sync commits this process has written as a writer, and the number of
    /// log segments they took. Commits made at the same time share a segment,
    /// so the second number grows more slowly under load.
    pub fn commits_and_segments(&self) -> (u64, u64) {
        (self.inner.commits.commits(), self.inner.commits.segments())
    }

    /// Releases every lease this process holds and closes every database that
    /// is not in use. Call before exiting so another process can take over
    /// without waiting for the lease to expire.
    pub async fn close(&self) -> Result<()> {
        let all: Vec<Arc<Shared>> = self
            .inner
            .dbs
            .lock()
            .expect("dbs lock")
            .values()
            .cloned()
            .collect();
        let mut first_error = None;
        for shared in all {
            let open = shared.slot.lock().expect("slot lock").clone();
            if let Some(open) = open {
                let session = open.session.lock().await.clone();
                if let Some(Session::Writer(writer)) = session.as_deref() {
                    if let Err(e) = writer.release().await {
                        first_error.get_or_insert(e);
                    }
                }
                drop(open);
                self.inner.close_if_idle(&shared);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

fn valid_part(part: &str) -> bool {
    let bytes = part.as_bytes();
    (2..=64).contains(&bytes.len())
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// A database is identified by `name` or `namespace/name`.
fn valid_name(name: &str) -> bool {
    let mut parts = name.split('/');
    let (first, second, third) = (parts.next(), parts.next(), parts.next());
    third.is_none() && first.is_some_and(valid_part) && second.is_none_or(valid_part)
}

/// A group of databases. See [`ValkvEdge::namespace`].
pub struct Namespace {
    kv: Arc<Inner>,
    namespace: String,
}

impl Namespace {
    /// Starts opening the database called `name` in this namespace.
    pub fn database(&self, name: &str) -> DatabaseBuilder {
        DatabaseBuilder {
            kv: self.kv.clone(),
            name: format!("{}/{}", self.namespace, name.replace('/', "\0")),
            durability: Durability::default(),
            key: None,
            ingest: false,
            prefixes: None,
            history: None,
        }
    }
}

/// Options for opening one database.
pub struct DatabaseBuilder {
    kv: Arc<Inner>,
    name: String,
    durability: Durability,
    key: Option<Zeroizing<KeyBytes>>,
    ingest: bool,
    prefixes: Option<Vec<Vec<u8>>>,
    history: Option<Duration>,
}

impl DatabaseBuilder {
    /// Keeps this database's history for `window`, so it can be restored
    /// or branched from at any point that recent ([`Database::restore`]).
    /// Overrides [`WriterConfig::history`] for this database; it takes
    /// effect when this process next becomes its writer.
    pub fn history(mut self, window: Duration) -> Self {
        self.history = Some(window);
        self
    }

    /// Keeps a partial copy: only keys starting with one of `prefixes` are
    /// downloaded, stored and followed. Reads and writes of any other key,
    /// and scans and watches that reach past the prefixes, fail with
    /// `NOT_REPLICATED`; [`Database::fetch`] still reads any key from a
    /// hosted service.
    ///
    /// For data shared by a team, where each copy should hold only what it
    /// is allowed to see. A hosted service sends only those keys, and a
    /// token can be limited to them; as a replica of an object store, the
    /// copy reads the whole log and keeps only those keys. A writer, or a
    /// database with no remote copy, cannot be partial. With a hosted
    /// service the prefixes must be UTF-8 text.
    ///
    /// At most [`MAX_PREFIXES`]. An empty prefix means every key. Opening
    /// a copy again with other prefixes downloads it again.
    pub fn prefixes<P: AsRef<[u8]>>(mut self, prefixes: impl IntoIterator<Item = P>) -> Self {
        self.prefixes = Some(prefixes.into_iter().map(|p| p.as_ref().to_vec()).collect());
        self
    }

    /// Opens the database for ingestion: writes are buffered on local disk,
    /// uploaded in the background, and removed once the service has them;
    /// nothing is downloaded and no local copy is kept. Reads go to the
    /// service with [`Database::fetch`] and the other `fetch` methods; the
    /// local reads ([`Database::get`], [`Database::prefix`], watching) fail
    /// with `UNSUPPORTED`.
    ///
    /// For devices that mostly write, such as sensors, where a copy of the
    /// whole database would only use disk. Needs a hosted service
    /// ([`Builder::remote`]). Writes default to [`Durability::Buffered`];
    /// sync and conditional writes still work and go straight to the service.
    pub fn ingest(mut self) -> Self {
        self.ingest = true;
        self.durability = Durability::Buffered;
        self
    }

    /// Sets the durability used by writes that do not name one.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Encrypts values before they are stored anywhere, and opens the
    /// database with `key`: one of its keys ([`Database::add_key`]).
    ///
    /// Values are sealed with the database's data key, which each of its keys
    /// opens. A new encrypted database also gets a recovery key, given out
    /// once by [`Database::take_recovery_key`]: store it somewhere safe. The
    /// keys themselves are never written anywhere; if every one is lost, the
    /// data cannot be recovered, by anyone.
    ///
    /// Keys (the names) are not encrypted: their bytes, value sizes,
    /// timestamps and expiry times remain visible to anyone who can read the
    /// local files or the object store.
    ///
    /// The first open of an encrypted database needs the network, unless
    /// this copy has opened it before.
    pub fn encryption_key(mut self, key: [u8; 32]) -> Self {
        self.key = Some(Zeroizing::new(key));
        self
    }

    /// Opens the database.
    ///
    /// An existing local copy opens without the network. A replica with no
    /// local copy downloads the current snapshot first.
    pub async fn open(self) -> Result<Database> {
        if !valid_name(&self.name) {
            return Err(ValkvEdgeError::InvalidName(self.name));
        }
        let shared = self
            .kv
            .dbs
            .lock()
            .expect("dbs lock")
            .entry(self.name.clone())
            .or_insert_with(|| {
                Arc::new(Shared {
                    path: self.kv.root.join(&self.name),
                    name: self.name.clone(),
                    slot: Mutex::new(None),
                    last_used_ms: AtomicU64::new(0),
                    has_outbox: AtomicBool::new(false),
                    cipher: OnceLock::new(),
                    ingest: OnceLock::new(),
                    flush_lock: tokio::sync::Mutex::new(()),
                    events: Mutex::new(VecDeque::new()),
                    feed: tokio::sync::broadcast::channel(WATCH_BUFFER).0,
                    following: AtomicBool::new(false),
                    prefixes: OnceLock::new(),
                    history: Mutex::new(None),
                    maintaining: AtomicBool::new(false),
                    recovery: Mutex::new(None),
                })
            })
            .clone();
        if let Some(history) = self.history {
            *shared.history.lock().expect("history lock") = Some(history);
        }

        // Without a key the database must not be encrypted: settled now. With
        // one, the data key it opens is found once the local store is open.
        if self.key.is_none() && shared.cipher.get_or_init(|| None).is_some() {
            return Err(ValkvEdgeError::InvalidEncryptionKey(
                "this database is already open with a key; open it with one of its keys".into(),
            ));
        }
        if self.ingest && !self.kv.is_remote() {
            return Err(ValkvEdgeError::Unsupported(
                "ingestion needs a hosted service to upload to: build with remote()".into(),
            ));
        }
        if *shared.ingest.get_or_init(|| self.ingest) != self.ingest {
            return Err(ValkvEdgeError::Unsupported(
                "this database is already open in this process for ingestion, or not for ingestion; open it the same way"
                    .into(),
            ));
        }
        let prefixes = match self.prefixes {
            Some(list) => Prefixes::new(list)?,
            None => None,
        };
        if let Some(prefixes) = &prefixes {
            if self.ingest {
                return Err(ValkvEdgeError::Unsupported(
                    "a database opened for ingestion keeps no copy, partial or whole".into(),
                ));
            }
            if self.kv.role() != Some(Role::Replica) && !self.kv.is_remote() {
                return Err(ValkvEdgeError::Unsupported(
                    "a partial copy follows a database kept elsewhere: use a hosted service or the replica role"
                        .into(),
                ));
            }
            if self.kv.is_remote()
                && prefixes
                    .as_slice()
                    .iter()
                    .any(|p| std::str::from_utf8(p).is_err())
            {
                return Err(ValkvEdgeError::Unsupported(
                    "with a hosted service, prefixes must be UTF-8 text".into(),
                ));
            }
        }
        if *shared.prefixes.get_or_init(|| prefixes.clone()) != prefixes {
            return Err(ValkvEdgeError::Unsupported(
                "this database is already open in this process with other prefixes; open it the same way"
                    .into(),
            ));
        }

        let db = Database {
            kv: self.kv,
            shared,
            durability: self.durability,
            touch: true,
        };
        let open = db.open()?;
        if let Some(key) = &self.key {
            db.set_cipher(&open, key).await?;
        }
        let pos = open.store.position()?;
        match db.kv.role() {
            None => {
                // No remote copy: the local store is the database.
                let empty = pos.applied_lsn == 0;
                let stored = open.store.encryption_check()?;
                if let Some(check) =
                    crypto::reconcile(db.cipher().map(Arc::as_ref), stored.as_deref(), empty)?
                {
                    open.store.set_encryption_check(&check)?;
                }
                // A replica with no local copy of a hosted database downloads
                // it, unless it is only ingesting.
                let cold = pos == Default::default() || open.store.needs_bootstrap()?;
                if db.kv.is_remote() && cold && !db.shared.ingest() {
                    drop(open);
                    db.sync().await?;
                }
                // From now on, changes made elsewhere are applied as they
                // happen, not at the next tick.
                if db.kv.is_remote() && !db.shared.ingest() && db.kv.options.background {
                    db.kv.follow(&db.shared);
                }
            }
            Some(Role::Replica) => {
                let cold = pos == Default::default() || open.store.needs_bootstrap()?;
                if cold {
                    drop(open);
                    db.sync().await?;
                }
            }
            Some(Role::Writer) => {}
        }
        Ok(db)
    }
}

/// The process that holds a database's lease. See [`Database::owner`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    /// The owner's node id.
    pub node_id: String,
    /// The address the owner advertised, empty if it set none.
    pub node_addr: String,
}

/// The version of a key's value. Every write to a key gives it a new one.
///
/// Read it with [`Database::get_versioned`] and require it with
/// [`Batch::if_version`]. As text it is 56 lowercase hex characters; treat
/// that as opaque.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Version(Hlc);

impl Version {
    /// The timestamp the write was stamped with.
    pub fn hlc(&self) -> Hlc {
        self.0
    }

    /// The version of a write stamped `hlc`.
    pub fn from_hlc(hlc: Hlc) -> Self {
        Version(hlc)
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut bytes = Vec::with_capacity(Hlc::SIZE);
        self.0.encode(&mut bytes);
        for byte in bytes {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// A string that is not a [`Version`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidVersion;

impl std::fmt::Display for InvalidVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a version is {} hex characters", Hlc::SIZE * 2)
    }
}

impl std::error::Error for InvalidVersion {}

impl std::str::FromStr for Version {
    type Err = InvalidVersion;

    fn from_str(text: &str) -> std::result::Result<Self, Self::Err> {
        if text.len() != Hlc::SIZE * 2 || !text.is_ascii() {
            return Err(InvalidVersion);
        }
        let bytes = (0..Hlc::SIZE)
            .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16))
            .collect::<std::result::Result<Vec<u8>, _>>()
            .map_err(|_| InvalidVersion)?;
        let mut dec = valkvedge_log::types::Dec::new(&bytes);
        Hlc::decode(&mut dec)
            .map(Version)
            .map_err(|_| InvalidVersion)
    }
}

/// What a write returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// The frame's position in the log, for writes that reached it.
    pub lsn: Option<Lsn>,
    /// The outbox sequence number, for buffered writes.
    pub outbox_seq: Option<u64>,
    /// The version each write in the batch gave its key, in batch order.
    /// Empty when a hosted service did not report them.
    pub versions: Vec<Version>,
}

/// A point-in-time summary of a database's local state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// Last log frame applied locally.
    pub applied_lsn: Lsn,
    /// Generation the local copy is in.
    pub generation: u64,
    /// Buffered batches not yet confirmed.
    pub outbox_entries: u64,
    /// Bytes of buffered batches not yet confirmed.
    pub outbox_bytes: u64,
    /// Local disk usage in bytes.
    pub disk_bytes: u64,
}

/// A buffered operation that did not take effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteEvent {
    /// Outbox sequence number of the batch it belonged to.
    pub outbox_seq: u64,
    /// The key.
    pub key: Vec<u8>,
    /// Why: superseded by a newer write, or rejected.
    pub outcome: WriteOutcome,
}

/// Why a buffered operation did not take effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// A newer write to the same key already existed.
    Superseded,
    /// The writer refused it.
    Rejected(RejectReason),
}

const MAX_EVENTS: usize = 1024;

/// Expired values deleted in one commit.
const EXPIRE_BATCH: usize = 1000;
/// Batches of changes a watcher may fall behind by before it is told to
/// reload ([`WatchEvent::Reset`]).
const WATCH_BUFFER: usize = 1024;
/// How many times [`Database::update`] tries before giving up.
const UPDATE_ATTEMPTS: usize = 16;
/// The most entries [`Database::fetch_page`] asks the service for at once.
const FETCH_PAGE_MAX: usize = 200;

/// One page read from a hosted service by [`Database::fetch_page`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FetchPage {
    /// Keys and their values, in key order.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Pass to the next call to continue; `None` when there is no more.
    pub next_page_token: Option<String>,
}

/// A handle to one database. Cheap to clone.
///
/// The handle stays valid when the database is closed to make room for
/// others: the next operation reopens it.
#[derive(Clone)]
pub struct Database {
    kv: Arc<Inner>,
    shared: Arc<Shared>,
    durability: Durability,
    touch: bool,
}

impl Database {
    fn open(&self) -> Result<Arc<Open>> {
        self.kv.open(&self.shared, self.touch)
    }

    fn cipher(&self) -> Option<&Arc<Cipher>> {
        self.shared.cipher.get().and_then(Option::as_ref)
    }

    /// Settles the data key a keyed open uses: the one `key` opens.
    async fn set_cipher(&self, open: &Arc<Open>, key: &KeyBytes) -> Result<()> {
        let wanted = match self.shared.cipher.get() {
            Some(None) => {
                return Err(ValkvEdgeError::InvalidEncryptionKey(
                    "this database is already open without a key".into(),
                ))
            }
            // Already open in this process: this key must open the same data key.
            Some(Some(current)) => {
                let slots = open.store.key_slots()?;
                let opens = crypto::key_id(key) == crypto::key_id(current.data_key())
                    || crypto::unwrap(&slots, key, &self.shared.name)
                        .is_some_and(|data_key| *data_key == *current.data_key());
                if !opens {
                    return Err(ValkvEdgeError::InvalidEncryptionKey(
                        "this database is already open with a key this one is not".into(),
                    ));
                }
                return Ok(());
            }
            None => self.data_cipher(open, key).await?,
        };
        let current = self.shared.cipher.get_or_init(|| Some(wanted.clone()));
        if !current.as_ref().is_some_and(|c| c.same_key(&wanted)) {
            return Err(ValkvEdgeError::InvalidEncryptionKey(
                "this database was opened at the same time with another key".into(),
            ));
        }
        // Remember the verified key locally so a wrong key is caught offline too.
        let check = open.store.encryption_check()?;
        match check {
            Some(check) => wanted.verify_check(&check)?,
            None if open.store.position()?.applied_lsn == 0
                || !open.store.key_slots()?.is_empty() =>
            {
                open.store.set_encryption_check(&wanted.make_check()?)?
            }
            None => {}
        }
        Ok(())
    }

    /// The cipher for the data key `key` opens. A new, empty database gets a
    /// data key and two key slots: `key`'s and a recovery key's.
    async fn data_cipher(&self, open: &Arc<Open>, key: &KeyBytes) -> Result<Arc<Cipher>> {
        let name = self.shared.name.clone();
        let cipher = |data_key: &KeyBytes| Arc::new(Cipher::new(data_key, &name));
        // What this copy already knows opens it offline.
        let local = open.store.key_slots()?;
        if let Some(data_key) = crypto::unwrap(&local, key, &name) {
            return Ok(cipher(&data_key));
        }
        let encrypted_here = open.store.encryption_check()?.is_some();
        for _ in 0..2 {
            let state = match self.key_state(open).await {
                Ok(state) => state,
                // Offline, a copy that has opened this database with its one
                // key before (it was encrypted before key slots) opens again.
                Err(e) if local.is_empty() && encrypted_here => {
                    tracing::debug!(database = %name, error = %e, "the database's keys cannot be read; opening with the key itself");
                    return Ok(cipher(key));
                }
                Err(e) => return Err(e),
            };
            if !state.slots.is_empty() {
                let data_key = crypto::unwrap(&state.slots, key, &name).ok_or_else(|| {
                    ValkvEdgeError::InvalidEncryptionKey(
                        "this key is not one of the database's keys".into(),
                    )
                })?;
                let found = cipher(&data_key);
                if let Some(check) = &state.check {
                    found.verify_check(check)?;
                }
                open.store.set_key_slots(&state.slots)?;
                return Ok(found);
            }
            let fresh = state.empty
                && state.check.is_none()
                && !encrypted_here
                && open.store.position()?.applied_lsn == 0;
            if !fresh || self.kv.role() == Some(Role::Replica) {
                // Encrypted before key slots existed: its one key is its data
                // key. Its marker says whether it is this one.
                let found = cipher(key);
                match &state.check {
                    Some(check) => found.verify_check(check)?,
                    // Checked against this copy's own marker.
                    None if encrypted_here => {}
                    // A hosted service keeps no marker: a wrong key shows when
                    // a value does not open.
                    None if self.kv.is_remote() => {}
                    None => {
                        return Err(ValkvEdgeError::InvalidEncryptionKey(
                            "this database already holds unencrypted data".into(),
                        ))
                    }
                }
                return Ok(found);
            }
            match self.create_key_slots(open, key).await {
                Err(ValkvEdgeError::ConditionFailed { .. }) => continue, // another copy got there first
                other => return other,
            }
        }
        Err(ValkvEdgeError::Unavailable(
            "the database's keys kept changing while it was opened".into(),
        ))
    }

    /// Gives a new database a random data key, opened by `key` and by a new
    /// recovery key, which is kept for [`Database::take_recovery_key`].
    async fn create_key_slots(&self, open: &Arc<Open>, key: &KeyBytes) -> Result<Arc<Cipher>> {
        let name = &self.shared.name;
        let data_key = crypto::random_key();
        let recovery = crypto::random_key();
        let slots = vec![
            crypto::wrap(&data_key, key, name)?,
            crypto::wrap(&data_key, &recovery, name)?,
        ];
        let cipher = Arc::new(Cipher::new(&data_key, name));
        self.store_key_slots(open, &[], slots, Some(cipher.make_check()?))
            .await?;
        *self.shared.recovery.lock().expect("recovery lock") =
            Some(Zeroizing::new(crypto::key_to_hex(&recovery)));
        Ok(cipher)
    }

    /// What the database itself records about its keys: in the object store
    /// or the hosted service, or locally for a database with no other copy.
    async fn key_state(&self, open: &Arc<Open>) -> Result<KeyState> {
        if self.kv.is_remote() {
            let session = self.session_within_limit(open).await?;
            if let Session::Remote(client) = session.as_ref() {
                return client.key_state().await;
            }
        }
        match self.kv.role() {
            None => Ok(KeyState {
                slots: open.store.key_slots()?,
                check: open.store.encryption_check()?,
                empty: open.store.position()?.applied_lsn == 0,
            }),
            Some(Role::Writer) => {
                let session = self.session_within_limit(open).await?;
                let Session::Writer(writer) = session.as_ref() else {
                    return Err(ValkvEdgeError::local("a writer's session is not a writer"));
                };
                Ok(KeyState {
                    slots: writer.key_slots().await?,
                    check: writer.encryption_check().await?,
                    empty: writer.position().await?.applied_lsn == 0,
                })
            }
            Some(Role::Replica) => {
                let (store, layout) = self
                    .kv
                    .layout(&self.shared.name)
                    .expect("a replica has an object store");
                let manifest = valkvedge_log::read_manifest(&store, &layout)
                    .await?
                    .ok_or_else(|| ValkvEdgeError::NotFound(self.shared.name.clone()))?
                    .0;
                Ok(KeyState {
                    slots: manifest.key_slots,
                    check: manifest.encryption_check,
                    empty: false,
                })
            }
        }
    }

    /// Replaces the database's key slots, if they are still `expected`
    /// (otherwise `CONDITION_FAILED`), with `check` as its encryption marker
    /// when given. A hosted service keeps the slots only.
    async fn store_key_slots(
        &self,
        open: &Arc<Open>,
        expected: &[KeySlot],
        slots: Vec<KeySlot>,
        check: Option<String>,
    ) -> Result<()> {
        let changed = || ValkvEdgeError::ConditionFailed {
            key: KEY_SLOTS_KEY.to_vec(),
        };
        let remote_session = if self.kv.is_remote() {
            Some(self.session_within_limit(open).await?)
        } else {
            None
        };
        match (remote_session.as_deref(), self.kv.role()) {
            (Some(Session::Remote(client)), _) => client.put_key_slots(expected, &slots).await?,
            (_, None) => {
                if open.store.key_slots()? != expected {
                    return Err(changed());
                }
                open.store.set_key_slots(&slots)?;
                if let Some(check) = &check {
                    open.store.set_encryption_check(check)?;
                }
            }
            (_, Some(Role::Writer)) => {
                let session = self.session_within_limit(open).await?;
                let Session::Writer(writer) = session.as_ref() else {
                    return Err(ValkvEdgeError::local("a writer's session is not a writer"));
                };
                if writer.key_slots().await? != expected {
                    return Err(changed());
                }
                // Slots first: a marker without them would make the data key
                // look like the key itself.
                writer.set_key_slots(slots.clone()).await?;
                if check.is_some() {
                    writer.set_encryption_check(check).await?;
                }
            }
            (_, Some(Role::Replica)) => {
                return Err(ValkvEdgeError::ReadOnly(
                    "a replica cannot change the database's keys; its writer can".into(),
                ))
            }
        }
        open.store.set_key_slots(&slots)
    }

    /// The recovery key made when this process encrypted the database, the
    /// first time it is asked for; `None` after, or if it did not.
    ///
    /// Store it where you keep secrets (a secret manager, or offline): it
    /// opens the database like any of its keys, and nobody else has it.
    pub fn take_recovery_key(&self) -> Option<String> {
        self.shared
            .recovery
            .lock()
            .expect("recovery lock")
            .take()
            .map(|key| key.to_string())
    }

    /// Lets `key` open this database too. Returns its id. Nothing is
    /// encrypted again: the key gets a copy of the data key, sealed with it.
    /// The database must be open with one of its keys.
    pub async fn add_key(&self, key: [u8; 32]) -> Result<String> {
        let key = Zeroizing::new(key);
        let cipher = self.keyed()?;
        let open = self.open()?;
        let state = self.key_state(&open).await?;
        let mut slots = state.slots.clone();
        let id = crypto::key_id(&key);
        if slots.iter().any(|s| s.id == id) {
            return Ok(id);
        }
        if slots.is_empty() {
            // Encrypted before key slots: its key is its data key, and gets
            // the first slot.
            slots.push(crypto::wrap(
                cipher.data_key(),
                cipher.data_key(),
                &self.shared.name,
            )?);
        }
        slots.push(crypto::wrap(cipher.data_key(), &key, &self.shared.name)?);
        self.store_key_slots(&open, &state.slots, slots, None)
            .await?;
        Ok(id)
    }

    /// Makes a new random key that opens this database, and returns it as 64
    /// hexadecimal characters, once. Store it where you keep secrets.
    pub async fn add_recovery_key(&self) -> Result<String> {
        let key = crypto::random_key();
        self.add_key(*key).await?;
        Ok(crypto::key_to_hex(&key))
    }

    /// Stops the key with this id from opening the database. The last key
    /// cannot be removed.
    ///
    /// For a database encrypted before key slots, its first key is also its
    /// data key: removing that key's slot does not stop it reading values
    /// already written.
    pub async fn remove_key(&self, id: &str) -> Result<()> {
        self.keyed()?;
        let open = self.open()?;
        let state = self.key_state(&open).await?;
        let slots: Vec<KeySlot> = state.slots.iter().filter(|s| s.id != id).cloned().collect();
        if slots.len() == state.slots.len() {
            return Err(ValkvEdgeError::NotFound(format!("key {id}")));
        }
        if slots.is_empty() {
            return Err(ValkvEdgeError::Unsupported(
                "a database keeps at least one key: add another before removing this one".into(),
            ));
        }
        self.store_key_slots(&open, &state.slots, slots, None).await
    }

    /// The ids of the keys that open this database ([`key_id`]).
    pub async fn key_ids(&self) -> Result<Vec<String>> {
        let cipher = self.keyed()?;
        let open = self.open()?;
        let state = self.key_state(&open).await?;
        if state.slots.is_empty() {
            return Ok(vec![crypto::key_id(cipher.data_key())]);
        }
        Ok(state.slots.into_iter().map(|s| s.id).collect())
    }

    fn keyed(&self) -> Result<Arc<Cipher>> {
        self.cipher().cloned().ok_or_else(|| {
            ValkvEdgeError::Unsupported("this database is not open with an encryption key".into())
        })
    }

    /// The key slots the database records, for a service that keeps them for
    /// clients that encrypt their own values. Needs no key: a slot reveals
    /// nothing without one of the keys. This process must be its writer.
    pub async fn stored_key_slots(&self) -> Result<Vec<KeySlot>> {
        let open = self.open()?;
        let session = self.session_within_limit(&open).await?;
        match session.as_ref() {
            Session::Writer(writer) => writer.key_slots().await,
            Session::Local => open.store.key_slots(),
            _ => Err(ValkvEdgeError::ReadOnly(
                "only the database's writer keeps its key slots".into(),
            )),
        }
    }

    /// Replaces the key slots the database records if they are still
    /// `expected`, for a service that keeps them for its clients. Fails with
    /// `CONDITION_FAILED` if they changed.
    pub async fn replace_stored_key_slots(
        &self,
        expected: &[KeySlot],
        slots: Vec<KeySlot>,
    ) -> Result<()> {
        let open = self.open()?;
        let session = self.session_within_limit(&open).await?;
        let changed = || ValkvEdgeError::ConditionFailed {
            key: KEY_SLOTS_KEY.to_vec(),
        };
        match session.as_ref() {
            Session::Writer(writer) => {
                if writer.key_slots().await? != expected {
                    return Err(changed());
                }
                writer.set_key_slots(slots).await
            }
            _ => Err(ValkvEdgeError::ReadOnly(
                "only the database's writer keeps its key slots".into(),
            )),
        }
    }

    /// The database's name.
    pub fn name(&self) -> &str {
        &self.shared.name
    }

    /// Refuses a local read of a database opened for ingestion, which keeps
    /// no local copy.
    fn local_reads(&self) -> Result<()> {
        if self.shared.ingest() {
            return Err(ValkvEdgeError::Unsupported(
                "this database is open for ingestion and keeps no local copy: read it from the service with fetch"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Refuses a key a partial copy does not hold.
    fn held(&self, key: &[u8]) -> Result<()> {
        match self.shared.prefixes.get().and_then(Option::as_ref) {
            Some(prefixes) if !prefixes.holds_key(key) => {
                Err(ValkvEdgeError::NotReplicated { key: key.to_vec() })
            }
            _ => Ok(()),
        }
    }

    /// Refuses a scan or watch of a prefix a partial copy does not hold whole.
    fn held_prefix(&self, prefix: &[u8]) -> Result<()> {
        match self.shared.prefixes.get().and_then(Option::as_ref) {
            Some(prefixes) if !prefixes.holds_prefix(prefix) => {
                Err(ValkvEdgeError::NotReplicated {
                    key: prefix.to_vec(),
                })
            }
            _ => Ok(()),
        }
    }

    /// Reads a key from the local store. Never touches the network.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.local_reads()?;
        self.held(key)?;
        let open = self.open()?;
        match (open.store.get_stored(key)?, self.cipher()) {
            (None, _) => Ok(None),
            (Some(stored), None) => Ok(Some(stored)),
            (Some(stored), Some(cipher)) => cipher.open(key, &stored).map(Some),
        }
    }

    /// Reads a key and the version of its value from the local store. Never
    /// touches the network.
    ///
    /// Pass the version to [`Batch::if_version`] to write only if nobody has
    /// written the key since. With a hosted service or as a replica, the
    /// local copy may be a moment behind; a write conditioned on an old
    /// version then fails and the copy is brought up to date.
    pub fn get_versioned(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Version)>> {
        self.local_reads()?;
        self.held(key)?;
        let open = self.open()?;
        match open.store.get_stored_versioned(key)? {
            None => Ok(None),
            Some((stored, hlc)) => {
                let value = match self.cipher() {
                    None => stored,
                    Some(cipher) => cipher.open(key, &stored)?,
                };
                Ok(Some((value, Version(hlc))))
            }
        }
    }

    /// Reports changes to keys starting with `prefix` as they reach the local
    /// copy: writes made here, buffered writes settling, and changes pulled
    /// from the object store or a hosted service. An empty prefix watches
    /// everything.
    ///
    /// A watched database stays open, and keeps following its hosted service
    /// or object store in the background, until the [`Watch`] is dropped.
    pub fn watch(&self, prefix: &[u8]) -> Result<Watch> {
        self.local_reads()?;
        self.held_prefix(prefix)?;
        let open = self.open()?;
        Ok(Watch::new(
            self.clone(),
            open,
            prefix.to_vec(),
            self.shared.feed.subscribe(),
        ))
    }

    /// Reads a key, computes its new value with `change`, and writes it back
    /// if nobody else wrote the key in between, trying again if they did.
    ///
    /// `change` gets the current value (`None` when there is none) and
    /// returns the new one (`None` deletes the key); it may run more than
    /// once. The write is a sync write. Fails with `CONDITION_FAILED` if the
    /// key keeps changing for 16 attempts.
    pub async fn update<F>(&self, key: &[u8], mut change: F) -> Result<Commit>
    where
        F: FnMut(Option<&[u8]>) -> Option<Vec<u8>>,
    {
        // A pending buffered write to the key would make every check fail
        // until it settles.
        if self.shared.has_outbox.load(Ordering::Relaxed) {
            self.flush().await?;
        }
        for attempt in 0..UPDATE_ATTEMPTS {
            if attempt > 0 {
                // Another writer won. Wait a moment, longer each time, so
                // writers racing for one key take turns instead of colliding
                // again and again.
                let ceiling = 1u64 << attempt.min(6);
                let pause = rand::Rng::gen_range(&mut rand::thread_rng(), 0..=ceiling);
                tokio::time::sleep(Duration::from_millis(pause)).await;
            }
            let current = if self.shared.ingest() {
                self.fetch_versioned(key).await?
            } else {
                self.get_versioned(key)?
            };
            let next = change(current.as_ref().map(|(value, _)| value.as_slice()));
            let mut batch = self.batch();
            match &current {
                Some((_, version)) => batch.if_version(key, version),
                None => batch.if_absent(key),
            };
            match (&current, next) {
                (None, None) => {
                    return Ok(Commit {
                        lsn: None,
                        outbox_seq: None,
                        versions: Vec::new(),
                    })
                }
                (_, Some(value)) => batch.put(key, &value),
                (Some(_), None) => batch.delete(key),
            };
            match batch.commit_with(Durability::Sync).await {
                Err(ValkvEdgeError::ConditionFailed { .. }) => continue,
                other => return other,
            }
        }
        Err(ValkvEdgeError::ConditionFailed { key: key.to_vec() })
    }

    /// Iterates keys in `range`, in order, from the local store.
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Result<Iter> {
        let own = |b: Bound<&K>| match b {
            Bound::Included(k) => Bound::Included(k.as_ref().to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref().to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.local_reads()?;
        if let Some(prefixes) = self.shared.prefixes.get().and_then(Option::as_ref) {
            let start = range.start_bound().map(K::as_ref);
            let end = range.end_bound().map(K::as_ref);
            if !prefixes.holds_range(start, end) {
                let start = match range.start_bound() {
                    Bound::Included(k) | Bound::Excluded(k) => k.as_ref().to_vec(),
                    Bound::Unbounded => Vec::new(),
                };
                return Err(ValkvEdgeError::NotReplicated { key: start });
            }
        }
        let open = self.open()?;
        let iters = open
            .store
            .scan_range((own(range.start_bound()), own(range.end_bound())));
        Ok(Iter::new(iters, self.cipher().cloned(), open))
    }

    /// Iterates keys starting with `prefix`, in order, from the local store.
    pub fn prefix(&self, prefix: &[u8]) -> Result<Iter> {
        self.local_reads()?;
        self.held_prefix(prefix)?;
        let open = self.open()?;
        let iters = open.store.scan_prefix(prefix);
        Ok(Iter::new(iters, self.cipher().cloned(), open))
    }

    /// The session with the hosted service, for reading from it.
    async fn service(&self) -> Result<Arc<Session>> {
        if !self.kv.is_remote() {
            return Err(ValkvEdgeError::Unsupported(
                "fetch reads from a hosted service, and this database has none: read it with get"
                    .into(),
            ));
        }
        let open = self.open()?;
        self.session_within_limit(&open).await
    }

    /// Reads a key from the hosted service rather than the local copy: the
    /// newest stored value, at the cost of a request. The key must be UTF-8
    /// text. This is how a database opened for ingestion is read; it works
    /// for any database that follows a hosted service.
    pub async fn fetch(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.fetch_versioned(key).await?.map(|(value, _)| value))
    }

    /// [`Database::fetch`] with the version of the value.
    pub async fn fetch_versioned(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Version)>> {
        let session = self.service().await?;
        let Session::Remote(client) = session.as_ref() else {
            return Err(ValkvEdgeError::Unsupported(
                "this database has no hosted service".into(),
            ));
        };
        match client.get(key).await? {
            None => Ok(None),
            Some((stored, hlc)) => {
                let value = match self.cipher() {
                    None => stored,
                    Some(cipher) => cipher.open(key, &stored)?,
                };
                Ok(Some((value, Version(hlc))))
            }
        }
    }

    /// One page of keys starting with `prefix`, in order, read from the
    /// hosted service. `limit` is 1 to 200. Pass the page's
    /// `next_page_token` back to continue. The prefix must be UTF-8 text.
    pub async fn fetch_page(
        &self,
        prefix: &[u8],
        limit: usize,
        page_token: Option<&str>,
    ) -> Result<FetchPage> {
        let session = self.service().await?;
        let Session::Remote(client) = session.as_ref() else {
            return Err(ValkvEdgeError::Unsupported(
                "this database has no hosted service".into(),
            ));
        };
        let (items, next_page_token) = client
            .scan(prefix, limit.clamp(1, FETCH_PAGE_MAX), page_token)
            .await?;
        let mut entries = Vec::with_capacity(items.len());
        for (key, stored, _) in items {
            let value = match self.cipher() {
                None => stored,
                Some(cipher) => cipher.open(&key, &stored)?,
            };
            entries.push((key, value));
        }
        Ok(FetchPage {
            entries,
            next_page_token,
        })
    }

    /// Stores a value, with this handle's default durability.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<Commit> {
        let mut batch = self.batch();
        batch.put(key, value);
        batch.commit().await
    }

    /// Stores a value that expires `ttl` from now, with this handle's
    /// default durability.
    ///
    /// Once it has expired, every copy reads the key as absent, by its own
    /// clock, and the database's writer deletes it with an ordinary delete
    /// that replicas and change feeds see. Writing the key again replaces the
    /// expiry: a plain [`Database::put`] keeps the value for good. The expiry
    /// time is stored beside the value, not encrypted with it.
    pub async fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: Duration) -> Result<Commit> {
        let mut batch = self.batch();
        batch.put_with_ttl(key, value, ttl);
        batch.commit().await
    }

    /// When the value of `key` expires, from the local store: `None` when it
    /// never does, or the key has no value.
    pub fn expires_at(&self, key: &[u8]) -> Result<Option<SystemTime>> {
        self.local_reads()?;
        self.held(key)?;
        let open = self.open()?;
        Ok(open
            .store
            .get_live(key)?
            .and_then(|live| live.expires_ms)
            .map(|ms| UNIX_EPOCH + Duration::from_millis(ms)))
    }

    /// Deletes values that have expired, now, as the writer, and returns
    /// how many. The library also does this on its own every few seconds
    /// for databases this process writes; a replica or a hosted copy leaves
    /// it to the writer and returns 0.
    pub async fn expire(&self) -> Result<usize> {
        let open = self.open()?;
        let session = self.session_within_limit(&open).await?;
        self.expire_with(&open, &session).await
    }

    async fn expire_with(&self, open: &Arc<Open>, session: &Arc<Session>) -> Result<usize> {
        let now = valkvedge_log::now_ms();
        match session.as_ref() {
            Session::Writer(writer) => match writer.expire(now, EXPIRE_BATCH).await {
                Ok(n) => Ok(n),
                Err(e @ ValkvEdgeError::ConditionFailed { .. }) => Err(e),
                Err(e) => {
                    self.reset_session(open, session).await;
                    Err(e)
                }
            },
            Session::Local => {
                let keys = open.store.expired(now, EXPIRE_BATCH)?;
                let mut deleted = 0;
                for key in keys {
                    let delete = Change {
                        kind: OpKind::Delete,
                        key: key.clone(),
                        value: Vec::new(),
                        expires_ms: None,
                    };
                    let unchanged = Condition { key, version: None };
                    match open
                        .store
                        .apply_local(vec![delete], &[unchanged], || open.clock.now())
                    {
                        Ok(_) => deleted += 1,
                        Err(ValkvEdgeError::ConditionFailed { .. }) => {}
                        Err(e) => return Err(e),
                    }
                }
                Ok(deleted)
            }
            Session::Remote(_) | Session::Replica => Ok(0),
        }
    }

    /// [`Database::expire`] for a database that already has its writer here,
    /// run by the background tick. It never opens a session: a database
    /// another process writes is left to that process.
    async fn expire_if_writing(&self) -> Result<usize> {
        let Some(open) = self.shared.current() else {
            return Ok(0);
        };
        // With no object store the local store is the database, and this
        // process its only writer.
        if self.kv.role().is_none() {
            return self.expire_with(&open, &Arc::new(Session::Local)).await;
        }
        let session = open.session.lock().await.clone();
        match session {
            Some(session) => self.expire_with(&open, &session).await,
            None => Ok(0),
        }
    }

    /// Removes a key, with this handle's default durability.
    pub async fn delete(&self, key: &[u8]) -> Result<Commit> {
        let mut batch = self.batch();
        batch.delete(key);
        batch.commit().await
    }

    /// Starts an atomic batch.
    pub fn batch(&self) -> Batch {
        Batch {
            db: self.clone(),
            changes: Vec::new(),
            conditions: Vec::new(),
        }
    }

    /// The writer settings for this database: the process's, with its own
    /// history window if it has one.
    fn writer_config(&self) -> WriterConfig {
        let mut config = self.kv.options.writer.clone();
        if let Some(history) = *self.shared.history.lock().expect("history lock") {
            config.history = history;
        }
        config
    }

    /// An empty local store under the root, for rebuilding a state from the
    /// history. Removed when dropped.
    fn scratch(&self) -> Result<Scratch> {
        let path = self
            .kv
            .root
            .join(SCRATCH_DIR)
            .join(ReplicaId::random().to_string());
        let store = LocalStore::open(
            &path,
            self.kv.options.cache_bytes_per_database,
            false,
            tokio::sync::broadcast::channel(1).0,
            None,
        )?;
        Ok(Scratch {
            store: Some(Arc::new(store)),
            path,
        })
    }

    /// How far back this database can be restored, or branched from.
    /// Asks the hosted service, or reads the object store.
    pub async fn history(&self) -> Result<History> {
        if self.kv.is_remote() {
            let open = self.open()?;
            let session = self.session_within_limit(&open).await?;
            if let Session::Remote(client) = session.as_ref() {
                return client.history().await;
            }
        }
        let (store, layout) = self.kv.layout(&self.shared.name).ok_or_else(|| {
            ValkvEdgeError::Unsupported("this database has no object store, so no history".into())
        })?;
        valkvedge_sync::history(&store, &layout).await
    }

    /// Puts the database back as it was at `point`, within its history
    /// ([`DatabaseBuilder::history`]): the writer rebuilds that state from
    /// the history and installs it as a new generation. Every replica then
    /// reloads the database. Writes made after the point are undone, and so
    /// are writes made while the restore runs; buffered writes still waiting
    /// on replicas apply on top when they are pushed. The restore is itself
    /// part of the history, so it can be undone the same way.
    ///
    /// Only the writer, or a hosted service, can restore. Fails with
    /// `NOT_IN_HISTORY` for a point the history does not reach.
    pub async fn restore(&self, point: Point) -> Result<Restored> {
        let open = self.open()?;
        let session = self.session_within_limit(&open).await?;
        match session.as_ref() {
            Session::Remote(client) => {
                let restored = client.restore(point).await?;
                if !self.shared.ingest() {
                    client.pull(open.store.as_ref()).await?;
                }
                Ok(restored)
            }
            Session::Writer(writer) => {
                let (store, layout) = self
                    .kv
                    .layout(&self.shared.name)
                    .expect("a writer has an object store");
                let scratch = self.scratch()?;
                let from =
                    valkvedge_sync::state_at(&store, &layout, point, scratch.store().as_ref())
                        .await?;
                match writer.restore(scratch.store().clone(), true).await {
                    Ok(pos) => Ok(Restored {
                        from,
                        lsn: pos.applied_lsn,
                    }),
                    Err(e) => {
                        self.reset_session(&open, &session).await;
                        Err(e)
                    }
                }
            }
            Session::Local | Session::Replica => Err(ValkvEdgeError::ReadOnly(
                "only the writer of a database, or a hosted service, can restore it".into(),
            )),
        }
    }

    /// Fills this database, which must be empty, with what `source` held at
    /// `point`: a branch. This process must be this database's writer, and
    /// `source` must be in an object store it can read. The branch starts
    /// with no replicas and a history of its own. Encrypted databases cannot
    /// be branched: their values are sealed to the database's name.
    pub async fn branch_from(&self, source: &Database, point: Point) -> Result<Restored> {
        let unsupported = |why: &str| Err(ValkvEdgeError::Unsupported(why.into()));
        if self.cipher().is_some() || source.cipher().is_some() {
            return unsupported(
                "an encrypted database cannot be branched: its values are sealed to its name",
            );
        }
        let Some((store, layout)) = source.kv.layout(&source.shared.name) else {
            return unsupported("a branch is made from a database in an object store");
        };
        let manifest = valkvedge_log::read_manifest(&store, &layout)
            .await?
            .ok_or_else(|| ValkvEdgeError::NotFound(source.shared.name.clone()))?
            .0;
        if manifest.encryption_check.is_some() {
            return unsupported(
                "an encrypted database cannot be branched: its values are sealed to its name",
            );
        }
        let open = self.open()?;
        let session = self.session_within_limit(&open).await?;
        let Session::Writer(writer) = session.as_ref() else {
            return Err(ValkvEdgeError::ReadOnly(
                "only the writer of a database can fill it with a branch".into(),
            ));
        };
        if writer.position().await?.applied_lsn != 0 {
            return unsupported("a branch is made into an empty database");
        }
        let scratch = self.scratch()?;
        let from =
            valkvedge_sync::state_at(&store, &layout, point, scratch.store().as_ref()).await?;
        match writer.restore(scratch.store().clone(), false).await {
            Ok(pos) => Ok(Restored {
                from,
                lsn: pos.applied_lsn,
            }),
            Err(e) => {
                self.reset_session(&open, &session).await;
                Err(e)
            }
        }
    }

    /// The database's session, created on first use and again after
    /// [`Database::reset_session`].
    async fn session(&self, open: &Arc<Open>) -> Result<Arc<Session>> {
        let mut slot = open.session.lock().await;
        if let Some(session) = slot.as_ref() {
            return Ok(session.clone());
        }
        let wait = self.kv.options.wait_for_lease;
        let session = Arc::new(self.start_session(open, wait).await?);
        *slot = Some(session.clone());
        Ok(session)
    }

    /// Forgets a session whose writer can no longer be trusted. The next
    /// operation opens a new one, which re-reads the tail of the log and so
    /// learns whether an unfinished write was stored.
    async fn reset_session(&self, open: &Arc<Open>, broken: &Arc<Session>) {
        let mut slot = open.session.lock().await;
        if matches!(slot.as_ref(), Some(current) if Arc::ptr_eq(current, broken)) {
            *slot = None;
        }
    }

    async fn start_session(&self, open: &Arc<Open>, wait_for_lease: bool) -> Result<Session> {
        {
            {
                if let Backend::Remote { url, auth } = &self.kv.backend {
                    if self.shared.name.contains('/') {
                        return Err(ValkvEdgeError::Unsupported(
                            "namespaces are not available with a hosted service".into(),
                        ));
                    }
                    let client = remote::Client::new(
                        url,
                        auth.clone(),
                        &self.shared.name,
                        open.clock.replica(),
                        self.kv.options.sync_write_timeout,
                        self.shared.prefixes(),
                    )?;
                    return Ok(Session::Remote(Box::new(client)));
                }
                let Some((store, layout)) = self.kv.layout(&self.shared.name) else {
                    return Ok(Session::Local);
                };
                // Unset while a keyed open is still finding its data key: it
                // checks the key itself once it has.
                let settled = self.shared.cipher.get().is_some();
                let cipher = self.cipher().map(Arc::as_ref);
                let session = match self.kv.role() {
                    Some(Role::Replica) => {
                        let (manifest, _) = valkvedge_log::read_manifest(&store, &layout)
                            .await?
                            .ok_or_else(|| ValkvEdgeError::NotFound(self.shared.name.clone()))?;
                        if settled {
                            crypto::reconcile(cipher, manifest.encryption_check.as_deref(), false)?;
                        }
                        Session::Replica
                    }
                    _ => {
                        let node = self.kv.node(&store).await?;
                        let writer = Writer::open(
                            store,
                            layout,
                            node,
                            open.store.clone(),
                            open.clock.clone(),
                            self.writer_config(),
                            wait_for_lease,
                        )
                        .await?
                        .with_counters(self.kv.commits.clone());
                        if settled {
                            let empty = writer.position().await?.applied_lsn == 0;
                            let stored = writer.encryption_check().await?;
                            if let Some(check) =
                                crypto::reconcile(cipher, stored.as_deref(), empty)?
                            {
                                writer.set_encryption_check(Some(check)).await?;
                            }
                        }
                        Session::Writer(Box::new(writer))
                    }
                };
                // Remember the verified key locally so a wrong key is caught
                // offline too.
                if let Some(cipher) = cipher {
                    open.store.set_encryption_check(&cipher.make_check()?)?;
                }
                Ok(session)
            }
        }
    }

    /// The session, waiting at most `sync_write_timeout` for it.
    ///
    /// Taking ownership can legitimately take longer than one call is willing
    /// to wait (watching a dead owner's lease expire). So it runs detached:
    /// this call gives up at the limit, the attempt carries on, and a later
    /// call finds the session ready.
    async fn session_within_limit(&self, open: &Arc<Open>) -> Result<Arc<Session>> {
        let limit = self.kv.options.sync_write_timeout;
        let cap = self.kv.options.lease.lease_ttl + limit * 2;
        let (db, held) = (self.clone(), open.clone());
        let opening = tokio::spawn(async move {
            match tokio::time::timeout(cap, db.session(&held)).await {
                Ok(result) => result,
                Err(_) => Err(ValkvEdgeError::Unavailable(
                    "could not open the database in the object store".into(),
                )),
            }
        });
        match tokio::time::timeout(limit, opening).await {
            Ok(Ok(result)) => result,
            Ok(Err(join)) => Err(ValkvEdgeError::local(join)),
            Err(_) => Err(ValkvEdgeError::Unavailable(format!(
                "could not reach the object store within {limit:?}"
            ))),
        }
    }

    /// A sync write through the writer, bounded by `sync_write_timeout`.
    /// Returns its LSN and the timestamp of each change.
    async fn commit_sync(
        &self,
        open: &Arc<Open>,
        changes: Vec<Change>,
        conditions: &[Condition],
    ) -> Result<(Lsn, Vec<Hlc>)> {
        let limit = self.kv.options.sync_write_timeout;
        let started = Instant::now();
        let unavailable = |what: &str| {
            ValkvEdgeError::Unavailable(format!(
                "{what} within {limit:?}; the write may or may not have been stored"
            ))
        };
        let session = self.session_within_limit(open).await?;
        if let Session::Remote(client) = session.as_ref() {
            // The service stores the write; the local copy then catches up to
            // it, so a read right after this returns sees it.
            let ingest = self.shared.ingest();
            let (lsn, stamps) = match client.commit(&changes, conditions).await {
                Ok(done) => done,
                Err(e @ ValkvEdgeError::ConditionFailed { .. }) => {
                    // The local copy was behind what the condition was
                    // checked against. Catch up, so the caller's next read
                    // sees the value that won.
                    if !ingest {
                        if let Err(pull) = client.pull(open.store.as_ref()).await {
                            tracing::warn!(database = %self.shared.name, error = %pull, "could not catch up after a failed condition");
                        }
                    }
                    return Err(e);
                }
                Err(e) => return Err(e),
            };
            // Ingesting, there is no local copy to bring up to the write.
            while !ingest && open.store.position()?.applied_lsn < lsn {
                let reached = client.pull(open.store.as_ref()).await?;
                if reached < lsn {
                    return Err(unavailable(
                        "the write was stored but could not be read back",
                    ));
                }
            }
            return Ok((lsn, stamps));
        }
        let Session::Writer(writer) = session.as_ref() else {
            return Err(ValkvEdgeError::ReadOnly(
                "this database has no writer".into(),
            ));
        };

        // The commit itself is cancelled at the limit. A cancelled or failed
        // commit leaves the writer in an unknown position, so the session is
        // dropped and the next write starts from what the log really holds.
        let remaining = limit.saturating_sub(started.elapsed());
        let committing = writer.commit_if(changes, conditions);
        let result = match tokio::time::timeout(remaining, committing).await {
            Ok(result) => result,
            Err(_) => Err(unavailable("the object store did not confirm the write")),
        };
        match result {
            Ok(done) => {
                self.compact_if_due(open, &session).await;
                Ok(done)
            }
            // Checked before anything was written: the writer is fine.
            Err(e @ ValkvEdgeError::ConditionFailed { .. }) => Err(e),
            Err(e) => {
                self.reset_session(open, &session).await;
                Err(e)
            }
        }
    }

    async fn write(
        &self,
        mut changes: Vec<Change>,
        conditions: Vec<Condition>,
        durability: Durability,
    ) -> Result<Commit> {
        let mut total = 0;
        let keys = changes
            .iter()
            .map(|c| &c.key)
            .chain(conditions.iter().map(|c| &c.key));
        for key in keys {
            if key.is_empty() || key.len() > MAX_KEY_BYTES {
                return Err(ValkvEdgeError::KeyTooLarge {
                    limit: MAX_KEY_BYTES,
                });
            }
            // A partial copy would never see what became of a write it did
            // not hold.
            self.held(key)?;
        }
        for change in &changes {
            if change.value.len() > MAX_VALUE_BYTES {
                return Err(ValkvEdgeError::ValueTooLarge {
                    limit: MAX_VALUE_BYTES,
                });
            }
            total += change.key.len() + change.value.len();
        }
        if total > MAX_BATCH_BYTES {
            return Err(ValkvEdgeError::BatchTooLarge {
                limit: MAX_BATCH_BYTES,
            });
        }
        if changes.is_empty() {
            return Ok(Commit {
                lsn: None,
                outbox_seq: None,
                versions: Vec::new(),
            });
        }
        if let Some(cipher) = self.cipher() {
            for change in changes.iter_mut().filter(|c| c.kind == OpKind::Put) {
                change.value = cipher.seal(&change.key, &change.value)?;
            }
        }
        let open = self.open()?;

        // From the caller's side a hosted database behaves like a writer's:
        // sync writes are confirmed remotely, buffered writes go to the outbox.
        let role = if self.kv.is_remote() {
            Some(Role::Writer)
        } else {
            self.kv.role()
        };
        match (role, durability) {
            (Some(Role::Replica), _) => Err(ValkvEdgeError::ReadOnly(
                "this process follows the database as a replica".into(),
            )),
            (Some(Role::Writer), Durability::Buffered) if !conditions.is_empty() => {
                Err(ValkvEdgeError::Unsupported(
                    "conditional writes must be sync writes: a buffered write cannot know what it will be checked against"
                        .into(),
                ))
            }
            (Some(Role::Writer), Durability::Buffered) => {
                // No network and no session: this path must work offline.
                // A write made now is later than whatever this replica already
                // holds for the key, even if that was stamped by another
                // process or by a clock slightly ahead of this one.
                for change in &changes {
                    if let Some(current) = open.store.version_of(&change.key)? {
                        open.clock.observe(&current);
                    }
                }
                let ops: Vec<Op> = changes
                    .into_iter()
                    .map(|c| Op {
                        expires_ms: c.expires_ms.filter(|_| c.kind == OpKind::Put),
                        kind: c.kind,
                        key: c.key,
                        value: c.value,
                        hlc: open.clock.now(),
                    })
                    .collect();
                let versions = ops.iter().map(|op| Version(op.hlc)).collect();
                let seq = open.store.buffer(&ops, self.kv.options.outbox_max_bytes)?;
                self.shared.has_outbox.store(true, Ordering::Relaxed);
                if open.store.outbox_stat()?.bytes >= self.kv.options.push_bytes {
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        let db = self.clone();
                        handle.spawn(async move {
                            if let Err(e) = db.flush().await {
                                tracing::warn!(error = %e, "outbox push failed");
                            }
                        });
                    }
                }
                Ok(Commit {
                    lsn: None,
                    outbox_seq: Some(seq),
                    versions,
                })
            }
            (None, _) => {
                // No object store: the local store is the database.
                let (lsn, stamps) =
                    open.store
                        .apply_local(changes, &conditions, || open.clock.now())?;
                Ok(Commit {
                    lsn: Some(lsn),
                    outbox_seq: None,
                    versions: stamps.into_iter().map(Version).collect(),
                })
            }
            (Some(Role::Writer), Durability::Sync) => {
                let (lsn, stamps) = self.commit_sync(&open, changes, &conditions).await?;
                Ok(Commit {
                    lsn: Some(lsn),
                    outbox_seq: None,
                    versions: stamps.into_iter().map(Version).collect(),
                })
            }
        }
    }

    async fn compact_if_due(&self, open: &Arc<Open>, session: &Arc<Session>) {
        let Session::Writer(writer) = session.as_ref() else {
            return;
        };
        if writer.should_compact().await {
            if let Err(e) = writer.compact().await {
                tracing::warn!(database = %self.shared.name, error = %e, "compaction failed");
                self.reset_session(open, session).await;
                return;
            }
            // Moving the history's base forward can mean replaying a long
            // log: it runs on its own, and writes go on meanwhile.
            if writer.history_due().await && !self.shared.maintaining.swap(true, Ordering::SeqCst) {
                let (db, session) = (self.clone(), session.clone());
                tokio::spawn(async move {
                    if let Session::Writer(writer) = session.as_ref() {
                        let done = match db.scratch() {
                            Ok(scratch) => writer.maintain_history(scratch.store().clone()).await,
                            Err(e) => Err(e),
                        };
                        if let Err(e) = done {
                            tracing::warn!(database = %db.shared.name, error = %e, "could not move the history forward");
                        }
                    }
                    db.shared.maintaining.store(false, Ordering::SeqCst);
                });
            }
        }
    }

    /// Pushes the outbox now. Returns the number of batches settled.
    ///
    /// Operations that lost a conflict or were refused are reported through
    /// [`Database::take_events`].
    pub async fn flush(&self) -> Result<u64> {
        if self.kv.role() != Some(Role::Writer) && !self.kv.is_remote() {
            return Ok(0);
        }
        let _one_at_a_time = self.shared.flush_lock.lock().await;
        let open = self.open()?;
        if open.store.outbox_stat()?.entries == 0 {
            self.shared.has_outbox.store(false, Ordering::Relaxed);
            return Ok(0);
        }
        let session = self.session_within_limit(&open).await?;
        let mut settled = 0;
        loop {
            let entries = open.store.outbox_peek(self.kv.options.push_bytes)?;
            if entries.is_empty() {
                break;
            }
            let limit = self.kv.options.sync_write_timeout;
            let pushed = match session.as_ref() {
                Session::Writer(writer) => {
                    let pushing = writer.push(open.clock.replica(), entries.clone());
                    tokio::time::timeout(limit, pushing)
                        .await
                        .unwrap_or_else(|_| {
                            Err(ValkvEdgeError::Unavailable(format!(
                                "the object store did not confirm the push within {limit:?}"
                            )))
                        })
                }
                Session::Remote(client) => {
                    match client.push(open.clock.replica(), &entries).await {
                        // Ingesting, there is no local copy: once the service
                        // has the entries, they are simply removed.
                        Ok(report) if self.shared.ingest() => Ok(report),
                        // The winners must be in the local copy before the
                        // pending entries that hid them are removed.
                        Ok(report) => client.pull(open.store.as_ref()).await.map(|_| report),
                        Err(e) => Err(e),
                    }
                }
                Session::Local | Session::Replica => return Ok(settled),
            };
            let report = match pushed {
                Ok(report) => report,
                Err(e) => {
                    // The entries stay in the outbox. A retried push is safe:
                    // the writer skips whatever was already applied.
                    self.reset_session(&open, &session).await;
                    return Err(e);
                }
            };
            {
                let mut events = self.shared.events.lock().expect("events lock");
                for (entry, (_, outcomes)) in entries.iter().zip(&report.entries) {
                    for (op, outcome) in entry.ops.iter().zip(outcomes) {
                        let outcome = match outcome {
                            OpOutcome::Superseded => WriteOutcome::Superseded,
                            OpOutcome::Rejected(reason) => WriteOutcome::Rejected(*reason),
                            OpOutcome::Applied | OpOutcome::Duplicate => continue,
                        };
                        if events.len() == MAX_EVENTS {
                            events.pop_front();
                        }
                        events.push_back(WriteEvent {
                            outbox_seq: entry.seq,
                            key: op.key.clone(),
                            outcome,
                        });
                    }
                }
            }
            settled += entries.len() as u64;
            let stat = open.store.outbox_settle(&entries)?;
            self.shared
                .has_outbox
                .store(stat.entries > 0, Ordering::Relaxed);
        }
        self.compact_if_due(&open, &session).await;
        Ok(settled)
    }

    /// Brings the local copy up to date and returns its LSN.
    ///
    /// A writer pushes its outbox. A replica pulls new frames, or the current
    /// snapshot if it is too far behind. A database opened for ingestion only
    /// uploads its buffered writes, and returns 0: it keeps no copy.
    pub async fn sync(&self) -> Result<Lsn> {
        let open = self.open()?;
        let session = self.session_within_limit(&open).await?;
        match session.as_ref() {
            Session::Local => {}
            Session::Writer(_) => {
                self.flush().await?;
            }
            Session::Remote(client) => {
                self.flush().await?;
                if !self.shared.ingest() {
                    client.pull(open.store.as_ref()).await?;
                }
            }
            Session::Replica => {
                let (store, layout) = self
                    .kv
                    .layout(&self.shared.name)
                    .expect("replica has a store");
                valkvedge_sync::pull(&store, &layout, open.store.as_ref()).await?;
            }
        }
        Ok(open.store.position()?.applied_lsn)
    }

    /// One step of following a hosted database: asks for what follows the
    /// local copy, waiting up to `follow_wait` for it, and applies it.
    async fn follow_step(&self) -> Result<Follow> {
        let Some(open) = self.shared.current() else {
            return Ok(Follow::Closed);
        };
        let session = self.session_within_limit(&open).await?;
        let Session::Remote(client) = session.as_ref() else {
            return Ok(Follow::Closed);
        };
        let wait = self.kv.options.follow_wait;
        let applied = open.store.position()?.applied_lsn;
        if wait.is_zero() || open.store.needs_bootstrap()? {
            let reached = client.pull(open.store.as_ref()).await?;
            return Ok(if reached > applied {
                Follow::Changed
            } else {
                Follow::Polled
            });
        }
        // While waiting, only the session is held: a database nobody uses
        // still closes when it has been idle long enough.
        drop(open);
        let asked = Instant::now();
        let page = client.log(applied, Some(wait)).await?;
        let Some(open) = self.shared.current() else {
            return Ok(Follow::Closed);
        };
        match page {
            remote::LogPage::SnapshotRequired => {
                client.pull(open.store.as_ref()).await?;
                Ok(Follow::Changed)
            }
            remote::LogPage::Frames { frames, waited, .. } if frames.is_empty() => {
                // A service that says it waited but answered at once would
                // otherwise be asked again and again without pause.
                let held = asked.elapsed() >= Duration::from_millis(500);
                Ok(if waited && held {
                    Follow::Waited
                } else {
                    Follow::Polled
                })
            }
            remote::LogPage::Frames {
                generation, frames, ..
            } => {
                // A write through this replica may have pulled some of these
                // in the meantime.
                let now = open.store.position()?.applied_lsn;
                let fresh: Vec<Frame> = frames.into_iter().filter(|f| f.lsn > now).collect();
                remote::apply_frames(&open.store, now, generation, &fresh)?;
                Ok(Follow::Changed)
            }
        }
    }

    /// The newest LSN in the object store, without applying anything.
    /// `None` when this client has no object store.
    pub async fn remote_lsn(&self) -> Result<Option<Lsn>> {
        if self.kv.is_remote() {
            let open = self.open()?;
            let session = self.session_within_limit(&open).await?;
            if let Session::Remote(client) = session.as_ref() {
                return client.head().await.map(Some);
            }
        }
        match self.kv.layout(&self.shared.name) {
            None => Ok(None),
            Some((store, layout)) => {
                Ok(Some(valkvedge_sync::remote_head(&store, &layout).await?.1))
            }
        }
    }

    /// Who currently holds this database's lease, as recorded in the object
    /// store: the owner's node id and the address it advertised. `None` when
    /// nobody has ever owned it, when the last owner released it, or when
    /// this client has no object store.
    pub async fn owner(&self) -> Result<Option<Owner>> {
        let Some((store, layout)) = self.kv.layout(&self.shared.name) else {
            return Ok(None);
        };
        Ok(valkvedge_log::lease::read_lease(&store, &layout)
            .await?
            .filter(|lease| lease.state == valkvedge_log::LeaseState::Held)
            .map(|lease| Owner {
                node_id: lease.node_id,
                node_addr: lease.node_addr,
            }))
    }

    /// Becomes the writer of this database, waiting out a dead owner if
    /// there is one.
    ///
    /// When another process holds the lease, this watches that process's
    /// heartbeat for one lease TTL. If the heartbeat never changes, the owner
    /// is dead and this process takes the database over. If the owner is
    /// alive, this fails with `LEASE_HELD`. Unlike a write, the wait is not
    /// cut short by `sync_write_timeout`.
    ///
    /// Use it when `wait_for_lease` is off and something outside the library
    /// suggests the owner is gone, for example it cannot be reached.
    pub async fn take_over(&self) -> Result<()> {
        if self.kv.role() != Some(Role::Writer) {
            return Err(ValkvEdgeError::ReadOnly(
                "only a writer with an object store can take a database over".into(),
            ));
        }
        let open = self.open()?;
        let mut slot = open.session.lock().await;
        if slot.is_none() {
            *slot = Some(Arc::new(self.start_session(&open, true).await?));
        }
        Ok(())
    }

    /// Where this database lives in the object store, or `None` when this
    /// client has no object store. See `docs/format.md` for what is under it.
    pub fn object_prefix(&self) -> Option<String> {
        self.kv
            .layout(&self.shared.name)
            .map(|(_, layout)| layout.prefix().to_string())
    }

    /// Applies outbox entries that another replica made, as the writer.
    ///
    /// This is the receiving end of a push: each entry becomes one frame
    /// holding the operations that won, by last write wins per key. Entries at
    /// or below the replica's watermark are reported as duplicates and not
    /// applied again, so the sender can retry safely.
    ///
    /// Use it when this process is the writer and other replicas hand it their
    /// buffered writes over a channel of your own.
    pub async fn accept_push(
        &self,
        replica: ReplicaId,
        entries: Vec<OutboxEntry>,
    ) -> Result<PushReport> {
        let open = self.open()?;
        let session = self.session_within_limit(&open).await?;
        let Session::Writer(writer) = session.as_ref() else {
            return Err(ValkvEdgeError::ReadOnly(
                "only the writer of a database with an object store accepts pushes".into(),
            ));
        };
        let limit = self.kv.options.sync_write_timeout;
        let pushed = tokio::time::timeout(limit, writer.push(replica, entries))
            .await
            .unwrap_or_else(|_| {
                Err(ValkvEdgeError::Unavailable(format!(
                    "the object store did not confirm the push within {limit:?}"
                )))
            });
        match pushed {
            Ok(report) => {
                self.compact_if_due(&open, &session).await;
                Ok(report)
            }
            Err(e) => {
                self.reset_session(&open, &session).await;
                Err(e)
            }
        }
    }

    /// Writes a snapshot and starts a new generation now. Writer only.
    pub async fn compact(&self) -> Result<()> {
        let open = self.open()?;
        let session = self.session_within_limit(&open).await?;
        match session.as_ref() {
            Session::Writer(writer) => match writer.compact().await {
                Ok(_) => Ok(()),
                Err(e) => {
                    self.reset_session(&open, &session).await;
                    Err(e)
                }
            },
            _ => Err(ValkvEdgeError::ReadOnly(
                "only the writer of a database with an object store can compact".into(),
            )),
        }
    }

    /// Local state: applied LSN, generation, outbox depth, disk usage.
    pub fn status(&self) -> Result<Status> {
        let open = self.open()?;
        let pos = open.store.position()?;
        let outbox = open.store.outbox_stat()?;
        Ok(Status {
            applied_lsn: pos.applied_lsn,
            generation: pos.generation,
            outbox_entries: outbox.entries,
            outbox_bytes: outbox.bytes,
            disk_bytes: open.store.disk_bytes(),
        })
    }

    /// Returns and clears the buffered operations that did not take effect
    /// since the last call. At most the latest 1,024 are kept.
    pub fn take_events(&self) -> Vec<WriteEvent> {
        self.shared
            .events
            .lock()
            .expect("events lock")
            .drain(..)
            .collect()
    }

    /// True while the database's local store is open.
    pub fn is_open(&self) -> bool {
        self.shared.is_open()
    }
}

/// An atomic group of writes. All of it is applied, or none of it.
///
/// A batch can also carry conditions ([`Batch::if_version`],
/// [`Batch::if_absent`]): it is then applied only if every one holds when
/// the writer orders it, and otherwise fails with `CONDITION_FAILED` having
/// written nothing. Conditions need a sync write; with buffered durability
/// the commit fails with `UNSUPPORTED`.
pub struct Batch {
    db: Database,
    changes: Vec<Change>,
    conditions: Vec<Condition>,
}

impl Batch {
    /// Applies the batch only if `key`'s value is still at `version`: nobody
    /// has written or deleted it since that version was read.
    pub fn if_version(&mut self, key: &[u8], version: &Version) -> &mut Self {
        self.conditions.push(Condition {
            key: key.to_vec(),
            version: Some(version.0),
        });
        self
    }

    /// Applies the batch only if `key` has no value: it was never written,
    /// or its last write was a delete.
    pub fn if_absent(&mut self, key: &[u8]) -> &mut Self {
        self.conditions.push(Condition {
            key: key.to_vec(),
            version: None,
        });
        self
    }

    /// Adds a put.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> &mut Self {
        self.changes.push(Change {
            kind: OpKind::Put,
            key: key.to_vec(),
            value: value.to_vec(),
            expires_ms: None,
        });
        self
    }

    /// Adds a put whose value expires `ttl` from now. See
    /// [`Database::put_with_ttl`].
    pub fn put_with_ttl(&mut self, key: &[u8], value: &[u8], ttl: Duration) -> &mut Self {
        self.put_expiring(key, value, SystemTime::now() + ttl)
    }

    /// Adds a put whose value expires at `at`. A time already past stores a
    /// value that reads as absent at once.
    pub fn put_expiring(&mut self, key: &[u8], value: &[u8], at: SystemTime) -> &mut Self {
        let ms = at
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        self.changes.push(Change {
            kind: OpKind::Put,
            key: key.to_vec(),
            value: value.to_vec(),
            expires_ms: Some(ms),
        });
        self
    }

    /// Adds a delete.
    pub fn delete(&mut self, key: &[u8]) -> &mut Self {
        self.changes.push(Change {
            kind: OpKind::Delete,
            key: key.to_vec(),
            value: Vec::new(),
            expires_ms: None,
        });
        self
    }

    /// Number of writes in the batch.
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// True when the batch has no writes.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Commits with the handle's default durability.
    pub async fn commit(self) -> Result<Commit> {
        let durability = self.db.durability;
        self.commit_with(durability).await
    }

    /// Commits with the given durability.
    pub async fn commit_with(self, durability: Durability) -> Result<Commit> {
        self.db
            .write(self.changes, self.conditions, durability)
            .await
    }
}
