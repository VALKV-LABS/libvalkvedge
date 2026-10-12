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
//!   It can be lost if the device is lost before the upload, and it can lose a
//!   conflict to a newer write to the same key (see [`Database::take_events`]).

#![forbid(unsafe_code)]

mod crypto;
mod local;

use crypto::Cipher;
use local::LocalStore;
use std::collections::{HashMap, VecDeque};
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use valkvedge_log::{HlcClock, Layout, Node, Store};
use valkvedge_sync::{Change, StateStore, Writer};

pub use local::Iter;
pub use valkvedge_log::{DeviceId, Hlc, Op};
pub use valkvedge_log::{Error as ValkvEdgeError, LeaseConfig, Lsn, OpKind, RejectReason, Result};
pub use valkvedge_sync::{OpOutcome, OutboxEntry, PushReport, WriterConfig};

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
    /// How often a replica pulls.
    pub pull_interval: Duration,
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
            wait_for_lease: true,
            lease: LeaseConfig::default(),
            writer: WriterConfig::default(),
            node_id: None,
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
}

/// Configures and creates a [`ValkvEdge`].
pub struct Builder {
    root: PathBuf,
    options: Options,
    object: Option<(Store, String)>,
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

    /// Creates the client. Does not touch the network.
    pub fn build(self) -> Result<ValkvEdge> {
        std::fs::create_dir_all(&self.root).map_err(ValkvEdgeError::local)?;
        let backend = match self.object {
            None => Backend::Local,
            Some((store, prefix)) => Backend::Object {
                store,
                prefix: prefix.trim_matches('/').to_string(),
                role: self.role,
            },
        };
        let inner = Arc::new(Inner {
            root: self.root,
            options: self.options,
            backend,
            dbs: Mutex::new(HashMap::new()),
            open_lock: Mutex::new(()),
            open_count: AtomicUsize::new(0),
            evictions: AtomicU64::new(0),
            started: Instant::now(),
            node: tokio::sync::OnceCell::new(),
        });
        if inner.options.background {
            if let (Backend::Object { role, .. }, Ok(handle)) =
                (&inner.backend, tokio::runtime::Handle::try_current())
            {
                let tick = match role {
                    Role::Writer => inner.options.push_interval,
                    Role::Replica => inner.options.pull_interval,
                };
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
    flush_lock: tokio::sync::Mutex<()>,
    events: Mutex<VecDeque<WriteEvent>>,
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
}

impl Inner {
    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn layout(&self, name: &str) -> Option<(Store, Layout)> {
        match &self.backend {
            Backend::Local => None,
            Backend::Object { store, prefix, .. } => {
                Some((store.clone(), Layout::new(format!("{prefix}/{name}"))))
            }
        }
    }

    fn role(&self) -> Option<Role> {
        match &self.backend {
            Backend::Local => None,
            Backend::Object { role, .. } => Some(*role),
        }
    }

    async fn node(&self, store: &Store) -> Result<Node> {
        let node =
            self.node
                .get_or_try_init(|| async {
                    let id =
                        self.options.node_id.clone().unwrap_or_else(|| {
                            format!("direct-{}", valkvedge_log::DeviceId::random())
                        });
                    Node::start(store.clone(), id, "", self.options.lease.clone()).await
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
            clock: Arc::new(HlcClock::new(store.device_id()?)),
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
            let result = match self.role() {
                Some(Role::Writer) if shared.has_outbox.load(Ordering::Relaxed) => {
                    db.flush().await.map(|_| ())
                }
                Some(Role::Replica) if shared.is_open() => db.sync().await.map(|_| ()),
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

impl Shared {
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
        }
    }
}

/// Options for opening one database.
pub struct DatabaseBuilder {
    kv: Arc<Inner>,
    name: String,
    durability: Durability,
    key: Option<[u8; 32]>,
}

impl DatabaseBuilder {
    /// Sets the durability used by writes that do not name one.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Encrypts values with `key` before they are stored anywhere.
    ///
    /// Keys are not encrypted: their bytes, value sizes and timestamps remain
    /// visible to anyone who can read the local files or the object store.
    /// The key is never written to disk. If it is lost, the data cannot be
    /// recovered.
    pub fn encryption_key(mut self, key: [u8; 32]) -> Self {
        self.key = Some(key);
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
                    flush_lock: tokio::sync::Mutex::new(()),
                    events: Mutex::new(VecDeque::new()),
                })
            })
            .clone();

        let wanted = self.key.map(|k| Arc::new(Cipher::new(&k, &self.name)));
        let current = shared.cipher.get_or_init(|| wanted.clone());
        let same = match (current, &wanted) {
            (None, None) => true,
            (Some(a), Some(b)) => a.same_key(b),
            _ => false,
        };
        if !same {
            return Err(ValkvEdgeError::InvalidEncryptionKey(
                "this database is already open with a different key".into(),
            ));
        }

        let db = Database {
            kv: self.kv,
            shared,
            durability: self.durability,
            touch: true,
        };
        let open = db.open()?;
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

/// What a write returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit {
    /// The frame's position in the log, for writes that reached it.
    pub lsn: Option<Lsn>,
    /// The outbox sequence number, for buffered writes.
    pub outbox_seq: Option<u64>,
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

    /// The database's name.
    pub fn name(&self) -> &str {
        &self.shared.name
    }

    /// Reads a key from the local store. Never touches the network.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let open = self.open()?;
        match (open.store.get_stored(key)?, self.cipher()) {
            (None, _) => Ok(None),
            (Some(stored), None) => Ok(Some(stored)),
            (Some(stored), Some(cipher)) => cipher.open(key, &stored).map(Some),
        }
    }

    /// Iterates keys in `range`, in order, from the local store.
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Result<Iter> {
        let own = |b: Bound<&K>| match b {
            Bound::Included(k) => Bound::Included(k.as_ref().to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref().to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let open = self.open()?;
        let iters = open
            .store
            .scan_range((own(range.start_bound()), own(range.end_bound())));
        Ok(Iter::new(iters, self.cipher().cloned(), open))
    }

    /// Iterates keys starting with `prefix`, in order, from the local store.
    pub fn prefix(&self, prefix: &[u8]) -> Result<Iter> {
        let open = self.open()?;
        let iters = open.store.scan_prefix(prefix);
        Ok(Iter::new(iters, self.cipher().cloned(), open))
    }

    /// Stores a value, with this handle's default durability.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<Commit> {
        let mut batch = self.batch();
        batch.put(key, value);
        batch.commit().await
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
        }
    }

    /// The database's session, created on first use and again after
    /// [`Database::reset_session`].
    async fn session(&self, open: &Arc<Open>) -> Result<Arc<Session>> {
        let mut slot = open.session.lock().await;
        if let Some(session) = slot.as_ref() {
            return Ok(session.clone());
        }
        let session = Arc::new(self.start_session(open).await?);
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

    async fn start_session(&self, open: &Arc<Open>) -> Result<Session> {
        {
            {
                let Some((store, layout)) = self.kv.layout(&self.shared.name) else {
                    return Ok(Session::Local);
                };
                let cipher = self.cipher().map(Arc::as_ref);
                let session = match self.kv.role() {
                    Some(Role::Replica) => {
                        let (manifest, _) = valkvedge_log::read_manifest(&store, &layout)
                            .await?
                            .ok_or_else(|| ValkvEdgeError::NotFound(self.shared.name.clone()))?;
                        crypto::reconcile(cipher, manifest.encryption_check.as_deref(), false)?;
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
                            self.kv.options.writer.clone(),
                            self.kv.options.wait_for_lease,
                        )
                        .await?;
                        let empty = writer.position().await?.applied_lsn == 0;
                        let stored = writer.encryption_check().await?;
                        if let Some(check) = crypto::reconcile(cipher, stored.as_deref(), empty)? {
                            writer.set_encryption_check(Some(check)).await?;
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
    async fn commit_sync(&self, open: &Arc<Open>, changes: Vec<Change>) -> Result<Lsn> {
        let limit = self.kv.options.sync_write_timeout;
        let started = Instant::now();
        let unavailable = |what: &str| {
            ValkvEdgeError::Unavailable(format!(
                "{what} within {limit:?}; the write may or may not have been stored"
            ))
        };
        let session = self.session_within_limit(open).await?;
        let Session::Writer(writer) = session.as_ref() else {
            return Err(ValkvEdgeError::ReadOnly(
                "this database has no writer".into(),
            ));
        };

        // The commit itself is cancelled at the limit. A cancelled or failed
        // commit leaves the writer in an unknown position, so the session is
        // dropped and the next write starts from what the log really holds.
        let remaining = limit.saturating_sub(started.elapsed());
        let result = match tokio::time::timeout(remaining, writer.commit(changes)).await {
            Ok(result) => result,
            Err(_) => Err(unavailable("the object store did not confirm the write")),
        };
        match result {
            Ok(lsn) => {
                self.compact_if_due(open, &session).await;
                Ok(lsn)
            }
            Err(e) => {
                self.reset_session(open, &session).await;
                Err(e)
            }
        }
    }

    async fn write(&self, mut changes: Vec<Change>, durability: Durability) -> Result<Commit> {
        let mut total = 0;
        for change in &changes {
            if change.key.is_empty() || change.key.len() > MAX_KEY_BYTES {
                return Err(ValkvEdgeError::KeyTooLarge {
                    limit: MAX_KEY_BYTES,
                });
            }
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
            });
        }
        if let Some(cipher) = self.cipher() {
            for change in changes.iter_mut().filter(|c| c.kind == OpKind::Put) {
                change.value = cipher.seal(&change.key, &change.value)?;
            }
        }
        let open = self.open()?;

        match (self.kv.role(), durability) {
            (Some(Role::Replica), _) => Err(ValkvEdgeError::ReadOnly(
                "this process follows the database as a replica".into(),
            )),
            (Some(Role::Writer), Durability::Buffered) => {
                // No network and no session: this path must work offline.
                // A write made now is later than whatever this device already
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
                        kind: c.kind,
                        key: c.key,
                        value: c.value,
                        hlc: open.clock.now(),
                    })
                    .collect();
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
                })
            }
            (None, _) => {
                // No object store: the local store is the database.
                let lsn = open.store.apply_local(changes, || open.clock.now())?;
                Ok(Commit {
                    lsn: Some(lsn),
                    outbox_seq: None,
                })
            }
            (Some(Role::Writer), Durability::Sync) => {
                let lsn = self.commit_sync(&open, changes).await?;
                Ok(Commit {
                    lsn: Some(lsn),
                    outbox_seq: None,
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
            }
        }
    }

    /// Pushes the outbox now. Returns the number of batches settled.
    ///
    /// Operations that lost a conflict or were refused are reported through
    /// [`Database::take_events`].
    pub async fn flush(&self) -> Result<u64> {
        if self.kv.role() != Some(Role::Writer) {
            return Ok(0);
        }
        let _one_at_a_time = self.shared.flush_lock.lock().await;
        let open = self.open()?;
        if open.store.outbox_stat()?.entries == 0 {
            self.shared.has_outbox.store(false, Ordering::Relaxed);
            return Ok(0);
        }
        let session = self.session_within_limit(&open).await?;
        let Session::Writer(writer) = session.as_ref() else {
            return Ok(0);
        };
        let mut settled = 0;
        loop {
            let entries = open.store.outbox_peek(self.kv.options.push_bytes)?;
            if entries.is_empty() {
                break;
            }
            let limit = self.kv.options.sync_write_timeout;
            let pushing = writer.push(open.clock.device(), entries.clone());
            let pushed = tokio::time::timeout(limit, pushing)
                .await
                .unwrap_or_else(|_| {
                    Err(ValkvEdgeError::Unavailable(format!(
                        "the object store did not confirm the push within {limit:?}"
                    )))
                });
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
    /// snapshot if it is too far behind.
    pub async fn sync(&self) -> Result<Lsn> {
        let open = self.open()?;
        let session = self.session_within_limit(&open).await?;
        match session.as_ref() {
            Session::Local => {}
            Session::Writer(_) => {
                self.flush().await?;
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

    /// The newest LSN in the object store, without applying anything.
    /// `None` when this client has no object store.
    pub async fn remote_lsn(&self) -> Result<Option<Lsn>> {
        match self.kv.layout(&self.shared.name) {
            None => Ok(None),
            Some((store, layout)) => {
                Ok(Some(valkvedge_sync::remote_head(&store, &layout).await?.1))
            }
        }
    }

    /// Where this database lives in the object store, or `None` when this
    /// client has no object store. See `docs/format.md` for what is under it.
    pub fn object_prefix(&self) -> Option<String> {
        self.kv
            .layout(&self.shared.name)
            .map(|(_, layout)| layout.prefix().to_string())
    }

    /// Applies outbox entries that another device made, as the writer.
    ///
    /// This is the receiving end of a push: each entry becomes one frame
    /// holding the operations that won, by last write wins per key. Entries at
    /// or below the device's watermark are reported as duplicates and not
    /// applied again, so the sender can retry safely.
    ///
    /// Use it when this process is the writer and other devices hand it their
    /// buffered writes over a channel of your own.
    pub async fn accept_push(
        &self,
        device: DeviceId,
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
        let pushed = tokio::time::timeout(limit, writer.push(device, entries))
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
pub struct Batch {
    db: Database,
    changes: Vec<Change>,
}

impl Batch {
    /// Adds a put.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> &mut Self {
        self.changes.push(Change {
            kind: OpKind::Put,
            key: key.to_vec(),
            value: value.to_vec(),
        });
        self
    }

    /// Adds a delete.
    pub fn delete(&mut self, key: &[u8]) -> &mut Self {
        self.changes.push(Change {
            kind: OpKind::Delete,
            key: key.to_vec(),
            value: Vec::new(),
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
        self.db.write(self.changes, durability).await
    }
}
