//! What happens when the object store stops answering or starts failing.

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use valkvedge::{object_store, Durability, LeaseConfig, Options, Role, ValkvEdge};

const UP: u8 = 0;
const HANGING: u8 = 1;
const FAILING: u8 = 2;

/// An in-memory object store that can be told to hang or to fail.
#[derive(Debug)]
struct Switchable {
    inner: InMemory,
    mode: AtomicU8,
}

impl Switchable {
    fn new() -> Arc<Self> {
        Arc::new(Switchable {
            inner: InMemory::new(),
            mode: AtomicU8::new(UP),
        })
    }

    fn set(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
    }

    /// Returns when the store is up; never returns while it hangs.
    async fn gate(&self) -> object_store::Result<()> {
        match self.mode.load(Ordering::SeqCst) {
            HANGING => std::future::pending().await,
            FAILING => Err(object_store::Error::Generic {
                store: "switchable",
                source: "the store is down".into(),
            }),
            _ => Ok(()),
        }
    }
}

impl std::fmt::Display for Switchable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Switchable")
    }
}

#[async_trait]
impl ObjectStore for Switchable {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.gate().await?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.gate().await?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.gate().await?;
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.gate().await?;
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.mode.load(Ordering::SeqCst) {
            HANGING => futures::stream::pending().boxed(),
            FAILING => futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "switchable",
                    source: "the store is down".into(),
                })
            })
            .boxed(),
            _ => self.inner.list(prefix),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.gate().await?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.gate().await?;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.gate().await?;
        self.inner.copy_if_not_exists(from, to).await
    }
}

const TIMEOUT: Duration = Duration::from_millis(400);

fn client(dir: &tempfile::TempDir, store: &Arc<Switchable>, role: Role) -> ValkvEdge {
    ValkvEdge::builder(dir.path())
        .options(Options {
            background: false,
            sync_write_timeout: TIMEOUT,
            lease: LeaseConfig {
                heartbeat_interval: Duration::from_millis(20),
                lease_ttl: Duration::from_millis(300),
                safety_margin: Duration::from_millis(100),
            },
            ..Options::default()
        })
        .object_store(store.clone(), "t")
        .role(role)
        .build()
        .unwrap()
}

fn text(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|b| String::from_utf8(b).unwrap())
}

/// Retries a sync write until it succeeds, as an application would.
async fn put_until_ok(db: &valkvedge::Database, key: &[u8], value: &[u8]) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match db.put(key, value).await {
            Ok(commit) => return commit.lsn.unwrap(),
            Err(e) if Instant::now() < deadline => {
                assert_eq!(e.code(), "UNAVAILABLE", "unexpected error: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("the writer never recovered: {e}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_write_times_out_instead_of_hanging_and_everything_local_keeps_working() {
    let store = Switchable::new();
    let dir = tempfile::tempdir().unwrap();
    let db = client(&dir, &store, Role::Writer)
        .database("app")
        .open()
        .await
        .unwrap();
    db.put(b"before", b"1").await.unwrap();

    store.set(HANGING);
    let started = Instant::now();
    let err = db.put(b"during", b"2").await.unwrap_err();
    let waited = started.elapsed();
    assert_eq!(err.code(), "UNAVAILABLE");
    assert!(waited >= TIMEOUT, "gave up too early: {waited:?}");
    assert!(
        waited < TIMEOUT * 3,
        "waited far past the limit: {waited:?}"
    );

    // Reads, scans and buffered writes never wait on the object store.
    let started = Instant::now();
    assert_eq!(text(db.get(b"before").unwrap()).as_deref(), Some("1"));
    assert_eq!(
        db.get(b"during").unwrap(),
        None,
        "the failed write is not visible"
    );
    assert_eq!(db.range::<&str, _>(..).unwrap().count(), 1);
    let mut batch = db.batch();
    batch.put(b"offline", b"3");
    batch.commit_with(Durability::Buffered).await.unwrap();
    assert_eq!(text(db.get(b"offline").unwrap()).as_deref(), Some("3"));
    assert!(started.elapsed() < Duration::from_millis(200));

    // A second sync write also fails in bounded time rather than queueing forever.
    let started = Instant::now();
    assert_eq!(
        db.put(b"during2", b"x").await.unwrap_err().code(),
        "UNAVAILABLE"
    );
    assert!(started.elapsed() < TIMEOUT * 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_writer_recovers_by_itself_when_the_store_comes_back() {
    let store = Switchable::new();
    let (dir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = client(&dir, &store, Role::Writer)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(db.put(b"a", b"1").await.unwrap().lsn, Some(1));

    store.set(HANGING);
    assert!(db.put(b"lost", b"x").await.is_err());
    let mut batch = db.batch();
    batch.put(b"buffered", b"2");
    batch.commit_with(Durability::Buffered).await.unwrap();
    assert_eq!(db.flush().await.unwrap_err().code(), "UNAVAILABLE");

    // Back up. No close, no reopen by the application: the same handle works.
    store.set(UP);
    let lsn = put_until_ok(&db, b"after", b"3").await;
    assert!(lsn >= 2);
    db.flush().await.unwrap();
    assert_eq!(db.status().unwrap().outbox_entries, 0);

    // The log is intact and in order: a fresh replica sees every acknowledged write.
    let follower = client(&rdir, &store, Role::Replica)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(text(follower.get(b"a").unwrap()).as_deref(), Some("1"));
    assert_eq!(text(follower.get(b"after").unwrap()).as_deref(), Some("3"));
    assert_eq!(
        text(follower.get(b"buffered").unwrap()).as_deref(),
        Some("2")
    );
    assert_eq!(follower.get(b"lost").unwrap(), None);
    assert_eq!(
        follower.status().unwrap().applied_lsn,
        db.status().unwrap().applied_lsn
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_that_returns_errors_fails_fast_and_the_writer_recovers() {
    let store = Switchable::new();
    let dir = tempfile::tempdir().unwrap();
    let db = client(&dir, &store, Role::Writer)
        .database("app")
        .open()
        .await
        .unwrap();
    db.put(b"a", b"1").await.unwrap();

    store.set(FAILING);
    let started = Instant::now();
    for _ in 0..3 {
        assert_eq!(db.put(b"b", b"2").await.unwrap_err().code(), "UNAVAILABLE");
    }
    assert!(
        started.elapsed() < TIMEOUT * 3,
        "errors should not wait for the timeout"
    );
    assert_eq!(text(db.get(b"a").unwrap()).as_deref(), Some("1"));

    store.set(UP);
    put_until_ok(&db, b"b", b"2").await;
    assert_eq!(text(db.get(b"b").unwrap()).as_deref(), Some("2"));
    assert_eq!(
        db.remote_lsn().await.unwrap(),
        Some(db.status().unwrap().applied_lsn)
    );
}
