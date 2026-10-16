//! The hosted client against a small reference server.
//!
//! The server below implements the data API described in
//! `docs/hosted-api.md` on top of a writer that owns the object store. It is
//! test scaffolding: one API key, one tenant, no limits.

// Handlers return a ready-made response as their error; boxing it would only
// add noise to scaffolding.
#![allow(clippy::result_large_err)]

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use object_store::memory::InMemory;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use valkvedge::{
    object_store, Database, Durability, Hlc, LeaseConfig, Op, OpKind, OpOutcome, Options,
    OutboxEntry, RejectReason, ReplicaId, ValkvEdge, Version, WatchEvent, WriteOutcome,
};
use valkvedge_log::{read_manifest, read_segments, Layout, Segment, Store};

const API_KEY: &str = "ekv_test_key";
const UP: u8 = 0;
const DOWN: u8 = 1;
const OVER_QUOTA: u8 = 2;

struct Server {
    kv: ValkvEdge,
    store: Store,
    mode: AtomicU8,
    /// The replica named on each read of the log, and the position it read from.
    readers: std::sync::Mutex<Vec<(String, u64)>>,
    /// The replica named in each push.
    pushers: std::sync::Mutex<Vec<String>>,
    /// Tokens the server accepts, and those it says have expired.
    tokens: std::sync::Mutex<Vec<String>>,
    expired: std::sync::Mutex<Vec<String>>,
    /// The credential presented on each request.
    presented: std::sync::Mutex<Vec<String>>,
}

type Shared = State<Arc<Server>>;

fn error(status: StatusCode, code: &'static str, message: &str) -> Response {
    let mut response = (
        status,
        Json(json!({ "error": { "code": code, "message": message } })),
    )
        .into_response();
    response
        .headers_mut()
        .insert("x-edgekv-error", code.parse().unwrap());
    response
}

fn library_error(e: valkvedge::ValkvEdgeError) -> Response {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL",
        &e.to_string(),
    )
}

impl Server {
    /// Authenticates and opens the database, or says why not.
    async fn open(
        &self,
        headers: &HeaderMap,
        database: &str,
        writing: bool,
    ) -> Result<Database, Response> {
        match self.mode.load(Ordering::SeqCst) {
            DOWN => return Err(StatusCode::SERVICE_UNAVAILABLE.into_response()),
            OVER_QUOTA if writing => {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "STORAGE_QUOTA_EXCEEDED",
                    "this account is over its storage limit",
                ))
            }
            _ => {}
        }
        let presented = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or_default()
            .to_string();
        self.presented.lock().unwrap().push(presented.clone());
        if self.expired.lock().unwrap().contains(&presented) {
            return Err(error(
                StatusCode::UNAUTHORIZED,
                "TOKEN_EXPIRED",
                "this token has expired",
            ));
        }
        if presented != API_KEY && !self.tokens.lock().unwrap().contains(&presented) {
            return Err(error(
                StatusCode::UNAUTHORIZED,
                "UNAUTHENTICATED",
                "a valid API key is required",
            ));
        }
        self.kv
            .database(database)
            .open()
            .await
            .map_err(library_error)
    }
}

fn decode(value: &Value) -> Vec<u8> {
    B64.decode(value.as_str().unwrap_or_default())
        .unwrap_or_default()
}

fn kind(value: &Value) -> OpKind {
    if value == "delete" {
        OpKind::Delete
    } else {
        OpKind::Put
    }
}

async fn commit(
    State(server): Shared,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let db = match server.open(&headers, &database, true).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let mut batch = db.batch();
    for op in body["ops"].as_array().cloned().unwrap_or_default() {
        match kind(&op["op"]) {
            OpKind::Put => batch.put(&decode(&op["key"]), &decode(&op["value"])),
            OpKind::Delete => batch.delete(&decode(&op["key"])),
        };
    }
    for condition in body["conditions"].as_array().cloned().unwrap_or_default() {
        let key = decode(&condition["key"]);
        match condition["version"].as_str() {
            Some(version) => batch.if_version(&key, &version.parse::<Version>().unwrap()),
            None => batch.if_absent(&key),
        };
    }
    match batch.commit_with(Durability::Sync).await {
        Ok(commit) => {
            let versions: Vec<String> = commit.versions.iter().map(Version::to_string).collect();
            Json(json!({ "lsn": commit.lsn, "versions": versions })).into_response()
        }
        Err(valkvedge::ValkvEdgeError::ConditionFailed { key }) => {
            let mut response = (
                StatusCode::PRECONDITION_FAILED,
                Json(json!({ "error": {
                    "code": "CONDITION_FAILED",
                    "message": "a key was not at the expected version",
                    "key": B64.encode(&key),
                } })),
            )
                .into_response();
            response
                .headers_mut()
                .insert("x-edgekv-error", "CONDITION_FAILED".parse().unwrap());
            response
        }
        Err(e) => library_error(e),
    }
}

async fn push(
    State(server): Shared,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let db = match server.open(&headers, &database, true).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let hex: String = body["replica_id"]
        .as_str()
        .unwrap_or_default()
        .chars()
        .filter(|c| *c != '-')
        .collect();
    let mut replica = [0u8; 16];
    for (i, byte) in replica.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap();
    }
    let replica = ReplicaId(replica);
    server.pushers.lock().unwrap().push(replica.to_string());
    let entries = body["entries"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|entry| OutboxEntry {
            seq: entry["seq"].as_u64().unwrap(),
            ops: entry["ops"]
                .as_array()
                .unwrap()
                .iter()
                .map(|op| Op {
                    kind: kind(&op["op"]),
                    key: decode(&op["key"]),
                    value: decode(&op["value"]),
                    hlc: Hlc {
                        physical_ms: op["hlc"]["ms"].as_u64().unwrap(),
                        counter: op["hlc"]["counter"].as_u64().unwrap_or(0) as u32,
                        replica,
                    },
                })
                .collect(),
        })
        .collect();
    let report = match db.accept_push(replica, entries).await {
        Ok(report) => report,
        Err(e) => return library_error(e),
    };
    let entries: Vec<Value> = report
        .entries
        .iter()
        .map(|(seq, outcomes)| {
            let results: Vec<&str> = outcomes
                .iter()
                .map(|o| match o {
                    OpOutcome::Applied => "applied",
                    OpOutcome::Superseded => "superseded",
                    OpOutcome::Duplicate => "duplicate",
                    OpOutcome::Rejected(RejectReason::ClockSkew) => "clock_skew",
                    OpOutcome::Rejected(RejectReason::StaleWrite) => "stale_write",
                })
                .collect();
            json!({ "seq": seq, "results": results })
        })
        .collect();
    Json(json!({ "watermark": report.watermark, "lsn": report.lsn, "entries": entries }))
        .into_response()
}

async fn head(State(server): Shared, Path(database): Path<String>, headers: HeaderMap) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let lsn = db.remote_lsn().await.ok().flatten().unwrap_or(0);
    Json(json!({ "generation": 0, "lsn": lsn })).into_response()
}

async fn log(
    State(server): Shared,
    Path(database): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let after: u64 = query.get("after").and_then(|v| v.parse().ok()).unwrap_or(0);
    if let Some(replica) = headers
        .get("x-edgekv-replica")
        .and_then(|v| v.to_str().ok())
    {
        server
            .readers
            .lock()
            .unwrap()
            .push((replica.to_string(), after));
    }
    let layout = Layout::new(db.object_prefix().unwrap());
    let mut frames = Vec::new();
    let mut generation = 0;
    if let Some((manifest, _)) = read_manifest(&server.store, &layout).await.unwrap() {
        if after < manifest.snapshot_lsn {
            return error(
                StatusCode::CONFLICT,
                "SNAPSHOT_REQUIRED",
                "use the snapshot",
            );
        }
        generation = manifest.generation;
        frames = read_segments(&server.store, &layout, generation, 0, 0, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|s| s.segment.frames)
            .filter(|f| f.lsn > after)
            // Small pages, so the client's paging loop is exercised.
            .take(3)
            .collect();
    }
    let (bytes, _) = Segment::new(0, frames).encode();
    let mut response = Bytes::from(bytes).into_response();
    response.headers_mut().insert(
        "x-edgekv-generation",
        generation.to_string().parse().unwrap(),
    );
    response
}

async fn snapshot(
    State(server): Shared,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let layout = Layout::new(db.object_prefix().unwrap());
    let Some((manifest, _)) = read_manifest(&server.store, &layout).await.unwrap() else {
        return Json(json!({ "generation": 0, "snapshot_lsn": 0, "parts": [] })).into_response();
    };
    let parts: Vec<Value> = manifest
        .snapshot_parts
        .iter()
        .enumerate()
        .map(|(i, part)| {
            json!({
                "index": i,
                "size": part.size,
                "checksum": part.checksum,
                "url": format!("/v1/db/{database}/snapshot/parts/{i}?generation={}", manifest.generation),
            })
        })
        .collect();
    Json(json!({
        "generation": manifest.generation,
        "snapshot_lsn": manifest.snapshot_lsn,
        "parts": parts,
    }))
    .into_response()
}

async fn snapshot_part(
    State(server): Shared,
    Path((database, part)): Path<(String, usize)>,
    headers: HeaderMap,
) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let layout = Layout::new(db.object_prefix().unwrap());
    let (manifest, _) = read_manifest(&server.store, &layout)
        .await
        .unwrap()
        .unwrap();
    let object = server
        .store
        .get(&manifest.snapshot_parts[part].key)
        .await
        .unwrap()
        .unwrap();
    object.bytes.into_response()
}

struct Service {
    url: String,
    server: Arc<Server>,
}

fn fast() -> Options {
    Options {
        background: false,
        sync_write_timeout: Duration::from_secs(2),
        lease: LeaseConfig {
            heartbeat_interval: Duration::from_millis(20),
            lease_ttl: Duration::from_millis(300),
            safety_margin: Duration::from_millis(100),
        },
        ..Options::default()
    }
}

async fn service(dir: &tempfile::TempDir) -> Service {
    let object_store = Arc::new(InMemory::new());
    let kv = ValkvEdge::builder(dir.path())
        .options(fast())
        .object_store(object_store.clone(), "service")
        .build()
        .unwrap();
    let server = Arc::new(Server {
        kv,
        store: Store::new(object_store),
        mode: AtomicU8::new(UP),
        readers: std::sync::Mutex::new(Vec::new()),
        pushers: std::sync::Mutex::new(Vec::new()),
        tokens: std::sync::Mutex::new(Vec::new()),
        expired: std::sync::Mutex::new(Vec::new()),
        presented: std::sync::Mutex::new(Vec::new()),
    });
    let app = Router::new()
        .route("/v1/db/{db}/commit", post(commit))
        .route("/v1/db/{db}/push", post(push))
        .route("/v1/db/{db}/head", get(head))
        .route("/v1/db/{db}/log", get(log))
        .route("/v1/db/{db}/snapshot", get(snapshot))
        .route("/v1/db/{db}/snapshot/parts/{part}", get(snapshot_part))
        .with_state(server.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Service { url, server }
}

fn replica(dir: &tempfile::TempDir, service: &Service, key: &str) -> ValkvEdge {
    ValkvEdge::builder(dir.path())
        .options(fast())
        .remote(&service.url, key)
        .build()
        .unwrap()
}

fn text(value: Option<Vec<u8>>) -> Option<String> {
    value.map(|v| String::from_utf8(v).unwrap())
}

fn dirs<const N: usize>() -> [tempfile::TempDir; N] {
    std::array::from_fn(|_| tempfile::tempdir().unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_write_is_confirmed_by_the_service_and_other_replicas_follow() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();

    assert_eq!(a.put(b"k1", b"from-a").await.unwrap().lsn, Some(1));
    assert_eq!(
        text(a.get(b"k1").unwrap()).as_deref(),
        Some("from-a"),
        "read your own write"
    );
    assert_eq!(a.status().unwrap().applied_lsn, 1);

    // A second replica with an empty directory downloads the database on open.
    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(text(b.get(b"k1").unwrap()).as_deref(), Some("from-a"));

    // Enough writes to need several pages of the log.
    for i in 0..8u8 {
        a.put(&[b'n', i], b"x").await.unwrap();
    }
    assert_eq!(b.get(&[b'n', 7]).unwrap(), None, "b has not pulled yet");
    assert_eq!(b.remote_lsn().await.unwrap(), Some(9));
    assert_eq!(b.sync().await.unwrap(), 9);
    assert_eq!(b.prefix(b"n").unwrap().count(), 8);

    let mut batch = b.batch();
    batch.put(b"k2", b"from-b").delete(b"k1");
    assert_eq!(
        batch.commit().await.unwrap().lsn,
        Some(10),
        "a batch is one frame"
    );
    a.sync().await.unwrap();
    assert_eq!(a.get(b"k1").unwrap(), None);
    assert_eq!(text(a.get(b"k2").unwrap()).as_deref(), Some("from-b"));
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_writes_work_while_the_service_is_unreachable_and_arrive_later() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .durability(Durability::Buffered)
        .open()
        .await
        .unwrap();
    a.sync().await.unwrap();

    service.server.mode.store(DOWN, Ordering::SeqCst);
    let started = Instant::now();
    a.put(b"offline", b"1").await.unwrap();
    assert_eq!(text(a.get(b"offline").unwrap()).as_deref(), Some("1"));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "nothing local waits on the service"
    );

    let mut sync_write = a.batch();
    sync_write.put(b"needs-service", b"x");
    let err = sync_write.commit_with(Durability::Sync).await.unwrap_err();
    assert_eq!(err.code(), "UNAVAILABLE");
    assert_eq!(a.flush().await.unwrap_err().code(), "UNAVAILABLE");
    assert_eq!(
        a.status().unwrap().outbox_entries,
        1,
        "the entry is kept for the next attempt"
    );

    service.server.mode.store(UP, Ordering::SeqCst);
    assert_eq!(a.flush().await.unwrap(), 1);
    assert_eq!(a.status().unwrap().outbox_entries, 0);
    assert_eq!(text(a.get(b"offline").unwrap()).as_deref(), Some("1"));
    assert!(a.take_events().is_empty());

    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(text(b.get(b"offline").unwrap()).as_deref(), Some("1"));
    assert_eq!(b.get(b"needs-service").unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_replicas_writing_the_same_key_offline_converge_and_the_loser_is_told() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();

    let buffered = |db: &Database, value: &'static [u8]| {
        let mut batch = db.batch();
        batch.put(b"title", value);
        batch.commit_with(Durability::Buffered)
    };
    buffered(&a, b"from-a").await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    buffered(&b, b"from-b").await.unwrap();

    // b wrote later but pushes first; a's older write then loses.
    b.flush().await.unwrap();
    a.flush().await.unwrap();
    b.sync().await.unwrap();

    assert_eq!(text(a.get(b"title").unwrap()).as_deref(), Some("from-b"));
    assert_eq!(text(b.get(b"title").unwrap()).as_deref(), Some("from-b"));
    let events = a.take_events();
    assert_eq!(events.len(), 1);
    assert_eq!(
        (events[0].key.as_slice(), events[0].outcome),
        (&b"title"[..], WriteOutcome::Superseded)
    );
    assert!(b.take_events().is_empty());

    // Pushing again changes nothing.
    assert_eq!(a.flush().await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_replica_starts_from_the_snapshot_and_a_stale_one_catches_up_through_it() {
    let [sdir, adir, bdir, cdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    a.put(b"early", b"1").await.unwrap();

    // b follows up to here, then falls behind.
    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(b.status().unwrap().applied_lsn, 1);

    for i in 0..10u8 {
        a.put(&[b'k', i], b"v").await.unwrap();
    }
    a.delete(b"early").await.unwrap();
    service
        .server
        .kv
        .database("app")
        .open()
        .await
        .unwrap()
        .compact()
        .await
        .unwrap();
    a.put(b"after", b"compaction").await.unwrap();

    // c is brand new: snapshot, then the one frame after it.
    let c = replica(&cdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let status = c.status().unwrap();
    assert_eq!((status.generation, status.applied_lsn), (1, 13));
    assert_eq!(c.prefix(b"k").unwrap().count(), 10);
    assert_eq!(c.get(b"early").unwrap(), None);
    assert_eq!(
        text(c.get(b"after").unwrap()).as_deref(),
        Some("compaction")
    );

    // b's frames were folded into the snapshot: it is rebuilt from it.
    assert_eq!(b.sync().await.unwrap(), 13);
    assert_eq!(b.get(b"early").unwrap(), None);
    assert_eq!(b.range::<&str, _>(..).unwrap().count(), 11);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pending_buffered_write_survives_a_rebuild_from_the_snapshot() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    a.put(b"k", b"1").await.unwrap();
    b.sync().await.unwrap();

    let mut pending = b.batch();
    pending.put(b"mine", b"unsent");
    pending.commit_with(Durability::Buffered).await.unwrap();

    a.put(b"k", b"2").await.unwrap();
    service
        .server
        .kv
        .database("app")
        .open()
        .await
        .unwrap()
        .compact()
        .await
        .unwrap();

    // b must rebuild from the snapshot; its unsent write is kept and then sent.
    b.sync().await.unwrap();
    assert_eq!(text(b.get(b"k").unwrap()).as_deref(), Some("2"));
    assert_eq!(text(b.get(b"mine").unwrap()).as_deref(), Some("unsent"));
    assert_eq!(b.status().unwrap().outbox_entries, 0);
    a.sync().await.unwrap();
    assert_eq!(text(a.get(b"mine").unwrap()).as_deref(), Some("unsent"));
}

#[tokio::test(flavor = "multi_thread")]
async fn service_refusals_are_reported_with_the_services_own_codes() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;

    let wrong = replica(&adir, &service, "ekv_wrong")
        .database("app")
        .open()
        .await;
    let err = wrong
        .err()
        .expect("a bad key cannot open a hosted database");
    assert_eq!(err.code(), "UNAUTHENTICATED");
    assert!(err.to_string().contains("API key"));

    let db = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    db.put(b"k", b"v").await.unwrap();
    service.server.mode.store(OVER_QUOTA, Ordering::SeqCst);
    assert_eq!(
        db.put(b"k2", b"v").await.unwrap_err().code(),
        "STORAGE_QUOTA_EXCEEDED"
    );
    // Reads and pulls are not writes: they still work.
    assert_eq!(text(db.get(b"k").unwrap()).as_deref(), Some("v"));
    assert_eq!(db.sync().await.unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn with_a_key_the_service_only_ever_sees_ciphertext() {
    let [sdir, adir, bdir, cdir] = dirs();
    let service = service(&sdir).await;
    let key = [5u8; 32];
    let a = replica(&adir, &service, API_KEY)
        .database("vault")
        .encryption_key(key)
        .open()
        .await
        .unwrap();
    a.put(b"pin", b"PLAINTEXT-1234").await.unwrap();
    let mut buffered = a.batch();
    buffered.put(b"cvv", b"PLAINTEXT-737");
    buffered.commit_with(Durability::Buffered).await.unwrap();
    a.flush().await.unwrap();

    // What the service holds for those keys is not the plaintext.
    let on_service = service.server.kv.database("vault").open().await.unwrap();
    for name in [&b"pin"[..], &b"cvv"[..]] {
        let stored = on_service
            .get(name)
            .unwrap()
            .expect("the service has the key name");
        assert!(!stored.windows(9).any(|w| w == b"PLAINTEXT"));
    }

    let b = replica(&bdir, &service, API_KEY)
        .database("vault")
        .encryption_key(key)
        .open()
        .await
        .unwrap();
    assert_eq!(
        text(b.get(b"cvv").unwrap()).as_deref(),
        Some("PLAINTEXT-737")
    );

    let c = replica(&cdir, &service, API_KEY)
        .database("vault")
        .encryption_key([6u8; 32])
        .open()
        .await
        .unwrap();
    assert_eq!(c.get(b"pin").unwrap_err().code(), "INVALID_ENCRYPTION_KEY");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_object_store_and_a_service_cannot_both_be_configured() {
    let dir = tempfile::tempdir().unwrap();
    let err = ValkvEdge::builder(dir.path())
        .object_store(Arc::new(InMemory::new()), "p")
        .remote("http://localhost:1", "ekv_x")
        .build()
        .err()
        .unwrap();
    assert_eq!(err.code(), "UNSUPPORTED");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_names_itself_when_it_reads_with_the_id_its_pushes_carry() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    for i in 0..5u8 {
        a.put(&[b'k', i], b"v").await.unwrap();
    }
    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(b.sync().await.unwrap(), 5);

    let readers = service.server.readers.lock().unwrap().clone();
    let mut replicas: Vec<&str> = readers
        .iter()
        .map(|(replica, _)| replica.as_str())
        .collect();
    replicas.sort_unstable();
    replicas.dedup();
    assert_eq!(replicas.len(), 2, "two replicas, two ids: {readers:?}");
    for replica in &replicas {
        assert_eq!(replica.len(), 36, "a UUID in its usual form: {replica}");
    }
    // Each read says how far that replica already is, so the last thing each
    // one said is that it has everything.
    for replica in &replicas {
        let last = readers.iter().rev().find(|(d, _)| d == replica).unwrap().1;
        assert_eq!(last, 5, "{replica} ended at the head");
    }

    // A buffered write is pushed under the same id the replica reads with.
    let before: Vec<String> = readers.iter().map(|(replica, _)| replica.clone()).collect();
    let mut batch = b.batch();
    batch.put(b"buffered", b"x");
    batch.commit_with(Durability::Buffered).await.unwrap();
    b.flush().await.unwrap();
    let pushers = service.server.pushers.lock().unwrap().clone();
    assert_eq!(pushers.len(), 1);
    assert!(
        before.contains(&pushers[0]),
        "the pushing id {} is one of the reading ids",
        pushers[0]
    );

    // Reopening the same directory keeps the id.
    drop(b);
    let again = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    again.sync().await.unwrap();
    let last = service
        .server
        .readers
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .0
        .clone();
    assert_eq!(last, pushers[0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_conditional_write_on_a_stale_version_fails_and_the_copy_catches_up() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();

    let first = a.put(b"stock", b"10").await.unwrap();
    assert_eq!(first.versions.len(), 1, "the service reports the version");
    b.sync().await.unwrap();
    let (_, seen) = b.get_versioned(b"stock").unwrap().unwrap();
    assert_eq!(
        seen, first.versions[0],
        "both replicas agree on the version"
    );

    // a writes; b still holds the old version.
    let second = a.put(b"stock", b"9").await.unwrap();
    let mut stale = b.batch();
    stale.if_version(b"stock", &seen).put(b"stock", b"9");
    let err = stale.commit().await.unwrap_err();
    assert_eq!(err.code(), "CONDITION_FAILED");
    assert!(
        matches!(&err, valkvedge::ValkvEdgeError::ConditionFailed { key } if key == b"stock"),
        "{err:?}"
    );
    // Nothing was written, and b now sees the value that won.
    let (value, version) = b.get_versioned(b"stock").unwrap().unwrap();
    assert_eq!((value.as_slice(), version), (&b"9"[..], second.versions[0]));

    let mut fresh = b.batch();
    fresh.if_version(b"stock", &version).put(b"stock", b"8");
    fresh.commit().await.unwrap();
    a.sync().await.unwrap();
    assert_eq!(text(a.get(b"stock").unwrap()).as_deref(), Some("8"));

    let mut create = a.batch();
    create.if_absent(b"stock").put(b"stock", b"0");
    assert_eq!(
        create.commit().await.unwrap_err().code(),
        "CONDITION_FAILED",
        "the key exists"
    );

    let mut buffered = a.batch();
    buffered.if_absent(b"other").put(b"other", b"x");
    assert_eq!(
        buffered
            .commit_with(Durability::Buffered)
            .await
            .unwrap_err()
            .code(),
        "UNSUPPORTED",
        "a buffered write cannot be conditional"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replicas_counting_at_once_through_the_service_lose_no_increment() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let mut handles = Vec::new();
    for dir in [&adir, &bdir] {
        let db = replica(dir, &service, API_KEY)
            .database("app")
            .open()
            .await
            .unwrap();
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                db.update(b"counter", |current| {
                    let n: u64 =
                        current.map_or(0, |v| std::str::from_utf8(v).unwrap().parse().unwrap());
                    Some((n + 1).to_string().into_bytes())
                })
                .await
                .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    let on_service = service.server.kv.database("app").open().await.unwrap();
    assert_eq!(
        text(on_service.get(b"counter").unwrap()).as_deref(),
        Some("20")
    );
}

/// A token in the shape the client reads: only the `exp` claim matters to it.
fn token_expiring_in(seconds: u64, n: usize) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = URL_SAFE_NO_PAD.encode(json!({ "exp": now + seconds, "n": n }).to_string());
    format!("eyJhbGciOiJIUzI1NiJ9.{claims}.sig")
}

fn token_replica(
    dir: &tempfile::TempDir,
    service: &Service,
    issued: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    lifetime_secs: u64,
) -> ValkvEdge {
    let server = service.server.clone();
    ValkvEdge::builder(dir.path())
        .options(fast())
        .remote_with_tokens(&service.url, move |database: String| {
            let (issued, server) = (issued.clone(), server.clone());
            async move {
                let mut issued = issued.lock().unwrap();
                let token = token_expiring_in(lifetime_secs, issued.len());
                server.tokens.lock().unwrap().push(token.clone());
                issued.push((database, token.clone()));
                Ok(token)
            }
        })
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_with_tokens_fetches_one_per_database_and_replaces_a_refused_one() {
    let [sdir, adir] = dirs();
    let service = service(&sdir).await;
    let issued = Arc::new(std::sync::Mutex::new(Vec::new()));
    let kv = token_replica(&adir, &service, issued.clone(), 3600);
    let db = kv.database("app").open().await.unwrap();
    db.put(b"k", b"1").await.unwrap();
    db.put(b"k", b"2").await.unwrap();
    db.sync().await.unwrap();
    {
        let issued = issued.lock().unwrap();
        assert_eq!(issued.len(), 1, "a token valid for an hour is reused");
        assert_eq!(issued[0].0, "app", "the source is told which database");
    }
    assert!(
        !service
            .server
            .presented
            .lock()
            .unwrap()
            .contains(&API_KEY.to_string()),
        "the replica never holds the API key"
    );

    // The service stops accepting the token: the replica gets a new one and
    // the write goes through.
    let first = issued.lock().unwrap()[0].1.clone();
    service.server.expired.lock().unwrap().push(first.clone());
    db.put(b"k", b"3").await.unwrap();
    assert_eq!(issued.lock().unwrap().len(), 2);
    assert_eq!(
        service.server.presented.lock().unwrap().last().unwrap(),
        &issued.lock().unwrap()[1].1
    );

    let other = kv.database("other").open().await.unwrap();
    other.put(b"k", b"x").await.unwrap();
    assert_eq!(
        issued.lock().unwrap()[2].0,
        "other",
        "one token per database"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_token_close_to_expiry_is_replaced_before_it_is_used() {
    let [sdir, adir] = dirs();
    let service = service(&sdir).await;
    let issued = Arc::new(std::sync::Mutex::new(Vec::new()));
    // Thirty seconds left is inside the margin: never worth sending.
    let db = token_replica(&adir, &service, issued.clone(), 30)
        .database("app")
        .open()
        .await
        .unwrap();
    let before = service.server.presented.lock().unwrap().len();
    db.put(b"k", b"1").await.unwrap();
    db.put(b"k", b"2").await.unwrap();
    let presented = service.server.presented.lock().unwrap()[before..].to_vec();
    let mut distinct = presented.clone();
    distinct.sort();
    distinct.dedup();
    assert!(presented.len() >= 2);
    assert_eq!(
        distinct.len(),
        presented.len(),
        "no such token is sent twice"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_watch_reports_what_another_replica_wrote_once_it_arrives() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let mut watch = b.watch(b"chat/").unwrap();

    a.put(b"chat/1", b"hello").await.unwrap();
    a.put(b"other", b"ignored").await.unwrap();
    a.delete(b"chat/1").await.unwrap();
    assert!(
        watch.try_next().unwrap().is_none(),
        "nothing has arrived yet"
    );
    b.sync().await.unwrap();

    // One pull applied all three frames; the value is read when delivered.
    let event = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        event,
        WatchEvent::Changed {
            key: b"chat/1".to_vec(),
            value: None
        }
    );
    assert!(
        watch.try_next().unwrap().is_none(),
        "{:?}",
        watch.try_next()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_watch_is_told_to_reload_when_the_copy_is_rebuilt_from_a_snapshot() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    a.put(b"k", b"1").await.unwrap();
    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let mut watch = b.watch(b"").unwrap();
    a.put(b"k", b"2").await.unwrap();
    service
        .server
        .kv
        .database("app")
        .open()
        .await
        .unwrap()
        .compact()
        .await
        .unwrap();
    b.sync().await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event, WatchEvent::Reset);
    assert_eq!(text(b.get(b"k").unwrap()).as_deref(), Some("2"));
}
