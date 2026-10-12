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
    object_store, Database, DeviceId, Durability, Hlc, LeaseConfig, Op, OpKind, OpOutcome, Options,
    OutboxEntry, RejectReason, ValkvEdge, WriteOutcome,
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
            .and_then(|v| v.strip_prefix("Bearer "));
        if presented != Some(API_KEY) {
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
    match batch.commit_with(Durability::Sync).await {
        Ok(commit) => Json(json!({ "lsn": commit.lsn })).into_response(),
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
    let hex: String = body["device_id"]
        .as_str()
        .unwrap_or_default()
        .chars()
        .filter(|c| *c != '-')
        .collect();
    let mut device = [0u8; 16];
    for (i, byte) in device.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap();
    }
    let device = DeviceId(device);
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
                        device,
                    },
                })
                .collect(),
        })
        .collect();
    let report = match db.accept_push(device, entries).await {
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

fn device(dir: &tempfile::TempDir, service: &Service, key: &str) -> ValkvEdge {
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
async fn a_sync_write_is_confirmed_by_the_service_and_other_devices_follow() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = device(&adir, &service, API_KEY)
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

    // A second device with an empty directory downloads the database on open.
    let b = device(&bdir, &service, API_KEY)
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
    let a = device(&adir, &service, API_KEY)
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

    let b = device(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(text(b.get(b"offline").unwrap()).as_deref(), Some("1"));
    assert_eq!(b.get(b"needs-service").unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_devices_writing_the_same_key_offline_converge_and_the_loser_is_told() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = device(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let b = device(&bdir, &service, API_KEY)
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
async fn a_new_device_starts_from_the_snapshot_and_a_stale_one_catches_up_through_it() {
    let [sdir, adir, bdir, cdir] = dirs();
    let service = service(&sdir).await;
    let a = device(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    a.put(b"early", b"1").await.unwrap();

    // b follows up to here, then falls behind.
    let b = device(&bdir, &service, API_KEY)
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
    let c = device(&cdir, &service, API_KEY)
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
    let a = device(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let b = device(&bdir, &service, API_KEY)
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

    let wrong = device(&adir, &service, "ekv_wrong")
        .database("app")
        .open()
        .await;
    let err = wrong
        .err()
        .expect("a bad key cannot open a hosted database");
    assert_eq!(err.code(), "UNAUTHENTICATED");
    assert!(err.to_string().contains("API key"));

    let db = device(&bdir, &service, API_KEY)
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
    let a = device(&adir, &service, API_KEY)
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

    let b = device(&bdir, &service, API_KEY)
        .database("vault")
        .encryption_key(key)
        .open()
        .await
        .unwrap();
    assert_eq!(
        text(b.get(b"cvv").unwrap()).as_deref(),
        Some("PLAINTEXT-737")
    );

    let c = device(&cdir, &service, API_KEY)
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
