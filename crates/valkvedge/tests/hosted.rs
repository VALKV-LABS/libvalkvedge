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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use valkvedge::{
    object_store, Database, Durability, Hlc, LeaseConfig, Op, OpKind, OpOutcome, Options,
    OutboxEntry, RejectReason, ReplicaId, ValkvEdge, Version, WatchEvent, WriteOutcome,
};
use valkvedge_log::format::{decode_snapshot_part, encode_snapshot_part};
use valkvedge_log::{read_manifest, read_segments, Layout, Record, Segment, Store};

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
    /// Holds a `GET /log` that asks to wait, as the service does. Off, the
    /// server answers at once, as one that does not hold requests would.
    hold_log: AtomicBool,
    /// How many `GET /log` requests asked to wait.
    waits_asked: AtomicUsize,
    /// The prefixes asked for on each read of the log or the snapshot.
    prefixes_asked: std::sync::Mutex<Vec<Vec<String>>>,
}

type Shared = State<Arc<Server>>;
/// Query parameters in order, so that one may appear more than once.
type Params = Query<Vec<(String, String)>>;

fn param<'a>(query: &'a [(String, String)], name: &str) -> Option<&'a str> {
    query
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// The `prefix` parameters of a read by a partial copy: empty for a whole one.
fn prefixes(server: &Server, query: &[(String, String)]) -> Vec<String> {
    let asked: Vec<String> = query
        .iter()
        .filter(|(k, _)| k == "prefix")
        .map(|(_, v)| v.clone())
        .collect();
    server.prefixes_asked.lock().unwrap().push(asked.clone());
    asked
}

fn kept(prefixes: &[String], key: &[u8]) -> bool {
    prefixes.is_empty() || prefixes.iter().any(|p| key.starts_with(p.as_bytes()))
}

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

/// An operation's `expires_at`, in milliseconds since the Unix epoch.
fn expires(op: &Value) -> Option<u64> {
    op["expires_at"].as_str().and_then(parse_rfc3339)
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
        let (key, value) = (decode(&op["key"]), decode(&op["value"]));
        match (kind(&op["op"]), expires(&op)) {
            (OpKind::Put, None) => batch.put(&key, &value),
            (OpKind::Put, Some(ms)) => batch.put_expiring(
                &key,
                &value,
                std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms),
            ),
            (OpKind::Delete, _) => batch.delete(&key),
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
                    expires_ms: expires(op),
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
    Query(query): Params,
    headers: HeaderMap,
) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let prefixes = prefixes(&server, &query);
    let after: u64 = param(&query, "after")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
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
    let wait = param(&query, "wait_ms")
        .and_then(|v| v.parse().ok())
        .map(Duration::from_millis);
    if wait.is_some() {
        server.waits_asked.fetch_add(1, Ordering::SeqCst);
    }
    let hold = wait.filter(|_| server.hold_log.load(Ordering::SeqCst));
    let deadline = Instant::now() + hold.unwrap_or_default();
    loop {
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
                // A partial copy gets every frame, with only its keys in it.
                .map(|mut f| {
                    f.ops.retain(|op| kept(&prefixes, &op.key));
                    f
                })
                .collect();
        }
        // Held: nothing new yet, and time left to wait for it.
        if frames.is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let (bytes, _) = Segment::new(0, frames).encode();
        let mut response = Bytes::from(bytes).into_response();
        response.headers_mut().insert(
            "x-edgekv-generation",
            generation.to_string().parse().unwrap(),
        );
        if hold.is_some() {
            response
                .headers_mut()
                .insert("x-edgekv-waited", "1".parse().unwrap());
        }
        return response;
    }
}

async fn snapshot(
    State(server): Shared,
    Path(database): Path<String>,
    Query(query): Params,
    headers: HeaderMap,
) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let prefixes = prefixes(&server, &query);
    let layout = Layout::new(db.object_prefix().unwrap());
    let Some((manifest, _)) = read_manifest(&server.store, &layout).await.unwrap() else {
        return Json(json!({ "generation": 0, "snapshot_lsn": 0, "parts": [] })).into_response();
    };
    let parts: Vec<Value> = manifest
        .snapshot_parts
        .iter()
        .enumerate()
        .map(|(i, part)| {
            let mut url = reqwest::Url::parse(&format!(
                "http://x/v1/db/{database}/snapshot/parts/{i}?generation={}",
                manifest.generation
            ))
            .unwrap();
            for prefix in &prefixes {
                url.query_pairs_mut().append_pair("prefix", prefix);
            }
            let url = &url.as_str()["http://x".len()..];
            if prefixes.is_empty() {
                json!({ "index": i, "size": part.size, "checksum": part.checksum, "url": url })
            } else {
                // Filtered when downloaded: its checksum is not known yet.
                json!({ "index": i, "url": url })
            }
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
    Query(query): Params,
    headers: HeaderMap,
) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let prefixes = prefixes(&server, &query);
    let layout = Layout::new(db.object_prefix().unwrap());
    let (manifest, _) = read_manifest(&server.store, &layout)
        .await
        .unwrap()
        .unwrap();
    let stored = &manifest.snapshot_parts[part];
    let object = server.store.get(&stored.key).await.unwrap().unwrap();
    if prefixes.is_empty() {
        return object.bytes.into_response();
    }
    let mut records = decode_snapshot_part(&object.bytes, stored.checksum).unwrap();
    records.retain(|record| match record {
        Record::Entry { key, .. } => kept(&prefixes, key),
        Record::Watermark { .. } => true,
    });
    let (bytes, _) = encode_snapshot_part(part as u32, &records);
    Bytes::from(bytes).into_response()
}

/// Days since the epoch for a civil date, and back (H. Hinnant).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - i64::from(m <= 2);
    let (era, yoe) = (y.div_euclid(400), y.rem_euclid(400));
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

fn rfc3339(ms: u64) -> String {
    let (days, rem) = ((ms / 86_400_000) as i64, ms % 86_400_000);
    let z = days + 719_468;
    let (era, doe) = (z.div_euclid(146_097), z.rem_euclid(146_097));
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let (d, m) = (
        doy - (153 * mp + 2) / 5 + 1,
        if mp < 10 { mp + 3 } else { mp - 9 },
    );
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ` only: what the client sends.
fn parse_rfc3339(text: &str) -> Option<u64> {
    let n = |range: std::ops::Range<usize>| text.get(range)?.parse::<i64>().ok();
    let days = days_from_civil(n(0..4)?, n(5..7)?, n(8..10)?);
    let ms = ((days * 24 + n(11..13)?) * 60 + n(14..16)?) * 60_000 + n(17..19)? * 1000 + n(20..23)?;
    u64::try_from(ms).ok()
}

async fn history(
    State(server): Shared,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    match db.history().await {
        Ok(reach) => Json(json!({
            "from_lsn": reach.from_lsn,
            "from": reach.from_ms.map(rfc3339),
            "to_lsn": reach.to_lsn,
        }))
        .into_response(),
        Err(e) => library_error(e),
    }
}

async fn restore(
    State(server): Shared,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let db = match server.open(&headers, &database, true).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let point = match (
        body["lsn"].as_u64(),
        body["at"].as_str().and_then(parse_rfc3339),
    ) {
        (Some(lsn), _) => valkvedge::Point::Lsn(lsn),
        (None, Some(ms)) => valkvedge::Point::Time(ms),
        (None, None) => return error(StatusCode::BAD_REQUEST, "BAD_REQUEST", "lsn or at"),
    };
    match db.restore(point).await {
        Ok(restored) => {
            Json(json!({ "from_lsn": restored.from, "lsn": restored.lsn })).into_response()
        }
        Err(e) if e.code() == "NOT_IN_HISTORY" => {
            error(StatusCode::CONFLICT, "NOT_IN_HISTORY", &e.to_string())
        }
        Err(e) => library_error(e),
    }
}

async fn kv_get(
    State(server): Shared,
    Path((database, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    match db.get_versioned(key.as_bytes()) {
        Ok(Some((value, version))) => {
            ([("x-edgekv-version", version.to_string())], value).into_response()
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "KEY_NOT_FOUND", "key not found"),
        Err(e) => library_error(e),
    }
}

async fn kv_scan(
    State(server): Shared,
    Path(database): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let db = match server.open(&headers, &database, false).await {
        Ok(db) => db,
        Err(response) => return response,
    };
    let prefix = query
        .get("prefix")
        .cloned()
        .unwrap_or_default()
        .into_bytes();
    let limit: usize = query
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    let after = query
        .get("page_token")
        .map(|token| B64.decode(token).unwrap());
    let mut items = Vec::new();
    let mut next_page_token = None;
    for entry in db.prefix(&prefix).unwrap() {
        let (key, value) = entry.unwrap();
        if after.as_ref().is_some_and(|after| key <= *after) {
            continue;
        }
        if items.len() == limit {
            next_page_token = items.last().map(|item: &Value| item["key"].clone());
            break;
        }
        let (_, version) = db.get_versioned(&key).unwrap().unwrap();
        items.push(json!({ "key": B64.encode(&key), "value": B64.encode(&value), "version": version.to_string() }));
    }
    Json(json!({ "items": items, "next_page_token": next_page_token })).into_response()
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
        hold_log: AtomicBool::new(true),
        waits_asked: AtomicUsize::new(0),
        prefixes_asked: std::sync::Mutex::new(Vec::new()),
    });
    let app = Router::new()
        .route("/v1/db/{db}/commit", post(commit))
        .route("/v1/db/{db}/push", post(push))
        .route("/v1/db/{db}/head", get(head))
        .route("/v1/db/{db}/log", get(log))
        .route("/v1/db/{db}/snapshot", get(snapshot))
        .route("/v1/db/{db}/snapshot/parts/{part}", get(snapshot_part))
        .route("/v1/db/{db}/history", get(history))
        .route("/v1/db/{db}/restore", post(restore))
        .route("/v1/db/{db}/kv", get(kv_scan))
        .route("/v1/db/{db}/kv/{*key}", get(kv_get))
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

#[tokio::test(flavor = "multi_thread")]
async fn a_database_opened_for_ingestion_uploads_its_writes_and_keeps_nothing() {
    let [sdir, idir, rdir] = dirs();
    let service = service(&sdir).await;
    // Nothing reaches the service before the first push, so the push is
    // what makes it take the database's lease: slower than the tight limit
    // the other tests use when the machine is busy running them all.
    let db = ValkvEdge::builder(idir.path())
        .options(Options {
            sync_write_timeout: Duration::from_secs(15),
            ..fast()
        })
        .remote(&service.url, API_KEY)
        .build()
        .unwrap()
        .database("readings")
        .ingest()
        .open()
        .await
        .unwrap();

    // Writes are buffered by default: they return before the service has them.
    assert!(db.put(b"t/1", b"20.5").await.unwrap().outbox_seq.is_some());
    db.put(b"t/2", b"21.0").await.unwrap();
    assert_eq!(db.status().unwrap().outbox_entries, 2);
    // Nothing is read locally.
    assert_eq!(db.get(b"t/1").unwrap_err().code(), "UNSUPPORTED");
    assert!(matches!(db.prefix(b"t/"), Err(e) if e.code() == "UNSUPPORTED"));
    assert!(db.watch(b"").is_err());

    // Once uploaded, the outbox is empty and no copy was ever made.
    assert_eq!(db.sync().await.unwrap(), 0);
    let status = db.status().unwrap();
    assert_eq!((status.outbox_entries, status.applied_lsn), (0, 0));

    // Reads go to the service.
    assert_eq!(
        text(db.fetch(b"t/1").await.unwrap()).as_deref(),
        Some("20.5")
    );
    assert_eq!(db.fetch(b"t/9").await.unwrap(), None);
    let first = db.fetch_page(b"t/", 1, None).await.unwrap();
    assert_eq!(first.entries, vec![(b"t/1".to_vec(), b"20.5".to_vec())]);
    let rest = db
        .fetch_page(b"t/", 10, first.next_page_token.as_deref())
        .await
        .unwrap();
    assert_eq!(rest.entries, vec![(b"t/2".to_vec(), b"21.0".to_vec())]);
    assert_eq!(rest.next_page_token, None);

    // Sync and conditional writes go straight to the service.
    let mut batch = db.batch();
    batch.put(b"t/3", b"22.0");
    assert!(batch
        .commit_with(Durability::Sync)
        .await
        .unwrap()
        .lsn
        .is_some());
    let increment = |current: Option<&[u8]>| {
        let n: u32 = current.map_or(0, |v| std::str::from_utf8(v).unwrap().parse().unwrap());
        Some((n + 1).to_string().into_bytes())
    };
    db.update(b"count", increment).await.unwrap();
    db.update(b"count", increment).await.unwrap();
    assert_eq!(
        text(db.fetch(b"count").await.unwrap()).as_deref(),
        Some("2")
    );

    // An ordinary replica of the same database sees all of it.
    let other = replica(&rdir, &service, API_KEY)
        .database("readings")
        .open()
        .await
        .unwrap();
    assert_eq!(text(other.get(b"t/3").unwrap()).as_deref(), Some("22.0"));
    assert_eq!(text(other.get(b"count").unwrap()).as_deref(), Some("2"));
}

#[tokio::test(flavor = "multi_thread")]
async fn ingestion_needs_a_service_and_one_way_of_opening_per_database() {
    let [sdir, ldir, idir] = dirs();
    let local = ValkvEdge::builder(ldir.path())
        .options(fast())
        .build()
        .unwrap();
    let refused = local.database("readings").ingest().open().await;
    assert_eq!(refused.err().unwrap().code(), "UNSUPPORTED");

    let service = service(&sdir).await;
    let kv = replica(&idir, &service, API_KEY);
    let _ingesting = kv.database("readings").ingest().open().await.unwrap();
    let other_way = kv.database("readings").open().await;
    assert_eq!(other_way.err().unwrap().code(), "UNSUPPORTED");
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_reads_the_newest_value_from_the_service_and_opens_sealed_values() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let key = [7u8; 32];
    let follower = replica(&adir, &service, API_KEY)
        .database("vault")
        .encryption_key(key)
        .open()
        .await
        .unwrap();
    let sensor = replica(&bdir, &service, API_KEY)
        .database("vault")
        .encryption_key(key)
        .ingest()
        .open()
        .await
        .unwrap();
    sensor.put(b"secret", b"hello").await.unwrap();
    sensor.flush().await.unwrap();

    // The service only ever held the sealed value.
    let held = service
        .server
        .kv
        .database("vault")
        .open()
        .await
        .unwrap()
        .get(b"secret")
        .unwrap()
        .unwrap();
    assert_ne!(held, b"hello");
    assert_eq!(
        text(sensor.fetch(b"secret").await.unwrap()).as_deref(),
        Some("hello")
    );

    // A replica whose copy is behind (no background sync in these tests)
    // reads the newest value with fetch.
    assert_eq!(follower.get(b"secret").unwrap(), None);
    assert_eq!(
        text(follower.fetch(b"secret").await.unwrap()).as_deref(),
        Some("hello")
    );
    // Keys travel in the URL, so fetch reads text keys only.
    assert_eq!(
        follower.fetch(&[0xff, 0x00]).await.unwrap_err().code(),
        "UNSUPPORTED"
    );
}

// --- Following: changes reach an open replica as they happen ----------------

/// A client whose open databases follow the service in the background, and
/// poll every `poll` only when the service does not hold their requests.
fn following(dir: &tempfile::TempDir, service: &Service, options: Options) -> ValkvEdge {
    ValkvEdge::builder(dir.path())
        .options(Options {
            background: true,
            follow_wait: Duration::from_secs(5),
            ..options
        })
        .remote(&service.url, API_KEY)
        .build()
        .unwrap()
}

/// Waits until `key` reads `value`, failing after `within`.
async fn arrives(db: &Database, key: &[u8], value: &str, within: Duration) {
    let started = Instant::now();
    while text(db.get(key).unwrap()).as_deref() != Some(value) {
        assert!(
            started.elapsed() < within,
            "{value} did not arrive within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_open_replica_is_told_of_a_write_at_once_not_at_its_next_poll() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let writer = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    writer.put(b"k", b"before").await.unwrap();

    // b would poll once a minute: a write that reaches it sooner was pushed.
    let poll_once_a_minute = Options {
        pull_interval: Duration::from_secs(60),
        ..fast()
    };
    let b = following(&bdir, &service, poll_once_a_minute)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(text(b.get(b"k").unwrap()).as_deref(), Some("before"));
    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let value = format!("v{i}");
        writer.put(b"k", value.as_bytes()).await.unwrap();
        arrives(&b, b"k", &value, Duration::from_secs(3)).await;
    }
    assert!(service.server.waits_asked.load(Ordering::SeqCst) >= 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn against_a_service_that_does_not_hold_requests_a_replica_polls_at_its_interval() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    service.server.hold_log.store(false, Ordering::SeqCst);
    let writer = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    writer.put(b"k", b"1").await.unwrap();
    let every_300_ms = Options {
        pull_interval: Duration::from_millis(300),
        ..fast()
    };
    let b = following(&bdir, &service, every_300_ms)
        .database("app")
        .open()
        .await
        .unwrap();

    // Answered at once, it asks again after its interval, not in a tight loop.
    let before = service.server.waits_asked.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let asked = service.server.waits_asked.load(Ordering::SeqCst) - before;
    assert!(asked <= 6, "asked {asked} times in one second");

    writer.put(b"k", b"2").await.unwrap();
    arrives(&b, b"k", "2", Duration::from_secs(3)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_followed_database_still_closes_when_nobody_uses_it() {
    let [sdir, bdir] = dirs();
    let service = service(&sdir).await;
    let close_soon = Options {
        pull_interval: Duration::from_millis(100),
        idle_close_after: Duration::from_millis(300),
        ..fast()
    };
    let kv = following(&bdir, &service, close_soon);
    let db = kv.database("app").open().await.unwrap();
    db.put(b"k", b"v").await.unwrap();
    drop(db);

    // Its follower is waiting on the service meanwhile; that must not keep
    // the database open.
    let started = Instant::now();
    while kv.open_databases() > 0 {
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "the database stayed open"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// --- Partial copies --------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_partial_copy_holds_and_receives_only_its_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let service = service(&dir).await;
    let [adir, bdir, cdir] = dirs();
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    a.put(b"team/1", b"ours").await.unwrap();
    a.put(b"other/1", b"theirs").await.unwrap();

    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .prefixes(["team/"])
        .open()
        .await
        .unwrap();
    assert_eq!(text(b.get(b"team/1").unwrap()).as_deref(), Some("ours"));
    assert_eq!(refused(b.get(b"other/1")), "NOT_REPLICATED");
    assert_eq!(refused(b.prefix(b"").map(|_| ())), "NOT_REPLICATED");
    assert_eq!(
        refused(b.range(b"team/".to_vec()..).map(|_| ())),
        "NOT_REPLICATED"
    );
    assert_eq!(refused(b.watch(b"oth").map(|_| ())), "NOT_REPLICATED");
    assert_eq!(
        b.range(b"team/0".to_vec()..b"team/9".to_vec())
            .unwrap()
            .count(),
        1
    );
    // The service is still there for keys the copy does not hold.
    assert_eq!(
        text(b.fetch(b"other/1").await.unwrap()).as_deref(),
        Some("theirs")
    );

    // Writes: only its own keys.
    assert_eq!(refused(b.put(b"other/2", b"x").await), "NOT_REPLICATED");
    let mut mixed = b.batch();
    mixed.put(b"team/2", b"x").put(b"other/2", b"x");
    assert_eq!(refused(mixed.commit().await), "NOT_REPLICATED");
    b.put(b"team/2", b"from-b").await.unwrap();

    // Followed: new writes under its prefix arrive, others do not.
    a.put(b"team/3", b"new").await.unwrap();
    a.put(b"other/3", b"new").await.unwrap();
    let lsn = b.sync().await.unwrap();
    assert_eq!(lsn, a.sync().await.unwrap(), "every frame counts");
    assert_eq!(b.prefix(b"team/").unwrap().count(), 3);
    assert!(
        server_asked(&service, &["team/"]),
        "the copy asked for its prefixes"
    );

    // A new partial copy after compaction is loaded from a filtered snapshot.
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
    let c = replica(&cdir, &service, API_KEY)
        .database("app")
        .prefixes(["other/"])
        .open()
        .await
        .unwrap();
    assert_eq!(c.prefix(b"other/").unwrap().count(), 2);
    assert_eq!(refused(c.get(b"team/1")), "NOT_REPLICATED");
    assert_eq!(c.status().unwrap().applied_lsn, lsn);
}

fn refused<T>(result: valkvedge::Result<T>) -> &'static str {
    result.err().expect("refused").code()
}

fn server_asked(service: &Service, prefixes: &[&str]) -> bool {
    let want: Vec<String> = prefixes.iter().map(|p| p.to_string()).collect();
    service
        .server
        .prefixes_asked
        .lock()
        .unwrap()
        .contains(&want)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_opened_with_other_prefixes_is_downloaded_again() {
    let dir = tempfile::tempdir().unwrap();
    let service = service(&dir).await;
    let [adir, bdir, cdir] = dirs();
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    a.put(b"team/1", b"t").await.unwrap();
    a.put(b"other/1", b"o").await.unwrap();

    {
        let b = replica(&bdir, &service, API_KEY)
            .database("app")
            .prefixes(["team/"])
            .open()
            .await
            .unwrap();
        assert_eq!(text(b.get(b"team/1").unwrap()).as_deref(), Some("t"));
    }
    // Same directory, another process: the whole database.
    let whole = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(text(whole.get(b"other/1").unwrap()).as_deref(), Some("o"));
    assert_eq!(text(whole.get(b"team/1").unwrap()).as_deref(), Some("t"));

    // In one process, a database is opened one way.
    let again = replica(&cdir, &service, API_KEY);
    again.database("app").open().await.unwrap();
    let err = again
        .database("app")
        .prefixes(["team/"])
        .open()
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "UNSUPPORTED");
}

#[tokio::test(flavor = "multi_thread")]
async fn prefixes_are_refused_where_a_partial_copy_makes_no_sense() {
    let dir = tempfile::tempdir().unwrap();
    let service = service(&dir).await;
    let [a, b, c] = dirs();
    let code = |r: valkvedge::Result<Database>| r.err().unwrap().code();

    let ingesting = replica(&a, &service, API_KEY)
        .database("app")
        .ingest()
        .prefixes(["team/"])
        .open()
        .await;
    assert_eq!(code(ingesting), "UNSUPPORTED");

    let binary = replica(&b, &service, API_KEY)
        .database("app")
        .prefixes([&[0xff, 0xfe][..]])
        .open()
        .await;
    assert_eq!(code(binary), "UNSUPPORTED", "hosted prefixes are text");

    let local = ValkvEdge::builder(c.path())
        .options(fast())
        .build()
        .unwrap()
        .database("app")
        .prefixes(["team/"])
        .open()
        .await;
    assert_eq!(
        code(local),
        "UNSUPPORTED",
        "nothing to be a partial copy of"
    );
}

// --- History and restores ----------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_asks_the_service_to_restore_and_every_copy_reloads() {
    let dir = tempfile::tempdir().unwrap();
    let service = service(&dir).await;
    let [adir, bdir] = dirs();
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
    a.put(b"plan", b"good").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let then = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    tokio::time::sleep(Duration::from_millis(20)).await;
    a.put(b"plan", b"oops").await.unwrap();
    b.sync().await.unwrap();

    let reach = a.history().await.unwrap();
    assert_eq!(
        (reach.from_lsn, reach.from_ms, reach.to_lsn),
        (0, Some(0), 2)
    );

    let restored = a.restore(valkvedge::Point::Time(then)).await.unwrap();
    assert_eq!((restored.from, restored.lsn), (1, 3));
    assert_eq!(
        text(a.get(b"plan").unwrap()).as_deref(),
        Some("good"),
        "reloaded at once"
    );
    assert_eq!(b.sync().await.unwrap(), 3);
    assert_eq!(text(b.get(b"plan").unwrap()).as_deref(), Some("good"));

    let err = a.restore(valkvedge::Point::Lsn(10)).await.unwrap_err();
    assert_eq!(err.code(), "NOT_IN_HISTORY");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_expiry_travels_through_the_service_and_its_writer_deletes_the_value() {
    let [sdir, adir, bdir] = dirs();
    let service = service(&sdir).await;
    let a = replica(&adir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    let ttl = Duration::from_secs(2);
    a.put_with_ttl(b"sync", b"1", ttl).await.unwrap();
    let mut buffered = a.batch();
    buffered.put_with_ttl(b"buffered", b"2", ttl);
    buffered.commit_with(Durability::Buffered).await.unwrap();
    a.flush().await.unwrap();
    let (sync_at, buffered_at) = (
        a.expires_at(b"sync").unwrap().expect("expires"),
        a.expires_at(b"buffered").unwrap().expect("expires"),
    );

    // Another replica gets the expiry with the values.
    let b = replica(&bdir, &service, API_KEY)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(b.expires_at(b"sync").unwrap(), Some(sync_at));
    assert_eq!(b.expires_at(b"buffered").unwrap(), Some(buffered_at));

    tokio::time::sleep(ttl + Duration::from_millis(100)).await;
    assert_eq!(b.get(b"sync").unwrap(), None);
    assert_eq!(b.get(b"buffered").unwrap(), None);
    // The service's writer deletes them; the replicas only read.
    assert_eq!(a.expire().await.unwrap(), 0);
    let owner = service.server.kv.database("app").open().await.unwrap();
    assert_eq!(owner.expire().await.unwrap(), 2);
    let lsn = owner.status().unwrap().applied_lsn;
    assert_eq!(b.sync().await.unwrap(), lsn);
}
