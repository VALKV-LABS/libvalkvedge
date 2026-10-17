//! The client side of the data API: how a replica reaches a database that a
//! service, not this process, writes to the object store.
//!
//! The API is described in `docs/hosted-api.md`.

use crate::local::LocalStore;
use crate::{Auth, Version};
use base64::engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD as B64URL};
use base64::Engine;
use serde_json::{json, Value};
use std::time::Duration;
use valkvedge_log::format::decode_snapshot_part;
use valkvedge_log::{
    now_ms, ChainPos, Error, Frame, Hlc, Lsn, OpKind, RejectReason, ReplicaId, Result, Segment,
};
use valkvedge_sync::{Change, Condition, OpOutcome, OutboxEntry, PushReport, StateStore};

/// Header carrying the error code of a failed request.
const ERROR_CODE_HEADER: &str = "x-edgekv-error";
/// Header by which this replica names itself when it reads, so the service can
/// tell which replicas follow a database and how far each has got.
const REPLICA_HEADER: &str = "x-edgekv-replica";
/// Header carrying the version of a value read.
const VERSION_HEADER: &str = "x-edgekv-version";
/// How long a download (log page or snapshot part) may take.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);
/// A token is replaced this long before it expires, so a request never
/// carries one that runs out on the way.
const TOKEN_MARGIN_MS: u64 = 60_000;

pub(crate) struct Client {
    http: reqwest::Client,
    base: String,
    auth: Auth,
    /// The database's name, which a token source is asked about.
    database: String,
    /// The token in use and when it expires (ms since the epoch, `u64::MAX`
    /// if it does not say).
    token: tokio::sync::Mutex<Option<(String, u64)>>,
    /// This installation's id for the database: the same one its pushes carry.
    replica: ReplicaId,
    /// Bound on a commit or a push.
    write_timeout: Duration,
}

/// When a token expires, read from its `exp` claim without checking the
/// signature: only the service can do that, and only the timing is needed.
fn token_expiry_ms(token: &str) -> Option<u64> {
    let payload = token.split('.').nth(1)?;
    let claims: Value = serde_json::from_slice(&B64URL.decode(payload).ok()?).ok()?;
    claims["exp"].as_u64()?.checked_mul(1000)
}

/// What `GET /log` answered.
enum LogPage {
    Frames {
        generation: u64,
        frames: Vec<Frame>,
    },
    /// The frames asked for exist only inside the current snapshot.
    SnapshotRequired,
}

fn unavailable(what: &str, e: reqwest::Error) -> Error {
    Error::Unavailable(format!("{what}: {e}"))
}

/// Codes the service is known to return, so they can be reported as static
/// strings. Anything else is reported as `SERVICE_ERROR`.
fn known_code(code: &str) -> &'static str {
    const KNOWN: &[&str] = &[
        "UNAUTHENTICATED",
        "TOKEN_EXPIRED",
        "FORBIDDEN",
        "ACCOUNT_SUSPENDED",
        "STORAGE_QUOTA_EXCEEDED",
        "DATABASE_SIZE_EXCEEDED",
        "TRANSFER_QUOTA_EXCEEDED",
        "WRITE_QUOTA_EXCEEDED",
        "RATE_LIMITED",
        "DATABASE_NOT_READY",
        "LEASE_HELD",
        "BAD_REQUEST",
        "KEY_NOT_FOUND",
        "SNAPSHOT_REQUIRED",
        "SNAPSHOT_CHANGED",
        "INVALID_PAGE_TOKEN",
        "FENCED",
        "INTERNAL",
    ];
    KNOWN
        .iter()
        .find(|known| **known == code)
        .copied()
        .unwrap_or("SERVICE_ERROR")
}

fn service_error(status: reqwest::StatusCode, code: Option<String>, body: &[u8]) -> Error {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let field = |name: &str| {
        parsed
            .as_ref()
            .and_then(|v| v["error"][name].as_str())
            .map(str::to_string)
    };
    let code = code.or_else(|| field("code")).unwrap_or_default();
    let message = field("message").unwrap_or_else(|| format!("the service answered {status}"));
    match code.as_str() {
        "CONDITION_FAILED" => Error::ConditionFailed {
            key: field("key")
                .and_then(|key| B64.decode(key).ok())
                .unwrap_or_default(),
        },
        "NOT_FOUND" => Error::NotFound(message),
        "UNAVAILABLE" => Error::Unavailable(message),
        "KEY_TOO_LARGE" => Error::KeyTooLarge {
            limit: crate::MAX_KEY_BYTES,
        },
        "VALUE_TOO_LARGE" => Error::ValueTooLarge {
            limit: crate::MAX_VALUE_BYTES,
        },
        "BATCH_TOO_LARGE" => Error::BatchTooLarge {
            limit: crate::MAX_BATCH_BYTES,
        },
        "READ_ONLY" => Error::ReadOnly(message),
        _ if status.is_server_error() && code.is_empty() => Error::Unavailable(message),
        other => Error::Service {
            code: known_code(other),
            message,
        },
    }
}

fn kind_name(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Put => "put",
        OpKind::Delete => "delete",
    }
}

impl Client {
    pub(crate) fn new(
        base_url: &str,
        auth: Auth,
        database: &str,
        replica: ReplicaId,
        write_timeout: Duration,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| Error::local(format!("HTTP client: {e}")))?;
        Ok(Client {
            http,
            base: format!("{}/v1/db/{database}", base_url.trim_end_matches('/')),
            auth,
            database: database.to_string(),
            token: tokio::sync::Mutex::new(None),
            replica,
            write_timeout,
        })
    }

    /// The credential for the next request: the API key, or a token that
    /// is not about to expire, fetched from the token source if needed.
    async fn credential(&self) -> Result<String> {
        let source = match &self.auth {
            Auth::ApiKey(key) => return Ok(key.clone()),
            Auth::Tokens(source) => source,
        };
        let mut held = self.token.lock().await;
        if let Some((token, expires)) = held.as_ref() {
            if now_ms().saturating_add(TOKEN_MARGIN_MS) < *expires {
                return Ok(token.clone());
            }
        }
        let token = source.token(&self.database).await?;
        let expires = token_expiry_ms(&token).unwrap_or(u64::MAX);
        *held = Some((token.clone(), expires));
        Ok(token)
    }

    /// Forgets a token the service refused, unless another request already
    /// replaced it.
    async fn forget_token(&self, refused: &str) {
        let mut held = self.token.lock().await;
        if held.as_ref().is_some_and(|(token, _)| token == refused) {
            *held = None;
        }
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        timeout: Duration,
    ) -> Result<reqwest::Response> {
        // A refused token is replaced and the request sent once more. The
        // bodies sent here are bytes in memory, so the request can be copied.
        let retry = match self.auth {
            Auth::Tokens(_) => request.try_clone(),
            Auth::ApiKey(_) => None,
        };
        let credential = self.credential().await?;
        match self.send_once(request, &credential, timeout).await {
            Err(Error::Service { code, .. })
                if matches!(code, "TOKEN_EXPIRED" | "UNAUTHENTICATED") && retry.is_some() =>
            {
                self.forget_token(&credential).await;
                let credential = self.credential().await?;
                let retry = retry.expect("checked above");
                self.send_once(retry, &credential, timeout).await
            }
            other => other,
        }
    }

    async fn send_once(
        &self,
        request: reqwest::RequestBuilder,
        credential: &str,
        timeout: Duration,
    ) -> Result<reqwest::Response> {
        let response = request
            .bearer_auth(credential)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| unavailable("could not reach the service", e))?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let code = response
            .headers()
            .get(ERROR_CODE_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = response.bytes().await.unwrap_or_default();
        Err(service_error(status, code, &body))
    }

    async fn json(&self, request: reqwest::RequestBuilder, timeout: Duration) -> Result<Value> {
        let response = self.send(request, timeout).await?;
        let bytes = response
            .bytes()
            .await
            .map_err(|e| unavailable("the response was cut short", e))?;
        serde_json::from_slice(&bytes).map_err(|e| Error::corrupt(format!("service response: {e}")))
    }

    /// One atomic sync write, applied only if every condition holds.
    /// Returns its LSN once the service has stored it, and the version each
    /// change was given (empty if the service does not say).
    pub(crate) async fn commit(
        &self,
        changes: &[Change],
        conditions: &[Condition],
    ) -> Result<(Lsn, Vec<Hlc>)> {
        let ops: Vec<Value> = changes
            .iter()
            .map(|c| {
                json!({
                    "op": kind_name(c.kind),
                    "key": B64.encode(&c.key),
                    "value": B64.encode(&c.value),
                })
            })
            .collect();
        let mut body = json!({ "ops": ops });
        if !conditions.is_empty() {
            body["conditions"] = conditions
                .iter()
                .map(|c| {
                    json!({
                        "key": B64.encode(&c.key),
                        "version": c.version.map(|v| Version::from_hlc(v).to_string()),
                    })
                })
                .collect();
        }
        let request = self.http.post(format!("{}/commit", self.base)).json(&body);
        let reply = self.json(request, self.write_timeout).await?;
        let lsn = reply["lsn"]
            .as_u64()
            .ok_or_else(|| Error::corrupt("commit response has no lsn"))?;
        let mut stamps = Vec::new();
        for version in reply["versions"].as_array().into_iter().flatten() {
            let version: Version = version
                .as_str()
                .and_then(|text| text.parse().ok())
                .ok_or_else(|| Error::corrupt("commit response has a malformed version"))?;
            stamps.push(version.hlc());
        }
        Ok((lsn, stamps))
    }

    /// Hands outbox entries to the service. Safe to repeat.
    pub(crate) async fn push(
        &self,
        replica: ReplicaId,
        entries: &[OutboxEntry],
    ) -> Result<PushReport> {
        let body: Vec<Value> = entries
            .iter()
            .map(|entry| {
                let ops: Vec<Value> = entry
                    .ops
                    .iter()
                    .map(|op| {
                        json!({
                            "op": kind_name(op.kind),
                            "key": B64.encode(&op.key),
                            "value": B64.encode(&op.value),
                            "hlc": { "ms": op.hlc.physical_ms, "counter": op.hlc.counter },
                        })
                    })
                    .collect();
                json!({ "seq": entry.seq, "ops": ops })
            })
            .collect();
        let request = self.http.post(format!("{}/push", self.base)).json(&json!({
            "replica_id": replica.to_string(),
            "entries": body,
        }));
        let reply = self.json(request, self.write_timeout).await?;
        let malformed = || Error::corrupt("push response is malformed");
        let mut report = Vec::new();
        for entry in reply["entries"].as_array().ok_or_else(malformed)? {
            let seq = entry["seq"].as_u64().ok_or_else(malformed)?;
            let mut outcomes = Vec::new();
            for result in entry["results"].as_array().ok_or_else(malformed)? {
                outcomes.push(match result.as_str().ok_or_else(malformed)? {
                    "applied" => OpOutcome::Applied,
                    "superseded" => OpOutcome::Superseded,
                    "duplicate" => OpOutcome::Duplicate,
                    "clock_skew" => OpOutcome::Rejected(RejectReason::ClockSkew),
                    "stale_write" => OpOutcome::Rejected(RejectReason::StaleWrite),
                    _ => return Err(malformed()),
                });
            }
            report.push((seq, outcomes));
        }
        Ok(PushReport {
            watermark: reply["watermark"].as_u64().ok_or_else(malformed)?,
            lsn: reply["lsn"].as_u64().ok_or_else(malformed)?,
            entries: report,
        })
    }

    /// Reads one key from the service, with its version. The key must be
    /// UTF-8 text, as it travels in the URL path.
    pub(crate) async fn get(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Hlc)>> {
        let mut url = self.url("kv")?;
        {
            let text = std::str::from_utf8(key).map_err(|_| {
                Error::Unsupported("a key read from the service must be UTF-8 text".into())
            })?;
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| Error::local("the service URL cannot take a path"))?;
            for segment in text.split('/') {
                segments.push(segment);
            }
        }
        let response = match self.send(self.http.get(url), self.write_timeout).await {
            Ok(response) => response,
            Err(Error::Service {
                code: "KEY_NOT_FOUND",
                ..
            }) => return Ok(None),
            Err(e) => return Err(e),
        };
        let version: Version = response
            .headers()
            .get(VERSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| Error::corrupt("a value from the service came without its version"))?;
        let value = response
            .bytes()
            .await
            .map_err(|e| unavailable("the response was cut short", e))?;
        Ok(Some((value.to_vec(), version.hlc())))
    }

    /// One page of keys starting with `prefix`, in order, from the service,
    /// with each value and its version, and the token for the next page.
    /// The prefix must be UTF-8 text, as it travels in the query.
    pub(crate) async fn scan(
        &self,
        prefix: &[u8],
        limit: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>, Hlc)>, Option<String>)> {
        let prefix = std::str::from_utf8(prefix).map_err(|_| {
            Error::Unsupported("a prefix read from the service must be UTF-8 text".into())
        })?;
        let mut url = self.url("kv")?;
        url.query_pairs_mut()
            .append_pair("prefix", prefix)
            .append_pair("limit", &limit.to_string());
        if let Some(token) = page_token {
            url.query_pairs_mut().append_pair("page_token", token);
        }
        let reply = self.json(self.http.get(url), self.write_timeout).await?;
        let malformed = || Error::corrupt("scan response is malformed");
        let mut entries = Vec::new();
        for item in reply["items"].as_array().ok_or_else(malformed)? {
            let field = |name: &str| {
                item[name]
                    .as_str()
                    .and_then(|text| B64.decode(text).ok())
                    .ok_or_else(malformed)
            };
            let version: Version = item["version"]
                .as_str()
                .and_then(|text| text.parse().ok())
                .ok_or_else(malformed)?;
            entries.push((field("key")?, field("value")?, version.hlc()));
        }
        let next = reply["next_page_token"].as_str().map(str::to_string);
        Ok((entries, next))
    }

    /// The database's URL with `path` appended.
    fn url(&self, path: &str) -> Result<reqwest::Url> {
        reqwest::Url::parse(&format!("{}/{path}", self.base))
            .map_err(|e| Error::local(format!("the service URL is not valid: {e}")))
    }

    /// The newest LSN the service holds for this database.
    pub(crate) async fn head(&self) -> Result<Lsn> {
        let request = self
            .http
            .get(format!("{}/head", self.base))
            .header(REPLICA_HEADER, self.replica.to_string());
        let reply = self.json(request, self.write_timeout).await?;
        reply["lsn"]
            .as_u64()
            .ok_or_else(|| Error::corrupt("head response has no lsn"))
    }

    async fn log(&self, after: Lsn) -> Result<LogPage> {
        // `after` is how far this replica has read: with the replica header,
        // the service knows how far behind it is.
        let request = self
            .http
            .get(format!("{}/log?after={after}", self.base))
            .header(REPLICA_HEADER, self.replica.to_string());
        let response = match self.send(request, DOWNLOAD_TIMEOUT).await {
            Ok(response) => response,
            Err(Error::Service {
                code: "SNAPSHOT_REQUIRED",
                ..
            }) => return Ok(LogPage::SnapshotRequired),
            Err(e) => return Err(e),
        };
        let generation = response
            .headers()
            .get("x-edgekv-generation")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let bytes = response
            .bytes()
            .await
            .map_err(|e| unavailable("the log download was cut short", e))?;
        // The page is a segment: every frame in it is checksummed.
        let (segment, _) = Segment::decode(&bytes)?;
        Ok(LogPage::Frames {
            generation,
            frames: segment.frames,
        })
    }

    /// Replaces the local replicated state with the service's current snapshot.
    async fn bootstrap(&self, state: &LocalStore) -> Result<()> {
        // A compaction can replace the snapshot between listing and download;
        // the service then says so and the listing is fetched again.
        for _ in 0..5 {
            let listing = self
                .json(
                    self.http.get(format!("{}/snapshot", self.base)),
                    self.write_timeout,
                )
                .await?;
            let malformed = || Error::corrupt("snapshot listing is malformed");
            let generation = listing["generation"].as_u64().ok_or_else(malformed)?;
            let snapshot_lsn = listing["snapshot_lsn"].as_u64().ok_or_else(malformed)?;
            let origin = self.base.split("/v1/").next().unwrap_or_default();

            state.begin_reset()?;
            let mut changed = false;
            for part in listing["parts"].as_array().ok_or_else(malformed)? {
                let url = part["url"].as_str().ok_or_else(malformed)?;
                let checksum = part["checksum"].as_u64().ok_or_else(malformed)? as u32;
                let request = self.http.get(format!("{origin}{url}"));
                let bytes = match self.send(request, DOWNLOAD_TIMEOUT).await {
                    Ok(response) => response
                        .bytes()
                        .await
                        .map_err(|e| unavailable("the snapshot download was cut short", e))?,
                    Err(Error::Service {
                        code: "SNAPSHOT_CHANGED",
                        ..
                    }) => {
                        changed = true;
                        break;
                    }
                    Err(e) => return Err(e),
                };
                state.load(&decode_snapshot_part(&bytes, checksum)?)?;
            }
            if changed {
                continue;
            }
            return state.finish_reset(ChainPos {
                generation,
                next_seq: 0,
                prev_checksum: 0,
                applied_lsn: snapshot_lsn,
            });
        }
        Err(Error::Unavailable(
            "the snapshot kept changing while it was being downloaded".into(),
        ))
    }

    /// Applies everything the service has that the local copy does not.
    /// Returns the local LSN afterwards.
    pub(crate) async fn pull(&self, state: &LocalStore) -> Result<Lsn> {
        if state.needs_bootstrap()? {
            self.bootstrap(state).await?;
        }
        loop {
            let applied = state.position()?.applied_lsn;
            match self.log(applied).await? {
                LogPage::SnapshotRequired => self.bootstrap(state).await?,
                LogPage::Frames { frames, .. } if frames.is_empty() => return Ok(applied),
                LogPage::Frames { generation, frames } => {
                    if frames[0].lsn != applied + 1 {
                        return Err(Error::corrupt(format!(
                            "the service sent frames starting at {} but the local copy is at {applied}",
                            frames[0].lsn
                        )));
                    }
                    let last = frames.last().map(|f| f.lsn).unwrap_or(applied);
                    // Applying frames and recording the position is one
                    // atomic local write, so a crash here loses nothing.
                    state.apply(
                        &frames,
                        ChainPos {
                            generation,
                            next_seq: 0,
                            prev_checksum: 0,
                            applied_lsn: last,
                        },
                    )?;
                }
            }
        }
    }
}
