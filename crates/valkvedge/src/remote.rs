//! The client side of the data API: how a replica reaches a database that a
//! service, not this process, writes to the object store.
//!
//! The API is described in `docs/hosted-api.md`.

use crate::local::LocalStore;
use crate::prefixes::Prefixes;
use crate::{Auth, History, Point, Restored, Version};
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
/// Header with which the service says it held a `GET /log` that asked to
/// wait. Without it, the service answered at once, and the replica polls.
const WAITED_HEADER: &str = "x-edgekv-waited";
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
    /// For a partial copy, the prefixes it asks the service for.
    prefixes: Option<Prefixes>,
}

/// When a token expires, read from its `exp` claim without checking the
/// signature: only the service can do that, and only the timing is needed.
fn token_expiry_ms(token: &str) -> Option<u64> {
    let payload = token.split('.').nth(1)?;
    let claims: Value = serde_json::from_slice(&B64URL.decode(payload).ok()?).ok()?;
    claims["exp"].as_u64()?.checked_mul(1000)
}

/// What `GET /log` answered.
pub(crate) enum LogPage {
    Frames {
        generation: u64,
        frames: Vec<Frame>,
        /// The service held the request until there was something new, or
        /// until the wait asked for ran out.
        waited: bool,
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
        "NOT_IN_HISTORY" => Error::NotInHistory(message),
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
        prefixes: Option<Prefixes>,
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
            prefixes,
        })
    }

    /// The database's URL with `path` appended and, for a partial copy, a
    /// `prefix` parameter for each prefix it holds, so the service sends
    /// only those keys.
    fn partial_url(&self, path: &str) -> Result<reqwest::Url> {
        let mut url = self.url(path)?;
        if let Some(prefixes) = &self.prefixes {
            let mut query = url.query_pairs_mut();
            for prefix in prefixes.as_slice() {
                // Checked to be UTF-8 when the database was opened.
                query.append_pair("prefix", &String::from_utf8_lossy(prefix));
            }
        }
        Ok(url)
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
                let mut op = json!({
                    "op": kind_name(c.kind),
                    "key": B64.encode(&c.key),
                    "value": B64.encode(&c.value),
                });
                if let (OpKind::Put, Some(ms)) = (c.kind, c.expires_ms) {
                    op["expires_at"] = rfc3339(ms).into();
                }
                op
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
                        let mut json = json!({
                            "op": kind_name(op.kind),
                            "key": B64.encode(&op.key),
                            "value": B64.encode(&op.value),
                            "hlc": { "ms": op.hlc.physical_ms, "counter": op.hlc.counter },
                        });
                        if let (OpKind::Put, Some(ms)) = (op.kind, op.expires_ms) {
                            json["expires_at"] = rfc3339(ms).into();
                        }
                        json
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

    /// How far back the service can restore this database.
    pub(crate) async fn history(&self) -> Result<History> {
        let reply = self
            .json(self.http.get(self.url("history")?), self.write_timeout)
            .await?;
        let malformed = || Error::corrupt("history response is malformed");
        Ok(History {
            from_lsn: reply["from_lsn"].as_u64().ok_or_else(malformed)?,
            from_ms: match reply["from"].as_str() {
                Some(text) => Some(parse_rfc3339(text).ok_or_else(malformed)?),
                None => None,
            },
            to_lsn: reply["to_lsn"].as_u64().ok_or_else(malformed)?,
        })
    }

    /// Asks the service to restore this database to `point`.
    pub(crate) async fn restore(&self, point: Point) -> Result<Restored> {
        let body = match point {
            Point::Lsn(lsn) => json!({ "lsn": lsn }),
            Point::Time(ms) => json!({ "at": rfc3339(ms) }),
        };
        let request = self.http.post(self.url("restore")?).json(&body);
        // Rebuilding a large database takes a while.
        let reply = self.json(request, DOWNLOAD_TIMEOUT).await?;
        let malformed = || Error::corrupt("restore response is malformed");
        Ok(Restored {
            from: reply["from_lsn"].as_u64().ok_or_else(malformed)?,
            lsn: reply["lsn"].as_u64().ok_or_else(malformed)?,
        })
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

    /// The frames after `after`. With `wait`, the service may hold the
    /// request until there are some, for at most that long.
    pub(crate) async fn log(&self, after: Lsn, wait: Option<Duration>) -> Result<LogPage> {
        // `after` is how far this replica has read: with the replica header,
        // the service knows how far behind it is.
        let mut url = self.partial_url("log")?;
        url.query_pairs_mut()
            .append_pair("after", &after.to_string());
        if let Some(wait) = wait {
            url.query_pairs_mut()
                .append_pair("wait_ms", &wait.as_millis().to_string());
        }
        let request = self
            .http
            .get(url)
            .header(REPLICA_HEADER, self.replica.to_string());
        let timeout = DOWNLOAD_TIMEOUT + wait.unwrap_or_default();
        let response = match self.send(request, timeout).await {
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
        let waited = response.headers().contains_key(WAITED_HEADER);
        let bytes = response
            .bytes()
            .await
            .map_err(|e| unavailable("the log download was cut short", e))?;
        // The page is a segment: every frame in it is checksummed.
        let (segment, _) = Segment::decode(&bytes)?;
        Ok(LogPage::Frames {
            generation,
            frames: segment.frames,
            waited,
        })
    }

    /// Replaces the local replicated state with the service's current snapshot.
    async fn bootstrap(&self, state: &LocalStore) -> Result<()> {
        // A compaction can replace the snapshot between listing and download;
        // the service then says so and the listing is fetched again.
        for _ in 0..5 {
            let listing = self
                .json(
                    self.http.get(self.partial_url("snapshot")?),
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
                // Parts filtered for a partial copy are made when they are
                // downloaded, so the listing cannot know their checksum:
                // each part's own checksum is checked instead.
                let checksum = match part["checksum"].as_u64() {
                    Some(checksum) => Some(checksum as u32),
                    None if self.prefixes.is_some() => None,
                    None => return Err(malformed()),
                };
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
                let checksum = match checksum {
                    Some(checksum) => checksum,
                    None => own_checksum(&bytes)?,
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
            match self.log(applied, None).await? {
                LogPage::SnapshotRequired => self.bootstrap(state).await?,
                LogPage::Frames { frames, .. } if frames.is_empty() => return Ok(applied),
                LogPage::Frames {
                    generation, frames, ..
                } => apply_frames(state, applied, generation, &frames)?,
            }
        }
    }
}

/// Milliseconds since the Unix epoch as RFC 3339 in UTC, to the millisecond.
pub(crate) fn rfc3339(ms: u64) -> String {
    let days = (ms / 86_400_000) as i64;
    let in_day = ms % 86_400_000;
    // Civil date from days since the epoch (H. Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        in_day / 3_600_000,
        in_day / 60_000 % 60,
        in_day / 1000 % 60,
        in_day % 1000
    )
}

/// The reverse of [`rfc3339`]: `YYYY-MM-DDTHH:MM:SS`, optional fraction,
/// then `Z`.
pub(crate) fn parse_rfc3339(text: &str) -> Option<u64> {
    let text = text.strip_suffix('Z').or_else(|| text.strip_suffix('z'))?;
    let (date, time) = text.split_once(['T', 't'])?;
    let mut date = date.splitn(3, '-').map(|p| p.parse::<i64>().ok());
    let (year, month, day) = (date.next()??, date.next()??, date.next()??);
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut clock = clock.splitn(3, ':').map(|p| p.parse::<u64>().ok());
    let (hour, minute, second) = (clock.next()??, clock.next()??, clock.next()??);
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let millis: u64 = format!("{fraction:0<3}")[..3].parse().ok()?;
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let m = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = u64::try_from(era * 146_097 + doe - 719_468).ok()?;
    Some(days * 86_400_000 + hour * 3_600_000 + minute * 60_000 + second * 1000 + millis)
}

/// The checksum a snapshot part ends with, which decoding then verifies
/// against the part's contents.
fn own_checksum(part: &[u8]) -> Result<u32> {
    let tail = part
        .len()
        .checked_sub(4)
        .map(|start| &part[start..])
        .ok_or_else(|| Error::corrupt("snapshot part too short"))?;
    Ok(u32::from_le_bytes(tail.try_into().expect("len 4")))
}

/// Applies frames read after `applied` to the local copy.
pub(crate) fn apply_frames(
    state: &LocalStore,
    applied: Lsn,
    generation: u64,
    frames: &[Frame],
) -> Result<()> {
    let Some(first) = frames.first() else {
        return Ok(());
    };
    if first.lsn != applied + 1 {
        return Err(Error::corrupt(format!(
            "the service sent frames starting at {} but the local copy is at {applied}",
            first.lsn
        )));
    }
    let last = frames.last().map(|f| f.lsn).unwrap_or(applied);
    // Applying frames and recording the position is one atomic local
    // write, so a crash here loses nothing.
    state.apply(
        frames,
        ChainPos {
            generation,
            next_seq: 0,
            prev_checksum: 0,
            applied_lsn: last,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_travel_as_rfc3339_and_back() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339(951_782_400_000), "2000-02-29T00:00:00.000Z");
        for ms in [0, 951_782_400_000, 1_792_000_000_123, 4_102_444_799_999] {
            assert_eq!(parse_rfc3339(&rfc3339(ms)), Some(ms), "{ms}");
        }
        assert_eq!(parse_rfc3339("2000-02-29T00:00:00Z"), Some(951_782_400_000));
        assert_eq!(
            parse_rfc3339("2000-02-29T00:00:00.5Z"),
            Some(951_782_400_500)
        );
        for bad in [
            "yesterday",
            "2000-13-01T00:00:00Z",
            "2000-02-29T00:00:00",
            "",
        ] {
            assert_eq!(parse_rfc3339(bad), None, "{bad}");
        }
    }
}
