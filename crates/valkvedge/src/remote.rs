//! The client side of the data API: how a device reaches a database that a
//! service, not this process, writes to the object store.
//!
//! The API is described in `docs/hosted-api.md`.

use crate::local::LocalStore;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::{json, Value};
use std::time::Duration;
use valkvedge_log::format::decode_snapshot_part;
use valkvedge_log::{ChainPos, DeviceId, Error, Frame, Lsn, OpKind, RejectReason, Result, Segment};
use valkvedge_sync::{Change, OpOutcome, OutboxEntry, PushReport, StateStore};

/// Header carrying the error code of a failed request.
const ERROR_CODE_HEADER: &str = "x-edgekv-error";
/// Header by which this device names itself when it reads, so the service can
/// tell which devices follow a database and how far each has got.
const DEVICE_HEADER: &str = "x-edgekv-device";
/// How long a download (log page or snapshot part) may take.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);

pub(crate) struct Client {
    http: reqwest::Client,
    base: String,
    api_key: String,
    /// This installation's id for the database: the same one its pushes carry.
    device: DeviceId,
    /// Bound on a commit or a push.
    write_timeout: Duration,
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
        "FORBIDDEN",
        "ACCOUNT_SUSPENDED",
        "STORAGE_QUOTA_EXCEEDED",
        "RATE_LIMITED",
        "DATABASE_NOT_READY",
        "LEASE_HELD",
        "BAD_REQUEST",
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
        "NOT_FOUND" | "KEY_NOT_FOUND" => Error::NotFound(message),
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
        api_key: &str,
        database: &str,
        device: DeviceId,
        write_timeout: Duration,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| Error::local(format!("HTTP client: {e}")))?;
        Ok(Client {
            http,
            base: format!("{}/v1/db/{database}", base_url.trim_end_matches('/')),
            api_key: api_key.to_string(),
            device,
            write_timeout,
        })
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        timeout: Duration,
    ) -> Result<reqwest::Response> {
        let response = request
            .bearer_auth(&self.api_key)
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

    /// One atomic sync write. Returns its LSN once the service has stored it.
    pub(crate) async fn commit(&self, changes: &[Change]) -> Result<Lsn> {
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
        let request = self
            .http
            .post(format!("{}/commit", self.base))
            .json(&json!({ "ops": ops }));
        let reply = self.json(request, self.write_timeout).await?;
        reply["lsn"]
            .as_u64()
            .ok_or_else(|| Error::corrupt("commit response has no lsn"))
    }

    /// Hands outbox entries to the service. Safe to repeat.
    pub(crate) async fn push(
        &self,
        device: DeviceId,
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
            "device_id": device.to_string(),
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

    /// The newest LSN the service holds for this database.
    pub(crate) async fn head(&self) -> Result<Lsn> {
        let request = self
            .http
            .get(format!("{}/head", self.base))
            .header(DEVICE_HEADER, self.device.to_string());
        let reply = self.json(request, self.write_timeout).await?;
        reply["lsn"]
            .as_u64()
            .ok_or_else(|| Error::corrupt("head response has no lsn"))
    }

    async fn log(&self, after: Lsn) -> Result<LogPage> {
        // `after` is how far this device has read: with the device header,
        // the service knows how far behind it is.
        let request = self
            .http
            .get(format!("{}/log?after={after}", self.base))
            .header(DEVICE_HEADER, self.device.to_string());
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
