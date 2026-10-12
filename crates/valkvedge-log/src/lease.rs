//! Node heartbeats and per-database leases built on conditional writes.
//!
//! A node renews one heartbeat object. A database lease names its owner node
//! and is valid while that node's heartbeat keeps changing. Clocks on
//! different machines are never compared: a watcher decides an owner is dead
//! only after seeing its heartbeat stand still for `lease_ttl` on the
//! watcher's own clock.

use crate::error::{Error, Result};
use crate::format::{node_key, Layout};
use crate::store::{PutOutcome, Store};
use crate::types::now_ms;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

/// Timing parameters for heartbeats and leases.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// How often a node renews its heartbeat.
    pub heartbeat_interval: Duration,
    /// How long a heartbeat must stand still before its leases are stale.
    pub lease_ttl: Duration,
    /// A node stops writing this long before its own leases could be taken.
    pub safety_margin: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        LeaseConfig {
            heartbeat_interval: Duration::from_secs(3),
            lease_ttl: Duration::from_secs(30),
            safety_margin: Duration::from_secs(6),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Heartbeat {
    node_epoch: u64,
    seq: u64,
    addr: String,
}

/// State of a lease object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeaseState {
    /// A node owns the database.
    Held,
    /// The last owner gave the database up cleanly.
    Released,
}

/// Contents of `lease.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    /// Held or released.
    pub state: LeaseState,
    /// Owner (or last owner) node id.
    pub node_id: String,
    /// The owner's process epoch when it took the lease.
    pub node_epoch: u64,
    /// Increases by one every time ownership changes.
    pub lease_epoch: u64,
    /// Address other nodes can forward to.
    pub node_addr: String,
}

#[derive(Debug)]
struct NodeInner {
    store: Store,
    node_id: String,
    node_epoch: u64,
    addr: String,
    config: LeaseConfig,
    seq: AtomicU64,
    last_ok: Mutex<(Instant, u64)>,
}

/// A running node identity: owns a heartbeat object and renews it.
///
/// Dropping the last clone stops the renewal task.
#[derive(Debug, Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
    task: Arc<TaskGuard>,
}

#[derive(Debug)]
struct TaskGuard(Arc<AtomicBool>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl NodeInner {
    async fn beat(&self) -> Result<()> {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let body = Heartbeat {
            node_epoch: self.node_epoch,
            seq,
            addr: self.addr.clone(),
        };
        let bytes = serde_json::to_vec(&body).expect("heartbeat serializes");
        self.store.put(&node_key(&self.node_id), bytes).await?;
        *self.last_ok.lock().expect("heartbeat lock") = (Instant::now(), now_ms());
        Ok(())
    }
}

impl Node {
    /// Writes the first heartbeat and starts renewing it in the background.
    ///
    /// `node_id` must be unique among live processes sharing the store.
    pub async fn start(
        store: Store,
        node_id: impl Into<String>,
        addr: impl Into<String>,
        config: LeaseConfig,
    ) -> Result<Self> {
        let inner = Arc::new(NodeInner {
            store,
            node_id: node_id.into(),
            node_epoch: now_ms(),
            addr: addr.into(),
            config,
            seq: AtomicU64::new(0),
            last_ok: Mutex::new((Instant::now(), now_ms())),
        });
        inner.beat().await?;
        let renew = inner.clone();
        // The heartbeat runs on its own thread and runtime. Liveness must not
        // depend on the application's runtime being free: a blocked or busy
        // executor would otherwise look like a dead node.
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::local(format!("heartbeat runtime: {e}")))?;
        std::thread::Builder::new()
            .name(format!("valkvedge-heartbeat-{}", renew.node_id))
            .spawn(move || loop {
                std::thread::sleep(renew.config.heartbeat_interval);
                if stopped.load(Ordering::SeqCst) {
                    break;
                }
                // Bounded, so a store that never answers cannot wedge this thread.
                let limit = renew.config.lease_ttl;
                let beat =
                    runtime.block_on(async { tokio::time::timeout(limit, renew.beat()).await });
                let beat =
                    beat.unwrap_or_else(|_| Err(Error::Unavailable("heartbeat timed out".into())));
                if let Err(e) = beat {
                    tracing::warn!(node = %renew.node_id, error = %e, "heartbeat renewal failed");
                }
            })
            .map_err(|e| Error::local(format!("heartbeat thread: {e}")))?;
        Ok(Node {
            inner,
            task: Arc::new(TaskGuard(stop)),
        })
    }

    /// This node's id.
    pub fn id(&self) -> &str {
        &self.inner.node_id
    }

    /// This process's epoch. Larger for every restart.
    pub fn epoch(&self) -> u64 {
        self.inner.node_epoch
    }

    /// Timing configuration.
    pub fn config(&self) -> &LeaseConfig {
        &self.inner.config
    }

    /// False once the heartbeat is overdue enough that another node could be
    /// about to take this node's leases. A node that is not live must not
    /// write or acknowledge anything.
    ///
    /// Both the monotonic and the wall clock are consulted, because a
    /// suspended machine may not advance the monotonic clock.
    pub fn is_live(&self) -> bool {
        let limit = self
            .inner
            .config
            .lease_ttl
            .saturating_sub(self.inner.config.safety_margin);
        let (mono, wall) = *self.inner.last_ok.lock().expect("heartbeat lock");
        let wall_elapsed = Duration::from_millis(now_ms().saturating_sub(wall));
        mono.elapsed() < limit && wall_elapsed < limit
    }

    /// Stops renewing the heartbeat. Used by tests to simulate a dead node.
    pub fn stop_heartbeat(&self) {
        self.task.0.store(true, Ordering::SeqCst);
    }
}

async fn read_heartbeat(store: &Store, node_id: &str) -> Result<Option<Heartbeat>> {
    match store.get(&node_key(node_id)).await? {
        None => Ok(None),
        Some(v) => Ok(serde_json::from_slice(&v.bytes).ok()),
    }
}

/// A lease this process holds.
#[derive(Debug, Clone)]
pub struct Lease {
    /// The record as written.
    pub record: LeaseRecord,
    etag: String,
    key: String,
}

async fn etag_or_fetch(store: &Store, key: &str, etag: String) -> Result<String> {
    if !etag.is_empty() {
        return Ok(etag);
    }
    store
        .get(key)
        .await?
        .map(|v| v.etag)
        .ok_or_else(|| Error::Unavailable(format!("{key} vanished after write")))
}

/// Acquires the lease for the database at `layout`.
///
/// With `wait` false, a lease held by a node that still looks alive fails
/// immediately with [`Error::LeaseHeld`]. With `wait` true, the call watches
/// the owner's heartbeat for up to `lease_ttl` and takes over if it never
/// changes.
pub async fn acquire(store: &Store, layout: &Layout, node: &Node, wait: bool) -> Result<Lease> {
    let key = layout.lease();
    let mine = |lease_epoch: u64| LeaseRecord {
        state: LeaseState::Held,
        node_id: node.id().to_string(),
        node_epoch: node.epoch(),
        lease_epoch,
        node_addr: node.inner.addr.clone(),
    };
    for _ in 0..8 {
        let Some(current) = store.get(&key).await? else {
            let record = mine(1);
            let bytes = serde_json::to_vec(&record).expect("lease serializes");
            if let PutOutcome::Stored(etag) = store.put_if_absent(&key, bytes).await? {
                let etag = etag_or_fetch(store, &key, etag).await?;
                return Ok(Lease { record, etag, key });
            }
            continue;
        };
        let existing: LeaseRecord = serde_json::from_slice(&current.bytes)
            .map_err(|e| Error::corrupt(format!("lease: {e}")))?;
        let ours = existing.node_id == node.id();
        if existing.state == LeaseState::Held && !ours {
            wait_until_stale(store, &existing, node.config(), wait).await?;
        }
        let record = mine(existing.lease_epoch + 1);
        let bytes = serde_json::to_vec(&record).expect("lease serializes");
        if let PutOutcome::Stored(etag) = store.put_if_match(&key, bytes, &current.etag).await? {
            let etag = etag_or_fetch(store, &key, etag).await?;
            return Ok(Lease { record, etag, key });
        }
    }
    Err(Error::Unavailable(
        "could not acquire lease: too much contention".into(),
    ))
}

async fn wait_until_stale(
    store: &Store,
    lease: &LeaseRecord,
    config: &LeaseConfig,
    wait: bool,
) -> Result<()> {
    let held = || Error::LeaseHeld {
        owner: lease.node_id.clone(),
    };
    let first = read_heartbeat(store, &lease.node_id).await?;
    // The owner restarted: its old leases died with the old process.
    if matches!(&first, Some(hb) if hb.node_epoch > lease.node_epoch) {
        return Ok(());
    }
    if !wait {
        return Err(held());
    }
    let started = Instant::now();
    let poll = config
        .heartbeat_interval
        .min(config.lease_ttl / 4)
        .max(Duration::from_millis(1));
    while started.elapsed() < config.lease_ttl {
        tokio::time::sleep(poll).await;
        let now = read_heartbeat(store, &lease.node_id).await?;
        if matches!(&now, Some(hb) if hb.node_epoch > lease.node_epoch) {
            return Ok(());
        }
        if now != first {
            return Err(held());
        }
    }
    Ok(())
}

impl Lease {
    /// Marks the lease released so another node can take it without waiting.
    /// Fails with [`Error::Fenced`] if the lease is no longer ours.
    pub async fn release(self, store: &Store) -> Result<()> {
        let mut record = self.record;
        record.state = LeaseState::Released;
        let bytes = serde_json::to_vec(&record).expect("lease serializes");
        match store.put_if_match(&self.key, bytes, &self.etag).await? {
            PutOutcome::Stored(_) => Ok(()),
            PutOutcome::ConditionFailed => Err(Error::Fenced),
        }
    }
}

/// Reads the current lease record, if any.
pub async fn read_lease(store: &Store, layout: &Layout) -> Result<Option<LeaseRecord>> {
    match store.get(&layout.lease()).await? {
        None => Ok(None),
        Some(v) => serde_json::from_slice(&v.bytes)
            .map(Some)
            .map_err(|e| Error::corrupt(format!("lease: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast() -> LeaseConfig {
        LeaseConfig {
            heartbeat_interval: Duration::from_millis(20),
            lease_ttl: Duration::from_millis(200),
            safety_margin: Duration::from_millis(50),
        }
    }

    #[tokio::test]
    async fn first_node_acquires_and_second_is_refused() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let a = Node::start(store.clone(), "a", "a:1", fast())
            .await
            .unwrap();
        let b = Node::start(store.clone(), "b", "b:1", fast())
            .await
            .unwrap();

        let lease = acquire(&store, &layout, &a, false).await.unwrap();
        assert_eq!(lease.record.lease_epoch, 1);
        let err = acquire(&store, &layout, &b, false).await.unwrap_err();
        assert_eq!(err.code(), "LEASE_HELD");
        // The owner is alive, so even a waiting acquire is refused.
        let err = acquire(&store, &layout, &b, true).await.unwrap_err();
        assert_eq!(err.code(), "LEASE_HELD");
    }

    #[tokio::test]
    async fn released_lease_is_taken_immediately_with_higher_epoch() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let a = Node::start(store.clone(), "a", "a:1", fast())
            .await
            .unwrap();
        let b = Node::start(store.clone(), "b", "b:1", fast())
            .await
            .unwrap();
        acquire(&store, &layout, &a, false)
            .await
            .unwrap()
            .release(&store)
            .await
            .unwrap();
        let lease = acquire(&store, &layout, &b, false).await.unwrap();
        assert_eq!(lease.record.lease_epoch, 2);
        assert_eq!(lease.record.node_id, "b");
    }

    #[tokio::test]
    async fn dead_owner_is_taken_over_after_ttl_and_old_lease_cannot_release() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let a = Node::start(store.clone(), "a", "a:1", fast())
            .await
            .unwrap();
        let b = Node::start(store.clone(), "b", "b:1", fast())
            .await
            .unwrap();
        let old = acquire(&store, &layout, &a, false).await.unwrap();
        a.stop_heartbeat();

        let started = Instant::now();
        let lease = acquire(&store, &layout, &b, true).await.unwrap();
        assert!(started.elapsed() >= fast().lease_ttl);
        assert_eq!(lease.record.lease_epoch, 2);
        assert_eq!(old.release(&store).await.unwrap_err().code(), "FENCED");
    }

    #[tokio::test]
    async fn restarted_owner_is_taken_over_without_waiting() {
        let store = Store::memory();
        let layout = Layout::new("db");
        let a1 = Node::start(store.clone(), "a", "a:1", fast())
            .await
            .unwrap();
        acquire(&store, &layout, &a1, false).await.unwrap();
        a1.stop_heartbeat();
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Same node id, new process: a newer epoch in the heartbeat object.
        let restarted = Node::start(store.clone(), "a", "a:1", fast())
            .await
            .unwrap();
        assert!(restarted.epoch() > a1.epoch());
        let b = Node::start(store.clone(), "b", "b:1", fast())
            .await
            .unwrap();
        let lease = acquire(&store, &layout, &b, false).await.unwrap();
        assert_eq!(lease.record.node_id, "b");
    }

    #[tokio::test]
    async fn node_stops_being_live_when_heartbeat_stalls() {
        let store = Store::memory();
        let a = Node::start(store, "a", "a:1", fast()).await.unwrap();
        assert!(a.is_live());
        a.stop_heartbeat();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!a.is_live());
    }
}
