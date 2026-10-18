//! The embedded store with an object store behind it: a writer and replicas.

use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use valkvedge::{object_store, Durability, LeaseConfig, Options, Role, ValkvEdge, WriteOutcome};

fn options() -> Options {
    Options {
        background: false,
        lease: LeaseConfig {
            heartbeat_interval: Duration::from_millis(20),
            lease_ttl: Duration::from_millis(300),
            safety_margin: Duration::from_millis(100),
        },
        ..Options::default()
    }
}

fn client(dir: &tempfile::TempDir, store: &Arc<InMemory>, role: Role) -> ValkvEdge {
    ValkvEdge::builder(dir.path())
        .options(options())
        .object_store(store.clone(), "tenant")
        .role(role)
        .build()
        .unwrap()
}

fn text(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|b| String::from_utf8(b).unwrap())
}

async fn object_count(store: &Arc<InMemory>) -> usize {
    use futures::TryStreamExt;
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    listed.len()
}

async fn all_bytes(store: &Arc<InMemory>) -> Vec<u8> {
    use futures::TryStreamExt;
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    let mut out = Vec::new();
    for meta in listed {
        out.extend_from_slice(
            &store
                .get(&meta.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        );
    }
    out
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[tokio::test]
async fn replica_cold_boots_then_follows_the_writer() {
    let store = Arc::new(InMemory::new());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let writer = client(&wdir, &store, Role::Writer);
    let db = writer.database("app").open().await.unwrap();
    assert_eq!(db.put(b"a", b"1").await.unwrap().lsn, Some(1));

    let replica = client(&rdir, &store, Role::Replica);
    let follower = replica.database("app").open().await.unwrap();
    assert_eq!(text(follower.get(b"a").unwrap()).as_deref(), Some("1"));

    db.put(b"b", b"2").await.unwrap();
    assert_eq!(follower.get(b"b").unwrap(), None, "not pulled yet");
    assert_eq!(follower.remote_lsn().await.unwrap(), Some(2));
    assert_eq!(follower.sync().await.unwrap(), 2);
    assert_eq!(text(follower.get(b"b").unwrap()).as_deref(), Some("2"));

    let err = follower.put(b"c", b"3").await.unwrap_err();
    assert_eq!(err.code(), "READ_ONLY");
}

#[tokio::test]
async fn opening_a_replica_of_a_missing_database_is_not_found() {
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let replica = client(&dir, &store, Role::Replica);
    let err = replica.database("ghost").open().await.err().unwrap();
    assert_eq!(err.code(), "NOT_FOUND");
}

#[tokio::test]
async fn a_replica_opens_and_reads_offline_once_it_has_a_local_copy() {
    let store = Arc::new(InMemory::new());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    client(&wdir, &store, Role::Writer)
        .database("app")
        .open()
        .await
        .unwrap()
        .put(b"k", b"v")
        .await
        .unwrap();
    client(&rdir, &store, Role::Replica)
        .database("app")
        .open()
        .await
        .unwrap();

    // Same local directory, but an object store that has nothing in it:
    // any network access would fail with NOT_FOUND.
    let empty = Arc::new(InMemory::new());
    let offline = client(&rdir, &empty, Role::Replica);
    let db = offline.database("app").open().await.unwrap();
    assert_eq!(text(db.get(b"k").unwrap()).as_deref(), Some("v"));
}

#[tokio::test]
async fn buffered_writes_are_readable_at_once_and_need_no_network() {
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let kv = client(&dir, &store, Role::Writer);
    let db = kv
        .database("telemetry")
        .durability(Durability::Buffered)
        .open()
        .await
        .unwrap();

    let commit = db.put(b"t1", b"21.5").await.unwrap();
    assert_eq!((commit.lsn, commit.outbox_seq), (None, Some(1)));
    db.put(b"t2", b"22.0").await.unwrap();
    assert_eq!(text(db.get(b"t1").unwrap()).as_deref(), Some("21.5"));
    assert_eq!(db.prefix(b"t").unwrap().count(), 2);
    assert_eq!(object_count(&store).await, 0, "nothing was sent anywhere");

    let status = db.status().unwrap();
    assert_eq!((status.outbox_entries, status.applied_lsn), (2, 0));

    assert_eq!(db.flush().await.unwrap(), 2);
    let status = db.status().unwrap();
    assert_eq!(
        (
            status.outbox_entries,
            status.outbox_bytes,
            status.applied_lsn
        ),
        (0, 0, 2)
    );
    assert_eq!(text(db.get(b"t1").unwrap()).as_deref(), Some("21.5"));
    assert!(db.take_events().is_empty());

    let rdir = tempfile::tempdir().unwrap();
    let follower = client(&rdir, &store, Role::Replica)
        .database("telemetry")
        .open()
        .await
        .unwrap();
    assert_eq!(text(follower.get(b"t2").unwrap()).as_deref(), Some("22.0"));
}

#[tokio::test]
async fn pending_buffered_writes_overlay_replicated_data_in_reads_and_scans() {
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let db = client(&dir, &store, Role::Writer)
        .database("app")
        .open()
        .await
        .unwrap();
    db.put(b"a", b"synced").await.unwrap();
    db.put(b"b", b"synced").await.unwrap();
    db.put(b"c", b"synced").await.unwrap();

    let mut batch = db.batch();
    batch
        .put(b"a", b"pending")
        .delete(b"b")
        .put(b"d", b"pending");
    batch.commit_with(Durability::Buffered).await.unwrap();

    assert_eq!(text(db.get(b"a").unwrap()).as_deref(), Some("pending"));
    assert_eq!(db.get(b"b").unwrap(), None);
    let seen: Vec<_> = db
        .range::<&str, _>(..)
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap())
        })
        .collect();
    assert_eq!(
        seen,
        vec![
            ("a".to_string(), "pending".to_string()),
            ("c".to_string(), "synced".to_string()),
            ("d".to_string(), "pending".to_string()),
        ]
    );

    db.flush().await.unwrap();
    assert_eq!(text(db.get(b"a").unwrap()).as_deref(), Some("pending"));
    assert_eq!(db.get(b"b").unwrap(), None);
    assert_eq!(db.range::<&str, _>(..).unwrap().count(), 3);
}

fn writer_at(dir: &tempfile::TempDir, store: &Arc<InMemory>, id: &str, addr: &str) -> ValkvEdge {
    ValkvEdge::builder(dir.path())
        .options(Options {
            node_id: Some(id.to_string()),
            node_addr: addr.to_string(),
            // Do not wait when someone else owns a database: fail at once.
            wait_for_lease: false,
            ..options()
        })
        .object_store(store.clone(), "tenant")
        .build()
        .unwrap()
}

#[tokio::test]
async fn the_owner_and_its_address_are_visible_and_a_live_owner_is_not_displaced() {
    let store = Arc::new(InMemory::new());
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = writer_at(&dir_a, &store, "node-a", "10.0.0.1:7000");
    let b = writer_at(&dir_b, &store, "node-b", "10.0.0.2:7000");
    let on_a = a.database("app").open().await.unwrap();
    let on_b = b.database("app").open().await.unwrap();
    assert_eq!(on_b.owner().await.unwrap(), None, "nobody owns it yet");

    on_a.put(b"k", b"1").await.unwrap();
    let owner = on_b.owner().await.unwrap().unwrap();
    assert_eq!(
        (owner.node_id.as_str(), owner.node_addr.as_str()),
        ("node-a", "10.0.0.1:7000")
    );

    // b is told at once, and even an explicit take-over is refused while a lives.
    assert_eq!(on_b.put(b"k", b"2").await.unwrap_err().code(), "LEASE_HELD");
    assert_eq!(on_b.take_over().await.unwrap_err().code(), "LEASE_HELD");
    assert_eq!(text(on_a.get(b"k").unwrap()).as_deref(), Some("1"));

    // After a clean close the lease is released: nobody owns it.
    a.close().await.unwrap();
    assert_eq!(on_b.owner().await.unwrap(), None);
    on_b.put(b"k", b"3").await.unwrap();
    assert_eq!(on_b.owner().await.unwrap().unwrap().node_id, "node-b");
}

#[tokio::test]
async fn take_over_claims_a_database_whose_owner_died() {
    let store = Arc::new(InMemory::new());
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    {
        // Process a dies without releasing: its runtime and heartbeat stop.
        let store = store.clone();
        let path = dir_a.path().to_path_buf();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let a = ValkvEdge::builder(path)
                    .options(Options {
                        node_id: Some("node-a".into()),
                        ..options()
                    })
                    .object_store(store, "tenant")
                    .build()
                    .unwrap();
                a.database("app")
                    .open()
                    .await
                    .unwrap()
                    .put(b"k", b"acked")
                    .await
                    .unwrap();
            });
        })
        .join()
        .unwrap();
    }
    let b = writer_at(&dir_b, &store, "node-b", "10.0.0.2:7000");
    let db = b.database("app").open().await.unwrap();
    // Without waiting, the stale lease still looks held.
    assert_eq!(db.put(b"k2", b"x").await.unwrap_err().code(), "LEASE_HELD");

    let started = std::time::Instant::now();
    db.take_over().await.unwrap();
    assert!(
        started.elapsed() >= options().lease.lease_ttl,
        "it watched for a full TTL"
    );
    assert_eq!(
        text(db.get(b"k").unwrap()).as_deref(),
        Some("acked"),
        "caught up on take-over"
    );
    assert_eq!(db.put(b"k2", b"after").await.unwrap().lsn, Some(2));
    assert_eq!(db.owner().await.unwrap().unwrap().node_id, "node-b");
}

#[tokio::test]
async fn namespaced_databases_have_their_own_prefix_and_replicate() {
    use futures::TryStreamExt;
    let store = Arc::new(InMemory::new());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let writer = client(&wdir, &store, Role::Writer);
    let a = writer
        .namespace("acme")
        .database("orders")
        .open()
        .await
        .unwrap();
    let b = writer
        .namespace("globex")
        .database("orders")
        .open()
        .await
        .unwrap();
    a.put(b"k", b"acme").await.unwrap();
    b.put(b"k", b"globex").await.unwrap();
    assert_eq!(a.object_prefix().as_deref(), Some("tenant/acme/orders"));

    let keys: Vec<String> = store
        .list(None)
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap();
    assert!(keys
        .iter()
        .any(|k| k.starts_with("tenant/acme/orders/gen/")));
    assert!(keys
        .iter()
        .any(|k| k.starts_with("tenant/globex/orders/gen/")));
    assert!(keys.iter().all(|k| k.starts_with("tenant/acme/")
        || k.starts_with("tenant/globex/")
        || k.starts_with("_nodes/")));

    let replica = client(&rdir, &store, Role::Replica);
    let follower = replica
        .namespace("globex")
        .database("orders")
        .open()
        .await
        .unwrap();
    assert_eq!(text(follower.get(b"k").unwrap()).as_deref(), Some("globex"));
}

#[tokio::test]
async fn the_writer_accepts_pushed_entries_from_other_replicas() {
    use valkvedge::{Hlc, Op, OpKind, OpOutcome, OutboxEntry, ReplicaId};
    let store = Arc::new(InMemory::new());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = client(&wdir, &store, Role::Writer)
        .database("app")
        .open()
        .await
        .unwrap();
    db.put(b"title", b"from-writer").await.unwrap();

    let phone = ReplicaId([7; 16]);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let op = |key: &str, value: &str, ms: u64| Op {
        kind: OpKind::Put,
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        hlc: Hlc {
            physical_ms: ms,
            counter: 0,
            replica: phone,
        },
    };
    let entries = vec![OutboxEntry {
        seq: 1,
        ops: vec![
            op("title", "stale-from-phone", now - 60_000),
            op("note", "from-phone", now - 60_000),
        ],
    }];

    let report = db.accept_push(phone, entries.clone()).await.unwrap();
    assert_eq!(
        report.entries,
        vec![(1, vec![OpOutcome::Superseded, OpOutcome::Applied])]
    );
    assert_eq!(report.watermark, 1);
    assert_eq!(
        text(db.get(b"title").unwrap()).as_deref(),
        Some("from-writer")
    );
    assert_eq!(
        text(db.get(b"note").unwrap()).as_deref(),
        Some("from-phone")
    );

    // The phone retries the same push: nothing is applied twice.
    let retry = db.accept_push(phone, entries).await.unwrap();
    assert_eq!(retry.entries[0].1, vec![OpOutcome::Duplicate; 2]);
    assert_eq!(retry.lsn, report.lsn);

    // A replica cannot accept pushes.
    let follower = client(&rdir, &store, Role::Replica)
        .database("app")
        .open()
        .await
        .unwrap();
    assert_eq!(
        text(follower.get(b"note").unwrap()).as_deref(),
        Some("from-phone")
    );
    let err = follower.accept_push(phone, Vec::new()).await.unwrap_err();
    assert_eq!(err.code(), "READ_ONLY");
}

#[tokio::test]
async fn a_buffered_write_made_after_a_sync_write_always_wins() {
    // Many rounds in quick succession, so writes land in the same millisecond.
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let db = client(&dir, &store, Role::Writer)
        .database("app")
        .open()
        .await
        .unwrap();
    for round in 0..200u32 {
        let (first, second) = (format!("sync-{round}"), format!("buffered-{round}"));
        db.put(b"k", first.as_bytes()).await.unwrap();
        let mut batch = db.batch();
        batch.put(b"k", second.as_bytes());
        batch.commit_with(Durability::Buffered).await.unwrap();
        db.flush().await.unwrap();
        assert_eq!(text(db.get(b"k").unwrap()), Some(second), "round {round}");
    }
    assert!(db.take_events().is_empty(), "no later write was superseded");
}

#[tokio::test]
async fn a_buffered_write_beats_a_newer_looking_value_this_replica_already_holds() {
    // The writer process restarts between the two writes, so each is stamped
    // by a fresh clock. The second must still be the later one.
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    for round in 0..20u32 {
        let kv = client(&dir, &store, Role::Writer);
        let db = kv.database("app").open().await.unwrap();
        db.put(b"k", b"sync").await.unwrap();
        kv.close().await.unwrap();
        drop((db, kv));

        let kv = client(&dir, &store, Role::Writer);
        let db = kv.database("app").open().await.unwrap();
        let mut batch = db.batch();
        batch.put(b"k", b"buffered");
        batch.commit_with(Durability::Buffered).await.unwrap();
        db.flush().await.unwrap();
        assert_eq!(
            text(db.get(b"k").unwrap()).as_deref(),
            Some("buffered"),
            "round {round}"
        );
        assert!(db.take_events().is_empty());
        kv.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_buffered_write_that_loses_a_conflict_is_reported() {
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let db = client(&dir, &store, Role::Writer)
        .database("app")
        .open()
        .await
        .unwrap();

    let mut batch = db.batch();
    batch.put(b"title", b"draft").put(b"body", b"draft");
    batch.commit_with(Durability::Buffered).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    // A sync write lands later, before the buffered one is pushed.
    let mut later = db.batch();
    later.put(b"title", b"final");
    later.commit_with(Durability::Sync).await.unwrap();

    // Until the push settles it, the replica still reads its own pending write.
    assert_eq!(text(db.get(b"title").unwrap()).as_deref(), Some("draft"));
    db.flush().await.unwrap();
    assert_eq!(text(db.get(b"title").unwrap()).as_deref(), Some("final"));
    assert_eq!(text(db.get(b"body").unwrap()).as_deref(), Some("draft"));

    let events = db.take_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key, b"title");
    assert_eq!(events[0].outcome, WriteOutcome::Superseded);
    assert!(db.take_events().is_empty());
}

#[tokio::test]
async fn the_outbox_survives_a_restart_and_is_pushed_exactly_once() {
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = client(&dir, &store, Role::Writer);
        let db = kv
            .database("app")
            .durability(Durability::Buffered)
            .open()
            .await
            .unwrap();
        db.put(b"k", b"v").await.unwrap();
        kv.close().await.unwrap();
    }
    let kv = client(&dir, &store, Role::Writer);
    let db = kv.database("app").open().await.unwrap();
    assert_eq!(db.status().unwrap().outbox_entries, 1);
    assert_eq!(text(db.get(b"k").unwrap()).as_deref(), Some("v"));
    assert_eq!(db.flush().await.unwrap(), 1);
    assert_eq!(db.flush().await.unwrap(), 0);
    assert_eq!(db.remote_lsn().await.unwrap(), Some(1));
}

#[tokio::test]
async fn the_outbox_is_bounded() {
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path())
        .options(Options {
            outbox_max_bytes: 4096,
            push_bytes: u64::MAX,
            ..options()
        })
        .object_store(store.clone(), "tenant")
        .build()
        .unwrap();
    let db = kv
        .database("app")
        .durability(Durability::Buffered)
        .open()
        .await
        .unwrap();
    let mut refused = None;
    for i in 0..100u32 {
        if let Err(e) = db.put(&i.to_be_bytes(), &[0; 512]).await {
            refused = Some(e);
            break;
        }
    }
    assert_eq!(refused.expect("outbox filled up").code(), "OUTBOX_FULL");
    db.flush().await.unwrap();
    db.put(b"again", b"ok").await.unwrap();
}

#[tokio::test]
async fn background_task_pushes_the_outbox_on_its_own() {
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path())
        .options(Options {
            background: true,
            push_interval: Duration::from_millis(50),
            ..options()
        })
        .object_store(store.clone(), "tenant")
        .build()
        .unwrap();
    let db = kv
        .database("app")
        .durability(Durability::Buffered)
        .open()
        .await
        .unwrap();
    db.put(b"k", b"v").await.unwrap();
    for _ in 0..100 {
        if db.status().unwrap().outbox_entries == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(db.status().unwrap().outbox_entries, 0);
    assert_eq!(db.remote_lsn().await.unwrap(), Some(1));
}

#[tokio::test]
async fn compaction_lets_a_new_replica_boot_from_the_snapshot() {
    let store = Arc::new(InMemory::new());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = client(&wdir, &store, Role::Writer)
        .database("app")
        .open()
        .await
        .unwrap();
    for i in 0..50u32 {
        db.put(format!("k{i:03}").as_bytes(), b"v").await.unwrap();
    }
    db.delete(b"k000").await.unwrap();
    db.compact().await.unwrap();
    db.put(b"after", b"compaction").await.unwrap();
    assert_eq!(db.status().unwrap().generation, 1);

    let follower = client(&rdir, &store, Role::Replica)
        .database("app")
        .open()
        .await
        .unwrap();
    let status = follower.status().unwrap();
    assert_eq!((status.generation, status.applied_lsn), (1, 52));
    assert_eq!(follower.range::<&str, _>(..).unwrap().count(), 50);
    assert_eq!(follower.get(b"k000").unwrap(), None);
    assert_eq!(
        text(follower.get(b"after").unwrap()).as_deref(),
        Some("compaction")
    );
}

#[tokio::test]
async fn a_second_process_takes_over_after_a_clean_close_without_waiting() {
    let store = Arc::new(InMemory::new());
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = client(&dir_a, &store, Role::Writer);
    a.database("app")
        .open()
        .await
        .unwrap()
        .put(b"from", b"a")
        .await
        .unwrap();
    a.close().await.unwrap();

    let b = client(&dir_b, &store, Role::Writer);
    let db = b.database("app").open().await.unwrap();
    // The lease is taken on the first write, so that is what is timed.
    let started = std::time::Instant::now();
    assert_eq!(db.put(b"also", b"b").await.unwrap().lsn, Some(2));
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "no lease wait after a clean close"
    );
    assert_eq!(
        text(db.get(b"from").unwrap()).as_deref(),
        Some("a"),
        "b caught up before writing"
    );
}

#[tokio::test]
async fn a_second_process_takes_over_from_a_dead_one_and_loses_nothing() {
    let store = Arc::new(InMemory::new());
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    {
        // Process a dies: its runtime, and so its heartbeat, stops.
        let store = store.clone();
        let path = dir_a.path().to_path_buf();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let a = ValkvEdge::builder(path)
                    .options(options())
                    .object_store(store, "tenant")
                    .build()
                    .unwrap();
                let db = a.database("app").open().await.unwrap();
                for i in 0..5u8 {
                    db.put(&[b'k', i], b"acked").await.unwrap();
                }
            });
        })
        .join()
        .unwrap();
    }
    let b = client(&dir_b, &store, Role::Writer);
    let db = b.database("app").open().await.unwrap();
    assert_eq!(db.put(b"next", b"b").await.unwrap().lsn, Some(6));
    for i in 0..5u8 {
        assert_eq!(text(db.get(&[b'k', i]).unwrap()).as_deref(), Some("acked"));
    }
}

#[tokio::test]
async fn eviction_does_not_break_a_writer() {
    let store = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path())
        .options(Options {
            max_open_databases: 1,
            ..options()
        })
        .object_store(store.clone(), "tenant")
        .build()
        .unwrap();
    let a = kv.database("aa").open().await.unwrap();
    let b = kv.database("bb").open().await.unwrap();
    for round in 0..3u8 {
        a.put(b"k", &[round]).await.unwrap();
        b.put(b"k", &[round]).await.unwrap();
        assert_eq!(kv.open_databases(), 1);
    }
    assert_eq!(a.get(b"k").unwrap().unwrap(), vec![2]);
    assert_eq!(a.remote_lsn().await.unwrap(), Some(3));
    assert!(kv.evictions() >= 5);
}

#[tokio::test]
async fn encrypted_values_never_reach_the_object_store_in_clear() {
    let store = Arc::new(InMemory::new());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let writer = client(&wdir, &store, Role::Writer);
    let db = writer
        .database("vault")
        .encryption_key([9; 32])
        .open()
        .await
        .unwrap();
    db.put(b"card-number", b"4111-1111-1111-1111")
        .await
        .unwrap();
    let mut buffered = db.batch();
    buffered.put(b"cvv", b"TOP-SECRET-737");
    buffered.commit_with(Durability::Buffered).await.unwrap();
    db.flush().await.unwrap();
    db.compact().await.unwrap();

    let stored = all_bytes(&store).await;
    assert!(!contains(&stored, b"4111-1111"));
    assert!(!contains(&stored, b"TOP-SECRET"));
    assert!(
        contains(&stored, b"card-number"),
        "keys are not encrypted, by design"
    );

    let replica = client(&rdir, &store, Role::Replica);
    let follower = replica
        .database("vault")
        .encryption_key([9; 32])
        .open()
        .await
        .unwrap();
    assert_eq!(
        text(follower.get(b"cvv").unwrap()).as_deref(),
        Some("TOP-SECRET-737")
    );

    for wrong in [Some([1; 32]), None] {
        let dir = tempfile::tempdir().unwrap();
        let other = client(&dir, &store, Role::Replica);
        let mut builder = other.database("vault");
        if let Some(key) = wrong {
            builder = builder.encryption_key(key);
        }
        assert_eq!(
            builder.open().await.err().unwrap().code(),
            "INVALID_ENCRYPTION_KEY"
        );
    }
}

#[tokio::test]
async fn a_partial_replica_keeps_only_its_prefixes_through_frames_and_snapshots() {
    let store = Arc::new(InMemory::new());
    let [wdir, rdir, ldir] = [(); 3].map(|_| tempfile::tempdir().unwrap());
    let writer = client(&wdir, &store, Role::Writer);
    let db = writer.database("app").open().await.unwrap();
    db.put(b"team/1", b"a").await.unwrap();
    db.put(b"other/1", b"b").await.unwrap();

    let replica = client(&rdir, &store, Role::Replica);
    let partial = replica
        .database("app")
        .prefixes(["team/"])
        .open()
        .await
        .unwrap();
    assert_eq!(text(partial.get(b"team/1").unwrap()).as_deref(), Some("a"));
    assert_eq!(
        partial.get(b"other/1").unwrap_err().code(),
        "NOT_REPLICATED"
    );

    db.put(b"team/2", b"c").await.unwrap();
    db.put(b"other/2", b"d").await.unwrap();
    assert_eq!(partial.sync().await.unwrap(), 4);
    let keys: Vec<Vec<u8>> = partial
        .prefix(b"team/")
        .unwrap()
        .map(|e| e.unwrap().0)
        .collect();
    assert_eq!(keys, vec![b"team/1".to_vec(), b"team/2".to_vec()]);

    // Loaded from a snapshot, the same.
    db.compact().await.unwrap();
    let late = client(&ldir, &store, Role::Replica)
        .database("app")
        .prefixes(["other/"])
        .open()
        .await
        .unwrap();
    assert_eq!(late.prefix(b"other/").unwrap().count(), 2);
    assert_eq!(late.get(b"team/1").unwrap_err().code(), "NOT_REPLICATED");

    // The writer holds everything: it cannot be partial.
    let err = client(&tempfile::tempdir().unwrap(), &store, Role::Writer)
        .database("app")
        .prefixes(["team/"])
        .open()
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "UNSUPPORTED");
}
