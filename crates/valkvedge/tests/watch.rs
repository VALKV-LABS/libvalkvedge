//! Watching a database's local copy for changes.

use object_store::memory::InMemory;
use std::sync::Arc;
use std::time::Duration;
use valkvedge::{
    Durability, Hlc, LeaseConfig, Op, OpKind, Options, OutboxEntry, ReplicaId, Role, ValkvEdge,
    Watch, WatchEvent, WriteOutcome,
};

fn object_options() -> Options {
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

async fn next(watch: &mut Watch) -> WatchEvent {
    tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("an event within 5 s")
        .unwrap()
}

fn changed(key: &str, value: Option<&str>) -> WatchEvent {
    WatchEvent::Changed {
        key: key.as_bytes().to_vec(),
        value: value.map(|v| v.as_bytes().to_vec()),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
async fn writes_under_the_prefix_are_reported_with_their_values() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();
    db.put(b"user/0", b"before").await.unwrap();
    let mut watch = db.watch(b"user/").unwrap();
    assert_eq!(watch.prefix(), b"user/");
    assert!(
        watch.try_next().unwrap().is_none(),
        "earlier writes are not replayed"
    );

    db.put(b"user/1", b"ada").await.unwrap();
    assert_eq!(next(&mut watch).await, changed("user/1", Some("ada")));
    db.put(b"order/1", b"elsewhere").await.unwrap();
    db.delete(b"user/1").await.unwrap();
    assert_eq!(next(&mut watch).await, changed("user/1", None));

    let mut batch = db.batch();
    batch
        .put(b"user/2", b"a")
        .put(b"user/2", b"b")
        .put(b"user/3", b"c");
    batch.commit().await.unwrap();
    assert_eq!(
        next(&mut watch).await,
        changed("user/2", Some("b")),
        "a key written twice in one batch is reported once"
    );
    assert_eq!(next(&mut watch).await, changed("user/3", Some("c")));
    assert!(watch.try_next().unwrap().is_none());

    let mut everything = db.watch(b"").unwrap();
    db.put(b"z", b"1").await.unwrap();
    assert_eq!(next(&mut everything).await, changed("z", Some("1")));
}

#[tokio::test]
async fn values_are_reported_decrypted() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv
        .database("vault")
        .encryption_key([9; 32])
        .open()
        .await
        .unwrap();
    let mut watch = db.watch(b"").unwrap();
    db.put(b"pin", b"1234").await.unwrap();
    assert_eq!(next(&mut watch).await, changed("pin", Some("1234")));
}

#[tokio::test]
async fn a_watcher_that_falls_far_behind_is_told_to_reload() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();
    let mut watch = db.watch(b"").unwrap();
    for i in 0..1100u32 {
        db.put(&i.to_be_bytes(), b"v").await.unwrap();
    }
    assert_eq!(next(&mut watch).await, WatchEvent::Reset);
    db.put(b"after", b"1").await.unwrap();
    // Whatever is still buffered comes first; the new write is reported.
    loop {
        if next(&mut watch).await == changed("after", Some("1")) {
            break;
        }
    }
}

#[tokio::test]
async fn a_watched_database_stays_open_until_the_watch_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();
    let watch = db.watch(b"").unwrap();
    kv.close().await.unwrap();
    assert!(db.is_open());
    drop(watch);
    kv.close().await.unwrap();
    assert!(!db.is_open());
}

#[tokio::test]
async fn a_replica_reports_what_it_pulls() {
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let store = Arc::new(InMemory::new());
    let writer = ValkvEdge::builder(wdir.path())
        .options(object_options())
        .object_store(store.clone(), "tenant")
        .build()
        .unwrap()
        .database("app")
        .open()
        .await
        .unwrap();
    writer.put(b"k", b"1").await.unwrap();
    let replica = ValkvEdge::builder(rdir.path())
        .options(object_options())
        .object_store(store, "tenant")
        .role(Role::Replica)
        .build()
        .unwrap()
        .database("app")
        .open()
        .await
        .unwrap();
    let mut watch = replica.watch(b"").unwrap();
    writer.put(b"k", b"2").await.unwrap();
    assert!(watch.try_next().unwrap().is_none());
    replica.sync().await.unwrap();
    assert_eq!(next(&mut watch).await, changed("k", Some("2")));
}

#[tokio::test]
async fn a_buffered_write_is_reported_at_once_and_again_only_if_it_loses() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path())
        .options(object_options())
        .object_store(Arc::new(InMemory::new()), "tenant")
        .build()
        .unwrap();
    let db = kv
        .database("app")
        .durability(Durability::Buffered)
        .open()
        .await
        .unwrap();
    let mut watch = db.watch(b"").unwrap();

    db.put(b"won", b"mine").await.unwrap();
    db.put(b"lost", b"mine").await.unwrap();
    assert_eq!(next(&mut watch).await, changed("won", Some("mine")));
    assert_eq!(next(&mut watch).await, changed("lost", Some("mine")));

    // Another replica's later write to `lost` reaches the writer first. While
    // this replica's own write is pending, `lost` still reads as it.
    let other = ReplicaId([7; 16]);
    let later = Op {
        kind: OpKind::Put,
        key: b"lost".to_vec(),
        value: b"theirs".to_vec(),
        hlc: Hlc {
            physical_ms: now_ms() + 1000,
            counter: 0,
            replica: other,
        },
    };
    db.accept_push(
        other,
        vec![OutboxEntry {
            seq: 1,
            ops: vec![later],
        }],
    )
    .await
    .unwrap();
    assert!(
        watch.try_next().unwrap().is_none(),
        "hidden by the pending write"
    );

    db.flush().await.unwrap();
    assert_eq!(
        next(&mut watch).await,
        changed("lost", Some("theirs")),
        "the losing write is replaced by the winner"
    );
    assert!(
        watch.try_next().unwrap().is_none(),
        "the write that won reads the same as before: not reported again"
    );
    assert_eq!(db.take_events()[0].outcome, WriteOutcome::Superseded);
}
