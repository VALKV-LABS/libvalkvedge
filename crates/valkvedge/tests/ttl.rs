//! Values that expire: hidden at once on every copy, then deleted by the
//! writer with an ordinary delete.

use object_store::memory::InMemory;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use valkvedge::{object_store, Durability, LeaseConfig, Options, Role, ValkvEdge};

/// Short enough to wait out. Nothing is read before it ends.
const SHORT: Duration = Duration::from_millis(300);

fn options(background: bool) -> Options {
    Options {
        background,
        push_interval: Duration::from_millis(50),
        pull_interval: Duration::from_millis(50),
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
        .options(options(false))
        .object_store(store.clone(), "tenant")
        .role(role)
        .build()
        .unwrap()
}

fn text(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|b| String::from_utf8(b).unwrap())
}

async fn wait_out(ttl: Duration) {
    tokio::time::sleep(ttl + Duration::from_millis(100)).await;
}

#[tokio::test]
async fn an_expired_value_reads_as_absent_everywhere_and_the_writer_deletes_it() {
    let store = Arc::new(InMemory::new());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let writer = client(&wdir, &store, Role::Writer);
    let db = writer.database("app").open().await.unwrap();
    let hour = Duration::from_secs(3600);
    let before = SystemTime::now();
    db.put_with_ttl(b"session", b"abc", hour).await.unwrap(); // 1
    db.put(b"keep", b"1").await.unwrap(); // 2

    let at = db.expires_at(b"session").unwrap().expect("expires");
    assert!(
        at >= before + hour && at <= SystemTime::now() + hour,
        "{at:?}"
    );
    assert_eq!(db.expires_at(b"keep").unwrap(), None);
    let replica = client(&rdir, &store, Role::Replica);
    let follower = replica.database("app").open().await.unwrap();
    assert_eq!(
        text(follower.get(b"session").unwrap()).as_deref(),
        Some("abc")
    );
    assert_eq!(follower.expires_at(b"session").unwrap(), Some(at));

    // A value whose time has passed reads as absent, on the writer and on
    // the replica, before any delete.
    let mut late = db.batch();
    late.put_expiring(b"gone", b"x", SystemTime::now() - Duration::from_secs(1));
    late.commit().await.unwrap(); // 3
    assert_eq!(db.get(b"gone").unwrap(), None);
    assert_eq!(db.expires_at(b"gone").unwrap(), None);
    assert_eq!(follower.sync().await.unwrap(), 3);
    assert_eq!(follower.get(b"gone").unwrap(), None);
    let keys: Vec<Vec<u8>> = db.prefix(b"").unwrap().map(|r| r.unwrap().0).collect();
    assert_eq!(keys, vec![b"keep".to_vec(), b"session".to_vec()]);

    // Then deleted, with a delete in the log that replicas apply.
    assert_eq!(db.expire().await.unwrap(), 1);
    assert_eq!(db.status().unwrap().applied_lsn, 4);
    assert_eq!(db.expire().await.unwrap(), 0, "nothing left to delete");
    assert_eq!(follower.sync().await.unwrap(), 4);
    assert_eq!(
        text(follower.get(b"session").unwrap()).as_deref(),
        Some("abc")
    );
}

#[tokio::test]
async fn a_value_written_again_is_not_deleted_and_an_expired_one_counts_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path())
        .options(options(false))
        .build()
        .unwrap();
    let db = kv.database("app").open().await.unwrap();

    // An expired value is absent to a condition and to update.
    db.put_with_ttl(b"k", b"1", SHORT).await.unwrap();
    wait_out(SHORT).await;
    let mut create = db.batch();
    create.if_absent(b"k").put(b"k", b"2");
    create.commit().await.unwrap();
    assert_eq!(text(db.get(b"k").unwrap()).as_deref(), Some("2"));
    assert_eq!(db.expires_at(b"k").unwrap(), None, "a plain put keeps it");

    // Overwritten before the writer got to it: left alone.
    db.put_with_ttl(b"k2", b"1", SHORT).await.unwrap();
    db.put(b"k2", b"permanent").await.unwrap();
    wait_out(SHORT).await;
    assert_eq!(db.expire().await.unwrap(), 0);
    assert_eq!(text(db.get(b"k2").unwrap()).as_deref(), Some("permanent"));

    // A new expiry replaces the old one.
    db.put_with_ttl(b"k3", b"1", SHORT).await.unwrap();
    db.put_with_ttl(b"k3", b"2", Duration::from_secs(3600))
        .await
        .unwrap();
    wait_out(SHORT).await;
    assert_eq!(db.expire().await.unwrap(), 0);
    assert_eq!(text(db.get(b"k3").unwrap()).as_deref(), Some("2"));
}

#[tokio::test]
async fn the_background_task_deletes_expired_values() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path())
        .options(options(true))
        .build()
        .unwrap();
    let db = kv.database("app").open().await.unwrap();
    db.put_with_ttl(b"k", b"v", SHORT).await.unwrap(); // 1
    let mut deleted = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if db.status().unwrap().applied_lsn == 2 {
            deleted = true;
            break;
        }
    }
    assert!(deleted, "the delete was never written");
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[tokio::test]
async fn expiry_survives_a_snapshot_and_a_buffered_write() {
    let store = Arc::new(InMemory::new());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let writer = client(&wdir, &store, Role::Writer);
    let db = writer.database("app").open().await.unwrap();
    let mut buffered = db.batch();
    buffered.put_with_ttl(b"b", b"1", Duration::from_secs(3600));
    buffered.commit_with(Durability::Buffered).await.unwrap();
    let pending = db.expires_at(b"b").unwrap().expect("pending write expires");
    db.flush().await.unwrap();
    db.put_with_ttl(b"s", b"2", Duration::from_secs(3600))
        .await
        .unwrap();
    let synced = db.expires_at(b"s").unwrap().expect("expires");
    db.compact().await.unwrap();

    // A new replica starts from the snapshot.
    let replica = client(&rdir, &store, Role::Replica);
    let follower = replica.database("app").open().await.unwrap();
    assert_eq!(follower.expires_at(b"b").unwrap(), Some(pending));
    assert_eq!(follower.expires_at(b"s").unwrap(), Some(synced));
}
