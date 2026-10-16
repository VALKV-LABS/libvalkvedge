//! Conditional writes: a batch applied only if keys are at the versions it
//! names.

use object_store::memory::InMemory;
use std::sync::Arc;
use std::time::Duration;
use valkvedge::{Durability, LeaseConfig, Options, Role, ValkvEdge, ValkvEdgeError, Version};

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

fn text(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|b| String::from_utf8(b).unwrap())
}

fn count(current: Option<&[u8]>) -> Option<Vec<u8>> {
    let n: u64 = current.map_or(0, |v| std::str::from_utf8(v).unwrap().parse().unwrap());
    Some((n + 1).to_string().into_bytes())
}

#[tokio::test]
async fn a_write_conditioned_on_the_version_it_read_succeeds_only_while_nobody_else_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();

    let commit = db.put(b"k", b"1").await.unwrap();
    let (value, version) = db.get_versioned(b"k").unwrap().unwrap();
    assert_eq!(value, b"1");
    assert_eq!(
        commit.versions,
        vec![version],
        "the write reports its version"
    );

    // Someone else writes in between.
    db.put(b"k", b"2").await.unwrap();
    let mut stale = db.batch();
    stale
        .if_version(b"k", &version)
        .put(b"k", b"3")
        .put(b"also", b"x");
    let err = stale.commit().await.unwrap_err();
    assert_eq!(err.code(), "CONDITION_FAILED");
    assert!(matches!(err, ValkvEdgeError::ConditionFailed { ref key } if key == b"k"));
    assert_eq!(text(db.get(b"k").unwrap()).as_deref(), Some("2"));
    assert_eq!(
        db.get(b"also").unwrap(),
        None,
        "nothing in the batch was written"
    );

    let (_, current) = db.get_versioned(b"k").unwrap().unwrap();
    let mut fresh = db.batch();
    fresh.if_version(b"k", &current).put(b"k", b"3");
    fresh.commit().await.unwrap();
    assert_eq!(text(db.get(b"k").unwrap()).as_deref(), Some("3"));
}

#[tokio::test]
async fn if_absent_holds_for_keys_never_written_or_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();

    let mut create = db.batch();
    create.if_absent(b"user/ada").put(b"user/ada", b"1");
    create.commit().await.unwrap();

    let mut again = db.batch();
    again.if_absent(b"user/ada").put(b"user/ada", b"2");
    assert_eq!(again.commit().await.unwrap_err().code(), "CONDITION_FAILED");

    db.delete(b"user/ada").await.unwrap();
    let mut after_delete = db.batch();
    after_delete.if_absent(b"user/ada").put(b"user/ada", b"3");
    after_delete.commit().await.unwrap();
    assert_eq!(text(db.get(b"user/ada").unwrap()).as_deref(), Some("3"));
}

#[tokio::test]
async fn a_version_round_trips_through_text() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();
    db.put(b"k", b"v").await.unwrap();
    let (_, version) = db.get_versioned(b"k").unwrap().unwrap();
    let text = version.to_string();
    assert_eq!(text.len(), 56);
    assert!(text
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    assert_eq!(text.parse::<Version>().unwrap(), version);
    assert!("nope".parse::<Version>().is_err());
    assert!("zz".repeat(28).parse::<Version>().is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_updates_on_one_writer_lose_no_increment() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            tokio::spawn(async move {
                for _ in 0..25 {
                    db.update(b"hits", count).await.unwrap();
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(text(db.get(b"hits").unwrap()).as_deref(), Some("100"));
}

#[tokio::test]
async fn update_can_delete_and_does_nothing_when_there_is_nothing_to_do() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();
    let nothing = db.update(b"k", |_| None).await.unwrap();
    assert_eq!((nothing.lsn, nothing.versions.len()), (None, 0));
    db.update(b"k", count).await.unwrap();
    db.update(b"k", |_| None).await.unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
    assert_eq!(db.status().unwrap().applied_lsn, 2);
}

#[tokio::test]
async fn with_an_object_store_the_writer_checks_conditions_and_stays_usable_after_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let kv = ValkvEdge::builder(dir.path())
        .options(object_options())
        .object_store(store.clone(), "tenant")
        .build()
        .unwrap();
    let db = kv.database("app").open().await.unwrap();

    let first = db.put(b"k", b"1").await.unwrap();
    db.put(b"k", b"2").await.unwrap();
    let mut stale = db.batch();
    stale.if_version(b"k", &first.versions[0]).put(b"k", b"x");
    assert_eq!(stale.commit().await.unwrap_err().code(), "CONDITION_FAILED");
    assert_eq!(
        db.status().unwrap().applied_lsn,
        2,
        "no frame for a failed condition"
    );

    db.update(b"k", |v| Some([v.unwrap(), &b"!"[..]].concat()))
        .await
        .unwrap();
    assert_eq!(text(db.get(b"k").unwrap()).as_deref(), Some("2!"));

    // The log carries the result: a replica sees it.
    let rdir = tempfile::tempdir().unwrap();
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
    assert_eq!(text(replica.get(b"k").unwrap()).as_deref(), Some("2!"));
    let (_, on_replica) = replica.get_versioned(b"k").unwrap().unwrap();
    let (_, on_writer) = db.get_versioned(b"k").unwrap().unwrap();
    assert_eq!(on_replica, on_writer, "versions are the same everywhere");
    let mut from_replica = replica.batch();
    from_replica.if_version(b"k", &on_replica).put(b"k", b"r");
    assert_eq!(from_replica.commit().await.unwrap_err().code(), "READ_ONLY");
}

#[tokio::test]
async fn a_conditional_write_must_be_sync_once_there_is_an_object_store() {
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
    let mut batch = db.batch();
    batch.if_absent(b"k").put(b"k", b"v");
    assert_eq!(batch.commit().await.unwrap_err().code(), "UNSUPPORTED");
    assert_eq!(db.status().unwrap().outbox_entries, 0);

    // update() writes sync whatever the handle's default.
    db.update(b"k", count).await.unwrap();
    assert_eq!(text(db.get(b"k").unwrap()).as_deref(), Some("1"));
}

#[tokio::test]
async fn update_settles_a_pending_buffered_write_to_the_key_first() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path())
        .options(object_options())
        .object_store(Arc::new(InMemory::new()), "tenant")
        .build()
        .unwrap();
    let db = kv.database("app").open().await.unwrap();
    let mut buffered = db.batch();
    buffered.put(b"k", b"5");
    buffered.commit_with(Durability::Buffered).await.unwrap();
    db.update(b"k", count).await.unwrap();
    assert_eq!(text(db.get(b"k").unwrap()).as_deref(), Some("6"));
    assert_eq!(db.status().unwrap().outbox_entries, 0);
}

#[tokio::test]
async fn encrypted_values_can_be_updated() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv
        .database("vault")
        .encryption_key([3; 32])
        .open()
        .await
        .unwrap();
    db.update(b"n", count).await.unwrap();
    db.update(b"n", count).await.unwrap();
    let (value, _) = db.get_versioned(b"n").unwrap().unwrap();
    assert_eq!(value, b"2");
}
