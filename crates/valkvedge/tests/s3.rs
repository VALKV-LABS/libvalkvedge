//! The same behaviour as `replicated.rs`, against a real S3 API.
//!
//! Skipped unless `VALKVEDGE_TEST_S3_BUCKET` is set. `make integ` runs it
//! against an S3 emulator; point the usual `AWS_*` variables at Amazon S3 to
//! run it against the real service.

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use valkvedge::{object_store, Durability, LeaseConfig, Options, Role, ValkvEdge};
use valkvedge_log::{Layout, LogWriter, Node, PutOutcome, Store};

fn bucket() -> Option<Arc<dyn ObjectStore>> {
    let bucket = std::env::var("VALKVEDGE_TEST_S3_BUCKET").ok()?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .expect("S3 configuration");
    Some(Arc::new(store))
}

fn lease() -> LeaseConfig {
    LeaseConfig {
        heartbeat_interval: Duration::from_millis(200),
        lease_ttl: Duration::from_secs(3),
        safety_margin: Duration::from_secs(1),
    }
}

fn unique(label: &str) -> String {
    format!("it-{label}-{}", valkvedge_log::now_ms())
}

fn client(
    dir: &tempfile::TempDir,
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    role: Role,
) -> ValkvEdge {
    ValkvEdge::builder(dir.path())
        .options(Options {
            background: false,
            lease: lease(),
            ..Options::default()
        })
        .object_store(store.clone(), prefix)
        .role(role)
        .build()
        .unwrap()
}

#[tokio::test]
async fn conditional_writes_work_on_this_store() {
    let Some(raw) = bucket() else { return };
    let store = Store::new(raw);
    let key = format!("{}/probe", unique("cond"));

    assert!(store.get(&key).await.unwrap().is_none());
    assert!(matches!(
        store.put_if_absent(&key, b"one".to_vec()).await.unwrap(),
        PutOutcome::Stored(_)
    ));
    assert_eq!(
        store.put_if_absent(&key, b"two".to_vec()).await.unwrap(),
        PutOutcome::ConditionFailed,
        "create-only write must not overwrite"
    );
    let current = store.get(&key).await.unwrap().unwrap();
    assert!(matches!(
        store
            .put_if_match(&key, b"two".to_vec(), &current.etag)
            .await
            .unwrap(),
        PutOutcome::Stored(_)
    ));
    assert_eq!(
        store
            .put_if_match(&key, b"three".to_vec(), &current.etag)
            .await
            .unwrap(),
        PutOutcome::ConditionFailed,
        "compare-and-swap must fail on a stale entity tag"
    );
    assert_eq!(&store.get(&key).await.unwrap().unwrap().bytes[..], b"two");
}

#[tokio::test]
async fn writer_and_replica_end_to_end() {
    let Some(store) = bucket() else { return };
    let prefix = unique("e2e");
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let writer = client(&wdir, &store, &prefix, Role::Writer);
    let db = writer.database("app").open().await.unwrap();
    for i in 0..20u32 {
        db.put(format!("k{i:02}").as_bytes(), b"sync")
            .await
            .unwrap();
    }
    let mut batch = db.batch();
    batch.put(b"buffered", b"yes").delete(b"k00");
    batch.commit_with(Durability::Buffered).await.unwrap();
    assert_eq!(db.flush().await.unwrap(), 1);
    db.compact().await.unwrap();
    db.put(b"after", b"compaction").await.unwrap();

    let replica = client(&rdir, &store, &prefix, Role::Replica);
    let follower = replica.database("app").open().await.unwrap();
    let status = follower.status().unwrap();
    assert_eq!((status.generation, status.applied_lsn), (1, 22));
    assert_eq!(follower.get(b"k00").unwrap(), None);
    assert_eq!(follower.get(b"buffered").unwrap().unwrap(), b"yes");
    assert_eq!(follower.get(b"after").unwrap().unwrap(), b"compaction");
    assert_eq!(follower.range::<&str, _>(..).unwrap().count(), 21);

    db.put(b"later", b"1").await.unwrap();
    assert_eq!(follower.sync().await.unwrap(), 23);
    assert_eq!(follower.get(b"later").unwrap().unwrap(), b"1");
    writer.close().await.unwrap();
}

#[tokio::test]
async fn takeover_fences_the_previous_owner() {
    let Some(raw) = bucket() else { return };
    let store = Store::new(raw);
    let layout = Layout::new(format!("{}/db", unique("fence")));
    let frame = |lsn| valkvedge_log::Frame {
        lsn,
        commit_hlc: Default::default(),
        origin: None,
        ops: Vec::new(),
    };

    let node_a = Node::start(store.clone(), unique("a"), "", lease())
        .await
        .unwrap();
    let mut a = LogWriter::open(store.clone(), layout.clone(), node_a.clone(), false)
        .await
        .unwrap();
    a.append(vec![frame(1)]).await.unwrap();

    // b is refused while a is alive.
    let node_b = Node::start(store.clone(), unique("b"), "", lease())
        .await
        .unwrap();
    let refused = LogWriter::open(store.clone(), layout.clone(), node_b.clone(), false).await;
    assert_eq!(refused.unwrap_err().code(), "LEASE_HELD");

    // a stops renewing; b waits out the TTL and takes over.
    node_a.stop_heartbeat();
    let mut b = LogWriter::open(store.clone(), layout.clone(), node_b, true)
        .await
        .unwrap();
    assert_eq!(b.next_lsn(), 2);
    b.append(vec![frame(2)]).await.unwrap();
    assert!(
        a.append(vec![frame(2)]).await.is_err(),
        "the old owner cannot write"
    );
    assert_eq!(
        valkvedge_sync::remote_head(&store, &layout)
            .await
            .unwrap()
            .1,
        2
    );
}
