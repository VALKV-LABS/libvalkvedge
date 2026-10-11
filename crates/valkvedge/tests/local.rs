//! The embedded store with no object store behind it.

use std::time::Duration;
use valkvedge::{Durability, Options, ValkvEdge};

fn pairs(iter: valkvedge::Iter) -> Vec<(String, String)> {
    iter.map(|r| {
        let (k, v) = r.unwrap();
        (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap())
    })
    .collect()
}

#[tokio::test]
async fn put_get_delete_and_scans() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();

    assert_eq!(db.get(b"missing").unwrap(), None);
    let commit = db.put(b"user:1", b"ada").await.unwrap();
    assert_eq!(commit.lsn, Some(1));
    db.put(b"user:2", b"grace").await.unwrap();
    db.put(b"zone", b"eu").await.unwrap();
    assert_eq!(db.get(b"user:1").unwrap().as_deref(), Some(&b"ada"[..]));

    assert_eq!(
        pairs(db.prefix(b"user:").unwrap()),
        vec![("user:1".into(), "ada".into()), ("user:2".into(), "grace".into())]
    );
    assert_eq!(pairs(db.range("user:2".."zzz").unwrap()).len(), 2);
    assert_eq!(pairs(db.range::<&str, _>(..).unwrap()).len(), 3);

    db.delete(b"user:1").await.unwrap();
    assert_eq!(db.get(b"user:1").unwrap(), None);
    assert_eq!(pairs(db.prefix(b"user:").unwrap()).len(), 1);
    assert_eq!(db.status().unwrap().applied_lsn, 4);
}

#[tokio::test]
async fn a_batch_is_one_atomic_commit() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();
    db.put(b"old", b"x").await.unwrap();

    let mut batch = db.batch();
    batch.put(b"a", b"1").put(b"b", b"2").delete(b"old");
    assert_eq!(batch.len(), 3);
    let commit = batch.commit_with(Durability::Sync).await.unwrap();
    assert_eq!(commit.lsn, Some(2), "three writes, one frame");
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"old").unwrap(), None);
}

#[tokio::test]
async fn data_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = ValkvEdge::builder(dir.path()).build().unwrap();
        let db = kv.database("app").open().await.unwrap();
        db.put(b"k", b"v").await.unwrap();
    }
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("app").open().await.unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(db.status().unwrap().applied_lsn, 1);
}

#[tokio::test]
async fn limits_and_names_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    for bad in ["", "a", "Upper", "1abc", "has space", "../escape", &"x".repeat(49)] {
        let err = kv.database(bad).open().await.err().expect(bad);
        assert_eq!(err.code(), "INVALID_NAME");
    }
    let db = kv.database("app").open().await.unwrap();
    let code = |r: valkvedge::Result<valkvedge::Commit>| r.unwrap_err().code();
    assert_eq!(code(db.put(b"", b"v").await), "KEY_TOO_LARGE");
    assert_eq!(code(db.put(&[b'k'; 1025], b"v").await), "KEY_TOO_LARGE");
    assert_eq!(code(db.put(b"k", &vec![0; 1024 * 1024 + 1]).await), "VALUE_TOO_LARGE");
    let mut batch = db.batch();
    for i in 0..5u8 {
        batch.put(&[i], &vec![0; 1024 * 1024]);
    }
    assert_eq!(code(batch.commit().await), "BATCH_TOO_LARGE");
    assert_eq!(db.status().unwrap().applied_lsn, 0, "nothing was written");
}

#[tokio::test]
async fn databases_are_isolated_from_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let a = kv.database("tenant-a").open().await.unwrap();
    let b = kv.database("tenant-b").open().await.unwrap();
    a.put(b"secret", b"a-only").await.unwrap();
    assert_eq!(b.get(b"secret").unwrap(), None);
    assert_eq!(pairs(b.range::<&str, _>(..).unwrap()).len(), 0);
    assert!(dir.path().join("tenant-a").is_dir());
    assert!(dir.path().join("tenant-b").is_dir());
}

#[tokio::test]
async fn open_handles_stay_bounded_and_evicted_databases_reopen_transparently() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ValkvEdge::builder(dir.path()).max_open_databases(3).build().unwrap();
    let mut handles = Vec::new();
    for i in 0..12 {
        let db = kv.database(&format!("db-{i:02}")).open().await.unwrap();
        db.put(b"id", format!("{i}").as_bytes()).await.unwrap();
        assert!(kv.open_databases() <= 3);
        handles.push(db);
    }
    assert!(kv.evictions() >= 9);
    assert!(!handles[0].is_open(), "the least recently used database was closed");

    // Every handle still works; using it reopens the store.
    for (i, db) in handles.iter().enumerate() {
        assert_eq!(db.get(b"id").unwrap().unwrap(), format!("{i}").as_bytes());
        assert!(kv.open_databases() <= 3);
    }
}

#[tokio::test]
async fn a_database_in_use_is_never_closed_and_the_open_fails_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options {
        max_open_databases: 2,
        open_wait_timeout: Duration::from_millis(100),
        ..Options::default()
    };
    let kv = ValkvEdge::builder(dir.path()).options(options).build().unwrap();
    let a = kv.database("aa").open().await.unwrap();
    let b = kv.database("bb").open().await.unwrap();
    a.put(b"k", b"v").await.unwrap();
    b.put(b"k", b"v").await.unwrap();

    // Two live iterators pin both databases open.
    let iter_a = a.range::<&str, _>(..).unwrap();
    let iter_b = b.range::<&str, _>(..).unwrap();
    let err = kv.database("cc").open().await.err().unwrap();
    assert_eq!(err.code(), "TOO_MANY_OPEN_DATABASES");

    drop(iter_a);
    let c = kv.database("cc").open().await.unwrap();
    c.put(b"k", b"v").await.unwrap();
    assert_eq!(pairs(iter_b).len(), 1, "the pinned iterator is still valid");
}

#[tokio::test]
async fn encrypted_database_rejects_a_wrong_or_missing_key() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = ValkvEdge::builder(dir.path()).build().unwrap();
        let db = kv.database("vault").encryption_key([1; 32]).open().await.unwrap();
        db.put(b"pin", b"1234").await.unwrap();
        assert_eq!(db.get(b"pin").unwrap().as_deref(), Some(&b"1234"[..]));
        assert_eq!(pairs(db.prefix(b"p").unwrap())[0].1, "1234");
    }
    {
        let kv = ValkvEdge::builder(dir.path()).build().unwrap();
        let err = kv.database("vault").encryption_key([2; 32]).open().await.err().unwrap();
        assert_eq!(err.code(), "INVALID_ENCRYPTION_KEY");
    }
    {
        let kv = ValkvEdge::builder(dir.path()).build().unwrap();
        let err = kv.database("vault").open().await.err().unwrap();
        assert_eq!(err.code(), "INVALID_ENCRYPTION_KEY");
    }
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let db = kv.database("vault").encryption_key([1; 32]).open().await.unwrap();
    assert_eq!(db.get(b"pin").unwrap().as_deref(), Some(&b"1234"[..]));
}

#[tokio::test]
async fn a_plaintext_database_cannot_be_reopened_with_a_key() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = ValkvEdge::builder(dir.path()).build().unwrap();
        kv.database("plain").open().await.unwrap().put(b"k", b"v").await.unwrap();
    }
    let kv = ValkvEdge::builder(dir.path()).build().unwrap();
    let err = kv.database("plain").encryption_key([1; 32]).open().await.err().unwrap();
    assert_eq!(err.code(), "INVALID_ENCRYPTION_KEY");
}
