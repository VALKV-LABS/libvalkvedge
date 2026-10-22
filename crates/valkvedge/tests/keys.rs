//! Keys that open an encrypted database: a data key sealed for each of
//! them, a recovery key, and adding and removing keys.

use object_store::memory::InMemory;
use std::sync::Arc;
use std::time::Duration;
use valkvedge::{key_from_hex, key_id, object_store, LeaseConfig, Options, Role, ValkvEdge};

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

fn local(dir: &tempfile::TempDir) -> ValkvEdge {
    ValkvEdge::builder(dir.path())
        .options(options())
        .build()
        .unwrap()
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

fn code<T>(result: valkvedge::Result<T>) -> String {
    match result {
        Ok(_) => "ok".into(),
        Err(e) => e.code().to_string(),
    }
}

const KEY: [u8; 32] = [1; 32];
const OTHER: [u8; 32] = [2; 32];

#[tokio::test]
async fn any_of_a_databases_keys_opens_it_and_a_removed_one_no_longer_does() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = local(&dir);
        let db = kv
            .database("vault")
            .encryption_key(KEY)
            .open()
            .await
            .unwrap();
        let recovery = db.take_recovery_key().expect("a new database has one");
        assert_eq!(recovery.len(), 64);
        assert_eq!(db.take_recovery_key(), None, "given out once");
        db.put(b"pin", b"1234").await.unwrap();
        let ids = db.key_ids().await.unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&key_id(&KEY)));
        assert!(ids.contains(&key_id(&key_from_hex(&recovery).unwrap())));

        assert_eq!(db.add_key(OTHER).await.unwrap(), key_id(&OTHER));
        assert_eq!(db.add_key(OTHER).await.unwrap(), key_id(&OTHER), "once");
        assert_eq!(db.key_ids().await.unwrap().len(), 3);
        db.remove_key(&key_id(&KEY)).await.unwrap();
        assert_eq!(code(db.remove_key(&key_id(&KEY)).await), "NOT_FOUND");
        kv.close().await.unwrap();
    }

    // Opened again in a new process.
    let kv = local(&dir);
    assert_eq!(
        code(kv.database("vault").encryption_key(KEY).open().await),
        "INVALID_ENCRYPTION_KEY",
        "removed"
    );
    let db = kv
        .database("vault")
        .encryption_key(OTHER)
        .open()
        .await
        .unwrap();
    assert_eq!(text(db.get(b"pin").unwrap()).as_deref(), Some("1234"));
    assert_eq!(db.take_recovery_key(), None);
    assert_eq!(
        code(kv.database("vault").open().await),
        "INVALID_ENCRYPTION_KEY",
        "no key"
    );
}

#[tokio::test]
async fn the_last_key_cannot_be_removed_and_an_unencrypted_database_has_none() {
    let dir = tempfile::tempdir().unwrap();
    let kv = local(&dir);
    let db = kv
        .database("vault")
        .encryption_key(KEY)
        .open()
        .await
        .unwrap();
    let recovery = key_from_hex(&db.take_recovery_key().unwrap()).unwrap();
    db.remove_key(&key_id(&recovery)).await.unwrap();
    assert_eq!(code(db.remove_key(&key_id(&KEY)).await), "UNSUPPORTED");

    let plain = kv.database("plain").open().await.unwrap();
    assert_eq!(code(plain.add_key(KEY).await), "UNSUPPORTED");
    assert_eq!(code(plain.key_ids().await), "UNSUPPORTED");
    // A database that holds unencrypted data cannot be opened with a key.
    plain.put(b"a", b"1").await.unwrap();
    drop(plain);
    kv.close().await.unwrap();
    let kv = local(&dir);
    assert_eq!(
        code(kv.database("plain").encryption_key(KEY).open().await),
        "INVALID_ENCRYPTION_KEY"
    );
}

#[tokio::test]
async fn a_replica_opens_with_any_key_the_writer_gave_the_database() {
    let store = Arc::new(InMemory::new());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let writer = client(&wdir, &store, Role::Writer);
    let db = writer
        .database("vault")
        .encryption_key(KEY)
        .open()
        .await
        .unwrap();
    let recovery = db.take_recovery_key().expect("a new database has one");
    db.put(b"pin", b"1234").await.unwrap();

    let replica = client(&rdir, &store, Role::Replica);
    let follower = replica
        .database("vault")
        .encryption_key(key_from_hex(&recovery).unwrap())
        .open()
        .await
        .unwrap();
    assert_eq!(text(follower.get(b"pin").unwrap()).as_deref(), Some("1234"));
    assert_eq!(code(follower.add_key(OTHER).await), "READ_ONLY");

    // A key the writer adds later opens it too; one it never added does not.
    db.add_key(OTHER).await.unwrap();
    let late = tempfile::tempdir().unwrap();
    let other = client(&late, &store, Role::Replica);
    other
        .database("vault")
        .encryption_key(OTHER)
        .open()
        .await
        .unwrap();
    let stranger = tempfile::tempdir().unwrap();
    assert_eq!(
        code(
            client(&stranger, &store, Role::Replica)
                .database("vault")
                .encryption_key([9; 32])
                .open()
                .await
        ),
        "INVALID_ENCRYPTION_KEY"
    );
}
