//! A database's history: states at past points, restores, and what is kept.

use std::sync::Arc;
use std::time::Duration;
use valkvedge_log::{
    now_ms, read_manifest, HlcClock, Layout, LeaseConfig, Node, OpKind, ReplicaId, Store,
};
use valkvedge_sync::{
    history, pull, state_at, Change, MemState, Point, StateStore, Writer, WriterConfig,
};

fn fast() -> LeaseConfig {
    LeaseConfig {
        heartbeat_interval: Duration::from_millis(20),
        lease_ttl: Duration::from_millis(200),
        safety_margin: Duration::from_millis(50),
    }
}

fn layout() -> Layout {
    Layout::new("db")
}

async fn writer(store: &Store, history: Duration) -> Writer<MemState> {
    let node = Node::start(store.clone(), "a", "addr", fast())
        .await
        .unwrap();
    Writer::open(
        store.clone(),
        layout(),
        node,
        Arc::new(MemState::new()),
        Arc::new(HlcClock::new(ReplicaId::random())),
        WriterConfig {
            history,
            generation_grace: Duration::ZERO,
            ..WriterConfig::default()
        },
        false,
    )
    .await
    .unwrap()
}

fn put(key: &str, value: &str) -> Change {
    Change {
        kind: OpKind::Put,
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

fn del(key: &str) -> Change {
    Change {
        kind: OpKind::Delete,
        key: key.as_bytes().to_vec(),
        value: vec![],
    }
}

fn text(state: &MemState, key: &str) -> Option<String> {
    state
        .get(key.as_bytes())
        .map(|v| String::from_utf8(v).unwrap())
}

/// Every snapshot part in the store.
async fn snapshot_parts(store: &Store) -> Vec<String> {
    store
        .list("db/gen/")
        .await
        .unwrap()
        .into_iter()
        .filter(|key| key.contains("/snapshot/"))
        .collect()
}

#[tokio::test]
async fn the_state_at_any_point_is_rebuilt_across_generations() {
    let store = Store::memory();
    let w = writer(&store, Duration::from_secs(3600)).await;
    w.commit(vec![put("a", "1")]).await.unwrap(); // 1
    w.commit(vec![put("b", "1")]).await.unwrap(); // 2
    w.compact().await.unwrap();
    let between = now_ms();
    tokio::time::sleep(Duration::from_millis(5)).await;
    w.commit(vec![put("a", "2"), del("b")]).await.unwrap(); // 3
    w.compact().await.unwrap();
    w.commit(vec![put("c", "1")]).await.unwrap(); // 4

    let scratch = MemState::new();
    assert_eq!(
        state_at(&store, &layout(), Point::Lsn(1), &scratch)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        (text(&scratch, "a").as_deref(), text(&scratch, "b")),
        (Some("1"), None)
    );

    assert_eq!(
        state_at(&store, &layout(), Point::Lsn(3), &scratch)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        (text(&scratch, "a").as_deref(), text(&scratch, "b")),
        (Some("2"), None)
    );

    assert_eq!(
        state_at(&store, &layout(), Point::Time(between), &scratch)
            .await
            .unwrap(),
        2
    );
    assert_eq!(text(&scratch, "b").as_deref(), Some("1"));

    assert_eq!(
        state_at(&store, &layout(), Point::Time(now_ms()), &scratch)
            .await
            .unwrap(),
        4
    );
    assert_eq!(text(&scratch, "c").as_deref(), Some("1"));

    let reach = history(&store, &layout()).await.unwrap();
    assert_eq!(
        (reach.from_lsn, reach.from_ms, reach.to_lsn),
        (0, Some(0), 4)
    );
    let err = state_at(&store, &layout(), Point::Lsn(5), &scratch)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "NOT_IN_HISTORY");
}

#[tokio::test]
async fn only_the_base_snapshot_is_kept_and_without_history_nothing_changes() {
    let store = Store::memory();
    let w = writer(&store, Duration::from_secs(3600)).await;
    for i in 0..3 {
        w.commit(vec![put(&format!("k{i}"), "v")]).await.unwrap();
        w.compact().await.unwrap();
    }
    // Generations 1 to 3 have snapshots; 1 and 2 are replaced and not the
    // base (generation 0, empty), so theirs are gone. Every log is kept.
    let parts = snapshot_parts(&store).await;
    assert!(
        parts.iter().all(|p| p.contains("gen/0000000003/")),
        "{parts:?}"
    );
    let (manifest, _) = read_manifest(&store, &layout()).await.unwrap().unwrap();
    assert_eq!(manifest.superseded.len(), 3);

    let plain = Store::memory();
    let w = writer(&plain, Duration::ZERO).await;
    for i in 0..3 {
        w.commit(vec![put(&format!("k{i}"), "v")]).await.unwrap();
        w.compact().await.unwrap();
    }
    // No history and no grace: replaced generations go at once.
    let (manifest, _) = read_manifest(&plain, &layout()).await.unwrap().unwrap();
    assert!(manifest.superseded.is_empty());
}

#[tokio::test]
async fn a_restore_is_a_new_generation_replicas_reload_and_it_can_be_undone() {
    let store = Store::memory();
    let w = writer(&store, Duration::from_secs(3600)).await;
    w.commit(vec![put("a", "good")]).await.unwrap(); // 1
    w.commit(vec![put("a", "bad"), put("b", "bad")])
        .await
        .unwrap(); // 2

    let replica = MemState::new();
    pull(&store, &layout(), &replica).await.unwrap();
    assert_eq!(text(&replica, "a").as_deref(), Some("bad"));

    let scratch = Arc::new(MemState::new());
    state_at(&store, &layout(), Point::Lsn(1), scratch.as_ref())
        .await
        .unwrap();
    let pos = w.restore(scratch, true).await.unwrap();
    assert_eq!(pos.applied_lsn, 3, "one past the tail");
    assert_eq!(text(w.state(), "a").as_deref(), Some("good"));
    assert_eq!(text(w.state(), "b"), None);

    // The replica cannot get there from the log: it reloads.
    let outcome = pull(&store, &layout(), &replica).await.unwrap();
    assert!(outcome.bootstrapped);
    assert_eq!(text(&replica, "a").as_deref(), Some("good"));
    assert_eq!(text(&replica, "b"), None);

    // Writes go on after it, and the restore itself can be undone.
    w.commit(vec![put("c", "after")]).await.unwrap(); // 4
    let scratch = Arc::new(MemState::new());
    state_at(&store, &layout(), Point::Lsn(2), scratch.as_ref())
        .await
        .unwrap();
    w.restore(scratch, true).await.unwrap(); // 5
    assert_eq!(text(w.state(), "a").as_deref(), Some("bad"));
    assert_eq!(text(w.state(), "c"), None);

    // States after a restore are rebuilt from the restore's own snapshot.
    let scratch = MemState::new();
    state_at(&store, &layout(), Point::Lsn(4), &scratch)
        .await
        .unwrap();
    assert_eq!(
        (
            text(&scratch, "a").as_deref(),
            text(&scratch, "c").as_deref()
        ),
        (Some("good"), Some("after"))
    );
}

#[tokio::test]
async fn the_base_moves_forward_and_older_generations_are_deleted() {
    let store = Store::memory();
    // 200 ms of history: the base moves once it is 250 ms older than that.
    let w = writer(&store, Duration::from_millis(200)).await;
    w.commit(vec![put("old", "1")]).await.unwrap(); // 1
    w.compact().await.unwrap(); // generation 0 replaced: the base
    w.commit(vec![put("mid", "1")]).await.unwrap(); // 2
    w.compact().await.unwrap(); // generation 1 replaced, snapshot dropped
    assert!(!w.history_due().await);

    tokio::time::sleep(Duration::from_millis(300)).await;
    w.commit(vec![put("new", "1")]).await.unwrap(); // 3
    w.compact().await.unwrap(); // generation 2 replaced, within the window
    assert!(w.history_due().await);
    assert!(w.maintain_history(Arc::new(MemState::new())).await.unwrap());
    assert!(!w.history_due().await);

    // Generation 2 is the base now, rebuilt at its snapshot (LSN 2); the
    // generations before it are gone.
    let (manifest, _) = read_manifest(&store, &layout()).await.unwrap().unwrap();
    let kept: Vec<u64> = manifest.superseded.iter().map(|s| s.generation).collect();
    assert_eq!(kept, vec![2]);
    let reach = history(&store, &layout()).await.unwrap();
    assert_eq!((reach.from_lsn, reach.to_lsn), (2, 3));

    let scratch = MemState::new();
    state_at(&store, &layout(), Point::Lsn(2), &scratch)
        .await
        .unwrap();
    assert_eq!(
        (
            text(&scratch, "old"),
            text(&scratch, "mid"),
            text(&scratch, "new")
        ),
        (Some("1".into()), Some("1".into()), None)
    );
    let err = state_at(&store, &layout(), Point::Lsn(1), &scratch)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "NOT_IN_HISTORY");
    assert_eq!(scratch.position().unwrap().applied_lsn, 2, "left as it was");
}
