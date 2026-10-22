//! Writer and replica behaviour against an in-memory object store.

use std::sync::Arc;
use std::time::Duration;
use valkvedge_log::{
    now_ms, Hlc, HlcClock, Layout, LeaseConfig, Node, Op, OpKind, RejectReason, ReplicaId, Store,
};
use valkvedge_sync::{
    pull, remote_head, Change, MemState, OpOutcome, OutboxEntry, StateStore, Writer, WriterConfig,
};

fn fast() -> LeaseConfig {
    LeaseConfig {
        heartbeat_interval: Duration::from_millis(20),
        lease_ttl: Duration::from_millis(200),
        safety_margin: Duration::from_millis(50),
    }
}

async fn writer_on(
    store: &Store,
    node_id: &str,
    state: Arc<MemState>,
    wait: bool,
) -> Writer<MemState> {
    writer_with(store, node_id, state, wait, WriterConfig::default()).await
}

async fn writer_with(
    store: &Store,
    node_id: &str,
    state: Arc<MemState>,
    wait: bool,
    config: WriterConfig,
) -> Writer<MemState> {
    let node = Node::start(store.clone(), node_id, "addr", fast())
        .await
        .unwrap();
    Writer::open(
        store.clone(),
        Layout::new("db"),
        node,
        state,
        Arc::new(HlcClock::new(ReplicaId::random())),
        config,
        wait,
    )
    .await
    .unwrap()
}

fn put(key: &str, value: &str) -> Change {
    Change {
        kind: OpKind::Put,
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        expires_ms: None,
    }
}

fn del(key: &str) -> Change {
    Change {
        kind: OpKind::Delete,
        key: key.as_bytes().to_vec(),
        value: vec![],
        expires_ms: None,
    }
}

fn stamped(key: &str, value: &str, ms: u64, replica: ReplicaId) -> Op {
    Op {
        kind: OpKind::Put,
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        hlc: Hlc {
            physical_ms: ms,
            counter: 0,
            replica,
        },
        expires_ms: None,
    }
}

fn get(state: &MemState, key: &str) -> Option<String> {
    state
        .get(key.as_bytes())
        .map(|v| String::from_utf8(v).unwrap())
}

#[tokio::test]
async fn replica_follows_the_writer_incrementally() {
    let store = Store::memory();
    let layout = Layout::new("db");
    let w = writer_on(&store, "a", Arc::new(MemState::new()), false).await;
    assert_eq!(w.commit(vec![put("k1", "v1")]).await.unwrap(), 1);

    let replica = MemState::new();
    let first = pull(&store, &layout, &replica).await.unwrap();
    assert_eq!(
        (first.applied_lsn, first.frames, first.bootstrapped),
        (1, 1, false)
    );
    assert_eq!(get(&replica, "k1").as_deref(), Some("v1"));

    w.commit(vec![put("k2", "v2"), del("k1")]).await.unwrap();
    let second = pull(&store, &layout, &replica).await.unwrap();
    assert_eq!((second.applied_lsn, second.frames), (2, 1));
    assert_eq!(get(&replica, "k1"), None);
    assert_eq!(get(&replica, "k2").as_deref(), Some("v2"));

    // Nothing new: a pull is a no-op.
    assert_eq!(pull(&store, &layout, &replica).await.unwrap().frames, 0);
    assert_eq!(remote_head(&store, &layout).await.unwrap(), (0, 2));
}

#[tokio::test]
async fn pull_of_a_missing_database_is_not_found() {
    let err = pull(&Store::memory(), &Layout::new("nope"), &MemState::new())
        .await
        .unwrap_err();
    assert_eq!(err.code(), "NOT_FOUND");
}

#[tokio::test]
async fn new_replica_cold_boots_from_the_snapshot_not_from_history() {
    let store = Store::memory();
    let layout = Layout::new("db");
    let w = writer_on(&store, "a", Arc::new(MemState::new()), false).await;
    for i in 0..20 {
        w.commit(vec![put(&format!("k{i:02}"), "old")])
            .await
            .unwrap();
    }
    w.compact().await.unwrap();
    w.commit(vec![put("k00", "new")]).await.unwrap();

    let replica = MemState::new();
    let outcome = pull(&store, &layout, &replica).await.unwrap();
    assert!(outcome.bootstrapped);
    assert_eq!(
        outcome.frames, 1,
        "only the frame after the snapshot is replayed"
    );
    assert_eq!(outcome.applied_lsn, 21);
    assert_eq!(get(&replica, "k00").as_deref(), Some("new"));
    assert_eq!(get(&replica, "k19").as_deref(), Some("old"));
}

#[tokio::test]
async fn replica_one_generation_behind_catches_up_from_frames() {
    let store = Store::memory();
    let layout = Layout::new("db");
    let w = writer_on(&store, "a", Arc::new(MemState::new()), false).await;
    w.commit(vec![put("a", "1")]).await.unwrap();

    let replica = MemState::new();
    pull(&store, &layout, &replica).await.unwrap();

    w.commit(vec![put("b", "2")]).await.unwrap();
    w.compact().await.unwrap();
    w.commit(vec![put("c", "3")]).await.unwrap();

    let outcome = pull(&store, &layout, &replica).await.unwrap();
    assert!(
        !outcome.bootstrapped,
        "old generation is still within its grace period"
    );
    assert_eq!(outcome.applied_lsn, 3);
    assert_eq!(replica.position().unwrap().generation, 1);
    assert_eq!(get(&replica, "c").as_deref(), Some("3"));
}

#[tokio::test]
async fn replica_whose_generation_was_deleted_re_bootstraps() {
    let store = Store::memory();
    let layout = Layout::new("db");
    let config = WriterConfig {
        generation_grace: Duration::ZERO,
        ..WriterConfig::default()
    };
    let w = writer_with(&store, "a", Arc::new(MemState::new()), false, config).await;
    w.commit(vec![put("a", "1")]).await.unwrap();

    let replica = MemState::new();
    pull(&store, &layout, &replica).await.unwrap();

    w.commit(vec![put("b", "2")]).await.unwrap();
    w.compact().await.unwrap();
    w.commit(vec![put("c", "3")]).await.unwrap();
    w.compact().await.unwrap();

    let outcome = pull(&store, &layout, &replica).await.unwrap();
    assert!(outcome.bootstrapped);
    assert_eq!(get(&replica, "a").as_deref(), Some("1"));
    assert_eq!(get(&replica, "c").as_deref(), Some("3"));
}

#[tokio::test]
async fn interrupted_snapshot_load_is_restarted() {
    let store = Store::memory();
    let layout = Layout::new("db");
    let w = writer_on(&store, "a", Arc::new(MemState::new()), false).await;
    w.commit(vec![put("a", "1")]).await.unwrap();
    w.compact().await.unwrap();

    let replica = MemState::new();
    pull(&store, &layout, &replica).await.unwrap();
    // A crash between begin_reset and finish_reset leaves the mark set.
    replica.begin_reset().unwrap();
    assert!(replica.needs_bootstrap().unwrap());

    let outcome = pull(&store, &layout, &replica).await.unwrap();
    assert!(outcome.bootstrapped);
    assert!(!replica.needs_bootstrap().unwrap());
    assert_eq!(get(&replica, "a").as_deref(), Some("1"));
}

#[tokio::test]
async fn pushed_writes_resolve_by_last_write_wins_per_key() {
    let store = Store::memory();
    let state = Arc::new(MemState::new());
    let w = writer_on(&store, "a", state.clone(), false).await;
    let (phone, laptop) = (ReplicaId([1; 16]), ReplicaId([2; 16]));
    let t = now_ms();

    // The laptop wrote later but pushes first.
    let report = w
        .push(
            laptop,
            vec![OutboxEntry {
                seq: 1,
                ops: vec![stamped("title", "laptop", t - 1_000, laptop)],
            }],
        )
        .await
        .unwrap();
    assert_eq!(report.entries, vec![(1, vec![OpOutcome::Applied])]);

    // The phone's batch: one key loses, the other wins. The batch is one frame.
    let report = w
        .push(
            phone,
            vec![OutboxEntry {
                seq: 1,
                ops: vec![
                    stamped("title", "phone", t - 5_000, phone),
                    stamped("body", "phone", t - 5_000, phone),
                ],
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        report.entries,
        vec![(1, vec![OpOutcome::Superseded, OpOutcome::Applied])]
    );
    assert_eq!(get(&state, "title").as_deref(), Some("laptop"));
    assert_eq!(get(&state, "body").as_deref(), Some("phone"));
}

#[tokio::test]
async fn a_delete_beats_an_older_buffered_write() {
    let store = Store::memory();
    let state = Arc::new(MemState::new());
    let w = writer_on(&store, "a", state.clone(), false).await;
    let replica = ReplicaId([1; 16]);
    w.commit(vec![put("k", "v")]).await.unwrap();
    w.commit(vec![del("k")]).await.unwrap();

    let report = w
        .push(
            replica,
            vec![OutboxEntry {
                seq: 1,
                ops: vec![stamped("k", "zombie", now_ms() - 60_000, replica)],
            }],
        )
        .await
        .unwrap();
    assert_eq!(report.entries[0].1, vec![OpOutcome::Superseded]);
    assert_eq!(get(&state, "k"), None);
}

#[tokio::test]
async fn a_sync_write_beats_an_earlier_write_stamped_in_the_future() {
    let store = Store::memory();
    let state = Arc::new(MemState::new());
    let w = writer_on(&store, "a", state.clone(), false).await;
    let replica = ReplicaId([1; 16]);
    let ahead = now_ms() + 60_000; // inside the allowed skew
    w.push(
        replica,
        vec![OutboxEntry {
            seq: 1,
            ops: vec![stamped("k", "from-the-future", ahead, replica)],
        }],
    )
    .await
    .unwrap();
    w.commit(vec![put("k", "sync")]).await.unwrap();
    assert_eq!(get(&state, "k").as_deref(), Some("sync"));
    assert!(state.version_of(b"k").unwrap().unwrap().physical_ms >= ahead);
}

#[tokio::test]
async fn clock_skew_and_stale_writes_are_rejected_and_reported() {
    let store = Store::memory();
    let state = Arc::new(MemState::new());
    let config = WriterConfig {
        max_clock_skew: Duration::from_secs(60),
        tombstone_retention: Duration::from_secs(3600),
        ..WriterConfig::default()
    };
    let w = writer_with(&store, "a", state.clone(), false, config).await;
    let replica = ReplicaId([1; 16]);
    let t = now_ms();
    let report = w
        .push(
            replica,
            vec![OutboxEntry {
                seq: 1,
                ops: vec![
                    stamped("future", "x", t + 10 * 60_000, replica),
                    stamped("ancient", "x", t - 2 * 3_600_000, replica),
                    stamped("fine", "x", t, replica),
                ],
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        report.entries[0].1,
        vec![
            OpOutcome::Rejected(RejectReason::ClockSkew),
            OpOutcome::Rejected(RejectReason::StaleWrite),
            OpOutcome::Applied,
        ]
    );
    assert_eq!(get(&state, "future"), None);
    assert_eq!(get(&state, "ancient"), None);
    assert_eq!(
        report.watermark, 1,
        "the entry is settled even though ops were refused"
    );
}

#[tokio::test]
async fn a_retried_push_is_not_applied_twice_even_after_failover() {
    let store = Store::memory();
    let layout = Layout::new("db");
    let replica = ReplicaId([1; 16]);
    let t = now_ms();
    let entries = || {
        vec![
            OutboxEntry {
                seq: 1,
                ops: vec![stamped("k", "first", t - 2_000, replica)],
            },
            OutboxEntry {
                seq: 2,
                ops: vec![stamped("k", "second", t - 1_000, replica)],
            },
        ]
    };

    let a = writer_on(&store, "a", Arc::new(MemState::new()), false).await;
    let first = a.push(replica, entries()).await.unwrap();
    assert_eq!((first.watermark, first.lsn), (2, 2));
    a.release().await.unwrap();

    // A different node with empty state takes over and learns the watermark
    // from the log.
    let state_b = Arc::new(MemState::new());
    let b = writer_on(&store, "b", state_b.clone(), false).await;
    assert_eq!(state_b.watermark(&replica).unwrap(), 2);
    let retry = b.push(replica, entries()).await.unwrap();
    assert_eq!(
        retry.entries,
        vec![
            (1, vec![OpOutcome::Duplicate]),
            (2, vec![OpOutcome::Duplicate])
        ]
    );
    assert_eq!(retry.lsn, 2, "no new frame was written");
    assert_eq!(remote_head(&store, &layout).await.unwrap().1, 2);
    assert_eq!(get(&state_b, "k").as_deref(), Some("second"));
}

#[tokio::test]
async fn watermarks_survive_compaction() {
    let store = Store::memory();
    let layout = Layout::new("db");
    let pusher = ReplicaId([1; 16]);
    let w = writer_on(&store, "a", Arc::new(MemState::new()), false).await;
    w.push(
        pusher,
        vec![OutboxEntry {
            seq: 7,
            ops: vec![stamped("k", "v", now_ms(), pusher)],
        }],
    )
    .await
    .unwrap();
    w.compact().await.unwrap();

    let follower = MemState::new();
    assert!(pull(&store, &layout, &follower).await.unwrap().bootstrapped);
    assert_eq!(follower.watermark(&pusher).unwrap(), 7);
}

#[tokio::test]
async fn no_acknowledged_write_is_lost_when_the_owner_dies() {
    let store = Store::memory();
    let layout = Layout::new("db");
    let node_a = Node::start(store.clone(), "a", "addr", fast())
        .await
        .unwrap();
    let a = Writer::open(
        store.clone(),
        layout.clone(),
        node_a.clone(),
        Arc::new(MemState::new()),
        Arc::new(HlcClock::new(ReplicaId::random())),
        WriterConfig::default(),
        false,
    )
    .await
    .unwrap();
    for i in 0..5 {
        a.commit(vec![put(&format!("k{i}"), "acked")])
            .await
            .unwrap();
    }

    // Node a dies without releasing. Node b is refused at first, then takes
    // over once a's heartbeat has stood still for the lease TTL.
    node_a.stop_heartbeat();
    let state_b = Arc::new(MemState::new());
    let b = writer_on(&store, "b", state_b.clone(), true).await;
    for i in 0..5 {
        assert_eq!(get(&state_b, &format!("k{i}")).as_deref(), Some("acked"));
    }
    assert_eq!(b.commit(vec![put("after", "failover")]).await.unwrap(), 6);

    // The old owner, if it comes back, cannot write.
    assert!(a.commit(vec![put("split", "brain")]).await.is_err());
    let replica = MemState::new();
    pull(&store, &layout, &replica).await.unwrap();
    assert_eq!(get(&replica, "split"), None);
    assert_eq!(get(&replica, "after").as_deref(), Some("failover"));
}

#[tokio::test]
async fn compaction_drops_expired_tombstones_only() {
    let store = Store::memory();
    let layout = Layout::new("db");
    let state = Arc::new(MemState::new());
    let config = WriterConfig {
        tombstone_retention: Duration::from_millis(50),
        ..WriterConfig::default()
    };
    let w = writer_with(&store, "a", state.clone(), false, config).await;
    w.commit(vec![put("old", "v"), put("keep", "v")])
        .await
        .unwrap();
    w.commit(vec![del("old")]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    w.commit(vec![put("fresh", "v")]).await.unwrap();
    w.commit(vec![del("fresh")]).await.unwrap();
    assert_eq!(state.len_with_tombstones(), 3);

    w.compact().await.unwrap();
    assert_eq!(
        state.len_with_tombstones(),
        2,
        "the expired tombstone is gone locally"
    );

    let replica = MemState::new();
    pull(&store, &layout, &replica).await.unwrap();
    assert_eq!(replica.len_with_tombstones(), 2);
    assert!(replica.version_of(b"old").unwrap().is_none());
    assert!(replica.version_of(b"fresh").unwrap().is_some());
    assert_eq!(get(&replica, "keep").as_deref(), Some("v"));
}

// --- Group commit ---------------------------------------------------------

/// A store whose every write takes 50 ms, as an object store's would, so
/// commits made meanwhile queue up behind it.
fn slow_store() -> Store {
    use object_store::memory::InMemory;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    let config = ThrottleConfig {
        wait_put_per_call: Duration::from_millis(50),
        ..ThrottleConfig::default()
    };
    Store::new(Arc::new(ThrottledStore::new(InMemory::new(), config)))
}

#[tokio::test]
async fn commits_made_together_share_segments_and_keep_their_own_lsns() {
    let store = slow_store();
    let w = Arc::new(writer_on(&store, "a", Arc::new(MemState::new()), false).await);

    // One at a time, each commit is its own segment: nothing waits to group.
    w.commit(vec![put("first", "1")]).await.unwrap();
    w.commit(vec![put("second", "2")]).await.unwrap();
    assert_eq!((w.counters().commits(), w.counters().segments()), (2, 2));

    let tasks: Vec<_> = (0..20)
        .map(|i| {
            let w = w.clone();
            tokio::spawn(async move { w.commit(vec![put(&format!("k{i:02}"), "v")]).await })
        })
        .collect();
    let mut lsns = Vec::new();
    for task in tasks {
        lsns.push(task.await.unwrap().unwrap());
    }
    lsns.sort_unstable();
    assert_eq!(
        lsns,
        (3..=22).collect::<Vec<_>>(),
        "each commit has its own LSN"
    );
    let (commits, segments) = (w.counters().commits(), w.counters().segments());
    assert_eq!(commits, 22);
    assert!(
        segments - 2 <= 5,
        "20 commits made together took {} segments",
        segments - 2
    );

    // A replica sees every commit, frame by frame.
    let replica = MemState::new();
    let pulled = pull(&store, &Layout::new("db"), &replica).await.unwrap();
    assert_eq!(pulled.applied_lsn, 22);
    for i in 0..20 {
        assert_eq!(get(&replica, &format!("k{i:02}")).as_deref(), Some("v"));
    }
}

#[tokio::test]
async fn conditions_in_a_group_see_the_commits_before_them() {
    use valkvedge_sync::Condition;
    let store = slow_store();
    let state = Arc::new(MemState::new());
    let w = Arc::new(writer_on(&store, "a", state.clone(), false).await);
    w.commit(vec![put("counter", "0")]).await.unwrap();
    let version = state.live_version(b"counter").unwrap();

    // Ten writers all read `expected` and try to replace it. A slow commit
    // is in flight first, so all ten queue behind it and are written as one
    // group: only checking each against the ones before it in the group
    // stops a second one from winning.
    let race = |key: &'static str, expected: Option<Hlc>| {
        let w = w.clone();
        async move {
            let before = (w.counters().commits(), w.counters().segments());
            let busy = {
                let w = w.clone();
                tokio::spawn(async move { w.commit(vec![put("busy", "1")]).await })
            };
            tokio::time::sleep(Duration::from_millis(10)).await;
            let racers: Vec<_> = (0..10)
                .map(|i| {
                    let w = w.clone();
                    tokio::spawn(async move {
                        let condition = Condition {
                            key: key.as_bytes().to_vec(),
                            version: expected,
                        };
                        w.commit_if(vec![put(key, &i.to_string())], &[condition])
                            .await
                    })
                })
                .collect();
            busy.await.unwrap().unwrap();
            let mut won = 0;
            for racer in racers {
                match racer.await.unwrap() {
                    Ok(_) => won += 1,
                    Err(e) => assert_eq!(e.code(), "CONDITION_FAILED"),
                }
            }
            let after = (w.counters().commits(), w.counters().segments());
            assert_eq!(
                (after.0 - before.0, after.1 - before.1),
                (2, 2),
                "the slow commit, then the racers in one segment"
            );
            won
        }
    };
    assert_eq!(race("counter", version).await, 1, "one replacement wins");
    assert_eq!(race("new-key", None).await, 1, "one create wins");
}

#[tokio::test]
async fn a_released_writer_fails_every_waiting_commit() {
    let w = writer_on(&Store::memory(), "a", Arc::new(MemState::new()), false).await;
    w.release().await.unwrap();
    let err = w.commit(vec![put("k", "v")]).await.unwrap_err();
    assert_eq!(err.code(), "FENCED");
}
