// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::{DataType, Field, Value, test_support::TempDir};
use std::{
    thread,
    time::{Duration, Instant},
};

fn setup(limits: crate::Limits, group: crate::GroupCommit) -> (TempDir, SharedDatabase, TableId) {
    let dir = TempDir::new();
    let db = SharedDatabase::open_with_options(
        &dir.0,
        Options {
            limits,
            group_commit: group,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db
        .create_table(
            "items",
            Schema::new(vec![Field {
                id: 1,
                name: "value".into(),
                data_type: DataType::UInt64,
                nullable: false,
            }])
            .unwrap(),
        )
        .unwrap();
    (dir, db, table)
}
fn wait_queued(db: &SharedDatabase, count: usize) {
    let end = Instant::now() + Duration::from_secs(5);
    while db.queue.state.lock().unwrap().count < count {
        assert!(Instant::now() < end, "queue never filled");
        thread::yield_now();
    }
}
#[test]
fn concurrent_writers_share_one_sync_keep_individual_frames_and_recover() {
    let (dir, db, table) = setup(crate::Limits::default(), crate::GroupCommit::default());
    let before = db.read_snapshot().unwrap();
    let guard = db.coordinator.lock().unwrap();
    let syncs = guard.database.wal_syncs;
    let writers: Vec<_> = (0..12)
        .map(|id| {
            let mut tx = db.transaction().unwrap();
            tx.create(table, id, [(1, Value::UInt64(id))].into())
                .unwrap();
            thread::spawn(move || tx.commit())
        })
        .collect();
    wait_queued(&db, 12);
    drop(guard);
    let mut sequences: Vec<_> = writers
        .into_iter()
        .map(|writer| writer.join().unwrap().unwrap())
        .collect();
    sequences.sort();
    assert_eq!(sequences, (2..=13).collect::<Vec<_>>());
    assert_eq!(db.coordinator.lock().unwrap().database.wal_syncs - syncs, 1);
    assert_eq!(db.queue.state.lock().unwrap().count, 0);
    assert_eq!(db.resource_usage().transactions, 0);
    for id in 0..12 {
        assert!(before.get(table, id).unwrap().is_none());
    }
    drop(before);
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.sequence(), 13);
    for id in 0..12 {
        assert_eq!(
            db.get(table, id).unwrap().unwrap().fields[&1],
            Value::UInt64(id)
        );
    }
}
#[test]
fn overlapping_writers_in_one_group_have_only_one_winner() {
    let (_dir, db, table) = setup(crate::Limits::default(), crate::GroupCommit::default());
    let guard = db.coordinator.lock().unwrap();
    let writers: Vec<_> = (0..8)
        .map(|id| {
            let mut tx = db.transaction().unwrap();
            tx.create(table, 1, [(1, Value::UInt64(id))].into())
                .unwrap();
            thread::spawn(move || tx.commit())
        })
        .collect();
    wait_queued(&db, 8);
    drop(guard);
    let results: Vec<_> = writers
        .into_iter()
        .map(|writer| writer.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(Error::Conflict { .. })))
            .count(),
        7
    );
    assert_eq!(db.sequence().unwrap(), 2);
}
#[test]
fn full_queue_rejects_and_expiry_releases_both_queue_budgets() {
    let (_dir, db, table) = setup(
        crate::Limits {
            max_queued_commits: 1,
            ..crate::Limits::default()
        },
        crate::GroupCommit::default(),
    );
    let mut first = db
        .transaction_with_options(TransactionOptions {
            timeout: Some(Duration::from_millis(200)),
            ..TransactionOptions::default()
        })
        .unwrap();
    first
        .create(table, 1, [(1, Value::UInt64(1))].into())
        .unwrap();
    let guard = db.coordinator.lock().unwrap();
    let waiter = thread::spawn(move || first.commit());
    wait_queued(&db, 1);
    let mut second = db.transaction().unwrap();
    second
        .create(table, 2, [(1, Value::UInt64(2))].into())
        .unwrap();
    assert!(matches!(
        second.commit(),
        Err(Error::LimitExceeded {
            resource: "queued commits",
            ..
        })
    ));
    assert!(matches!(
        waiter.join().unwrap(),
        Err(Error::DeadlineExceeded)
    ));
    assert_eq!(db.queue.state.lock().unwrap().count, 0);
    assert_eq!(db.queue.state.lock().unwrap().bytes, 0);
    assert_eq!(db.resource_usage().transactions, 0);
    drop(guard);
    assert!(db.get(table, 1).unwrap().is_none());
    db.create(table, 2, [(1, Value::UInt64(2))].into()).unwrap();
}
#[test]
fn group_bounds_split_the_queue_without_starving_the_next_leader() {
    let (_dir, db, table) = setup(
        crate::Limits::default(),
        crate::GroupCommit {
            max_transactions: 2,
            ..crate::GroupCommit::default()
        },
    );
    let guard = db.coordinator.lock().unwrap();
    let syncs = guard.database.wal_syncs;
    let writers: Vec<_> = (0..5)
        .map(|id| {
            let mut tx = db.transaction().unwrap();
            tx.create(table, id, [(1, Value::UInt64(id))].into())
                .unwrap();
            thread::spawn(move || tx.commit())
        })
        .collect();
    wait_queued(&db, 5);
    drop(guard);
    for writer in writers {
        writer.join().unwrap().unwrap();
    }
    assert_eq!(db.coordinator.lock().unwrap().database.wal_syncs - syncs, 3);
}

#[cfg(feature = "fault-injection")]
#[test]
fn group_crash_child() {
    let Ok(path) = std::env::var("EVEDB_GROUP_CHILD") else {
        return;
    };
    let db = SharedDatabase::open(path).unwrap();
    let snapshot = db.read_snapshot().unwrap();
    let guard = db.coordinator.lock().unwrap();
    let writers: Vec<_> = (1..=2)
        .map(|id| {
            let mut tx = db.transaction().unwrap();
            tx.create(1, id, [(1, Value::UInt64(id))].into()).unwrap();
            tx.apply(1, id, [(1, Value::UInt64(id + 10))].into())
                .unwrap();
            thread::spawn(move || tx.commit())
        })
        .collect();
    wait_queued(&db, 2);
    drop(guard);
    for writer in writers {
        assert!(matches!(
            writer.join().unwrap(),
            Err(Error::CommitUnknown(_))
        ));
    }
    assert!(matches!(snapshot.get(1, 1), Err(Error::NeedsRecovery)));
    assert!(matches!(db.transaction(), Err(Error::NeedsRecovery)));
    assert_eq!(db.queue.state.lock().unwrap().count, 0);
}
#[cfg(feature = "fault-injection")]
#[test]
fn group_wal_crashes_and_io_errors_preserve_transaction_atomicity() {
    for (variable, point, committed) in [
        ("EVEDB_FAILPOINT", "wal-before-write", false),
        ("EVEDB_FAILPOINT", "wal-partial", false),
        ("EVEDB_FAILPOINT", "wal-written", true),
        ("EVEDB_FAILPOINT", "wal-synced", true),
        ("EVEDB_IO_ERROR", "before-write", false),
        ("EVEDB_IO_ERROR", "partial-write", false),
        ("EVEDB_IO_ERROR", "before-sync", true),
        ("EVEDB_IO_ERROR", "after-sync", true),
    ] {
        let (dir, db, _) = setup(crate::Limits::default(), crate::GroupCommit::default());
        drop(db);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "shared::group_tests::group_crash_child",
                "--nocapture",
            ])
            .env("EVEDB_GROUP_CHILD", &dir.0)
            .env(variable, point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(if variable == "EVEDB_FAILPOINT" { 91 } else { 0 }),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let db = Database::open(&dir.0).unwrap();
        for id in 1..=2 {
            let entity = db.get(1, id).unwrap();
            if committed {
                let entity = entity.unwrap();
                assert_eq!(entity.version, 1);
                assert_eq!(entity.fields[&1], Value::UInt64(id + 10));
            } else {
                assert!(entity.is_none());
            }
        }
    }
}

#[test]
fn byte_queue_and_group_limits_are_enforced_independently() {
    let limits = crate::Limits {
        max_transaction_bytes: 128,
        max_queued_commit_bytes: 128,
        ..crate::Limits::default()
    };
    let (_dir, db, table) = setup(
        limits,
        crate::GroupCommit {
            max_bytes: 46,
            ..crate::GroupCommit::default()
        },
    );
    let guard = db.coordinator.lock().unwrap();
    let syncs = guard.database.wal_syncs;
    let writers: Vec<_> = (0..2)
        .map(|id| {
            let mut tx = db.transaction().unwrap();
            tx.create(table, id, [(1, Value::UInt64(id))].into())
                .unwrap();
            thread::spawn(move || tx.commit())
        })
        .collect();
    wait_queued(&db, 2);
    let mut excess = db.transaction().unwrap();
    excess
        .create(table, 3, [(1, Value::UInt64(3))].into())
        .unwrap();
    assert!(matches!(
        excess.commit(),
        Err(Error::LimitExceeded {
            resource: "queued commit bytes",
            ..
        })
    ));
    drop(guard);
    for writer in writers {
        writer.join().unwrap().unwrap();
    }
    assert_eq!(db.coordinator.lock().unwrap().database.wal_syncs - syncs, 2);
}

#[test]
fn automatic_checkpoints_prune_revisions_but_preserve_live_writer_conflicts() {
    let dir = TempDir::new();
    let db = SharedDatabase::open_with_options(
        &dir.0,
        Options {
            checkpoint_bytes: 4096,
            snapshot_interval: 0,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db
        .create_table(
            "items",
            Schema::new(vec![Field {
                id: 1,
                name: "value".into(),
                data_type: DataType::UInt64,
                nullable: false,
            }])
            .unwrap(),
        )
        .unwrap();
    db.create(table, 1, [(1, Value::UInt64(1))].into()).unwrap();
    let mut stale = db.transaction().unwrap();
    stale
        .apply(table, 1, [(1, Value::UInt64(99))].into())
        .unwrap();
    db.apply(table, 1, [(1, Value::UInt64(2))].into()).unwrap();
    for id in 2..180 {
        db.create(table, id, [(1, Value::UInt64(id))].into())
            .unwrap();
    }
    assert!(
        db.coordinator
            .lock()
            .unwrap()
            .database
            .checkpoint_generation()
            > 0
    );
    assert!(matches!(stale.commit(), Err(Error::Conflict { .. })));
    for id in 180..360 {
        db.create(table, id, [(1, Value::UInt64(id))].into())
            .unwrap();
    }
    let coordinator = db.coordinator.lock().unwrap();
    assert!(
        coordinator.revisions.len() < 100,
        "automatic checkpoints must release unneeded conflict revisions"
    );
    drop(coordinator);
    assert_eq!(
        db.get(table, 1).unwrap().unwrap().fields[&1],
        Value::UInt64(2)
    );
}
