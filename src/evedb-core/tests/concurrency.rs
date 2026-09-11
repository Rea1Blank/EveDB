// SPDX-License-Identifier: AGPL-3.0-only

//! Independent writers, isolation contracts, and atomic conflict handling.

mod common;
use common::{TestDir, fields, schema};
use evedb_core::{Database, Error, IsolationLevel, SharedDatabase, TransactionOptions, Value};
use std::sync::{Arc, Barrier};

#[test]
fn independent_clients_stage_together_and_commit_cross_table_writes() {
    fn shareable<T: Send + Sync + Clone>() {}
    shareable::<SharedDatabase>();
    let dir = TestDir::new();
    let db = SharedDatabase::open(&dir.0).unwrap();
    let a = db.create_table("a", schema()).unwrap();
    let b = db.create_table("b", schema()).unwrap();
    let old = db.read_snapshot().unwrap();
    let barrier = Arc::new(Barrier::new(8));
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for id in 0..8 {
            let db = db.clone();
            let barrier = barrier.clone();
            workers.push(scope.spawn(move || {
                let mut tx = db.transaction().unwrap();
                tx.create(a, id, fields(id as i64)).unwrap();
                tx.create(b, id, fields(id as i64)).unwrap();
                tx.apply(a, id, fields(100)).unwrap();
                tx.apply(a, id, fields(200)).unwrap();
                assert_eq!(tx.get(a, id).unwrap().unwrap().version, 2);
                assert!(db.get(a, id).unwrap().is_none());
                // This deadlocks if begin or staging owns the commit mutex.
                barrier.wait();
                (id, tx.commit().unwrap())
            }));
        }
        let mut sequences = Vec::new();
        for worker in workers {
            let (id, sequence) = worker.join().unwrap();
            sequences.push(sequence);
            let events = db.reader().events(a, id).unwrap();
            assert_eq!(events.len(), 2);
            assert!(events.iter().all(|event| event.transaction == sequence));
        }
        sequences.sort_unstable();
        assert_eq!(sequences, (3..=10).collect::<Vec<_>>());
    });
    assert!(old.get(a, 0).unwrap().is_none());
    db.compact().unwrap();
    drop(old);
    drop(db);
    let db = SharedDatabase::open(&dir.0).unwrap();
    for id in 0..8 {
        assert_eq!(
            db.get(a, id).unwrap().unwrap().fields[&1],
            Value::Int64(200)
        );
        assert_eq!(
            db.get(b, id).unwrap().unwrap().fields[&1],
            Value::Int64(id as i64)
        );
    }
}

#[test]
fn conflicting_writers_abort_every_write_and_retry_from_a_fresh_view() {
    let dir = TestDir::new();
    let db = SharedDatabase::open(&dir.0).unwrap();
    let a = db.create_table("a", schema()).unwrap();
    let b = db.create_table("b", schema()).unwrap();
    db.create(a, 1, fields(0)).unwrap();
    let mut first = db.transaction().unwrap();
    let mut second = db.transaction().unwrap();
    first.apply(a, 1, fields(1)).unwrap();
    second.apply(a, 1, fields(2)).unwrap();
    second.create(b, 9, fields(99)).unwrap();
    first.commit().unwrap();
    let sequence = db.sequence().unwrap();
    assert!(
        matches!(second.commit(), Err(Error::Conflict { table: Some(t), entity: Some(1) }) if t == a)
    );
    assert_eq!(db.sequence().unwrap(), sequence);
    assert!(db.get(b, 9).unwrap().is_none());
    db.apply(a, 1, fields(2)).unwrap();
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.get(a, 1).unwrap().unwrap().version, 2);
    assert!(db.get(b, 9).unwrap().is_none());
}

#[test]
fn an_absent_key_has_only_one_concurrent_creator() {
    let dir = TestDir::new();
    let db = SharedDatabase::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    let barrier = Arc::new(Barrier::new(4));
    let results = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for value in 0..4 {
            let db = db.clone();
            let barrier = barrier.clone();
            workers.push(scope.spawn(move || {
                let mut tx = db.transaction().unwrap();
                assert!(tx.get(table, 1).unwrap().is_none());
                tx.create(table, 1, fields(value)).unwrap();
                barrier.wait();
                tx.commit()
            }));
        }
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(Error::Conflict { .. })))
            .count(),
        3
    );
}

#[test]
fn retention_and_snapshot_writes_conflict_even_without_a_version_change() {
    for snapshot_only in [false, true] {
        let dir = TestDir::new();
        let db = SharedDatabase::open(&dir.0).unwrap();
        let table = db.create_table("items", schema()).unwrap();
        db.create(table, 1, fields(0)).unwrap();
        db.apply(table, 1, fields(1)).unwrap();
        db.checkpoint().unwrap();
        let mut old = db.transaction().unwrap();
        old.apply(table, 1, fields(2)).unwrap();
        db.write(|tx| {
            if snapshot_only {
                tx.snapshot(table, 1)
            } else {
                tx.retain_last(table, 1, 0)
            }
        })
        .unwrap();
        assert_eq!(db.get(table, 1).unwrap().unwrap().version, 1);
        // Pruning must retain the conflict marker while an older writer is alive.
        for _ in 0..3 {
            db.compact().unwrap();
        }
        assert!(matches!(old.commit(), Err(Error::Conflict { .. })));
        let expected = if snapshot_only { (0, 1) } else { (1, 1) };
        assert_eq!(db.reader().retained_range(table, 1).unwrap(), expected);
    }
}

#[test]
fn catalog_changes_abort_stale_writers_and_do_not_overwrite_committed_data() {
    let dir = TestDir::new();
    let db = SharedDatabase::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    let mut stale = db.transaction().unwrap();
    stale.apply(table, 1, fields(2)).unwrap();
    let mut ddl = db.transaction().unwrap();
    ddl.rename_table(table, "renamed").unwrap();
    db.create(table, 2, fields(2)).unwrap();
    ddl.commit().unwrap();
    assert!(matches!(
        stale.commit(),
        Err(Error::Conflict {
            table: None,
            entity: None
        })
    ));
    assert!(db.reader().table("renamed").unwrap().is_some());
    assert!(db.get(table, 2).unwrap().is_some());
    let mut first = db.transaction().unwrap();
    let mut second = db.transaction().unwrap();
    first.create_table("one", schema()).unwrap();
    second.create_table("two", schema()).unwrap();
    second.commit().unwrap();
    assert!(matches!(first.commit(), Err(Error::Conflict { .. })));
}

#[test]
fn unsupported_isolation_is_explicit_and_snapshot_reads_are_repeatable() {
    let dir = TestDir::new();
    let db = SharedDatabase::open(&dir.0).unwrap();
    for isolation in [IsolationLevel::ReadCommitted, IsolationLevel::Serializable] {
        assert!(
            matches!(db.transaction_with_options(TransactionOptions::with_isolation(isolation)), Err(Error::UnsupportedIsolation(actual)) if actual == isolation)
        );
    }
    assert_eq!(db.snapshot_stats().active, 0);
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    let mut tx = db
        .transaction_with_options(TransactionOptions::with_isolation(IsolationLevel::Snapshot))
        .unwrap();
    let sequence = db.sequence().unwrap();
    db.apply(table, 1, fields(2)).unwrap();
    assert_eq!(
        tx.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(1)
    );
    tx.create(table, 3, fields(3)).unwrap();
    assert!(
        tx.apply(table, 1, [(1, Value::Text("wrong".into()))].into())
            .is_err()
    );
    assert!(tx.commit().is_err());
    assert!(db.get(table, 3).unwrap().is_none());
    let readonly = db.transaction().unwrap();
    db.apply(table, 1, fields(3)).unwrap();
    assert_eq!(readonly.commit().unwrap(), sequence + 1);
}

#[test]
fn snapshot_write_skew_is_documented_instead_of_claiming_serializable() {
    let dir = TestDir::new();
    let db = SharedDatabase::open(&dir.0).unwrap();
    let table = db.create_table("on_call", schema()).unwrap();
    db.write(|tx| {
        tx.create(table, 1, fields(1))?;
        tx.create(table, 2, fields(1))
    })
    .unwrap();
    let mut a = db.transaction().unwrap();
    let mut b = db.transaction().unwrap();
    assert_eq!(
        a.get(table, 2).unwrap().unwrap().fields[&1],
        Value::Int64(1)
    );
    assert_eq!(
        b.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(1)
    );
    a.apply(table, 1, fields(0)).unwrap();
    b.apply(table, 2, fields(0)).unwrap();
    a.commit().unwrap();
    b.commit().unwrap();
    assert_eq!(
        db.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(0)
    );
    assert_eq!(
        db.get(table, 2).unwrap().unwrap().fields[&1],
        Value::Int64(0)
    );
}

#[test]
fn transactions_keep_the_owner_alive_and_dropped_writes_do_not_commit() {
    let dir = TestDir::new();
    let db = SharedDatabase::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    let mut aborted = db.transaction().unwrap();
    aborted.create(table, 1, fields(1)).unwrap();
    drop(aborted);
    let mut survivor = db.transaction().unwrap();
    survivor.create(table, 2, fields(2)).unwrap();
    drop(db);
    assert!(matches!(Database::open(&dir.0), Err(Error::Locked)));
    survivor.commit().unwrap();
    let db = Database::open(&dir.0).unwrap();
    assert!(db.get(table, 1).unwrap().is_none());
    assert!(db.get(table, 2).unwrap().is_some());
}

#[test]
fn late_staging_uses_the_begin_view_and_preserves_old_event_sequences() {
    let dir = TestDir::new();
    let db = SharedDatabase::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0)).unwrap();
    db.apply(table, 1, fields(1)).unwrap();
    let original_sequence = db.sequence().unwrap();
    db.checkpoint().unwrap();
    let mut old = db.transaction().unwrap();
    db.create(table, 2, fields(2)).unwrap();
    old.apply(table, 1, fields(3)).unwrap();
    let committed = old.commit().unwrap();
    let events = db.reader().events(table, 1).unwrap();
    assert_eq!(events[0].transaction, original_sequence);
    assert_eq!(events[1].transaction, committed);
    let mut late = db.transaction().unwrap();
    db.delete(table, 1).unwrap();
    db.compact().unwrap();
    // Staging after the winner committed must still read the begin snapshot.
    late.apply(table, 1, fields(4)).unwrap();
    assert!(matches!(late.commit(), Err(Error::Conflict { .. })));
    drop(db);
    let db = SharedDatabase::open(&dir.0).unwrap();
    assert!(db.get(table, 1).unwrap().is_none());
    assert!(matches!(
        db.create(table, 1, fields(5)),
        Err(Error::AlreadyExists(_))
    ));
    assert_eq!(
        db.reader().events(table, 1).unwrap()[1].transaction,
        committed
    );
}

#[test]
fn a_scan_callback_can_commit_without_changing_the_scan_view() {
    let dir = TestDir::new();
    let db = SharedDatabase::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    db.checkpoint().unwrap();
    let mut visited = Vec::new();
    db.reader()
        .scan(table, |entity| {
            visited.push(entity.id);
            db.create(table, 2, fields(2))?;
            db.compact()
        })
        .unwrap();
    assert_eq!(visited, [1]);
    assert!(db.get(table, 2).unwrap().is_some());
}
