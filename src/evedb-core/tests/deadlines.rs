// SPDX-License-Identifier: AGPL-3.0-only

//! Abandoned handle expiry and cooperative operation deadlines.
mod common;
use common::{TestDir, fields, schema};
use evedb_core::{Database, Error, Options, SharedDatabase, Timeouts, TransactionOptions};
use std::time::{Duration, Instant};

fn options() -> Options {
    Options {
        timeouts: Timeouts {
            transaction: Duration::from_secs(5),
            idle_transaction: Duration::from_millis(40),
            snapshot: Duration::from_secs(5),
            operation: Duration::from_secs(5),
            reap_interval: Duration::from_millis(5),
            ..Timeouts::default()
        },
        ..Options::default()
    }
}
fn until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < end, "expiry did not release resources");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn abandoned_shared_transaction_releases_writes_and_pins_without_another_call() {
    let dir = TestDir::new();
    let db = SharedDatabase::open_with_options(&dir.0, options()).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.checkpoint().unwrap();
    let mut tx = db.transaction().unwrap();
    tx.create(table, 1, fields(1)).unwrap();
    until(|| db.resource_usage().transactions == 0);
    assert_eq!(db.resource_usage().resident_write_bytes, 0);
    assert_eq!(db.resource_usage().snapshots, 0);
    assert!(matches!(tx.get(table, 1), Err(Error::TransactionExpired)));
    assert!(matches!(tx.commit(), Err(Error::TransactionExpired)));
    assert!(db.get(table, 1).unwrap().is_none());
}

#[test]
fn abandoned_local_transaction_releases_its_budget() {
    let dir = TestDir::new();
    let mut db = Database::open_with_options(&dir.0, options()).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.checkpoint().unwrap();
    let reader = db.reader();
    let mut tx = db.transaction().unwrap();
    tx.create(table, 1, fields(1)).unwrap();
    until(|| reader.resource_usage().transactions == 0);
    assert_eq!(reader.resource_usage().resident_write_bytes, 0);
    assert!(matches!(tx.commit(), Err(Error::TransactionExpired)));
    assert!(db.get(table, 1).unwrap().is_none());
}

#[test]
fn expired_snapshot_clones_release_files_but_preserve_metadata() {
    let dir = TestDir::new();
    let mut config = options();
    config.timeouts.snapshot = Duration::from_millis(60);
    let mut db = Database::open_with_options(&dir.0, config).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    db.checkpoint().unwrap();
    let generation = db.generations().last().unwrap().generation;
    let path = dir.0.join(format!("tables/{table:020}/{generation:020}"));
    let snapshot = db.read_snapshot().unwrap();
    let clone = snapshot.clone();
    until(|| db.resource_usage().snapshots == 0);
    assert!(matches!(
        snapshot.get(table, 1),
        Err(Error::TransactionExpired)
    ));
    assert!(matches!(
        clone.get(table, 1),
        Err(Error::TransactionExpired)
    ));
    assert!(snapshot.table("items").is_some());
    for _ in 0..3 {
        db.compact().unwrap();
    }
    assert!(!path.exists());
}

#[test]
fn total_deadline_cannot_be_extended_by_regular_activity() {
    let dir = TestDir::new();
    let db = SharedDatabase::open_with_options(&dir.0, options()).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    let mut requested = TransactionOptions::default();
    requested.timeout = Some(Duration::from_millis(60));
    let mut tx = db.transaction_with_options(requested).unwrap();
    let mut expired = false;
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end {
        match tx.get(table, 1) {
            Ok(None) => {}
            Err(Error::TransactionExpired | Error::DeadlineExceeded) => {
                expired = true;
                break;
            }
            other => panic!("unexpected read: {other:?}"),
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(expired);
    assert!(tx.create(table, 1, fields(1)).is_err());
}

#[test]
fn slow_scan_callbacks_are_cooperatively_timed_out_without_holding_publication() {
    let dir = TestDir::new();
    let mut config = options();
    config.timeouts.operation = Duration::from_millis(40);
    let db = SharedDatabase::open_with_options(&dir.0, config).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    let result = db.reader().scan(table, |_| {
        std::thread::sleep(Duration::from_millis(70));
        Ok(())
    });
    assert!(matches!(result, Err(Error::DeadlineExceeded)));
    assert!(db.get(table, 1).unwrap().is_some());
}
