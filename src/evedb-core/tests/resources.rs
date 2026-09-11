// SPDX-License-Identifier: AGPL-3.0-only

//! Admission boundaries and retained mutation ownership.
mod common;
use common::{TestDir, fields, schema};
use evedb_core::{Database, Error, Limits, Options, SharedDatabase};

fn baseline() -> TestDir {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    db.create_table("items", schema()).unwrap();
    db.checkpoint().unwrap();
    dir
}
fn options(limits: Limits) -> Options {
    Options {
        limits,
        snapshot_interval: 0,
        ..Options::default()
    }
}

#[test]
fn active_transaction_and_pin_permits_are_released_on_all_paths() {
    let dir = baseline();
    let db = SharedDatabase::open_with_options(
        &dir.0,
        options(Limits {
            max_transactions: 1,
            max_snapshots: 2,
            ..Limits::default()
        }),
    )
    .unwrap();
    let tx = db.transaction().unwrap();
    assert!(matches!(
        db.transaction(),
        Err(Error::LimitExceeded {
            resource: "active transactions",
            ..
        })
    ));
    assert_eq!(db.resource_usage().transactions, 1);
    assert_eq!(db.resource_usage().snapshots, 1);
    let pin = db.read_snapshot().unwrap();
    let clone = pin.clone();
    assert!(matches!(
        db.read_snapshot(),
        Err(Error::LimitExceeded {
            resource: "active snapshots",
            ..
        })
    ));
    drop(tx);
    drop(pin);
    assert_eq!(db.resource_usage().snapshots, 1);
    drop(clone);
    assert_eq!(db.resource_usage().snapshots, 0);
    assert_eq!(db.resource_usage().resident_write_bytes, 0);
    db.transaction().unwrap().commit().unwrap();
    assert_eq!(db.resource_usage().transactions, 0);
}

#[test]
fn oversized_batches_abort_atomically_in_both_apis() {
    for shared in [false, true] {
        let dir = baseline();
        let mut local = Database::open_with_options(
            &dir.0,
            options(Limits {
                max_transaction_operations: 1,
                ..Limits::default()
            }),
        )
        .unwrap();
        if shared {
            let db = local.into_shared();
            let mut tx = db.transaction().unwrap();
            tx.create(1, 1, fields(1)).unwrap();
            assert!(matches!(
                tx.create(1, 2, fields(2)),
                Err(Error::LimitExceeded { .. })
            ));
            assert!(tx.commit().is_err());
            assert!(db.get(1, 1).unwrap().is_none());
            assert_eq!(db.resource_usage().resident_write_bytes, 0);
        } else {
            let mut tx = local.transaction().unwrap();
            tx.create(1, 1, fields(1)).unwrap();
            assert!(matches!(
                tx.create(1, 2, fields(2)),
                Err(Error::LimitExceeded { .. })
            ));
            assert!(tx.commit().is_err());
            assert!(local.get(1, 1).unwrap().is_none());
        }
    }
}

#[test]
fn mutation_budget_transfers_to_publication_and_survives_a_pinned_checkpoint() {
    let dir = baseline();
    let db = SharedDatabase::open_with_options(
        &dir.0,
        options(Limits {
            max_transaction_bytes: 46,
            max_resident_write_bytes: 92,
            ..Limits::default()
        }),
    )
    .unwrap();
    // One Int64 assignment is exactly 46 encoded bytes including batch header.
    db.create(1, 1, fields(1)).unwrap();
    assert_eq!(db.resource_usage().resident_write_bytes, 46);
    let old = db.read_snapshot().unwrap();
    db.apply(1, 1, fields(2)).unwrap();
    assert_eq!(db.resource_usage().resident_write_bytes, 92);
    assert!(matches!(db.transaction(), Err(Error::LimitExceeded { .. })));
    assert_eq!(db.resource_usage().transactions, 0);
    db.checkpoint().unwrap();
    assert_eq!(db.resource_usage().resident_write_bytes, 46);
    drop(old);
    assert_eq!(db.resource_usage().resident_write_bytes, 0);
    let mut tx = db.transaction().unwrap();
    tx.apply(1, 1, fields(3)).unwrap();
    assert!(matches!(
        tx.apply(1, 1, fields(4)),
        Err(Error::LimitExceeded {
            resource: "transaction bytes",
            ..
        })
    ));
    drop(tx);
    assert_eq!(db.resource_usage().resident_write_bytes, 0);
}

#[test]
fn recovery_does_not_reject_acknowledged_writes_when_limits_are_lowered() {
    let dir = baseline();
    {
        let db = SharedDatabase::open(&dir.0).unwrap();
        for id in 0..10 {
            db.create(1, id, fields(id as i64)).unwrap();
        }
    }
    let db = SharedDatabase::open_with_options(
        &dir.0,
        options(Limits {
            max_transaction_bytes: 46,
            max_resident_write_bytes: 46,
            ..Limits::default()
        }),
    )
    .unwrap();
    assert!(db.get(1, 9).unwrap().is_some());
    assert!(db.resource_usage().resident_write_bytes > 46);
    assert!(matches!(db.transaction(), Err(Error::LimitExceeded { .. })));
    db.checkpoint().unwrap();
    db.create(1, 10, fields(10)).unwrap();
}

#[test]
fn checkpoint_pin_byte_limit_is_checked_without_leaking_a_permit() {
    let dir = baseline();
    let db = SharedDatabase::open_with_options(
        &dir.0,
        options(Limits {
            max_pinned_bytes: 0,
            ..Limits::default()
        }),
    )
    .unwrap();
    assert!(matches!(
        db.read_snapshot(),
        Err(Error::LimitExceeded {
            resource: "pinned checkpoint bytes",
            ..
        })
    ));
    assert_eq!(db.resource_usage().snapshots, 0);
    assert_eq!(db.resource_usage().pinned_bytes, 0);
}
