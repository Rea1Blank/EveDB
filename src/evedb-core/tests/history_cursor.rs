// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded history ownership, cancellation, and decoded-memory admission.
#[allow(dead_code)]
mod common;
use common::TestDir;
use evedb_core::{
    CancellationToken, DataType, Database, Error, Field, Fields, HistoryOptions, Limits, Options,
    Schema, SharedDatabase, Timeouts, Value,
};
use std::time::Duration;

fn schema() -> Schema {
    Schema::new(vec![Field {
        id: 1,
        name: "bytes".into(),
        data_type: DataType::Bytes,
        nullable: false,
    }])
    .unwrap()
}
fn fields(size: usize, value: u8) -> Fields {
    [(1, Value::Bytes(vec![value; size]))].into()
}
fn baseline(size: usize, events: usize) -> TestDir {
    let dir = TestDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            snapshot_interval: 0,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(size, 0)).unwrap();
    db.write(|tx| {
        for i in 1..=events {
            tx.apply(table, 1, fields(size, (i % 251) as u8))?;
        }
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap();
    dir
}

#[test]
fn batches_cross_disk_memory_and_partition_boundaries_on_one_snapshot() {
    let dir = baseline(8, 1030);
    let db = SharedDatabase::open(&dir.0).unwrap();
    for _ in 0..3 {
        db.apply(1, 1, fields(8, 7)).unwrap();
    }
    let mut cursor = db
        .history(
            1,
            1,
            HistoryOptions {
                first_version: Some(1022),
                last_version: Some(1033),
                max_events: 3,
                max_bytes: 2048,
                ..HistoryOptions::default()
            },
        )
        .unwrap();
    db.write(|tx| tx.retain_last(1, 1, 1)).unwrap();
    db.compact().unwrap();
    db.checkpoint().unwrap();
    let mut versions = Vec::new();
    while let Some(batch) = cursor.next_batch().unwrap() {
        assert!(batch.len() <= 3);
        assert!(batch.memory_bytes() <= 2048);
        assert!(db.resource_usage().read_memory_bytes >= batch.memory_bytes());
        versions.extend(batch.iter().map(|event| event.version));
    }
    assert_eq!(versions, (1022..=1033).collect::<Vec<_>>());
    assert_eq!(db.resource_usage().read_memory_bytes, 0);
    assert_eq!(db.resource_usage().snapshots, 0);
    assert!(cursor.next_batch().unwrap().is_none());
}

#[test]
fn byte_limits_stop_before_the_next_event_and_owned_batches_keep_their_budget() {
    let dir = baseline(1024, 4);
    let db = Database::open(&dir.0).unwrap();
    let mut cursor = db
        .history(
            1,
            1,
            HistoryOptions {
                max_events: 99,
                max_bytes: 1700,
                ..HistoryOptions::default()
            },
        )
        .unwrap();
    let first = cursor.next_batch().unwrap().unwrap();
    assert_eq!(first.len(), 1);
    let second = cursor.next_batch().unwrap().unwrap();
    assert_eq!(second.iter().next().unwrap().version, 2);
    assert_eq!(
        db.resource_usage().read_memory_bytes,
        first.memory_bytes() + second.memory_bytes()
    );
    drop(cursor);
    assert_eq!(db.resource_usage().snapshots, 0);
    assert!(db.resource_usage().read_memory_bytes > 0);
    drop(first);
    drop(second);
    assert_eq!(db.resource_usage().read_memory_bytes, 0);
    let mut too_small = db
        .history(
            1,
            1,
            HistoryOptions {
                max_bytes: 100,
                ..HistoryOptions::default()
            },
        )
        .unwrap();
    assert!(matches!(
        too_small.next_batch(),
        Err(Error::LimitExceeded {
            resource: "history batch bytes",
            ..
        })
    ));
    assert_eq!(db.resource_usage().read_memory_bytes, 0);
    assert_eq!(db.resource_usage().scratch_bytes, 0);
    assert!(too_small.next_batch().unwrap().is_none());
}

#[test]
fn concurrent_batches_are_admitted_against_one_pool_and_release_on_error() {
    let dir = baseline(8192, 4);
    let db = Database::open_with_options(
        &dir.0,
        Options {
            limits: Limits {
                max_read_memory_bytes: 12_000,
                ..Limits::default()
            },
            ..Options::default()
        },
    )
    .unwrap();
    let mut cursor = db
        .history(
            1,
            1,
            HistoryOptions {
                max_events: 1,
                ..HistoryOptions::default()
            },
        )
        .unwrap();
    let held = cursor.next_batch().unwrap().unwrap();
    assert!(matches!(
        cursor.next_batch(),
        Err(Error::LimitExceeded {
            resource: "decoded read bytes",
            ..
        })
    ));
    assert_eq!(db.resource_usage().read_memory_bytes, held.memory_bytes());
    assert_eq!(db.resource_usage().snapshots, 0);
    drop(held);
    let mut next = db
        .history(
            1,
            1,
            HistoryOptions {
                first_version: Some(2),
                max_events: 1,
                ..HistoryOptions::default()
            },
        )
        .unwrap();
    assert_eq!(
        next.next_batch()
            .unwrap()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .version,
        2
    );
}

#[test]
fn cancellation_expiry_empty_history_and_invalid_ranges_release_pins() {
    let dir = baseline(8, 4);
    let db = SharedDatabase::open_with_options(
        &dir.0,
        Options {
            timeouts: Timeouts {
                snapshot: Duration::from_millis(120),
                reap_interval: Duration::from_millis(5),
                ..Timeouts::default()
            },
            ..Options::default()
        },
    )
    .unwrap();
    let token = CancellationToken::default();
    let mut cursor = db
        .history(
            1,
            1,
            HistoryOptions {
                cancellation: token.clone(),
                ..HistoryOptions::default()
            },
        )
        .unwrap();
    std::thread::spawn(move || token.cancel()).join().unwrap();
    assert!(matches!(cursor.next_batch(), Err(Error::Cancelled)));
    assert_eq!(db.resource_usage().snapshots, 0);
    let mut expired = db.history(1, 1, HistoryOptions::default()).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert!(matches!(
        expired.next_batch(),
        Err(Error::TransactionExpired)
    ));
    assert_eq!(db.resource_usage().snapshots, 0);
    assert!(matches!(
        db.history(
            1,
            1,
            HistoryOptions {
                first_version: Some(0),
                ..HistoryOptions::default()
            }
        ),
        Err(Error::VersionUnavailable { .. })
    ));
    assert!(matches!(
        db.history(
            1,
            1,
            HistoryOptions {
                first_version: Some(3),
                last_version: Some(2),
                ..HistoryOptions::default()
            }
        ),
        Err(Error::Invalid(_))
    ));
    db.create(1, 2, fields(8, 0)).unwrap();
    assert!(
        db.history(1, 2, HistoryOptions::default())
            .unwrap()
            .next_batch()
            .unwrap()
            .is_none()
    );
}

#[test]
fn scratch_decoded_record_and_collector_limits_fail_without_leaks() {
    let dir = baseline(8, 0);
    let mut db = Database::open(&dir.0).unwrap();
    db.apply(1, 1, fields(65536, 0)).unwrap();
    db.apply(1, 1, fields(8, 1)).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    for (limits, resource) in [
        (
            Limits {
                max_scratch_bytes: 1024,
                ..Limits::default()
            },
            "scratch bytes",
        ),
        (
            Limits {
                max_decoded_record_bytes: 1024,
                ..Limits::default()
            },
            "decoded record bytes",
        ),
        (
            Limits {
                max_read_result_bytes: 1024,
                ..Limits::default()
            },
            "read result bytes",
        ),
    ] {
        let db = Database::open_with_options(
            &dir.0,
            Options {
                limits,
                ..Options::default()
            },
        )
        .unwrap();
        assert!(
            matches!(db.events(1, 1), Err(Error::LimitExceeded { resource: actual, .. }) if actual == resource),
            "{resource}"
        );
        assert_eq!(db.resource_usage().read_memory_bytes, 0);
        assert_eq!(db.resource_usage().scratch_bytes, 0);
        assert_eq!(db.resource_usage().snapshots, 0);
    }
}

#[test]
fn small_encoded_updates_cannot_bypass_decoded_staging_limits_and_recovery_is_exempt() {
    let dir = baseline(8192, 0);
    let limits = Limits {
        max_transaction_decoded_bytes: 2000,
        max_resident_decoded_write_bytes: 2000,
        ..Limits::default()
    };
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            limits: limits.clone(),
            ..Options::default()
        },
    )
    .unwrap();
    assert!(matches!(
        db.apply(1, 1, fields(8, 1)),
        Err(Error::LimitExceeded {
            resource: "transaction decoded bytes",
            ..
        })
    ));
    assert_eq!(db.resource_usage().decoded_write_bytes, 0);
    let mut spare = Vec::with_capacity(8192);
    spare.extend([1u8; 8]);
    assert!(matches!(
        db.create(1, 2, [(1, Value::Bytes(spare))].into()),
        Err(Error::LimitExceeded {
            resource: "transaction decoded bytes",
            ..
        })
    ));
    assert_eq!(db.resource_usage().decoded_write_bytes, 0);
    drop(db);
    let mut db = Database::open(&dir.0).unwrap();
    db.apply(1, 1, fields(8192, 2)).unwrap();
    drop(db);
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            limits,
            ..Options::default()
        },
    )
    .unwrap();
    assert!(db.resource_usage().decoded_write_bytes > 2000);
    assert_eq!(db.get(1, 1).unwrap().unwrap().fields, fields(8192, 2));
    db.checkpoint().unwrap();
    assert_eq!(db.resource_usage().decoded_write_bytes, 0);
    db.create(1, 2, fields(8, 1)).unwrap();
}
