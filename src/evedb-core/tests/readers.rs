// SPDX-License-Identifier: AGPL-3.0-only

//! Concurrent read visibility and snapshot resource lifetimes.

mod common;
use common::{TestDir, fields, schema};
use evedb_core::{Database, Error, ReadSnapshot, Reader, Value};
use std::sync::{Arc, Barrier};

#[test]
fn snapshots_pin_catalog_history_files_and_directory_ownership() {
    fn shareable<T: Send + Sync + Clone>() {}
    shareable::<Reader>();
    shareable::<ReadSnapshot>();
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    db.apply(table, 1, fields(2)).unwrap();
    db.checkpoint().unwrap();
    let reader = db.reader();
    let old = reader.pin().unwrap();
    let sequence = old.sequence();
    let generation = db.generations().last().unwrap().generation;
    let old_path = dir.0.join(format!("tables/{table:020}/{generation:020}"));
    let stats = reader.snapshot_stats();
    assert_eq!(stats.active, 1);
    assert_eq!(stats.oldest_sequence, Some(sequence));
    assert!(stats.referenced_bytes > 0);
    db.write(|tx| {
        tx.rename_table(table, "renamed")?;
        tx.retain_last(table, 1, 0)?;
        tx.delete(table, 1)?;
        tx.create(table, 2, fields(20))
    })
    .unwrap();
    for _ in 0..3 {
        db.compact().unwrap();
    }
    assert!(old_path.exists());
    assert_eq!(
        old.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(2)
    );
    assert!(old.get(table, 2).unwrap().is_none());
    assert_eq!(
        old.get_at_version(table, 1, 0).unwrap().fields[&1],
        Value::Int64(1)
    );
    assert_eq!(old.replay(table, 1).unwrap().version, 1);
    assert_eq!(old.replay_to_version(table, 1, 0).unwrap().version, 0);
    assert_eq!(old.events(table, 1).unwrap().len(), 1);
    assert_eq!(old.retained_range(table, 1).unwrap(), (0, 1));
    let mut ids = Vec::new();
    old.scan(table, |entity| {
        ids.push(entity.id);
        Ok(())
    })
    .unwrap();
    assert_eq!(ids, [1]);
    assert!(old.table("items").is_some());
    assert!(reader.table("items").unwrap().is_none());
    assert!(reader.get(table, 1).unwrap().is_none());
    assert!(reader.get(table, 2).unwrap().is_some());
    drop(old);
    assert_eq!(reader.snapshot_stats().active, 0);
    db.checkpoint().unwrap();
    assert!(!old_path.exists());
    let current = reader.pin().unwrap();
    drop(db);
    assert!(matches!(Database::open(&dir.0), Err(Error::Locked)));
    drop(reader);
    assert!(current.get(table, 2).unwrap().is_some());
    assert!(matches!(Database::open(&dir.0), Err(Error::Locked)));
    drop(current);
    assert!(
        Database::open(&dir.0)
            .unwrap()
            .get(table, 2)
            .unwrap()
            .is_some()
    );
}

#[test]
fn readers_progress_while_a_writer_stages_and_commits() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.write(|tx| {
        tx.create(table, 1, fields(0))?;
        tx.create(table, 2, fields(0))
    })
    .unwrap();
    let reader = db.reader();
    let pinned = reader.pin().unwrap();
    let barrier = Arc::new(Barrier::new(5));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let reader = reader.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..200 {
                    let view = reader.pin().unwrap();
                    let a = view.get(table, 1).unwrap().unwrap();
                    let b = view.get(table, 2).unwrap().unwrap();
                    assert_eq!(a.fields, b.fields);
                    assert_eq!(a.version, b.version);
                }
            });
        }
        let mut tx = db.transaction().unwrap();
        tx.apply(table, 1, fields(1)).unwrap();
        barrier.wait();
        assert_eq!(
            reader.get(table, 1).unwrap().unwrap().fields[&1],
            Value::Int64(0)
        );
        tx.apply(table, 2, fields(1)).unwrap();
        tx.commit().unwrap();
        for value in 2..20 {
            db.write(|tx| {
                tx.apply(table, 1, fields(value))?;
                tx.apply(table, 2, fields(value))
            })
            .unwrap();
            db.checkpoint().unwrap();
        }
    });
    assert_eq!(
        pinned.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(0)
    );
    assert_eq!(
        reader.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(19)
    );
}
