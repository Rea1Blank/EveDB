// SPDX-License-Identifier: AGPL-3.0-only

//! Database lifecycle, file storage, history, and corruption recovery tests.

mod common;
use common::{TestDir, fields, schema};
use evedb_core::{DataType, Database, Error, Field, Options, Value};
use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
};

#[test]
fn catalog_only_writes_trigger_automatic_checkpoints() {
    let dir = TestDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            checkpoint_bytes: 4096,
            ..Options::default()
        },
    )
    .unwrap();
    for id in 0..40 {
        db.create_table(&format!("table_{id}"), schema()).unwrap();
    }
    assert!(fs::read_dir(dir.0.join("catalog")).unwrap().count() >= 2);
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.tables().count(), 40);
    assert_eq!(db.sequence(), 40);
}

#[test]
fn atomic_multitable_transactions_survive_wal_reopen() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let (a, b) = db
        .write(|tx| {
            let a = tx.create_table("accounts", schema())?;
            let b = tx.create_table("balances", schema())?;
            tx.create(a, 7, fields(10))?;
            tx.create(b, 9, fields(20))?;
            Ok((a, b))
        })
        .unwrap();
    db.write(|tx| {
        tx.apply(a, 7, fields(11))?;
        tx.apply(a, 7, fields(12))?;
        tx.apply(b, 9, fields(21))?;
        assert_eq!(tx.get(a, 7)?.unwrap().version, 2);
        Ok(())
    })
    .unwrap();
    assert_eq!(db.sequence(), 2);
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.get(a, 7).unwrap().unwrap().fields[&1], Value::Int64(12));
    assert_eq!(db.get(b, 9).unwrap().unwrap().fields[&1], Value::Int64(21));
    assert_eq!(
        db.get_at_version(a, 7, 1).unwrap().fields[&1],
        Value::Int64(11)
    );
    assert_eq!(
        db.events(a, 7)
            .unwrap()
            .iter()
            .map(|e| e.transaction)
            .collect::<Vec<_>>(),
        [2, 2]
    );
    assert_eq!(db.replay(a, 7).unwrap(), db.get(a, 7).unwrap().unwrap());
}

#[test]
fn abort_and_invalid_assignment_do_not_publish_any_changes() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    let before = db.sequence();
    {
        let mut tx = db.transaction().unwrap();
        tx.apply(table, 1, fields(2)).unwrap();
        assert!(tx.apply(table, 1, [(1, Value::Null)].into()).is_err());
        assert!(tx.commit().is_err());
    }
    {
        let mut tx = db.transaction().unwrap();
        tx.create(table, 2, fields(2)).unwrap();
    }
    assert_eq!(db.sequence(), before);
    assert_eq!(
        db.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(1)
    );
    assert!(db.get(table, 2).unwrap().is_none());
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.sequence(), before);
    assert!(db.events(table, 1).unwrap().is_empty());
}

#[test]
fn btree_point_reads_and_scans_work_across_leaf_boundaries() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.write(|tx| {
        for id in (0..900).rev() {
            tx.create(table, id, fields(id as i64))?;
        }
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open(&dir.0).unwrap();
    for id in [0, 1, 253, 254, 255, 508, 899] {
        assert_eq!(
            db.get(table, id).unwrap().unwrap().fields[&1],
            Value::Int64(id as i64)
        );
    }
    assert!(db.get(table, 900).unwrap().is_none());
    db.apply(table, 254, fields(-1)).unwrap();
    db.create(table, 1000, fields(1000)).unwrap();
    db.delete(table, 255).unwrap();
    let mut ids = Vec::new();
    db.scan(table, |entity| {
        ids.push(entity.id);
        Ok(())
    })
    .unwrap();
    assert_eq!(ids.len(), 900);
    assert!(!ids.contains(&255));
    assert_eq!(ids.last(), Some(&1000));
    assert!(ids.windows(2).all(|w| w[0] < w[1]));
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(
        db.get(table, 254).unwrap().unwrap().fields[&1],
        Value::Int64(-1)
    );
    assert!(db.get(table, 255).unwrap().is_none());
}

#[test]
fn overflow_records_and_rotating_history_segments_survive_checkpoint() {
    let dir = TestDir::new();
    let options = Options {
        history_segment_bytes: 4096,
        compress_history: false,
        ..Options::default()
    };
    let mut db = Database::open_with_options(&dir.0, options).unwrap();
    let table = db.create_table("blobs", schema()).unwrap();
    let payload: Vec<u8> = (0..180_000).map(|i| (i % 251) as u8).collect();
    let mut initial = fields(0);
    initial.insert(3, Value::Bytes(payload.clone()));
    db.create(table, 1, initial).unwrap();
    db.write(|tx| {
        for i in 1..=3 {
            tx.apply(table, 1, [(3, Value::Bytes(vec![i as u8; 30_000]))].into())?;
        }
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(
        db.get_at_version(table, 1, 0).unwrap().fields[&3],
        Value::Bytes(payload)
    );
    assert_eq!(
        db.get(table, 1).unwrap().unwrap().fields[&3],
        Value::Bytes(vec![3; 30_000])
    );
    let history = dir
        .0
        .join("tables/00000000000000000001/00000000000000000001/history");
    assert!(fs::read_dir(history).unwrap().count() >= 3);
}

#[test]
fn retention_snapshots_nulls_and_deletion_keep_consistent_history() {
    let dir = TestDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            retain_events: Some(3),
            snapshot_interval: 2,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0)).unwrap();
    db.write(|tx| {
        for value in 1..=8 {
            tx.apply(table, 1, fields(value))?;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(db.retained_range(table, 1).unwrap(), (5, 8));
    assert_eq!(db.events(table, 1).unwrap().len(), 3);
    assert!(matches!(
        db.get_at_version(table, 1, 4),
        Err(Error::VersionUnavailable {
            first: 5,
            last: 8,
            ..
        })
    ));
    for version in 5..=8 {
        assert_eq!(
            db.get_at_version(table, 1, version).unwrap(),
            db.replay_to_version(table, 1, version).unwrap()
        );
    }
    db.checkpoint().unwrap();
    drop(db);
    // Recovery must reproduce recorded retention, independent of options used to reopen.
    let mut db = Database::open(&dir.0).unwrap();
    assert_eq!(db.retained_range(table, 1).unwrap(), (5, 8));
    db.apply(table, 1, [(2, Value::Text("label".into()))].into())
        .unwrap();
    db.apply(table, 1, [(2, Value::Null)].into()).unwrap();
    assert_eq!(
        db.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(8)
    );
    assert_eq!(db.get(table, 1).unwrap().unwrap().fields[&2], Value::Null);
    db.delete(table, 1).unwrap();
    assert!(db.get(table, 1).unwrap().is_none());
    assert!(db.replay(table, 1).unwrap().deleted);
    assert!(db.create(table, 1, fields(0)).is_err());
    db.retain_last(table, 1, 0).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.retained_range(table, 1).unwrap(), (11, 11));
    assert!(db.replay(table, 1).unwrap().deleted);
}

#[test]
fn rename_and_additive_schema_evolution_preserve_old_versions() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("old", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    db.checkpoint().unwrap();
    db.write(|tx| {
        tx.rename_table(table, "new")?;
        let mut fields = schema().fields;
        fields[0].name = "renamed".into();
        fields.push(Field {
            id: 4,
            name: "enabled".into(),
            data_type: DataType::Bool,
            nullable: true,
        });
        tx.alter_table(table, fields)?;
        tx.apply(table, 1, [(4, Value::Bool(true))].into())?;
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert!(db.table("old").is_none());
    assert_eq!(db.table("new").unwrap().id, table);
    let old = db.get_at_version(table, 1, 0).unwrap();
    assert_eq!(old.schema_version, 1);
    assert!(!old.fields.contains_key(&4));
    assert_eq!(db.get(table, 1).unwrap().unwrap().schema_version, 2);
    assert_eq!(db.replay(table, 1).unwrap().fields[&4], Value::Bool(true));
}

#[test]
fn exclusive_lock_is_released_when_handle_drops() {
    let dir = TestDir::new();
    let db = Database::open(&dir.0).unwrap();
    assert!(matches!(Database::open(&dir.0), Err(Error::Locked)));
    drop(db);
    assert!(Database::open(&dir.0).is_ok());
}

fn flip(path: &std::path::Path, offset: u64) {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn damaged_latest_checkpoint_is_rebuilt_from_previous_and_wal() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    db.checkpoint().unwrap();
    db.apply(table, 1, fields(2)).unwrap();
    db.checkpoint().unwrap();
    db.apply(table, 1, fields(3)).unwrap();
    let sequence = db.sequence();
    drop(db);
    flip(
        &dir.0
            .join("tables/00000000000000000001/00000000000000000002/current.pages"),
        8191,
    );
    let mut db = Database::open(&dir.0).unwrap();
    assert_eq!(db.sequence(), sequence);
    assert_eq!(
        db.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(3)
    );
    db.checkpoint().unwrap();
    drop(db);
    flip(
        &dir.0
            .join("tables/00000000000000000001/00000000000000000003/current.pages"),
        8191,
    );
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.sequence(), sequence);
    assert_eq!(db.replay(table, 1).unwrap().fields[&1], Value::Int64(3));
}

#[test]
fn committed_wal_corruption_is_an_error_instead_of_silent_rollback() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    db.create_table("items", schema()).unwrap();
    drop(db);
    let path = dir.0.join("wal/00000000000000000000.wal");
    let end = fs::metadata(&path).unwrap().len();
    flip(&path, end - 9);
    assert!(matches!(Database::open(&dir.0), Err(Error::Corrupt(_))));
}

#[test]
fn repeated_checkpoints_reclaim_old_generations_without_losing_inactive_entities() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    db.create(table, 2, fields(20)).unwrap();
    for value in 2..=5 {
        db.apply(table, 1, fields(value)).unwrap();
        db.retain_last(table, 1, 1).unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(fs::read_dir(dir.0.join("wal")).unwrap().count(), 2);
    let directories = || {
        fs::read_dir(dir.0.join("tables/00000000000000000001"))
            .unwrap()
            .count()
    };
    // Entity 2 was never touched, so it still lives where it was first written
    // and its generation is still referenced.
    let first = db.generations()[0];
    assert_eq!((first.entities, first.dead), (2, 1));
    assert!(directories() <= Options::default().max_generations + 1);
    // Collecting everything leaves one generation, plus the retained baseline
    // until a second pass replaces that too.
    db.compact().unwrap();
    assert_eq!(db.generations().len(), 1);
    db.compact().unwrap();
    assert_eq!(directories(), 2);
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(
        db.get(table, 2).unwrap().unwrap().fields[&1],
        Value::Int64(20)
    );
    assert_eq!(db.retained_range(table, 1).unwrap(), (3, 4));
}

#[test]
fn damage_to_a_shared_file_is_reported_rather_than_recovered() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1)).unwrap();
    db.create(table, 2, fields(2)).unwrap();
    db.checkpoint().unwrap();
    let shared = db.generations()[0].generation;
    db.apply(table, 1, fields(3)).unwrap();
    db.checkpoint().unwrap();
    // Entity 2 never moved, so both the published and the retained manifest read
    // it from the same file. Sharing files is what makes a checkpoint cheap; it
    // also means the retained checkpoint is no longer an independent copy.
    assert!(db.generations().iter().any(|e| e.generation == shared));
    drop(db);
    flip(
        &dir.0
            .join("tables/00000000000000000001")
            .join(format!("{shared:020}"))
            .join("current.pages"),
        8191,
    );
    assert!(matches!(Database::open(&dir.0), Err(Error::Corrupt(_))));
}

#[test]
fn an_untouched_entity_is_not_rewritten_by_a_checkpoint() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    for id in 1..=4 {
        db.create(table, id, fields(id as i64)).unwrap();
    }
    db.checkpoint().unwrap();
    let original = db.generations()[0].generation;
    db.apply(table, 1, fields(100)).unwrap();
    db.checkpoint().unwrap();
    let generations = db.generations();
    assert_eq!(generations.len(), 2);
    // The first generation kept all four records and lost one to the rewrite.
    assert_eq!(generations[0].generation, original);
    assert_eq!((generations[0].entities, generations[0].dead), (4, 1));
    // The new generation holds only the entity the transaction touched.
    assert_eq!((generations[1].entities, generations[1].dead), (1, 0));
    assert_eq!(
        db.get(table, 4).unwrap().unwrap().fields[&1],
        Value::Int64(4)
    );
    assert_eq!(
        db.get(table, 1).unwrap().unwrap().fields[&1],
        Value::Int64(100)
    );
}

#[test]
fn a_generation_is_collected_once_it_loses_density() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    for id in 1..=4 {
        db.create(table, id, fields(id as i64)).unwrap();
    }
    db.checkpoint().unwrap();
    let original = db.generations()[0].generation;
    // Three of four entities move away, which drops the generation below half.
    for id in 1..=3 {
        db.apply(table, id, fields(100 + id as i64)).unwrap();
        db.checkpoint().unwrap();
    }
    // The policy reads the published manifest, so the generation that just lost
    // its third entity is collected by the checkpoint that follows.
    assert!(
        db.generations()
            .iter()
            .any(|entry| entry.generation == original)
    );
    db.checkpoint().unwrap();
    assert!(
        db.generations()
            .iter()
            .all(|entry| entry.generation != original),
        "a generation holding one live entity out of four must be collected"
    );
    for id in 1..=4 {
        let expected = if id <= 3 { 100 + id } else { id };
        assert_eq!(
            db.get(table, id as u64).unwrap().unwrap().fields[&1],
            Value::Int64(expected)
        );
    }
}

#[test]
fn collection_is_spread_over_checkpoints_when_it_exceeds_its_budget() {
    let dir = TestDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            // Mark the first generation for collection as soon as it loses one
            // entity, then let only two entities move per checkpoint.
            compact_live_ratio: 0.9,
            collect_entities: 2,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    for id in 1..=7 {
        db.create(table, id, fields(id as i64)).unwrap();
    }
    db.checkpoint().unwrap();
    let draining = db.generations()[0].generation;
    db.apply(table, 1, fields(100)).unwrap();
    let live = |db: &Database| {
        db.generations()
            .iter()
            .find(|entry| entry.generation == draining)
            .map(|entry| entry.entities - entry.dead)
    };
    // The first checkpoint publishes the rewritten entity, which is what marks
    // the generation for collection; the remaining six then move two at a time.
    let mut seen = Vec::new();
    for _ in 0..4 {
        db.checkpoint().unwrap();
        seen.push(live(&db));
    }
    assert_eq!(seen, [Some(6), Some(4), Some(2), None]);
    for id in 1..=7 {
        let expected = if id == 1 { 100 } else { id };
        assert_eq!(
            db.get(table, id as u64).unwrap().unwrap().fields[&1],
            Value::Int64(expected)
        );
    }
}

#[test]
fn churn_keeps_every_entity_readable_and_the_database_bounded() {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    let entities = 200u64;
    db.write(|tx| {
        for id in 1..=entities {
            tx.create(table, id, fields(0))?;
        }
        Ok(())
    })
    .unwrap();
    db.compact().unwrap();
    db.compact().unwrap();
    let mut expected = vec![0i64; entities as usize + 1];
    let mut random = 12345u64;
    let mut halfway = 0;
    for round in 1..=80i64 {
        for _ in 0..20 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let id = random % entities + 1;
            db.apply(table, id, fields(round)).unwrap();
            expected[id as usize] = round;
        }
        db.checkpoint().unwrap();
        assert!(db.generations().len() <= Options::default().max_generations);
        if round == 40 {
            halfway = size(&dir.0);
        }
    }
    // Every entity reads back, whichever generation now holds it.
    for id in 1..=entities {
        assert_eq!(
            db.get(table, id).unwrap().unwrap().fields[&1],
            Value::Int64(expected[id as usize])
        );
    }
    // Superseded records are reclaimed as generations are collected, so churn
    // settles instead of growing with the number of checkpoints. The steady
    // state is larger than a compacted database: it carries the superseded
    // records a collection has not reached yet, and every referenced generation
    // costs at least a few pages per table.
    let churned = size(&dir.0);
    assert!(
        churned < halfway * 3 / 2,
        "{churned} bytes after 80 rounds against {halfway} after 40"
    );
    drop(db);
    let mut db = Database::open(&dir.0).unwrap();
    db.compact().unwrap();
    db.compact().unwrap();
    assert_eq!(db.generations().len(), 1);
    // A full pass reclaims what the bounded collections had not reached.
    assert!(size(&dir.0) < churned);
    for id in 1..=entities {
        assert_eq!(
            db.get(table, id).unwrap().unwrap().fields[&1],
            Value::Int64(expected[id as usize])
        );
    }
}

fn size(path: &std::path::Path) -> u64 {
    let mut total = 0;
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        total += if entry.file_type().unwrap().is_dir() {
            size(&entry.path())
        } else {
            entry.metadata().unwrap().len()
        };
    }
    total
}

#[test]
fn the_generation_budget_bounds_a_manifest() {
    let dir = TestDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            // Keep every generation dense so only the budget can collect one.
            compact_live_ratio: 0.0,
            max_generations: 3,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    for id in 1..=20 {
        db.create(table, id, fields(id as i64)).unwrap();
        db.checkpoint().unwrap();
        assert!(db.generations().len() <= 3);
    }
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    for id in 1..=20 {
        assert_eq!(
            db.get(table, id).unwrap().unwrap().fields[&1],
            Value::Int64(id as i64)
        );
    }
}

#[test]
fn historical_reads_seek_to_a_snapshot_and_skip_earlier_events() {
    let dir = TestDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            snapshot_interval: 2,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0)).unwrap();
    db.write(|tx| {
        for value in 1..=600 {
            tx.apply(table, 1, fields(value))?;
        }
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    for version in [0, 1, 253, 254, 509, 510, 599, 600] {
        assert_eq!(
            db.get_at_version(table, 1, version).unwrap().fields[&1],
            Value::Int64(version as i64)
        );
    }
    // Damage an early event after startup verification. A late historical read
    // should touch only its eligible snapshot and the subsequent event range.
    flip(
        &dir.0.join(
            "tables/00000000000000000001/00000000000000000001/history/00000000000000000000.events",
        ),
        35,
    );
    assert_eq!(
        db.get_at_version(table, 1, 599).unwrap().fields[&1],
        Value::Int64(599)
    );
    assert!(db.replay_to_version(table, 1, 599).is_err());
}
