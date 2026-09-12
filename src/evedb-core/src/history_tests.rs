// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use crate::{DataType, Field, Value, test_support::TempDir};
fn schema() -> Schema {
    Schema::new(vec![Field {
        id: 1,
        name: "payload".into(),
        data_type: DataType::Bytes,
        nullable: false,
    }])
    .unwrap()
}
fn fields(byte: u8, size: usize) -> Fields {
    [(1, Value::Bytes(vec![byte; size]))].into()
}

#[test]
fn streaming_detects_missing_versions_and_stops_cooperatively_between_records() {
    let dir = TempDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0, 8)).unwrap();
    for i in 1..=4 {
        db.apply(table, 1, fields(i, 8)).unwrap();
    }
    let mut damaged = (**db.state.overlay.get(&(table, 1)).unwrap()).clone();
    damaged.committed.remove(&2);
    let healthy = db.state.clone();
    Arc::make_mut(&mut db.state)
        .overlay
        .insert((table, 1), Arc::new(damaged));
    db.readers.publish(db.state.clone());
    let mut cursor = db
        .history(table, 1, crate::HistoryOptions::default())
        .unwrap();
    assert!(matches!(cursor.next_batch(), Err(Error::Corrupt(_))));
    assert_eq!(db.resource_usage().read_memory_bytes, 0);
    assert_eq!(db.resource_usage().snapshots, 0);
    db.state = healthy;
    db.readers.publish(db.state.clone());
    db.checkpoint().unwrap();
    let mut checks = 0;
    let mut events = Vec::new();
    let result = db.state.root.visit_events(
        &db.state.pager,
        table,
        1,
        1,
        4,
        &mut || {
            checks += 1;
            if checks == 2 {
                Err(Error::Cancelled)
            } else {
                Ok(())
            }
        },
        &mut |_| Ok(true),
        &mut |event| {
            events.push(event);
            Ok(true)
        },
    );
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(events.len(), 1);
    assert!(db.resource_usage().read_memory_bytes > 0);
    drop(events);
    assert_eq!(db.resource_usage().read_memory_bytes, 0);
    assert_eq!(db.resource_usage().scratch_bytes, 0);
}

#[test]
fn unchanged_primary_and_history_partitions_keep_their_files_and_survive_slot_remapping() {
    let dir = TempDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            snapshot_interval: 32,
            checkpoint_bytes: 64 * 1024 * 1024,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(1, 8)).unwrap();
    db.write(|tx| {
        for i in 1..=2050 {
            tx.apply(table, 1, fields((i % 251) as u8, 8))?;
        }
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap();
    db.create(table, 4096, fields(9, 8)).unwrap();
    db.checkpoint().unwrap();
    let first = &db.state.root.partitions[&(table, 4, [1, 0])];
    let second = &db.state.root.partitions[&(table, 4, [1, 1024])];
    assert_eq!(
        first.file, second.file,
        "small trees share one immutable file"
    );
    assert_ne!(first.base, second.base, "each tree has its own direct root");
    let routes = [
        (table, 3, [4096, 0]),
        (table, 4, [1, 0]),
        (table, 4, [1, 1024]),
        (table, 5, [1, 0]),
    ];
    let before: Vec<_> = routes
        .iter()
        .map(|route| db.state.root.partitions[route].file)
        .collect();
    let pin = db.reader().pin().unwrap();
    db.apply(table, 1, fields(7, 8)).unwrap();
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    for (route, file) in routes.into_iter().zip(before) {
        assert_eq!(db.state.root.partitions[&route].file, file, "{route:?}");
    }
    assert_eq!(db.get(table, 4096).unwrap().unwrap().fields, fields(9, 8));
    for version in [0, 1, 1023, 1024, 1025, 2047, 2048, 2050, 2051] {
        assert_eq!(
            db.get_at_version(table, 1, version).unwrap(),
            db.replay_to_version(table, 1, version).unwrap()
        );
    }
    db.retain_last(table, 1, 100).unwrap();
    db.checkpoint().unwrap();
    assert!(!db.state.root.partitions.contains_key(&(table, 4, [1, 0])));
    assert_eq!(db.events(table, 1).unwrap().len(), 100);
    db.compact().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(pin.events(table, 1).unwrap().len(), 2050);
    drop(pin);
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.events(table, 1).unwrap().len(), 100);
    assert_eq!(db.get(table, 4096).unwrap().unwrap().fields, fields(9, 8));
}

#[test]
fn partition_routes_cover_sparse_maximum_ids_and_admit_before_extra_index_files() {
    let dir = TempDir::new();
    let options = Options {
        snapshot_interval: 0,
        limits: crate::Limits {
            max_index_partitions: 2,
            ..crate::Limits::default()
        },
        ..Options::default()
    };
    let mut db = Database::open_with_options(&dir.0, options).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    for id in [0, 1023, u64::MAX] {
        db.create(table, id, fields(1, 8)).unwrap();
    }
    db.checkpoint().unwrap();
    let mut ids = Vec::new();
    db.scan(table, |entity| {
        ids.push(entity.id);
        Ok(())
    })
    .unwrap();
    assert_eq!(ids, [0, 1023, u64::MAX]);
    db.create(table, 1024, fields(2, 8)).unwrap();
    assert!(matches!(
        db.checkpoint(),
        Err(Error::LimitExceeded {
            resource: "index partitions",
            ..
        })
    ));
    drop(db);
    let mut db = Database::open(&dir.0).unwrap();
    assert!(db.get(table, 1024).unwrap().is_some());
    db.checkpoint().unwrap();
    assert!(db.get(table, u64::MAX).unwrap().is_some());
}
#[test]
fn writes_load_only_current_and_base_and_share_committed_event_payloads() {
    let dir = TempDir::new();
    let options = Options {
        snapshot_interval: 0,
        compress_history: false,
        checkpoint_bytes: 64 * 1024 * 1024,
        ..Options::default()
    };
    let mut db = Database::open_with_options(&dir.0, options.clone()).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0, 32)).unwrap();
    db.write(|tx| {
        for value in 1..=100 {
            tx.apply(table, 1, fields(value, 8192))?;
        }
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open_with_options(&dir.0, options).unwrap();
    assert_eq!(db.state.pager.stats().open_files, 0);
    let mut tx = db.transaction().unwrap();
    tx.apply(table, 1, fields(101, 16)).unwrap();
    tx.data
        .with(|state| {
            let data = &state.staged[&(table, 1)];
            assert!(data.disk.is_some());
            assert_eq!(data.committed.range(..).count(), 0);
            assert_eq!(data.events.len(), 1);
            Ok(())
        })
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.state.pager.stats().open_files,
        3,
        "only primary, current and base files should be read"
    );
    let payload = db
        .state
        .overlay
        .get(&(table, 1))
        .unwrap()
        .committed
        .get(&101)
        .unwrap()
        .clone();
    for value in 102..=120 {
        db.apply(table, 1, fields(value, 16)).unwrap();
    }
    let latest = db.state.overlay.get(&(table, 1)).unwrap();
    assert!(Arc::ptr_eq(&payload, latest.committed.get(&101).unwrap()));
    assert!(latest.events.is_empty());
    assert_eq!(db.events(table, 1).unwrap().len(), 120);
    assert_eq!(
        db.replay(table, 1).unwrap(),
        db.get(table, 1).unwrap().unwrap()
    );
}
#[test]
fn checkpoints_reuse_old_history_payloads_after_current_generations_are_collected() {
    let dir = TempDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            snapshot_interval: 0,
            compress_history: false,
            max_generations: 2,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0, 16)).unwrap();
    db.write(|tx| {
        for value in 1..=50 {
            tx.apply(table, 1, fields(value, 8192))?;
        }
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap();
    let first = db.state.root.generation;
    let history: Vec<_> = db
        .state
        .root
        .file_sizes()
        .filter(|(file, size)| file.kind == 6 && *size > 0)
        .collect();
    for value in 51..=55 {
        db.apply(table, 1, fields(value, 16)).unwrap();
        db.checkpoint().unwrap();
        let new_bytes: u64 = db
            .state
            .root
            .file_sizes()
            .filter(|(file, _)| file.kind == 6 && file.generation == db.state.root.generation)
            .map(|(_, size)| size)
            .sum();
        assert!(
            new_bytes < 256,
            "one small new event must not rewrite old payloads"
        );
    }
    assert!(
        !db.state
            .root
            .generations
            .iter()
            .any(|entry| entry.generation == first)
    );
    for (file, bytes) in history {
        assert_eq!(fs::metadata(file.path(&dir.0)).unwrap().len(), bytes);
        assert!(db.state.root.file_ids().any(|entry| entry == file));
    }
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.events(table, 1).unwrap().len(), 55);
    assert_eq!(
        db.get_at_version(table, 1, 25).unwrap().fields,
        fields(25, 8192)
    );
}

#[test]
fn a_staged_writer_survives_compaction_without_pinning_obsolete_history_after_commit() {
    let dir = TempDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            snapshot_interval: 0,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0, 16)).unwrap();
    for value in 1..=10 {
        db.apply(table, 1, fields(value, 128)).unwrap();
    }
    db.checkpoint().unwrap();
    let db = db.into_shared();
    let mut writer = db.transaction().unwrap();
    writer.snapshot(table, 1).unwrap();
    writer.apply(table, 1, fields(11, 16)).unwrap();
    db.compact().unwrap();
    db.compact().unwrap();
    writer.commit().unwrap();
    let pinned = db.read_snapshot().unwrap();
    db.checkpoint().unwrap();
    db.compact().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(pinned.events(table, 1).unwrap().len(), 11);
    assert_eq!(
        pinned.get_at_version(table, 1, 5).unwrap().fields,
        fields(5, 128)
    );
    assert_eq!(pinned.replay(table, 1).unwrap().fields, fields(11, 16));
    drop(pinned);
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.events(table, 1).unwrap().len(), 11);
}
#[test]
fn retention_reclaims_unreferenced_segments_only_after_pinned_views_release_them() {
    let dir = TempDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            snapshot_interval: 0,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0, 16)).unwrap();
    for value in 1..=10 {
        db.apply(table, 1, fields(value, 128)).unwrap();
    }
    db.checkpoint().unwrap();
    let old_files: Vec<_> = db
        .state
        .root
        .file_sizes()
        .filter(|(file, bytes)| file.kind == 6 && *bytes > 0)
        .map(|(file, _)| file.path(&dir.0))
        .collect();
    let pinned = db.read_snapshot().unwrap();
    db.retain_last(table, 1, 0).unwrap();
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    assert!(db.events(table, 1).unwrap().is_empty());
    assert_eq!(db.retained_range(table, 1).unwrap(), (10, 10));
    assert_eq!(pinned.events(table, 1).unwrap().len(), 10);
    assert!(old_files.iter().all(|path| path.exists()));
    drop(pinned);
    db.checkpoint().unwrap();
    assert!(old_files.iter().all(|path| !path.exists()));
}
#[test]
fn automatic_checkpoint_rebinds_local_history_and_keeps_budget_ownership() {
    let dir = TempDir::new();
    let mut db = Database::open_with_options(
        &dir.0,
        Options {
            checkpoint_bytes: 4096,
            snapshot_interval: 0,
            compress_history: false,
            ..Options::default()
        },
    )
    .unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0, 16)).unwrap();
    for value in 1..=25 {
        db.apply(table, 1, fields(value, 4096)).unwrap();
    }
    assert!(db.generations().last().unwrap().generation > 1);
    assert_eq!(db.events(table, 1).unwrap().len(), 25);
    assert_eq!(db.replay(table, 1).unwrap().fields, fields(25, 4096));
    let data = db.state.overlay.get(&(table, 1)).unwrap();
    assert_eq!(
        data.disk.as_ref().unwrap().root.generation,
        db.state.root.generation
    );
    assert!(data.committed.range(..).count() <= 2);
    db.checkpoint().unwrap();
    assert_eq!(db.resource_usage().resident_write_bytes, 0);
}
#[test]
fn old_control_format_is_rejected_without_rewriting_user_files() {
    let dir = TempDir::new();
    fs::write(dir.0.join("control"), b"EVEDB002").unwrap();
    fs::write(dir.0.join("sentinel"), b"existing data").unwrap();
    assert!(matches!(Database::open(&dir.0), Err(Error::Corrupt(_))));
    assert_eq!(fs::read(dir.0.join("control")).unwrap(), b"EVEDB002");
    assert_eq!(fs::read(dir.0.join("sentinel")).unwrap(), b"existing data");
}

#[test]
fn history_file_budget_fails_before_publication_and_compaction_restores_progress() {
    let dir = TempDir::new();
    let options = Options {
        snapshot_interval: 0,
        limits: crate::Limits {
            max_history_files: 3,
            ..crate::Limits::default()
        },
        ..Options::default()
    };
    let mut db = Database::open_with_options(&dir.0, options.clone()).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0, 16)).unwrap();
    db.apply(table, 1, fields(1, 128)).unwrap();
    db.checkpoint().unwrap();
    db.apply(table, 1, fields(2, 128)).unwrap();
    db.checkpoint().unwrap();
    db.apply(table, 1, fields(3, 128)).unwrap();
    let generation = db.state.root.generation;
    assert!(matches!(
        db.checkpoint(),
        Err(Error::LimitExceeded {
            resource: "history files",
            ..
        })
    ));
    assert_eq!(db.state.root.generation, generation);
    let acknowledged = db.sequence();
    drop(db);
    let mut db = Database::open_with_options(&dir.0, options).unwrap();
    assert_eq!(db.sequence(), acknowledged);
    assert_eq!(db.events(table, 1).unwrap().len(), 3);
    db.compact().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.replay(table, 1).unwrap().fields, fields(3, 128));
}

#[test]
fn generated_history_retention_and_checkpoint_sequences_match_a_version_model() {
    let dir = TempDir::new();
    let options = Options {
        snapshot_interval: 5,
        history_segment_bytes: 4096,
        max_generations: 3,
        ..Options::default()
    };
    let mut db = Database::open_with_options(&dir.0, options.clone()).unwrap();
    let table = db.create_table("items", schema()).unwrap();
    db.create(table, 1, fields(0, 17)).unwrap();
    let mut versions = vec![fields(0, 17)];
    let mut base = 0usize;
    let mut random = 891723u64;
    for step in 0..240 {
        random = random
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        match (random >> 32) % 8 {
            0..=3 => {
                let next = fields(versions.len() as u8, if step % 13 == 0 { 8192 } else { 17 });
                db.apply(table, 1, next.clone()).unwrap();
                versions.push(next);
            }
            4 => {
                let keep = (random >> 16) as usize % 10;
                db.retain_last(table, 1, keep).unwrap();
                base = base.max((versions.len() - 1).saturating_sub(keep));
            }
            5 => db.write(|tx| tx.snapshot(table, 1)).unwrap(),
            6 => db.checkpoint().unwrap(),
            _ => {
                db.compact().unwrap();
                drop(db);
                db = Database::open_with_options(&dir.0, options.clone()).unwrap();
            }
        }
        let last = versions.len() - 1;
        assert_eq!(
            db.retained_range(table, 1).unwrap(),
            (base as u64, last as u64),
            "step {step}"
        );
        let target = base + random as usize % (last - base + 1);
        assert_eq!(
            db.get_at_version(table, 1, target as u64).unwrap().fields,
            versions[target],
            "step {step}, version {target}"
        );
        assert_eq!(
            db.replay_to_version(table, 1, target as u64)
                .unwrap()
                .fields,
            versions[target],
            "step {step}, version {target}"
        );
        assert_eq!(db.replay(table, 1).unwrap().fields, versions[last]);
        let events = db.events(table, 1).unwrap();
        assert_eq!(events.len(), last - base);
        for event in events {
            assert_eq!(event.fields, versions[event.version as usize]);
        }
    }
}
