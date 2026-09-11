// SPDX-License-Identifier: AGPL-3.0-only

//! Repeatable storage smoke benchmark; results include the OS file cache.
//! Run in release mode with an unused directory and optional row count.

use evedb_core::{DataType, Database, Error, Field, Fields, Options, Result, Schema, Value};
use std::{
    env, fs,
    hint::black_box,
    path::{Path, PathBuf},
    time::Instant,
};

fn fields(value: u64) -> Fields {
    [
        (1, Value::UInt64(value)),
        (2, Value::Bytes(vec![b'x'; 1024])),
    ]
    .into()
}

fn size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            total += size(&entry.path())?;
        } else {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

fn main() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| Error::Invalid("usage: storage_bench <unused-directory> [rows]".into()))?;
    let rows = match args.next() {
        Some(arg) => arg
            .to_str()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| (1..=1_000_000).contains(&n))
            .ok_or_else(|| Error::Invalid("rows must be in 1..=1000000".into()))?,
        None => 10_000,
    };
    if path.exists() {
        return Err(Error::Invalid(
            "choose an unused benchmark directory".into(),
        ));
    }
    let options = Options::default();
    println!(
        "rows={rows}, payload_bytes=1024, batch_rows=128, checkpoint_bytes={}",
        options.checkpoint_bytes
    );
    let mut db = Database::open_with_options(&path, options)?;
    let table = db.create_table(
        "items",
        Schema::new(vec![
            Field {
                id: 1,
                name: "number".into(),
                data_type: DataType::UInt64,
                nullable: false,
            },
            Field {
                id: 2,
                name: "payload".into(),
                data_type: DataType::Bytes,
                nullable: false,
            },
        ])?,
    )?;
    let start = Instant::now();
    for first in (0..rows).step_by(128) {
        db.write(|tx| {
            for id in first..(first + 128).min(rows) {
                tx.create(table, id, fields(id))?;
            }
            Ok(())
        })?;
    }
    println!("insert_ms={:.3}", start.elapsed().as_secs_f64() * 1000.0);
    let start = Instant::now();
    db.checkpoint()?;
    println!(
        "checkpoint_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    drop(db);
    let start = Instant::now();
    let mut db = Database::open(&path)?;
    println!(
        "reopen_verify_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let start = Instant::now();
    let mut state = 7u64;
    for _ in 0..1000 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = state % rows;
        let entity = db.get(table, id)?.unwrap();
        assert_eq!(entity.fields[&1], Value::UInt64(id));
        black_box(entity);
    }
    println!(
        "point_reads_1000_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let start = Instant::now();
    db.write(|tx| {
        for version in 1..=300 {
            tx.apply(table, 0, [(1, Value::UInt64(version))].into())?;
        }
        Ok(())
    })?;
    println!(
        "update_300_events_one_tx_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    db.checkpoint()?;
    let start = Instant::now();
    for _ in 0..100 {
        assert_eq!(
            db.get_at_version(table, 0, 299)?.fields[&1],
            Value::UInt64(299)
        );
    }
    println!(
        "historical_reads_100_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let start = Instant::now();
    for _ in 0..100 {
        assert_eq!(db.replay(table, 0)?.fields[&1], Value::UInt64(300));
    }
    println!(
        "full_replays_100_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let start = Instant::now();
    let mut scanned = 0;
    db.scan(table, |entity| {
        black_box(entity);
        scanned += 1;
        Ok(())
    })?;
    assert_eq!(scanned, rows);
    println!("scan_ms={:.3}", start.elapsed().as_secs_f64() * 1000.0);
    println!("directory_bytes_before_retention={}", size(&path)?);
    let start = Instant::now();
    db.retain_last(table, 0, 10)?;
    db.checkpoint()?;
    db.checkpoint()?;
    assert_eq!(db.retained_range(table, 0)?, (290, 300));
    println!(
        "retention_two_checkpoints_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    println!("directory_bytes_after_retention={}", size(&path)?);
    println!("OS cache is enabled; no hard memory limit or power-loss simulation was applied.");
    Ok(())
}
