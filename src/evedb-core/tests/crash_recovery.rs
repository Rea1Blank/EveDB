// SPDX-License-Identifier: AGPL-3.0-only

//! Subprocess tests kill the writer without running destructors.
#![cfg(feature = "fault-injection")]

mod common;
use common::{TestDir, fields, schema};
use evedb_core::{Database, Error, Value};
use std::process::Command;

#[test]
fn crash_child() {
    let Ok(path) = std::env::var("EVEDB_CHILD_PATH") else {
        return;
    };
    let mode = std::env::var("EVEDB_CHILD_MODE").unwrap();
    let mut db = Database::open(path).unwrap();
    let result = db.write(|tx| {
        tx.apply(1, 1, fields(11))?;
        tx.apply(1, 1, fields(12))?;
        tx.apply(2, 1, fields(21))?;
        Ok(())
    });
    if mode == "io" {
        assert!(matches!(result, Err(Error::CommitUnknown(_))));
        assert!(matches!(db.get(1, 1), Err(Error::NeedsRecovery)));
        assert!(matches!(db.checkpoint(), Err(Error::NeedsRecovery)));
        return;
    }
    result.unwrap();
    if mode == "checkpoint" {
        db.checkpoint().unwrap();
    }
}

fn baseline() -> TestDir {
    let dir = TestDir::new();
    let mut db = Database::open(&dir.0).unwrap();
    db.write(|tx| {
        let a = tx.create_table("a", schema())?;
        let b = tx.create_table("b", schema())?;
        tx.create(a, 1, fields(10))?;
        tx.create(b, 1, fields(20))?;
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap();
    dir
}
fn child(dir: &TestDir, mode: &str, variable: &str, point: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env("EVEDB_CHILD_PATH", &dir.0)
        .env("EVEDB_CHILD_MODE", mode)
        .env(variable, point)
        .output()
        .unwrap()
}
fn verify(dir: &TestDir, committed: bool) {
    let mut db = Database::open(&dir.0).unwrap();
    assert_eq!(
        db.get(1, 1).unwrap().unwrap().fields[&1],
        Value::Int64(if committed { 12 } else { 10 })
    );
    assert_eq!(
        db.get(2, 1).unwrap().unwrap().fields[&1],
        Value::Int64(if committed { 21 } else { 20 })
    );
    if committed {
        assert_eq!(
            db.get_at_version(1, 1, 1).unwrap().fields[&1],
            Value::Int64(11)
        );
        assert_eq!(db.events(1, 1).unwrap().len(), 2);
    }
    // Reopening must truncate incomplete tails and allow subsequent commits.
    db.apply(2, 1, fields(99)).unwrap();
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(&dir.0).unwrap();
    assert_eq!(db.get(2, 1).unwrap().unwrap().fields[&1], Value::Int64(99));
}

#[test]
fn crash_at_each_wal_boundary_preserves_atomicity() {
    for (point, committed) in [
        ("wal-before-write", false),
        ("wal-partial", false),
        ("wal-written", true),
        ("wal-synced", true),
    ] {
        let dir = baseline();
        let output = child(&dir, "write", "EVEDB_FAILPOINT", point);
        assert_eq!(
            output.status.code(),
            Some(91),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        verify(&dir, committed);
    }
}

#[test]
fn crash_at_each_checkpoint_boundary_keeps_acknowledged_transactions() {
    for point in [
        "checkpoint-files",
        "checkpoint-catalog",
        "checkpoint-wal-partial",
        "checkpoint-published",
        "checkpoint-cleanup",
    ] {
        let dir = baseline();
        let output = child(&dir, "checkpoint", "EVEDB_FAILPOINT", point);
        assert_eq!(
            output.status.code(),
            Some(91),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        verify(&dir, true);
    }
}

#[test]
fn write_and_sync_errors_poison_the_handle_until_recovery() {
    for (point, committed) in [
        ("before-write", false),
        ("partial-write", false),
        ("before-sync", true),
        ("after-sync", true),
    ] {
        let dir = baseline();
        let output = child(&dir, "io", "EVEDB_IO_ERROR", point);
        assert!(
            output.status.success(),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        verify(&dir, committed);
    }
}
