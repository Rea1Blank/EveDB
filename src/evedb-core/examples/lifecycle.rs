// SPDX-License-Identifier: AGPL-3.0-only

//! Run with `cargo run -p evedb-core --example lifecycle -- <unused-directory>`.

use evedb_core::{DataType, Database, Error, Field, Fields, Result, Schema, Value};
use std::{env, path::PathBuf};

fn balance(amount: i64) -> Fields {
    [(1, Value::Int64(amount))].into()
}

fn main() -> Result<()> {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| Error::Invalid("usage: lifecycle <unused-directory>".into()))?;
    if path.exists() {
        return Err(Error::Invalid("choose an unused demo directory".into()));
    }
    let mut db = Database::open(&path)?;
    let schema = Schema::new(vec![Field {
        id: 1,
        name: "balance".into(),
        data_type: DataType::Int64,
        nullable: false,
    }])?;
    let table = db.write(|tx| {
        let table = tx.create_table("accounts", schema)?;
        tx.create(table, 1, balance(100))?;
        tx.create(table, 2, balance(0))?;
        Ok(table)
    })?;
    db.write(|tx| {
        // Separate events keep their own versions inside one atomic transaction.
        tx.apply(table, 1, balance(90))?;
        tx.apply(table, 1, balance(80))?;
        tx.apply(table, 2, balance(20))?;
        Ok(())
    })?;
    assert_eq!(db.get_at_version(table, 1, 1)?.fields, balance(90));
    assert_eq!(db.get(table, 1)?.unwrap(), db.replay(table, 1)?);
    db.checkpoint()?;
    drop(db);

    let mut db = Database::open(&path)?;
    println!("Reopened: {:?}", db.get(table, 1)?.unwrap());
    println!(
        "Intermediate version: {:?}",
        db.get_at_version(table, 1, 1)?
    );
    db.retain_last(table, 1, 1)?;
    assert_eq!(db.retained_range(table, 1)?, (1, 2));
    assert!(matches!(
        db.get_at_version(table, 1, 0),
        Err(Error::VersionUnavailable { .. })
    ));
    assert_eq!(db.replay(table, 1)?.fields, balance(80));
    db.delete(table, 1)?;
    assert!(db.get(table, 1)?.is_none());
    assert!(db.replay(table, 1)?.deleted);
    db.checkpoint()?;
    println!(
        "Retained range after deletion: {:?}",
        db.retained_range(table, 1)?
    );
    println!("Database saved at {}", path.display());
    Ok(())
}
