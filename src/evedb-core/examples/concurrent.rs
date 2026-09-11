// SPDX-License-Identifier: AGPL-3.0-only

//! Independent clients stage writes while a reader holds a repeatable snapshot.

use evedb_core::{
    DataType, Field, IsolationLevel, Schema, SharedDatabase, TransactionOptions, Value,
};
use std::sync::{Arc, Barrier};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: concurrent <unused-directory>")?;
    if std::path::Path::new(&path).exists() {
        return Err("choose an unused directory".into());
    }
    let db = SharedDatabase::open(path)?;
    let table = db.create_table(
        "counters",
        Schema::new(vec![Field {
            id: 1,
            name: "value".into(),
            data_type: DataType::Int64,
            nullable: false,
        }])?,
    )?;
    let reader = db.reader();
    let before = reader.pin()?;
    let barrier = Arc::new(Barrier::new(4));
    std::thread::scope(|scope| {
        let mut clients = Vec::new();
        for id in 0..4 {
            let connection = db.clone();
            let barrier = barrier.clone();
            clients.push(scope.spawn(move || -> evedb_core::Result<u64> {
                let mut tx = connection.transaction_with_options(
                    TransactionOptions::with_isolation(IsolationLevel::Snapshot),
                )?;
                // All clients begin before any client commits.
                barrier.wait();
                tx.create(table, id, [(1, Value::Int64(id as i64))].into())?;
                tx.commit()
            }));
        }
        for client in clients {
            println!(
                "Committed sequence {}",
                client.join().expect("client thread")?
            );
        }
        Ok::<_, evedb_core::Error>(())
    })?;
    assert!(before.get(table, 0)?.is_none());
    reader.scan(table, |entity| {
        println!("Entity {}: {:?}", entity.id, entity.fields);
        Ok(())
    })?;
    db.checkpoint()?;
    println!(
        "Pinned sequence {}, snapshot stats: {:?}",
        before.sequence(),
        db.snapshot_stats()
    );
    Ok(())
}
