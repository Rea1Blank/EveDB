// SPDX-License-Identifier: AGPL-3.0-only

use evedb_core::{DataType, Field, Fields, Schema, Value};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct TestDir(pub PathBuf);
impl TestDir {
    pub fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "evedb-test-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        assert!(self.0.starts_with(std::env::temp_dir()));
        assert!(
            self.0
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("evedb-test-")
        );
        let _ = fs::remove_dir_all(&self.0);
    }
}
pub fn schema() -> Schema {
    Schema::new(vec![
        Field {
            id: 1,
            name: "count".into(),
            data_type: DataType::Int64,
            nullable: false,
        },
        Field {
            id: 2,
            name: "label".into(),
            data_type: DataType::Text,
            nullable: true,
        },
        Field {
            id: 3,
            name: "payload".into(),
            data_type: DataType::Bytes,
            nullable: true,
        },
    ])
    .unwrap()
}
pub fn fields(value: i64) -> Fields {
    [(1, Value::Int64(value))].into()
}
