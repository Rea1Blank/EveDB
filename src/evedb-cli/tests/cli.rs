// SPDX-License-Identifier: AGPL-3.0-only

//! Tests for the public command-line contract.

use std::process::Command;

#[test]
fn help_and_no_arguments_describe_the_engine() {
    for args in [vec![], vec!["--help"], vec!["-h"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_evedb"))
            .args(args)
            .output()
            .expect("CLI should launch");
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).expect("help should be UTF-8");
        assert!(stdout.contains("Usage: evedb"));
        assert!(stdout.contains("checkpoint"));
        assert!(stdout.contains("Experimental"));
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn database_commands_initialize_inspect_and_checkpoint() {
    let path = std::env::temp_dir().join(format!("evedb-cli-{}", std::process::id()));
    assert!(!path.exists(), "test directory must be unused");
    let run = |command| {
        Command::new(env!("CARGO_BIN_EXE_evedb"))
            .arg(command)
            .arg(&path)
            .output()
            .unwrap()
    };
    // Read/maintenance commands must not silently initialize a mistyped path.
    assert!(!run("inspect").status.success());
    assert!(!run("checkpoint").status.success());
    assert!(!path.exists());
    assert!(run("init").status.success());
    {
        let mut db = evedb_core::Database::open(&path).unwrap();
        let schema = evedb_core::Schema::new(vec![evedb_core::Field {
            id: 1,
            name: "value".into(),
            data_type: evedb_core::DataType::Int64,
            nullable: false,
        }])
        .unwrap();
        db.create_table("items", schema).unwrap();
    }
    let output = run("inspect");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Transaction sequence: 1"));
    assert!(stdout.contains("1: items (schema 1, 1 fields)"));
    assert!(run("checkpoint").status.success());
    assert!(run("init").status.success());
    assert!(run("inspect").status.success());
    let canonical = path.canonicalize().unwrap();
    assert!(canonical.starts_with(std::env::temp_dir().canonicalize().unwrap()));
    std::fs::remove_dir_all(canonical).unwrap();
}

#[test]
fn version_matches_the_workspace() {
    for flag in ["--version", "-V"] {
        let output = Command::new(env!("CARGO_BIN_EXE_evedb"))
            .arg(flag)
            .output()
            .expect("CLI should launch");
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).expect("version should be UTF-8"),
            format!("evedb {}\n", evedb_core::VERSION)
        );
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn unsupported_or_extra_arguments_fail() {
    for args in [vec!["serve"], vec!["--help", "extra"], vec!["-V", "extra"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_evedb"))
            .args(args)
            .output()
            .expect("CLI should launch");
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}
