// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

//! Tests for the public command-line contract.

use std::process::Command;

#[test]
fn help_and_no_arguments_describe_the_scaffold() {
    for args in [vec![], vec!["--help"], vec!["-h"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_evedb"))
            .args(args)
            .output()
            .expect("CLI should launch");
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).expect("help should be UTF-8");
        assert!(stdout.contains("Usage: evedb"));
        assert!(stdout.contains("not implemented yet"));
        assert!(output.stderr.is_empty());
    }
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
