// SPDX-License-Identifier: AGPL-3.0-only

//! Command-line entry point for EveDB.

use std::{env, process::ExitCode};

fn main() -> ExitCode {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.is_empty() || (args.len() == 1 && (args[0] == "--help" || args[0] == "-h")) {
        println!(
            "EveDB {}\n\n\
             Database project scaffold. The database engine is not implemented yet.\n\n\
             Usage: evedb [--help | --version]\n\n\
             Options:\n\
               -h, --help     Show this help\n\
               -V, --version  Show the version",
            evedb_core::VERSION
        );
        ExitCode::SUCCESS
    } else if args.len() == 1 && (args[0] == "--version" || args[0] == "-V") {
        println!("evedb {}", evedb_core::VERSION);
        ExitCode::SUCCESS
    } else {
        eprintln!("Unsupported arguments. Run 'evedb --help' for usage.");
        ExitCode::from(2)
    }
}
