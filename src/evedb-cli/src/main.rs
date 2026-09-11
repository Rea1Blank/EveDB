// SPDX-License-Identifier: AGPL-3.0-only

//! Command-line entry point for EveDB.

use evedb_core::{Database, Error, Result};
use std::{env, path::Path, process::ExitCode};

fn main() -> ExitCode {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.is_empty() || (args.len() == 1 && (args[0] == "--help" || args[0] == "-h")) {
        println!(
            "EveDB {}\n\n\
             Experimental local entity storage engine.\n\n\
             Usage: evedb <init | inspect | checkpoint | compact> <directory>\n\
                    evedb [--help | --version]\n\n\
             Commands:\n\
               init          Initialize or open a database directory\n\
               inspect       Open an existing database and show its catalog\n\
               checkpoint    Save changed entities into a new generation\n\
               compact       Rewrite every live entity and release the rest\n\n\
             Options:\n\
               -h, --help     Show this help\n\
               -V, --version  Show the version",
            evedb_core::VERSION
        );
        ExitCode::SUCCESS
    } else if args.len() == 1 && (args[0] == "--version" || args[0] == "-V") {
        println!("evedb {}", evedb_core::VERSION);
        ExitCode::SUCCESS
    } else if args.len() == 2
        && ["init", "inspect", "checkpoint", "compact"]
            .iter()
            .any(|command| args[0] == *command)
    {
        match run(args[0].to_str().unwrap(), Path::new(&args[1])) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        }
    } else {
        eprintln!("Unsupported arguments. Run 'evedb --help' for usage.");
        ExitCode::from(2)
    }
}

fn run(command: &str, directory: &Path) -> Result<()> {
    if command != "init" && !directory.join("control").is_file() {
        return Err(Error::NotFound(format!(
            "database at {}",
            directory.display()
        )));
    }
    let mut db = Database::open(directory)?;
    match command {
        "init" => println!("Database ready at {}", directory.display()),
        "inspect" => {
            println!("Database: {}", directory.display());
            println!("Transaction sequence: {}", db.sequence());
            println!("Tables: {}", db.tables().count());
            for table in db.tables() {
                println!(
                    "  {}: {} (schema {}, {} fields)",
                    table.id,
                    table.name,
                    table.schema().version,
                    table.schema().fields.len()
                );
            }
            println!("Generations: {}", db.generations().len());
            for entry in db.generations() {
                println!(
                    "  {}: {} entities, {} superseded",
                    entry.generation, entry.entities, entry.dead
                );
            }
        }
        "checkpoint" => {
            db.checkpoint()?;
            println!("Checkpoint saved at transaction {}", db.sequence());
        }
        "compact" => {
            db.compact()?;
            println!(
                "Compacted into generation {}",
                db.generations()[0].generation
            );
        }
        _ => unreachable!("validated command"),
    }
    Ok(())
}
