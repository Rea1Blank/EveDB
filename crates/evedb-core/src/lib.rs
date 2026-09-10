// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

//! Core library for EveDB.
//!
//! This crate is the starting point for the database engine. Storage, queries,
//! transactions, and networking are not implemented yet.

/// The version shared by all EveDB workspace packages.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
