// SPDX-License-Identifier: AGPL-3.0-only

//! Core library for EveDB.
//!
//! This crate is the starting point for the database engine. Storage, queries,
//! transactions, and networking are not implemented yet.

/// The version shared by all EveDB workspace packages.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
