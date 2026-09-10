// SPDX-License-Identifier: AGPL-3.0-only

//! Core library for EveDB, starting with checked storage pages.

pub mod storage;
mod checksum;

/// The version shared by all EveDB workspace packages.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
