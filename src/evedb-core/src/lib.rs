// SPDX-License-Identifier: AGPL-3.0-only

//! Local storage engine for EveDB's typed entities and versioned events.
//!
//! [`Database`] provides atomic cross-table writes, indexed current and historical
//! reads, replay, retention, and checkpoint/WAL recovery. The byte formats and
//! public API are experimental; no on-disk compatibility is promised yet.

pub mod storage;

mod checksum;
mod codec;
mod commit_queue;
mod database;
mod deadline;
mod error;
mod model;
mod ordered_map;
mod reader;
mod resources;
mod shared;
mod snapshot;
#[cfg(test)]
mod test_support;

pub use commit_queue::GroupCommit;
pub use database::{Database, Options, Transaction};
pub use deadline::Timeouts;
pub use reader::{ReadSnapshot, Reader, SnapshotStats};
pub use resources::{Limits, ResourceUsage};
pub use shared::{IsolationLevel, SharedDatabase, TransactionOptions};

pub use error::{Error, Result};
pub use model::{
    DataType, Entity, EntityId, Event, EventKind, Field, Fields, Schema, Table, TableId, Value,
};

/// The version shared by all EveDB workspace packages.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
