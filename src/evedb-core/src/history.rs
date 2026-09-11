// SPDX-License-Identifier: AGPL-3.0-only

use crate::{TableId, snapshot::Root, storage::pager::Pager};
use std::sync::Arc;

/// Immutable disk history is loaded only by historical reads or collection.
/// Current writes carry this descriptor and share in-memory event tree roots.
pub(crate) struct DiskHistory {
    pub root: Arc<Root>,
    pub pager: Arc<Pager>,
    pub table: TableId,
    pub through: u64,
}
