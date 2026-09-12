// SPDX-License-Identifier: AGPL-3.0-only

//! Admission and ownership-based accounting for writes, reads, and scratch buffers.

use crate::{Error, Result, codec::MAX_FRAME};
use std::sync::{Arc, Mutex};

/// Engine admission limits with separate encoded, decoded, and scratch budgets.
/// Decoded charges include a per-field allowance and do not measure process RSS.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Logical decoded bytes in one entity or event (including per-field allowance).
    pub max_decoded_record_bytes: usize,
    /// Logical decoded allocations admitted by one transaction.
    pub max_transaction_decoded_bytes: usize,
    /// Decoded transaction charges retained by staging, queued commits, and views.
    pub max_resident_decoded_write_bytes: usize,
    /// Decoded read buffers and returned history batches retained by the engine.
    pub max_read_memory_bytes: usize,
    /// Concurrent raw record and decompression scratch buffers.
    pub max_scratch_bytes: usize,
    /// Maximum logical size of the compatibility events() collector.
    pub max_read_result_bytes: usize,
    /// Maximum history events admitted in one batch.
    pub max_history_batch_events: usize,
    /// Maximum logical history-batch size, including its event slots.
    pub max_history_batch_bytes: usize,
    /// Simultaneously active transactions, including queued commits.
    pub max_transactions: usize,
    /// Independently pinned snapshots; clones share a pin.
    pub max_snapshots: usize,
    /// Sum of checkpoint file bytes per independent pin (shared files count again).
    pub max_pinned_bytes: usize,
    /// Operations in one transaction, including automatic maintenance operations.
    pub max_transaction_operations: usize,
    /// Encoded transaction payload, including its operation-count header.
    pub max_transaction_bytes: usize,
    /// Accepted mutation bytes still staged, queued, or retained in published views.
    pub max_resident_write_bytes: usize,
    /// Waiting and currently syncing commit requests.
    pub max_queued_commits: usize,
    /// Encoded mutation bytes in waiting and currently syncing commit requests.
    pub max_queued_commit_bytes: usize,
    /// History payload files (events and snapshots) in a newly published checkpoint.
    /// Reaching this bound requires compaction or explicit retention before checkpointing.
    pub max_history_files: usize,
    /// Index routing entries in a newly published checkpoint.
    pub max_index_partitions: usize,
    /// Transaction WAL bytes after the last completed checkpoint frontier.
    pub max_uncheckpointed_wal_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_decoded_record_bytes: 64 * 1024 * 1024,
            max_transaction_decoded_bytes: 256 * 1024 * 1024,
            max_resident_decoded_write_bytes: 1024 * 1024 * 1024,
            max_read_memory_bytes: 256 * 1024 * 1024,
            max_scratch_bytes: 128 * 1024 * 1024,
            max_read_result_bytes: 64 * 1024 * 1024,
            max_history_batch_events: 4096,
            max_history_batch_bytes: 64 * 1024 * 1024,
            max_transactions: 1024,
            max_snapshots: 4096,
            max_pinned_bytes: 8 * 1024 * 1024 * 1024usize,
            max_transaction_operations: 100_000,
            max_transaction_bytes: MAX_FRAME,
            max_resident_write_bytes: 512 * 1024 * 1024,
            max_queued_commits: 256,
            max_queued_commit_bytes: 128 * 1024 * 1024,
            max_history_files: 4096,
            max_index_partitions: 65_536,
            max_uncheckpointed_wal_bytes: 512 * 1024 * 1024,
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_decoded_record_bytes == 0
            || self.max_transaction_decoded_bytes == 0
            || self.max_resident_decoded_write_bytes < self.max_transaction_decoded_bytes
            || self.max_read_memory_bytes == 0
            || self.max_scratch_bytes == 0
            || self.max_read_result_bytes == 0
            || self.max_history_batch_events == 0
            || self.max_history_batch_bytes == 0
            || self.max_uncheckpointed_wal_bytes == 0
            || self.max_transactions == 0
            || self.max_history_files == 0
            || self.max_index_partitions == 0
            || self.max_queued_commits == 0
            || self.max_queued_commit_bytes < self.max_transaction_bytes
            || self.max_snapshots == 0
            || self.max_transaction_operations == 0
            || self.max_transaction_bytes < 8
            || self.max_transaction_bytes > MAX_FRAME
            || self.max_resident_write_bytes < self.max_transaction_bytes
        {
            return Err(Error::Invalid("invalid admission limits".into()));
        }
        Ok(())
    }
}

/// Current admission counters, useful for enforcing and diagnosing limits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceUsage {
    /// Logical decoded transaction bytes retained by accepted writes and views.
    pub decoded_write_bytes: usize,
    /// Decoded read allocations, including batches that callers still own.
    pub read_memory_bytes: usize,
    /// Raw/decompression buffers currently retained by reads.
    pub scratch_bytes: usize,
    /// Active transactions, including requests waiting for commit.
    pub transactions: usize,
    /// Independent live snapshot pins.
    pub snapshots: usize,
    /// Checkpoint bytes charged to pins, counting files once per pin.
    pub pinned_bytes: usize,
    /// Encoded mutation bytes owned by staging, commit requests, or published views.
    pub resident_write_bytes: usize,
}
pub(crate) struct Resources {
    pub limits: Limits,
    usage: Mutex<ResourceUsage>,
    pub timeouts: crate::Timeouts,
    pub reaper: Arc<crate::deadline::Reaper>,
}
impl Resources {
    pub fn new(limits: Limits, timeouts: crate::Timeouts) -> Result<Arc<Self>> {
        let reaper = crate::deadline::Reaper::new(timeouts.reap_interval)?;
        Ok(Arc::new(Self {
            reaper,
            timeouts,
            limits,
            usage: Mutex::new(ResourceUsage::default()),
        }))
    }
    pub fn usage(&self) -> ResourceUsage {
        *self.usage.lock().expect("resource accounting")
    }
    pub fn transaction(self: &Arc<Self>) -> Result<Permit> {
        let mut usage = self.usage.lock().map_err(|_| Error::NeedsRecovery)?;
        check(
            "active transactions",
            usage.transactions,
            1,
            self.limits.max_transactions,
        )?;
        usage.transactions += 1;
        Ok(Permit {
            resources: self.clone(),
            kind: PermitKind::Transaction,
        })
    }
    pub fn snapshot(self: &Arc<Self>, bytes: usize) -> Result<Permit> {
        let mut usage = self.usage.lock().map_err(|_| Error::NeedsRecovery)?;
        check(
            "active snapshots",
            usage.snapshots,
            1,
            self.limits.max_snapshots,
        )?;
        check(
            "pinned checkpoint bytes",
            usage.pinned_bytes,
            bytes,
            self.limits.max_pinned_bytes,
        )?;
        usage.snapshots += 1;
        usage.pinned_bytes += bytes;
        Ok(Permit {
            resources: self.clone(),
            kind: PermitKind::Snapshot(bytes),
        })
    }
    pub fn reservation(self: &Arc<Self>) -> Reservation {
        Reservation {
            resources: self.clone(),
            bytes: 0,
            decoded_bytes: 0,
        }
    }
}
enum PermitKind {
    Transaction,
    Snapshot(usize),
}
pub(crate) struct Permit {
    resources: Arc<Resources>,
    kind: PermitKind,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut usage = self.resources.usage.lock().expect("resource accounting");
        match self.kind {
            PermitKind::Transaction => usage.transactions -= 1,
            PermitKind::Snapshot(bytes) => {
                usage.snapshots -= 1;
                usage.pinned_bytes -= bytes;
            }
        }
    }
}
pub(crate) struct Reservation {
    resources: Arc<Resources>,
    pub bytes: usize,
    pub decoded_bytes: usize,
}
impl Reservation {
    pub fn grow_decoded(&mut self, bytes: usize, recovery: bool) -> Result<()> {
        let mut usage = self
            .resources
            .usage
            .lock()
            .map_err(|_| Error::NeedsRecovery)?;
        if !recovery {
            check(
                "transaction decoded bytes",
                self.decoded_bytes,
                bytes,
                self.resources.limits.max_transaction_decoded_bytes,
            )?;
            check(
                "resident decoded write bytes",
                usage.decoded_write_bytes,
                bytes,
                self.resources.limits.max_resident_decoded_write_bytes,
            )?;
        }
        usage.decoded_write_bytes += bytes;
        self.decoded_bytes += bytes;
        Ok(())
    }
    pub fn recover(&mut self, bytes: usize) {
        self.resources
            .usage
            .lock()
            .expect("resource accounting")
            .resident_write_bytes += bytes;
        self.bytes += bytes;
    }
    pub fn grow(&mut self, bytes: usize) -> Result<()> {
        let mut usage = self
            .resources
            .usage
            .lock()
            .map_err(|_| Error::NeedsRecovery)?;
        check(
            "resident mutation bytes",
            usage.resident_write_bytes,
            bytes,
            self.resources.limits.max_resident_write_bytes,
        )?;
        usage.resident_write_bytes += bytes;
        self.bytes += bytes;
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut usage = self.resources.usage.lock().expect("resource accounting");
        usage.resident_write_bytes -= self.bytes;
        usage.decoded_write_bytes -= self.decoded_bytes;
    }
}
#[derive(Clone, Copy)]
pub(crate) enum MemoryKind {
    Read,
    Scratch,
}
pub(crate) struct MemoryReservation {
    resources: Arc<Resources>,
    kind: MemoryKind,
    pub bytes: usize,
}
impl Resources {
    pub fn memory(
        self: &Arc<Self>,
        kind: MemoryKind,
        bytes: usize,
        recovery: bool,
    ) -> Result<MemoryReservation> {
        let mut memory = MemoryReservation {
            resources: self.clone(),
            kind,
            bytes: 0,
        };
        memory.grow(bytes, recovery)?;
        Ok(memory)
    }
}
impl MemoryReservation {
    pub fn grow(&mut self, bytes: usize, recovery: bool) -> Result<()> {
        let mut usage = self
            .resources
            .usage
            .lock()
            .map_err(|_| Error::NeedsRecovery)?;
        let (used, limit, name) = match self.kind {
            MemoryKind::Read => (
                &mut usage.read_memory_bytes,
                self.resources.limits.max_read_memory_bytes,
                "decoded read bytes",
            ),
            MemoryKind::Scratch => (
                &mut usage.scratch_bytes,
                self.resources.limits.max_scratch_bytes,
                "scratch bytes",
            ),
        };
        if !recovery {
            check(name, *used, bytes, limit)?;
        }
        *used += bytes;
        self.bytes += bytes;
        Ok(())
    }
}
impl Drop for MemoryReservation {
    fn drop(&mut self) {
        let mut usage = self.resources.usage.lock().expect("memory accounting");
        match self.kind {
            MemoryKind::Read => usage.read_memory_bytes -= self.bytes,
            MemoryKind::Scratch => usage.scratch_bytes -= self.bytes,
        }
    }
}
pub(crate) fn check(
    resource: &'static str,
    used: usize,
    additional: usize,
    limit: usize,
) -> Result<()> {
    if used
        .checked_add(additional)
        .is_none_or(|total| total > limit)
    {
        Err(Error::LimitExceeded { resource, limit })
    } else {
        Ok(())
    }
}
