// SPDX-License-Identifier: AGPL-3.0-only

//! Admission and ownership-based accounting of accepted mutation bytes.

use crate::{Error, Result, codec::MAX_FRAME};
use std::sync::{Arc, Mutex};

/// Engine admission limits. Byte limits count encoded mutations, not process RSS.
/// Page caches, decoded records and allocator overhead have separate costs.
#[derive(Clone, Debug)]
pub struct Limits {
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
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_transactions: 1024,
            max_snapshots: 4096,
            max_pinned_bytes: 8 * 1024 * 1024 * 1024usize,
            max_transaction_operations: 100_000,
            max_transaction_bytes: MAX_FRAME,
            max_resident_write_bytes: 512 * 1024 * 1024,
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_transactions == 0
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
}
impl Resources {
    pub fn new(limits: Limits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            usage: Mutex::new(ResourceUsage::default()),
        })
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
}
impl Reservation {
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
        self.resources
            .usage
            .lock()
            .expect("resource accounting")
            .resident_write_bytes -= self.bytes;
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
