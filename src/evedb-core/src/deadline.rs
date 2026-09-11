// SPDX-License-Identifier: AGPL-3.0-only

//! Cooperative deadlines with eager expiry of abandoned handles.

use crate::{Error, Result};
use std::{
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

/// Default upper bounds. Individual transactions may request shorter deadlines.
#[derive(Clone, Debug)]
pub struct Timeouts {
    /// Maximum transaction lifetime, including preparation and commit admission.
    pub transaction: Duration,
    /// Maximum idle interval between transaction calls.
    pub idle_transaction: Duration,
    /// Maximum lifetime of an independently pinned read snapshot.
    pub snapshot: Duration,
    /// Cooperative duration limit for one operation; does not interrupt OS I/O.
    pub operation: Duration,
    /// Maximum wait for commit admission, before WAL writing begins.
    pub commit_queue: Duration,
    /// How often abandoned handles are swept for expiry.
    pub reap_interval: Duration,
}
impl Default for Timeouts {
    fn default() -> Self {
        Self {
            transaction: Duration::from_secs(60),
            idle_transaction: Duration::from_secs(30),
            snapshot: Duration::from_secs(300),
            operation: Duration::from_secs(30),
            commit_queue: Duration::from_secs(5),
            reap_interval: Duration::from_millis(50),
        }
    }
}
impl Timeouts {
    pub(crate) fn validate(&self) -> Result<()> {
        for value in [
            self.transaction,
            self.idle_transaction,
            self.snapshot,
            self.operation,
            self.commit_queue,
            self.reap_interval,
        ] {
            deadline(value)?;
        }
        Ok(())
    }
}
pub(crate) fn deadline(duration: Duration) -> Result<Instant> {
    if duration.is_zero() {
        return Err(Error::Invalid("timeouts must be positive".into()));
    }
    Instant::now()
        .checked_add(duration)
        .ok_or_else(|| Error::Invalid("timeout overflow".into()))
}
pub(crate) fn check(deadline: Option<Instant>) -> Result<()> {
    if deadline.is_some_and(|end| Instant::now() >= end) {
        Err(Error::DeadlineExceeded)
    } else {
        Ok(())
    }
}
trait Expire: Send + Sync {
    fn expire(&self, now: Instant);
}
pub(crate) struct Reaper {
    entries: Mutex<Vec<Weak<dyn Expire>>>,
}
impl Reaper {
    pub fn new(interval: Duration) -> Result<Arc<Self>> {
        let result = Arc::new(Self {
            entries: Mutex::new(Vec::new()),
        });
        let weak = Arc::downgrade(&result);
        std::thread::Builder::new()
            .name("evedb-expiry".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    let Some(reaper) = weak.upgrade() else {
                        break;
                    };
                    reaper.sweep();
                }
            })?;
        Ok(result)
    }
    fn sweep(&self) {
        let mut entries = self.entries.lock().expect("expiry registry");
        entries.retain(|weak| {
            if let Some(entry) = weak.upgrade() {
                entry.expire(Instant::now());
                true
            } else {
                false
            }
        });
    }
    fn register<T: Send + 'static>(&self, lease: &Arc<LeaseState<T>>) {
        let entry: Arc<dyn Expire> = lease.clone();
        let mut entries = self.entries.lock().expect("expiry registry");
        entries.retain(|entry| entry.strong_count() != 0);
        entries.push(Arc::downgrade(&entry));
    }
}
struct State<T> {
    value: Option<T>,
    last_access: Instant,
}
pub(crate) struct LeaseState<T> {
    state: Mutex<State<T>>,
    pub deadline: Option<Instant>,
    idle: Option<Duration>,
    operation: Option<Duration>,
}
impl<T: Send + 'static> LeaseState<T> {
    pub fn new(
        value: T,
        reaper: &Reaper,
        timeout: Option<Duration>,
        idle: Option<Duration>,
        operation: Option<Duration>,
    ) -> Result<Arc<Self>> {
        let result = Arc::new(Self {
            state: Mutex::new(State {
                value: Some(value),
                last_access: Instant::now(),
            }),
            deadline: timeout.map(deadline).transpose()?,
            idle,
            operation,
        });
        if timeout.is_some() || idle.is_some() {
            reaper.register(&result);
        }
        Ok(result)
    }
    fn expired(&self, state: &State<T>, now: Instant) -> bool {
        self.deadline.is_some_and(|end| now >= end)
            || self
                .idle
                .is_some_and(|idle| now.duration_since(state.last_access) >= idle)
    }
    pub fn with<R>(&self, operation: impl FnOnce(&mut T) -> Result<R>) -> Result<R> {
        let mut state = self.state.lock().map_err(|_| Error::NeedsRecovery)?;
        let start = Instant::now();
        if self.expired(&state, start) {
            state.value = None;
        }
        let value = state.value.as_mut().ok_or(Error::TransactionExpired)?;
        let result = operation(value);
        let now = Instant::now();
        if self.deadline.is_some_and(|end| now >= end)
            || self
                .operation
                .is_some_and(|limit| now.duration_since(start) >= limit)
        {
            state.value = None;
            return Err(Error::DeadlineExceeded);
        }
        state.last_access = now;
        result
    }
    pub fn take(&self) -> Result<T> {
        let mut state = self.state.lock().map_err(|_| Error::NeedsRecovery)?;
        if self.expired(&state, Instant::now()) {
            state.value = None;
        }
        state.value.take().ok_or(Error::TransactionExpired)
    }
}
impl<T: Send> Expire for LeaseState<T> {
    fn expire(&self, now: Instant) {
        // Never revoke storage under an operation currently using it.
        if let Ok(mut state) = self.state.try_lock()
            && (self.deadline.is_some_and(|end| now >= end)
                || self
                    .idle
                    .is_some_and(|idle| now.duration_since(state.last_access) >= idle))
        {
            state.value = None;
        }
    }
}

// The registry temporarily upgrades weak references while sweeping. It must not
// extend the lifetime of a caller's last handle and its directory lock.
pub(crate) struct Lease<T: Send + 'static> {
    inner: Arc<LeaseState<T>>,
}
impl<T: Send + 'static> Lease<T> {
    pub fn new(
        value: T,
        reaper: &Reaper,
        timeout: Option<Duration>,
        idle: Option<Duration>,
        operation: Option<Duration>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            inner: LeaseState::new(value, reaper, timeout, idle, operation)?,
        }))
    }
}
impl<T: Send + 'static> std::ops::Deref for Lease<T> {
    type Target = LeaseState<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl<T: Send + 'static> Drop for Lease<T> {
    fn drop(&mut self) {
        self.inner.state.lock().expect("lease ownership").value = None;
    }
}
