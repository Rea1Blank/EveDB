// SPDX-License-Identifier: AGPL-3.0-only

use crate::{Error, Result, database::Prepared};
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

/// Limits one shared WAL flush. Local exclusive transactions flush individually.
#[derive(Clone, Debug)]
pub struct GroupCommit {
    /// Maximum transactions in one flush.
    pub max_transactions: usize,
    /// Maximum encoded mutation bytes in one flush. A larger single request runs alone.
    pub max_bytes: usize,
    /// Optional bounded delay to gather writers; zero adds no intentional delay.
    pub delay: Duration,
}
impl Default for GroupCommit {
    fn default() -> Self {
        Self {
            max_transactions: 64,
            max_bytes: 4 * 1024 * 1024,
            delay: Duration::ZERO,
        }
    }
}
impl GroupCommit {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_transactions == 0 || self.max_bytes == 0 || self.delay > Duration::from_secs(1)
        {
            return Err(Error::Invalid(
                "invalid group commit bounds (delay must be at most one second)".into(),
            ));
        }
        Ok(())
    }
}
pub(crate) struct Request {
    pub batch: Option<Prepared>,
    pub result: Option<Result<u64>>,
    pub bytes: usize,
}
pub(crate) type Ticket = Arc<Mutex<Request>>;
#[derive(Default)]
pub(crate) struct State {
    pub waiting: VecDeque<Ticket>,
    pub running: bool,
    pub count: usize,
    pub bytes: usize,
}
#[derive(Default)]
pub(crate) struct Queue {
    pub state: Mutex<State>,
    pub changed: Condvar,
}
impl Queue {
    pub fn submit(&self, batch: Prepared, limits: &crate::Limits) -> Result<Ticket> {
        let bytes = batch.bytes();
        let mut state = self.state.lock().map_err(|_| Error::NeedsRecovery)?;
        crate::resources::check("queued commits", state.count, 1, limits.max_queued_commits)?;
        crate::resources::check(
            "queued commit bytes",
            state.bytes,
            bytes,
            limits.max_queued_commit_bytes,
        )?;
        let ticket = Arc::new(Mutex::new(Request {
            batch: Some(batch),
            result: None,
            bytes,
        }));
        state.count += 1;
        state.bytes += bytes;
        state.waiting.push_back(ticket.clone());
        self.changed.notify_all();
        Ok(ticket)
    }
    // Caller holds the queue mutex; a claimed request cannot be cancelled.
    pub fn cancel(state: &mut State, ticket: &Ticket) -> bool {
        let mut request = ticket.lock().expect("commit request");
        if request.batch.take().is_some() {
            state.waiting.retain(|entry| !Arc::ptr_eq(entry, ticket));
            state.count -= 1;
            state.bytes -= request.bytes;
            request.result = Some(Err(Error::DeadlineExceeded));
            true
        } else {
            false
        }
    }
    pub fn claim(state: &mut State, bounds: &GroupCommit) -> Vec<(Ticket, Prepared)> {
        let mut result = Vec::new();
        let mut bytes: usize = 0;
        while let Some(ticket) = state.waiting.front() {
            let request_bytes = ticket.lock().expect("commit request").bytes;
            if !result.is_empty()
                && (result.len() == bounds.max_transactions
                    || bytes.saturating_add(request_bytes) > bounds.max_bytes)
            {
                break;
            }
            let ticket = state.waiting.pop_front().unwrap();
            let batch = ticket.lock().expect("commit request").batch.take().unwrap();
            bytes += request_bytes;
            result.push((ticket, batch));
        }
        result
    }
    pub fn finish(&self, results: Vec<(Ticket, Result<u64>)>) {
        let mut state = self.state.lock().expect("commit queue");
        for (ticket, result) in results {
            let mut request = ticket.lock().expect("commit request");
            state.count -= 1;
            state.bytes -= request.bytes;
            request.result = Some(result);
        }
        state.running = false;
        self.changed.notify_all();
    }
    pub fn expired(end: Option<Instant>) -> bool {
        end.is_some_and(|end| Instant::now() >= end)
    }
}
