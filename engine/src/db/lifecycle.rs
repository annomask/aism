use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

use crate::{common::slow_trace, db::ReefsDb};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReefsDbLifecycleState {
    Open,
    Draining,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReefsDbLeaseError {
    Draining,
    Closed,
    Deleted,
    CountOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum DrainWaitOutcome {
    Drained,
    TimedOut,
    Interrupted,
}

#[derive(Debug)]
struct LifecycleInner {
    state: ReefsDbLifecycleState,
    active_operations: usize,
    opened: bool,
    retired: bool,
}

#[derive(Debug)]
pub(crate) struct ReefsDbLifecycle {
    inner: Mutex<LifecycleInner>,
    drained: Condvar,
}

impl Default for ReefsDbLifecycle {
    fn default() -> Self {
        Self {
            inner: Mutex::new(LifecycleInner {
                state: ReefsDbLifecycleState::Closed,
                active_operations: 0,
                opened: false,
                retired: false,
            }),
            drained: Condvar::new(),
        }
    }
}

impl ReefsDbLifecycle {
    pub(crate) fn check_open_allowed(&self) -> Result<(), ReefsDbLeaseError> {
        let inner = self.inner.lock();
        if inner.retired {
            return Err(ReefsDbLeaseError::Deleted);
        }
        match inner.state {
            ReefsDbLifecycleState::Draining => Err(ReefsDbLeaseError::Draining),
            ReefsDbLifecycleState::Closed if inner.opened => Err(ReefsDbLeaseError::Closed),
            ReefsDbLifecycleState::Open | ReefsDbLifecycleState::Closed => Ok(()),
        }
    }

    pub(crate) fn mark_open(&self) -> Result<(), ReefsDbLeaseError> {
        let mut inner = self.inner.lock();
        if inner.retired {
            return Err(ReefsDbLeaseError::Deleted);
        }
        if inner.state == ReefsDbLifecycleState::Draining {
            return Err(ReefsDbLeaseError::Draining);
        }
        if inner.state == ReefsDbLifecycleState::Closed && inner.opened {
            return Err(ReefsDbLeaseError::Closed);
        }
        inner.state = ReefsDbLifecycleState::Open;
        inner.opened = true;
        Ok(())
    }

    pub(crate) fn acquire(self: &Arc<Self>) -> Result<Arc<LeaseToken>, ReefsDbLeaseError> {
        #[cfg(feature = "slow-trace")]
        let lock_started = slow_trace::now();
        let mut inner = self.inner.lock();
        #[cfg(feature = "slow-trace")]
        let lock_wait = slow_trace::elapsed(lock_started);
        match (inner.state, inner.retired) {
            (_, true) => return Err(ReefsDbLeaseError::Deleted),
            (ReefsDbLifecycleState::Draining, false) => return Err(ReefsDbLeaseError::Draining),
            (ReefsDbLifecycleState::Closed, false) => return Err(ReefsDbLeaseError::Closed),
            (ReefsDbLifecycleState::Open, false) => {}
        }
        inner.active_operations = inner
            .active_operations
            .checked_add(1)
            .ok_or(ReefsDbLeaseError::CountOverflow)?;
        #[cfg(feature = "slow-trace")]
        {
            drop(inner);
            slow_trace::log_planner_lock_wait("reefsdb.lifecycle.inner.acquire", lock_wait);
        }
        Ok(Arc::new(LeaseToken {
            lifecycle: Arc::clone(self),
        }))
    }

    pub(crate) fn begin_drain(&self, retire: bool) {
        let mut inner = self.inner.lock();
        if retire {
            inner.retired = true;
        }
        if inner.state == ReefsDbLifecycleState::Open {
            inner.state = ReefsDbLifecycleState::Draining;
        }
        if inner.active_operations == 0 {
            self.drained.notify_all();
        }
    }

    pub(crate) fn wait_for_drain(
        &self,
        timeout: Option<Duration>,
        interrupted: impl Fn() -> bool,
    ) -> DrainWaitOutcome {
        const INTERRUPT_POLL: Duration = Duration::from_millis(25);

        let started = Instant::now();
        let mut inner = self.inner.lock();
        loop {
            if inner.active_operations == 0 {
                return DrainWaitOutcome::Drained;
            }
            if interrupted() {
                return DrainWaitOutcome::Interrupted;
            }

            let wait = match timeout {
                Some(limit) => {
                    let Some(remaining) = limit.checked_sub(started.elapsed()) else {
                        return DrainWaitOutcome::TimedOut;
                    };
                    remaining.min(INTERRUPT_POLL)
                }
                None => INTERRUPT_POLL,
            };
            self.drained.wait_for(&mut inner, wait);
        }
    }

    pub(crate) fn mark_closed(&self) -> Result<(), ReefsDbLeaseError> {
        let mut inner = self.inner.lock();
        if inner.active_operations != 0 {
            return Err(ReefsDbLeaseError::Draining);
        }
        inner.state = ReefsDbLifecycleState::Closed;
        self.drained.notify_all();
        Ok(())
    }

    pub(crate) fn notify_waiters(&self) {
        self.drained.notify_all();
    }

    pub(crate) fn state(&self) -> ReefsDbLifecycleState {
        self.inner.lock().state
    }

    pub(crate) fn active_operations(&self) -> usize {
        self.inner.lock().active_operations
    }

    pub(crate) fn is_retired(&self) -> bool {
        self.inner.lock().retired
    }
}

#[derive(Debug)]
pub(crate) struct LeaseToken {
    lifecycle: Arc<ReefsDbLifecycle>,
}

impl Drop for LeaseToken {
    fn drop(&mut self) {
        #[cfg(feature = "slow-trace")]
        let lock_started = slow_trace::now();
        let mut inner = self.lifecycle.inner.lock();
        #[cfg(feature = "slow-trace")]
        let lock_wait = slow_trace::elapsed(lock_started);
        inner.active_operations = inner
            .active_operations
            .checked_sub(1)
            .expect("shard operation count underflow");
        if inner.active_operations == 0 {
            self.lifecycle.drained.notify_all();
        }
        #[cfg(feature = "slow-trace")]
        {
            drop(inner);
            slow_trace::log_planner_lock_wait("reefsdb.lifecycle.inner.release", lock_wait);
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReefsDbOperationLease {
    reefsdb: Arc<ReefsDb>,
    _token: Arc<LeaseToken>,
}

impl ReefsDbOperationLease {
    pub(crate) fn new(reefsdb: Arc<ReefsDb>) -> Result<Self, ReefsDbLeaseError> {
        let token = reefsdb.lifecycle().acquire()?;
        Ok(Self {
            reefsdb,
            _token: token,
        })
    }

    pub fn reefsdb(&self) -> &Arc<ReefsDb> {
        &self.reefsdb
    }

    pub(crate) fn belongs_to(&self, reefsdb: &ReefsDb) -> bool {
        std::ptr::eq(self.reefsdb.as_ref(), reefsdb)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReefsDbOperationLeaseSet(Vec<ReefsDbOperationLease>);

impl ReefsDbOperationLeaseSet {
    pub(crate) fn new(leases: Vec<ReefsDbOperationLease>) -> Self {
        Self(leases)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &ReefsDbOperationLease> {
        self.0.iter()
    }

    pub(crate) fn reefsdbs(&self) -> Vec<Arc<ReefsDb>> {
        self.0
            .iter()
            .map(|lease| Arc::clone(lease.reefsdb()))
            .collect()
    }
}

pub(crate) struct BorrowedReefsDbOperationLease<'a> {
    _shard: &'a ReefsDb,
    _token: Arc<LeaseToken>,
}

impl<'a> BorrowedReefsDbOperationLease<'a> {
    pub(crate) fn new(shard: &'a ReefsDb) -> Result<Self, ReefsDbLeaseError> {
        Ok(Self {
            _shard: shard,
            _token: shard.lifecycle().acquire()?,
        })
    }
}
