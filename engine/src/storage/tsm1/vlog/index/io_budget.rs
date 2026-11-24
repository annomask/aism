//! Charge output bytes before IO and keep rate-limit waits cancellable.

use super::{Error, Result, invalid};
use crate::storage::limiter::rate::{Error as RateError, Limiter};
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub(crate) struct IoBudget {
    limiter: Option<Limiter>,
    cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
}
impl Default for IoBudget {
    fn default() -> Self {
        Self::new(None, || false)
    }
}
impl IoBudget {
    pub(crate) fn new(
        limiter: Option<Limiter>,
        cancelled: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            limiter,
            cancelled: Arc::new(cancelled),
        }
    }
    pub(crate) fn charge(&self, mut bytes: usize) -> Result<()> {
        while bytes > 0 {
            if (self.cancelled)() {
                return Err(Error::CompactionCancelled);
            }
            let Some(limiter) = &self.limiter else {
                return Ok(());
            };
            let n = bytes.min(limiter.burst().clamp(1, 16 * 1024) as usize);
            match limiter.wait_n(n as i64, Duration::from_millis(25)) {
                Ok(()) => bytes -= n,
                Err(RateError::WaitExceedsDeadline { .. }) => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Err(_) => return Err(invalid("aggregate output limiter has no usable burst")),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::limiter::rate::Limit;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[test]
    fn exhausted_output_budget_can_be_cancelled() {
        let limiter = Limiter::new(Limit(1.0), 1);
        limiter.wait_n(1, Duration::ZERO).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let budget = IoBudget::new(Some(limiter), move || flag.load(Ordering::Acquire));
        let worker = std::thread::spawn(move || budget.charge(1024));
        stop.store(true, Ordering::Release);
        assert!(matches!(
            worker.join().unwrap(),
            Err(Error::CompactionCancelled)
        ));
    }
}
