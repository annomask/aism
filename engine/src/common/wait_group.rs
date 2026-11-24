use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

struct Inner {
    cvar: Condvar,
    count: Mutex<usize>,
}

/// A WaitGroup waits for a collection of threads to finish.
/// The main thread calls [`new`] to creates a new wait group
/// and returns the single reference to it. Then each of the goroutines
/// calls [`add`] to add delta to the wait group counter, and
/// calls [`done`] to decrease the wait group counter by one.
/// [`wait`]  block until all threads have finished their work.
///
/// # Example
///
/// ```rust,ignore
/// use std::{
///     sync::{
///         atomic::{AtomicUsize, Ordering},
///         Arc,
///     },
///     thread::{sleep, spawn},
///     time::Duration,
/// };
///
/// use crate::common::wait_group::WaitGroup;
///
/// let wg = WaitGroup::new();
///
/// for _ in 0..5 {
///     let wg = wg.clone();
///     wg.add(1);
///     spawn(move || {
///         // Do some work.
///
///         // Drop the reference to the wait group.
///         wg.done();
///     });
/// }
/// // Block until all threads have finished their work.
/// wg.wait();
/// ```
///
/// [`wait`]: struct.WaitGroup.html#method.wait
/// [`clone`]: trait.Clone.html#method.clone
pub struct WaitGroup {
    inner: Arc<Inner>,
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                cvar: Condvar::new(),
                count: Mutex::new(1),
            }),
        }
    }
}

impl Clone for WaitGroup {
    fn clone(&self) -> Self {
        let _inner = self.inner.count.lock();

        Self {
            inner: self.inner.clone(),
        }
    }
}

// Drop the reference to the wait group.
// impl Drop for WaitGroup {
//     fn drop(&mut self) {
//         let mut count = self.inner.count.lock();
//         *count -= 1;
//         if *count == 1 {
//             self.inner.cvar.notify_all();
//         }
//     }
// }

impl std::fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.inner.count.lock();
        f.debug_struct("WaitGroup").field("count", &*count).finish()
    }
}

impl WaitGroup {
    /// Creates a new wait group and returns the single reference to it.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use crate::common::wait_group::WaitGroup;
    ///
    /// let wg = WaitGroup::new();
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// waitings return how many jobs are waiting.
    pub fn waitings(&self) -> usize {
        *self.inner.count.lock()
    }

    pub fn add(&self, count: usize) {
        let mut ctr = self.inner.count.lock();

        *ctr += count;
        // Self {
        //     inner: self.inner.clone(),
        // }
    }

    pub fn done(&self) {
        let mut count = self.inner.count.lock();
        *count -= 1;

        if *count == 1 {
            self.inner.cvar.notify_all();
        }
    }

    /// wait blocks until the WaitGroup counter is zero.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::thread;
    ///
    /// use crate::common::wait_group::WaitGroup;
    ///
    /// let wg = WaitGroup::new();
    /// let t_wg = wg.clone();
    /// t_wg.add(1);
    /// thread::spawn(move || {
    ///     // do some time consuming task
    ///     t_wg.done();
    /// });
    ///
    /// // wait other thread completes
    /// wg.wait();
    /// ```
    pub fn wait(&self) {
        if *self.inner.count.lock() == 1 {
            return;
        }

        let mut inner = self.inner.count.lock();
        while *inner > 1 {
            {
                self.inner.cvar.wait(&mut inner);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use crate::common::wait_group::WaitGroup;

    #[test]
    fn test_sync_wait_group_reuse() {
        let wg = WaitGroup::new();
        let ctr = Arc::new(AtomicUsize::new(0));
        for _ in 0..6 {
            let wg = wg.clone();
            wg.add(1);
            let ctrx = ctr.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5));
                ctrx.fetch_add(1, Ordering::Relaxed);
                wg.done();
            });
        }

        wg.wait();
        assert_eq!(ctr.load(Ordering::Relaxed), 6);

        let worker = wg.clone();
        wg.add(1);
        let ctrx = ctr.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            ctrx.fetch_add(1, Ordering::Relaxed);
            worker.done();
        });
        wg.wait();
        assert_eq!(ctr.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn test_sync_wait_group_nested() {
        let wg = WaitGroup::new();
        let ctr = Arc::new(AtomicUsize::new(0));
        for _ in 0..5 {
            let worker = wg.clone();
            worker.add(1);
            let ctrx = ctr.clone();
            std::thread::spawn(move || {
                let nested_worker = worker.clone();
                nested_worker.add(1);
                let ctrxx = ctrx.clone();
                std::thread::spawn(move || {
                    ctrxx.fetch_add(1, Ordering::Relaxed);
                    nested_worker.done();
                });
                ctrx.fetch_add(1, Ordering::Relaxed);
                worker.done();
            });
        }

        wg.wait();
        assert_eq!(ctr.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_sync_wait_group_from() {
        std::thread::scope(|s| {
            let wg = WaitGroup::new();
            for _ in 0..5 {
                let t = wg.clone();
                t.add(1);
                s.spawn(move || {
                    t.done();
                });
            }
            wg.wait();
        });
    }

    #[test]
    fn test_clone_and_fmt() {
        let swg = WaitGroup::new();
        let swg1 = swg.clone();
        swg1.add(1);
        assert_eq!(format!("{:?}", swg), format!("{:?}", swg1));
    }
}
