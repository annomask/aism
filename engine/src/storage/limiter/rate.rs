//! Rate limiter.
// use chrono::Duration;
use std::{
    ops::{Add, Deref, DerefMut, Sub},
    sync::Arc,
    time::Duration,
};

use crate::common::time::*;
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limit(pub f64);

/// Inf is the infinite rate limit; it allows all events (even if burst is zero).
pub const INF: Limit = Limit(f64::MAX);
///  INF_DURATION is the duration returned by delay when a reservation is not OK.
pub const INF_DURATION: Duration = Duration::MAX;

/// Error type for rate.
#[derive(Debug, Snafu)]
pub enum Error {
    /// Wait exceeds limiter's burst.
    #[snafu(display("rate: wait(n={}) exceeds limiter's burst {}", wait, burst))]
    WaitExceedsBurst { wait: i64, burst: i64 },

    /// Wait exceeds limiter's deadline.
    #[snafu(display("rate: wait(n={}) exceeds limiter's deadline", wait))]
    WaitExceedsDeadline { wait: i64 },
}

/// Result type for Limiter.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// Every converts a minimum time interval between events to a Limit.
fn every(interval: Duration) -> Limit {
    if interval <= Duration::ZERO {
        return INF;
    }
    Limit(1.0 / interval.as_secs_f64())
}

// A Limiter controls how frequently events are allowed to happen.
// It implements a "token bucket" of size b, initially full and refilled
// at rate r tokens per second.
// Informally, in any large enough time interval, the Limiter limits the
// rate to r tokens per second, with a maximum burst size of b events.
// As a special case, if r == Inf (the infinite rate), b is ignored.
// See https://en.wikipedia.org/wiki/Token_bucket for more about token buckets.
//
// The zero value is a valid Limiter, but it will reject all events.
// Use NewLimiter to create non-zero Limiters.
//
// Limiter has three main methods, Allow, Reserve, and Wait.
// Most callers should use Wait.
//
// Each of the three methods consumes a single token.
// They differ in their behavior when no token is available.
// If no token is available, Allow returns false.
// If no token is available, Reserve returns a reservation for a future token
// and the amount of time the caller must wait before using it.
// If no token is available, Wait blocks until one can be obtained
// or its associated context.Context is canceled.
//
// The methods AllowN, ReserveN, and WaitN consume n tokens.
#[derive(Clone, Debug)]
pub struct Limiter(Arc<RwLock<Inner>>);

#[derive(Debug)]
pub struct Inner {
    limit: Limit,
    burst: i64, // the maximum number of bytes that can be consumed in a single call to Allow

    tokens: f64,
    // last is the last time the limiter's tokens field was updated
    last: Time,
    // lastEvent is the latest time of a rate-limited event (past or future)
    last_event: Time,
}

impl Limiter {
    /// new returns a new Limiter that allows events up to rate r and permits
    /// bursts of at most b tokens.
    pub fn new(r: Limit, b: i64) -> Self {
        Limiter(Arc::new(RwLock::new(Inner {
            limit: r,
            burst: b,
            tokens: 0.0,
            last: Time::MIN,
            last_event: Time::MIN,
        })))
    }

    // /// limit returns the maximum overall event rate.
    // pub fn limiter(&self) -> Limit {

    // }

    /// burst returns the maximum burst size. Burst is the maximum number of tokens
    /// that can be consumed in a single call to Allow, Reserve, or Wait, so higher
    /// Burst values allow more events to happen at once.
    /// A zero Burst allows no events, unless limit == Inf.
    pub fn burst(&self) -> i64 {
        self.read().burst
    }

    /// allow is shorthand for allow_n(Time::now(), 1).
    pub fn allow(&self) -> bool {
        self.allow_n(Time::now(), 1)
    }

    /// allow_n reports whether n events may happen at time now.
    /// Use this method if you intend to drop / skip events that exceed the rate limit.
    /// Otherwise use Reserve or Wait.
    pub fn allow_n(&self, now: Time, n: i64) -> bool {
        self.new_reserve_n(now, n, Duration::new(0, 0)).ok
    }

    /// reserve is shorthand for reserve_n(Time::now(), 1).
    pub fn reserve(&self) -> Reservation {
        self.reserve_n(Time::now(), 1)
    }

    /// reserve_n is a helper method for allow_n, reserve_n, and wait_n.
    /// maxFutureReserve specifies the maximum reservation wait duration allowed.
    /// reserve_n returns Reservation, not *Reservation, to avoid allocation in allow_n and wait_n.
    pub fn reserve_n(&self, now: Time, n: i64) -> Reservation {
        self.new_reserve_n(now, n, INF_DURATION)
    }

    /// wait is shorthand for wait_n(1, Duration::from_nanos(0)).
    pub fn wait(&self) -> Result<()> {
        self.wait_n(1, Duration::from_nanos(0))
    }

    // TODO:
    /// wait_n blocks until lim permits n events to happen.
    /// It returns an error if n exceeds the Limiter's burst size,
    /// The burst limit is ignored if the rate limit is INF.
    pub fn wait_n(&self, n: i64, wait_limit: Duration) -> Result<()> {
        let burst: i64;
        let limit: Limit;
        {
            let lim = self.write();
            burst = lim.burst;
            limit = lim.limit;
        }

        ensure!(
            !(n > burst && limit != INF),
            WaitExceedsBurstSnafu { wait: n, burst }
        );

        // Determine wait limit
        let now = Time::now();
        let wait_limit = match wait_limit.eq(&Duration::ZERO) {
            true => INF_DURATION,
            false => wait_limit,
        };

        // Reserve
        let r = self.new_reserve_n(now, n, wait_limit);
        ensure!(r.ok, WaitExceedsDeadlineSnafu { wait: n });

        // Wait if necessary
        let delay = r.delay_from(now);
        if delay == Duration::ZERO {
            return Ok(());
        }

        let ticker = crossbeam_channel::tick(delay);
        crossbeam_channel::select! {
            recv(ticker) -> _ => Ok(()),
            // default => Ok(()),
        }
    }

    /// set_limit is shorthand for set_limit_at(Time::Now(), new_limit).
    pub fn set_limit(&mut self, new_limit: Limit) {
        self.set_limit_at(Time::now(), new_limit);
    }

    /// set_limit_at sets a new limit for the limiter. The new limit, and burst, may be violated
    /// or underutilized by those which reserved (using reserve or wait) but did not yet act
    /// before set_limit_at was called.
    pub fn set_limit_at(&mut self, now: Time, new_limit: Limit) {
        let mut lim = self.write();

        let (now, _, tokens) = self.advance(now, lim.limit, lim.burst, lim.tokens, lim.last);
        lim.last = now;
        lim.tokens = tokens;
        lim.limit = new_limit;
    }

    /// set_burst is shorthand for set_burst(Time::Now(), new_burst).
    pub fn set_burst(&mut self, new_burst: i64) {
        self.set_burst_at(Time::now(), new_burst);
    }

    /// set_burst_at sets a new burst size for the limiter.
    pub fn set_burst_at(&mut self, now: Time, new_burst: i64) {
        let mut lim = self.write();

        let (now, _, tokens) = self.advance(now, lim.limit, lim.burst, lim.tokens, lim.last);
        lim.last = now;
        lim.tokens = tokens;
        lim.burst = new_burst;
    }

    /// new_reserve_n is a helper method for allow_n, reserve_n, and wait_n.
    /// maxFutureReserve specifies the maximum reservation wait duration allowed.
    /// new_reserve_n returns Reservation, not *Reservation, to avoid allocation in allow_n and wait_n.
    pub fn new_reserve_n(&self, now: Time, n: i64, max_future_reserve: Duration) -> Reservation {
        let mut lim = self.write();
        if lim.limit == INF {
            drop(lim);
            return Reservation {
                ok: true,
                lim: self,
                tokens: n,
                time_to_act: now,
                limit: Limit(0.0),
            };
        }
        let (now, last, mut tokens) = self.advance(now, lim.limit, lim.burst, lim.tokens, lim.last);
        // Calculate the remaining number of tokens resulting from the request.
        tokens -= n as f64;
        // Calculate the wait duration
        let wait_duration: Duration = if tokens < 0.0 {
            Duration::from_nanos(lim.limit.duration_from_tokens(-tokens) as u64)
        } else {
            Duration::ZERO
        };
        // Decide result
        let ok = (n <= lim.burst) && (wait_duration <= max_future_reserve);
        // Prepare reservation
        let mut r = Reservation {
            ok,
            lim: self,
            tokens: 0,
            time_to_act: Time::MIN,
            limit: lim.limit,
        };
        if ok {
            r.tokens = n;
            r.time_to_act = now.add(wait_duration);
        }
        // Update state
        if ok {
            lim.last = now;
            lim.tokens = tokens;
            lim.last_event = r.time_to_act;
        } else {
            lim.last = last;
        }
        r
    }

    /// advance calculates and returns an updated state for lim resulting from the passage of time.
    /// lim is not changed.
    /// it returns (new_now, new_last, new_tokens)
    pub fn advance(
        &self,
        now: Time,
        limit: Limit,
        burst: i64,
        tokens: f64,
        last: Time,
    ) -> (Time, Time, f64) {
        let mut last = last;
        if now.cmp(&last) == std::cmp::Ordering::Less {
            last = now
        }

        // Avoid making delta overflow below when last is very old.
        let max_elapsed = limit.duration_from_tokens(burst as f64 - tokens);
        let mut elapsed = now.sub(last);
        if max_elapsed > 0 && (elapsed.as_nanos() > max_elapsed as u128) {
            elapsed = Duration::from_nanos(max_elapsed as u64);
        }

        // Calculate the new number of tokens, due to time that passed.
        let delta = limit.tokens_from_duration(elapsed);
        let mut tokens = tokens + delta;
        let burst = burst as f64;
        if tokens > burst {
            tokens = burst;
        }

        (now, last, tokens)
    }
}

impl Deref for Limiter {
    type Target = Arc<RwLock<Inner>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Limiter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// A Reservation holds information about events that are permitted by a Limiter to happen after a delay.
// A Reservation may be canceled, which may enable the Limiter to permit additional events.
pub struct Reservation<'a> {
    pub(crate) ok: bool,
    lim: &'a Limiter,
    tokens: i64,
    time_to_act: Time,
    // This is the Limit at reservation time, it can change later.
    limit: Limit,
}

impl<'a> Reservation<'a> {
    /// Returns whether the limiter can provide the requested number of tokens
    /// within the maximum wait time. If ok is false, delay returns  INF_DURATION, and
    /// cancel does nothing.
    fn ok(&self) -> bool {
        self.ok
    }

    /// delay is shorthand for delay_from(Time::now()).
    fn delay(&self) -> Duration {
        self.delay_from(Time::now())
    }

    /// delay_from returns the duration for which the reservation holder must wait
    /// before taking the reserved action.  Zero duration means act immediately.
    ///  INF_DURATION means the limiter cannot grant the tokens requested in this
    /// Reservation within the maximum wait time.
    fn delay_from(&self, now: Time) -> Duration {
        if !self.ok {
            return INF_DURATION;
        }
        // let delay = self.time_to_act.checked_duration_since(now).unwrap();
        let delay = self.time_to_act - now;
        if delay < Duration::ZERO {
            return Duration::ZERO;
        }
        delay
    }

    /// cancel is shorthand for CancelAt(time.Now()).
    fn cancel(&self) {
        self.cancel_at(Time::now())
    }

    /// cancel_at indicates that the reservation holder will not perform the reserved action
    /// and reverses the effects of this Reservation on the rate limit as much as possible,
    /// considering that other reservations may have already been made.
    fn cancel_at(&self, now: Time) {
        if !self.ok {
            return;
        }

        let mut lim = self.lim.write();

        if lim.limit == INF
            || self.tokens == 0
            || self.time_to_act.cmp(&now) == std::cmp::Ordering::Less
        {
            return;
        }

        // calculate tokens to restore
        // The duration between lim.last_event and self.time_to_act tells us how many tokens were reserved
        // after r was obtained. These tokens should not be restored.
        let restore_tokens = (self.tokens as f64)
            - self
                .limit
                .tokens_from_duration(lim.last_event.sub(self.time_to_act));
        if restore_tokens <= 0.0 {
            return;
        }
        // advance time to now
        let (now, _, mut tokens) = self
            .lim
            .advance(now, lim.limit, lim.burst, lim.tokens, lim.last);
        // calculate new number of tokens
        tokens += restore_tokens;
        let burst = lim.burst as f64;
        if tokens > burst {
            tokens = burst
        }
        // update state
        lim.last = now;
        lim.tokens = tokens;
        if self.time_to_act == lim.last_event {
            let prev_event = self.time_to_act.sub(Duration::from_nanos(
                self.limit.duration_from_tokens(self.tokens as f64) as u64,
            ));
            match prev_event.cmp(&now) {
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                    lim.last_event = prev_event
                }
                _ => {}
            }
        }
    }
}

impl Limit {
    pub fn new(max_freq: f64) -> Self {
        Limit(max_freq)
    }

    // duration_from_tokens is a unit conversion function from the number of tokens to the duration
    // of time (nanoseconds) it takes to accumulate them at a rate of limit tokens per second.
    pub fn duration_from_tokens(&self, tokens: f64) -> i64 {
        let seconds = tokens / self.0;
        (1e9 * seconds) as i64
    }

    // tokens_from_duration is a unit conversion function from a time duration to the number of tokens
    // which could be accumulated during that duration at a rate of limit tokens per second.
    fn tokens_from_duration(&self, d: Duration) -> f64 {
        // Split the integer and fractional parts ourself to minimize rounding errors.
        let sec = (d.as_secs() as f64) * (self.0);
        let nsec = (d.as_nanos() as f64) * (self.0);
        sec + nsec / 1e9
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicI32, AtomicU32, Ordering},
        },
        time::{Duration, Instant},
    };

    use crate::common::time::*;
    use lazy_static::lazy_static;

    use crate::storage::limiter::rate::*;

    #[test]
    fn test_limit() {
        assert_ne!(Limit(10.0), INF, "Limit(10) == INF should be false");
    }

    fn close_enough(a: Limit, b: Limit) -> bool {
        (f64::abs(a.0 / b.0) - 1.0) < 1e-9
    }

    #[test]
    fn test_limit_every() {
        struct Case {
            interval: Duration,
            lim: Limit,
        }

        let cases = vec![
            Case {
                interval: Duration::ZERO,
                lim: INF,
            },
            Case {
                interval: Duration::from_nanos(1),
                lim: INF,
            },
            Case {
                interval: Duration::from_micros(1),
                lim: Limit(1e9),
            },
            Case {
                interval: Duration::from_millis(1),
                lim: Limit(1e6),
            },
            Case {
                interval: Duration::from_millis(1),
                lim: Limit(1e3),
            },
            Case {
                interval: Duration::from_millis(10),
                lim: Limit(100.0),
            },
            Case {
                interval: Duration::from_millis(100),
                lim: Limit(10.0),
            },
            Case {
                interval: Duration::from_secs(1),
                lim: Limit(1.0),
            },
            Case {
                interval: Duration::from_secs(2),
                lim: Limit(0.5),
            },
            Case {
                interval: Duration::from_secs_f64(2.5),
                lim: Limit(0.4),
            },
            Case {
                interval: Duration::from_secs(4),
                lim: Limit(0.25),
            },
            Case {
                interval: Duration::from_secs(10),
                lim: Limit(0.1),
            },
            Case {
                interval: Duration::from_secs(u64::MAX),
                lim: Limit(1e9 / (i64::MAX as f64)),
            },
        ];

        for tc in &cases {
            let lim = every(tc.interval);
            assert!(
                close_enough(lim, tc.lim),
                "Every({:?}) = {:?} want {:?}",
                tc.interval,
                lim,
                tc.lim
            );
        }
    }

    const D: u32 = 100_000_000; // 100 * Duration::MILLISECOND
    lazy_static! {
        static ref T0: Time = Time::now();
        static ref T1: Time = *T0 + Duration::from_nanos(1) * D;
        static ref T2: Time = *T0 + Duration::from_nanos(2) * D;
        static ref T3: Time = *T0 + Duration::from_nanos(3) * D;
        static ref T4: Time = *T0 + Duration::from_nanos(4) * D;
        static ref T5: Time = *T0 + Duration::from_nanos(5) * D;
        static ref T9: Time = *T0 + Duration::from_nanos(9) * D;
    }
    struct Allow {
        t: Time,
        n: i64,
        ok: bool,
    }

    fn run(lim: &Limiter, allows: &[Allow]) {
        for (i, allow) in allows.iter().enumerate() {
            let ok = lim.allow_n(allow.t, allow.n);
            assert_eq!(
                ok, allow.ok,
                "step {}: lim.allow_n({:?}, {}) = {}, want {}",
                i, allow.t, allow.n, ok, allow.ok
            );
        }
    }

    #[test]
    fn test_limiter_burst_1() {
        let limiter = Limiter::new(Limit(10.0), 1);
        let allows = vec![
            Allow {
                t: *T0,
                n: 1,
                ok: true,
            },
            Allow {
                t: *T0,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T0,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T1,
                n: 1,
                ok: true,
            },
            Allow {
                t: *T1,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T1,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T2,
                n: 2,
                ok: false,
            }, // burst size is 1, so n=2 always fails
            Allow {
                t: *T2,
                n: 1,
                ok: true,
            },
            Allow {
                t: *T2,
                n: 1,
                ok: false,
            },
        ];
        run(&limiter, &allows);
    }

    #[test]
    fn test_limiter_burst_3() {
        let limiter = Limiter::new(Limit(10.0), 3);
        let allows = vec![
            Allow {
                t: *T0,
                n: 2,
                ok: true,
            },
            Allow {
                t: *T0,
                n: 2,
                ok: false,
            },
            Allow {
                t: *T0,
                n: 1,
                ok: true,
            },
            Allow {
                t: *T0,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T1,
                n: 4,
                ok: false,
            },
            Allow {
                t: *T2,
                n: 1,
                ok: true,
            },
            Allow {
                t: *T3,
                n: 1,
                ok: true,
            },
            Allow {
                t: *T4,
                n: 1,
                ok: true,
            },
            Allow {
                t: *T4,
                n: 1,
                ok: true,
            },
            Allow {
                t: *T4,
                n: 1,
                ok: false,
            }, // fail
            Allow {
                t: *T4,
                n: 1,
                ok: false,
            }, // fail
            Allow {
                t: *T9,
                n: 3,
                ok: true,
            },
            Allow {
                t: *T9,
                n: 0,
                ok: true,
            },
        ];
        run(&limiter, &allows);
    }

    #[test]
    fn test_limiter_jump_backwards() {
        let limiter = Limiter::new(Limit(10.0), 3);
        let allows = vec![
            Allow {
                t: *T1,
                n: 1,
                ok: true,
            }, // start at t1
            Allow {
                t: *T0,
                n: 1,
                ok: true,
            }, // jump back to t0, two tokens remain
            Allow {
                t: *T0,
                n: 1,
                ok: true,
            },
            Allow {
                t: *T0,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T0,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T1,
                n: 1,
                ok: true,
            }, // got a token
            Allow {
                t: *T1,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T1,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T2,
                n: 1,
                ok: true,
            }, // got another token
            Allow {
                t: *T2,
                n: 1,
                ok: false,
            },
            Allow {
                t: *T2,
                n: 1,
                ok: false,
            },
        ];
        run(&limiter, &allows);
    }

    // Ensure that tokensFromDuration doesn't produce
    // rounding errors by truncating nanoseconds.
    #[test]
    fn test_limiter_no_truncation_errors() {
        assert!(
            Limiter::new(Limit(0.7692307692307693), 1).allow(),
            "expected true"
        );
    }

    #[test]
    fn test_simulataneous_requests() {
        const LIMIT: f64 = 1.0;
        const BURST: i64 = 5;
        const NUM_REQUESTS: usize = 15;

        let num_ok = Arc::new(AtomicU32::new(0));

        // Very slow replenishing bucket.
        let lim = Arc::new(Limiter::new(Limit(LIMIT), BURST));

        // Tries to take a token, atomically updates the counter and decreases the wait group counter.
        let mut handles = Vec::with_capacity(10);
        for i in 0..NUM_REQUESTS {
            let lim = Arc::clone(&lim);
            let num_ok = Arc::clone(&num_ok);
            handles.push(std::thread::spawn(move || {
                if lim.allow() {
                    num_ok.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        // Wait for other threads to finish.
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            num_ok.load(Ordering::SeqCst) as i64,
            BURST,
            "num_ok: {}, want: {}",
            num_ok.load(Ordering::SeqCst),
            BURST
        );
    }

    // #[test]
    // fn test_long_running_qps() {
    //     // The test runs for a few seconds executing many requests and then checks
    //     // that overall number of requests is reasonable.
    //     const LIMIT: f64 = 100.0;
    //     const BURST: i64 = 100;

    //     let num_ok = Arc::new(AtomicI32::new(0));

    //     // Very slow replenishing bucket.
    //     let lim = Arc::new(Limiter::new(Limit(LIMIT), BURST));

    //     // Tries to take a token, atomically updates the counter and decreases the wait group counter.
    //     let mut handles = Vec::with_capacity(10);
    //     let start = Instant::now();
    //     let end = start + Duration::from_secs(5);
    //     while Instant::now() < end {
    //         let lim = Arc::clone(&lim);
    //         let num_ok = Arc::clone(&num_ok);
    //         handles.push(std::thread::spawn(move || {
    //             if lim.allow() {
    //                 num_ok.fetch_add(1, Ordering::SeqCst);
    //             }
    //         }));

    //         // This will still offer ~500 requests per second, but won't consume
    //         // outrageous amount of CPU.
    //         std::thread::sleep(Duration::from_millis(2));
    //     }

    //     // Wait for other threads to finish.
    //     for handle in handles {
    //         handle.join().unwrap();
    //     }

    //     let elapsed = start.elapsed();
    //     let ideal = BURST as f64
    //         + (LIMIT * (elapsed.as_nanos() as f64) / (Duration::from_secs(1).as_nanos() as f64));
    //     // We should never get more requests than allowed.
    //     let want = (ideal + 1.0) as i32;
    //     assert!(
    //         num_ok.load(Ordering::SeqCst) <= want,
    //         "num_ok = {}, want {} (ideal {})",
    //         num_ok.load(Ordering::SeqCst),
    //         want,
    //         ideal
    //     );

    //     // FIXME: too close ?
    //     // want = 601, num_ok = 599
    //     // We should get very close to the number of requests allowed.
    //     assert!(
    //         num_ok.load(Ordering::SeqCst) >= want,
    //         "num_ok = {}, want {} (ideal {})",
    //         num_ok.load(Ordering::SeqCst),
    //         want,
    //         ideal
    //     );
    // }

    struct Request {
        t: Time,
        n: i64,
        act: Time,
        ok: bool,
    }

    // converts a duration to a multiple of the global const d
    fn d_from_duration(dur: Duration) -> i64 {
        // Adding a millisecond to be swallowed by the integer division
        // because we don't care about small inaccuracies
        ((dur + Duration::from_millis(1)).as_nanos() / (D as u128)) as i64
    }

    // returns multiples of d since t0
    fn d_since(t: Time) -> i64 {
        d_from_duration(t - *T0)
    }

    fn run_reserve(lim: &Limiter, req: Request) -> Reservation {
        run_reserve_max(lim, req, INF_DURATION)
    }

    fn run_reserve_max(lim: &Limiter, req: Request, max_reserve: Duration) -> Reservation {
        let r = lim.new_reserve_n(req.t, req.n, max_reserve);
        if r.ok && (d_since(r.time_to_act) != d_since(req.act)) || r.ok != req.ok {
            panic!(
                "li,.reserve_n(t{}, {}, {:?}) = (t{}, {}) want (t{}, {})",
                d_since(req.t),
                req.n,
                max_reserve,
                d_since(r.time_to_act),
                r.ok,
                d_since(req.act),
                req.ok
            );
        }

        r
    }

    #[test]
    fn test_simple_reserve() {
        let lim = Limiter::new(Limit(10.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
        );
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T2,
                ok: true,
            },
        );
        run_reserve(
            &lim,
            Request {
                t: *T3,
                n: 2,
                act: *T4,
                ok: true,
            },
        );
    }

    #[test]
    fn test_mix() {
        let lim = Limiter::new(Limit(10.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 3,
                act: *T1,
                ok: false,
            },
        ); // should return false because n > burst
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
        );
        run(
            &lim,
            &[Allow {
                t: *T1,
                n: 2,
                ok: false,
            }],
        ); // not enought tokens - don't allow
        run_reserve(
            &lim,
            Request {
                t: *T1,
                n: 2,
                act: *T2,
                ok: true,
            },
        );
        run(
            &lim,
            &[Allow {
                t: *T1,
                n: 1,
                ok: false,
            }],
        ); // negative tokens - don't allow
        run(
            &lim,
            &[Allow {
                t: *T3,
                n: 1,
                ok: true,
            }],
        );
    }

    #[test]
    fn test_cancel_invalid() {
        let lim = Limiter::new(Limit(10.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
        );
        let r = run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 3,
                act: *T3,
                ok: false,
            },
        );
        r.cancel_at(*T0); // should have no effect
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T2,
                ok: true,
            },
        ); // did not get extra tokens
    }

    #[test]
    fn test_cancel_last() {
        let lim = Limiter::new(Limit(10.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
        );
        let r = run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T2,
                ok: true,
            },
        );
        r.cancel_at(*T1); // got 2 tokens back
        run_reserve(
            &lim,
            Request {
                t: *T1,
                n: 2,
                act: *T2,
                ok: true,
            },
        );
    }

    #[test]
    fn test_cancel_too_late() {
        let lim = Limiter::new(Limit(10.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
        );
        let r = run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T2,
                ok: true,
            },
        );
        r.cancel_at(*T3); // too late to cancel - should have no effect
        run_reserve(
            &lim,
            Request {
                t: *T3,
                n: 2,
                act: *T4,
                ok: true,
            },
        );
    }

    #[test]
    fn test_cancel_10_tokens() {
        let lim = Limiter::new(Limit(10.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
        );
        let r = run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 1,
                act: *T1,
                ok: true,
            },
        );
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 1,
                act: *T2,
                ok: true,
            },
        );
        r.cancel_at(*T0); // got 0 tokens back
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 1,
                act: *T3,
                ok: true,
            },
        );
    }

    #[test]
    fn test_cancel_11_tokens() {
        let lim = Limiter::new(Limit(10.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
        );
        let r = run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T2,
                ok: true,
            },
        );
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 1,
                act: *T3,
                ok: true,
            },
        );
        r.cancel_at(*T0); // got 1 token back
        run_reserve(
            &lim,
            Request {
                t: *T2,
                n: 2,
                act: *T4,
                ok: true,
            },
        );
    }

    // FIXME: bug
    #[test]
    fn test_cancel_multi() {
        let lim = Limiter::new(Limit(10.0), 4);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 4,
                act: *T0,
                ok: true,
            },
        );
        let ra = run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 3,
                act: *T3,
                ok: true,
            },
        );
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 1,
                act: *T4,
                ok: true,
            },
        );
        let rc = run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 1,
                act: *T5,
                ok: true,
            },
        );
        rc.cancel_at(*T1); // get 1 token back
        ra.cancel_at(*T1); // get 2 tokens back, as if C was never reserved
        run_reserve(
            &lim,
            Request {
                t: *T1,
                n: 3,
                act: *T5,
                ok: true,
            },
        );
    }

    #[test]
    fn test_reserve_jump_back() {
        let lim = Limiter::new(Limit(10.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T1,
                n: 2,
                act: *T1,
                ok: true,
            },
        ); // start at t1
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 1,
                act: *T1,
                ok: true,
            },
        ); // should violate limit, burst
        run_reserve(
            &lim,
            Request {
                t: *T2,
                n: 2,
                act: *T3,
                ok: true,
            },
        );
    }

    #[test]
    fn test_reserve_jump_back_cancel() {
        let lim = Limiter::new(Limit(10.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T1,
                n: 2,
                act: *T1,
                ok: true,
            },
        ); // start at t1
        let r = run_reserve(
            &lim,
            Request {
                t: *T1,
                n: 2,
                act: *T3,
                ok: true,
            },
        );
        run_reserve(
            &lim,
            Request {
                t: *T1,
                n: 1,
                act: *T4,
                ok: true,
            },
        );
        r.cancel_at(*T0); // cancel at t0, get 1 token back
        run_reserve(
            &lim,
            Request {
                t: *T1,
                n: 2,
                act: *T4,
                ok: true,
            },
        ); // should violate limit, burst
    }

    #[test]
    fn test_reserve_set_limit() {
        let mut lim = Limiter::new(Limit(5.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
        );
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T4,
                ok: true,
            },
        );
        lim.set_limit_at(*T2, Limit(10.0));
        run_reserve(
            &lim,
            Request {
                t: *T2,
                n: 1,
                act: *T4,
                ok: true,
            },
        ); // violates limit and burst
    }

    #[test]
    fn test_reserve_set_burst() {
        let mut lim = Limiter::new(Limit(5.0), 2);

        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
        );
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T4,
                ok: true,
            },
        );
        lim.set_burst_at(*T3, 4);
        run_reserve(
            &lim,
            Request {
                t: *T0,
                n: 4,
                act: *T9,
                ok: true,
            },
        ); // violates limit and burst
    }

    // FIXME:
    #[test]
    fn test_reserve_set_limit_cancel() {
        // let mut lim = Limiter::new(Limit(5.0), 2);

        // run_reserve(&lim, Request { t: *T0, n: 2, act: *T0, ok: true });
        // let r  = run_reserve(&lim, Request { t: *T0, n: 2, act: *T4, ok: true });
        // lim.set_limit_at(*T2, Limit(10.0));
        // r.cancel_at(*T2);  // 2 tokens back
        // run_reserve(&lim, Request { t: *T2, n: 2, act: *T3, ok: true });
    }

    #[test]
    fn test_reserve_max() {
        let lim = Limiter::new(Limit(10.0), 2);
        let max_t = Duration::from_nanos(D as u64);

        run_reserve_max(
            &lim,
            Request {
                t: *T0,
                n: 2,
                act: *T0,
                ok: true,
            },
            max_t,
        );
        run_reserve_max(
            &lim,
            Request {
                t: *T0,
                n: 1,
                act: *T1,
                ok: true,
            },
            max_t,
        ); // reserve for close future
        run_reserve_max(
            &lim,
            Request {
                t: *T0,
                n: 1,
                act: *T2,
                ok: false,
            },
            max_t,
        ); // time to act too far in the future
    }

    struct Wait {
        name: String,
        n: i64,
        delay: i64, // in multiples of d
        nil_err: bool,
    }

    fn run_wait(lim: &Limiter, wait_limit: Duration, w: Wait) {
        let start = std::time::Instant::now();
        let err = lim.wait_n(w.n, wait_limit);
        let delay = start.elapsed();
        if (w.nil_err && err.is_err())
            || (!w.nil_err && err.is_ok())
            || (w.delay != d_from_duration(delay))
        {
            let mut err_string = "<nil>";
            if !w.nil_err {
                err_string = "<non-nil error>";
            }
            panic!(
                "lim.wait_n({}, lim,, {}) = {:?} with delay {:?}; want {} with delay {:?}",
                w.name,
                w.n,
                err,
                delay,
                err_string,
                D * std::time::Duration::from_nanos(w.delay as u64)
            );
        }
    }

    #[test]
    fn test_wait_simple() {
        let lim = Limiter::new(Limit(10.0), 3);

        // TODO: add atomic_bool cancel flag in wait_n, adn test already-canceled
        // run_wait(&lim, Wait {name: "alreadyt-canceled".to_string(), n: 1, delay: 0, nil_err: false});
        run_wait(
            &lim,
            Duration::from_nanos(0),
            Wait {
                name: "exceed-burst-error".to_string(),
                n: 4,
                delay: 0,
                nil_err: false,
            },
        );
        run_wait(
            &lim,
            Duration::from_nanos(0),
            Wait {
                name: "act-now".to_string(),
                n: 2,
                delay: 0,
                nil_err: true,
            },
        );
        run_wait(
            &lim,
            Duration::from_nanos(0),
            Wait {
                name: "act-later".to_string(),
                n: 3,
                delay: 2,
                nil_err: true,
            },
        );
    }

    // TODO: add atomic_bool cancel flag in wait_n ?
    // #[test]
    // fn test_wait_cancel() {
    //     let lim = Limiter::new(Limit(10.0), 3);
    //     run_wait(&lim, Wait {name: "act-now".to_string(), n: 2, delay: 0, nil_err: true});  // after this lim.tokens = 1

    // }

    #[test]
    fn test_wait_timeout() {
        let lim = Limiter::new(Limit(10.0), 3);

        run_wait(
            &lim,
            Duration::from_nanos(D as u64),
            Wait {
                name: "act-now".to_string(),
                n: 2,
                delay: 0,
                nil_err: true,
            },
        );
        run_wait(
            &lim,
            Duration::from_nanos(D as u64),
            Wait {
                name: "w-timeout-err".to_string(),
                n: 3,
                delay: 0,
                nil_err: false,
            },
        );
    }

    #[test]
    fn test_wait_inf() {
        let lim = Limiter::new(INF, 0);

        run_wait(
            &lim,
            Duration::from_nanos(0),
            Wait {
                name: "exceed-burst-no-error".to_string(),
                n: 3,
                delay: 0,
                nil_err: true,
            },
        );
    }
}
