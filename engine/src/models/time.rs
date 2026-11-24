//! time

use crate::common::time::Time;
use chrono::{DateTime, TimeZone, Utc};
use snafu::{Snafu, ensure};

use crate::models::points::{Result, TimeOutsideRangeSnafu, get_precision_multiplier};

/// Timestamp sentinel indicating that no time is present.
///
/// This value is outside the valid line-protocol timestamp range.
pub const ZERO_TIME: i64 = i64::MIN;

// MinNanoTime is the minimum time that can be represented.
//
// 1677-09-21 00:12:43.145224194 +0000 UTC
//
// The two lowest minimum integers are used as sentinel values.  The
// minimum value needs to be used as a value lower than any other value for
// comparisons and another separate value is needed to act as a sentinel
// default value that is unusable by the user, but usable internally.
// Because these two values need to be used for a special purpose, we do
// not allow users to write points at these two times.
pub const MIN_NANO_TIME_I64: i64 = i64::MIN + 2;

// MaxNanoTime is the maximum time that can be represented.
//
// 2262-04-11 23:47:16.854775806 +0000 UTC
//
// The highest time represented by a nanosecond needs to be used for an
// exclusive range in the shard group, so the maximum time needs to be one
// less than the possible maximum number of nanoseconds representable by an
// int64 so that we don't lose a point at that one time.
pub const MAX_NANO_TIME_I64: i64 = i64::MAX - 1;

// TODO:
// pub const MIN_NANO_TIME: DateTime<Utc> = DateTime { datetime: naive::MIN_DATETIME, offset: Utc };
// pub const MIN_NANO_TIME: DateTime<Utc> = Utc.timestamp_nanos(MIN_NANO_TIME_I64);
// pub const MAX_NANO_TIME: DateTime<Utc> = Utc.timestamp_nanos(MAX_NANO_TIME_I64);

pub const MIN_NANO_TIME: Time = Time::MIN;
pub const MAX_NANO_TIME: Time = Time::MAX;

// /// Error type for time.
// #[derive(Debug, Snafu)]
// pub enum Error {
//     /// Time outside range.
//     #[snafu(display("Time outside range: {} > {}", MIN_NANO_TIME_I64, MAX_NANO_TIME_I64))]
//     TimeOutsideRange,
// }

// /// Result type for time.
// type Result<T> = std::result::Result<T, Error>;

// safe_calc_time safely calculates the time given, it returns error if the time if outside the supported range.
pub fn safe_calc_time(t: i64, precision: &str) -> Result<Time> {
    let mult = get_precision_multiplier(precision);
    let (t, ok) = safe_signed_mult(t, mult);
    ensure!(ok, TimeOutsideRangeSnafu);
    let tme = Time::from_timestamp_nanos(t);
    ensure!(check_time(&tme).is_ok(), TimeOutsideRangeSnafu);

    Ok(tme)
}

// check_time checks that a time is within the safe range.
pub fn check_time(t: &Time) -> Result<()> {
    ensure!(
        *t >= MIN_NANO_TIME && *t <= MAX_NANO_TIME,
        TimeOutsideRangeSnafu
    );
    Ok(())
}

// perform the multiplication and check to make sure it didn't overflow.
fn safe_signed_mult(a: i64, b: i64) -> (i64, bool) {
    if a == 0 || b == 0 || a == 1 || b == 1 {
        return (a * b, true);
    }
    if a == MIN_NANO_TIME_I64 || b == MAX_NANO_TIME_I64 {
        return (0, false);
    }

    let c = a * b;

    (c, c / b == a)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_time_remains_outside_the_valid_storage_range() {
        assert_eq!(ZERO_TIME, i64::MIN);
        assert_eq!(MIN_NANO_TIME_I64, i64::MIN + 2);
        assert_eq!(MAX_NANO_TIME_I64, i64::MAX - 1);
        assert!(ZERO_TIME < MIN_NANO_TIME_I64);
    }
}
