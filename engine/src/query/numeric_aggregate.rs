//! Typed reductions over validated blocks and arbitrary ordered time windows.

use super::{
    aggregate_windows::AggregateWindows,
    projected_aggregate::{AggregateNumber, ProjectedAggregateState},
    storage_ir::{AggregatePoint, AggregatePointValue, AggregateWideSum},
};
use crate::models::{FLOAT, FieldType, INTEGER, UNSIGNED};

/// Shared numeric semantics for scalar, array, streaming, and summary reductions.
/// Integer SUM wraps; floating SUM retains input order and starts at positive
/// zero. Extrema use total ordering, including signed zero and NaN payloads.
pub(crate) trait NumericValue: Copy {
    const TYPE: FieldType;
    fn from_number(value: AggregateNumber) -> Result<Self, &'static str>;
    fn number(self) -> AggregateNumber;
    fn wide(self) -> Option<AggregateWideSum> {
        None
    }
    fn endpoint(self) -> AggregatePointValue;
    fn singleton_sum(self) -> Self;
    fn add(self, other: Self) -> Self;
    fn compare(self, other: Self) -> std::cmp::Ordering;
    fn min(self, other: Self) -> Self {
        if other.compare(self).is_lt() {
            other
        } else {
            self
        }
    }
    fn max(self, other: Self) -> Self {
        if other.compare(self).is_gt() {
            other
        } else {
            self
        }
    }
}

macro_rules! integer_value {
    ($ty:ty, $field:ident, $variant:ident) => {
        impl NumericValue for $ty {
            const TYPE: FieldType = $field;
            fn from_number(value: AggregateNumber) -> Result<Self, &'static str> {
                match value {
                    AggregateNumber::$variant(value) => Ok(value),
                    _ => Err("numeric aggregate state has a different type"),
                }
            }
            fn number(self) -> AggregateNumber {
                AggregateNumber::$variant(self)
            }
            fn wide(self) -> Option<AggregateWideSum> {
                Some(AggregateWideSum::$variant(self.into()))
            }
            fn endpoint(self) -> AggregatePointValue {
                AggregatePointValue::$variant(self)
            }
            fn singleton_sum(self) -> Self {
                self
            }
            fn add(self, other: Self) -> Self {
                self.wrapping_add(other)
            }
            fn compare(self, other: Self) -> std::cmp::Ordering {
                self.cmp(&other)
            }
        }
    };
}
integer_value!(i64, INTEGER, Integer);
integer_value!(u64, UNSIGNED, Unsigned);

impl NumericValue for f64 {
    const TYPE: FieldType = FLOAT;
    fn from_number(value: AggregateNumber) -> Result<Self, &'static str> {
        match value {
            AggregateNumber::Float(value) => Ok(value),
            _ => Err("numeric aggregate state has a different type"),
        }
    }
    fn number(self) -> AggregateNumber {
        AggregateNumber::Float(self)
    }
    fn endpoint(self) -> AggregatePointValue {
        AggregatePointValue::Float(self)
    }
    fn singleton_sum(self) -> Self {
        0.0 + self
    }
    fn add(self, other: Self) -> Self {
        self + other
    }
    fn compare(self, other: Self) -> std::cmp::Ordering {
        self.total_cmp(&other)
    }
}

/// Callers verify strict timestamp ordering and block/index consistency first.
pub(crate) fn count_windows(
    times: &[i64],
    field_type: FieldType,
    windows: AggregateWindows<'_>,
    states: &mut [ProjectedAggregateState],
) -> Result<(), &'static str> {
    for (window, span) in windows.ordered_spans(times) {
        let state = states
            .get_mut(window)
            .ok_or("aggregate count window state missing")?;
        if state.field_type != field_type || !state.projection.count_only() {
            return Err("timestamp-only count requires a matching count-only state");
        }
        let count = state
            .count
            .as_mut()
            .ok_or("aggregate count state missing")?;
        *count = count.wrapping_add(span.len() as u64);
    }
    Ok(())
}

/// Callers verify strict timestamp ordering and block/index consistency first.
pub(crate) fn reduce_windows<T: NumericValue>(
    times: &[i64],
    values: &[T],
    windows: AggregateWindows<'_>,
    states: &mut [ProjectedAggregateState],
) -> Result<(), &'static str> {
    if times.len() != values.len() {
        return Err("numeric aggregate arrays have different lengths");
    }
    for (window, span) in windows.ordered_spans(times) {
        let state = states
            .get_mut(window)
            .ok_or("numeric aggregate window state missing")?;
        if state.field_type != T::TYPE {
            return Err("numeric aggregate point has a different field type");
        }
        let values = &values[span.clone()];
        let projection = state.projection;
        let mask = u8::from(projection.sum)
            | (u8::from(projection.min) << 1)
            | (u8::from(projection.max) << 2);
        match mask {
            0 => (),
            1 => reduce_values::<T, 1>(values, state)?,
            2 => reduce_values::<T, 2>(values, state)?,
            3 => reduce_values::<T, 3>(values, state)?,
            4 => reduce_values::<T, 4>(values, state)?,
            5 => reduce_values::<T, 5>(values, state)?,
            6 => reduce_values::<T, 6>(values, state)?,
            7 => reduce_values::<T, 7>(values, state)?,
            _ => unreachable!(),
        }
        if let Some(count) = &mut state.count {
            *count = count.wrapping_add(values.len() as u64);
        }
        let first_time = times[span.start];
        if projection.first
            && state
                .first
                .as_ref()
                .is_none_or(|point| first_time < point.time)
        {
            state.first = Some(AggregatePoint {
                time: first_time,
                value: values[0].endpoint(),
            });
        }
        let last_time = times[span.end - 1];
        if projection.last
            && state
                .last
                .as_ref()
                .is_none_or(|point| last_time > point.time)
        {
            state.last = Some(AggregatePoint {
                time: last_time,
                value: values[values.len() - 1].endpoint(),
            });
        }
    }
    Ok(())
}

/// Select components once per slice; compile out unrequested work in the loop.
pub(crate) fn reduce_values<T: NumericValue, const MASK: u8>(
    values: &[T],
    state: &mut ProjectedAggregateState,
) -> Result<(), &'static str> {
    let mut sum = if MASK & 1 != 0 {
        state.sum.map(T::from_number).transpose()?
    } else {
        None
    };
    let mut wide_sum = state.wide_sum;
    let mut min = if MASK & 2 != 0 {
        state.min.map(T::from_number).transpose()?
    } else {
        None
    };
    let mut max = if MASK & 4 != 0 {
        state.max.map(T::from_number).transpose()?
    } else {
        None
    };
    for value in values {
        if MASK & 1 != 0 {
            wide_sum = AggregateWideSum::merge(wide_sum, value.wide())?;
            // Keep the original per-component order and signed-zero normalization.
            let value = value.singleton_sum();
            sum = Some(sum.map_or(value, |old| old.add(value)));
        }
        if MASK & 2 != 0 {
            min = Some(min.map_or(*value, |old| old.min(*value)));
        }
        if MASK & 4 != 0 {
            max = Some(max.map_or(*value, |old| old.max(*value)));
        }
    }
    if MASK & 1 != 0 {
        state.sum = sum.map(T::number);
        state.wide_sum = wide_sum;
    }
    if MASK & 2 != 0 {
        state.min = min.map(T::number);
    }
    if MASK & 4 != 0 {
        state.max = max.map(T::number);
    }
    Ok(())
}

/// Arithmetic may select either NaN payload for SUM. Every other float result,
/// including signed-zero SUM and ordered extrema/endpoints, must match bits.
#[cfg(test)]
pub(crate) fn test_signature(state: &ProjectedAggregateState) -> String {
    fn number(value: Option<AggregateNumber>) -> String {
        match value {
            Some(AggregateNumber::Float(value)) => format!("float:{}", value.to_bits()),
            value => format!("{value:?}"),
        }
    }
    fn point(value: &Option<AggregatePoint>) -> String {
        match value {
            Some(AggregatePoint {
                time,
                value: AggregatePointValue::Float(value),
            }) => format!("{time}:{}", value.to_bits()),
            value => format!("{value:?}"),
        }
    }
    let sum = match state.sum {
        Some(AggregateNumber::Float(value)) if value.is_nan() => "float:NaN".into(),
        value => number(value),
    };
    format!(
        "{:?}/{:?}/{:?}/{}/{}/{}/{}/{}/{:?}",
        state.field_type,
        state.projection,
        state.count,
        sum,
        number(state.min),
        number(state.max),
        point(&state.first),
        point(&state.last),
        state.wide_sum
    )
}

#[cfg(test)]
mod tests {
    use super::test_signature as signature;
    use super::*;
    use crate::{
        models::points::{FloatValue, IntegerValue, UnsignedValue, Value},
        query::{projected_aggregate::AggregateProjection, storage_ir::TimeRange},
    };

    #[test]
    fn result_comparison_only_normalizes_nan_sum_payloads() {
        let nan_a = f64::from_bits(0x7ff8000000000001);
        let nan_b = f64::from_bits(0xfff8000000000002);
        let mut a =
            ProjectedAggregateState::empty(FLOAT, AggregateProjection::complete(FLOAT, true));
        a.sum = Some(AggregateNumber::Float(nan_a));
        let mut b = a.clone();
        b.sum = Some(AggregateNumber::Float(nan_b));
        assert_eq!(signature(&a), signature(&b));
        a.sum = Some(AggregateNumber::Float(-0.0));
        b.sum = Some(AggregateNumber::Float(0.0));
        assert_ne!(signature(&a), signature(&b));
        for component in 0..4 {
            let mut b = a.clone();
            match component {
                0 => {
                    a.min = Some(AggregateNumber::Float(nan_a));
                    b.min = Some(AggregateNumber::Float(nan_b));
                }
                1 => {
                    a.max = Some(AggregateNumber::Float(nan_a));
                    b.max = Some(AggregateNumber::Float(nan_b));
                }
                2 => {
                    a.first = Some(AggregatePoint {
                        time: 0,
                        value: nan_a.endpoint(),
                    });
                    b.first = Some(AggregatePoint {
                        time: 0,
                        value: nan_b.endpoint(),
                    });
                }
                _ => {
                    a.last = Some(AggregatePoint {
                        time: 0,
                        value: nan_a.endpoint(),
                    });
                    b.last = Some(AggregatePoint {
                        time: 0,
                        value: nan_b.endpoint(),
                    });
                }
            }
            assert_ne!(signature(&a), signature(&b));
        }
    }

    #[test]
    fn numeric_kernels_match_independent_wide_and_total_order_references() {
        use crate::query::storage_ir::{AggregateState, AggregateValues};
        let signed = [i64::MIN, i64::MAX, i64::MAX, -7];
        let unsigned = [u64::MAX, u64::MAX, 0, 7];
        let times = [0, 1, 2, 3];
        let ranges = [TimeRange::new(0, 3)];
        let windows = AggregateWindows::new(&ranges).unwrap();
        let mut complete_signed = AggregateState::empty(INTEGER);
        let mut complete_unsigned = AggregateState::empty(UNSIGNED);
        for value in signed {
            complete_signed.add_integer(value);
        }
        for value in unsigned {
            complete_unsigned.add_unsigned(value);
        }
        let signed_exact: i128 = signed.iter().map(|v| i128::from(*v)).sum();
        let unsigned_exact: u128 = unsigned.iter().map(|v| u128::from(*v)).sum();
        assert_eq!(
            complete_signed.values,
            Some(AggregateValues::Integer {
                sum: signed_exact as i64,
                min: i64::MIN,
                max: i64::MAX,
            })
        );
        assert_eq!(
            complete_unsigned.values,
            Some(AggregateValues::Unsigned {
                sum: unsigned_exact as u64,
                min: 0,
                max: u64::MAX,
            })
        );
        for (typ, complete) in [(INTEGER, complete_signed), (UNSIGNED, complete_unsigned)] {
            let projection = AggregateProjection::complete(typ, false);
            let mut states = [ProjectedAggregateState::empty(typ, projection)];
            if typ == INTEGER {
                reduce_windows(&times, &signed, windows, &mut states).unwrap();
                assert_eq!(
                    states[0].wide_sum,
                    Some(AggregateWideSum::Integer(signed_exact))
                );
            } else {
                reduce_windows(&times, &unsigned, windows, &mut states).unwrap();
                assert_eq!(
                    states[0].wide_sum,
                    Some(AggregateWideSum::Unsigned(unsigned_exact))
                );
            }
            assert_eq!(states[0].clone().into_complete().unwrap(), complete);
        }
        let negative_nan = f64::from_bits(0xfff8000000000002);
        let positive_nan = f64::from_bits(0x7ff8000000000001);
        let mut state =
            ProjectedAggregateState::empty(FLOAT, AggregateProjection::complete(FLOAT, false));
        reduce_values::<f64, 7>(&[0.0, positive_nan, -0.0, negative_nan], &mut state).unwrap();
        assert!(matches!(state.sum, Some(AggregateNumber::Float(v)) if v.is_nan()));
        assert!(
            matches!(state.min, Some(AggregateNumber::Float(v)) if v.to_bits() == negative_nan.to_bits())
        );
        assert!(
            matches!(state.max, Some(AggregateNumber::Float(v)) if v.to_bits() == positive_nan.to_bits())
        );
    }

    fn compare<T: NumericValue>(values: &[T], point: impl Fn(i64, T) -> Value) {
        let times = [i64::MIN, -20, -10, -1, 0, 1, 3, 5, 9, 11, i64::MAX];
        let ranges = [
            TimeRange::new(i64::MIN, -10),
            TimeRange::new(-1, 1),
            TimeRange::new(3, 3),
            TimeRange::new(7, i64::MAX),
        ];
        let windows = AggregateWindows::new(&ranges).unwrap();
        for mask in 0..64 {
            let projection = AggregateProjection {
                count: mask & 1 != 0,
                sum: mask & 2 != 0,
                min: mask & 4 != 0,
                max: mask & 8 != 0,
                first: mask & 16 != 0,
                last: mask & 32 != 0,
            };
            let mut expected =
                vec![ProjectedAggregateState::empty(T::TYPE, projection); ranges.len()];
            for state in &mut expected {
                state.add_value(point(6, values[1])).unwrap();
                if let Some(count) = &mut state.count {
                    *count = u64::MAX - 2;
                }
            }
            let mut actual = expected.clone();
            for (time, value) in times.iter().zip(values) {
                if let Some(window) = windows.for_time(*time) {
                    expected[window].add_value(point(*time, *value)).unwrap();
                }
            }
            reduce_windows(&times, values, windows, &mut actual).unwrap();
            for (expected, actual) in expected.iter().zip(&actual) {
                assert_eq!(signature(actual), signature(expected), "projection {mask}");
            }
        }
    }

    #[test]
    fn typed_windows_preserve_all_projections_and_numeric_semantics() {
        compare(
            &[i64::MIN, i64::MAX, -1, 0, 1, 2, 3, 4, 5, 6, 7],
            |time, value| Value::IntegerValue(IntegerValue::new(time, value)),
        );
        compare(&[u64::MAX, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], |time, value| {
            Value::UnsignedValue(UnsignedValue::new(time, value))
        });
        compare(
            &[
                -0.0,
                0.0,
                -1.0,
                1e20,
                1.0,
                -1e20,
                f64::NEG_INFINITY,
                f64::INFINITY,
                f64::from_bits(0x7ff8000000000001),
                f64::from_bits(0xfff8000000000002),
                2.0,
            ],
            |time, value| Value::FloatValue(FloatValue::new(time, value)),
        );
    }

    #[test]
    fn typed_windows_reject_mismatched_arrays_and_field_types() {
        let ranges = [TimeRange::new(0, 10)];
        let windows = AggregateWindows::new(&ranges).unwrap();
        let mut states = vec![ProjectedAggregateState::empty(
            INTEGER,
            AggregateProjection::default(),
        )];
        assert!(reduce_windows(&[0, 1], &[0_i64], windows, &mut states).is_err());
        assert!(reduce_windows(&[0], &[0.0_f64], windows, &mut states).is_err());
        assert!(reduce_windows(&[0], &[0_i64], windows, &mut []).is_err());
    }

    #[test]
    fn timestamp_counts_cover_sparse_windows_all_types_and_wrapping_states() {
        let times = [i64::MIN, -5, 0, 1, 3, 8, i64::MAX];
        let ranges = [
            TimeRange::new(i64::MIN, -5),
            TimeRange::new(1, 3),
            TimeRange::new(4, 7),
            TimeRange::new(8, i64::MAX),
        ];
        let windows = AggregateWindows::new(&ranges).unwrap();
        let projection = AggregateProjection {
            count: true,
            ..Default::default()
        };
        for typ in [
            FLOAT,
            INTEGER,
            UNSIGNED,
            crate::models::BOOL,
            crate::models::STRING,
        ] {
            let mut states = vec![ProjectedAggregateState::empty(typ, projection); ranges.len()];
            for state in &mut states {
                state.count = Some(u64::MAX);
            }
            count_windows(&times, typ, windows, &mut states).unwrap();
            assert_eq!(
                states.iter().map(|s| s.count.unwrap()).collect::<Vec<_>>(),
                [1, 1, u64::MAX, 1]
            );
            let expected = states.clone();
            count_windows(&[], typ, windows, &mut states).unwrap();
            assert_eq!(states, expected);
            assert!(count_windows(&times, typ, windows, &mut []).is_err());
            states[0].projection.sum = true;
            assert!(count_windows(&times, typ, windows, &mut states).is_err());
        }
    }
}
