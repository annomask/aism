//! Exact integer sums retained independently of the legacy wrapping SUM result.

use super::projected_aggregate::AggregateNumber;
use super::storage_ir::AggregateValues;
use crate::models::{FLOAT, FieldType, INTEGER, UNSIGNED};

/// Integer accumulator for AVG, independent of the wrapping 64-bit SUM result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateWideSum {
    Integer(i128),
    Unsigned(u128),
}

impl AggregateWideSum {
    pub(crate) const fn zero(typ: FieldType) -> Option<Self> {
        match typ {
            INTEGER => Some(Self::Integer(0)),
            UNSIGNED => Some(Self::Unsigned(0)),
            _ => None,
        }
    }

    pub(crate) fn checked_add(self, other: Self) -> Result<Self, &'static str> {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.checked_add(b).map(Self::Integer),
            (Self::Unsigned(a), Self::Unsigned(b)) => a.checked_add(b).map(Self::Unsigned),
            _ => return Err("cannot merge integer sums with different types"),
        }
        .ok_or("wide aggregate sum overflow")
    }

    pub(crate) fn merge(a: Option<Self>, b: Option<Self>) -> Result<Option<Self>, &'static str> {
        // None can also mean a legacy summary has lost its high bits. Never
        // manufacture an exact sum by adding only the known half of a merge.
        match (a, b) {
            (Some(a), Some(b)) => a.checked_add(b).map(Some),
            _ => Ok(None),
        }
    }

    pub(crate) fn as_f64(self) -> f64 {
        match self {
            Self::Integer(value) => value as f64,
            Self::Unsigned(value) => value as f64,
        }
    }

    /// A missing wide sum may represent a legacy summary with unknown high bits.
    /// A present one must agree with both the field type and wrapping SUM.
    pub(crate) fn validate(
        sum: Option<Self>,
        typ: FieldType,
        narrow: Option<AggregateNumber>,
    ) -> Result<(), &'static str> {
        let Some(sum) = sum else {
            return Ok(());
        };
        let matches = match (typ, sum, narrow) {
            (INTEGER, Self::Integer(wide), Some(AggregateNumber::Integer(value))) => {
                wide as i64 == value
            }
            (UNSIGNED, Self::Unsigned(wide), Some(AggregateNumber::Unsigned(value))) => {
                wide as u64 == value
            }
            (INTEGER, Self::Integer(0), None) | (UNSIGNED, Self::Unsigned(0), None) => true,
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            Err("wide aggregate sum is inconsistent with numeric sum")
        }
    }

    /// Recover a legacy 64-bit sum only when count and extrema prove a unique
    /// value congruent to it. Ambiguous summaries require raw-value reduction.
    pub(crate) fn from_legacy(values: AggregateValues, count: u64) -> Option<Self> {
        match values {
            AggregateValues::Integer { sum, min, max } if min <= max => {
                let modulus = 1i128 << 64;
                let lower = i128::from(min) * i128::from(count);
                let upper = i128::from(max) * i128::from(count);
                let mut candidate = lower
                    .div_euclid(modulus)
                    .checked_mul(modulus)?
                    .checked_add(i128::from(sum).rem_euclid(modulus))?;
                if candidate < lower {
                    candidate = candidate.checked_add(modulus)?;
                }
                (candidate <= upper
                    && candidate
                        .checked_add(modulus)
                        .is_none_or(|next| next > upper))
                .then_some(Self::Integer(candidate))
            }
            AggregateValues::Unsigned { sum, min, max } if min <= max => {
                let modulus = 1u128 << 64;
                let lower = u128::from(min) * u128::from(count);
                let upper = u128::from(max) * u128::from(count);
                let mut candidate = (lower / modulus * modulus).checked_add(u128::from(sum))?;
                if candidate < lower {
                    candidate = candidate.checked_add(modulus)?;
                }
                (candidate <= upper
                    && candidate
                        .checked_add(modulus)
                        .is_none_or(|next| next > upper))
                .then_some(Self::Unsigned(candidate))
            }
            _ => None,
        }
    }
}

/// Shared AVG contract for complete and projected results. No hot reduction
/// loop calls this: public result consumption is the validation boundary.
pub(crate) fn average(
    typ: FieldType,
    count: u64,
    sum: Option<AggregateNumber>,
    wide_sum: Option<AggregateWideSum>,
) -> Result<Option<f64>, &'static str> {
    if sum.is_some_and(|sum| sum.field_type() != typ) {
        return Err("numeric aggregate sum has a different field type");
    }
    AggregateWideSum::validate(wide_sum, typ, sum)?;
    if count == 0 {
        return Ok(None);
    }
    let sum = match (typ, wide_sum, sum) {
        (
            INTEGER,
            Some(sum @ AggregateWideSum::Integer(_)),
            Some(AggregateNumber::Integer(_)),
        )
        | (
            UNSIGNED,
            Some(sum @ AggregateWideSum::Unsigned(_)),
            Some(AggregateNumber::Unsigned(_)),
        ) => sum.as_f64(),
        (FLOAT, None, Some(AggregateNumber::Float(sum))) => sum,
        _ => return Err("average requires an exact numeric sum"),
    };
    Ok(Some(sum / count as f64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::storage_ir::{
        AggregateKind, AggregateProjection, AggregateState, ProjectedAggregateState,
    };

    #[test]
    fn integer_average_preserves_wide_sum_through_merges_and_projection() {
        for typ in [INTEGER, UNSIGNED] {
            let mut state = AggregateState::empty(typ);
            assert_eq!(state.average().unwrap(), None);
            for _ in 0..2 {
                let mut partial = AggregateState::empty(typ);
                if typ == INTEGER {
                    partial.add_integer(i64::MAX);
                } else {
                    partial.add_unsigned(u64::MAX);
                }
                state.merge(partial).unwrap();
            }
            let (wide, average) = if typ == INTEGER {
                assert!(matches!(
                    state.values,
                    Some(AggregateValues::Integer { sum: -2, .. })
                ));
                (
                    AggregateWideSum::Integer(i128::from(i64::MAX) * 2),
                    i64::MAX as f64,
                )
            } else {
                assert!(
                    matches!(state.values, Some(AggregateValues::Unsigned { sum, .. }) if sum == u64::MAX - 1)
                );
                (
                    AggregateWideSum::Unsigned(u128::from(u64::MAX) * 2),
                    u64::MAX as f64,
                )
            };
            assert_eq!(state.wide_sum, Some(wide));
            assert_eq!(state.average().unwrap(), Some(average));
            let complete = ProjectedAggregateState::from_complete(
                state.clone(),
                AggregateProjection::complete(typ, false),
            );
            assert_eq!(complete.clone().into_complete().unwrap(), state);
            let avg = AggregateProjection::from_aggregates(&[AggregateKind::Avg]);
            let mut projected = ProjectedAggregateState::empty(typ, avg);
            assert_eq!(projected.average().unwrap(), None);
            projected
                .merge(ProjectedAggregateState::from_complete(state.clone(), avg))
                .unwrap();
            projected
                .merge(ProjectedAggregateState::from_complete(state.clone(), avg))
                .unwrap();
            assert_eq!(projected.average().unwrap(), Some(average));
            let mut unknown = projected.clone();
            unknown.wide_sum = None;
            projected.merge(unknown).unwrap();
            assert!(projected.average().is_err());
            assert!(
                ProjectedAggregateState::from_complete(
                    state,
                    AggregateProjection::from_aggregates(&[AggregateKind::Sum])
                )
                .average()
                .is_err()
            );
        }
        let mut signed = AggregateState::empty(INTEGER);
        signed.add_integer(i64::MIN);
        signed.add_integer(i64::MIN);
        assert_eq!(signed.average().unwrap(), Some(i64::MIN as f64));
        signed.add_integer(i64::MAX);
        signed.add_integer(i64::MAX);
        assert_eq!(signed.average().unwrap(), Some(-0.5));
    }

    #[test]
    fn integer_average_rejects_count_only_states_without_a_numeric_sum() {
        let mut state = AggregateState::empty(INTEGER);
        state.add_count(2);
        assert!(state.average().is_err());
        let projection = AggregateProjection::from_aggregates(&[AggregateKind::Avg]);
        assert!(
            ProjectedAggregateState::from_complete(state, projection)
                .average()
                .is_err()
        );
    }

    #[test]
    fn legacy_integer_sums_require_unique_wide_reconstruction() {
        for count in [1, 2, 1000, u64::MAX] {
            for value in [i64::MIN, -7, 0, 7, i64::MAX] {
                let exact = i128::from(value) * i128::from(count);
                assert_eq!(
                    AggregateWideSum::from_legacy(
                        AggregateValues::Integer {
                            sum: exact as i64,
                            min: value,
                            max: value,
                        },
                        count
                    ),
                    Some(AggregateWideSum::Integer(exact))
                );
            }
            for value in [0, 7, u64::MAX] {
                let exact = u128::from(value) * u128::from(count);
                assert_eq!(
                    AggregateWideSum::from_legacy(
                        AggregateValues::Unsigned {
                            sum: exact as u64,
                            min: value,
                            max: value,
                        },
                        count
                    ),
                    Some(AggregateWideSum::Unsigned(exact))
                );
            }
        }
        assert_eq!(
            AggregateWideSum::from_legacy(
                AggregateValues::Integer {
                    sum: -1,
                    min: i64::MIN,
                    max: i64::MAX,
                },
                3
            ),
            None
        );
        assert_eq!(
            AggregateWideSum::from_legacy(
                AggregateValues::Unsigned {
                    sum: u64::MAX,
                    min: 0,
                    max: u64::MAX,
                },
                3
            ),
            None
        );
        assert_eq!(
            AggregateWideSum::from_legacy(
                AggregateValues::Integer {
                    sum: 5,
                    min: 1,
                    max: 2,
                },
                1
            ),
            None
        );
    }
}
