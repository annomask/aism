use std::cmp::Ordering;

use crate::models::{BOOL, FLOAT, FieldType, INTEGER, STRING, UNSIGNED, points::Value};

use super::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum AggSum {
    Int(i128),
    Uint(u128),
    Float(f64),
    CountOnly,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum AggScalar {
    Int(i64),
    Uint(u64),
    Float(f64),
    Bool(bool),
    None,
}

impl AggScalar {
    fn from_value(value: &Value) -> Result<Self> {
        Ok(match value {
            Value::IntegerValue(v) => Self::Int(v.value),
            Value::UnsignedValue(v) => Self::Uint(v.value),
            Value::FloatValue(v) => Self::Float(v.value),
            Value::BoolValue(v) => Self::Bool(v.value),
            Value::StringValue(_) => Self::None,
            Value::EmptyValue(_) => return Err(Error::InvalidRecord),
        })
    }

    fn compare(self, other: Self) -> Result<Ordering> {
        Ok(match (self, other) {
            (Self::Int(a), Self::Int(b)) => a.cmp(&b),
            (Self::Uint(a), Self::Uint(b)) => a.cmp(&b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(&b),
            (Self::Bool(a), Self::Bool(b)) => a.cmp(&b),
            (Self::None, Self::None) => Ordering::Equal,
            _ => return Err(Error::TypeMismatch),
        })
    }

    fn as_f64(self) -> f64 {
        match self {
            Self::Int(v) => v as f64,
            Self::Uint(v) => v as f64,
            Self::Float(v) => v,
            Self::Bool(_) | Self::None => 0.0,
        }
    }
}

/// Nine mergeable components. Strings carry only count; endpoints require raw data.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct AggValues {
    pub(crate) count: u64,
    pub(crate) sum: AggSum,
    pub(crate) sumsq: f64,
    pub(crate) min: AggScalar,
    pub(crate) min_at: i64,
    pub(crate) max: AggScalar,
    pub(crate) max_at: i64,
    pub(crate) first: AggScalar,
    pub(crate) last: AggScalar,
}

impl AggValues {
    pub(crate) fn field_type(&self) -> FieldType {
        match self.first {
            AggScalar::Int(_) => INTEGER,
            AggScalar::Uint(_) => UNSIGNED,
            AggScalar::Float(_) => FLOAT,
            AggScalar::Bool(_) => BOOL,
            AggScalar::None => STRING,
        }
    }

    /// Requires the same sorted, deduplicated values supplied to the block encoder.
    pub(crate) fn from_block(values: &[Value]) -> Result<Self> {
        let first = values.first().ok_or(Error::InvalidRecord)?;
        let scalar = AggScalar::from_value(first)?;
        let mut result = Self {
            count: 0,
            sum: match scalar {
                AggScalar::Int(_) => AggSum::Int(0),
                AggScalar::Uint(_) | AggScalar::Bool(_) => AggSum::Uint(0),
                AggScalar::Float(_) => AggSum::Float(0.0),
                AggScalar::None => AggSum::CountOnly,
            },
            sumsq: 0.0,
            min: scalar,
            min_at: first.unix_nano(),
            max: scalar,
            max_at: first.unix_nano(),
            first: scalar,
            last: scalar,
        };
        let mut previous_time = None;
        for value in values {
            if value.value_type() != result.field_type() {
                return Err(Error::TypeMismatch);
            }
            let time = value.unix_nano();
            if previous_time.is_some_and(|previous| previous >= time) {
                return Err(Error::InvalidTimeOrder);
            }
            previous_time = Some(time);
            let scalar = AggScalar::from_value(value)?;
            result.count = result.count.checked_add(1).ok_or(Error::Overflow)?;
            result.sum = match (result.sum, scalar) {
                (AggSum::Int(sum), AggScalar::Int(v)) => {
                    AggSum::Int(sum.checked_add(i128::from(v)).ok_or(Error::Overflow)?)
                }
                (AggSum::Uint(sum), AggScalar::Uint(v)) => {
                    AggSum::Uint(sum.checked_add(u128::from(v)).ok_or(Error::Overflow)?)
                }
                (AggSum::Uint(sum), AggScalar::Bool(v)) => {
                    AggSum::Uint(sum.checked_add(u128::from(v)).ok_or(Error::Overflow)?)
                }
                (AggSum::Float(sum), AggScalar::Float(v)) => AggSum::Float(sum + v),
                (AggSum::CountOnly, AggScalar::None) => AggSum::CountOnly,
                _ => return Err(Error::TypeMismatch),
            };
            let v = scalar.as_f64();
            result.sumsq += v * v;
            if scalar.compare(result.min)?.is_lt() {
                result.min = scalar;
                result.min_at = time;
            }
            if scalar.compare(result.max)?.is_gt() {
                result.max = scalar;
                result.max_at = time;
            }
            result.last = scalar;
        }
        Ok(result)
    }

    /// Combines disjoint, time-ordered intervals. Record validates their ranges.
    pub(crate) fn merge_ordered(self, newer: Self) -> Result<Self> {
        if self.count == 0 || newer.count == 0 {
            return Err(Error::InvalidRecord);
        }
        if self.field_type() != newer.field_type() {
            return Err(Error::TypeMismatch);
        }
        let sum = match (self.sum, newer.sum) {
            (AggSum::Int(a), AggSum::Int(b)) => {
                AggSum::Int(a.checked_add(b).ok_or(Error::Overflow)?)
            }
            (AggSum::Uint(a), AggSum::Uint(b)) => {
                AggSum::Uint(a.checked_add(b).ok_or(Error::Overflow)?)
            }
            (AggSum::Float(a), AggSum::Float(b)) => AggSum::Float(a + b),
            (AggSum::CountOnly, AggSum::CountOnly) => AggSum::CountOnly,
            _ => return Err(Error::TypeMismatch),
        };
        let min_order = newer.min.compare(self.min)?;
        let max_order = newer.max.compare(self.max)?;
        let (min, min_at) =
            if min_order.is_lt() || (min_order.is_eq() && newer.min_at < self.min_at) {
                (newer.min, newer.min_at)
            } else {
                (self.min, self.min_at)
            };
        let (max, max_at) =
            if max_order.is_gt() || (max_order.is_eq() && newer.max_at < self.max_at) {
                (newer.max, newer.max_at)
            } else {
                (self.max, self.max_at)
            };
        Ok(Self {
            count: self.count.checked_add(newer.count).ok_or(Error::Overflow)?,
            sum,
            sumsq: self.sumsq + newer.sumsq,
            min,
            min_at,
            max,
            max_at,
            first: self.first,
            last: newer.last,
        })
    }
}
