//! Typed statistics for one logical TSM block.

use crate::models::points::{BOOL, FLOAT, INTEGER, STRING, UNSIGNED, Value};

/// Serialized bytes appended to a v2 index entry.
///
/// Layout: kind (1), count (8), sum (8), min (8), max (8), first (8), last (8).
pub const BLOCK_STATISTICS_SIZE: usize = 49;

const STATS_NONE: u8 = 0;
const STATS_COUNT: u8 = 1;
const STATS_INTEGER: u8 = 2;
const STATS_UNSIGNED: u8 = 3;
const STATS_FLOAT: u8 = 4;
const STATS_BOOLEAN: u8 = 5;

/// The value-dependent part of block statistics.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BlockStatisticValue {
    Count,
    Integer {
        sum: i64,
        min: i64,
        max: i64,
        first: i64,
        last: i64,
    },
    Unsigned {
        sum: u64,
        min: u64,
        max: u64,
        first: u64,
        last: u64,
    },
    Float {
        sum: f64,
        min: f64,
        max: f64,
        first: f64,
        last: f64,
    },
    Boolean {
        first: bool,
        last: bool,
    },
}

/// Statistics describing exactly the logical values encoded in one TSM block.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BlockStatistics {
    pub count: u64,
    pub value: BlockStatisticValue,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DatafusionFloatMinMaxSafety {
    Safe,
    UnsupportedStatistics,
    Nan,
    SignedZero,
    ZeroCrossing,
}

impl BlockStatistics {
    pub fn from_values(values: &[Value]) -> Option<Self> {
        let first = values.first()?;
        match first {
            Value::IntegerValue(_) => Self::from_integer(
                values
                    .iter()
                    .map(|value| value.integer_value().expect("homogeneous integer block")),
            ),
            Value::UnsignedValue(_) => Self::from_unsigned(
                values
                    .iter()
                    .map(|value| value.unsigned_value().expect("homogeneous unsigned block")),
            ),
            Value::FloatValue(_) => Self::from_float(
                values
                    .iter()
                    .map(|value| value.float_value().expect("homogeneous float block")),
            ),
            Value::BoolValue(_) => Self::from_bool(
                values
                    .iter()
                    .map(|value| value.bool_value().expect("homogeneous boolean block")),
            ),
            Value::StringValue(_) => Some(Self {
                count: values.len() as u64,
                value: BlockStatisticValue::Count,
            }),
            Value::EmptyValue(_) => None,
        }
    }

    pub fn from_integer(values: impl IntoIterator<Item = i64>) -> Option<Self> {
        let mut values = values.into_iter();
        let first = values.next()?;
        let mut count = 1_u64;
        let mut sum = first;
        let mut min = first;
        let mut max = first;
        let mut last = first;
        for value in values {
            count += 1;
            sum = sum.wrapping_add(value);
            min = min.min(value);
            max = max.max(value);
            last = value;
        }
        Some(Self {
            count,
            value: BlockStatisticValue::Integer {
                sum,
                min,
                max,
                first,
                last,
            },
        })
    }

    pub fn from_unsigned(values: impl IntoIterator<Item = u64>) -> Option<Self> {
        let mut values = values.into_iter();
        let first = values.next()?;
        let mut count = 1_u64;
        let mut sum = first;
        let mut min = first;
        let mut max = first;
        let mut last = first;
        for value in values {
            count += 1;
            sum = sum.wrapping_add(value);
            min = min.min(value);
            max = max.max(value);
            last = value;
        }
        Some(Self {
            count,
            value: BlockStatisticValue::Unsigned {
                sum,
                min,
                max,
                first,
                last,
            },
        })
    }

    pub fn from_float(values: impl IntoIterator<Item = f64>) -> Option<Self> {
        let mut values = values.into_iter();
        let first = values.next()?;
        let mut count = 1_u64;
        // Arrow's floating SUM accumulator starts at +0.0. Preserve that
        // observable behavior for a block containing only -0.0.
        let mut sum = 0.0 + first;
        let mut min = first;
        let mut max = first;
        let mut last = first;
        for value in values {
            count += 1;
            sum += value;
            if value.total_cmp(&min).is_lt() {
                min = value;
            }
            if value.total_cmp(&max).is_gt() {
                max = value;
            }
            last = value;
        }
        Some(Self {
            count,
            value: BlockStatisticValue::Float {
                sum,
                min,
                max,
                first,
                last,
            },
        })
    }

    pub fn from_bool(values: impl IntoIterator<Item = bool>) -> Option<Self> {
        let mut values = values.into_iter();
        let first = values.next()?;
        let mut count = 1_u64;
        let mut last = first;
        for value in values {
            count += 1;
            last = value;
        }
        Some(Self {
            count,
            value: BlockStatisticValue::Boolean { first, last },
        })
    }

    pub fn count_only(count: usize) -> Option<Self> {
        (count != 0).then_some(Self {
            count: count as u64,
            value: BlockStatisticValue::Count,
        })
    }

    pub fn supports_block_type(&self, block_type: u8) -> bool {
        matches!(
            (block_type, self.value),
            (INTEGER, BlockStatisticValue::Integer { .. })
                | (UNSIGNED, BlockStatisticValue::Unsigned { .. })
                | (FLOAT, BlockStatisticValue::Float { .. })
                | (BOOL, BlockStatisticValue::Boolean { .. })
                | (STRING, BlockStatisticValue::Count)
        )
    }

    pub fn has_first_last(&self) -> bool {
        !matches!(self.value, BlockStatisticValue::Count)
    }

    /// Whether this Float64 block is in a domain where DataFusion's grouped
    /// `partial_cmp` MIN/MAX and ReefsDB's total-order extrema are bitwise
    /// equivalent regardless of update or partial-state merge order.
    ///
    /// NaNs make DataFusion order dependent. Signed zero is also order
    /// dependent, and existing statistics cannot prove that a zero is absent
    /// when the total-order range touches or crosses zero. Requiring the whole
    /// block to be strictly on one side of zero excludes both cases while
    /// retaining infinities. A query subset of a safe block is also safe.
    pub(crate) fn is_datafusion_grouped_float_min_max_safe(&self) -> bool {
        self.datafusion_grouped_float_min_max_safety() == DatafusionFloatMinMaxSafety::Safe
    }

    pub(crate) fn datafusion_grouped_float_min_max_safety(&self) -> DatafusionFloatMinMaxSafety {
        let BlockStatisticValue::Float { min, max, .. } = self.value else {
            return DatafusionFloatMinMaxSafety::UnsupportedStatistics;
        };
        if min.is_nan() || max.is_nan() {
            DatafusionFloatMinMaxSafety::Nan
        } else if min == 0.0 || max == 0.0 {
            DatafusionFloatMinMaxSafety::SignedZero
        } else if min.total_cmp(&0.0).is_gt() || max.total_cmp(&-0.0).is_lt() {
            DatafusionFloatMinMaxSafety::Safe
        } else {
            DatafusionFloatMinMaxSafety::ZeroCrossing
        }
    }

    pub(crate) fn has_float_infinite_extreme(&self) -> bool {
        matches!(
            self.value,
            BlockStatisticValue::Float { min, max, .. } if min.is_infinite() || max.is_infinite()
        )
    }

    pub(crate) fn encode_into(statistics: Option<Self>, dst: &mut [u8]) {
        debug_assert!(dst.len() >= BLOCK_STATISTICS_SIZE);
        dst[..BLOCK_STATISTICS_SIZE].fill(0);
        let Some(statistics) = statistics else {
            dst[0] = STATS_NONE;
            return;
        };
        dst[0] = match statistics.value {
            BlockStatisticValue::Count => STATS_COUNT,
            BlockStatisticValue::Integer { .. } => STATS_INTEGER,
            BlockStatisticValue::Unsigned { .. } => STATS_UNSIGNED,
            BlockStatisticValue::Float { .. } => STATS_FLOAT,
            BlockStatisticValue::Boolean { .. } => STATS_BOOLEAN,
        };
        dst[1..9].copy_from_slice(&statistics.count.to_be_bytes());
        let (sum, min, max, first, last) = match statistics.value {
            BlockStatisticValue::Count => (0, 0, 0, 0, 0),
            BlockStatisticValue::Integer {
                sum,
                min,
                max,
                first,
                last,
            } => (
                sum as u64,
                min as u64,
                max as u64,
                first as u64,
                last as u64,
            ),
            BlockStatisticValue::Unsigned {
                sum,
                min,
                max,
                first,
                last,
            } => (sum, min, max, first, last),
            BlockStatisticValue::Float {
                sum,
                min,
                max,
                first,
                last,
            } => (
                sum.to_bits(),
                min.to_bits(),
                max.to_bits(),
                first.to_bits(),
                last.to_bits(),
            ),
            BlockStatisticValue::Boolean { first, last } => {
                (0, 0, 0, u64::from(first), u64::from(last))
            }
        };
        dst[9..17].copy_from_slice(&sum.to_be_bytes());
        dst[17..25].copy_from_slice(&min.to_be_bytes());
        dst[25..33].copy_from_slice(&max.to_be_bytes());
        dst[33..41].copy_from_slice(&first.to_be_bytes());
        dst[41..49].copy_from_slice(&last.to_be_bytes());
    }

    pub(crate) fn decode(block_type: u8, src: &[u8]) -> Result<Option<Self>, String> {
        if src.len() < BLOCK_STATISTICS_SIZE {
            return Err(format!(
                "short block statistics: {} < {}",
                src.len(),
                BLOCK_STATISTICS_SIZE
            ));
        }
        if src[0] == STATS_NONE {
            return Ok(None);
        }
        let count = u64::from_be_bytes(src[1..9].try_into().unwrap());
        if count == 0 {
            return Err("block statistics count must be non-zero".to_string());
        }
        let sum = u64::from_be_bytes(src[9..17].try_into().unwrap());
        let min = u64::from_be_bytes(src[17..25].try_into().unwrap());
        let max = u64::from_be_bytes(src[25..33].try_into().unwrap());
        let first = u64::from_be_bytes(src[33..41].try_into().unwrap());
        let last = u64::from_be_bytes(src[41..49].try_into().unwrap());
        let value = match src[0] {
            STATS_COUNT if block_type == STRING => BlockStatisticValue::Count,
            STATS_INTEGER if block_type == INTEGER => BlockStatisticValue::Integer {
                sum: sum as i64,
                min: min as i64,
                max: max as i64,
                first: first as i64,
                last: last as i64,
            },
            STATS_UNSIGNED if block_type == UNSIGNED => BlockStatisticValue::Unsigned {
                sum,
                min,
                max,
                first,
                last,
            },
            STATS_FLOAT if block_type == FLOAT => BlockStatisticValue::Float {
                sum: f64::from_bits(sum),
                min: f64::from_bits(min),
                max: f64::from_bits(max),
                first: f64::from_bits(first),
                last: f64::from_bits(last),
            },
            STATS_BOOLEAN if block_type == BOOL && first <= 1 && last <= 1 => {
                BlockStatisticValue::Boolean {
                    first: first != 0,
                    last: last != 0,
                }
            }
            kind => {
                return Err(format!(
                    "block statistics kind {kind} does not match block type {block_type}"
                ));
            }
        };
        let ordered = match value {
            BlockStatisticValue::Count => true,
            BlockStatisticValue::Integer { min, max, .. } => min <= max,
            BlockStatisticValue::Unsigned { min, max, .. } => min <= max,
            BlockStatisticValue::Float { min, max, .. } => !min.total_cmp(&max).is_gt(),
            BlockStatisticValue::Boolean { .. } => true,
        };
        if !ordered {
            return Err("block statistics minimum exceeds maximum".to_string());
        }
        let endpoints_in_range = match value {
            BlockStatisticValue::Count | BlockStatisticValue::Boolean { .. } => true,
            BlockStatisticValue::Integer {
                min,
                max,
                first,
                last,
                ..
            } => (min..=max).contains(&first) && (min..=max).contains(&last),
            BlockStatisticValue::Unsigned {
                min,
                max,
                first,
                last,
                ..
            } => (min..=max).contains(&first) && (min..=max).contains(&last),
            BlockStatisticValue::Float {
                min,
                max,
                first,
                last,
                ..
            } => {
                !first.total_cmp(&min).is_lt()
                    && !first.total_cmp(&max).is_gt()
                    && !last.total_cmp(&min).is_lt()
                    && !last.total_cmp(&max).is_gt()
            }
        };
        if !endpoints_in_range {
            return Err("block statistics endpoint is outside minimum/maximum".to_string());
        }
        Ok(Some(Self { count, value }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn float_statistics_bits(values: &[f64]) -> [u64; 6] {
        let statistics = BlockStatistics::from_float(values.iter().copied()).unwrap();
        let BlockStatisticValue::Float {
            sum,
            min,
            max,
            first,
            last,
        } = statistics.value
        else {
            panic!("expected float statistics")
        };
        [
            statistics.count,
            sum.to_bits(),
            min.to_bits(),
            max.to_bits(),
            first.to_bits(),
            last.to_bits(),
        ]
    }

    fn datafusion_grouped_min(values: &[f64]) -> f64 {
        values.iter().copied().fold(f64::MAX, |current, new| {
            if matches!(
                new.partial_cmp(&current),
                Some(std::cmp::Ordering::Less) | None
            ) {
                new
            } else {
                current
            }
        })
    }

    fn datafusion_grouped_max(values: &[f64]) -> f64 {
        values.iter().copied().fold(f64::MIN, |current, new| {
            if matches!(
                new.partial_cmp(&current),
                Some(std::cmp::Ordering::Greater) | None
            ) {
                new
            } else {
                current
            }
        })
    }

    #[test]
    fn statistics_round_trip_preserves_float_bits_and_wrapping_integer_sum() {
        let statistics = BlockStatistics::from_integer([i64::MAX, 1]).unwrap();
        assert_eq!(
            statistics.value,
            BlockStatisticValue::Integer {
                sum: i64::MIN,
                min: 1,
                max: i64::MAX,
                first: i64::MAX,
                last: 1,
            }
        );
        let mut encoded = [0; BLOCK_STATISTICS_SIZE];
        BlockStatistics::encode_into(Some(statistics), &mut encoded);
        assert_eq!(
            BlockStatistics::decode(INTEGER, &encoded).unwrap(),
            Some(statistics)
        );

        let statistics = BlockStatistics::from_float([-0.0, 0.0, f64::NAN]).unwrap();
        BlockStatistics::encode_into(Some(statistics), &mut encoded);
        let decoded = BlockStatistics::decode(FLOAT, &encoded).unwrap().unwrap();
        let BlockStatisticValue::Float { min, max, .. } = decoded.value else {
            panic!("expected float statistics")
        };
        assert_eq!(min.to_bits(), (-0.0_f64).to_bits());
        assert!(max.is_nan());
    }

    #[test]
    fn persisted_float_statistics_cannot_reconstruct_datafusion_grouped_min_max_bits() {
        let nan = f64::from_bits(0x7ff8_0000_0000_0042);

        let max_left = [0.0, nan, 1.0, 2.0, 0.0];
        let max_right = [0.0, 1.0, 2.0, nan, 0.0];
        assert_eq!(
            float_statistics_bits(&max_left),
            float_statistics_bits(&max_right)
        );
        assert_eq!(
            datafusion_grouped_max(&max_left).to_bits(),
            2.0_f64.to_bits()
        );
        assert_eq!(
            datafusion_grouped_max(&max_right).to_bits(),
            0.0_f64.to_bits()
        );

        let min_left = [10.0, nan, 1.0, 2.0, 10.0];
        let min_right = [10.0, 1.0, 2.0, nan, 10.0];
        assert_eq!(
            float_statistics_bits(&min_left),
            float_statistics_bits(&min_right)
        );
        assert_eq!(
            datafusion_grouped_min(&min_left).to_bits(),
            1.0_f64.to_bits()
        );
        assert_eq!(
            datafusion_grouped_min(&min_right).to_bits(),
            10.0_f64.to_bits()
        );

        let zero_min_left = [1.0, 0.0, -0.0, 1.0];
        let zero_min_right = [1.0, -0.0, 0.0, 1.0];
        assert_eq!(
            float_statistics_bits(&zero_min_left),
            float_statistics_bits(&zero_min_right)
        );
        assert_eq!(
            datafusion_grouped_min(&zero_min_left).to_bits(),
            0.0_f64.to_bits()
        );
        assert_eq!(
            datafusion_grouped_min(&zero_min_right).to_bits(),
            (-0.0_f64).to_bits()
        );

        let zero_max_left = [-1.0, 0.0, -0.0, -1.0];
        let zero_max_right = [-1.0, -0.0, 0.0, -1.0];
        assert_eq!(
            float_statistics_bits(&zero_max_left),
            float_statistics_bits(&zero_max_right)
        );
        assert_eq!(
            datafusion_grouped_max(&zero_max_left).to_bits(),
            0.0_f64.to_bits()
        );
        assert_eq!(
            datafusion_grouped_max(&zero_max_right).to_bits(),
            (-0.0_f64).to_bits()
        );
    }

    #[test]
    fn datafusion_grouped_float_min_max_safe_domain_is_conservative() {
        for values in [
            vec![1.0, 2.0, f64::INFINITY],
            vec![-1.0, -2.0, f64::NEG_INFINITY],
            vec![f64::INFINITY],
            vec![f64::NEG_INFINITY],
        ] {
            let statistics = BlockStatistics::from_float(values.iter().copied()).unwrap();
            assert!(statistics.is_datafusion_grouped_float_min_max_safe());
            let BlockStatisticValue::Float { min, max, .. } = statistics.value else {
                panic!("expected float statistics")
            };
            let expected_min = if min == f64::INFINITY { f64::MAX } else { min };
            let expected_max = if max == f64::NEG_INFINITY {
                f64::MIN
            } else {
                max
            };
            assert_eq!(
                datafusion_grouped_min(&values).to_bits(),
                expected_min.to_bits()
            );
            assert_eq!(
                datafusion_grouped_max(&values).to_bits(),
                expected_max.to_bits()
            );
        }

        for values in [
            vec![f64::NAN],
            vec![1.0, f64::NAN],
            vec![0.0],
            vec![-0.0],
            vec![0.0, -0.0],
            vec![-1.0, 1.0],
        ] {
            assert!(
                !BlockStatistics::from_float(values)
                    .unwrap()
                    .is_datafusion_grouped_float_min_max_safe()
            );
        }

        assert_eq!(
            BlockStatistics::from_float([1.0, f64::INFINITY])
                .unwrap()
                .datafusion_grouped_float_min_max_safety(),
            DatafusionFloatMinMaxSafety::Safe
        );
        assert!(
            BlockStatistics::from_float([1.0, f64::INFINITY])
                .unwrap()
                .has_float_infinite_extreme()
        );
        assert_eq!(
            BlockStatistics::from_float([f64::NAN])
                .unwrap()
                .datafusion_grouped_float_min_max_safety(),
            DatafusionFloatMinMaxSafety::Nan
        );
        assert_eq!(
            BlockStatistics::from_float([-0.0, 1.0])
                .unwrap()
                .datafusion_grouped_float_min_max_safety(),
            DatafusionFloatMinMaxSafety::SignedZero
        );
        assert_eq!(
            BlockStatistics::from_float([-1.0, 1.0])
                .unwrap()
                .datafusion_grouped_float_min_max_safety(),
            DatafusionFloatMinMaxSafety::ZeroCrossing
        );
        assert_eq!(
            BlockStatistics::from_integer([1, 2])
                .unwrap()
                .datafusion_grouped_float_min_max_safety(),
            DatafusionFloatMinMaxSafety::UnsupportedStatistics
        );
    }

    #[test]
    fn float_sum_matches_arrow_zero_and_special_value_semantics() {
        let statistics = BlockStatistics::from_float([-0.0]).unwrap();
        let BlockStatisticValue::Float { sum, min, max, .. } = statistics.value else {
            panic!("expected float statistics")
        };
        assert_eq!(sum.to_bits(), 0.0_f64.to_bits());
        assert_eq!(min.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(max.to_bits(), (-0.0_f64).to_bits());

        for (values, expected) in [
            (vec![1.5, -2.0, 4.0], 3.5),
            (vec![f64::INFINITY, 1.0], f64::INFINITY),
            (vec![f64::NEG_INFINITY, -1.0], f64::NEG_INFINITY),
        ] {
            let statistics = BlockStatistics::from_float(values).unwrap();
            let BlockStatisticValue::Float { sum, .. } = statistics.value else {
                panic!("expected float statistics")
            };
            assert_eq!(sum.to_bits(), expected.to_bits());
        }

        for values in [vec![f64::NAN, 1.0], vec![f64::INFINITY, f64::NEG_INFINITY]] {
            let statistics = BlockStatistics::from_float(values).unwrap();
            let BlockStatisticValue::Float { sum, .. } = statistics.value else {
                panic!("expected float statistics")
            };
            assert!(sum.is_nan());
        }
    }

    #[test]
    fn first_last_values_round_trip_without_separate_timestamps() {
        for (block_type, statistics) in [
            (INTEGER, BlockStatistics::from_integer([-7, 4, 2]).unwrap()),
            (
                UNSIGNED,
                BlockStatistics::from_unsigned([u64::MAX, 4, 2]).unwrap(),
            ),
            (
                FLOAT,
                BlockStatistics::from_float([-0.0, f64::INFINITY, 0.0]).unwrap(),
            ),
            (
                BOOL,
                BlockStatistics::from_bool([true, true, false]).unwrap(),
            ),
        ] {
            let mut encoded = [0; BLOCK_STATISTICS_SIZE];
            BlockStatistics::encode_into(Some(statistics), &mut encoded);
            let decoded = BlockStatistics::decode(block_type, &encoded)
                .unwrap()
                .unwrap();
            assert_eq!(decoded, statistics);
            assert!(decoded.has_first_last());
            if block_type == FLOAT {
                let BlockStatisticValue::Float { first, last, .. } = decoded.value else {
                    unreachable!()
                };
                assert_eq!(first.to_bits(), (-0.0_f64).to_bits());
                assert_eq!(last.to_bits(), 0.0_f64.to_bits());
            }
        }

        let string = BlockStatistics::count_only(3).unwrap();
        assert!(!string.has_first_last());

        let statistics = BlockStatistics::from_integer([-7, 4, 2]).unwrap();
        let mut encoded = [0; BLOCK_STATISTICS_SIZE];
        BlockStatistics::encode_into(Some(statistics), &mut encoded);
        encoded[33..41].copy_from_slice(&i64::MAX.to_be_bytes());
        assert_eq!(
            BlockStatistics::decode(INTEGER, &encoded).unwrap_err(),
            "block statistics endpoint is outside minimum/maximum"
        );
    }
}
