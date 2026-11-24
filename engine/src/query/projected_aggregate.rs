//! Optional aggregate components for request-driven storage projection.
use super::numeric_aggregate::NumericValue;
use super::storage_ir::{
    AggregateKind, AggregatePoint, AggregatePointValue, AggregateResult, AggregateScanMetrics,
    AggregateState, AggregateValues, AggregateWideSum,
};
use crate::models::points::Value;
use crate::models::{BOOL, FLOAT, FieldType, INTEGER, STRING, UNSIGNED};

/// Components required to merge a request. AVG requires both count and sum.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AggregateProjection {
    pub count: bool,
    pub sum: bool,
    pub min: bool,
    pub max: bool,
    pub first: bool,
    pub last: bool,
}
impl AggregateProjection {
    pub(crate) fn count_only(self) -> bool {
        self == Self {
            count: true,
            ..Self::default()
        }
    }

    pub(crate) fn has_endpoints(self) -> bool {
        self.first || self.last
    }

    pub(crate) fn extrema_only(self) -> bool {
        (self.min || self.max)
            && self
                == Self {
                    min: self.min,
                    max: self.max,
                    ..Self::default()
                }
    }

    /// Logical type support belongs to the query contract, not a file codec.
    pub(crate) fn validate_field_type(self, typ: FieldType) -> Result<(), &'static str> {
        match typ {
            INTEGER | UNSIGNED | FLOAT => Ok(()),
            BOOL | STRING if !(self.sum || self.min || self.max) => Ok(()),
            BOOL | STRING => Err(
                "only COUNT, FIRST, and LAST are supported for boolean and string aggregate fields",
            ),
            _ => Err("unsupported aggregate field type"),
        }
    }

    pub fn from_aggregates(aggregates: &[AggregateKind]) -> Self {
        let mut result = Self::default();
        for kind in aggregates {
            match kind {
                AggregateKind::Count => result.count = true,
                AggregateKind::Sum => result.sum = true,
                AggregateKind::Min => result.min = true,
                AggregateKind::Max => result.max = true,
                AggregateKind::Avg => {
                    result.count = true;
                    result.sum = true;
                }
                AggregateKind::First => result.first = true,
                AggregateKind::Last => result.last = true,
            }
        }
        result
    }
    pub(crate) fn complete(typ: FieldType, endpoints: bool) -> Self {
        let numeric = matches!(typ, INTEGER | UNSIGNED | FLOAT);
        Self {
            count: true,
            sum: numeric,
            min: numeric,
            max: numeric,
            first: endpoints,
            last: endpoints,
        }
    }
}

/// Numeric component of a projected aggregate, retaining the field's type.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggregateNumber {
    Integer(i64),
    Unsigned(u64),
    Float(f64),
}

/// Only numeric reductions can be passed to the numeric merge implementation.
#[derive(Clone, Copy)]
enum NumericReduction {
    Sum,
    Min,
    Max,
}

impl AggregateNumber {
    pub(crate) fn field_type(self) -> FieldType {
        match self {
            Self::Integer(_) => INTEGER,
            Self::Unsigned(_) => UNSIGNED,
            Self::Float(_) => FLOAT,
        }
    }

    fn wide(self) -> Option<AggregateWideSum> {
        match self {
            Self::Integer(v) => v.wide(),
            Self::Unsigned(v) => v.wide(),
            Self::Float(v) => v.wide(),
        }
    }

    fn singleton_sum(self) -> Self {
        match self {
            Self::Integer(v) => v.singleton_sum().number(),
            Self::Unsigned(v) => v.singleton_sum().number(),
            Self::Float(v) => v.singleton_sum().number(),
        }
    }

    fn compare(self, other: Self) -> Result<std::cmp::Ordering, &'static str> {
        Ok(match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.compare(b),
            (Self::Unsigned(a), Self::Unsigned(b)) => a.compare(b),
            (Self::Float(a), Self::Float(b)) => a.compare(b),
            _ => return Err("cannot compare aggregate numbers with different types"),
        })
    }

    fn combine(self, other: Self, kind: NumericReduction) -> Result<Self, &'static str> {
        use NumericReduction::*;
        Ok(match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => Self::Integer(match kind {
                Sum => NumericValue::add(a, b),
                Min => NumericValue::min(a, b),
                Max => NumericValue::max(a, b),
            }),
            (Self::Unsigned(a), Self::Unsigned(b)) => Self::Unsigned(match kind {
                Sum => NumericValue::add(a, b),
                Min => NumericValue::min(a, b),
                Max => NumericValue::max(a, b),
            }),
            (Self::Float(a), Self::Float(b)) => Self::Float(match kind {
                Sum => NumericValue::add(a, b),
                Min => NumericValue::min(a, b),
                Max => NumericValue::max(a, b),
            }),
            _ => return Err("cannot merge projected numbers with different types"),
        })
    }
}

/// Mergeable requested components, including AVG dependencies. Unrequested
/// components are `None`. For empty input, requested count is `Some(0)` and
/// other result components are `None`; `projection` distinguishes these cases.
/// Requested integer sums retain a zero `wide_sum` accumulator even when empty.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedAggregateState {
    pub field_type: FieldType,
    pub projection: AggregateProjection,
    pub count: Option<u64>,
    pub sum: Option<AggregateNumber>,
    /// Exact integer sum for AVG, separate from the compatible wrapping SUM.
    pub wide_sum: Option<AggregateWideSum>,
    pub min: Option<AggregateNumber>,
    pub max: Option<AggregateNumber>,
    pub first: Option<AggregatePoint>,
    pub last: Option<AggregatePoint>,
}
fn merge_number(
    a: Option<AggregateNumber>,
    b: Option<AggregateNumber>,
    kind: NumericReduction,
) -> Result<Option<AggregateNumber>, &'static str> {
    Ok(match (a, b) {
        (Some(a), Some(b)) => Some(a.combine(b, kind)?),
        (a, b) => a.or(b),
    })
}
impl ProjectedAggregateState {
    /// Check externally constructible state at result/conversion boundaries.
    /// Reduction loops operate on trusted states and do not repeat these checks.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        self.projection.validate_field_type(self.field_type)?;
        if self.count.is_some() != self.projection.count {
            return Err("projected count does not match projection");
        }
        // A wrapped count of zero does not prove emptiness. Conversely, any
        // populated result component proves all requested components are due.
        let has_values = self.count.is_some_and(|count| count != 0)
            || self.sum.is_some()
            || self.min.is_some()
            || self.max.is_some()
            || self.first.is_some()
            || self.last.is_some();
        for (requested, number) in [
            (self.projection.sum, self.sum),
            (self.projection.min, self.min),
            (self.projection.max, self.max),
        ] {
            if has_values && requested && number.is_none() {
                return Err("requested numeric aggregate component is missing");
            }
            if let Some(number) = number {
                if !requested {
                    return Err("unrequested numeric aggregate component is populated");
                }
                if number.field_type() != self.field_type {
                    return Err("numeric aggregate component has a different field type");
                }
            }
        }
        if !self.projection.sum && self.wide_sum.is_some() {
            return Err("unrequested wide aggregate sum is populated");
        }
        AggregateWideSum::validate(self.wide_sum, self.field_type, self.sum)?;
        for (requested, point) in [
            (self.projection.first, &self.first),
            (self.projection.last, &self.last),
        ] {
            if has_values && requested && point.is_none() {
                return Err("requested aggregate endpoint is missing");
            }
            if let Some(point) = point {
                if !requested {
                    return Err("unrequested aggregate endpoint is populated");
                }
                if point.value.field_type() != self.field_type {
                    return Err("aggregate endpoint has a different field type");
                }
            }
        }
        if let (Some(first), Some(last)) = (&self.first, &self.last) {
            if first.time > last.time {
                return Err("aggregate endpoints are not time ordered");
            }
        }
        Ok(())
    }

    /// A verified visible point is sufficient evidence for MIN/MAX candidates.
    pub(crate) fn add_extrema_witness(
        &mut self,
        number: AggregateNumber,
    ) -> Result<(), &'static str> {
        if !self.projection.extrema_only() {
            return Err("extrema witnesses require a MIN/MAX-only projection");
        }
        if !matches!(
            (self.field_type, number),
            (INTEGER, AggregateNumber::Integer(_))
                | (UNSIGNED, AggregateNumber::Unsigned(_))
                | (FLOAT, AggregateNumber::Float(_))
        ) {
            return Err("extrema witness has a different field type");
        }
        if self.projection.min {
            self.min = merge_number(self.min, Some(number), NumericReduction::Min)?;
        }
        if self.projection.max {
            self.max = merge_number(self.max, Some(number), NumericReduction::Max)?;
        }
        Ok(())
    }

    /// Every requested extremum must be dominated by an already visible candidate.
    pub(crate) fn dominates_extrema(
        &self,
        min: Option<AggregateNumber>,
        max: Option<AggregateNumber>,
        prior: Option<&Self>,
    ) -> Result<bool, &'static str> {
        if !self.projection.extrema_only() {
            return Ok(false);
        }
        if prior.is_some_and(|state| {
            state.field_type != self.field_type || state.projection != self.projection
        }) {
            return Err("extrema bounds have a different field type or projection");
        }
        if self.projection.min {
            let candidate = merge_number(
                self.min,
                prior.and_then(|state| state.min),
                NumericReduction::Min,
            )?;
            let (Some(bound), Some(candidate)) = (min, candidate) else {
                return Ok(false);
            };
            if bound.compare(candidate)?.is_lt() {
                return Ok(false);
            }
        }
        if self.projection.max {
            let candidate = merge_number(
                self.max,
                prior.and_then(|state| state.max),
                NumericReduction::Max,
            )?;
            let (Some(bound), Some(candidate)) = (max, candidate) else {
                return Ok(false);
            };
            if bound.compare(candidate)?.is_gt() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Reduces one visible point without materializing unrequested components.
    pub(crate) fn add_value(&mut self, value: Value) -> Result<(), &'static str> {
        if value.value_type() != self.field_type {
            return Err("aggregate point has a different field type");
        }
        let time = value.unix_nano();
        let (number, endpoint) = match value {
            Value::IntegerValue(v) => (
                Some(AggregateNumber::Integer(v.value)),
                AggregatePointValue::Integer(v.value),
            ),
            Value::UnsignedValue(v) => (
                Some(AggregateNumber::Unsigned(v.value)),
                AggregatePointValue::Unsigned(v.value),
            ),
            Value::FloatValue(v) => (
                Some(AggregateNumber::Float(v.value)),
                AggregatePointValue::Float(v.value),
            ),
            Value::BoolValue(v) => (None, AggregatePointValue::Boolean(v.value)),
            Value::StringValue(v) => (None, AggregatePointValue::String(v.value)),
            Value::EmptyValue(_) => return Err("cannot aggregate an empty point"),
        };
        if let Some(number) = number {
            if self.projection.sum {
                self.wide_sum = AggregateWideSum::merge(self.wide_sum, number.wide())?;
                self.sum = merge_number(
                    self.sum,
                    Some(number.singleton_sum()),
                    NumericReduction::Sum,
                )?;
            }
            if self.projection.min {
                self.min = merge_number(self.min, Some(number), NumericReduction::Min)?;
            }
            if self.projection.max {
                self.max = merge_number(self.max, Some(number), NumericReduction::Max)?;
            }
        } else if self.projection.sum || self.projection.min || self.projection.max {
            return Err("numeric aggregate requires a numeric point");
        }
        if let Some(count) = &mut self.count {
            *count = count.wrapping_add(1);
        }
        if self.projection.first && self.first.as_ref().is_none_or(|p| time < p.time) {
            self.first = Some(AggregatePoint {
                time,
                value: endpoint.clone(),
            });
        }
        if self.projection.last && self.last.as_ref().is_none_or(|p| time > p.time) {
            self.last = Some(AggregatePoint {
                time,
                value: endpoint,
            });
        }
        Ok(())
    }

    pub fn empty(field_type: FieldType, projection: AggregateProjection) -> Self {
        Self {
            field_type,
            projection,
            count: projection.count.then_some(0),
            sum: None,
            wide_sum: if projection.sum {
                AggregateWideSum::zero(field_type)
            } else {
                None
            },
            min: None,
            max: None,
            first: None,
            last: None,
        }
    }
    /// Merges states with identical field types and projections.
    pub fn merge(&mut self, other: Self) -> Result<(), &'static str> {
        if self.field_type != other.field_type || self.projection != other.projection {
            return Err("cannot merge aggregate states with different types or projections");
        }
        let sum = merge_number(self.sum, other.sum, NumericReduction::Sum)?;
        let wide_sum = AggregateWideSum::merge(self.wide_sum, other.wide_sum)?;
        let min = merge_number(self.min, other.min, NumericReduction::Min)?;
        let max = merge_number(self.max, other.max, NumericReduction::Max)?;
        let count = match (self.count, other.count) {
            (Some(a), Some(b)) => Some(a.wrapping_add(b)),
            (None, None) => None,
            _ => return Err("inconsistent projected count state"),
        };
        self.sum = sum;
        self.wide_sum = wide_sum;
        self.min = min;
        self.max = max;
        self.count = count;
        if let Some(point) = other.first
            && self.first.as_ref().is_none_or(|p| point.time < p.time)
        {
            self.first = Some(point);
        }
        if let Some(point) = other.last
            && self.last.as_ref().is_none_or(|p| point.time > p.time)
        {
            self.last = Some(point);
        }
        Ok(())
    }
    pub(crate) fn from_complete(state: AggregateState, projection: AggregateProjection) -> Self {
        let mut result = Self::empty(state.field_type, projection);
        result.count = projection.count.then_some(state.count);
        result.wide_sum = if projection.sum { state.wide_sum } else { None };
        let values = state.values.and_then(AggregateValues::numeric_components);
        if let Some([sum, min, max]) = values {
            result.sum = projection.sum.then_some(sum);
            result.min = projection.min.then_some(min);
            result.max = projection.max.then_some(max);
        }
        result.first = if projection.first { state.first } else { None };
        result.last = if projection.last { state.last } else { None };
        result
    }
    pub(crate) fn into_complete(self) -> Result<AggregateState, &'static str> {
        self.validate()?;
        let count = self.count.ok_or("complete aggregate requires count")?;
        let values = match (self.field_type, self.sum, self.min, self.max) {
            (_, None, None, None) if count == 0 => None,
            (
                INTEGER,
                Some(AggregateNumber::Integer(sum)),
                Some(AggregateNumber::Integer(min)),
                Some(AggregateNumber::Integer(max)),
            ) => Some(AggregateValues::Integer { sum, min, max }),
            (
                UNSIGNED,
                Some(AggregateNumber::Unsigned(sum)),
                Some(AggregateNumber::Unsigned(min)),
                Some(AggregateNumber::Unsigned(max)),
            ) => Some(AggregateValues::Unsigned { sum, min, max }),
            (
                FLOAT,
                Some(AggregateNumber::Float(sum)),
                Some(AggregateNumber::Float(min)),
                Some(AggregateNumber::Float(max)),
            ) => Some(AggregateValues::Float { sum, min, max }),
            (BOOL | STRING, None, None, None) => Some(AggregateValues::CountOnly),
            _ => return Err("complete aggregate requires all numeric components"),
        };
        Ok(AggregateState {
            field_type: self.field_type,
            count,
            values,
            wide_sum: self.wide_sum,
            first: self.first,
            last: self.last,
        })
    }

    /// Returns AVG when count and sum were requested. Integer means use wide_sum.
    pub fn average(&self) -> Result<Option<f64>, &'static str> {
        self.validate()?;
        let count = self.count.ok_or("average requires count")?;
        if !self.projection.sum {
            return Err("average requires sum");
        }
        super::aggregate_sum::average(self.field_type, count, self.sum, self.wide_sum)
    }
}

/// Logical page-group payload access, including cache hits; excludes metadata,
/// VLog data and tombstones. These are not device IO counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AggregateIndexReadMetrics {
    pub requested_bytes: u64,
    pub full_group_bytes: u64,
    pub read_requests: u64,
    pub decoded_columns: u64,
    pub legacy_groups: u64,
}
impl AggregateIndexReadMetrics {
    pub(crate) fn merge(&mut self, other: Self) {
        self.requested_bytes += other.requested_bytes;
        self.full_group_bytes += other.full_group_bytes;
        self.read_requests += other.read_requests;
        self.decoded_columns += other.decoded_columns;
        self.legacy_groups += other.legacy_groups;
    }
}
/// Result of a request-projected aggregate scan.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedAggregateResult {
    pub state: ProjectedAggregateState,
    pub metrics: AggregateScanMetrics,
    pub index_reads: AggregateIndexReadMetrics,
}

/// States aligned to the caller's ordered inclusive windows. IO and path metrics
/// cover the complete traversal once, rather than being duplicated per window.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedAggregateWindowsResult {
    pub states: Vec<ProjectedAggregateState>,
    pub metrics: AggregateScanMetrics,
    pub index_reads: AggregateIndexReadMetrics,
}
impl ProjectedAggregateResult {
    pub(crate) fn into_complete(self) -> Result<AggregateResult, &'static str> {
        Ok(AggregateResult {
            state: self.state.into_complete()?,
            metrics: self.metrics,
        })
    }

    pub(crate) fn from_complete(result: AggregateResult, projection: AggregateProjection) -> Self {
        Self {
            state: ProjectedAggregateState::from_complete(result.state, projection),
            metrics: result.metrics,
            index_reads: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_validation_rejects_inconsistent_public_state_fields() {
        let mut complete = AggregateState::empty(INTEGER);
        complete.add_integer_at(3, i64::MAX);
        complete.add_integer_at(7, 1);
        let valid = ProjectedAggregateState::from_complete(
            complete,
            AggregateProjection::complete(INTEGER, true),
        );
        assert!(valid.validate().is_ok());
        let corruptions: &[fn(&mut ProjectedAggregateState)] = &[
            |s| s.count = None,
            |s| s.projection.count = false,
            |s| s.sum = Some(AggregateNumber::Float(1.0)),
            |s| s.min = None,
            |s| s.projection.max = false,
            |s| s.wide_sum = Some(AggregateWideSum::Unsigned(0)),
            |s| s.wide_sum = Some(AggregateWideSum::Integer(1)),
            |s| s.first.as_mut().unwrap().value = AggregatePointValue::Boolean(true),
            |s| s.first.as_mut().unwrap().time = 8,
            |s| s.last = None,
            |s| s.projection.last = false,
        ];
        for corrupt in corruptions {
            let mut invalid = valid.clone();
            corrupt(&mut invalid);
            assert!(invalid.validate().is_err(), "{invalid:?}");
            assert!(invalid.average().is_err(), "{invalid:?}");
            assert!(invalid.into_complete().is_err());
        }
    }

    #[test]
    fn result_validation_preserves_empty_projection_legacy_and_wrapping_states() {
        for typ in [INTEGER, UNSIGNED, FLOAT, BOOL, STRING] {
            let projection = AggregateProjection::complete(typ, true);
            let state = ProjectedAggregateState::empty(typ, projection);
            assert!(state.validate().is_ok());
            assert!(state.into_complete().is_ok());
            assert!(
                ProjectedAggregateState::empty(typ, AggregateProjection::default())
                    .validate()
                    .is_ok()
            );
        }
        let mut complete = AggregateState::empty(INTEGER);
        complete.add_integer(i64::MAX);
        complete.add_integer(1);
        let mut legacy = ProjectedAggregateState::from_complete(
            complete,
            AggregateProjection::complete(INTEGER, false),
        );
        legacy.wide_sum = None;
        assert!(legacy.validate().is_ok());
        assert!(legacy.clone().into_complete().is_ok());
        assert!(legacy.average().is_err());
        legacy.count = Some(0);
        assert!(legacy.validate().is_ok());
        assert!(legacy.into_complete().is_ok());

        let mut unrequested = ProjectedAggregateState::empty(
            INTEGER,
            AggregateProjection::from_aggregates(&[AggregateKind::Count]),
        );
        unrequested.count = Some(1);
        assert!(unrequested.validate().is_ok());
        unrequested.wide_sum = Some(AggregateWideSum::Integer(0));
        assert!(unrequested.validate().is_err());

        let mut empty = ProjectedAggregateState::empty(
            INTEGER,
            AggregateProjection::from_aggregates(&[AggregateKind::Avg]),
        );
        empty.wide_sum = Some(AggregateWideSum::Integer(1));
        assert!(empty.validate().is_err());
    }

    #[test]
    fn extrema_bounds_require_all_components_and_preserve_float_order() {
        for mask in 0..64 {
            let projection = AggregateProjection {
                count: mask & 1 != 0,
                sum: mask & 2 != 0,
                min: mask & 4 != 0,
                max: mask & 8 != 0,
                first: mask & 16 != 0,
                last: mask & 32 != 0,
            };
            let mut state = ProjectedAggregateState::empty(INTEGER, projection);
            if projection.extrema_only() {
                assert!(
                    !state
                        .dominates_extrema(
                            Some(AggregateNumber::Integer(20)),
                            Some(AggregateNumber::Integer(30)),
                            None
                        )
                        .unwrap()
                );
                state
                    .add_extrema_witness(AggregateNumber::Integer(10))
                    .unwrap();
                state
                    .add_extrema_witness(AggregateNumber::Integer(40))
                    .unwrap();
                assert!(
                    state
                        .dominates_extrema(
                            Some(AggregateNumber::Integer(20)),
                            Some(AggregateNumber::Integer(30)),
                            None
                        )
                        .unwrap()
                );
                if projection.min {
                    assert!(
                        !state
                            .dominates_extrema(
                                Some(AggregateNumber::Integer(0)),
                                Some(AggregateNumber::Integer(30)),
                                None
                            )
                            .unwrap()
                    );
                }
                if projection.max {
                    assert!(
                        !state
                            .dominates_extrema(
                                Some(AggregateNumber::Integer(20)),
                                Some(AggregateNumber::Integer(50)),
                                None
                            )
                            .unwrap()
                    );
                }
            } else {
                assert!(
                    state
                        .add_extrema_witness(AggregateNumber::Integer(10))
                        .is_err()
                );
                assert!(
                    !state
                        .dominates_extrema(
                            Some(AggregateNumber::Integer(20)),
                            Some(AggregateNumber::Integer(30)),
                            None
                        )
                        .unwrap()
                );
            }
        }
        let projection = AggregateProjection::from_aggregates(&[AggregateKind::Max]);
        let mut state = ProjectedAggregateState::empty(FLOAT, projection);
        state
            .add_extrema_witness(AggregateNumber::Float(-0.0))
            .unwrap();
        assert!(
            !state
                .dominates_extrema(None, Some(AggregateNumber::Float(0.0)), None)
                .unwrap()
        );
        let nan = f64::from_bits(0x7ff8000000000001);
        state
            .add_extrema_witness(AggregateNumber::Float(nan))
            .unwrap();
        assert!(
            state
                .dominates_extrema(None, Some(AggregateNumber::Float(nan)), None)
                .unwrap()
        );
        assert!(
            !state
                .dominates_extrema(
                    None,
                    Some(AggregateNumber::Float(f64::from_bits(0x7ff8000000000002))),
                    None
                )
                .unwrap()
        );
        assert!(
            state
                .add_extrema_witness(AggregateNumber::Unsigned(1))
                .is_err()
        );
        let prior = ProjectedAggregateState::empty(INTEGER, projection);
        assert!(
            state
                .dominates_extrema(None, Some(AggregateNumber::Float(0.0)), Some(&prior))
                .is_err()
        );
    }
    #[test]
    fn direct_projected_points_preserve_wrapping_sums_endpoints_and_float_order() {
        use crate::models::points::{FloatValue, IntegerValue, StringValue};
        let projection = AggregateProjection::from_aggregates(&[
            AggregateKind::Avg,
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::First,
            AggregateKind::Last,
        ]);
        let mut integers = ProjectedAggregateState::empty(INTEGER, projection);
        integers
            .add_value(Value::IntegerValue(IntegerValue {
                unix_nano: 2,
                value: i64::MAX,
            }))
            .unwrap();
        integers
            .add_value(Value::IntegerValue(IntegerValue {
                unix_nano: 1,
                value: 1,
            }))
            .unwrap();
        assert_eq!(integers.count, Some(2));
        assert_eq!(integers.sum, Some(AggregateNumber::Integer(i64::MIN)));
        assert_eq!(integers.first.unwrap().time, 1);
        assert_eq!(integers.last.unwrap().time, 2);
        let mut floats = ProjectedAggregateState::empty(
            FLOAT,
            AggregateProjection::from_aggregates(&[AggregateKind::Min, AggregateKind::Max]),
        );
        for value in [0.0, -0.0, f64::INFINITY, f64::NAN] {
            floats
                .add_value(Value::FloatValue(FloatValue {
                    unix_nano: 0,
                    value,
                }))
                .unwrap();
        }
        assert!(
            matches!(floats.min, Some(AggregateNumber::Float(v)) if v.to_bits() == (-0.0f64).to_bits())
        );
        assert!(matches!(floats.max, Some(AggregateNumber::Float(v)) if v.is_nan()));
        assert_eq!(floats.sum, None);
        assert_eq!(floats.count, None);
        let mut strings = ProjectedAggregateState::empty(
            STRING,
            AggregateProjection::from_aggregates(&[AggregateKind::Count, AggregateKind::Last]),
        );
        strings
            .add_value(Value::StringValue(StringValue {
                unix_nano: 4,
                value: "last".into(),
            }))
            .unwrap();
        strings
            .add_value(Value::StringValue(StringValue {
                unix_nano: 3,
                value: "earlier".into(),
            }))
            .unwrap();
        assert_eq!(strings.count, Some(2));
        assert_eq!(
            strings.last.unwrap().value,
            AggregatePointValue::String("last".into())
        );
    }

    #[test]
    fn aggregate_projection_validates_operation_dependencies_for_each_field_type() {
        for (kind, numeric_only) in [
            (AggregateKind::Count, false),
            (AggregateKind::Sum, true),
            (AggregateKind::Avg, true),
            (AggregateKind::Min, true),
            (AggregateKind::Max, true),
            (AggregateKind::First, false),
            (AggregateKind::Last, false),
        ] {
            let projection = AggregateProjection::from_aggregates(&[kind]);
            for typ in [INTEGER, UNSIGNED, FLOAT] {
                assert!(projection.validate_field_type(typ).is_ok());
            }
            for typ in [BOOL, STRING] {
                assert_eq!(projection.validate_field_type(typ).is_err(), numeric_only);
            }
            assert!(projection.validate_field_type(u8::MAX).is_err());
        }
        let avg = AggregateProjection::from_aggregates(&[AggregateKind::Avg, AggregateKind::Avg]);
        assert_eq!(
            avg,
            AggregateProjection::from_aggregates(&[AggregateKind::Count, AggregateKind::Sum])
        );
    }

    #[test]
    fn projected_aggregate_merge_preserves_avg_dependencies_and_wrapping_sums() {
        for typ in [INTEGER, UNSIGNED, FLOAT] {
            let mut left = AggregateState::empty(typ);
            let mut right = AggregateState::empty(typ);
            match typ {
                INTEGER => {
                    left.add_integer_at(1, i64::MAX);
                    right.add_integer_at(2, 2);
                }
                UNSIGNED => {
                    left.add_unsigned_at(1, u64::MAX);
                    right.add_unsigned_at(2, 2);
                }
                FLOAT => {
                    left.add_float_at(1, -0.0);
                    right.add_float_at(2, 3.5);
                }
                _ => unreachable!(),
            }
            let mut full = left.clone();
            full.merge(right.clone()).unwrap();
            for kinds in [
                vec![AggregateKind::Sum],
                vec![AggregateKind::Avg],
                vec![AggregateKind::Min, AggregateKind::Max, AggregateKind::Last],
                vec![AggregateKind::Count, AggregateKind::First],
            ] {
                let projection = AggregateProjection::from_aggregates(&kinds);
                let mut actual = ProjectedAggregateState::empty(typ, projection);
                actual
                    .merge(ProjectedAggregateState::from_complete(
                        left.clone(),
                        projection,
                    ))
                    .unwrap();
                actual
                    .merge(ProjectedAggregateState::from_complete(
                        right.clone(),
                        projection,
                    ))
                    .unwrap();
                assert_eq!(
                    actual,
                    ProjectedAggregateState::from_complete(full.clone(), projection)
                );
                actual
                    .merge(ProjectedAggregateState::empty(typ, projection))
                    .unwrap();
                assert_eq!(
                    actual,
                    ProjectedAggregateState::from_complete(full.clone(), projection)
                );
            }
        }
    }

    #[test]
    fn projected_aggregate_merge_rejects_different_projections_without_mutation() {
        let sum = AggregateProjection::from_aggregates(&[AggregateKind::Sum]);
        let avg = AggregateProjection::from_aggregates(&[AggregateKind::Avg]);
        let mut left = ProjectedAggregateState::empty(INTEGER, sum);
        left.sum = Some(AggregateNumber::Integer(7));
        let before = left.clone();
        assert!(
            left.merge(ProjectedAggregateState::empty(INTEGER, avg))
                .is_err()
        );
        assert_eq!(left, before);
        assert!(
            left.merge(ProjectedAggregateState::empty(UNSIGNED, sum))
                .is_err()
        );
        assert_eq!(left, before);
    }
}
