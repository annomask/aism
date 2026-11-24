//! Query-owned column groups and bounded views of their selected rows.

use super::{
    AggScalar, AggSum, Level, Position, Result, SeqNo,
    column_layout::COLUMN_COUNT,
    invalid,
    page_group::{Column, ColumnValue, SelectedRecord},
};
use crate::{
    models::{FLOAT, INTEGER, UNSIGNED},
    query::{
        numeric_aggregate::NumericValue,
        projected_aggregate::{AggregateNumber, ProjectedAggregateState},
        storage_ir::AggregateWideSum,
    },
};
use std::{ops::Range, sync::Arc};

pub(crate) struct ProjectedGroup {
    pub(crate) seq_no: SeqNo,
    pub(crate) level: Level,
    pub(crate) positions: Vec<Position>,
    pub(crate) columns: [Option<Vec<ColumnValue>>; COLUMN_COUNT],
}

impl ProjectedGroup {
    #[cfg(test)]
    pub(crate) fn from_records(records: Vec<SelectedRecord>) -> Result<Self> {
        let first = records.first().ok_or(invalid("empty projected group"))?;
        let mut result = Self {
            seq_no: first.seq_no,
            level: first.level,
            positions: Vec::with_capacity(records.capacity()),
            columns: std::array::from_fn(|_| None),
        };
        for record in &records {
            result.positions.push(record.pos);
        }
        for column in 0..COLUMN_COUNT {
            if first.values[column].is_some() {
                result.columns[column] = Some(
                    records
                        .iter()
                        .map(|r| {
                            r.values[column].ok_or(invalid("inconsistent projected group columns"))
                        })
                        .collect::<Result<_>>()?,
                );
            }
        }
        Ok(result)
    }
    pub(crate) fn len(&self) -> usize {
        self.positions.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
    pub(crate) fn capacity(&self) -> usize {
        self.positions.capacity()
    }
    pub(crate) fn record(&self, row: usize) -> SelectedRecord {
        SelectedRecord {
            seq_no: self.seq_no,
            level: self.level,
            pos: self.positions[row],
            values: std::array::from_fn(|c| self.columns[c].as_ref().map(|v| v[row])),
        }
    }
    pub(crate) fn records(&self) -> Vec<SelectedRecord> {
        (0..self.len()).map(|row| self.record(row)).collect()
    }
    pub(crate) fn merge_numeric(
        &self,
        rows: Range<usize>,
        state: &mut ProjectedAggregateState,
    ) -> Result<()> {
        let started = crate::common::slow_trace::now();
        if state.projection.has_endpoints() {
            return Err(invalid("batch summaries do not include endpoints"));
        }
        if let Some(count) = &mut state.count {
            let values = self.columns[Column::Count as usize]
                .as_ref()
                .ok_or(invalid("aggregate projection requires count"))?;
            for value in &values[rows.clone()] {
                let ColumnValue::Count(value) = value else {
                    return Err(invalid("invalid batch count column"));
                };
                *count = count.wrapping_add(*value);
            }
        }
        macro_rules! numeric {
            ($typ:ty) => {{
                for (needed, column, output) in [
                    (state.projection.sum, Column::Sum, &mut state.sum),
                    (state.projection.min, Column::Min, &mut state.min),
                    (state.projection.max, Column::Max, &mut state.max),
                ] {
                    if !needed {
                        continue;
                    }
                    let values = self.columns[column as usize]
                        .as_ref()
                        .ok_or(invalid("batch aggregate column missing"))?;
                    let mut accumulator = output
                        .map(<$typ>::from_number)
                        .transpose()
                        .map_err(invalid)?;
                    let mut wide_sum = state.wide_sum;
                    for value in &values[rows.clone()] {
                        if column == Column::Sum {
                            let wide = match value {
                                ColumnValue::Sum(AggSum::Int(v)) => Some(AggregateWideSum::Integer(*v)),
                                ColumnValue::Sum(AggSum::Uint(v)) => Some(AggregateWideSum::Unsigned(*v)),
                                _ => None,
                            };
                            wide_sum = AggregateWideSum::merge(wide_sum, wide).map_err(invalid)?;
                        }
                        let number = match (state.field_type, value) {
                            (FLOAT, ColumnValue::Sum(AggSum::Float(v))) => {
                                AggregateNumber::Float(*v)
                            }
                            (FLOAT, ColumnValue::Scalar(AggScalar::Float(v))) => {
                                AggregateNumber::Float(*v)
                            }
                            (INTEGER, ColumnValue::Sum(AggSum::Int(v))) => {
                                AggregateNumber::Integer(*v as i64)
                            }
                            (INTEGER, ColumnValue::Scalar(AggScalar::Int(v))) => {
                                AggregateNumber::Integer(*v)
                            }
                            (UNSIGNED, ColumnValue::Sum(AggSum::Uint(v))) => {
                                AggregateNumber::Unsigned(*v as u64)
                            }
                            (UNSIGNED, ColumnValue::Scalar(AggScalar::Uint(v))) => {
                                AggregateNumber::Unsigned(*v)
                            }
                            _ => {
                                return Err(invalid("batch aggregate column has a different type"));
                            }
                        };
                        let value = <$typ>::from_number(number).map_err(invalid)?;
                        accumulator = Some(match accumulator {
                            None => value,
                            Some(old) => match column {
                                Column::Sum => NumericValue::add(old, value),
                                Column::Min => NumericValue::min(old, value),
                                Column::Max => NumericValue::max(old, value),
                                _ => unreachable!(),
                            },
                        });
                    }
                    *output = accumulator.map(NumericValue::number);
                    if column == Column::Sum {
                        state.wide_sum = wide_sum;
                    }
                }
            }};
        }
        match state.field_type {
            FLOAT => numeric!(f64),
            INTEGER => numeric!(i64),
            UNSIGNED => numeric!(u64),
            _ if !(state.projection.sum || state.projection.min || state.projection.max) => (),
            _ => return Err(invalid("batch summaries require numeric columns")),
        }
        crate::common::slow_trace::log_query_stage(
            "query_index_summary_batch",
            started,
            "ordered column reduction",
        );
        Ok(())
    }
}

pub(crate) struct SelectedBatch {
    pub(crate) group: Arc<ProjectedGroup>,
    pub(crate) rows: Range<usize>,
}
impl SelectedBatch {
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }
    pub(crate) fn first(&self) -> Position {
        self.group.positions[self.rows.start]
    }
    pub(crate) fn last(&self) -> Position {
        self.group.positions[self.rows.end - 1]
    }
    pub(crate) fn pop_first(&mut self) -> SelectedRecord {
        let row = self.rows.start;
        self.rows.start += 1;
        self.group.record(row)
    }
    pub(crate) fn records(&self) -> impl DoubleEndedIterator<Item = SelectedRecord> + '_ {
        self.rows.clone().map(|row| self.group.record(row))
    }
    pub(crate) fn positions(&self) -> impl DoubleEndedIterator<Item = Position> + '_ {
        self.group.positions[self.rows.clone()].iter().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::projected_aggregate::AggregateProjection;

    #[test]
    fn position_views_follow_remaining_rows_without_materializing_columns() {
        let positions = (0..8)
            .map(|i| Position {
                start: i * 2,
                end: i * 2 + 1,
                block: Default::default(),
                block_size: 0,
                flags: 0,
                child_count: 0,
                version: 0,
            })
            .collect::<Vec<_>>();
        // An empty statistic column makes record materialization invalid, but
        // positions alone remain valid for metadata-only frontier inspection.
        let mut columns = std::array::from_fn(|_| None);
        columns[Column::Count as usize] = Some(Vec::new());
        let group = Arc::new(ProjectedGroup {
            seq_no: SeqNo::new(1, 1).unwrap(),
            level: super::super::LEVEL_BLOCK,
            positions: positions.clone(),
            columns,
        });
        let mut batch = SelectedBatch {
            group,
            rows: 2..7,
        };
        assert_eq!(batch.positions().collect::<Vec<_>>(), positions[2..7]);
        assert_eq!(
            batch.positions().rev().collect::<Vec<_>>(),
            positions[2..7].iter().rev().copied().collect::<Vec<_>>()
        );
        batch.rows.start = 7;
        assert_eq!(batch.positions().count(), 0);
    }

    #[test]
    fn column_batches_match_ordered_scalar_merges_with_wrapping_counts() {
        for typ in [FLOAT, INTEGER, UNSIGNED] {
            for mask in 0..16 {
                let p = AggregateProjection {
                    count: mask & 1 != 0,
                    sum: mask & 2 != 0,
                    min: mask & 4 != 0,
                    max: mask & 8 != 0,
                    ..Default::default()
                };
                let numbers = (0..64)
                    .map(|i| match typ {
                        FLOAT => AggregateNumber::Float(
                            [1e20, 1.0, -1e20, -0.0, 0.0, -7.0, 8.0, 9.0][i % 8],
                        ),
                        INTEGER => {
                            AggregateNumber::Integer([i64::MAX, i64::MIN, -1, 0, 1, 2, 3, 4][i % 8])
                        }
                        UNSIGNED => {
                            AggregateNumber::Unsigned([u64::MAX, 0, 1, 2, 3, 4, 5, 6][i % 8])
                        }
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();
                let scalar = |number| match number {
                    AggregateNumber::Float(v) => AggScalar::Float(v),
                    AggregateNumber::Integer(v) => AggScalar::Int(v),
                    AggregateNumber::Unsigned(v) => AggScalar::Uint(v),
                };
                let sum = |number| match number {
                    AggregateNumber::Float(v) => AggSum::Float(v),
                    AggregateNumber::Integer(v) => AggSum::Int(v as i128),
                    AggregateNumber::Unsigned(v) => AggSum::Uint(v as u128),
                };
                let positions = (0..64)
                    .map(|i| Position {
                        start: i * 2,
                        end: i * 2 + 1,
                        block: Default::default(),
                        block_size: 0,
                        flags: 0,
                        child_count: 0,
                        version: 0,
                    })
                    .collect();
                let mut columns = std::array::from_fn(|_| None);
                columns[Column::Count as usize] = Some(vec![ColumnValue::Count(u64::MAX - 2); 64]);
                columns[Column::Sum as usize] =
                    Some(numbers.iter().map(|n| ColumnValue::Sum(sum(*n))).collect());
                columns[Column::Min as usize] = Some(
                    numbers
                        .iter()
                        .map(|n| ColumnValue::Scalar(scalar(*n)))
                        .collect(),
                );
                columns[Column::Max as usize] = Some(
                    numbers
                        .iter()
                        .map(|n| ColumnValue::Scalar(scalar(*n)))
                        .collect(),
                );
                let group = ProjectedGroup {
                    seq_no: SeqNo::new(1, 1).unwrap(),
                    level: 1,
                    positions,
                    columns,
                };
                let mut expected = ProjectedAggregateState::empty(typ, p);
                for number in &numbers[3..61] {
                    expected
                        .merge(ProjectedAggregateState {
                            field_type: typ,
                            projection: p,
                            count: p.count.then_some(u64::MAX - 2),
                            sum: p.sum.then_some(*number),
                            wide_sum: if p.sum {
                                match number {
                                    AggregateNumber::Integer(v) => Some(AggregateWideSum::Integer(i128::from(*v))),
                                    AggregateNumber::Unsigned(v) => Some(AggregateWideSum::Unsigned(u128::from(*v))),
                                    _ => None,
                                }
                            } else { None },
                            min: p.min.then_some(*number),
                            max: p.max.then_some(*number),
                            first: None,
                            last: None,
                        })
                        .unwrap();
                }
                let mut actual = ProjectedAggregateState::empty(typ, p);
                group.merge_numeric(3..19, &mut actual).unwrap();
                group.merge_numeric(19..61, &mut actual).unwrap();
                assert_eq!(
                    crate::query::numeric_aggregate::test_signature(&actual),
                    crate::query::numeric_aggregate::test_signature(&expected),
                    "type {typ} projection {mask}"
                );
            }
        }
    }
}
