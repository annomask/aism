//! Logical aggregates spanning caller-selected, disjoint shard ranges.
//! No published record contains a file identity or a child address.
#[cfg(test)]
#[path = "cross_shard_tests.rs"]
mod tests;
use super::*;
use crate::query::storage_ir::{
    AggregateNumber, AggregatePoint, AggregatePointValue, AggregateProjection, AggregateWideSum,
    ProjectedAggregateState, TimeRange,
};
use store::IndexStore;

pub(crate) const MAX_CROSS_SHARD_LEAVES: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct LogicalPosition {
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) child_count: u8,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct LogicalRecord {
    pub(crate) pos: LogicalPosition,
    pub(crate) level: Level,
    pub(crate) values: AggValues,
}

#[derive(Debug)]
pub(crate) struct LogicalTree {
    pub(crate) levels: Vec<Vec<LogicalRecord>>,
}

fn logical(record: Record) -> LogicalRecord {
    LogicalRecord {
        pos: LogicalPosition {
            start: record.pos.start,
            end: record.pos.end,
            child_count: record.pos.child_count,
        },
        level: record.level,
        values: record.values,
    }
}

impl LogicalTree {
    pub(crate) fn matches(&self, leaves: &[LogicalRecord]) -> bool {
        self.levels[0] == leaves
    }

    pub(crate) fn bytes(&self) -> usize {
        self.levels
            .iter()
            .map(|v| v.capacity() * std::mem::size_of::<LogicalRecord>())
            .sum()
    }

    pub(crate) fn build(leaves: Vec<LogicalRecord>) -> Result<Self> {
        if leaves.is_empty() || leaves.len() > MAX_CROSS_SHARD_LEAVES {
            return Err(invalid("invalid cross-shard leaf count"));
        }
        for pair in leaves.windows(2) {
            if pair[0].pos.end >= pair[1].pos.start {
                return Err(invalid("overlapping cross-shard aggregate ranges"));
            }
        }
        let mut levels = vec![leaves];
        while levels.last().unwrap().len() > FAN_OUT && levels.len() < MAX_LEVELS as usize {
            let lower = levels.last().unwrap();
            let groups = lower.len().div_ceil(FAN_OUT);
            let base = lower.len() / groups;
            let remainder = lower.len() % groups;
            let mut upper = Vec::with_capacity(groups);
            let mut first = 0;
            for group in 0..groups {
                let size = base + usize::from(group < remainder);
                let children = &lower[first..first + size];
                let mut parent = children[0];
                parent.level += 1;
                parent.pos.child_count = size as u8;
                for child in &children[1..] {
                    parent.values = parent.values.merge_ordered(child.values)?;
                    parent.pos.end = child.pos.end;
                }
                upper.push(parent);
                first += size;
            }
            levels.push(upper);
        }
        Ok(Self { levels })
    }

    /// Returns complete summaries and boundary ranges. Boundary reads go back
    /// through the native shard query, which resolves its current locators.
    pub(crate) fn visit(
        &self,
        range: TimeRange,
        mut summary: impl FnMut(&LogicalRecord) -> Result<()>,
        mut boundary: impl FnMut(TimeRange),
    ) -> Result<()> {
        fn descend(
            tree: &LogicalTree,
            level: usize,
            lo: i64,
            hi: i64,
            range: TimeRange,
            summary: &mut impl FnMut(&LogicalRecord) -> Result<()>,
            boundary: &mut impl FnMut(TimeRange),
        ) -> Result<()> {
            let records = &tree.levels[level];
            let begin = records.partition_point(|r| r.pos.end < lo.max(range.start_time));
            for record in &records[begin..] {
                let p = record.pos;
                if p.start > hi.min(range.end_time) {
                    break;
                }
                if p.start >= range.start_time && p.end <= range.end_time {
                    summary(record)?;
                } else if level == 0 {
                    boundary(TimeRange::new(
                        p.start.max(range.start_time),
                        p.end.min(range.end_time),
                    ));
                } else {
                    descend(tree, level - 1, p.start, p.end, range, summary, boundary)?;
                }
            }
            Ok(())
        }
        descend(
            self,
            self.levels.len() - 1,
            i64::MIN,
            i64::MAX,
            range,
            &mut summary,
            &mut boundary,
        )
    }
}

impl IndexStore {
    /// Captures only clean visible leaves through the native locator protocol.
    /// Partial deletes, overlaps and the metadata bound use the local query.
    pub(crate) fn cross_shard_leaves(
        &self,
        seq: SeqNo,
        scope: TimeRange,
    ) -> Result<Option<Vec<LogicalRecord>>> {
        let (files, _vlogs, native) = self.native_query_view(seq)?;
        let mut leaves = Vec::new();
        for file in &files {
            let Some(entry) = file.lookup(seq)? else {
                continue;
            };
            let header = file.segment_header(&entry)?;
            if header.level(LEVEL_BLOCK).is_none() {
                continue;
            }
            let deletes = native
                .as_ref()
                .map_or_else(|| file.tombstone_view(seq), |n| n.tombstones(file, seq));
            let mut groups = file.groups(&entry, &header, LEVEL_BLOCK)?;
            for group in 0..groups.len() {
                for selected in groups.read_selected(group, &page_group::ALL_COLUMNS)? {
                    let record = selected.into_record()?;
                    let p = record.pos;
                    if p.end < scope.start_time || p.start > scope.end_time {
                        continue;
                    }
                    let (deleted, covered) = deletes.coverage(p.start, p.end)?;
                    if covered {
                        continue;
                    }
                    if deleted
                        || p.flags != 0
                        || p.start < scope.start_time
                        || p.end > scope.end_time
                        || leaves.len() == MAX_CROSS_SHARD_LEAVES
                    {
                        return Ok(None);
                    }
                    leaves.push(logical(record));
                }
            }
        }
        leaves.sort_unstable_by_key(|r| r.pos.start);
        if leaves.windows(2).any(|r| r[0].pos.end >= r[1].pos.start) {
            return Ok(None);
        }
        Ok(Some(leaves))
    }
}

pub(crate) fn project(
    record: &LogicalRecord,
    projection: AggregateProjection,
) -> Result<ProjectedAggregateState> {
    let v = record.values;
    let mut state = ProjectedAggregateState::empty(v.field_type(), projection);
    if projection.count {
        state.count = Some(v.count)
    }
    if projection.sum {
        let (sum, wide) = match v.sum {
            AggSum::Int(n) => (
                AggregateNumber::Integer(n as i64),
                Some(AggregateWideSum::Integer(n)),
            ),
            AggSum::Uint(n) => (
                AggregateNumber::Unsigned(n as u64),
                Some(AggregateWideSum::Unsigned(n)),
            ),
            AggSum::Float(n) => (AggregateNumber::Float(n), None),
            AggSum::CountOnly => return Err(invalid("non-numeric cross-shard sum")),
        };
        state.sum = Some(sum);
        state.wide_sum = wide;
    }
    let number = |v| match v {
        AggScalar::Int(n) => Ok(AggregateNumber::Integer(n)),
        AggScalar::Uint(n) => Ok(AggregateNumber::Unsigned(n)),
        AggScalar::Float(n) => Ok(AggregateNumber::Float(n)),
        _ => Err(invalid("non-numeric cross-shard extremum")),
    };
    if projection.min {
        state.min = Some(number(v.min)?)
    }
    if projection.max {
        state.max = Some(number(v.max)?)
    }
    let point = |value, time| {
        Ok(AggregatePoint {
            time,
            value: match value {
                AggScalar::Int(n) => AggregatePointValue::Integer(n),
                AggScalar::Uint(n) => AggregatePointValue::Unsigned(n),
                AggScalar::Float(n) => AggregatePointValue::Float(n),
                AggScalar::Bool(n) => AggregatePointValue::Boolean(n),
                AggScalar::None => {
                    return Err(invalid("string endpoints require the native query"));
                }
            },
        })
    };
    if projection.first {
        state.first = Some(point(v.first, record.pos.start)?)
    }
    if projection.last {
        state.last = Some(point(v.last, record.pos.end)?)
    }
    Ok(state)
}
