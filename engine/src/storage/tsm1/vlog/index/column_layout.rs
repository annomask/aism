//! Physical statistic IDs and byte-range planning, independent of file IO.
//!
//! These IDs are part of the persisted v2-v4 schema. Adding a physical column
//! requires a format revision; adding a derived aggregate can reuse these IDs.

use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(crate) enum Column {
    Count = 0,
    Sum = 1,
    SumSq = 2,
    Min = 3,
    MinAt = 4,
    Max = 5,
    MaxAt = 6,
    First = 7,
    Last = 8,
}

pub(crate) const ALL_COLUMNS: [Column; 9] = [
    Column::Count,
    Column::Sum,
    Column::SumSq,
    Column::Min,
    Column::MinAt,
    Column::Max,
    Column::MaxAt,
    Column::First,
    Column::Last,
];
pub(crate) const COLUMN_COUNT: usize = ALL_COLUMNS.len();
pub(super) const PART_COUNT: usize = 1 + COLUMN_COUNT;
pub(super) const PART_CHECKSUM_SIZE: usize = size_of::<u32>();

/// One contiguous read and the payload parts it contains. Part zero is position
/// data; statistic column IDs address the remaining parts in schema order.
pub(super) struct ColumnReadRange {
    pub(super) parts: Range<usize>,
    pub(super) bytes: Range<usize>,
}

/// A bounded plan with no file handles or IO. Position data is always selected.
pub(super) struct ColumnReadPlan {
    offsets: [usize; PART_COUNT + 1],
    selected: [bool; PART_COUNT],
}

impl ColumnReadPlan {
    pub(super) fn new(
        position_len: u16,
        column_lens: &[u16; COLUMN_COUNT],
        columns: &[Column],
    ) -> Self {
        let mut offsets = [0; PART_COUNT + 1];
        offsets[1] = usize::from(position_len);
        for (i, len) in column_lens.iter().enumerate() {
            offsets[i + 2] = offsets[i + 1] + usize::from(*len);
        }
        let selected = std::array::from_fn(|i| i == 0 || columns.contains(&ALL_COLUMNS[i - 1]));
        Self { offsets, selected }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.selected.iter().all(|selected| *selected)
    }

    pub(super) fn part_range(&self, part: usize) -> Range<usize> {
        self.offsets[part]..self.offsets[part + 1]
    }

    /// Coalesces adjacent selected parts without including an unselected column.
    pub(super) fn ranges(&self) -> impl Iterator<Item = ColumnReadRange> + '_ {
        let mut next = 0;
        std::iter::from_fn(move || {
            while next < PART_COUNT && !self.selected[next] {
                next += 1;
            }
            if next == PART_COUNT {
                return None;
            }
            let begin = next;
            while next < PART_COUNT && self.selected[next] {
                next += 1;
            }
            Some(ColumnReadRange {
                parts: begin..next,
                bytes: self.offsets[begin]..self.offsets[next],
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_read_plan_covers_exact_bytes_for_every_projection() {
        let lengths = [3, 5, 2, 7, 4, 9, 6, 8, 11];
        // Independent byte labels detect skipped, duplicated and unwanted bytes.
        let mut labels = vec![None; 13];
        for (column, len) in ALL_COLUMNS.iter().zip(lengths) {
            labels.extend(std::iter::repeat_n(Some(*column), len as usize));
        }
        for mask in 0..1 << COLUMN_COUNT {
            let columns: Vec<_> = ALL_COLUMNS
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, column)| *column)
                .collect();
            let plan = ColumnReadPlan::new(13, &lengths, &columns);
            let ranges: Vec<_> = plan.ranges().collect();
            let actual: Vec<_> = ranges.iter().flat_map(|r| r.bytes.clone()).collect();
            let expected: Vec<_> = labels
                .iter()
                .enumerate()
                .filter(|(_, c)| c.is_none_or(|c| columns.contains(&c)))
                .map(|(i, _)| i)
                .collect();
            assert_eq!(actual, expected);
            assert_eq!(plan.is_complete(), columns.len() == COLUMN_COUNT);
            assert!(
                ranges
                    .windows(2)
                    .all(|pair| pair[0].bytes.end < pair[1].bytes.start)
            );
            for range in ranges {
                let parts: Vec<_> = range.parts.flat_map(|i| plan.part_range(i)).collect();
                assert_eq!(parts, range.bytes.collect::<Vec<_>>());
            }
            // Request order and duplicates must not change physical IO.
            let mut repeated = columns.clone();
            repeated.reverse();
            repeated.extend(&columns);
            let repeated = ColumnReadPlan::new(13, &lengths, &repeated);
            assert_eq!(
                repeated.ranges().flat_map(|r| r.bytes).collect::<Vec<_>>(),
                expected,
            );
        }
    }

    #[test]
    fn persisted_column_ids_remain_stable() {
        assert_eq!(
            ALL_COLUMNS.map(|column| column as usize),
            [0, 1, 2, 3, 4, 5, 6, 7, 8],
        );
    }
}
