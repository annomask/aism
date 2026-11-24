//! Ordered, disjoint inclusive windows shared by logical aggregate execution.

use super::storage_ir::TimeRange;

#[derive(Clone, Copy)]
pub(crate) struct AggregateWindows<'a> {
    ranges: &'a [TimeRange],
}

impl<'a> AggregateWindows<'a> {
    pub(crate) fn new(ranges: &'a [TimeRange]) -> Result<Self, &'static str> {
        if ranges.is_empty()
            || ranges.iter().any(|range| range.start_time > range.end_time)
            || ranges
                .windows(2)
                .any(|pair| pair[0].end_time >= pair[1].start_time)
        {
            return Err("aggregate windows must be nonempty, ordered and disjoint");
        }
        Ok(Self { ranges })
    }

    pub(crate) fn envelope(self) -> TimeRange {
        TimeRange::new(
            self.ranges[0].start_time,
            self.ranges.last().unwrap().end_time,
        )
    }

    pub(crate) fn for_time(self, time: i64) -> Option<usize> {
        let index = self
            .ranges
            .partition_point(|range| range.start_time <= time)
            .checked_sub(1)?;
        self.ranges[index].contains(time).then_some(index)
    }

    /// Routes ascending timestamps in linear time, including gaps and extremes.
    pub(crate) fn ordered_cursor(self) -> OrderedWindowCursor<'a> {
        OrderedWindowCursor {
            windows: self,
            index: 0,
        }
    }

    /// Groups validated ascending timestamps into contiguous window slices.
    pub(crate) fn ordered_spans<'b>(self, times: &'b [i64]) -> WindowSpans<'a, 'b> {
        WindowSpans {
            windows: self,
            times,
            point: 0,
            window: times.first().map_or(0, |time| {
                self.ranges.partition_point(|range| range.end_time < *time)
            }),
        }
    }

    pub(crate) fn containing(self, start: i64, end: i64) -> Option<usize> {
        self.for_time(start)
            .filter(|index| end <= self.ranges[*index].end_time)
    }

    pub(crate) fn intersects(self, start: i64, end: i64) -> bool {
        let index = self.ranges.partition_point(|range| range.end_time < start);
        self.ranges
            .get(index)
            .is_some_and(|range| range.start_time <= end)
    }

    /// Indices of windows touched by an inclusive record interval, including gaps.
    pub(crate) fn intersecting(self, start: i64, end: i64) -> std::ops::Range<usize> {
        let first = self.ranges.partition_point(|range| range.end_time < start);
        let last = self.ranges.partition_point(|range| range.start_time <= end);
        first..last
    }
}

pub(crate) struct WindowSpans<'a, 'b> {
    windows: AggregateWindows<'a>,
    times: &'b [i64],
    point: usize,
    window: usize,
}

impl Iterator for WindowSpans<'_, '_> {
    type Item = (usize, std::ops::Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(range) = self.windows.ranges.get(self.window) {
            let window = self.window;
            self.window += 1;
            let begin = self.point
                + self.times[self.point..].partition_point(|time| *time < range.start_time);
            if begin == self.times.len() {
                return None;
            }
            self.point =
                begin + self.times[begin..].partition_point(|time| *time <= range.end_time);
            if begin < self.point {
                return Some((window, begin..self.point));
            }
        }
        None
    }
}

pub(crate) struct OrderedWindowCursor<'a> {
    windows: AggregateWindows<'a>,
    index: usize,
}

impl OrderedWindowCursor<'_> {
    pub(crate) fn for_time(&mut self, time: i64) -> Option<usize> {
        while self.index < self.windows.ranges.len()
            && self.windows.ranges[self.index].end_time < time
        {
            self.index += 1;
        }
        self.windows
            .ranges
            .get(self.index)
            .filter(|range| range.contains(time))
            .map(|_| self.index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersecting_indices_match_inclusive_windows_and_gaps() {
        let ranges = [
            TimeRange::new(i64::MIN, -2),
            TimeRange::new(0, 1),
            TimeRange::new(5, i64::MAX),
        ];
        let windows = AggregateWindows::new(&ranges).unwrap();
        for (start, end) in [
            (i64::MIN, i64::MAX),
            (-1, -1),
            (-2, 0),
            (1, 5),
            (2, 4),
            (i64::MAX, i64::MAX),
        ] {
            let actual: Vec<_> = windows.intersecting(start, end).collect();
            let expected: Vec<_> = ranges
                .iter()
                .enumerate()
                .filter(|(_, range)| range.start_time <= end && start <= range.end_time)
                .map(|(i, _)| i)
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn ordered_spans_match_point_routing_across_gaps_and_extremes() {
        let ranges = [
            TimeRange::new(i64::MIN, -2),
            TimeRange::new(0, 1),
            TimeRange::new(5, 8),
            TimeRange::new(10, i64::MAX),
        ];
        let windows = AggregateWindows::new(&ranges).unwrap();
        for times in [
            vec![],
            vec![i64::MIN, -2, -1, 0, 1, 2, 5, 8, 9, 10, i64::MAX],
            vec![2, 3, 4],
            vec![i64::MAX],
            vec![-1],
        ] {
            let actual = windows
                .ordered_spans(&times)
                .flat_map(|(window, span)| span.map(move |point| (point, window)))
                .collect::<Vec<_>>();
            let expected = times
                .iter()
                .enumerate()
                .filter_map(|(point, time)| windows.for_time(*time).map(|window| (point, window)))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn windows_resolve_inclusive_edges_gaps_and_extreme_timestamps() {
        let ranges = [
            TimeRange::new(i64::MIN, -1),
            TimeRange::new(2, 3),
            TimeRange::new(8, i64::MAX),
        ];
        let windows = AggregateWindows::new(&ranges).unwrap();
        let mut cursor = windows.ordered_cursor();
        for time in [i64::MIN, -1, 0, 1, 2, 3, 4, 7, 8, i64::MAX] {
            assert_eq!(cursor.for_time(time), windows.for_time(time));
        }
        assert_eq!(windows.for_time(i64::MIN), Some(0));
        assert_eq!(windows.for_time(-1), Some(0));
        assert_eq!(windows.for_time(0), None);
        assert_eq!(windows.for_time(3), Some(1));
        assert_eq!(windows.for_time(i64::MAX), Some(2));
        assert_eq!(windows.containing(2, 8), None);
        assert!(!windows.intersects(4, 7));
        assert!(windows.intersects(-2, 2));
        assert_eq!(windows.envelope(), TimeRange::new(i64::MIN, i64::MAX));
        for ranges in [
            vec![],
            vec![TimeRange::new(2, 1)],
            vec![TimeRange::new(0, 1), TimeRange::new(1, 2)],
        ] {
            assert!(AggregateWindows::new(&ranges).is_err());
        }
    }
}
