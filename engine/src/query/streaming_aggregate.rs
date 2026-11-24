//! Bounded numeric reductions fed directly by checked block decoders.

use super::{
    aggregate_windows::AggregateWindows, numeric_aggregate::NumericValue,
    projected_aggregate::ProjectedAggregateState, storage_ir::AggregatePoint,
};
use std::ops::Range;

pub(crate) const MAX_STREAM_WINDOWS: usize = 16;

struct Span {
    window: usize,
    range: Range<usize>,
    state: ProjectedAggregateState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        models::{FLOAT, INTEGER, UNSIGNED},
        query::{
            numeric_aggregate::reduce_windows,
            projected_aggregate::AggregateProjection,
            storage_ir::TimeRange,
        },
    };

    fn signature(states: &[ProjectedAggregateState]) -> String {
        states
            .iter()
            .map(crate::query::numeric_aggregate::test_signature)
            .collect::<Vec<_>>()
            .join(";")
    }

    fn compare<T: NumericValue + Default, const MASK: u8>(
        values: &[T],
        projection: AggregateProjection,
    ) {
        let times = [i64::MIN, -10, -1, 0, 1, 3, 7, 11, i64::MAX];
        let ranges = [
            TimeRange::new(i64::MIN, -10),
            TimeRange::new(0, 3),
            TimeRange::new(9, i64::MAX),
        ];
        let windows = AggregateWindows::new(&ranges).unwrap();
        let mut expected = vec![ProjectedAggregateState::empty(T::TYPE, projection); ranges.len()];
        reduce_windows(&times, values, windows, &mut expected).unwrap();
        // Exercise continuation, rather than only a sum starting at zero.
        let mut actual = expected.clone();
        reduce_windows(&times, values, windows, &mut expected).unwrap();
        let mut reduction = StreamingReduction::<T, MASK>::new(&times, windows, &actual).unwrap();
        for &value in values {
            reduction.push(value);
        }
        reduction.finish(&mut actual).unwrap();
        assert_eq!(signature(&actual), signature(&expected));
        for count in [values.len() - 1, values.len() + 1] {
            let before = signature(&actual);
            let mut reduction =
                StreamingReduction::<T, MASK>::new(&times, windows, &actual).unwrap();
            for i in 0..count {
                reduction.push(values[i % values.len()]);
            }
            assert!(reduction.finish(&mut actual).is_err());
            assert_eq!(signature(&actual), before);
        }
    }

    #[test]
    fn streaming_reductions_match_arrays_and_commit_only_complete_blocks() {
        for mask in 0..64 {
            let p = AggregateProjection {
                count: mask & 1 != 0,
                sum: mask & 2 != 0,
                min: mask & 4 != 0,
                max: mask & 8 != 0,
                first: mask & 16 != 0,
                last: mask & 32 != 0,
            };
            let numeric = u8::from(p.sum) | (u8::from(p.min) << 1) | (u8::from(p.max) << 2);
            macro_rules! run {
                ($m:expr) => {{
                    compare::<i64, $m>(&[i64::MIN, i64::MAX, -1, 0, 1, 3, 7, 11, 100], p);
                    compare::<u64, $m>(&[u64::MAX, 0, 1, 2, 3, 4, 5, 6, 7], p);
                    compare::<f64, $m>(
                        &[
                            -0.0,
                            0.0,
                            1e20,
                            1.0,
                            -1e20,
                            f64::NEG_INFINITY,
                            f64::INFINITY,
                            f64::from_bits(0x7ff8000000000001),
                            2.0,
                        ],
                        p,
                    );
                }};
            }
            match numeric {
                0 => run!(0),
                1 => run!(1),
                2 => run!(2),
                3 => run!(3),
                4 => run!(4),
                5 => run!(5),
                6 => run!(6),
                7 => run!(7),
                _ => unreachable!(),
            }
        }
        assert_eq!(
            (
                <i64 as NumericValue>::TYPE,
                <u64 as NumericValue>::TYPE,
                <f64 as NumericValue>::TYPE
            ),
            (INTEGER, UNSIGNED, FLOAT)
        );
    }

    #[test]
    fn buffered_streaming_preserves_projections_across_chunks_and_gaps() {
        let times = (0..1027).map(|i| i * 3).collect::<Vec<_>>();
        let ranges = [
            TimeRange::new(7, 391),
            TimeRange::new(407, 902),
            TimeRange::new(1001, 3078),
        ];
        let windows = AggregateWindows::new(&ranges).unwrap();
        fn check<T: NumericValue + Default, const MASK: u8>(
            times: &[i64],
            values: &[T],
            windows: AggregateWindows<'_>,
            p: AggregateProjection,
        ) {
            let mut expected = vec![ProjectedAggregateState::empty(T::TYPE, p); 3];
            reduce_windows(times, values, windows, &mut expected).unwrap();
            let mut actual = expected.clone();
            reduce_windows(times, values, windows, &mut expected).unwrap();
            let mut r = StreamingReduction::<T, MASK>::new(times, windows, &actual).unwrap();
            for &v in values {
                r.push(v);
            }
            r.finish(&mut actual).unwrap();
            assert_eq!(signature(&actual), signature(&expected));
        }
        for flags in 0..64 {
            let p = AggregateProjection {
                count: flags & 1 != 0,
                sum: flags & 2 != 0,
                min: flags & 4 != 0,
                max: flags & 8 != 0,
                first: flags & 16 != 0,
                last: flags & 32 != 0,
            };
            let mask = u8::from(p.sum) | (u8::from(p.min) << 1) | (u8::from(p.max) << 2);
            macro_rules! run {
                ($mask:expr) => {{
                    let signed = (0..1027)
                        .map(|i| [i64::MAX, i64::MIN, -1, 0, 1][i % 5])
                        .collect::<Vec<_>>();
                    let unsigned = (0..1027)
                        .map(|i| [u64::MAX, 0, 1, 2, 3][i % 5])
                        .collect::<Vec<_>>();
                    let float = (0..1027)
                        .map(|i| [1e20, 1.0, -1e20, -0.0, 0.0][i % 5])
                        .collect::<Vec<_>>();
                    check::<i64, $mask>(&times, &signed, windows, p);
                    check::<u64, $mask>(&times, &unsigned, windows, p);
                    check::<f64, $mask>(&times, &float, windows, p);
                }};
            }
            match mask {
                0 => run!(0),
                1 => run!(1),
                2 => run!(2),
                3 => run!(3),
                4 => run!(4),
                5 => run!(5),
                6 => run!(6),
                7 => run!(7),
                _ => unreachable!(),
            }
        }
    }
}

/// Recycled per-reader storage; its capacity is charged to the read allowance.
#[derive(Default)]
pub(crate) struct StreamingWorkspace {
    spans: Vec<Span>,
}
impl StreamingWorkspace {
    pub(crate) fn bytes(&self) -> usize {
        self.spans.capacity() * std::mem::size_of::<Span>()
    }
}

/// Decode into a bounded vector and reduce contiguous slices. Span states are
/// local until complete decoding; chunks retain the array kernel's sum order.
pub(crate) struct StreamingReduction<'a, T, const MASK: u8> {
    times: &'a [i64],
    spans: Vec<Span>,
    seen: usize,
    buffered: usize,
    buffer: [T; 64],
    error: Option<&'static str>,
}
impl<'a, T: NumericValue + Default, const MASK: u8> StreamingReduction<'a, T, MASK> {
    #[cfg(test)]
    pub(crate) fn new(
        times: &'a [i64],
        windows: AggregateWindows<'_>,
        states: &[ProjectedAggregateState],
    ) -> Result<Self, &'static str> {
        Self::new_reusing(times, windows, states, &mut StreamingWorkspace::default())
    }
    pub(crate) fn new_reusing(
        times: &'a [i64],
        windows: AggregateWindows<'_>,
        states: &[ProjectedAggregateState],
        workspace: &mut StreamingWorkspace,
    ) -> Result<Self, &'static str> {
        let mut spans = std::mem::take(&mut workspace.spans);
        spans.clear();
        for (window, range) in windows.ordered_spans(times) {
            if spans.len() == MAX_STREAM_WINDOWS {
                return Err("streaming aggregate exceeds its window bound");
            }
            let mut state = states
                .get(window)
                .ok_or("streaming window state missing")?
                .clone();
            if state.field_type != T::TYPE {
                return Err("streaming aggregate state has a different type");
            }
            let mask = u8::from(state.projection.sum)
                | (u8::from(state.projection.min) << 1)
                | (u8::from(state.projection.max) << 2);
            if mask != MASK {
                return Err("streaming aggregate projection differs");
            }
            for (needed, number) in [
                (MASK & 1 != 0, state.sum),
                (MASK & 2 != 0, state.min),
                (MASK & 4 != 0, state.max),
            ] {
                if needed {
                    number.map(T::from_number).transpose()?;
                }
            }
            if let Some(count) = &mut state.count {
                *count = count.wrapping_add(range.len() as u64);
            }
            spans.push(Span {
                window,
                range,
                state,
            });
        }
        Ok(Self {
            times,
            spans,
            seen: 0,
            buffered: 0,
            buffer: [T::default(); 64],
            error: None,
        })
    }
    #[inline]
    pub(crate) fn push(&mut self, value: T) {
        self.seen = self.seen.saturating_add(1);
        if self.seen > self.times.len() || self.error.is_some() {
            return;
        }
        self.buffer[self.buffered] = value;
        self.buffered += 1;
        if self.buffered == self.buffer.len() {
            self.reduce_chunk();
        }
    }
    fn reduce_chunk(&mut self) {
        let start = self.seen - self.buffered;
        let end = self.seen;
        for span in &mut self.spans {
            let begin = span.range.start.max(start);
            let finish = span.range.end.min(end);
            if begin >= finish {
                continue;
            }
            let values = &self.buffer[begin - start..finish - start];
            if let Err(error) =
                super::numeric_aggregate::reduce_values::<T, MASK>(values, &mut span.state)
            {
                self.error = Some(error);
                self.buffered = 0;
                return;
            }
            let first_time = self.times[begin];
            if span.state.projection.first
                && span
                    .state
                    .first
                    .as_ref()
                    .is_none_or(|p| first_time < p.time)
            {
                span.state.first = Some(AggregatePoint {
                    time: first_time,
                    value: values[0].endpoint(),
                });
            }
            let last_time = self.times[finish - 1];
            if span.state.projection.last
                && span.state.last.as_ref().is_none_or(|p| last_time > p.time)
            {
                span.state.last = Some(AggregatePoint {
                    time: last_time,
                    value: values[values.len() - 1].endpoint(),
                });
            }
        }
        self.buffered = 0;
    }
    #[cfg(test)]
    pub(crate) fn finish(self, states: &mut [ProjectedAggregateState]) -> Result<(), &'static str> {
        self.finish_reusing(states, &mut StreamingWorkspace::default())
    }
    pub(crate) fn finish_reusing(
        mut self,
        states: &mut [ProjectedAggregateState],
        workspace: &mut StreamingWorkspace,
    ) -> Result<(), &'static str> {
        if self.seen != self.times.len() {
            return Err("streaming value count does not match timestamps");
        }
        self.reduce_chunk();
        if let Some(error) = self.error {
            return Err(error);
        }
        for span in self.spans.drain(..) {
            states[span.window] = span.state;
        }
        workspace.spans = self.spans;
        Ok(())
    }
}
