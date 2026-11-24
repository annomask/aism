//! Query-owned positioned reads and typed block decoding. No metadata guard
//! survives IO or decoding; retained bytes and file handles have a byte budget.

use super::reader::{Error, Result, VLogReader};
use crate::{
    common::slow_trace,
    models::{
        FLOAT, FieldType, INTEGER, UNSIGNED,
        points::{FloatValue, IntegerValue, UnsignedValue, Value},
    },
    storage::{
        AggregateQueryOptions,
        tsm1::{
            cursor::array_values::{FloatArray, IntegerArray, UnsignedArray},
            data_encoding::{array_encoding, encoding},
        },
    },
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) enum DecodedBlock {
    Float(FloatArray),
    Integer(IntegerArray),
    Unsigned(UnsignedArray),
    Other(Vec<Value>),
}

impl DecodedBlock {
    fn buffer_bytes(&self) -> Option<usize> {
        fn arrays<T>(times: usize, values: usize) -> Option<usize> {
            times
                .checked_mul(std::mem::size_of::<i64>())?
                .checked_add(values.checked_mul(std::mem::size_of::<T>())?)
        }
        match self {
            Self::Float(a) => arrays::<f64>(a.timestamps.capacity(), a.values.capacity()),
            Self::Integer(a) => arrays::<i64>(a.timestamps.capacity(), a.values.capacity()),
            Self::Unsigned(a) => arrays::<u64>(a.timestamps.capacity(), a.values.capacity()),
            Self::Other(_) => None,
        }
    }

    pub(crate) fn reduce_windows(
        &self,
        windows: crate::query::aggregate_windows::AggregateWindows<'_>,
        states: &mut [crate::query::projected_aggregate::ProjectedAggregateState],
    ) -> std::result::Result<bool, &'static str> {
        use crate::query::numeric_aggregate::reduce_windows;
        match self {
            Self::Float(a) => reduce_windows(&a.timestamps, &a.values, windows, states)?,
            Self::Integer(a) => reduce_windows(&a.timestamps, &a.values, windows, states)?,
            Self::Unsigned(a) => reduce_windows(&a.timestamps, &a.values, windows, states)?,
            Self::Other(_) => return Ok(false),
        }
        Ok(true)
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Float(a) => a.timestamps.len(),
            Self::Integer(a) => a.timestamps.len(),
            Self::Unsigned(a) => a.timestamps.len(),
            Self::Other(a) => a.len(),
        }
    }

    pub(crate) fn valid_range(&self, count: u64, start: i64, end: i64, typ: FieldType) -> bool {
        fn valid(times: &[i64], values: usize, count: u64, start: i64, end: i64) -> bool {
            times.len() == values
                && times.len() as u64 == count
                && times.first() == Some(&start)
                && times.last() == Some(&end)
                && times.windows(2).all(|p| p[0] < p[1])
        }
        match self {
            Self::Float(a) => {
                typ == FLOAT && valid(&a.timestamps, a.values.len(), count, start, end)
            }
            Self::Integer(a) => {
                typ == INTEGER && valid(&a.timestamps, a.values.len(), count, start, end)
            }
            Self::Unsigned(a) => {
                typ == UNSIGNED && valid(&a.timestamps, a.values.len(), count, start, end)
            }
            Self::Other(a) => {
                a.len() as u64 == count
                    && a.first().is_some_and(|v| v.unix_nano() == start)
                    && a.last().is_some_and(|v| v.unix_nano() == end)
                    && a.iter().all(|v| v.value_type() == typ)
                    && a.windows(2).all(|p| p[0].unix_nano() < p[1].unix_nano())
            }
        }
    }

    pub(crate) fn into_values(self) -> BlockValues {
        match self {
            Self::Float(a) => BlockValues::Float(a.timestamps.into_iter().zip(a.values)),
            Self::Integer(a) => BlockValues::Integer(a.timestamps.into_iter().zip(a.values)),
            Self::Unsigned(a) => BlockValues::Unsigned(a.timestamps.into_iter().zip(a.values)),
            Self::Other(a) => BlockValues::Other(a.into_iter()),
        }
    }
}

pub(crate) enum BlockValues {
    Float(std::iter::Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<f64>>),
    Integer(std::iter::Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<i64>>),
    Unsigned(std::iter::Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<u64>>),
    Other(std::vec::IntoIter<Value>),
}
impl Iterator for BlockValues {
    type Item = Value;
    fn next(&mut self) -> Option<Value> {
        match self {
            Self::Float(a) => a
                .next()
                .map(|(unix_nano, value)| Value::FloatValue(FloatValue { unix_nano, value })),
            Self::Integer(a) => a
                .next()
                .map(|(unix_nano, value)| Value::IntegerValue(IntegerValue { unix_nano, value })),
            Self::Unsigned(a) => a
                .next()
                .map(|(unix_nano, value)| Value::UnsignedValue(UnsignedValue { unix_nano, value })),
            Self::Other(a) => a.next(),
        }
    }
}

#[cfg(test)]
fn decode(bytes: &[u8], typ: FieldType) -> Result<DecodedBlock> {
    decode_reusing(bytes, typ, &mut None)
}

fn decode_reusing(
    bytes: &[u8],
    typ: FieldType,
    reusable: &mut Option<DecodedBlock>,
) -> Result<DecodedBlock> {
    if bytes.len() < 5 {
        return Err(Error::StringErr {
            reason: "invalid aggregate block size".into(),
        });
    }
    let started = slow_trace::now();
    let checksum = u32::from_be_bytes(bytes[..4].try_into().unwrap());
    if crc32fast::hash(&bytes[4..]) != checksum {
        return Err(Error::StringErr {
            reason: "VLog block checksum mismatch".into(),
        });
    }
    slow_trace::log_query_stage("query_block_checksum", started, "checked VLog block");
    let started = slow_trace::now();
    let previous = reusable.take();
    let result = match typ {
        FLOAT => {
            let mut a = match previous {
                Some(DecodedBlock::Float(a)) => a,
                _ => FloatArray::default(),
            };
            array_encoding::decode_float_array_block(&bytes[4..], &mut a)
                .map_err(|source| Error::ArrayEncoding { source })?;
            DecodedBlock::Float(a)
        }
        INTEGER => {
            let mut a = match previous {
                Some(DecodedBlock::Integer(a)) => a,
                _ => IntegerArray::default(),
            };
            array_encoding::decode_integer_array_block(&bytes[4..], &mut a)
                .map_err(|source| Error::ArrayEncoding { source })?;
            DecodedBlock::Integer(a)
        }
        UNSIGNED => {
            let mut a = match previous {
                Some(DecodedBlock::Unsigned(a)) => a,
                _ => UnsignedArray::default(),
            };
            array_encoding::decode_unsigned_array_block(&bytes[4..], &mut a)
                .map_err(|source| Error::ArrayEncoding { source })?;
            DecodedBlock::Unsigned(a)
        }
        _ => DecodedBlock::Other(
            encoding::decode_block(&bytes[4..]).map_err(|source| Error::Encoding { source })?,
        ),
    };
    slow_trace::log_query_stage("query_block_decode", started, "checked VLog block");
    Ok(result)
}

#[cfg(test)]
fn decode_reduce<const MASK: u8>(
    bytes: &[u8],
    times: &mut Vec<i64>,
    typ: FieldType,
    windows: crate::query::aggregate_windows::AggregateWindows<'_>,
    states: &mut [crate::query::projected_aggregate::ProjectedAggregateState],
    expected: u64,
    start: i64,
    end: i64,
) -> Result<()> {
    decode_reduce_reusing::<MASK>(
        bytes,
        times,
        typ,
        windows,
        states,
        expected,
        start,
        end,
        &mut crate::query::streaming_aggregate::StreamingWorkspace::default(),
    )
}

fn decode_reduce_reusing<const MASK: u8>(
    bytes: &[u8],
    times: &mut Vec<i64>,
    typ: FieldType,
    windows: crate::query::aggregate_windows::AggregateWindows<'_>,
    states: &mut [crate::query::projected_aggregate::ProjectedAggregateState],
    expected: u64,
    start: i64,
    end: i64,
    workspace: &mut crate::query::streaming_aggregate::StreamingWorkspace,
) -> Result<()> {
    use crate::query::streaming_aggregate::StreamingReduction;
    use crate::storage::tsm1::data_encoding::{batch_float, streaming_int};
    let error = |reason: &str| Error::StringErr {
        reason: reason.into(),
    };
    let started = slow_trace::now();
    if bytes.len() < 5
        || crc32fast::hash(&bytes[4..]) != u32::from_be_bytes(bytes[..4].try_into().unwrap())
    {
        return Err(error("VLog block checksum mismatch"));
    }
    slow_trace::log_query_stage("query_block_checksum", started, "streaming numeric block");
    let block_type = match typ {
        FLOAT => encoding::BLOCK_FLOAT64,
        INTEGER => encoding::BLOCK_INTEGER,
        UNSIGNED => encoding::BLOCK_UNSIGNED,
        _ => return Err(error("invalid streaming numeric type")),
    };
    let started = slow_trace::now();
    array_encoding::decode_timestamp_array_block_into(&bytes[4..], block_type, times)
        .map_err(|source| Error::ArrayEncoding { source })?;
    if times.len() as u64 != expected
        || times.first() != Some(&start)
        || times.last() != Some(&end)
        || !times.windows(2).all(|p| p[0] < p[1])
    {
        return Err(error("aggregate VLog timestamps do not match index"));
    }
    slow_trace::log_query_stage("query_timestamp_decode", started, "streaming numeric block");
    let (_, values) =
        encoding::unpack_block(&bytes[5..]).map_err(|source| Error::Encoding { source })?;
    let started = slow_trace::now();
    match typ {
        FLOAT => {
            let mut reduction =
                StreamingReduction::<f64, MASK>::new_reusing(times, windows, states, workspace)
                    .map_err(error)?;
            batch_float::float_array_decode_all_with(values, |value| reduction.push(value))
                .map_err(|source| Error::ArrayEncoding {
                    source: array_encoding::Error::BatchFloat { source },
                })?;
            reduction.finish_reusing(states, workspace).map_err(error)?;
        }
        INTEGER => {
            let mut reduction =
                StreamingReduction::<i64, MASK>::new_reusing(times, windows, states, workspace)
                    .map_err(error)?;
            streaming_int::decode_with(values, times.len(), |value| reduction.push(value))
                .map_err(|source| Error::ArrayEncoding {
                    source: array_encoding::Error::BatchInt { source },
                })?;
            reduction.finish_reusing(states, workspace).map_err(error)?;
        }
        UNSIGNED => {
            let mut reduction =
                StreamingReduction::<u64, MASK>::new_reusing(times, windows, states, workspace)
                    .map_err(error)?;
            streaming_int::decode_with(values, times.len(), |value| reduction.push(value as u64))
                .map_err(|source| Error::ArrayEncoding {
                source: array_encoding::Error::BatchInt { source },
            })?;
            reduction.finish_reusing(states, workspace).map_err(error)?;
        }
        _ => unreachable!(),
    }
    slow_trace::log_query_stage(
        "query_numeric_stream_reduction",
        started,
        "value decode and ordered reduction",
    );
    Ok(())
}

struct Span {
    // Pin the exact immutable reader identity even if publication changes.
    _reader: Arc<VLogReader>,
    bytes: Vec<u8>,
    used: u64,
}

struct Handle {
    _reader: Arc<VLogReader>,
    fd: Arc<std::fs::File>,
    size: u64,
    used: u64,
}

const MAX_PLANNED_READ_BYTES: usize = 64 * 1024;
const MAX_PLANNED_GAP_BYTES: u64 = 4096;
pub(crate) const MAX_PLANNED_BLOCKS: usize = 64;

struct PlannedSpan {
    reader: Arc<VLogReader>,
    offset: u64,
    bytes: Vec<u8>,
}

fn planned_read_range(
    current: (u64, u64),
    candidates: &mut [(u64, u64)],
    budget: usize,
) -> Option<(u64, u64)> {
    let (start, size) = current;
    if size < 5 || size > budget as u64 {
        return None;
    }
    let mut end = start.checked_add(size)?;
    let mut required = size;
    let mut count = 1;
    let candidate_count = candidates.len().min(MAX_PLANNED_BLOCKS - 1);
    let candidates = &mut candidates[..candidate_count];
    candidates.sort_unstable();
    for &(offset, size) in candidates.iter() {
        // Sorting in the caller-owned workspace avoids a second allocation.
        // Accepted blocks advance end, so duplicate ranges are skipped here.
        if offset < end || size < 5 {
            continue;
        }
        let next_end = offset.checked_add(size)?;
        let next_required = required.checked_add(size)?;
        if offset - end > MAX_PLANNED_GAP_BYTES
            || next_end - start > budget as u64
            || next_end - start > next_required.saturating_mul(2)
        {
            break;
        }
        end = next_end;
        required = next_required;
        count += 1;
    }
    (count > 1).then_some((start, end))
}

pub(crate) struct QueryBlockReader {
    options: AggregateQueryOptions,
    spans: BTreeMap<(usize, u64), Span>,
    handles: BTreeMap<usize, Handle>,
    lru: BTreeMap<u64, (usize, u64)>,
    scratch: Vec<u8>,
    retained: usize,
    reusable: Option<DecodedBlock>,
    timestamp_scratch: Vec<i64>,
    streaming_workspace: crate::query::streaming_aggregate::StreamingWorkspace,
    stream_values: bool,
    clock: u64,
    planned: Option<PlannedSpan>,
}

impl QueryBlockReader {
    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.encoded_retained_bytes() + self.idle_bytes()
    }
    pub(crate) fn new(options: AggregateQueryOptions) -> Self {
        Self {
            options,
            spans: BTreeMap::new(),
            handles: BTreeMap::new(),
            lru: BTreeMap::new(),
            scratch: Vec::new(),
            retained: 0,
            reusable: None,
            timestamp_scratch: Vec::new(),
            streaming_workspace: Default::default(),
            stream_values: {
                #[cfg(any(test, feature = "benchmarking"))]
                {
                    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
                    *ENABLED.get_or_init(|| {
                        std::env::var("REEFS_EXECUTION_BENCH_STREAM")
                            .is_ok_and(|value| value == "on")
                    })
                }
                #[cfg(not(any(test, feature = "benchmarking")))]
                {
                    false
                }
            },
            clock: 0,
            planned: None,
        }
    }

    fn encoded_retained_bytes(&self) -> usize {
        self.retained + self.planned.as_ref().map_or(0, |span| span.bytes.capacity())
    }

    pub(crate) fn has_planned_block(&self, reader: &Arc<VLogReader>, offset: u64, size: u64) -> bool {
        self.planned.as_ref().is_some_and(|span| {
            Arc::ptr_eq(&span.reader, reader)
                && offset >= span.offset
                && offset.checked_add(size).is_some_and(|end| {
                    end <= span.offset + span.bytes.len() as u64
                })
        })
    }

    fn prefetch_budget(&self) -> usize {
        self.options
            .read_cache_bytes
            .saturating_sub(self.retained)
            .saturating_sub(self.idle_bytes())
            .min(MAX_PLANNED_READ_BYTES)
    }

    /// Avoid inspecting the frontier when even this block cannot be prefetched.
    pub(crate) fn can_prefetch_blocks(
        &self,
        reader: &Arc<VLogReader>,
        current: (u64, u64),
    ) -> bool {
        self.options.read_ahead_bytes == 0
            && !self.has_planned_block(reader, current.0, current.1)
            && current.1 >= 5
            && current.1 <= self.prefetch_budget() as u64
            && current.0.checked_add(current.1).is_some()
    }

    /// Read only a bounded run containing this block and known required successors.
    pub(crate) fn prefetch_blocks(
        &mut self,
        reader: &Arc<VLogReader>,
        current: (u64, u64),
        candidates: &mut [(u64, u64)],
    ) -> Result<()> {
        let planning_started = slow_trace::now();
        if self.options.read_ahead_bytes != 0 || self.has_planned_block(reader, current.0, current.1) {
            slow_trace::log_query_stage(
                "query_vlog_prefetch_plan",
                planning_started,
                "skipped before range planning",
            );
            slow_trace::profile_work(
                "query_vlog_prefetch_plan",
                if self.options.read_ahead_bytes != 0 { "read ahead enabled" } else { "already planned" },
                1,
            );
            return Ok(());
        }
        let budget = self.prefetch_budget();
        let Some((start, end)) = planned_read_range(current, candidates, budget) else {
            slow_trace::log_query_stage(
                "query_vlog_prefetch_plan",
                planning_started,
                "no coalesced range",
            );
            slow_trace::profile_work(
                "query_vlog_prefetch_plan",
                if current.1 < 5 { "invalid block size" }
                else if current.1 > budget as u64 { "insufficient byte budget" }
                else if current.0.checked_add(current.1).is_none() { "block range overflow" }
                else { "no compatible successors" },
                1,
            );
            return Ok(());
        };
        slow_trace::log_query_stage(
            "query_vlog_prefetch_plan",
            planning_started,
            "coalesced range accepted",
        );
        slow_trace::profile_work("query_vlog_prefetch_plan", "coalesced range accepted", 1);
        slow_trace::profile_work("query_vlog_prefetch_plan", "planned span bytes", end - start);
        let started = slow_trace::now();
        let (fd, file_size) = self.file_handle(reader)?;
        if end > file_size {
            return Err(Error::StringErr { reason: "planned VLog read exceeds file length".into() });
        }
        let mut bytes = self.planned.take().map_or_else(Vec::new, |span| span.bytes);
        if bytes.capacity() > budget {
            bytes = Vec::new();
        }
        bytes.clear();
        bytes.reserve_exact((end - start) as usize);
        bytes.resize((end - start) as usize, 0);
        super::index::reader::pread(&fd, start, &mut bytes)?;
        // Logical demand is counted when each requested block is consumed.
        slow_trace::profile_vlog_io(0, bytes.len() as u64, 1, false);
        slow_trace::log_query_stage("query_vlog_batch_read", started, "planned immutable block run");
        self.planned = Some(PlannedSpan { reader: Arc::clone(reader), offset: start, bytes });
        Ok(())
    }

    fn file_handle(&mut self, reader: &Arc<VLogReader>) -> Result<(Arc<std::fs::File>, u64)> {
        let identity = Arc::as_ptr(reader) as usize;
        if let Some(handle) = self.handles.get_mut(&identity) {
            handle.used = self.clock;
            return Ok((Arc::clone(&handle.fd), handle.size));
        }
        let (fd, size) = reader.aggregate_file_handle()?;
        let fd = Arc::new(fd);
        if self.options.max_open_files > 0 {
            if self.handles.len() >= self.options.max_open_files {
                let victim = *self.handles.iter().min_by_key(|(_, h)| h.used).unwrap().0;
                self.handles.remove(&victim);
            }
            self.handles.insert(identity, Handle {
                _reader: Arc::clone(reader), fd: Arc::clone(&fd), size, used: self.clock,
            });
        }
        Ok((fd, size))
    }

    pub(crate) fn read(
        &mut self,
        reader: &Arc<VLogReader>,
        offset: u64,
        size: u64,
        typ: FieldType,
    ) -> Result<DecodedBlock> {
        self.with_block(reader, offset, size, |bytes, reusable| {
            decode_reusing(bytes, typ, reusable)
        })
    }

    /// Values remain local to the decoder; only validated block states are committed.
    pub(crate) fn reduce_numeric(
        &mut self,
        reader: &Arc<VLogReader>,
        offset: u64,
        size: u64,
        typ: FieldType,
        windows: crate::query::aggregate_windows::AggregateWindows<'_>,
        states: &mut [crate::query::projected_aggregate::ProjectedAggregateState],
        expected: u64,
        start: i64,
        end: i64,
    ) -> Result<bool> {
        if !self.stream_values {
            return Ok(false);
        }
        if !matches!(typ, FLOAT | INTEGER | UNSIGNED)
            || windows.intersecting(start, end).count()
                > crate::query::streaming_aggregate::MAX_STREAM_WINDOWS
        {
            return Ok(false);
        }
        let projection = states
            .first()
            .ok_or_else(|| Error::StringErr {
                reason: "streaming states missing".into(),
            })?
            .projection;
        let mask = u8::from(projection.sum)
            | (u8::from(projection.min) << 1)
            | (u8::from(projection.max) << 2);
        let mut times = std::mem::take(&mut self.timestamp_scratch);
        let mut workspace = std::mem::take(&mut self.streaming_workspace);
        let result = self.with_block(reader, offset, size, |bytes, _| {
            macro_rules! reduce {
                ($mask:expr) => {
                    decode_reduce_reusing::<$mask>(
                        bytes,
                        &mut times,
                        typ,
                        windows,
                        states,
                        expected,
                        start,
                        end,
                        &mut workspace,
                    )
                };
            }
            match mask {
                0 => reduce!(0),
                1 => reduce!(1),
                2 => reduce!(2),
                3 => reduce!(3),
                4 => reduce!(4),
                5 => reduce!(5),
                6 => reduce!(6),
                7 => reduce!(7),
                _ => unreachable!(),
            }
        });
        let available = self
            .options
            .read_cache_bytes
            .saturating_sub(self.encoded_retained_bytes())
            .saturating_sub(self.idle_bytes());
        if workspace.bytes() <= available {
            self.streaming_workspace = workspace;
        }
        self.recycle_timestamps(times);
        result.map(|_| true)
    }

    pub(crate) fn read_timestamps(
        &mut self,
        reader: &Arc<VLogReader>,
        offset: u64,
        size: u64,
        typ: FieldType,
    ) -> Result<Vec<i64>> {
        let mut times = std::mem::take(&mut self.timestamp_scratch);
        self.with_block(reader, offset, size, |bytes, _| {
            let started = slow_trace::now();
            if bytes.len() < 5
                || crc32fast::hash(&bytes[4..])
                    != u32::from_be_bytes(bytes[..4].try_into().unwrap())
            {
                return Err(Error::StringErr {
                    reason: "VLog timestamp block checksum mismatch".into(),
                });
            }
            slow_trace::log_query_stage(
                "query_block_checksum",
                started,
                "timestamp-only VLog block",
            );
            let started = slow_trace::now();
            let block_type = match typ {
                FLOAT => encoding::BLOCK_FLOAT64,
                INTEGER => encoding::BLOCK_INTEGER,
                UNSIGNED => encoding::BLOCK_UNSIGNED,
                crate::models::BOOL => encoding::BLOCK_BOOL,
                crate::models::STRING => encoding::BLOCK_STRING,
                _ => {
                    return Err(Error::StringErr {
                        reason: "unsupported timestamp-only field type".into(),
                    });
                }
            };
            array_encoding::decode_timestamp_array_block_into(&bytes[4..], block_type, &mut times)
                .map_err(|source| Error::ArrayEncoding { source })?;
            slow_trace::log_query_stage(
                "query_timestamp_only_decode",
                started,
                "count boundary block",
            );
            Ok(())
        })?;
        Ok(times)
    }

    fn idle_bytes(&self) -> usize {
        self.timestamp_scratch.capacity() * std::mem::size_of::<i64>()
            + self.streaming_workspace.bytes()
            + self
                .reusable
                .as_ref()
                .and_then(DecodedBlock::buffer_bytes)
                .unwrap_or(0)
    }

    pub(crate) fn recycle_timestamps(&mut self, times: Vec<i64>) {
        let available = self
            .options
            .read_cache_bytes
            .saturating_sub(self.encoded_retained_bytes())
            .saturating_sub(
                self.reusable
                    .as_ref()
                    .and_then(DecodedBlock::buffer_bytes)
                    .unwrap_or(0),
            )
            .saturating_sub(self.streaming_workspace.bytes());
        self.timestamp_scratch = if times.capacity() * std::mem::size_of::<i64>() <= available {
            times
        } else {
            Vec::new()
        };
    }

    fn with_block<T>(
        &mut self,
        reader: &Arc<VLogReader>,
        offset: u64,
        size: u64,
        apply: impl FnOnce(&[u8], &mut Option<DecodedBlock>) -> Result<T>,
    ) -> Result<T> {
        if size < 5 || offset.checked_add(size).is_none() {
            return Err(Error::StringErr {
                reason: "invalid VLog block range".into(),
            });
        }
        let started = slow_trace::now();
        self.clock = self.clock.wrapping_add(1);
        if self.has_planned_block(reader, offset, size) {
            let span = self.planned.as_ref().unwrap();
            let begin = (offset - span.offset) as usize;
            slow_trace::profile_vlog_io(size, 0, 0, true);
            slow_trace::log_query_stage("query_vlog_lookup", started, "planned block cache hit");
            return apply(&span.bytes[begin..begin + size as usize], &mut self.reusable);
        }
        let ahead = self
            .options
            .read_ahead_bytes
            .min(self.options.read_cache_bytes);
        let base = if ahead > 0 {
            offset / ahead as u64 * ahead as u64
        } else {
            offset
        };
        let key = (Arc::as_ptr(reader) as usize, base);
        let begin = (offset - base) as usize;
        let length = usize::try_from(size).map_err(|_| Error::StringErr {
            reason: "VLog block size exceeds address space".into(),
        })?;
        if let Some(span) = self.spans.get_mut(&key)
            && begin
                .checked_add(length)
                .is_some_and(|end| end <= span.bytes.len())
        {
            self.lru.remove(&span.used);
            span.used = self.clock;
            self.lru.insert(self.clock, key);
            slow_trace::profile_vlog_io(size, 0, 0, true);
            slow_trace::log_query_stage("query_vlog_lookup", started, "checked VLog block IO");
            return apply(&span.bytes[begin..begin + length], &mut self.reusable);
        }
        let (fd, file_size) = self.file_handle(reader)?;
        let end = offset + size;
        if end > file_size {
            return Err(Error::StringErr {
                reason: "VLog block exceeds file length".into(),
            });
        }
        let read_len = (file_size - base).min((ahead as u64).max(end - base)) as usize;
        self.scratch.resize(read_len, 0);
        super::index::reader::pread(&fd, base, &mut self.scratch)?;
        slow_trace::profile_vlog_io(size, read_len as u64, 1, false);
        slow_trace::log_query_stage("query_vlog_lookup", started, "checked VLog block IO");
        let result = apply(&self.scratch[begin..begin + length], &mut self.reusable)?;
        let footprint = self.scratch.capacity();
        let cache_budget = self
            .options
            .read_cache_bytes
            .saturating_sub(self.idle_bytes())
            .saturating_sub(self.planned.as_ref().map_or(0, |span| span.bytes.capacity()));
        if ahead > 0 && footprint <= cache_budget {
            if let Some(old) = self.spans.remove(&key) {
                self.retained -= old.bytes.capacity();
                self.lru.remove(&old.used);
            }
            while self.retained > cache_budget - footprint {
                let (_, victim) = self.lru.pop_first().unwrap();
                self.retained -= self.spans.remove(&victim).unwrap().bytes.capacity();
            }
            self.retained += footprint;
            self.spans.insert(
                key,
                Span {
                    _reader: Arc::clone(reader),
                    bytes: std::mem::take(&mut self.scratch),
                    used: self.clock,
                },
            );
            self.lru.insert(self.clock, key);
        }
        Ok(result)
    }

    /// Retains at most one numeric block's buffers within the shared byte allowance.
    pub(crate) fn recycle(&mut self, block: DecodedBlock) {
        self.reusable = if block.buffer_bytes().is_some_and(|bytes| {
            bytes
                <= self
                    .options
                    .read_cache_bytes
                    .saturating_sub(self.encoded_retained_bytes())
                    .saturating_sub(self.timestamp_scratch.capacity() * std::mem::size_of::<i64>())
                    .saturating_sub(self.streaming_workspace.bytes())
        }) {
            Some(block)
        } else {
            None
        };
    }
}

#[cfg(test)]
#[path = "streaming_tests.rs"]
mod streaming_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tsm1::data_encoding::encoding_values::Values;

    #[test]
    fn planned_ranges_bound_gaps_overread_and_candidate_memory() {
        assert_eq!(planned_read_range((100, 10), &mut [(120, 10), (100, 10)], 64), Some((100, 130)));
        assert_eq!(planned_read_range((100, 10), &mut [(1000, 10)], 2048), None);
        assert_eq!(planned_read_range((100, 10), &mut [(110, 10)], 19), None);
        assert_eq!(planned_read_range((100, 10), &mut [(110, 10)], 0), None);
        assert_eq!(planned_read_range((u64::MAX - 5, 10), &mut [], 1024), None);
        assert_eq!(planned_read_range((100, 10), &mut [(120, 10), (110, 10), (110, 10)], 64), Some((100, 130)));
        let mut candidates = [(1000, 10); MAX_PLANNED_BLOCKS];
        candidates[MAX_PLANNED_BLOCKS - 1] = (110, 10);
        assert_eq!(planned_read_range((100, 10), &mut candidates, 2048), None);
        assert_eq!(candidates[MAX_PLANNED_BLOCKS - 1], (110, 10));
    }

    #[test]
    fn planned_block_reads_preserve_values_checksums_and_retention_budget() {
        use crate::storage::tsm1::vlog::writer::VLogWriter;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("000000001-000000001.vlog");
        let fd = std::fs::OpenOptions::new().write(true).create_new(true).open(&path).unwrap();
        let mut writer = VLogWriter::new(fd, 1, 1, &path);
        let mut blocks = Vec::new();
        for index in 0..3 {
            let values = vec![Value::FloatValue(FloatValue::new(index * 10, index as f64 / 3.0))];
            let (offset, size) = writer.write_block(b"cpu#!~#f", index * 10, index * 10,
                                                   &Values::encode(&values).unwrap()).unwrap();
            blocks.push((offset as u64, size as u64));
        }
        writer.close().unwrap();
        let file = Arc::new(VLogReader::new_bare(&path, 1, 1).unwrap());
        for budget in [0, 4096] {
            let mut reader = QueryBlockReader::new(AggregateQueryOptions {
                read_cache_bytes: budget, ..Default::default()
            });
            assert_eq!(reader.can_prefetch_blocks(&file, blocks[0]), budget != 0);
            reader.prefetch_blocks(&file, blocks[0], &mut blocks[1..].to_vec()).unwrap();
            assert_eq!(reader.has_planned_block(&file, blocks[1].0, blocks[1].1), budget != 0);
            assert!(!reader.can_prefetch_blocks(&file, blocks[0]));
            for (index, &(offset, size)) in blocks.iter().enumerate() {
                let decoded = reader.read(&file, offset, size, FLOAT).unwrap();
                assert_eq!(decoded.into_values().collect::<Vec<_>>(),
                           vec![Value::FloatValue(FloatValue::new(index as i64 * 10, index as f64 / 3.0))]);
            }
            assert!(reader.retained_bytes() <= budget);
        }
        let mut read_ahead = QueryBlockReader::new(AggregateQueryOptions {
            read_cache_bytes: 4096,
            read_ahead_bytes: 64,
            ..Default::default()
        });
        assert!(!read_ahead.can_prefetch_blocks(&file, blocks[0]));
        read_ahead.prefetch_blocks(&file, blocks[0], &mut blocks[1..].to_vec()).unwrap();
        assert!(!read_ahead.has_planned_block(&file, blocks[1].0, blocks[1].1));
        assert_eq!(read_ahead.read(&file, blocks[0].0, blocks[0].1, FLOAT).unwrap().len(), 1);
        let bounded = QueryBlockReader::new(AggregateQueryOptions {
            read_cache_bytes: blocks[0].1 as usize - 1,
            ..Default::default()
        });
        assert!(!bounded.can_prefetch_blocks(&file, blocks[0]));
        assert!(!bounded.can_prefetch_blocks(&file, (u64::MAX - 4, 10)));
        assert!(!bounded.can_prefetch_blocks(&file, (blocks[0].0, 4)));
        use std::io::{Seek, SeekFrom, Write};
        let mut fd = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        fd.seek(SeekFrom::Start(blocks[1].0)).unwrap();
        fd.write_all(&[255; 4]).unwrap();
        let mut reader = QueryBlockReader::new(Default::default());
        reader.prefetch_blocks(&file, blocks[0], &mut blocks[1..].to_vec()).unwrap();
        assert!(reader.read(&file, blocks[1].0, blocks[1].1, FLOAT).is_err());
    }

    fn encoded(values: &[Value]) -> Vec<u8> {
        let payload = Values::encode(values).unwrap();
        let mut bytes = crc32fast::hash(&payload).to_be_bytes().to_vec();
        bytes.extend(payload);
        bytes
    }

    #[test]
    fn timestamp_reads_validate_type_crc_and_share_recycled_buffer_budget() {
        use crate::storage::tsm1::vlog::writer::VLogWriter;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("000000001-000000001.vlog");
        let fd = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let mut writer = VLogWriter::new(fd, 1, 1, &path);
        let values: Vec<_> = (0..16)
            .map(|t| Value::IntegerValue(IntegerValue::new(t, t)))
            .collect();
        let (offset, size) = writer
            .write_block(b"cpu#!~#i", 0, 15, &Values::encode(&values).unwrap())
            .unwrap();
        writer.close().unwrap();
        let file = Arc::new(VLogReader::new_bare(&path, 1, 1).unwrap());
        for budget in [0, 64, 4096] {
            let mut reader = QueryBlockReader::new(AggregateQueryOptions {
                read_cache_bytes: budget,
                read_ahead_bytes: 64,
                ..Default::default()
            });
            for _ in 0..3 {
                let block = reader
                    .read(&file, offset as u64, size as u64, INTEGER)
                    .unwrap();
                reader.recycle(block);
                let times = reader
                    .read_timestamps(&file, offset as u64, size as u64, INTEGER)
                    .unwrap();
                assert_eq!(times, (0..16).collect::<Vec<_>>());
                reader.recycle_timestamps(times);
                assert!(reader.retained_bytes() <= budget);
            }
            assert!(
                reader
                    .read_timestamps(&file, offset as u64, size as u64, FLOAT)
                    .is_err()
            );
        }
        use std::io::{Seek, SeekFrom, Write};
        let mut fd = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        fd.seek(SeekFrom::Start(offset as u64)).unwrap();
        fd.write_all(&[255; 4]).unwrap();
        let mut reader = QueryBlockReader::new(Default::default());
        assert!(
            reader
                .read_timestamps(&file, offset as u64, size as u64, INTEGER)
                .is_err()
        );
    }

    #[test]
    fn numeric_decode_reuses_buffers_without_crossing_the_read_budget() {
        for typ in [FLOAT, INTEGER, UNSIGNED] {
            let mut reader = QueryBlockReader::new(AggregateQueryOptions {
                read_cache_bytes: 4096,
                ..Default::default()
            });
            let mut previous = None;
            for count in [16, 8, 12] {
                let values = (0..count)
                    .map(|i| match typ {
                        FLOAT => Value::FloatValue(FloatValue::new(i * 10, i as f64 / 3.0)),
                        INTEGER => Value::IntegerValue(IntegerValue::new(i * 10, i - 8)),
                        UNSIGNED => {
                            Value::UnsignedValue(UnsignedValue::new(i * 10, u64::MAX - i as u64))
                        }
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();
                let bytes = encoded(&values);
                let block = decode_reusing(&bytes, typ, &mut reader.reusable).unwrap();
                assert!(block.valid_range(count as u64, 0, (count - 1) * 10, typ));
                let pointers = match &block {
                    DecodedBlock::Float(a) => {
                        assert_eq!(
                            a.values,
                            (0..count).map(|i| i as f64 / 3.0).collect::<Vec<_>>()
                        );
                        (a.timestamps.as_ptr() as usize, a.values.as_ptr() as usize)
                    }
                    DecodedBlock::Integer(a) => {
                        assert_eq!(a.values, (0..count).map(|i| i - 8).collect::<Vec<_>>());
                        (a.timestamps.as_ptr() as usize, a.values.as_ptr() as usize)
                    }
                    DecodedBlock::Unsigned(a) => {
                        assert_eq!(
                            a.values,
                            (0..count).map(|i| u64::MAX - i as u64).collect::<Vec<_>>()
                        );
                        (a.timestamps.as_ptr() as usize, a.values.as_ptr() as usize)
                    }
                    _ => unreachable!(),
                };
                if let Some(previous) = previous {
                    assert_eq!(pointers, previous);
                }
                previous = Some(pointers);
                reader.recycle(block);
                assert!(reader.retained_bytes() <= 4096);
            }
            reader.options.read_cache_bytes = 1;
            let block = reader.reusable.take().unwrap();
            reader.recycle(block);
            assert!(reader.reusable.is_none());
            assert_eq!(reader.retained_bytes(), 0);
        }
    }

    #[test]
    fn typed_blocks_preserve_numeric_values_and_validate_crc_and_bounds() {
        for values in [
            vec![
                Value::IntegerValue(IntegerValue::new(-2, i64::MIN)),
                Value::IntegerValue(IntegerValue::new(9, i64::MAX)),
            ],
            vec![
                Value::UnsignedValue(UnsignedValue::new(-2, 0)),
                Value::UnsignedValue(UnsignedValue::new(9, u64::MAX)),
            ],
            vec![
                Value::FloatValue(FloatValue::new(-2, -0.0)),
                Value::FloatValue(FloatValue::new(9, 1.5)),
            ],
        ] {
            let typ = values[0].value_type();
            let payload = Values::encode(&values).unwrap();
            let mut bytes = crc32fast::hash(&payload).to_be_bytes().to_vec();
            bytes.extend(payload);
            let block = decode(&bytes, typ).unwrap();
            assert!(block.valid_range(2, -2, 9, typ));
            assert!(!block.valid_range(1, -2, 9, typ));
            assert!(!block.valid_range(2, -1, 9, typ));
            assert_eq!(block.into_values().collect::<Vec<_>>(), values);
            bytes[4] ^= 1;
            assert!(decode(&bytes, typ).is_err());
        }
        assert!(decode(&[0; 4], FLOAT).is_err());
    }
}
