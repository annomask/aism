//! Ordered raw scans with one decoded group and block per input index.

use super::{
    LEVEL_BLOCK, Position, Result, SeqNo, invalid,
    page_group::{Column, ColumnValue, SelectedRecord},
    reader::{GroupCursor, IndexFile},
    store::IndexStore,
};
use crate::storage::tsm1::vlog::reader::VLogReader;
use crate::{
    common::slow_trace,
    models::{FieldType, points::Value},
};
use std::{
    collections::{BTreeMap, BinaryHeap},
    ops::Deref,
    sync::Arc,
    vec::IntoIter,
};

const OUTPUT_VALUES: usize = 4000;

struct Source<F> {
    deletes: super::tombstone::TombstoneView,
    groups: GroupCursor<F>,
    next_group: Option<usize>,
    records: IntoIter<SelectedRecord>,
    values: IntoIter<Value>,
    head: Option<Value>,
    version: u32,
    #[cfg(test)]
    peak_records: usize,
    #[cfg(test)]
    peak_values: usize,
    decoded_blocks: usize,
}

/// All handles come from one publication snapshot. No file-list lock is held
/// while reading groups or blocks, including after compaction replaces inputs.
pub(crate) struct RawCursor<F = Arc<IndexFile>> {
    sources: Vec<Source<F>>,
    readers: BTreeMap<u32, Arc<VLogReader>>,
    heap: BinaryHeap<(i128, usize)>,
    typ: FieldType,
    min: i64,
    max: i64,
    ascending: bool,
    initialized: bool,
    batch: Option<Vec<Value>>,
    failed: bool,
    native: Option<super::store::native::NativeRead>,
}

impl IndexStore {
    pub(crate) fn raw_cursor(
        &self,
        seq: SeqNo,
        typ: FieldType,
        min: i64,
        max: i64,
        ascending: bool,
    ) -> Result<RawCursor> {
        let started = slow_trace::now();
        let (files, readers, native) = self.native_query_view(seq)?;
        let mut cursor = RawCursor {
            sources: Vec::new(),
            readers: readers
                .into_iter()
                .map(|f| (((f.generation as u32) << 8) | f.seq as u32, f))
                .collect(),
            heap: BinaryHeap::new(),
            typ,
            min,
            max,
            ascending,
            initialized: false,
            batch: None,
            failed: false,
            native,
        };
        if min <= max {
            for file in files {
                let Some(entry) = file.lookup(seq)? else {
                    continue;
                };
                let header = file.segment_header(&entry)?;
                if entry.typ != typ || entry.min_start > max || entry.max_end < min {
                    continue;
                }
                if header.level(LEVEL_BLOCK).is_none() {
                    continue;
                }
                let mut groups = file.owned_groups(&entry, &header, LEVEL_BLOCK)?;
                let next_group = Some(groups.predecessor(if ascending { min } else { max })?);
                cursor.sources.push(Source {
                    deletes: cursor.native.as_ref().map_or_else(|| file.tombstone_view(seq), |n| n.tombstones(&file, seq)),
                    groups,
                    next_group,
                    records: Vec::new().into_iter(),
                    values: Vec::new().into_iter(),
                    head: None,
                    version: 0,
                    #[cfg(test)]
                    peak_records: 0,
                    #[cfg(test)]
                    peak_values: 0,
                    decoded_blocks: 0,
                });
            }
        }
        slow_trace::log_query_stage(
            "query_index_lookup",
            started,
            "streaming raw cursor directory",
        );
        Ok(cursor)
    }
}

impl<F: Deref<Target = IndexFile>> Source<F> {
    fn next_record(
        &mut self,
        min: i64,
        max: i64,
        ascending: bool,
    ) -> Result<Option<SelectedRecord>> {
        loop {
            let record = if ascending {
                self.records.next()
            } else {
                self.records.next_back()
            };
            if let Some(record) = record {
                if record.pos.start <= max && record.pos.end >= min {
                    return Ok(Some(record));
                }
                continue;
            }
            self.records = Vec::new().into_iter();
            let Some(group) = self.next_group else {
                return Ok(None);
            };
            let entry = self.groups.entry(group)?;
            if (ascending && entry.first_start > max) || (!ascending && entry.last_end < min) {
                self.next_group = None;
                return Ok(None);
            }
            self.next_group = if ascending {
                (group + 1 < self.groups.len()).then_some(group + 1)
            } else {
                group.checked_sub(1)
            };
            if entry.first_start > max || entry.last_end < min {
                continue;
            }
            let started = slow_trace::now();
            let records = self.groups.read_selected(group, &[Column::Count])?;
            slow_trace::log_query_stage("query_index_lookup", started, "streaming raw leaf group");
            #[cfg(test)]
            {
                self.peak_records = self.peak_records.max(records.len());
            }
            self.records = records.into_iter();
        }
    }

    fn advance(
        &mut self,
        readers: &BTreeMap<u32, Arc<VLogReader>>,
        typ: FieldType,
        min: i64,
        max: i64,
        ascending: bool,
        cancelled: &impl Fn() -> bool,
        native: Option<&super::store::native::NativeRead>,
    ) -> Result<()> {
        self.head = None;
        loop {
            let value = if ascending {
                self.values.next()
            } else {
                self.values.next_back()
            };
            if let Some(value) = value {
                let time = value.unix_nano();
                if time >= min && time <= max && !self.deletes.removes(time)? {
                    self.head = Some(value);
                    return Ok(());
                }
                continue;
            }
            self.values = Vec::new().into_iter();
            super::compact::check_cancelled(cancelled)?;
            let Some(record) = self.next_record(min, max, ascending)? else {
                return Ok(());
            };
            if self.deletes.coverage(record.pos.start, record.pos.end)?.1 {
                continue;
            }
            let Some(ColumnValue::Count(count)) = record.values[Column::Count as usize] else {
                return Err(invalid("raw block count missing"));
            };
            let Position {
                block, block_size, ..
            } = record.pos;
            let reader = readers
                .get(&block.file_no)
                .ok_or(invalid("raw index references missing VLog"))?;
            if count == 0
                || block.offset < 5
                || block_size < 5
                || u64::from(block.offset) + u64::from(block_size) > reader.size()
            {
                return Err(invalid("raw index references invalid VLog block"));
            }
            let started = slow_trace::now();
            let values = reader
                .read_checked_block(u64::from(block.offset), u64::from(block_size))
                .map_err(|source| record.pos.read_error("raw value decode", source))?;
            if values.len() as u64 != count
                || values
                    .first()
                    .is_none_or(|v| v.unix_nano() != record.pos.start)
                || values
                    .last()
                    .is_none_or(|v| v.unix_nano() != record.pos.end)
                || values
                    .windows(2)
                    .any(|p| p[0].unix_nano() >= p[1].unix_nano())
                || values.iter().any(|v| v.value_type() != typ)
            {
                return Err(invalid("raw VLog block does not match index"));
            }
            slow_trace::log_query_stage("query_block_decode", started, "streaming raw block");
            #[cfg(test)]
            {
                self.peak_values = self.peak_values.max(values.len());
            }
            self.decoded_blocks += 1;
            self.version = record.pos.write_version();
            self.values = values.into_iter();
        }
    }
}

impl<'a> RawCursor<&'a IndexFile> {
    pub(super) fn from_segments(
        inputs: &[super::compact::SegmentSource<'a>],
        readers: &[Arc<VLogReader>],
    ) -> Result<Self> {
        let first = inputs.first().ok_or(invalid("empty repair sources"))?;
        let mut sources = Vec::new();
        for input in inputs {
            if input.header.level(LEVEL_BLOCK).is_none() {
                continue;
            }
            sources.push(Source {
                deletes: input.file.tombstone_view(input.entry.seq_no),
                groups: input
                    .file
                    .groups(&input.entry, &input.header, LEVEL_BLOCK)?,
                next_group: Some(0),
                records: Vec::new().into_iter(),
                values: Vec::new().into_iter(),
                head: None,
                version: 0,
                #[cfg(test)]
                peak_records: 0,
                #[cfg(test)]
                peak_values: 0,
                decoded_blocks: 0,
            });
        }
        Ok(Self {
            sources,
            readers: readers
                .iter()
                .map(|f| (((f.generation as u32) << 8) | f.seq as u32, Arc::clone(f)))
                .collect(),
            heap: BinaryHeap::new(),
            typ: first.entry.typ,
            min: i64::MIN,
            max: i64::MAX,
            ascending: true,
            initialized: false,
            batch: None,
            failed: false,
            native: None,
        })
    }
}

impl<F: Deref<Target = IndexFile>> RawCursor<F> {
    fn advance(&mut self, index: usize, cancelled: &impl Fn() -> bool) -> Result<()> {
        let source = &mut self.sources[index];
        source.advance(
            &self.readers,
            self.typ,
            self.min,
            self.max,
            self.ascending,
            cancelled,
            self.native.as_ref(),
        )?;
        if let Some(head) = &source.head {
            // i128 also handles negating the minimum i64 timestamp.
            let time = i128::from(head.unix_nano());
            self.heap
                .push((if self.ascending { -time } else { time }, index));
        }
        Ok(())
    }

    pub(super) fn next_versioned(
        &mut self,
        cancelled: &impl Fn() -> bool,
    ) -> Result<Option<(u32, Value)>> {
        if !self.initialized {
            for index in 0..self.sources.len() {
                self.advance(index, cancelled)?;
            }
            self.initialized = true;
        }
        let Some((time, index)) = self.heap.pop() else {
            return Ok(None);
        };
        let source = &mut self.sources[index];
        let mut value = source
            .head
            .take()
            .ok_or(invalid("raw merge head missing"))?;
        let mut version = source.version;
        self.advance(index, cancelled)?;
        while self.heap.peek().is_some_and(|(next, _)| *next == time) {
            let (_, index) = self.heap.pop().unwrap();
            let source = &mut self.sources[index];
            let candidate = source
                .head
                .take()
                .ok_or(invalid("raw merge head missing"))?;
            if source.version >= version {
                value = candidate;
                version = source.version;
            }
            self.advance(index, cancelled)?;
        }
        Ok(Some((version, value)))
    }

    pub(super) fn decoded_blocks(&self) -> usize {
        self.sources.iter().map(|s| s.decoded_blocks).sum()
    }

    fn fill_batch(&mut self) -> Result<Vec<Value>> {
        let mut batch = Vec::with_capacity(OUTPUT_VALUES);
        while batch.len() < OUTPUT_VALUES {
            let Some((_, value)) = self.next_versioned(&|| false)? else {
                break;
            };
            batch.push(value);
        }
        // Typed descending cursors consume each returned batch from the end.
        if !self.ascending {
            batch.reverse();
        }
        Ok(batch)
    }

    pub(crate) fn read_batch(&mut self) -> Result<&[Value]> {
        if self.failed {
            return Err(invalid("raw cursor is failed"));
        }
        if self.batch.is_none() {
            let started = slow_trace::now();
            let result = self.fill_batch();
            slow_trace::log_query_stage("query_vlog_lookup", started, "streaming raw batch");
            match result {
                Ok(batch) => self.batch = Some(batch),
                Err(error) => {
                    self.failed = true;
                    return Err(error);
                }
            }
        }
        Ok(self.batch.as_ref().unwrap())
    }

    pub(crate) fn next(&mut self) {
        self.batch = None;
    }
    pub(crate) fn has_values(&mut self, cancelled: &impl Fn() -> bool) -> Result<bool> {
        Ok(self.next_versioned(cancelled)?.is_some())
    }

    #[cfg(test)]
    pub(crate) fn buffer_metrics(&self) -> (usize, usize, usize) {
        (
            self.sources.iter().map(|s| s.peak_records).sum(),
            self.sources.iter().map(|s| s.peak_values).sum(),
            self.sources.iter().map(|s| s.decoded_blocks).sum(),
        )
    }
}
