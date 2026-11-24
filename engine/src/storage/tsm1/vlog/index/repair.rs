//! Repair conflicting leaves while retaining complete unaffected subtrees.

use super::allocation::{OutputGenerations, OutputIds};
use super::compact::{
    Fragment, MergeStats, SegmentSource, check_cancelled, merge_fragment_outputs,
};
use super::output::{FieldTreeBuilder, IndexOutput};
use super::page_group::{ALL_COLUMNS, SelectedRecord};
use super::reader::IndexFile;
use super::tree::Forest;
use super::{AggValues, Record, Result, SeqNo, invalid};
use crate::models::points::Value;
use crate::storage::tsm1::{
    data_encoding::encoding_values::Values,
    vlog::{VLogIndex, reader::VLogReader, writer::VLogWriter},
};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

pub(super) type DataOutput = (PathBuf, i64, i64);
// Large scattered conflicts use a streaming field rebuild, never an unbounded
// fragment plan. Ordered joins do not enter this fallback.
pub(super) const MAX_LOCAL_REPAIR_RECORDS: usize = 512;
pub(super) struct RepairWriter {
    budget: super::io_budget::IoBudget,
    dir: PathBuf,
    identities: OutputIds,
    writer: Option<VLogWriter<File>>,
    paths: Vec<DataOutput>,
    keep: bool,
}
impl RepairWriter {
    pub(super) fn new(dir: &Path, generations: OutputGenerations) -> Self {
        Self {
            budget: Default::default(),
            dir: dir.into(),
            identities: OutputIds::new(generations, 0),
            writer: None,
            paths: Vec::new(),
            keep: false,
        }
    }
    pub(super) fn set_io_budget(&mut self, budget: super::io_budget::IoBudget) {
        self.budget = budget;
    }
    fn write(&mut self, seq: SeqNo, version: u32, values: &[Value]) -> Result<Record> {
        let bytes = Values::encode(values).map_err(|_| invalid("repair block encoding failed"))?;
        let min = values[0].unix_nano();
        let max = values.last().unwrap().unix_nano();
        let (block, block_size) = self.append(seq, min, max, &bytes)?;
        let mut record = Record::from_vlog_index(
            seq,
            &VLogIndex {
                aggregate: None,
                count: values.len() as i32,
                typ: values[0].value_type(),
                min,
                max,
                generation: block.generation(),
                seq: block.seq(),
                offset: i64::from(block.offset),
                size: i64::from(block_size),
            },
            AggValues::from_block(values)?,
        )?;
        record.pos.version = version;
        Ok(record)
    }
    pub(super) fn relocate(&mut self, mut record: Record, bytes: &[u8]) -> Result<Record> {
        let version = record.pos.write_version();
        let (block, size) = self.append(record.seq_no, record.pos.start, record.pos.end, bytes)?;
        record.pos.block = block;
        record.pos.block_size = size;
        record.pos.version = version;
        Ok(record)
    }
    fn append(
        &mut self,
        seq: SeqNo,
        min: i64,
        max: i64,
        bytes: &[u8],
    ) -> Result<(super::BlockLoc, u32)> {
        self.budget.charge(bytes.len() + 4)?;
        if self
            .writer
            .as_ref()
            .is_some_and(|w| w.size() > 128 * 1024 * 1024)
        {
            self.writer
                .take()
                .unwrap()
                .close()
                .map_err(|_| invalid("repair VLog close failed"))?;
        }
        if self.writer.is_none() {
            let (generation, file_seq) = self.identities.next_vlog()?;
            let path = self
                .dir
                .join(format!("{generation:09}-{file_seq:09}.vlog.tmp"));
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            self.writer = Some(VLogWriter::new(file, generation, file_seq, &path));
            self.paths.push((path, generation, file_seq));
        }
        let (offset, size) = self
            .writer
            .as_mut()
            .unwrap()
            .write_block(&seq.get().to_be_bytes(), min, max, &bytes)
            .map_err(|_| invalid("repair VLog write failed"))?;
        Ok((
            super::BlockLoc::from_vlog(
                self.paths.last().unwrap().1,
                self.paths.last().unwrap().2,
                offset,
            )?,
            u32::try_from(size).map_err(|_| super::Error::InvalidBlockLocation)?,
        ))
    }
    pub(super) fn finish(mut self) -> Result<Vec<DataOutput>> {
        if let Some(mut writer) = self.writer.take() {
            writer
                .close()
                .map_err(|_| invalid("repair VLog close failed"))?;
        }
        self.keep = true;
        Ok(std::mem::take(&mut self.paths))
    }
}
impl Drop for RepairWriter {
    fn drop(&mut self) {
        self.writer.take();
        if !self.keep {
            for (path, _, _) in &self.paths {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

#[cfg(test)]
mod allocation_tests {
    use super::*;
    use crate::models::points::IntegerValue;

    #[test]
    fn repair_rolls_physical_generations_and_preserves_write_precedence() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = RepairWriter::new(dir.path(), OutputGenerations::isolated(2));
        let seq = SeqNo::new(1, 1).unwrap();
        let version = crate::storage::tsm1::vlog::index::BlockLoc::from_vlog(1, 1, 5)
            .unwrap()
            .file_no;
        let records: Vec<_> = (0..260)
            .map(|time| {
                let record = writer
                    .write(
                        seq,
                        version,
                        &[Value::IntegerValue(IntegerValue::new(time, 42))],
                    )
                    .unwrap();
                // Exercise each physical roll without writing 32 GiB of test data.
                writer.writer.take().unwrap().close().unwrap();
                record
            })
            .collect();
        let paths = writer.finish().unwrap();
        assert_eq!(paths.len(), 260);
        assert_eq!((paths[255].1, paths[255].2), (2, 255));
        assert_eq!((paths[256].1, paths[256].2), (3, 0));
        for (time, (record, (path, generation, sequence))) in records.iter().zip(paths).enumerate()
        {
            assert_eq!(record.pos.write_version(), version);
            assert_eq!(record.pos.block.generation(), generation);
            assert_eq!(record.pos.block.seq(), sequence);
            let reader = VLogReader::new_bare(&path, generation, sequence).unwrap();
            assert_eq!(
                reader
                    .read_integer_block(
                        u64::from(record.pos.block.offset),
                        u64::from(record.pos.block_size),
                    )
                    .unwrap(),
                vec![IntegerValue::new(time as i64, 42)]
            );
        }
    }
}

fn partition(
    source: usize,
    record: Record,
    inputs: &[SegmentSource<'_>],
    ranges: &super::conflict_ranges::SourceRanges,
    clean: &mut Vec<Fragment>,
    dirty: &mut Vec<(usize, Record)>,
    deletes: &[super::tombstone::TombstoneView],
    cancelled: &impl Fn() -> bool,
) -> Result<()> {
    check_cancelled(cancelled)?;
    if clean.len() + dirty.len() >= MAX_LOCAL_REPAIR_RECORDS {
        return Err(super::Error::RepairBudgetExceeded);
    }
    let (deleted, covered) = deletes[source].coverage(record.pos.start, record.pos.end)?;
    if covered {
        return Ok(());
    }
    let conflict = record.pos.flags != 0
        || deleted
        || super::conflict_ranges::overlaps(ranges, source, record.pos.start, record.pos.end);
    if !conflict {
        clean.push(Fragment {
            source,
            forest: Forest::new(vec![record])?,
        });
    } else if record.level == 1 {
        dirty.push((source, record));
    } else {
        let input = &inputs[source];
        let children = input.file.read_level(
            &input.entry,
            &input.header,
            record.level - 1,
            record.pos.start,
            record.pos.end,
            &ALL_COLUMNS,
        )?;
        if children.len() != record.pos.child_count as usize
            || children
                .first()
                .is_none_or(|r| r.pos.start != record.pos.start)
            || children.last().is_none_or(|r| r.pos.end != record.pos.end)
        {
            return Err(invalid("repair parent has incomplete children"));
        }
        for child in children {
            partition(
                source,
                child.into_record()?,
                inputs,
                ranges,
                clean,
                dirty,
                deletes,
                cancelled,
            )?;
        }
    }
    Ok(())
}

pub(super) fn merge_overlapping(
    writer: &mut IndexOutput,
    inputs: &[SegmentSource<'_>],
    readers: &[Arc<VLogReader>],
    data: &mut RepairWriter,
    cancelled: &impl Fn() -> bool,
) -> Result<MergeStats> {
    let seq = inputs[0].entry.seq_no;
    let typ = inputs[0].entry.typ;
    let mut fragments = Vec::new();
    let mut dirty = Vec::new();
    let deletes = inputs
        .iter()
        .map(|s| s.file.tombstone_view(seq))
        .collect::<Vec<_>>();
    let roots = inputs.iter().map(|input| {
        if input.header.level(1).is_none() { Ok(Vec::new()) }
        else { Ok(input.roots()?.roots) }
    }).collect::<Result<Vec<_>>>()?;
    let mut ranges = roots.iter().map(|roots| roots.iter()
        .map(|record| (record.pos.start, record.pos.end)).collect()).collect();
    super::conflict_ranges::refine(inputs, &mut ranges, None, |source| {
        check_cancelled(cancelled)?;
        let input = &inputs[source];
        Ok(input.file.read_level(&input.entry, &input.header, 1, i64::MIN, i64::MAX, &[])?
            .into_iter().map(|record| (record.pos.start, record.pos.end)).collect())
    })?;
    for (i, roots) in roots.into_iter().enumerate() {
        for root in roots {
            let result = partition(
                i,
                root,
                inputs,
                &ranges,
                &mut fragments,
                &mut dirty,
                &deletes,
                cancelled,
            );
            if matches!(result, Err(super::Error::RepairBudgetExceeded)) {
                drop(fragments);
                drop(dirty);
                return stream_rebuild(writer, inputs, readers, data, cancelled);
            }
            result?;
        }
    }
    dirty.sort_by_key(|(_, r)| (r.pos.start, r.pos.end));
    let repaired_blocks = dirty.len();
    let temp = tempfile::Builder::new()
        .prefix("agg-repair-")
        .suffix(".tmp")
        .tempdir_in(&data.dir)?;
    let mut repaired = Vec::new();
    let scratch_generations = OutputGenerations::isolated(1);
    let mut begin = 0;
    while begin < dirty.len() {
        let mut end = begin + 1;
        let mut max = dirty[begin].1.pos.end;
        while end < dirty.len() && dirty[end].1.pos.start <= max {
            max = max.max(dirty[end].1.pos.end);
            end += 1;
        }
        let mut builder = FieldTreeBuilder::new(temp.path(), seq, typ)?;
        let mut staged = IndexOutput::new(
            temp.path(),
            scratch_generations.next_batch(),
            0,
            128 * 1024 * 1024,
            Default::default(),
        );
        staged.set_cancellation(cancelled);
        let mut pending = BTreeMap::<i64, (u32, Value)>::new();
        let mut buffer = Vec::<Value>::new();
        let mut buffer_version = 0;
        let mut produced = false;
        let mut emit_value = |version: u32, value: Value| -> Result<()> {
            if !buffer.is_empty() && (buffer_version != version || buffer.len() == 4000) {
                let leaf = data.write(seq, buffer_version, &buffer)?;
                builder.push(leaf, &mut staged)?;
                produced = true;
                buffer.clear();
            }
            buffer_version = version;
            buffer.push(value);
            Ok(())
        };
        for (source, record) in &dirty[begin..end] {
            check_cancelled(cancelled)?;
            while pending
                .first_key_value()
                .is_some_and(|(t, _)| *t < record.pos.start)
            {
                let (_, (version, value)) = pending.pop_first().unwrap();
                emit_value(version, value)?;
            }
            let loc = record.pos.block;
            let reader = readers
                .iter()
                .find(|f| f.generation == loc.generation() && f.seq == loc.seq())
                .ok_or(invalid("repair references missing VLog"))?;
            let values = reader
                .read_checked_block(u64::from(loc.offset), u64::from(record.pos.block_size))
                .map_err(|source| record.pos.read_error("repair value decode", source))?;
            if values.len() as u64 != record.values.count
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
                return Err(invalid("repair block does not match leaf"));
            }
            for value in values {
                let version = record.pos.write_version();
                if deletes[*source].removes(value.unix_nano())? {
                    continue;
                }
                let slot = pending
                    .entry(value.unix_nano())
                    .or_insert_with(|| (version, value.clone()));
                if slot.0 <= version {
                    *slot = (version, value);
                }
            }
        }
        for (_, (version, value)) in pending {
            emit_value(version, value)?;
        }
        drop(emit_value);
        if !produced && buffer.is_empty() {
            begin = end;
            continue;
        }
        if !buffer.is_empty() {
            builder.push(data.write(seq, buffer_version, &buffer)?, &mut staged)?;
        }
        builder.finish(&mut staged)?;
        for meta in staged.finish()? {
            let file = IndexFile::from_meta(meta)?;
            let entry = file.lookup(seq)?.ok_or(invalid("repaired field missing"))?;
            let header = file.segment_header(&entry)?;
            let forest = SegmentSource {
                file: &file,
                entry,
                header,
            }
            .roots()?;
            repaired.push(file);
            fragments.push(Fragment {
                source: inputs.len() + repaired.len() - 1,
                forest,
            });
        }
        begin = end;
    }
    let mut sources = inputs
        .iter()
        .map(|s| SegmentSource {
            file: s.file,
            entry: s.entry.clone(),
            header: s.header.clone(),
        })
        .collect::<Vec<_>>();
    for file in &repaired {
        let entry = file
            .lookup(seq)?
            .ok_or(invalid("repair staging field missing"))?;
        let header = file.segment_header(&entry)?;
        sources.push(SegmentSource {
            file,
            entry,
            header,
        });
    }
    fragments.sort_by_key(|f| f.forest.roots[0].pos.start);
    if fragments.is_empty() {
        // Keep field identity and generation without retaining a data reference.
        let first = &inputs[0];
        let mut marker = first
            .file
            .read_level(
                &first.entry,
                &first.header,
                first.entry.top_level,
                first.entry.min_start,
                first.entry.max_end,
                &ALL_COLUMNS,
            )?
            .into_iter()
            .next()
            .ok_or(invalid("empty repair input field"))?
            .into_record()?;
        marker.level = super::LEVEL_DIRTY;
        marker.pos.block = super::BlockLoc::default();
        marker.pos.block_size = 0;
        marker.pos.child_count = 0;
        marker.pos.version = 0;
        marker.pos.flags = 1;
        marker.values.count = 0;
        writer.marker(marker)?;
        return Ok(MergeStats {
            repaired_blocks,
            ..Default::default()
        });
    }
    let mut stats = merge_fragment_outputs(writer, &sources, &fragments, cancelled)?;
    stats.repaired_blocks = repaired_blocks;
    Ok(stats)
}

fn stream_rebuild(
    writer: &mut IndexOutput,
    inputs: &[SegmentSource<'_>],
    readers: &[Arc<VLogReader>],
    data: &mut RepairWriter,
    cancelled: &impl Fn() -> bool,
) -> Result<MergeStats> {
    let seq = inputs[0].entry.seq_no;
    let typ = inputs[0].entry.typ;
    let mut scan = super::raw_cursor::RawCursor::from_segments(inputs, readers)?;
    let mut builder = FieldTreeBuilder::new(&data.dir, seq, typ)?;
    let mut buffer = Vec::with_capacity(4000);
    let mut buffer_version = 0;
    let mut produced = false;
    while let Some((version, value)) = scan.next_versioned(cancelled)? {
        check_cancelled(cancelled)?;
        if !buffer.is_empty() && (buffer.len() == 4000 || version != buffer_version) {
            builder.push(data.write(seq, buffer_version, &buffer)?, writer)?;
            produced = true;
            buffer.clear();
        }
        buffer_version = version;
        buffer.push(value);
    }
    if !buffer.is_empty() {
        builder.push(data.write(seq, buffer_version, &buffer)?, writer)?;
        produced = true;
    }
    if produced {
        builder.finish(writer)?;
    } else {
        let first = &inputs[0];
        let mut marker = first
            .file
            .read_level(
                &first.entry,
                &first.header,
                first.entry.top_level,
                first.entry.min_start,
                first.entry.max_end,
                &ALL_COLUMNS,
            )?
            .into_iter()
            .next()
            .ok_or(invalid("empty repair input field"))?
            .into_record()?;
        marker.level = super::LEVEL_DIRTY;
        marker.pos.block = super::BlockLoc::default();
        marker.pos.block_size = 0;
        marker.pos.child_count = 0;
        marker.pos.version = 0;
        marker.pos.flags = 1;
        marker.values.count = 0;
        writer.marker(marker)?;
    }
    Ok(MergeStats {
        repaired_blocks: scan.decoded_blocks(),
        streamed_rebuild_fields: 1,
        ..Default::default()
    })
}
