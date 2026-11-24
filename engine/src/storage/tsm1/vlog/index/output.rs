//! File rolling shared by snapshot construction and compaction. Each field
//! fragment is a complete tree, staged before it enters an immutable file.

use super::{
    Error, FAN_OUT, MAX_LEVELS, Record, Result, SeqNo,
    allocation::{OutputGenerations, OutputIds},
    builder::TreeBuilder,
    column_layout::PART_COUNT,
    directory::{Footer, HEADER_SIZE, MAX_INDEX_FILE_SIZE},
    io_budget::IoBudget,
    page_group::{EncodedPageGroup, PageGroupEntry},
    spool::TreeSpool,
    writer::{IndexFileMeta, IndexFileWriter},
};
use crate::models::FieldType;
use std::path::{Path, PathBuf};

/// The merge algorithm can retain encoded groups or emit changed records.
pub(super) trait SegmentSink {
    fn push(&mut self, record: &Record) -> Result<()>;
    fn append_group(&mut self, group: EncodedPageGroup) -> Result<()>;
}
impl SegmentSink for IndexFileWriter {
    fn push(&mut self, record: &Record) -> Result<()> {
        self.push(record)
    }
    fn append_group(&mut self, group: EncodedPageGroup) -> Result<()> {
        self.append_group(group)
    }
}
impl SegmentSink for TreeSpool {
    fn push(&mut self, record: &Record) -> Result<()> {
        self.push(*record)
    }
    fn append_group(&mut self, group: EncodedPageGroup) -> Result<()> {
        self.append_group(group)
    }
}

pub(super) struct IndexOutput<'a> {
    dir: PathBuf,
    identities: OutputIds,
    target: u64,
    hard_limit: u64,
    budget: IoBudget,
    writer: Option<IndexFileWriter>,
    last_field: Option<SeqNo>,
    paths: Vec<PathBuf>,
    files: Vec<IndexFileMeta>,
    keep: bool,
    cancelled: &'a dyn Fn() -> bool,
}
impl<'a> IndexOutput<'a> {
    pub(super) fn new(
        dir: &Path,
        generations: OutputGenerations,
        first_sequence: i64,
        target: u64,
        budget: IoBudget,
    ) -> Self {
        Self {
            dir: dir.into(),
            identities: OutputIds::new(generations, first_sequence),
            target,
            hard_limit: MAX_INDEX_FILE_SIZE,
            budget,
            writer: None,
            last_field: None,
            paths: Vec::new(),
            files: Vec::new(),
            keep: false,
            cancelled: &|| false,
        }
    }
    pub(super) fn set_cancellation(&mut self, cancelled: &'a dyn Fn() -> bool) {
        self.cancelled = cancelled;
    }
    pub(super) fn dir(&self) -> &Path {
        &self.dir
    }
    fn segment_bytes(limit: u64) -> u64 {
        limit.saturating_sub(HEADER_SIZE as u64 + Footer::directory_bytes(1))
    }
    pub(super) fn segment_target(&self) -> u64 {
        Self::segment_bytes(self.target.min(self.hard_limit))
    }
    pub(super) fn segment_limit(&self) -> u64 {
        Self::segment_bytes(self.hard_limit)
    }
    fn close(&mut self) -> Result<()> {
        if (self.cancelled)() {
            return Err(Error::CompactionCancelled);
        }
        if let Some(writer) = self.writer.take() {
            self.files.push(writer.finish()?);
        }
        Ok(())
    }
    pub(super) fn append(
        &mut self,
        seq: SeqNo,
        typ: FieldType,
        staged: &mut TreeSpool,
    ) -> Result<()> {
        if (self.cancelled)() {
            return Err(Error::CompactionCancelled);
        }
        let bytes = staged.encoded_size()?;
        if bytes > self.segment_limit() {
            return Err(Error::MaxFileExceed);
        }
        if self.last_field.is_some_and(|last| last > seq) {
            return Err(super::invalid("output fields are not ordered"));
        }
        if self.writer.as_ref().is_some_and(|w| {
            self.last_field == Some(seq)
                || w.size_with_segment(bytes) > self.target.min(self.hard_limit)
        }) {
            self.close()?;
        }
        if self.writer.is_none() {
            let (generation, file_seq) = self.identities.next_index()?;
            let path = self
                .dir
                .join(format!("{generation:09}-{file_seq:09}.idx.tmp"));
            let mut writer = IndexFileWriter::create(&path, generation, file_seq)?;
            self.paths.push(path);
            writer.set_io_budget(self.budget.clone());
            self.writer = Some(writer);
        }
        let writer = self.writer.as_mut().unwrap();
        if !writer.can_fit_segment(bytes) || writer.size_with_segment(bytes) > self.hard_limit {
            return Err(Error::MaxFileExceed);
        }
        writer.begin_segment(seq, typ)?;
        staged.replay(|group| {
            if (self.cancelled)() {
                return Err(Error::CompactionCancelled);
            }
            writer.append_group(group)
        })?;
        writer.end_segment()?;
        self.last_field = Some(seq);
        Ok(())
    }
    pub(super) fn marker(&mut self, record: Record) -> Result<()> {
        let mut staged = TreeSpool::new(&self.dir, record.seq_no, record.values.field_type())?;
        staged.push(record)?;
        self.append(record.seq_no, record.values.field_type(), &mut staged)
    }
    pub(super) fn finish(mut self) -> Result<Vec<IndexFileMeta>> {
        self.close()?;
        self.keep = true;
        Ok(std::mem::take(&mut self.files))
    }
}
impl Drop for IndexOutput<'_> {
    fn drop(&mut self) {
        self.writer.take();
        if !self.keep {
            for path in &self.paths {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

/// Streaming construction retains only a bounded suffix at each tree level.
/// The target is soft at page boundaries; format-derived headroom makes the
/// hard limit safe even when the final suffix produces new parent groups.
pub(super) struct FieldTreeBuilder {
    builder: TreeBuilder,
    staged: TreeSpool,
    seq: SeqNo,
    typ: FieldType,
    leaves: u64,
    previous_end: Option<i64>,
}
impl FieldTreeBuilder {
    pub(super) fn new(dir: &Path, seq: SeqNo, typ: FieldType) -> Result<Self> {
        Ok(Self {
            builder: TreeBuilder::new(),
            staged: TreeSpool::new(dir, seq, typ)?,
            seq,
            typ,
            leaves: 0,
            previous_end: None,
        })
    }
    pub(super) fn push(&mut self, record: Record, output: &mut IndexOutput) -> Result<()> {
        if record.seq_no != self.seq
            || record.values.field_type() != self.typ
            || self.previous_end.is_some_and(|end| end >= record.pos.start)
        {
            return Err(super::invalid("invalid segmented tree input"));
        }
        self.builder.push(record, &mut |r| self.staged.push(r))?;
        self.leaves += 1;
        self.previous_end = Some(record.pos.end);
        // Each encoded part has a u16 length. A push can flush one group per
        // level; finishing can flush another and leave one partial group.
        let max_group = (PART_COUNT * u16::MAX as usize + PageGroupEntry::SIZE) as u64;
        let reserve = 3 * u64::from(MAX_LEVELS) * max_group;
        let threshold = output
            .segment_target()
            .min(output.segment_limit().saturating_sub(reserve));
        if self.staged.flushed_size() >= threshold
            || self.leaves == (FAN_OUT as u64).pow(MAX_LEVELS as u32)
        {
            self.flush(output)?;
        }
        Ok(())
    }
    fn flush(&mut self, output: &mut IndexOutput) -> Result<()> {
        if self.leaves == 0 {
            return Ok(());
        }
        let builder = std::mem::replace(&mut self.builder, TreeBuilder::new());
        builder.finish(&mut |r| self.staged.push(r))?;
        output.append(self.seq, self.typ, &mut self.staged)?;
        self.staged = TreeSpool::new(output.dir(), self.seq, self.typ)?;
        self.leaves = 0;
        Ok(())
    }
    pub(super) fn finish(mut self, output: &mut IndexOutput) -> Result<()> {
        if self.leaves == 0 {
            return Ok(());
        }
        // EOF consumes this builder; do not allocate replacement staging state.
        self.builder.finish(&mut |record| self.staged.push(record))?;
        output.append(self.seq, self.typ, &mut self.staged)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{AggValues, BlockLoc, Position, page_group::ALL_COLUMNS, reader::IndexFile};
    use super::*;
    use crate::models::{
        INTEGER,
        points::{IntegerValue, Value},
    };

    fn leaf(time: i64) -> Record {
        Record {
            seq_no: SeqNo::new(1, 1).unwrap(),
            level: 1,
            pos: Position {
                start: time,
                end: time,
                block: BlockLoc::from_vlog(1, 0, 5 + time).unwrap(),
                block_size: 32,
                flags: 0,
                child_count: 0,
                version: 0,
            },
            values: AggValues::from_block(&[Value::IntegerValue(IntegerValue::new(time, time))])
                .unwrap(),
        }
    }

    #[test]
    fn rolling_output_counts_directory_bytes_and_cleans_identity_exhaustion() {
        let dir = tempfile::tempdir().unwrap();
        let record = leaf(1);
        let mut staged = TreeSpool::new(dir.path(), record.seq_no, INTEGER).unwrap();
        staged.push(record).unwrap();
        let exact =
            HEADER_SIZE as u64 + staged.encoded_size().unwrap() + Footer::directory_bytes(1);
        {
            let mut output = IndexOutput::new(
                dir.path(),
                OutputGenerations::isolated(2),
                0,
                exact,
                Default::default(),
            );
            output.hard_limit = exact - 1;
            assert!(matches!(
                output.append(record.seq_no, INTEGER, &mut staged),
                Err(Error::MaxFileExceed)
            ));
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        {
            let mut output = IndexOutput::new(
                dir.path(),
                OutputGenerations::isolated((1 << 24) - 1),
                255,
                exact,
                Default::default(),
            );
            output.hard_limit = exact;
            output.append(record.seq_no, INTEGER, &mut staged).unwrap();
            assert!(matches!(
                output.append(record.seq_no, INTEGER, &mut staged),
                Err(Error::InvalidBlockLocation)
            ));
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        let mut output = IndexOutput::new(
            dir.path(),
            OutputGenerations::isolated(2),
            0,
            exact,
            Default::default(),
        );
        output.hard_limit = exact;
        output.append(record.seq_no, INTEGER, &mut staged).unwrap();
        let files = output.finish().unwrap();
        assert_eq!(std::fs::metadata(&files[0].path).unwrap().len(), exact);
    }

    #[test]
    fn streamed_tree_rolls_before_the_hard_limit_and_validates_cross_file_order() {
        let dir = tempfile::tempdir().unwrap();
        let seq = leaf(0).seq_no;
        let mut output = IndexOutput::new(
            dir.path(),
            OutputGenerations::isolated(2),
            0,
            4096,
            Default::default(),
        );
        output.hard_limit = 4096;
        let mut builder = FieldTreeBuilder::new(dir.path(), seq, INTEGER).unwrap();
        for time in 0..70 {
            builder.push(leaf(time), &mut output).unwrap();
        }
        assert!(builder.push(leaf(69), &mut output).is_err());
        builder.finish(&mut output).unwrap();
        let mut times = Vec::new();
        let files = output.finish().unwrap();
        assert!(files.len() > 1);
        for meta in files {
            assert!(std::fs::metadata(&meta.path).unwrap().len() <= 4096);
            let file = IndexFile::from_meta(meta).unwrap();
            let entry = file.lookup(seq).unwrap().unwrap();
            let header = file.segment_header(&entry).unwrap();
            times.extend(
                file.read_level(&entry, &header, 1, i64::MIN, i64::MAX, &ALL_COLUMNS)
                    .unwrap()
                    .into_iter()
                    .map(|r| r.pos.start),
            );
        }
        assert_eq!(times, (0..70).collect::<Vec<_>>());
    }

    #[test]
    fn streamed_tree_finishes_multilevel_fragments_at_page_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let seq = leaf(0).seq_no;
        let mut output = IndexOutput::new(
            dir.path(),
            OutputGenerations::isolated(2),
            0,
            4096,
            Default::default(),
        );
        let mut builder = FieldTreeBuilder::new(dir.path(), seq, INTEGER).unwrap();
        for time in 0..4096 {
            builder.push(leaf(time), &mut output).unwrap();
        }
        builder.finish(&mut output).unwrap();
        let files = output.finish().unwrap();
        assert!(files.len() > 1);
        let mut times = Vec::new();
        for meta in files {
            let file = IndexFile::from_meta(meta).unwrap();
            let entry = file.lookup(seq).unwrap().unwrap();
            let header = file.segment_header(&entry).unwrap();
            let roots = super::super::compact::SegmentSource {
                file: &file,
                entry: entry.clone(),
                header: header.clone(),
            }
            .roots()
            .unwrap();
            assert!(roots.level() > 1);
            let records = file
                .read_level(&entry, &header, 1, i64::MIN, i64::MAX, &ALL_COLUMNS)
                .unwrap();
            assert_eq!(
                roots.roots.iter().map(|r| r.values.count).sum::<u64>(),
                records.len() as u64
            );
            times.extend(records.into_iter().map(|r| r.pos.start));
        }
        assert_eq!(times, (0..4096).collect::<Vec<_>>());
    }
}
