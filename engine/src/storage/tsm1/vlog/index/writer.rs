use super::directory::*;
use super::page_group::{
    EncodedPageGroup, PageGroupEntry, encode_group, valid_type, validate_record,
};
use super::segment::{LEVEL_HEADER_SIZE, LevelHeader, SegmentHeader, level_bit};
use super::spool::Spool;
use super::{Error, Level, MAX_LEVELS, PAGE_GROUP, Record, Result, SeqNo, invalid};
use crate::models::FieldType;
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const FSYNC_EVERY: u64 = 25 * 1024 * 1024;
pub(super) const STREAM_HEADER_SIZE: usize = 8 + LEVEL_HEADER_SIZE * (MAX_LEVELS as usize + 1);

#[derive(Clone, Debug)]
pub(crate) struct IndexFileMeta {
    pub(crate) path: PathBuf,
    pub(crate) generation: i64,
    pub(crate) seq: i64,
    pub(crate) footer: Footer,
    pub(crate) top_index: TopIndex,
}
struct PendingLevel {
    level: Level,
    header: LevelHeader,
    group: Vec<Record>,
    entries: Spool,
}
struct PendingSegment {
    seq_no: SeqNo,
    start: u64,
    header: SegmentHeader,
    current: Option<PendingLevel>,
}

struct DirectorySpool {
    data: Spool,
    minima: Spool,
    top: Vec<u64>,
    first: Option<SeqNo>,
    last: Option<SeqNo>,
    count: usize,
}
impl DirectorySpool {
    fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            data: Spool::new(path)?,
            minima: Spool::new(path)?,
            top: Vec::new(),
            first: None,
            last: None,
            count: 0,
        })
    }
    fn push(&mut self, entry: DirEntry) -> Result<()> {
        if self.count % DIR_PER_PAGE == 0 {
            self.minima.append(&entry.seq_no.get().to_be_bytes())?;
            if self.count / DIR_PER_PAGE % INDEX_PER_PAGE == 0 {
                self.top.push(entry.seq_no.get());
            }
        }
        let mut bytes = Vec::with_capacity(DIR_ENTRY_SIZE);
        entry.encode(&mut bytes);
        self.data.append(&bytes)?;
        self.first.get_or_insert(entry.seq_no);
        self.last = Some(entry.seq_no);
        self.count += 1;
        Ok(())
    }
}

/// Writes one immutable index file. A failed write cannot be finalized.
pub(crate) struct IndexFileWriter {
    budget: super::io_budget::IoBudget,
    w: BufWriter<File>,
    path: PathBuf,
    generation: i64,
    seq: i64,
    dir: DirectorySpool,
    offset: u64,
    last_sync: u64,
    active: Option<PendingSegment>,
    failed: bool,
    #[cfg(test)]
    legacy_groups: bool,
}
impl IndexFileWriter {
    pub(crate) fn create(path: &Path, generation: i64, seq: i64) -> Result<Self> {
        let file_no = super::allocation::index_file_no(generation, seq)?;
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path)?;
        let mut result = Self {
            budget: Default::default(),
            w: BufWriter::new(file),
            path: path.to_path_buf(),
            generation,
            seq,
            dir: DirectorySpool::new(
                path.parent()
                    .ok_or(invalid("index has no parent directory"))?,
            )?,
            offset: 0,
            last_sync: 0,
            active: None,
            failed: false,
            #[cfg(test)]
            legacy_groups: false,
        };
        let mut header = vec![0; HEADER_SIZE];
        header[..4].copy_from_slice(&MAGIC);
        header[4..6].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
        header[8..12].copy_from_slice(&file_no.to_be_bytes());
        result.append(&header)?;
        Ok(result)
    }
    #[cfg(test)]
    pub(crate) fn create_legacy(
        path: &Path,
        generation: i64,
        seq: i64,
        version: u16,
    ) -> Result<Self> {
        assert!((2..=3).contains(&version));
        let mut writer = Self::create(path, generation, seq)?;
        writer.w.seek(SeekFrom::Start(4))?;
        writer.w.write_all(&version.to_be_bytes())?;
        writer.w.seek(SeekFrom::Start(writer.offset))?;
        writer.legacy_groups = true;
        Ok(writer)
    }
    fn prepare_group(&self, group: EncodedPageGroup) -> Result<EncodedPageGroup> {
        #[cfg(test)]
        if self.legacy_groups {
            return group.without_column_checksums();
        }
        group.with_column_checksums()
    }
    fn encode_group(
        &self,
        typ: FieldType,
        records: &[Record],
        offset: u32,
    ) -> Result<(PageGroupEntry, Vec<u8>)> {
        let (entry, bytes) = encode_group(typ, records, offset)?;
        let group = self.prepare_group(EncodedPageGroup {
            seq_no: records[0].seq_no,
            level: records[0].level,
            typ,
            entry,
            bytes,
        })?;
        Ok((group.entry, group.bytes))
    }
    pub(crate) fn size(&self) -> u64 {
        self.offset
    }
    pub(super) fn is_empty(&self) -> bool {
        self.dir.count == 0
    }
    pub(super) fn can_fit_segment(&self, bytes: u64) -> bool {
        self.size_with_segment(bytes) <= MAX_INDEX_FILE_SIZE
    }
    pub(super) fn size_with_segment(&self, bytes: u64) -> u64 {
        self.offset
            .saturating_add(bytes)
            .saturating_add(Footer::directory_bytes(self.dir.count + 1))
    }
    pub(crate) fn set_io_budget(&mut self, budget: super::io_budget::IoBudget) {
        self.budget = budget;
    }
    fn append_spool(&mut self, spool: &mut Spool) -> Result<()> {
        spool.rewind()?;
        let mut remaining = spool.len;
        let mut buffer = [0; 16 * 1024];
        while remaining > 0 {
            let size = remaining.min(buffer.len() as u64) as usize;
            spool.read_exact(&mut buffer[..size])?;
            self.append(&buffer[..size])?;
            remaining -= size as u64;
        }
        Ok(())
    }
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        if self.failed {
            return Err(invalid("writer is failed"));
        }
        let result = (|| {
            self.budget.charge(bytes.len())?;
            self.w.write_all(bytes)?;
            self.offset += bytes.len() as u64;
            if self.offset - self.last_sync >= FSYNC_EVERY {
                self.w.flush()?;
                self.w.get_ref().sync_data()?;
                self.last_sync = self.offset;
            }
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn check_next(&self, seq_no: SeqNo, typ: FieldType) -> Result<()> {
        if self.failed
            || self.active.is_some()
            || !valid_type(typ)
            || self.dir.last.is_some_and(|last| last >= seq_no)
        {
            return Err(invalid("invalid segment write order or state"));
        }
        if self.dir.count >= u32::MAX as usize {
            return Err(Error::MaxFileExceed);
        }
        Ok(())
    }
    fn entry(seq_no: SeqNo, start: u64, end: u64, header: &SegmentHeader) -> Result<DirEntry> {
        let seg_len = u32::try_from(end - start).map_err(|_| Error::MaxFileExceed)?;
        header.validate(seg_len)?;
        Ok(DirEntry {
            seq_no,
            seg_offset: start,
            seg_len,
            typ: header.typ,
            top_level: header.levels[0].0,
            level_bits: header.level_bits,
            min_start: header
                .levels
                .iter()
                .map(|(_, h)| h.min_start)
                .min()
                .unwrap(),
            max_end: header.levels.iter().map(|(_, h)| h.max_end).max().unwrap(),
        })
    }
    pub(crate) fn write_segment(
        &mut self,
        seq_no: SeqNo,
        typ: FieldType,
        levels: &[(Level, Vec<Record>)],
    ) -> Result<()> {
        self.check_next(seq_no, typ)?;
        if levels.is_empty() || levels.len() > MAX_LEVELS as usize + 1 {
            return Err(invalid("invalid segment levels"));
        }
        let mut header = SegmentHeader {
            typ,
            flags: 0,
            level_bits: 0,
            levels: Vec::new(),
        };
        let header_size = 8 + LEVEL_HEADER_SIZE * levels.len();
        let mut data = vec![0; header_size];
        let mut previous = u8::MAX;
        for (level, records) in levels {
            if *level >= previous || records.is_empty() {
                return Err(invalid("unordered or empty level"));
            }
            previous = *level;
            header.level_bits |= level_bit(*level)?;
            for r in records {
                if r.seq_no != seq_no || r.level != *level {
                    return Err(invalid("record identity mismatch"));
                }
            }
            if records.windows(2).any(|p| p[0].pos.end >= p[1].pos.start) {
                return Err(invalid("overlapping level records"));
            }
            let start = data.len();
            header.levels.push((
                *level,
                LevelHeader {
                    count: u32::try_from(records.len()).map_err(|_| Error::MaxFileExceed)?,
                    level_offset: u32::try_from(start).map_err(|_| Error::MaxFileExceed)?,
                    group_count: records.len().div_ceil(PAGE_GROUP) as u32,
                    table_offset: 0,
                    min_start: records[0].pos.start,
                    max_end: records.last().unwrap().pos.end,
                },
            ));
            let mut table = Vec::new();
            for (i, group) in records.chunks(PAGE_GROUP).enumerate() {
                let (mut entry, bytes) = self.encode_group(
                    typ,
                    group,
                    u32::try_from(data.len() - start).map_err(|_| Error::MaxFileExceed)?,
                )?;
                entry.first_row = (i * PAGE_GROUP) as u32;
                entry.encode(&mut table);
                data.extend_from_slice(&bytes);
            }
            header.levels.last_mut().unwrap().1.table_offset =
                u32::try_from(data.len() - start).map_err(|_| Error::MaxFileExceed)?;
            data.extend_from_slice(&table);
        }
        let end = self.offset + data.len() as u64;
        if end + Footer::directory_bytes(self.dir.count + 1) > MAX_INDEX_FILE_SIZE {
            return Err(Error::MaxFileExceed);
        }
        let entry = Self::entry(seq_no, self.offset, end, &header)?;
        let mut header_bytes = Vec::new();
        header.encode(&mut header_bytes);
        data[..header_size].copy_from_slice(&header_bytes);
        self.append(&data)?;
        self.dir.push(entry)?;
        Ok(())
    }
    pub(crate) fn begin_segment(&mut self, seq_no: SeqNo, typ: FieldType) -> Result<()> {
        self.check_next(seq_no, typ)?;
        if self.offset + STREAM_HEADER_SIZE as u64 + Footer::directory_bytes(self.dir.count + 1)
            >= MAX_INDEX_FILE_SIZE
        {
            return Err(Error::MaxFileExceed);
        }
        let start = self.offset;
        self.append(&[0; STREAM_HEADER_SIZE])?;
        self.active = Some(PendingSegment {
            seq_no,
            start,
            header: SegmentHeader {
                typ,
                flags: 0,
                level_bits: 0,
                levels: Vec::new(),
            },
            current: None,
        });
        Ok(())
    }
    fn flush_group(&mut self, segment: &mut PendingSegment) -> Result<()> {
        let level = segment.current.as_mut().ok_or(invalid("no active level"))?;
        if level.group.is_empty() {
            return Ok(());
        }
        let relative = self.offset - segment.start - u64::from(level.header.level_offset);
        let (mut entry, bytes) = self.encode_group(
            segment.header.typ,
            &level.group,
            u32::try_from(relative).map_err(|_| Error::MaxFileExceed)?,
        )?;
        entry.first_row = level.header.count - level.group.len() as u32;
        self.append(&bytes)?;
        let mut bytes = Vec::with_capacity(PageGroupEntry::SIZE);
        entry.encode(&mut bytes);
        level.entries.append(&bytes)?;
        level.group.clear();
        Ok(())
    }
    fn finish_level(&mut self, segment: &mut PendingSegment) -> Result<()> {
        if segment.current.is_none() {
            return Ok(());
        }
        self.flush_group(segment)?;
        let mut level = segment.current.take().unwrap();
        level.header.group_count = u32::try_from(level.entries.len / PageGroupEntry::SIZE as u64)
            .map_err(|_| Error::MaxFileExceed)?;
        level.header.table_offset =
            u32::try_from(self.offset - segment.start - u64::from(level.header.level_offset))
                .map_err(|_| Error::MaxFileExceed)?;
        self.append_spool(&mut level.entries)?;
        segment.header.level_bits |= level_bit(level.level)?;
        segment.header.levels.push((level.level, level.header));
        Ok(())
    }
    fn prepare_level(
        &mut self,
        segment: &mut PendingSegment,
        level: Level,
        start: i64,
        end: i64,
    ) -> Result<()> {
        level_bit(level)?;
        if start > end {
            return Err(invalid("reversed stream range"));
        }
        if let Some(current) = &segment.current {
            if level > current.level || (level == current.level && start <= current.header.max_end)
            {
                return Err(invalid("unordered stream record"));
            }
        }
        if segment.current.as_ref().is_none_or(|l| l.level != level) {
            self.finish_level(segment)?;
            segment.current = Some(PendingLevel {
                level,
                header: LevelHeader {
                    count: 0,
                    level_offset: u32::try_from(self.offset - segment.start)
                        .map_err(|_| Error::MaxFileExceed)?,
                    group_count: 0,
                    table_offset: 0,
                    min_start: start,
                    max_end: end,
                },
                group: Vec::with_capacity(PAGE_GROUP),
                entries: Spool::new(self.path.parent().unwrap())?,
            });
        }
        Ok(())
    }
    /// Appends compressed bytes without decoding columns or realigning rows.
    pub(crate) fn append_group(&mut self, group: EncodedPageGroup) -> Result<()> {
        if self.failed {
            return Err(invalid("writer is failed"));
        }
        let mut segment = self.active.take().ok_or(invalid("no active segment"))?;
        let result = (|| {
            let group = self.prepare_group(group)?;
            if group.seq_no != segment.seq_no || group.typ != segment.header.typ {
                return Err(invalid("copied group identity mismatch"));
            }
            group.entry.verify_payload(&group.bytes)?;
            self.prepare_level(
                &mut segment,
                group.level,
                group.entry.first_start,
                group.entry.last_end,
            )?;
            self.flush_group(&mut segment)?;
            let current = segment.current.as_mut().unwrap();
            let mut entry = group.entry;
            entry.group_offset =
                u32::try_from(self.offset - segment.start - u64::from(current.header.level_offset))
                    .map_err(|_| Error::MaxFileExceed)?;
            entry.first_row = current.header.count;
            current.header.count = current
                .header
                .count
                .checked_add(u32::from(entry.row_count))
                .ok_or(Error::MaxFileExceed)?;
            current.header.max_end = entry.last_end;
            self.append(&group.bytes)?;
            let mut bytes = Vec::with_capacity(PageGroupEntry::SIZE);
            entry.encode(&mut bytes);
            current.entries.append(&bytes)?;
            if self.offset + Footer::directory_bytes(self.dir.count + 1) > MAX_INDEX_FILE_SIZE {
                return Err(Error::MaxFileExceed);
            }
            Ok(())
        })();
        self.active = Some(segment);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub(crate) fn push(&mut self, record: &Record) -> Result<()> {
        if self.failed {
            return Err(invalid("writer is failed"));
        }
        let mut segment = self.active.take().ok_or(invalid("no active segment"))?;
        let result = (|| {
            validate_record(record, segment.header.typ)?;
            if record.seq_no != segment.seq_no {
                return Err(invalid("stream sequence mismatch"));
            }
            self.prepare_level(&mut segment, record.level, record.pos.start, record.pos.end)?;
            let current = segment.current.as_mut().unwrap();
            current.header.count = current
                .header
                .count
                .checked_add(1)
                .ok_or(Error::MaxFileExceed)?;
            current.header.max_end = record.pos.end;
            current.group.push(*record);
            if current.group.len() == PAGE_GROUP {
                self.flush_group(&mut segment)?;
            }
            if self.offset + Footer::directory_bytes(self.dir.count + 1) > MAX_INDEX_FILE_SIZE {
                return Err(Error::MaxFileExceed);
            }
            Ok(())
        })();
        self.active = Some(segment);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub(crate) fn end_segment(&mut self) -> Result<()> {
        if self.failed {
            return Err(invalid("writer is failed"));
        }
        let mut segment = self.active.take().ok_or(invalid("no active segment"))?;
        let result = (|| {
            self.finish_level(&mut segment)?;
            let entry = Self::entry(segment.seq_no, segment.start, self.offset, &segment.header)?;
            if self.offset + Footer::directory_bytes(self.dir.count + 1) > MAX_INDEX_FILE_SIZE {
                return Err(Error::MaxFileExceed);
            }
            let mut bytes = Vec::new();
            segment.header.encode(&mut bytes);
            self.budget.charge(bytes.len())?;
            self.w.seek(SeekFrom::Start(segment.start))?;
            self.w.write_all(&bytes)?;
            self.w.seek(SeekFrom::Start(self.offset))?;
            self.dir.push(entry)?;
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub(crate) fn finish(mut self) -> Result<IndexFileMeta> {
        if self.failed || self.active.is_some() || self.dir.count == 0 {
            return Err(invalid("cannot finish incomplete or empty index"));
        }
        if self.offset + Footer::directory_bytes(self.dir.count) > MAX_INDEX_FILE_SIZE {
            return Err(Error::MaxFileExceed);
        }
        let empty = DirectorySpool::new(self.path.parent().unwrap())?;
        let mut directory = std::mem::replace(&mut self.dir, empty);
        let page_index_offset = self.offset;
        let top_index = TopIndex(directory.top);
        self.append_spool(&mut directory.minima)?;
        let dir_offset = self.offset;
        directory.data.rewind()?;
        for start in (0..directory.count).step_by(DIR_PER_PAGE) {
            let count = (directory.count - start).min(DIR_PER_PAGE);
            let mut bytes = [0; PAGE];
            directory
                .data
                .read_exact(&mut bytes[..count * DIR_ENTRY_SIZE])?;
            let len = if start + count < directory.count {
                PAGE
            } else {
                count * DIR_ENTRY_SIZE
            };
            self.append(&bytes[..len])?;
        }
        let top_index_offset = self.offset;
        let top = top_index.encode();
        self.append(&top)?;
        let footer = Footer {
            index_offset: HEADER_SIZE as u64,
            page_index_offset,
            dir_offset,
            top_index_offset,
            min_seq_no: directory.first.unwrap().get(),
            max_seq_no: directory.last.unwrap().get(),
            series_count: directory.count as u32,
            flags: 0,
        };
        self.append(&footer.encode(&top))?;
        self.w.flush()?;
        self.w.get_ref().sync_all()?;
        Ok(IndexFileMeta {
            path: self.path,
            generation: self.generation,
            seq: self.seq,
            footer,
            top_index,
        })
    }
}
