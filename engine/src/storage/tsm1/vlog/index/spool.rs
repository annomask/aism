//! Disk staging for construction metadata, bounded independently of field count.

use super::page_group::{ALL_COLUMNS, EncodedPageGroup, PageGroupEntry, encode_group};
use super::{MAX_LEVELS, PAGE_GROUP, Record, Result, SeqNo, invalid};
use crate::models::FieldType;
use std::{
    fs::File,
    io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const MEMORY_LIMIT: usize = 64 * 1024;
enum Storage {
    Memory(Cursor<Vec<u8>>),
    Disk(BufWriter<File>),
}

pub(super) struct Spool {
    storage: Storage,
    dir: PathBuf,
    pub(super) len: u64,
}
impl Spool {
    pub(super) fn new(dir: &Path) -> Result<Self> {
        Ok(Self {
            storage: Storage::Memory(Cursor::new(Vec::new())),
            dir: dir.into(),
            len: 0,
        })
    }
    pub(super) fn append(&mut self, bytes: &[u8]) -> Result<()> {
        if let Storage::Memory(memory) = &self.storage {
            if self.len + bytes.len() as u64 > MEMORY_LIMIT as u64 {
                let mut file = BufWriter::new(tempfile::tempfile_in(&self.dir)?);
                file.write_all(memory.get_ref())?;
                self.storage = Storage::Disk(file);
            }
        }
        match &mut self.storage {
            Storage::Memory(memory) => memory.write_all(bytes)?,
            Storage::Disk(file) => file.write_all(bytes)?,
        }
        self.len += bytes.len() as u64;
        Ok(())
    }
    pub(super) fn rewind(&mut self) -> Result<()> {
        match &mut self.storage {
            Storage::Memory(memory) => {
                memory.set_position(0);
            }
            Storage::Disk(file) => {
                file.flush()?;
                file.seek(SeekFrom::Start(0))?;
            }
        }
        Ok(())
    }
    pub(super) fn read_exact(&mut self, bytes: &mut [u8]) -> Result<()> {
        match &mut self.storage {
            Storage::Memory(memory) => memory.read_exact(bytes)?,
            Storage::Disk(file) => file.get_mut().read_exact(bytes)?,
        }
        Ok(())
    }
}

struct LevelSpool {
    data: Spool,
    pending: Vec<Record>,
}

/// At most one compressed page's source records are buffered per level.
pub(super) struct TreeSpool {
    levels: Vec<LevelSpool>,
    seq: SeqNo,
    typ: FieldType,
    bytes: u64,
    limit: u64,
}
impl TreeSpool {
    pub(super) fn new(dir: &Path, seq: SeqNo, typ: FieldType) -> Result<Self> {
        Ok(Self {
            seq,
            typ,
            bytes: super::writer::STREAM_HEADER_SIZE as u64,
            limit: u64::MAX,
            levels: (0..=MAX_LEVELS)
                .map(|_| {
                    Ok(LevelSpool {
                        data: Spool::new(dir)?,
                        pending: Vec::new(),
                    })
                })
                .collect::<Result<_>>()?,
        })
    }
    pub(super) fn set_limit(&mut self, limit: u64) {
        self.limit = limit;
    }
    pub(super) fn flushed_size(&self) -> u64 {
        self.bytes
    }
    fn level_index(level: u8) -> Result<usize> {
        if level == super::LEVEL_DIRTY {
            Ok(MAX_LEVELS as usize)
        } else if (1..=MAX_LEVELS).contains(&level) {
            Ok(level as usize - 1)
        } else {
            Err(invalid("invalid staged tree level"))
        }
    }
    pub(super) fn push(&mut self, record: Record) -> Result<()> {
        if record.seq_no != self.seq || record.values.field_type() != self.typ {
            return Err(invalid("staged tree identity mismatch"));
        }
        let level = self
            .levels
            .get_mut(Self::level_index(record.level)?)
            .ok_or(invalid("invalid staged tree level"))?;
        level.pending.push(record);
        if level.pending.len() == PAGE_GROUP {
            Self::flush(self.typ, level, &mut self.bytes, self.limit)?;
        }
        Ok(())
    }
    fn flush(typ: FieldType, level: &mut LevelSpool, total: &mut u64, limit: u64) -> Result<()> {
        if level.pending.is_empty() {
            return Ok(());
        }
        let (entry, bytes) = encode_group(typ, &level.pending, 0)?;
        Self::append(level, entry, &bytes, total, limit)?;
        level.pending.clear();
        Ok(())
    }
    fn append(
        level: &mut LevelSpool,
        entry: PageGroupEntry,
        bytes: &[u8],
        total: &mut u64,
        limit: u64,
    ) -> Result<()> {
        let next = total.saturating_add((PageGroupEntry::SIZE + bytes.len()) as u64);
        if next > limit {
            return Err(super::Error::SegmentBudgetExceeded);
        }
        let mut header = Vec::with_capacity(PageGroupEntry::SIZE);
        entry.encode(&mut header);
        level.data.append(&header)?;
        level.data.append(bytes)?;
        *total = next;
        Ok(())
    }
    pub(super) fn append_group(&mut self, group: EncodedPageGroup) -> Result<()> {
        if group.seq_no != self.seq || group.typ != self.typ {
            return Err(invalid("staged group identity mismatch"));
        }
        let group = group.with_column_checksums()?;
        let level = self
            .levels
            .get_mut(Self::level_index(group.level)?)
            .ok_or(invalid("invalid staged group level"))?;
        Self::flush(self.typ, level, &mut self.bytes, self.limit)?;
        Self::append(
            level,
            group.entry,
            &group.bytes,
            &mut self.bytes,
            self.limit,
        )
    }
    pub(super) fn encoded_size(&mut self) -> Result<u64> {
        let mut bytes = super::writer::STREAM_HEADER_SIZE as u64;
        for level in &mut self.levels {
            Self::flush(self.typ, level, &mut self.bytes, self.limit)?;
            bytes = bytes
                .checked_add(level.data.len)
                .ok_or(super::Error::MaxFileExceed)?;
        }
        Ok(bytes)
    }
    pub(super) fn replay(
        &mut self,
        mut visit: impl FnMut(EncodedPageGroup) -> Result<()>,
    ) -> Result<()> {
        for (level_no, level) in self.levels.iter_mut().enumerate().rev() {
            Self::flush(self.typ, level, &mut self.bytes, self.limit)?;
            level.data.rewind()?;
            let mut remaining = level.data.len;
            while remaining != 0 {
                let mut header = [0; PageGroupEntry::SIZE];
                level.data.read_exact(&mut header)?;
                let entry = PageGroupEntry::decode(&header)?;
                let len = entry.byte_len(&ALL_COLUMNS);
                let mut bytes = vec![0; len];
                level.data.read_exact(&mut bytes)?;
                remaining = remaining
                    .checked_sub((len + header.len()) as u64)
                    .ok_or(invalid("invalid staged tree length"))?;
                visit(EncodedPageGroup {
                    seq_no: self.seq,
                    level: if level_no == MAX_LEVELS as usize {
                        super::LEVEL_DIRTY
                    } else {
                        level_no as u8 + 1
                    },
                    typ: self.typ,
                    entry,
                    bytes,
                })?;
            }
        }
        Ok(())
    }
}
