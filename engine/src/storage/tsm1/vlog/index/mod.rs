//! Records shared by the disk-resident, multilevel aggregate index.
//!
//! Aggregate levels describe logical time intervals, independently of file
//! generations and compaction levels. These types do not change the VLog format.

use snafu::Snafu;

use super::VLogIndex;

pub(crate) mod allocation;
pub(crate) mod builder;
pub(crate) mod caches;
mod column_layout;
pub(crate) mod compact;
mod conflict_ranges;
pub(crate) mod cross_shard;
pub(crate) mod delete;
pub(crate) mod delete_batch;
pub(crate) mod directory;
mod encode;
pub(crate) mod io_budget;
mod output;
pub(crate) mod page_group;
pub(crate) mod planner;
pub(crate) mod projected_batch;
pub(crate) mod query;
pub(crate) mod raw_cursor;
#[cfg(test)]
mod raw_cursor_tests;
pub(crate) mod reader;
mod repair;
pub(crate) mod segment;
pub(crate) mod snapshot;
mod spool;
pub(crate) mod store;
mod summarize;
pub(crate) mod tombstone;
pub(crate) mod tree;
pub(crate) mod writer;
pub(crate) use summarize::{AggScalar, AggSum, AggValues};

pub(crate) type Level = u8;
pub(crate) const LEVEL_BLOCK: Level = 1;
pub(crate) const LEVEL_DIRTY: Level = 0xfe;
pub(crate) const FAN_OUT: usize = 64;
pub(crate) const MIN_CHILDREN: usize = FAN_OUT / 2;
pub(crate) const PAGE_GROUP: usize = 256;
pub(crate) const MAX_LEVELS: Level = 5;

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("invalid aggregate index sequence number"))]
    InvalidSequence,
    #[snafu(display("VLog block location exceeds aggregate index limits"))]
    InvalidBlockLocation,
    #[snafu(display("invalid aggregate index record"))]
    InvalidRecord,
    #[snafu(display("aggregate input types do not match"))]
    TypeMismatch,
    #[snafu(display("aggregate input timestamps must be strictly increasing"))]
    InvalidTimeOrder,
    #[snafu(display("aggregate arithmetic overflow"))]
    Overflow,
    #[snafu(display("invalid aggregate index format: {reason}"))]
    InvalidFormat { reason: &'static str },
    #[snafu(display("aggregate index IO failed: {source}"))]
    Io { source: std::io::Error },
    #[snafu(display(
        "aggregate VLog {operation} failed for file {file_no}, block offset {offset}, size {size}: {source}"
    ))]
    VLogBlock {
        operation: &'static str,
        file_no: u32,
        offset: u32,
        size: u32,
        source: Box<super::reader::Error>,
    },
    #[snafu(display("aggregate index file size limit exceeded"))]
    MaxFileExceed,
    #[snafu(display("aggregate segment exceeds its staging budget"))]
    SegmentBudgetExceeded,
    #[snafu(display("aggregate tree capacity exceeded"))]
    TreeCapacityExceeded,
    #[snafu(display("aggregate index compaction was cancelled"))]
    CompactionCancelled,
    #[snafu(display("aggregate index compaction is already running"))]
    CompactionBusy,
    #[snafu(display("aggregate index inputs overlap and require raw-data repair"))]
    OverlappingInputs,
    #[snafu(display("local aggregate repair exceeds its metadata budget"))]
    RepairBudgetExceeded,
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::Io { source }
    }
}

pub(super) fn invalid(reason: &'static str) -> Error {
    Error::InvalidFormat { reason }
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// A database series ID and a nonzero measurement-local field ordinal.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SeqNo(u64);

impl SeqNo {
    pub(crate) fn new(series_id: u64, field_ordinal: u8) -> Result<Self> {
        if series_id == 0 || series_id > u64::MAX >> 8 || field_ordinal == 0 {
            return Err(Error::InvalidSequence);
        }
        Ok(Self((series_id << 8) | u64::from(field_ordinal)))
    }

    pub(crate) fn from_raw(value: u64) -> Result<Self> {
        Self::new(value >> 8, value as u8)
    }

    pub(crate) fn get(self) -> u64 {
        self.0
    }
    pub(crate) fn series_id(self) -> u64 {
        self.0 >> 8
    }
    pub(crate) fn field_ordinal(self) -> u8 {
        self.0 as u8
    }
}

/// Packed VLog identity and byte offset; zero is reserved for summary records.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BlockLoc {
    pub(crate) file_no: u32,
    pub(crate) offset: u32,
}

impl BlockLoc {
    pub(crate) fn from_vlog(generation: i64, seq: i64, offset: i64) -> Result<Self> {
        if !(1..1 << 24).contains(&generation)
            || !(0..256).contains(&seq)
            || !(5..=i64::from(u32::MAX)).contains(&offset)
        {
            return Err(Error::InvalidBlockLocation);
        }
        Ok(Self {
            file_no: ((generation as u32) << 8) | seq as u32,
            offset: offset as u32,
        })
    }

    pub(crate) fn generation(self) -> i64 {
        i64::from(self.file_no >> 8)
    }
    pub(crate) fn seq(self) -> i64 {
        i64::from(self.file_no & 0xff)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Position {
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) block: BlockLoc,
    /// Physical size for blocks; a saturating estimate for summaries.
    pub(crate) block_size: u32,
    pub(crate) flags: u8,
    /// Number of direct children; zero for leaf blocks and dirty intervals.
    pub(crate) child_count: u8,
    /// Original write identity for relocated blocks; zero uses the physical file.
    pub(crate) version: u32,
}

impl Position {
    pub(crate) fn read_error(self, operation: &'static str, source: super::reader::Error) -> Error {
        Error::VLogBlock {
            operation,
            file_no: self.block.file_no,
            offset: self.block.offset,
            size: self.block_size,
            source: Box::new(source),
        }
    }

    pub(crate) fn write_version(self) -> u32 {
        if self.version == 0 {
            self.block.file_no
        } else {
            self.version
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Record {
    pub(crate) seq_no: SeqNo,
    pub(crate) level: Level,
    pub(crate) pos: Position,
    pub(crate) values: AggValues,
}

impl Record {
    pub(crate) fn from_vlog_index(
        seq_no: SeqNo,
        index: &VLogIndex,
        values: AggValues,
    ) -> Result<Self> {
        if index.count <= 0
            || index.min > index.max
            || index.size < 5
            || index.size > i64::from(u32::MAX)
            || values.count != index.count as u64
            || values.field_type() != index.typ
        {
            return Err(Error::InvalidRecord);
        }
        Ok(Self {
            seq_no,
            level: LEVEL_BLOCK,
            pos: Position {
                start: index.min,
                end: index.max,
                block: BlockLoc::from_vlog(index.generation, index.seq, index.offset)?,
                block_size: index.size as u32,
                flags: 0,
                child_count: 0,
                version: 0,
            },
            values,
        })
    }

    pub(crate) fn to_vlog_index(&self) -> Result<VLogIndex> {
        if self.level != LEVEL_BLOCK
            || self.pos.child_count != 0
            || self.pos.start > self.pos.end
            || self.pos.block_size < 5
            || self.values.count == 0
        {
            return Err(Error::InvalidRecord);
        }
        let location = self.pos.block;
        BlockLoc::from_vlog(
            location.generation(),
            location.seq(),
            i64::from(location.offset),
        )?;
        Ok(VLogIndex {
            aggregate: None,
            count: i32::try_from(self.values.count).map_err(|_| Error::Overflow)?,
            typ: self.values.field_type(),
            min: self.pos.start,
            max: self.pos.end,
            generation: location.generation(),
            seq: location.seq(),
            offset: i64::from(location.offset),
            size: i64::from(self.pos.block_size),
        })
    }

    /// Builds a complete internal node. The caller supplies every direct child.
    pub(crate) fn summarize(children: &[Self]) -> Result<Self> {
        if !(MIN_CHILDREN..=FAN_OUT).contains(&children.len()) {
            return Err(Error::InvalidRecord);
        }
        let first = children[0];
        if first.level < LEVEL_BLOCK
            || first.level >= MAX_LEVELS
            || first.pos.start > first.pos.end
            || first.values.count == 0
        {
            return Err(Error::InvalidRecord);
        }
        let mut result = first;
        for child in children {
            page_group::validate_record(child, first.values.field_type())?;
        }
        result.level += 1;
        result.pos.block = BlockLoc::default();
        result.pos.version = 0;
        result.pos.child_count = children.len() as u8;
        for child in &children[1..] {
            if child.seq_no != first.seq_no
                || child.level != first.level
                || child.pos.start > child.pos.end
                || child.pos.start <= result.pos.end
            {
                return Err(Error::InvalidRecord);
            }
            result.values = result.values.merge_ordered(child.values)?;
            result.pos.end = child.pos.end;
            result.pos.block_size = result.pos.block_size.saturating_add(child.pos.block_size);
            result.pos.flags |= child.pos.flags;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tree_tests;
