use std::{fmt::Display, sync::Arc};

use snafu::{ResultExt, Snafu, ensure};
use tokio::sync::{
    mpsc,
    oneshot::{self, Receiver as OneshotReceiver, Sender as OneshotSender},
};

pub mod compact;
pub(crate) mod aggregate_reader;
pub mod default_compact_plan;
pub mod file_store;
pub(crate) mod index;
pub mod key_cursor;
pub mod lazy_compact_plan;
pub mod query_iterator;
pub mod reader;
pub mod tombstone;
pub mod vlog_index_cache;
pub mod vlog_index_ring;
pub mod vlog_index_wal;
pub mod vlog_wal;
pub mod writer;

pub enum WriteVLogType {
    Write,
    Update,
    Delete,
}

pub enum WriteVLogEntry {
    WriteEntry(WriteEntry),
    DeleteEntry(DeleteEntry),
}

pub struct WriteEntry {
    key: Arc<Vec<u8>>,
    block: Vec<u8>,
    min_time: i64,
    max_time: i64,
}

pub struct DeleteEntry {
    pub keys: Vec<Vec<u8>>,
    pub size: usize,
}

#[derive(Clone, Default, Debug)]
pub struct VLogIndex {
    pub(crate) aggregate: Option<std::sync::Arc<aggregate_record::AggregateRecord>>,
    pub count: i32,
    pub typ: u8,
    pub min: i64,
    pub max: i64,
    pub generation: i64,
    pub seq: i64,
    pub offset: i64,
    pub size: i64,
}

impl VLogIndex {
    pub const SIZE: usize = size_of::<i32>()  // count
        + size_of::<u8>() // typ
        + size_of::<i64>() // min
        + size_of::<i64>() // max
        + size_of::<i64>() // gen
        + size_of::<i64>() // seq
        + size_of::<i64>() // offset
        + size_of::<i64>();

    pub(crate) fn memory_size(&self) -> usize {
        Self::SIZE + self.aggregate.as_ref().map_or(0, |_| {
            std::mem::size_of::<aggregate_record::AggregateRecord>() + 2 * std::mem::size_of::<usize>()
        })
    }

    /// The min and max times are inclusive.
    pub fn contains(&self, t: i64) -> bool {
        self.min <= t && t <= self.max
    }

    /// Returns true if the given time ranges are completely within the index's time bounds.
    pub fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        self.min <= max && self.max >= min
    }
}

impl Display for VLogIndex {
    // String returns the string representation of the value and its timestamp.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "count={},type={},min={},max={},gen={},seq={},offset={},size={}",
            self.count,
            self.typ,
            self.min,
            self.max,
            self.generation,
            self.seq,
            self.offset,
            self.size
        )
    }
}

// Aggregates extend a locator; they do not change native locator identity.
impl PartialEq for VLogIndex {
    fn eq(&self, other: &Self) -> bool {
        (self.count, self.typ, self.min, self.max, self.generation, self.seq, self.offset, self.size)
            == (other.count, other.typ, other.min, other.max, other.generation, other.seq, other.offset, other.size)
    }
}
impl Eq for VLogIndex {}
pub(crate) mod aggregate_record;
