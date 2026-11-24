use std::{
    any::{self, Any},
    cell::RefCell,
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    f64::consts::E,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

use crate::models::points::Val;
use crossbeam_channel::select;
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};
use tokio_util::sync::CancellationToken;

// DefaultStatsInterval is the default value for IteratorEncoder.StatsInterval.
pub const DEFAULT_STATS_INTERVAL: Duration = std::time::Duration::from_secs(1);

/// Error type for query iterator.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for query iterator.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// IteratorCost contains statistics retrieved for explaining what potential
// cost may be incurred by instantiating an iterator.
#[derive(Default)]
pub struct IteratorCost {
    // The total number of shards that are touched by this query.
    pub num_shards: i64,

    // The total number of non-unique series that are accessed by this query.
    // This number matches the number of cursors created by the query since
    // one cursor is created for every series.
    pub num_series: i64,

    // CachedValues returns the number of cached values that may be read by this
    // query.
    pub cached_values: i64,

    // The total number of non-unique files that may be accessed by this query.
    // This will count the number of files accessed by each series so files
    // will likely be double counted.
    pub num_files: i64,

    // The number of blocks that had the potential to be accessed.
    pub blocks_read: i64,

    // The amount of data that can be potentially read.
    pub block_size: i64,
}

/// IteratorStats represents statistics about an iterator.
/// Some statistics are available immediately upon iterator creation while
/// some are derived as the iterator processes data.
#[derive(Default, Clone)]
pub struct IteratorStats {
    pub series_n: i32, // series represented
    pub point_n: i32,  // points returned
}

impl IteratorStats {
    /// Add aggregates fields from self and other together. Overwrites self.
    pub fn add(&mut self, other: &IteratorStats) {
        self.series_n += other.series_n;
        self.point_n += other.point_n;
    }
}
