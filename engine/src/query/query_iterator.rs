#[cfg(feature = "slow-trace")]
use std::time::Instant;
use std::{
    any::{self, Any},
    cmp::Ordering,
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

use crate::models::{
    points::Val,
    time::{MAX_NANO_TIME_I64, MIN_NANO_TIME_I64, ZERO_TIME},
};
use parking_lot::RwLock;
use snafu::Snafu;
use tokio_util::sync::CancellationToken;

use super::query_point;
#[cfg(feature = "slow-trace")]
use crate::query::storage_ir::GroupedAggregateProfileMetrics;
use crate::query::storage_ir::{
    AggregateState, GroupedAggregateKey, GroupedAggregateTarget, GroupedAggregateWindow,
    OutputOrder, StorageLimit, StorageScan, TimeOrder,
};
use crate::storage::active_cursor::{
    self as iterator, BoolIter, FloatIter, IntegerIter, StringIter, UnsignedIter,
};

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

/// The physical value type produced by a storage iterator.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IteratorDataType {
    #[default]
    Unknown,
    Integer,
    Unsigned,
    Float,
    Boolean,
    String,
}

// Keep the generated iterator implementations compact while the public
// contract exposes the storage-owned name above.
use IteratorDataType as VarRefDataType;

pub trait QueryIterator {
    fn data_type(&self) -> VarRefDataType;
    fn stats(&self) -> IteratorStats;
    fn close(&mut self);
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

pub trait IntegerIterator: QueryIterator {
    fn next(&mut self) -> Option<query_point::IntegerPoint>;
}

pub trait UnsignedIterator: QueryIterator {
    fn next(&mut self) -> Option<query_point::UnsignedPoint>;
}

pub trait FloatIterator: QueryIterator {
    fn next(&mut self) -> Option<query_point::FloatPoint>;
}

pub trait BoolIterator: QueryIterator {
    fn next(&mut self) -> Option<query_point::BoolPoint>;
}

pub trait StringIterator: QueryIterator {
    fn next(&mut self) -> Option<query_point::StringPoint>;
}

#[derive(Clone)]
struct MergeOptions {
    dimensions: Vec<String>,
    start_time: i64,
    end_time: i64,
    ascending: bool,
    output_order: OutputOrder,
    interval: Option<(i64, i64)>,
}

impl MergeOptions {
    fn from_storage_scan(scan: &StorageScan) -> Self {
        Self {
            dimensions: scan.projection.series.identity_tag_keys.clone(),
            start_time: scan.time_range.start_time,
            end_time: scan.time_range.end_time,
            ascending: scan.time_order == TimeOrder::Ascending,
            output_order: scan.output_order,
            interval: scan
                .execution
                .window
                .map(|window| (window.duration, window.offset)),
        }
    }

    fn merge_sorted(&self) -> bool {
        self.output_order == OutputOrder::TimeOrdered
    }

    fn window(&self, timestamp: i64) -> (i64, i64) {
        let Some((duration, offset)) = self.interval else {
            return (self.start_time, self.end_time + 1);
        };

        let timestamp = timestamp - offset;
        let mut delta = timestamp % duration;
        if delta < 0 {
            delta += duration;
        }
        let mut start = if MIN_NANO_TIME_I64 + delta >= timestamp {
            MIN_NANO_TIME_I64
        } else {
            timestamp - delta
        };
        start += offset;

        let delta = duration - delta;
        let mut end = if MAX_NANO_TIME_I64 <= delta {
            MAX_NANO_TIME_I64
        } else {
            timestamp + delta
        };
        end += offset;
        (start, end)
    }
}

/// Iterators represents a list of iterators.
pub struct Iterators(pub Vec<Box<dyn QueryIterator + Sync + Send>>);

impl Iterators {
    pub fn into_inner(self) -> Vec<Box<dyn QueryIterator + Sync + Send>> {
        self.0
    }

    fn data_type(&self) -> VarRefDataType {
        if self.is_empty() {
            return VarRefDataType::Unknown;
        }

        self.0[0].data_type()
    }

    /// coerce forces an array of iterators to be a single type.
    /// Iterators that are not of the same type as the first element in the slice
    /// will be closed and dropped.
    fn coere(self) -> Vec<Box<dyn QueryIterator + Sync + Send>> {
        let typ = self.data_type();
        match typ {
            VarRefDataType::Integer => new_integer_iters(self.into_inner()),
            VarRefDataType::Unsigned => new_unsigned_iters(self.into_inner()),
            VarRefDataType::Float => new_float_iters(self.into_inner()),
            VarRefDataType::Boolean => new_bool_iters(self.into_inner()),
            VarRefDataType::String => new_string_iters(self.into_inner()),
            _ => self.into_inner(),
        }
    }

    /// Returns an iterator that will stop producing output
    /// when the passed-in channel is closed.
    pub fn new_interrupt_iterator(
        input: Box<dyn QueryIterator + Sync + Send>,
        closing: CancellationToken,
    ) -> Box<dyn QueryIterator + Sync + Send> {
        match (*input).as_any().type_id() {
            id if id == any::TypeId::of::<IntegerIter>() => {
                let input = input.into_any();
                Box::new(IntegerInterruptIter::new(
                    input.downcast::<IntegerIter>().unwrap()
                        as Box<dyn IntegerIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<IntegerSortedMergeIter>() => {
                let input = input.into_any();
                Box::new(IntegerInterruptIter::new(
                    input.downcast::<IntegerSortedMergeIter>().unwrap()
                        as Box<dyn IntegerIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<IntegerInterruptIter>() => {
                let input = input.into_any();
                Box::new(IntegerInterruptIter::new(
                    input.downcast::<IntegerInterruptIter>().unwrap()
                        as Box<dyn IntegerIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<UnsignedIter>() => {
                let input = input.into_any();
                Box::new(UnsignedInterruptIter::new(
                    input.downcast::<UnsignedIter>().unwrap()
                        as Box<dyn UnsignedIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<UnsignedSortedMergeIter>() => {
                let input = input.into_any();
                Box::new(UnsignedInterruptIter::new(
                    input.downcast::<UnsignedSortedMergeIter>().unwrap()
                        as Box<dyn UnsignedIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<UnsignedInterruptIter>() => {
                let input = input.into_any();
                Box::new(UnsignedInterruptIter::new(
                    input.downcast::<UnsignedInterruptIter>().unwrap()
                        as Box<dyn UnsignedIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<FloatIter>() => {
                let input = input.into_any();
                Box::new(FloatInterruptIter::new(
                    input.downcast::<FloatIter>().unwrap() as Box<dyn FloatIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<FloatSortedMergeIter>() => {
                let input = input.into_any();
                Box::new(FloatInterruptIter::new(
                    input.downcast::<FloatSortedMergeIter>().unwrap()
                        as Box<dyn FloatIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<FloatInterruptIter>() => {
                let input = input.into_any();
                Box::new(FloatInterruptIter::new(
                    input.downcast::<FloatInterruptIter>().unwrap()
                        as Box<dyn FloatIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<BoolIter>() => {
                let input = input.into_any();
                Box::new(BoolInterruptIter::new(
                    input.downcast::<BoolIter>().unwrap() as Box<dyn BoolIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<BoolSortedMergeIter>() => {
                let input = input.into_any();
                Box::new(BoolInterruptIter::new(
                    input.downcast::<BoolSortedMergeIter>().unwrap()
                        as Box<dyn BoolIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<BoolInterruptIter>() => {
                let input = input.into_any();
                Box::new(BoolInterruptIter::new(
                    input.downcast::<BoolInterruptIter>().unwrap()
                        as Box<dyn BoolIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<StringIter>() => {
                let input = input.into_any();
                Box::new(StringInterruptIter::new(
                    input.downcast::<StringIter>().unwrap()
                        as Box<dyn StringIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<StringSortedMergeIter>() => {
                let input = input.into_any();
                Box::new(StringInterruptIter::new(
                    input.downcast::<StringSortedMergeIter>().unwrap()
                        as Box<dyn StringIterator + Sync + Send>,
                    closing,
                ))
            }
            id if id == any::TypeId::of::<StringInterruptIter>() => {
                let input = input.into_any();
                Box::new(StringInterruptIter::new(
                    input.downcast::<StringInterruptIter>().unwrap()
                        as Box<dyn StringIterator + Sync + Send>,
                    closing,
                ))
            }
            id => panic!("unsupported sorted merge iterator type: {:?}", id),
        }
    }
}

impl QueryIterator for Iterators {
    fn data_type(&self) -> VarRefDataType {
        self.0[0].data_type()
    }

    /// Returns the aggregation of all iterator stats.
    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        for itr in self.iter() {
            stats.add(&itr.stats())
        }
        stats
    }

    fn close(&mut self) {
        for itr in self.iter_mut() {
            itr.close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl Deref for Iterators {
    type Target = Vec<Box<dyn QueryIterator + Sync + Send>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Iterators {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Interval represents a repeating interval for a query.
/// Basic GROUP BY time() queries require an function in the
/// SELECT clause and a time range in the WHERE clause.
#[derive(Clone, Default)]
pub struct Interval {
    // The duration in the GROUP BY time() clause is a duration literal.
    // It determines how database groups query results over time.
    pub duration: Duration,
    // The offset_interval is a duration literal. It shifts forward or back
    // the database’s preset time boundaries. The offset_interval can be positive
    //  or negative.
    pub offset: Duration,
}

impl Interval {
    /// Returns true if the interval has no duration.
    pub fn is_zero(&self) -> bool {
        self.duration.is_zero()
    }
}

// SELECT usage,core FROM cpu_usage WHERE "region"='beijing' AND ("host"='server1' OR "host"='server2') AND time >= '2020-10-01T08:00:00.000000000+08:00' AND time < '2025-10-01T08:00:05.000000000+08:00' GROUP BY "A","host","X","region","C","X","X","D","C"
/*
expr(type is query IR Expr): None

aux(type is query IR VarRef):
    [0](VarRef): val=core type=float
    [1](VarRef): val=usage type=float

dimensions:
    [0] "A"
    [1] "host"
    [2] "X"
    [3] "region"
    [4] "C"
    [5] "X"
    [6] "X"
    [7] "D"
    [8] "C"

group_by:
    [0] "A"
    [1] "host"
    [2] "X"
    [3] "region"
    [4] "C"
    [5] "D"

condition(query IR BinaryExpr):
    op: AND
    lhs((query IR BinaryExpr)): [
        "EQ"
        LHS(VarRef):
                val="region"
                type=Tag
        RHS(StringLiteral):
                val="beijing"
    ]
    rhs((query IR ParenExpr)): [
        "OR"
        LHS(BinaryExpr): [
            "EQ"
            LHS(VarRef):
                val="host"
                type=Tag
            RHS(StringLiteral):
                val="server1"
        ],
        RHS(BinaryExpr): [
            "EQ"
            LHS(VarRef):
                val="host"
                type=Tag
            RHS(StringLiteral):
                val="server2"
        ]
    ]
*/

// SELECT mean("usage") FROM "cpu_usage" WHERE "region"='beijing' AND ("host"='server1' OR "host"='server2') AND time >= '2020-10-01T08:00:00.000000000+08:00' AND time < '2025-10-01T08:00:05.000000000+08:00' GROUP BY region, time(1d) fill(0)
/*
expr(query IR Call):
    name: mean
    args:
          val=usage
          type=float

aux: None

dimensions:
    [0]:region
group_by:
    [0]:region
condition(query IR BinaryExpr):
    op: AND
    lhs:...
    rhs:...
*/

// /// IteratorOptions is an object passed to CreateIterator to specify creation options.
// #[derive(Default)]
// pub struct IteratorOptions {
//     pub vlog_enabled: bool,
//     // // OrgID is the organization for which this query is being executed.
//     // OrgID platform.ID

//     // None
//     // Reference cursor created for SELECT projections that include a function,
//     //  such as or .last()mean()
//     // Expression to iterate for, this can be VarRef or a Call.
//     // It is used to build main cursor.
//     pub expr: Option<Box<dyn Expr>>,

//     // Auxiliary cursor created for simple expression projections
//     // (not selectors or an aggregation).
//     // For example, SELECT foo FROM m or SELECT foo+bar FROM m, where foo and bar are fields.
//     // Auxiliary tags or values to also retrieve for the point.
//     // It is used to build aux cursor.
//     pub aux: Vec<VarRef>,

//     // // Data sources from which to receive data. This is only used for encoding
//     // // measurements over RPC and is no longer used in the open source version.
//     // Sources []agilorql.Source

//     // The following group of parameters is used to track group by interval and tags.
//     // GROUP BY "A","host","X","region","C","X","X","D","C"
//     // interval: Zero
//     // dimensions: vec!["A","host","X","region","C","X","X","D","C"];
//     // group_by: HashSet["A","host","X","region","C","D"]
//     // Group by interval and tags.
//     pub interval: Interval,
//     // 如果没有where，那么 dimensions和group_by为empty
//     pub dimensions: Vec<String>, // The final dimensions of the query (stays the same even in subqueries).  // group by
//     pub group_by: HashSet<String>, // Dimensions to group points by in intermediate iterators.
//     // Location   *time.location,

//     // Fill options.
//     pub fill: ql::FillOption,
//     pub fill_value: Val,

//     // Condition cursor created for fields referenced in a WHERE clause.
//     // // Condition to filter by.
//     pub condition: Option<Box<dyn Expr>>,

//     // Time range for the iterator.
//     pub start_time: i64,
//     pub end_time: i64, // include

//     // Sorted in time ascending order if true.
//     pub ascending: bool,

//     // Limits the number of points per series.
//     pub limit: i64,
//     pub offset: i64,

//     // Limits the number of series.
//     pub slimit: i64,
//     pub soffset: i64,

//     // Removes the measurement name. Useful for meta queries.
//     pub strip_name: bool,

//     // Removes duplicate rows from raw queries.
//     pub dedupe: bool,

//     // Determines if this is a query for raw data or an aggregate/selector.
//     pub ordered: bool,

//     // Limits on the creation of iterators.
//     pub max_series_n: i64,

//     // If this channel is set and is closed, the iterator should try to exit
//     // and close as soon as possible.
//     pub interrupt: Option<tokio_util::sync::CancellationToken>,
//     // // Authorizer can limit access to data
//     // Authorizer Authorizer
// }

// impl IteratorOptions {
//     /// Returns true if the options require a sorted merge.
//     pub fn merge_sorted(&self) -> bool {
//         self.ordered
//     }

//     /// Returns the time the iterator should start from.
//     /// For ascending iterators this is the start time, for descending iterators it's the end time.
//     pub fn seek_time(&self) -> i64 {
//         if self.ascending {
//             return self.start_time;
//         }
//         self.end_time
//     }

//     /// Returns the time the iterator should end at.
//     /// For ascending iterators this is the end time, for descending iterators it's the start time.
//     pub fn stop_time(&self) -> i64 {
//         if self.ascending {
//             return self.end_time;
//         }
//         self.start_time
//     }

//     /// Returns the time window [start,end) that t falls within.
//     pub fn window(&self, t: i64) -> (i64, i64) {
//         if self.interval.is_zero() {
//             return (self.start_time, self.end_time + 1);
//         }

//         // Subtract the offset to the time so we calculate the correct base interval.
//         let t = t - self.interval.offset.as_nanos() as i64;

//         // Retrieve the zone offset for the start time.
//         let mut zone = 0i64;
//         // if opt.Location != nil {
//         //     _, zone = opt.Zone(t)
//         // }

//         // Truncate time by duration.
//         let mut dt = (t + zone) % (self.interval.duration.as_nanos() as i64);
//         if dt < 0 {
//             // Negative modulo rounds up instead of down, so offset
//             // with the duration.
//             dt += self.interval.duration.as_nanos() as i64;
//         }

//         // Find the start time.
//         let mut start = if MIN_NANO_TIME_I64 + dt >= t {
//             MIN_NANO_TIME_I64
//         } else {
//             t - dt
//         };

//         start += self.interval.offset.as_nanos() as i64;

//         // Look for the start offset again because the first time may have been
//         // after the offset switch. Now that we are at midnight in UTC, we can
//         // lookup the zone offset again to get the real starting offset.
//         // if opt.Location != nil {
//         //     _, startOffset := opt.Zone(start)
//         //     // Do not adjust the offset if the offset change is greater than or
//         //     // equal to the duration.
//         //     if o := zone - startOffset; o != 0 && abs(o) < int64(opt.Interval.Duration) {
//         //         start += o
//         //     }
//         // }

//         // Find the end time.
//         let dt = self.interval.duration.as_nanos() as i64 - dt;
//         let mut end = if MAX_NANO_TIME_I64 <= dt {
//             MAX_NANO_TIME_I64
//         } else {
//             t + dt
//         };

//         // Retrieve the zone offset for the end time.
//         // if opt.Location != nil {
//         //     _, endOffset := opt.Zone(end)
//         //     // Adjust the end time if the offset is different from the start offset.
//         //     // Only apply the offset if it is smaller than the duration.
//         //     // This prevents going back in time and creating time windows
//         //     // that don't make any sense.
//         //     if o := zone - endOffset; o != 0 && abs(o) < int64(opt.Interval.Duration) {
//         //         // If the offset is greater than 0, that means we are adding time.
//         //         // Added time goes into the previous interval because the clocks
//         //         // move backwards. If the offset is less than 0, then we are skipping
//         //         // time. Skipped time comes after the switch so if we have a time
//         //         // interval that lands on the switch, it comes from the next
//         //         // interval and not the current one. For this reason, we need to know
//         //         // when the actual switch happens by seeing if the time switch is within
//         //         // the current interval. We calculate the zone offset with the offset
//         //         // and see if the value is the same. If it is, we apply the
//         //         // offset.
//         //         if o > 0 {
//         //             end += o
//         //         } else if _, z := opt.Zone(end + o); z == endOffset {
//         //             end += o
//         //         }
//         //     }
//         // }

//         end += self.interval.offset.as_nanos() as i64;

//         (start, end)
//     }

//     /// Returns the time interval for the derivative function.
//     pub fn derivative_interval(&self) -> Interval {
//         todo!("implemet me "); // used for functions
//     }

//     /// Returns the time interval for the elapsed function.
//     pub fn elapsed_interval(&self) -> Interval {
//         todo!("implemet me "); // used for functions
//     }

//     /// Returns the time interval for the integral function.
//     pub fn integral_interval(&self) -> Interval {
//         todo!("implemet me "); // used for functions
//     }

//     /// Retrieves the dimensions for this query.
//     pub fn get_dimensions(&self) -> Vec<String> {
//         if !self.group_by.is_empty() {
//             let mut dims = Vec::with_capacity(self.group_by.len());
//             for dim in self.group_by.iter() {
//                 dims.push(dim.clone());
//             }
//             return dims;
//         }
//         self.dimensions.clone()
//     }

//     /// Returns the zone information for the given time. The offset is in nanoseconds.
//     pub fn zone(&self, ns: i64) -> (String, i64) {
//         todo!("implemet me ");
//     }
// }

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

// ////////////////////////////////////////////////
// ///////// query/iterator.gen ///////////////////
// ////////////////////////////////////////////////
// ////////////////////////////////////////////////

fn new_integer_iters(
    itrs: Vec<Box<dyn QueryIterator + Sync + Send>>,
) -> Vec<Box<dyn QueryIterator + Sync + Send>> {
    let mut a = vec![];
    for mut itr in itrs {
        if itr.data_type() == VarRefDataType::Integer {
            a.push(itr);
        } else {
            itr.close();
        }
    }

    a
}

fn new_unsigned_iters(
    itrs: Vec<Box<dyn QueryIterator + Sync + Send>>,
) -> Vec<Box<dyn QueryIterator + Sync + Send>> {
    let mut a = vec![];
    for mut itr in itrs {
        if itr.data_type() == VarRefDataType::Unsigned {
            a.push(itr);
        } else {
            itr.close();
        }
    }

    a
}

fn new_float_iters(
    itrs: Vec<Box<dyn QueryIterator + Sync + Send>>,
) -> Vec<Box<dyn QueryIterator + Sync + Send>> {
    let mut a = vec![];
    for mut itr in itrs {
        if itr.data_type() == VarRefDataType::Float {
            a.push(itr);
        } else {
            itr.close();
        }
    }

    a
}

fn new_bool_iters(
    itrs: Vec<Box<dyn QueryIterator + Sync + Send>>,
) -> Vec<Box<dyn QueryIterator + Sync + Send>> {
    let mut a = vec![];
    for mut itr in itrs {
        if itr.data_type() == VarRefDataType::Boolean {
            a.push(itr);
        } else {
            itr.close();
        }
    }

    a
}

fn new_string_iters(
    itrs: Vec<Box<dyn QueryIterator + Sync + Send>>,
) -> Vec<Box<dyn QueryIterator + Sync + Send>> {
    let mut a = vec![];
    for mut itr in itrs {
        if itr.data_type() == VarRefDataType::String {
            a.push(itr);
        } else {
            itr.close();
        }
    }

    a
}

// ////////////////////////////////////////////////////////////
// /////////////// IntegerMergeIter   /////////////////////////
// ////////////////////////////////////////////////////////////

#[derive(Default)]
struct Window {
    name: String,
    tags: String,
    start_time: i64,
    end_time: i64,
}

/// Represents a buffered IntegerIterator.

#[derive(Clone)]
struct BufIntegerIter {
    itr: Arc<RwLock<Box<dyn IntegerIterator + Sync + Send>>>,
    buf: Option<query_point::IntegerPoint>,
}

impl BufIntegerIter {
    fn new(itr: Arc<RwLock<Box<dyn IntegerIterator + Sync + Send>>>) -> Self {
        Self { itr, buf: None }
    }

    /// Returns the next point without removing it from the iterator.
    fn peek(&mut self) -> Option<query_point::IntegerPoint> {
        let p = self.next();
        self.unread(p.clone());
        p
    }

    /// Returns the time of the next point.
    /// Returns zero time if no more points available.
    fn peek_time(&mut self) -> i64 {
        if let Some(p) = self.peek() {
            return p.time;
        }
        ZERO_TIME
    }

    /// Returns the next value if it is between [start_time, end_time).
    /// If the next value is outside the range then it is moved to the buffer.
    fn next_window(&mut self, start_time: i64, end_time: i64) -> Option<query_point::IntegerPoint> {
        let v = self.next();
        if let Some(v) = v {
            let t = v.time;
            if t >= end_time || t < start_time {
                self.unread(Some(v));
                return None;
            }
            return Some(v);
        }

        None
    }

    /// Sets v to the buffer. It is read on the next call to next().
    fn unread(&mut self, v: Option<query_point::IntegerPoint>) {
        self.buf = v;
    }
}

impl QueryIterator for BufIntegerIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Integer
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        self.itr.read().stats()
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        self.itr.write().close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl IntegerIterator for BufIntegerIter {
    /// Returns the current buffer, if exists, or calls the underlying iterator.
    fn next(&mut self) -> Option<query_point::IntegerPoint> {
        let buf = self.buf.clone();
        if buf.is_some() {
            self.buf = None;
            return buf;
        }
        self.itr.write().next()
    }
}

/// IntegerMergeIter Represents an iterator that combines multiple integer iterators.
pub struct IntegerMergeIter {
    inner: RwLock<IntegerMergeIterInner>,
}

struct IntegerMergeIterInner {
    inputs: Vec<Arc<RwLock<Box<dyn IntegerIterator + Sync + Send>>>>,
    heap: IntegerMergeHeap,
    init: bool,

    closed: bool,

    // Current iterator and window.
    curr: Option<IntegerMergeHeapItem>,
    window: Window,
}

impl IntegerMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn IntegerIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut inner = IntegerMergeIterInner {
            inputs: vec![],
            heap: IntegerMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
            },
            init: false,

            closed: false,

            curr: None,
            window: Window::default(),
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));
            // Wrap in buffer, ignore any inputs without anymore points.
            let buf_input = BufIntegerIter::new(Arc::clone(&input));
            inner.inputs.push(input);
            // Append to the heap.
            inner
                .heap
                .items
                .push(IntegerMergeHeapItem { itr: buf_input });
        }

        Self {
            inner: RwLock::new(inner),
        }
    }
}

impl QueryIterator for IntegerMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Integer
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        let inner = self.inner.read();
        for input in inner.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        let mut inner = self.inner.write();
        for input in inner.inputs.iter_mut() {
            input.write().close();
        }

        inner.curr = None;
        inner.inputs = vec![];
        inner.heap.items = vec![];
        inner.closed = true;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl IntegerIterator for IntegerMergeIter {
    fn next(&mut self) -> Option<query_point::IntegerPoint> {
        let mut inner = self.inner.write();
        if inner.closed {
            return None;
        }

        // Initialize the heap. This needs to be done lazily on the first call to this iterator
        // so that iterator initialization done through the Select() call returns quickly.
        // Queries can only be interrupted after the Select() call completes so any operations
        // done during iterator creation cannot be interrupted, which is why we do it here
        // instead so an interrupt can happen while initializing the heap.
        if !inner.init {
            let items_len = inner.heap.items.len();
            let items = std::mem::replace(&mut inner.heap.items, Vec::with_capacity(items_len));
            for mut item in items {
                if item.itr.peek().is_none() {
                    continue;
                }
                inner.heap.items.push(item);
            }

            inner.heap.init(); // make heap
            inner.init = true;
        }

        loop {
            // Retrieve the next iterator if we don't have one.
            if inner.curr.is_none() {
                if inner.heap.items.is_empty() {
                    return None;
                }
                inner.curr = inner.heap.pop();

                // Read point and set current window.
                if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                    let tags = p.tags.subset(&inner.heap.opt.dimensions);
                    (inner.window.name, inner.window.tags) = (p.name.clone(), tags.id());
                    (inner.window.start_time, inner.window.end_time) =
                        inner.heap.opt.window(p.time);
                    return Some(p);
                }

                return None;
            }

            // Read the next point from the current iterator.
            // If there are no more points then remove iterator from heap and find next.;
            let p = if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                p
            } else {
                inner.curr = None;
                continue;
            };

            // Check if the point is inside of our current window.
            let mut in_window = true;
            let window = &inner.window;
            if window.name != p.name {
                in_window = false;
            } else {
                let tags = p.tags.subset(&inner.heap.opt.dimensions);
                if window.tags != tags.id() {
                    in_window = false;
                } else if inner.heap.opt.ascending && p.time >= window.end_time {
                    in_window = false;
                } else if !inner.heap.opt.ascending && p.time < window.start_time {
                    in_window = false;
                }
            }

            // If it's outside our window then push iterator back on the heap and find new iterator.
            if !in_window {
                inner.curr.as_mut().unwrap().itr.unread(Some(p));
                let item = std::mem::take(&mut inner.curr).unwrap();
                inner.heap.push(item);
                continue;
            }

            return Some(p);
        }
    }
}

/// IntegerMergeHeap represents a heap of IntegerMergeHeapItems. Items are sorted by their next window and then by name/tags.
struct IntegerMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<IntegerMergeHeapItem>,
}

impl IntegerMergeHeap {
    fn new(opt: Arc<MergeOptions>, items: Vec<IntegerMergeHeapItem>) -> Self {
        Self { opt, items }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            if j2 < n && self.less_heap_item(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.less_heap_item(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&mut self, i: usize, j: usize) -> bool {
        let (x, y) = (
            self.items[i].itr.peek().unwrap(),
            self.items[j].itr.next().unwrap(),
        );

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() < ytags.id();
                }
            }
        } else {
            if x.name != y.name {
                return x.name > y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() > ytags.id();
                }
            }
        }

        let (xt, _) = self.opt.window(x.time);
        let (yt, _) = self.opt.window(y.time);

        if self.opt.ascending {
            return xt < yt;
        }

        xt > yt
    }

    pub fn push(&mut self, item: IntegerMergeHeapItem) {
        self.items.push(item);
    }

    /// Removes the last element.
    pub fn pop(&mut self) -> Option<IntegerMergeHeapItem> {
        self.items.pop()
    }
}

#[derive(Clone)]
struct IntegerMergeHeapItem {
    itr: BufIntegerIter,
}

/// Represents a buffered UnsignedIterator.

#[derive(Clone)]
struct BufUnsignedIter {
    itr: Arc<RwLock<Box<dyn UnsignedIterator + Sync + Send>>>,
    buf: Option<query_point::UnsignedPoint>,
}

impl BufUnsignedIter {
    fn new(itr: Arc<RwLock<Box<dyn UnsignedIterator + Sync + Send>>>) -> Self {
        Self { itr, buf: None }
    }

    /// Returns the next point without removing it from the iterator.
    fn peek(&mut self) -> Option<query_point::UnsignedPoint> {
        let p = self.next();
        self.unread(p.clone());
        p
    }

    /// Returns the time of the next point.
    /// Returns zero time if no more points available.
    fn peek_time(&mut self) -> i64 {
        if let Some(p) = self.peek() {
            return p.time;
        }
        ZERO_TIME
    }

    /// Returns the next value if it is between [start_time, end_time).
    /// If the next value is outside the range then it is moved to the buffer.
    fn next_window(
        &mut self,
        start_time: i64,
        end_time: i64,
    ) -> Option<query_point::UnsignedPoint> {
        let v = self.next();
        if let Some(v) = v {
            let t = v.time;
            if t >= end_time || t < start_time {
                self.unread(Some(v));
                return None;
            }
            return Some(v);
        }

        None
    }

    /// Sets v to the buffer. It is read on the next call to next().
    fn unread(&mut self, v: Option<query_point::UnsignedPoint>) {
        self.buf = v;
    }
}

impl QueryIterator for BufUnsignedIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Unsigned
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        self.itr.read().stats()
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        self.itr.write().close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl UnsignedIterator for BufUnsignedIter {
    /// Returns the current buffer, if exists, or calls the underlying iterator.
    fn next(&mut self) -> Option<query_point::UnsignedPoint> {
        let buf = self.buf.clone();
        if buf.is_some() {
            self.buf = None;
            return buf;
        }
        self.itr.write().next()
    }
}

/// UnsignedMergeIter Represents an iterator that combines multiple unsigned iterators.
pub struct UnsignedMergeIter {
    inner: RwLock<UnsignedMergeIterInner>,
}

struct UnsignedMergeIterInner {
    inputs: Vec<Arc<RwLock<Box<dyn UnsignedIterator + Sync + Send>>>>,
    heap: UnsignedMergeHeap,
    init: bool,

    closed: bool,

    // Current iterator and window.
    curr: Option<UnsignedMergeHeapItem>,
    window: Window,
}

impl UnsignedMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn UnsignedIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut inner = UnsignedMergeIterInner {
            inputs: vec![],
            heap: UnsignedMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
            },
            init: false,

            closed: false,

            curr: None,
            window: Window::default(),
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));
            // Wrap in buffer, ignore any inputs without anymore points.
            let buf_input = BufUnsignedIter::new(Arc::clone(&input));
            inner.inputs.push(input);
            // Append to the heap.
            inner
                .heap
                .items
                .push(UnsignedMergeHeapItem { itr: buf_input });
        }

        Self {
            inner: RwLock::new(inner),
        }
    }
}

impl QueryIterator for UnsignedMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Unsigned
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        let inner = self.inner.read();
        for input in inner.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        let mut inner = self.inner.write();
        for input in inner.inputs.iter_mut() {
            input.write().close();
        }

        inner.curr = None;
        inner.inputs = vec![];
        inner.heap.items = vec![];
        inner.closed = true;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl UnsignedIterator for UnsignedMergeIter {
    fn next(&mut self) -> Option<query_point::UnsignedPoint> {
        let mut inner = self.inner.write();
        if inner.closed {
            return None;
        }

        // Initialize the heap. This needs to be done lazily on the first call to this iterator
        // so that iterator initialization done through the Select() call returns quickly.
        // Queries can only be interrupted after the Select() call completes so any operations
        // done during iterator creation cannot be interrupted, which is why we do it here
        // instead so an interrupt can happen while initializing the heap.
        if !inner.init {
            let items_len = inner.heap.items.len();
            let items = std::mem::replace(&mut inner.heap.items, Vec::with_capacity(items_len));
            for mut item in items {
                if item.itr.peek().is_none() {
                    continue;
                }
                inner.heap.items.push(item);
            }

            inner.heap.init(); // make heap
            inner.init = true;
        }

        loop {
            // Retrieve the next iterator if we don't have one.
            if inner.curr.is_none() {
                if inner.heap.items.is_empty() {
                    return None;
                }
                inner.curr = inner.heap.pop();

                // Read point and set current window.
                if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                    let tags = p.tags.subset(&inner.heap.opt.dimensions);
                    (inner.window.name, inner.window.tags) = (p.name.clone(), tags.id());
                    (inner.window.start_time, inner.window.end_time) =
                        inner.heap.opt.window(p.time);
                    return Some(p);
                }

                return None;
            }

            // Read the next point from the current iterator.
            // If there are no more points then remove iterator from heap and find next.;
            let p = if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                p
            } else {
                inner.curr = None;
                continue;
            };

            // Check if the point is inside of our current window.
            let mut in_window = true;
            let window = &inner.window;
            if window.name != p.name {
                in_window = false;
            } else {
                let tags = p.tags.subset(&inner.heap.opt.dimensions);
                if window.tags != tags.id() {
                    in_window = false;
                } else if inner.heap.opt.ascending && p.time >= window.end_time {
                    in_window = false;
                } else if !inner.heap.opt.ascending && p.time < window.start_time {
                    in_window = false;
                }
            }

            // If it's outside our window then push iterator back on the heap and find new iterator.
            if !in_window {
                inner.curr.as_mut().unwrap().itr.unread(Some(p));
                let item = std::mem::take(&mut inner.curr).unwrap();
                inner.heap.push(item);
                continue;
            }

            return Some(p);
        }
    }
}

/// UnsignedMergeHeap represents a heap of UnsignedMergeHeapItems. Items are sorted by their next window and then by name/tags.
struct UnsignedMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<UnsignedMergeHeapItem>,
}

impl UnsignedMergeHeap {
    fn new(opt: Arc<MergeOptions>, items: Vec<UnsignedMergeHeapItem>) -> Self {
        Self { opt, items }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            if j2 < n && self.less_heap_item(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.less_heap_item(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&mut self, i: usize, j: usize) -> bool {
        let (x, y) = (
            self.items[i].itr.peek().unwrap(),
            self.items[j].itr.next().unwrap(),
        );

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() < ytags.id();
                }
            }
        } else {
            if x.name != y.name {
                return x.name > y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() > ytags.id();
                }
            }
        }

        let (xt, _) = self.opt.window(x.time);
        let (yt, _) = self.opt.window(y.time);

        if self.opt.ascending {
            return xt < yt;
        }

        xt > yt
    }

    pub fn push(&mut self, item: UnsignedMergeHeapItem) {
        self.items.push(item);
    }

    /// Removes the last element.
    pub fn pop(&mut self) -> Option<UnsignedMergeHeapItem> {
        self.items.pop()
    }
}

#[derive(Clone)]
struct UnsignedMergeHeapItem {
    itr: BufUnsignedIter,
}

/// Represents a buffered FloatIterator.
#[derive(Clone)]
struct BufFloatIter {
    itr: Arc<RwLock<Box<dyn FloatIterator + Sync + Send>>>, // FIXME: remove RwLock
    buf: Option<query_point::FloatPoint>,
}

impl BufFloatIter {
    fn new(itr: Arc<RwLock<Box<dyn FloatIterator + Sync + Send>>>) -> Self {
        Self { itr, buf: None }
    }

    /// Returns the next point without removing it from the iterator.
    fn peek(&mut self) -> Option<query_point::FloatPoint> {
        let p = self.next();
        self.unread(p.clone());
        p
    }

    /// Returns the time of the next point.
    /// Returns zero time if no more points available.
    fn peek_time(&mut self) -> i64 {
        if let Some(p) = self.peek() {
            return p.time;
        }
        ZERO_TIME
    }

    /// Returns the next value if it is between [start_time, end_time).
    /// If the next value is outside the range then it is moved to the buffer.
    fn next_window(&mut self, start_time: i64, end_time: i64) -> Option<query_point::FloatPoint> {
        let v = self.next();
        if let Some(v) = v {
            let t = v.time;
            if t >= end_time || t < start_time {
                self.unread(Some(v));
                return None;
            }
            return Some(v);
        }

        None
    }

    /// Sets v to the buffer. It is read on the next call to next().
    fn unread(&mut self, v: Option<query_point::FloatPoint>) {
        self.buf = v;
    }
}

impl QueryIterator for BufFloatIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Float
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        self.itr.read().stats()
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        self.itr.write().close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl FloatIterator for BufFloatIter {
    /// Returns the current buffer, if exists, or calls the underlying iterator.
    fn next(&mut self) -> Option<query_point::FloatPoint> {
        let buf = self.buf.clone();
        if buf.is_some() {
            self.buf = None;
            return buf;
        }
        self.itr.write().next()
    }
}

/// FloatMergeIter Represents an iterator that combines multiple float iterators.
pub struct FloatMergeIter {
    inner: RwLock<FloatMergeIterInner>,
}

struct FloatMergeIterInner {
    inputs: Vec<Arc<RwLock<Box<dyn FloatIterator + Sync + Send>>>>,
    heap: FloatMergeHeap,
    init: bool,

    closed: bool,

    // Current iterator and window.
    curr: Option<FloatMergeHeapItem>,
    window: Window,
}

impl FloatMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn FloatIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut inner = FloatMergeIterInner {
            inputs: vec![],
            heap: FloatMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
            },
            init: false,

            closed: false,

            curr: None,
            window: Window::default(),
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));
            // Wrap in buffer, ignore any inputs without anymore points.
            let buf_input = BufFloatIter::new(Arc::clone(&input));
            inner.inputs.push(input);
            // Append to the heap.
            inner.heap.items.push(FloatMergeHeapItem { itr: buf_input });
        }

        Self {
            inner: RwLock::new(inner),
        }
    }
}

impl QueryIterator for FloatMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Float
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        let inner = self.inner.read();
        for input in inner.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        let mut inner = self.inner.write();
        for input in inner.inputs.iter_mut() {
            input.write().close();
        }

        inner.curr = None;
        inner.inputs = vec![];
        inner.heap.items = vec![];
        inner.closed = true;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl FloatIterator for FloatMergeIter {
    fn next(&mut self) -> Option<query_point::FloatPoint> {
        let mut inner = self.inner.write();
        if inner.closed {
            return None;
        }

        // Initialize the heap. This needs to be done lazily on the first call to this iterator
        // so that iterator initialization done through the Select() call returns quickly.
        // Queries can only be interrupted after the Select() call completes so any operations
        // done during iterator creation cannot be interrupted, which is why we do it here
        // instead so an interrupt can happen while initializing the heap.
        if !inner.init {
            let items_len = inner.heap.items.len();
            let items = std::mem::replace(&mut inner.heap.items, Vec::with_capacity(items_len));
            for mut item in items {
                let p = item.itr.peek();
                if p.is_none() {
                    continue;
                }
                inner.heap.items.push(item);
            }

            inner.heap.init(); // make heap
            inner.init = true;
        }

        loop {
            // Retrieve the next iterator if we don't have one.
            if inner.curr.is_none() {
                if inner.heap.items.is_empty() {
                    return None;
                }
                inner.curr = inner.heap.pop();

                // Read point and set current window.
                if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                    let tags = p.tags.subset(&inner.heap.opt.dimensions);
                    (inner.window.name, inner.window.tags) = (p.name.clone(), tags.id());
                    (inner.window.start_time, inner.window.end_time) =
                        inner.heap.opt.window(p.time);
                    return Some(p);
                }

                return None;
            }

            // Read the next point from the current iterator.
            // If there are no more points then remove iterator from heap and find next.;
            let p = if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                p
            } else {
                inner.curr = None;
                continue;
            };

            // Check if the point is inside of our current window.
            let mut in_window = true;
            let window = &inner.window;
            if window.name != p.name {
                in_window = false;
            } else {
                let tags = p.tags.subset(&inner.heap.opt.dimensions);
                if window.tags != tags.id() {
                    in_window = false;
                } else if inner.heap.opt.ascending && p.time >= window.end_time {
                    in_window = false;
                } else if !inner.heap.opt.ascending && p.time < window.start_time {
                    in_window = false;
                }
            }

            // If it's outside our window then push iterator back on the heap and find new iterator.
            if !in_window {
                inner.curr.as_mut().unwrap().itr.unread(Some(p));
                let item = std::mem::take(&mut inner.curr).unwrap();
                inner.heap.push(item);
                continue;
            }

            return Some(p);
        }
    }
}

/// FloatMergeHeap represents a heap of FloatMergeHeapItems. Items are sorted by their next window and then by name/tags.
struct FloatMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<FloatMergeHeapItem>,
}

impl FloatMergeHeap {
    fn new(opt: Arc<MergeOptions>, items: Vec<FloatMergeHeapItem>) -> Self {
        Self { opt, items }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            if j2 < n && self.less_heap_item(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.less_heap_item(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&mut self, i: usize, j: usize) -> bool {
        let x = self.items[i].itr.peek().unwrap();
        let y = self.items[j].itr.peek().unwrap();

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() < ytags.id();
                }
            }
        } else {
            if x.name != y.name {
                return x.name > y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() > ytags.id();
                }
            }
        }

        let (xt, _) = self.opt.window(x.time);
        let (yt, _) = self.opt.window(y.time);

        if self.opt.ascending {
            return xt < yt;
        }

        xt > yt
    }

    pub fn push(&mut self, item: FloatMergeHeapItem) {
        self.items.push(item);
    }

    /// Removes the last element.
    pub fn pop(&mut self) -> Option<FloatMergeHeapItem> {
        self.items.pop()
    }
}

#[derive(Clone)]
struct FloatMergeHeapItem {
    itr: BufFloatIter,
}

/// Represents a buffered BoolIterator.

#[derive(Clone)]
struct BufBoolIter {
    itr: Arc<RwLock<Box<dyn BoolIterator + Sync + Send>>>,
    buf: Option<query_point::BoolPoint>,
}

impl BufBoolIter {
    fn new(itr: Arc<RwLock<Box<dyn BoolIterator + Sync + Send>>>) -> Self {
        Self { itr, buf: None }
    }

    /// Returns the next point without removing it from the iterator.
    fn peek(&mut self) -> Option<query_point::BoolPoint> {
        let p = self.next();
        self.unread(p.clone());
        p
    }

    /// Returns the time of the next point.
    /// Returns zero time if no more points available.
    fn peek_time(&mut self) -> i64 {
        if let Some(p) = self.peek() {
            return p.time;
        }
        ZERO_TIME
    }

    /// Returns the next value if it is between [start_time, end_time).
    /// If the next value is outside the range then it is moved to the buffer.
    fn next_window(&mut self, start_time: i64, end_time: i64) -> Option<query_point::BoolPoint> {
        let v = self.next();
        if let Some(v) = v {
            let t = v.time;
            if t >= end_time || t < start_time {
                self.unread(Some(v));
                return None;
            }
            return Some(v);
        }

        None
    }

    /// Sets v to the buffer. It is read on the next call to next().
    fn unread(&mut self, v: Option<query_point::BoolPoint>) {
        self.buf = v;
    }
}

impl QueryIterator for BufBoolIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Boolean
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        self.itr.read().stats()
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        self.itr.write().close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl BoolIterator for BufBoolIter {
    /// Returns the current buffer, if exists, or calls the underlying iterator.
    fn next(&mut self) -> Option<query_point::BoolPoint> {
        let buf = self.buf.clone();
        if buf.is_some() {
            self.buf = None;
            return buf;
        }
        self.itr.write().next()
    }
}

/// BoolMergeIter Represents an iterator that combines multiple bool iterators.
pub struct BoolMergeIter {
    inner: RwLock<BoolMergeIterInner>,
}

struct BoolMergeIterInner {
    inputs: Vec<Arc<RwLock<Box<dyn BoolIterator + Sync + Send>>>>,
    heap: BoolMergeHeap,
    init: bool,

    closed: bool,

    // Current iterator and window.
    curr: Option<BoolMergeHeapItem>,
    window: Window,
}

impl BoolMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn BoolIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut inner = BoolMergeIterInner {
            inputs: vec![],
            heap: BoolMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
            },
            init: false,

            closed: false,

            curr: None,
            window: Window::default(),
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));
            // Wrap in buffer, ignore any inputs without anymore points.
            let buf_input = BufBoolIter::new(Arc::clone(&input));
            inner.inputs.push(input);
            // Append to the heap.
            inner.heap.items.push(BoolMergeHeapItem { itr: buf_input });
        }

        Self {
            inner: RwLock::new(inner),
        }
    }
}

impl QueryIterator for BoolMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Boolean
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        let inner = self.inner.read();
        for input in inner.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        let mut inner = self.inner.write();
        for input in inner.inputs.iter_mut() {
            input.write().close();
        }

        inner.curr = None;
        inner.inputs = vec![];
        inner.heap.items = vec![];
        inner.closed = true;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl BoolIterator for BoolMergeIter {
    fn next(&mut self) -> Option<query_point::BoolPoint> {
        let mut inner = self.inner.write();
        if inner.closed {
            return None;
        }

        // Initialize the heap. This needs to be done lazily on the first call to this iterator
        // so that iterator initialization done through the Select() call returns quickly.
        // Queries can only be interrupted after the Select() call completes so any operations
        // done during iterator creation cannot be interrupted, which is why we do it here
        // instead so an interrupt can happen while initializing the heap.
        if !inner.init {
            let items_len = inner.heap.items.len();
            let items = std::mem::replace(&mut inner.heap.items, Vec::with_capacity(items_len));
            for mut item in items {
                if item.itr.peek().is_none() {
                    continue;
                }
                inner.heap.items.push(item);
            }

            inner.heap.init(); // make heap
            inner.init = true;
        }

        loop {
            // Retrieve the next iterator if we don't have one.
            if inner.curr.is_none() {
                if inner.heap.items.is_empty() {
                    return None;
                }
                inner.curr = inner.heap.pop();

                // Read point and set current window.
                if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                    let tags = p.tags.subset(&inner.heap.opt.dimensions);
                    (inner.window.name, inner.window.tags) = (p.name.clone(), tags.id());
                    (inner.window.start_time, inner.window.end_time) =
                        inner.heap.opt.window(p.time);
                    return Some(p);
                }

                return None;
            }

            // Read the next point from the current iterator.
            // If there are no more points then remove iterator from heap and find next.;
            let p = if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                p
            } else {
                inner.curr = None;
                continue;
            };

            // Check if the point is inside of our current window.
            let mut in_window = true;
            let window = &inner.window;
            if window.name != p.name {
                in_window = false;
            } else {
                let tags = p.tags.subset(&inner.heap.opt.dimensions);
                if window.tags != tags.id() {
                    in_window = false;
                } else if inner.heap.opt.ascending && p.time >= window.end_time {
                    in_window = false;
                } else if !inner.heap.opt.ascending && p.time < window.start_time {
                    in_window = false;
                }
            }

            // If it's outside our window then push iterator back on the heap and find new iterator.
            if !in_window {
                inner.curr.as_mut().unwrap().itr.unread(Some(p));
                let item = std::mem::take(&mut inner.curr).unwrap();
                inner.heap.push(item);
                continue;
            }

            return Some(p);
        }
    }
}

/// BoolMergeHeap represents a heap of BoolMergeHeapItems. Items are sorted by their next window and then by name/tags.
struct BoolMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<BoolMergeHeapItem>,
}

impl BoolMergeHeap {
    fn new(opt: Arc<MergeOptions>, items: Vec<BoolMergeHeapItem>) -> Self {
        Self { opt, items }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            if j2 < n && self.less_heap_item(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.less_heap_item(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&mut self, i: usize, j: usize) -> bool {
        let (x, y) = (
            self.items[i].itr.peek().unwrap(),
            self.items[j].itr.next().unwrap(),
        );

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() < ytags.id();
                }
            }
        } else {
            if x.name != y.name {
                return x.name > y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() > ytags.id();
                }
            }
        }

        let (xt, _) = self.opt.window(x.time);
        let (yt, _) = self.opt.window(y.time);

        if self.opt.ascending {
            return xt < yt;
        }

        xt > yt
    }

    pub fn push(&mut self, item: BoolMergeHeapItem) {
        self.items.push(item);
    }

    /// Removes the last element.
    pub fn pop(&mut self) -> Option<BoolMergeHeapItem> {
        self.items.pop()
    }
}

#[derive(Clone)]
struct BoolMergeHeapItem {
    itr: BufBoolIter,
}

/// Represents a buffered StringIterator.

#[derive(Clone)]
struct BufStringIter {
    itr: Arc<RwLock<Box<dyn StringIterator + Sync + Send>>>,
    buf: Option<query_point::StringPoint>,
}

impl BufStringIter {
    fn new(itr: Arc<RwLock<Box<dyn StringIterator + Sync + Send>>>) -> Self {
        Self { itr, buf: None }
    }

    /// Returns the next point without removing it from the iterator.
    fn peek(&mut self) -> Option<query_point::StringPoint> {
        let p = self.next();
        self.unread(p.clone());
        p
    }

    /// Returns the time of the next point.
    /// Returns zero time if no more points available.
    fn peek_time(&mut self) -> i64 {
        if let Some(p) = self.peek() {
            return p.time;
        }
        ZERO_TIME
    }

    /// Returns the next value if it is between [start_time, end_time).
    /// If the next value is outside the range then it is moved to the buffer.
    fn next_window(&mut self, start_time: i64, end_time: i64) -> Option<query_point::StringPoint> {
        let v = self.next();
        if let Some(v) = v {
            let t = v.time;
            if t >= end_time || t < start_time {
                self.unread(Some(v));
                return None;
            }
            return Some(v);
        }

        None
    }

    /// Sets v to the buffer. It is read on the next call to next().
    fn unread(&mut self, v: Option<query_point::StringPoint>) {
        self.buf = v;
    }
}

impl QueryIterator for BufStringIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::String
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        self.itr.read().stats()
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        self.itr.write().close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl StringIterator for BufStringIter {
    /// Returns the current buffer, if exists, or calls the underlying iterator.
    fn next(&mut self) -> Option<query_point::StringPoint> {
        let buf = self.buf.clone();
        if buf.is_some() {
            self.buf = None;
            return buf;
        }
        self.itr.write().next()
    }
}

/// StringMergeIter Represents an iterator that combines multiple string iterators.
pub struct StringMergeIter {
    inner: RwLock<StringMergeIterInner>,
}

struct StringMergeIterInner {
    inputs: Vec<Arc<RwLock<Box<dyn StringIterator + Sync + Send>>>>,
    heap: StringMergeHeap,
    init: bool,

    closed: bool,

    // Current iterator and window.
    curr: Option<StringMergeHeapItem>,
    window: Window,
}

impl StringMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn StringIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut inner = StringMergeIterInner {
            inputs: vec![],
            heap: StringMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
            },
            init: false,

            closed: false,

            curr: None,
            window: Window::default(),
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));
            // Wrap in buffer, ignore any inputs without anymore points.
            let buf_input = BufStringIter::new(Arc::clone(&input));
            inner.inputs.push(input);
            // Append to the heap.
            inner
                .heap
                .items
                .push(StringMergeHeapItem { itr: buf_input });
        }

        Self {
            inner: RwLock::new(inner),
        }
    }
}

impl QueryIterator for StringMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::String
    }

    /// Returns statistics from the input iterator.
    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        let inner = self.inner.read();
        for input in inner.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    /// Close the underlying iterator.
    fn close(&mut self) {
        let mut inner = self.inner.write();
        for input in inner.inputs.iter_mut() {
            input.write().close();
        }

        inner.curr = None;
        inner.inputs = vec![];
        inner.heap.items = vec![];
        inner.closed = true;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl StringIterator for StringMergeIter {
    fn next(&mut self) -> Option<query_point::StringPoint> {
        let mut inner = self.inner.write();
        if inner.closed {
            return None;
        }

        // Initialize the heap. This needs to be done lazily on the first call to this iterator
        // so that iterator initialization done through the Select() call returns quickly.
        // Queries can only be interrupted after the Select() call completes so any operations
        // done during iterator creation cannot be interrupted, which is why we do it here
        // instead so an interrupt can happen while initializing the heap.
        if !inner.init {
            let items_len = inner.heap.items.len();
            let items = std::mem::replace(&mut inner.heap.items, Vec::with_capacity(items_len));
            for mut item in items {
                if item.itr.peek().is_none() {
                    continue;
                }
                inner.heap.items.push(item);
            }

            inner.heap.init(); // make heap
            inner.init = true;
        }

        loop {
            // Retrieve the next iterator if we don't have one.
            if inner.curr.is_none() {
                if inner.heap.items.is_empty() {
                    return None;
                }
                inner.curr = inner.heap.pop();

                // Read point and set current window.
                if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                    let tags = p.tags.subset(&inner.heap.opt.dimensions);
                    (inner.window.name, inner.window.tags) = (p.name.clone(), tags.id());
                    (inner.window.start_time, inner.window.end_time) =
                        inner.heap.opt.window(p.time);
                    return Some(p);
                }

                return None;
            }

            // Read the next point from the current iterator.
            // If there are no more points then remove iterator from heap and find next.;
            let p = if let Some(p) = inner.curr.as_mut().unwrap().itr.next() {
                p
            } else {
                inner.curr = None;
                continue;
            };

            // Check if the point is inside of our current window.
            let mut in_window = true;
            let window = &inner.window;
            if window.name != p.name {
                in_window = false;
            } else {
                let tags = p.tags.subset(&inner.heap.opt.dimensions);
                if window.tags != tags.id() {
                    in_window = false;
                } else if inner.heap.opt.ascending && p.time >= window.end_time {
                    in_window = false;
                } else if !inner.heap.opt.ascending && p.time < window.start_time {
                    in_window = false;
                }
            }

            // If it's outside our window then push iterator back on the heap and find new iterator.
            if !in_window {
                inner.curr.as_mut().unwrap().itr.unread(Some(p));
                let item = std::mem::take(&mut inner.curr).unwrap();
                inner.heap.push(item);
                continue;
            }

            return Some(p);
        }
    }
}

/// StringMergeHeap represents a heap of StringMergeHeapItems. Items are sorted by their next window and then by name/tags.
struct StringMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<StringMergeHeapItem>,
}

impl StringMergeHeap {
    fn new(opt: Arc<MergeOptions>, items: Vec<StringMergeHeapItem>) -> Self {
        Self { opt, items }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            if j2 < n && self.less_heap_item(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.less_heap_item(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&mut self, i: usize, j: usize) -> bool {
        let (x, y) = (
            self.items[i].itr.peek().unwrap(),
            self.items[j].itr.next().unwrap(),
        );

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() < ytags.id();
                }
            }
        } else {
            if x.name != y.name {
                return x.name > y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.id().eq(&ytags.id()) {
                    return xtags.id() > ytags.id();
                }
            }
        }

        let (xt, _) = self.opt.window(x.time);
        let (yt, _) = self.opt.window(y.time);

        if self.opt.ascending {
            return xt < yt;
        }

        xt > yt
    }

    pub fn push(&mut self, item: StringMergeHeapItem) {
        self.items.push(item);
    }

    /// Removes the last element.
    pub fn pop(&mut self) -> Option<StringMergeHeapItem> {
        self.items.pop()
    }
}

#[derive(Clone)]
struct StringMergeHeapItem {
    itr: BufStringIter,
}

// ////////////////////////////////////////////////////////////
// /////////////// IntegerSortedMergeIter   ///////////////////
// ////////////////////////////////////////////////////////////

/// IntegerSortedMergeIter is an iterator that sorts and merges multiple iterators into one.
pub struct IntegerSortedMergeIter {
    inputs: Vec<Arc<RwLock<Box<dyn IntegerIterator + Sync + Send>>>>,
    heap: IntegerSortedMergeHeap,
    init: bool,
}

impl IntegerSortedMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn IntegerIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut itr = IntegerSortedMergeIter {
            inputs: vec![],
            heap: IntegerSortedMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
                fast: false,
            },
            init: false,
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));

            itr.inputs.push(Arc::clone(&input));
            itr.heap.items.push(IntegerSortedMergeHeapItem {
                point: None,
                itr: input,
                fast_idx: 0,
            });
        }

        itr
    }

    /// Returns the next integer point from the heap.
    /// Reads the next point from item's cursor and puts it back on the heap.
    fn pop(&mut self) -> Option<query_point::IntegerPoint> {
        // Initialize the heap: make heap first.
        if !self.init {
            let items = self.heap.items.clone();
            self.heap.items = vec![];
            for mut item in items {
                if let Some(point) = item.itr.write().next() {
                    item.point = Some(point.clone());
                } else {
                    continue;
                }
                self.heap.items.push(item);
            }

            self.heap.detect_fast();
            self.heap.init(); // make heap
            self.init = true;
        }

        if self.heap.items.is_empty() {
            return None;
        }

        // Read the next item from the heap.
        if let Some(mut item) = self.heap.pop() {
            item.point.as_ref()?;

            // Copy the point for return.
            let p = item.point.clone();

            // Read the next item from the cursor. Push back to heap if one exists.
            item.point = item.itr.write().next();
            if item.point.is_some() {
                self.heap.push(item);
            }

            return p;
        }

        None
    }
}

impl QueryIterator for IntegerSortedMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Integer
    }

    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        for input in self.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    fn close(&mut self) {
        for input in self.inputs.iter_mut() {
            input.write().close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl IntegerIterator for IntegerSortedMergeIter {
    /// Rreturns the next integer points from the iterator.
    fn next(&mut self) -> Option<query_point::IntegerPoint> {
        self.pop()
    }
}

// IntegerSortedMergeHeap represents a heap of IntegerSortedMergeHeapItems.
// Items are sorted with the following priority:
//     - By their measurement name;
//     - By their tag keys/values;
//     - By time; or
//     - By their Aux field values.
//
struct IntegerSortedMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<IntegerSortedMergeHeapItem>, // FIXME: always single-thread
    // if each input comes from a unique single time series, we can make a shortcut.
    // detection of the shortcut introduces some overhead but it gets significant
    // performance improvement in cases like SELECT * FROM m GROUP BY *
    fast: bool,
}

impl IntegerSortedMergeHeap {
    /// Create a new IntegerSortedMergeHeap Instance.
    /// Note: the returned instance hasn't established the heap.
    fn new(opt: Arc<MergeOptions>, items: Vec<IntegerSortedMergeHeapItem>) -> Self {
        Self {
            opt,
            items,
            fast: false,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            // if j2 < n && self.items[j2] < self.items[j1] {
            //     j = j2;  // = 2*i + 2  // right child
            // }
            // if !(self.items[j] < self.items[i]) {
            //     break;
            // }

            if j2 < n && self.item_less(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.item_less(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&self, i: usize, j: usize) -> bool {
        let (x, y) = (
            self.items[i].point.as_ref().unwrap(),
            self.items[j].point.as_ref().unwrap(),
        );

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.eq(&ytags) {
                    return xtags.id() < ytags.id();
                }
            }

            if x.time != y.time {
                return x.time < y.time;
            }

            if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
                for i in 0..x.aux.len() {
                    let (v1, ok1) = match &x.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    let (v2, ok2) = match &y.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    if !ok1 || !ok2 {
                        // Unsupported types used in Aux fields. Maybe they
                        // need to be added here?
                        return false;
                    } else if v1 == v2 {
                        continue;
                    }

                    return v1 < v2;
                }
            }
            return false; // Times and/or aux fields are equal.
        }

        if x.name != y.name {
            return x.name > y.name;
        } else {
            let (xtags, ytags) = (
                x.tags.subset(&self.opt.dimensions),
                y.tags.subset(&self.opt.dimensions),
            );
            if !xtags.eq(&ytags) {
                return xtags.id() > ytags.id();
            }
        }

        if x.time != y.time {
            return x.time > y.time;
        }

        if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
            for i in 0..x.aux.len() {
                let (v1, ok1) = match &x.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                let (v2, ok2) = match &y.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                if !ok1 || !ok2 {
                    // Unsupported types used in Aux fields. Maybe they
                    // need to be added here?
                    return false;
                } else if v1 == v2 {
                    continue;
                }

                return v1 > v2;
            }
        }

        false // Times and/or aux fields are equal.
    }

    fn detect_fast(&mut self) {
        for item in self.items.iter() {
            if item.itr.read().stats().series_n != 1 {
                return;
            }
        }

        let mut has_deq = false;
        self.items.sort_by(|a, b| {
            let x = a.point.as_ref().unwrap();
            let y = b.point.as_ref().unwrap();
            let mut ret = x.name.cmp(&y.name);
            if ret == Ordering::Equal {
                ret = x.tags.id().cmp(&y.tags.id());
            }

            if ret != Ordering::Equal {
                // TT
                // ret | == -1 | h.opt.Ascending | result
                //  1  | false |  false          | true
                // -1  | true  |  false          | false
                //  1  | false |  true           | false
                // -1  | true  |  true           | true
                if (ret == Ordering::Less) == self.opt.ascending {
                    return Ordering::Less;
                } else {
                    return ret;
                }
            }

            has_deq = true;
            ret
        });

        if !has_deq {
            self.fast = true;
            for (i, item) in self.items.iter_mut().enumerate() {
                item.fast_idx = i;
            }
        }
    }

    fn item_less(&self, i: usize, j: usize) -> bool {
        if self.fast {
            self.items[i].fast_idx < self.items[j].fast_idx
        } else {
            self.less_heap_item(i, j)
        }
    }

    pub fn push(&mut self, item: IntegerSortedMergeHeapItem) {
        self.items.push(item);
        let mut child = self.items.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if !self.item_less(child, parent) {
                break;
            }
            self.items.swap(child, parent);
            child = parent;
        }
    }

    /// Removes the least element while retaining heap order.
    pub fn pop(&mut self) -> Option<IntegerSortedMergeHeapItem> {
        let last = self.items.len().checked_sub(1)?;
        self.items.swap(0, last);
        let item = self.items.pop();
        if !self.items.is_empty() {
            self.down(0, self.items.len());
        }
        item
    }
}

#[derive(Clone)]
struct IntegerSortedMergeHeapItem {
    point: Option<query_point::IntegerPoint>,
    itr: Arc<RwLock<Box<dyn IntegerIterator + Sync + Send>>>,
    // index for fast shortcut
    fast_idx: usize,
}

/// UnsignedSortedMergeIter is an iterator that sorts and merges multiple iterators into one.
pub struct UnsignedSortedMergeIter {
    inputs: Vec<Arc<RwLock<Box<dyn UnsignedIterator + Sync + Send>>>>,
    heap: UnsignedSortedMergeHeap,
    init: bool,
}

impl UnsignedSortedMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn UnsignedIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut itr = UnsignedSortedMergeIter {
            inputs: vec![],
            heap: UnsignedSortedMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
                fast: false,
            },
            init: false,
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));

            itr.inputs.push(Arc::clone(&input));
            itr.heap.items.push(UnsignedSortedMergeHeapItem {
                point: None,
                itr: input,
                fast_idx: 0,
            });
        }

        itr
    }

    /// Returns the next unsigned point from the heap.
    /// Reads the next point from item's cursor and puts it back on the heap.
    fn pop(&mut self) -> Option<query_point::UnsignedPoint> {
        // Initialize the heap: make heap first.
        if !self.init {
            let items = self.heap.items.clone();
            self.heap.items = vec![];
            for mut item in items {
                if let Some(point) = item.itr.write().next() {
                    item.point = Some(point.clone());
                } else {
                    continue;
                }
                self.heap.items.push(item);
            }

            self.heap.detect_fast();
            self.heap.init(); // make heap
            self.init = true;
        }

        if self.heap.items.is_empty() {
            return None;
        }

        // Read the next item from the heap.
        if let Some(mut item) = self.heap.pop() {
            item.point.as_ref()?;

            // Copy the point for return.
            let p = item.point.clone();

            // Read the next item from the cursor. Push back to heap if one exists.
            item.point = item.itr.write().next();
            if item.point.is_some() {
                self.heap.push(item);
            }

            return p;
        }

        None
    }
}

impl QueryIterator for UnsignedSortedMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Unsigned
    }

    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        for input in self.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    fn close(&mut self) {
        for input in self.inputs.iter_mut() {
            input.write().close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl UnsignedIterator for UnsignedSortedMergeIter {
    /// Rreturns the next unsigned points from the iterator.
    fn next(&mut self) -> Option<query_point::UnsignedPoint> {
        self.pop()
    }
}

// UnsignedSortedMergeHeap represents a heap of UnsignedSortedMergeHeapItems.
// Items are sorted with the following priority:
//     - By their measurement name;
//     - By their tag keys/values;
//     - By time; or
//     - By their Aux field values.
//
struct UnsignedSortedMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<UnsignedSortedMergeHeapItem>, // FIXME: always single-thread
    // if each input comes from a unique single time series, we can make a shortcut.
    // detection of the shortcut introduces some overhead but it gets significant
    // performance improvement in cases like SELECT * FROM m GROUP BY *
    fast: bool,
}

impl UnsignedSortedMergeHeap {
    /// Create a new UnsignedSortedMergeHeap Instance.
    /// Note: the returned instance hasn't established the heap.
    fn new(opt: Arc<MergeOptions>, items: Vec<UnsignedSortedMergeHeapItem>) -> Self {
        Self {
            opt,
            items,
            fast: false,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            // if j2 < n && self.items[j2] < self.items[j1] {
            //     j = j2;  // = 2*i + 2  // right child
            // }
            // if !(self.items[j] < self.items[i]) {
            //     break;
            // }

            if j2 < n && self.item_less(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.item_less(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&self, i: usize, j: usize) -> bool {
        let (x, y) = (
            self.items[i].point.as_ref().unwrap(),
            self.items[j].point.as_ref().unwrap(),
        );

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.eq(&ytags) {
                    return xtags.id() < ytags.id();
                }
            }

            if x.time != y.time {
                return x.time < y.time;
            }

            if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
                for i in 0..x.aux.len() {
                    let (v1, ok1) = match &x.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    let (v2, ok2) = match &y.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    if !ok1 || !ok2 {
                        // Unsupported types used in Aux fields. Maybe they
                        // need to be added here?
                        return false;
                    } else if v1 == v2 {
                        continue;
                    }

                    return v1 < v2;
                }
            }
            return false; // Times and/or aux fields are equal.
        }

        if x.name != y.name {
            return x.name > y.name;
        } else {
            let (xtags, ytags) = (
                x.tags.subset(&self.opt.dimensions),
                y.tags.subset(&self.opt.dimensions),
            );
            if !xtags.eq(&ytags) {
                return xtags.id() > ytags.id();
            }
        }

        if x.time != y.time {
            return x.time > y.time;
        }

        if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
            for i in 0..x.aux.len() {
                let (v1, ok1) = match &x.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                let (v2, ok2) = match &y.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                if !ok1 || !ok2 {
                    // Unsupported types used in Aux fields. Maybe they
                    // need to be added here?
                    return false;
                } else if v1 == v2 {
                    continue;
                }

                return v1 > v2;
            }
        }

        false // Times and/or aux fields are equal.
    }

    fn detect_fast(&mut self) {
        for item in self.items.iter() {
            if item.itr.read().stats().series_n != 1 {
                return;
            }
        }

        let mut has_deq = false;
        self.items.sort_by(|a, b| {
            let x = a.point.as_ref().unwrap();
            let y = b.point.as_ref().unwrap();
            let mut ret = x.name.cmp(&y.name);
            if ret == Ordering::Equal {
                ret = x.tags.id().cmp(&y.tags.id());
            }

            if ret != Ordering::Equal {
                // TT
                // ret | == -1 | h.opt.Ascending | result
                //  1  | false |  false          | true
                // -1  | true  |  false          | false
                //  1  | false |  true           | false
                // -1  | true  |  true           | true
                if (ret == Ordering::Less) == self.opt.ascending {
                    return Ordering::Less;
                } else {
                    return ret;
                }
            }

            has_deq = true;
            ret
        });

        if !has_deq {
            self.fast = true;
            for (i, item) in self.items.iter_mut().enumerate() {
                item.fast_idx = i;
            }
        }
    }

    fn item_less(&self, i: usize, j: usize) -> bool {
        if self.fast {
            self.items[i].fast_idx < self.items[j].fast_idx
        } else {
            self.less_heap_item(i, j)
        }
    }

    pub fn push(&mut self, item: UnsignedSortedMergeHeapItem) {
        self.items.push(item);
        let mut child = self.items.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if !self.item_less(child, parent) {
                break;
            }
            self.items.swap(child, parent);
            child = parent;
        }
    }

    /// Removes the least element while retaining heap order.
    pub fn pop(&mut self) -> Option<UnsignedSortedMergeHeapItem> {
        let last = self.items.len().checked_sub(1)?;
        self.items.swap(0, last);
        let item = self.items.pop();
        if !self.items.is_empty() {
            self.down(0, self.items.len());
        }
        item
    }
}

#[derive(Clone)]
struct UnsignedSortedMergeHeapItem {
    point: Option<query_point::UnsignedPoint>,
    itr: Arc<RwLock<Box<dyn UnsignedIterator + Sync + Send>>>,
    // index for fast shortcut
    fast_idx: usize,
}

/// FloatSortedMergeIter is an iterator that sorts and merges multiple iterators into one.
pub struct FloatSortedMergeIter {
    inputs: Vec<Arc<RwLock<Box<dyn FloatIterator + Sync + Send>>>>,
    heap: FloatSortedMergeHeap,
    init: bool,
}

impl FloatSortedMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn FloatIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut itr = FloatSortedMergeIter {
            inputs: vec![],
            heap: FloatSortedMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
                fast: false,
            },
            init: false,
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));
            itr.inputs.push(Arc::clone(&input));
            itr.heap.items.push(FloatSortedMergeHeapItem {
                point: None,
                itr: input,
                fast_idx: 0,
            });
        }

        itr
    }

    /// Returns the next float point from the heap.
    /// Reads the next point from item's cursor and puts it back on the heap.
    fn pop(&mut self) -> Option<query_point::FloatPoint> {
        // Initialize the heap: make heap first.
        if !self.init {
            let items = self.heap.items.clone();
            self.heap.items = vec![];
            for mut item in items {
                if let Some(point) = item.itr.write().next() {
                    item.point = Some(point.clone());
                } else {
                    continue;
                }
                self.heap.items.push(item);
            }

            self.heap.detect_fast();
            self.heap.init(); // make heap
            self.init = true;
        }

        if self.heap.items.is_empty() {
            return None;
        }

        // Read the next item from the heap.
        if let Some(mut item) = self.heap.pop() {
            item.point.as_ref()?;

            // Copy the point for return.
            let p = item.point.clone();

            // Read the next item from the cursor. Push back to heap if one exists.
            item.point = item.itr.write().next();
            if item.point.is_some() {
                self.heap.push(item);
            }

            return p;
        }

        None
    }
}

impl QueryIterator for FloatSortedMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Float
    }

    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        for input in self.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    fn close(&mut self) {
        for input in self.inputs.iter_mut() {
            input.write().close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl FloatIterator for FloatSortedMergeIter {
    /// Rreturns the next float points from the iterator.
    fn next(&mut self) -> Option<query_point::FloatPoint> {
        self.pop()
    }
}

// FloatSortedMergeHeap represents a heap of FloatSortedMergeHeapItems.
// Items are sorted with the following priority:
//     - By their measurement name;
//     - By their tag keys/values;
//     - By time; or
//     - By their Aux field values.
//
struct FloatSortedMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<FloatSortedMergeHeapItem>, // FIXME: always single-thread
    // if each input comes from a unique single time series, we can make a shortcut.
    // detection of the shortcut introduces some overhead but it gets significant
    // performance improvement in cases like SELECT * FROM m GROUP BY *
    fast: bool,
}

impl FloatSortedMergeHeap {
    /// Create a new FloatSortedMergeHeap Instance.
    /// Note: the returned instance hasn't established the heap.
    fn new(opt: Arc<MergeOptions>, items: Vec<FloatSortedMergeHeapItem>) -> Self {
        Self {
            opt,
            items,
            fast: false,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            // if j2 < n && self.items[j2] < self.items[j1] {
            //     j = j2;  // = 2*i + 2  // right child
            // }
            // if !(self.items[j] < self.items[i]) {
            //     break;
            // }

            if j2 < n && self.item_less(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.item_less(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&self, i: usize, j: usize) -> bool {
        let (x, y) = (
            self.items[i].point.as_ref().unwrap(),
            self.items[j].point.as_ref().unwrap(),
        );

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.eq(&ytags) {
                    return xtags.id() < ytags.id();
                }
            }

            if x.time != y.time {
                return x.time < y.time;
            }

            if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
                for i in 0..x.aux.len() {
                    let (v1, ok1) = match &x.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    let (v2, ok2) = match &y.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    if !ok1 || !ok2 {
                        // Unsupported types used in Aux fields. Maybe they
                        // need to be added here?
                        return false;
                    } else if v1 == v2 {
                        continue;
                    }

                    return v1 < v2;
                }
            }
            return false; // Times and/or aux fields are equal.
        }

        if x.name != y.name {
            return x.name > y.name;
        } else {
            let (xtags, ytags) = (
                x.tags.subset(&self.opt.dimensions),
                y.tags.subset(&self.opt.dimensions),
            );
            if !xtags.eq(&ytags) {
                return xtags.id() > ytags.id();
            }
        }

        if x.time != y.time {
            return x.time > y.time;
        }

        if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
            for i in 0..x.aux.len() {
                let (v1, ok1) = match &x.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                let (v2, ok2) = match &y.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                if !ok1 || !ok2 {
                    // Unsupported types used in Aux fields. Maybe they
                    // need to be added here?
                    return false;
                } else if v1 == v2 {
                    continue;
                }

                return v1 > v2;
            }
        }

        false // Times and/or aux fields are equal.
    }

    fn detect_fast(&mut self) {
        for item in self.items.iter() {
            if item.itr.read().stats().series_n != 1 {
                return;
            }
        }

        let mut has_deq = false;
        self.items.sort_by(|a, b| {
            let x = a.point.as_ref().unwrap();
            let y = b.point.as_ref().unwrap();
            let mut ret = x.name.cmp(&y.name);
            if ret == Ordering::Equal {
                ret = x.tags.id().cmp(&y.tags.id());
            }

            if ret != Ordering::Equal {
                // TT
                // ret | == -1 | h.opt.Ascending | result
                //  1  | false |  false          | true
                // -1  | true  |  false          | false
                //  1  | false |  true           | false
                // -1  | true  |  true           | true
                if (ret == Ordering::Less) == self.opt.ascending {
                    return Ordering::Less;
                } else {
                    return ret;
                }
            }

            has_deq = true;
            ret
        });

        if !has_deq {
            self.fast = true;
            for (i, item) in self.items.iter_mut().enumerate() {
                item.fast_idx = i;
            }
        }
    }

    fn item_less(&self, i: usize, j: usize) -> bool {
        if self.fast {
            self.items[i].fast_idx < self.items[j].fast_idx
        } else {
            self.less_heap_item(i, j)
        }
    }

    pub fn push(&mut self, item: FloatSortedMergeHeapItem) {
        self.items.push(item);
        let mut child = self.items.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if !self.item_less(child, parent) {
                break;
            }
            self.items.swap(child, parent);
            child = parent;
        }
    }

    /// Removes the least element while retaining heap order.
    pub fn pop(&mut self) -> Option<FloatSortedMergeHeapItem> {
        let last = self.items.len().checked_sub(1)?;
        self.items.swap(0, last);
        let item = self.items.pop();
        if !self.items.is_empty() {
            self.down(0, self.items.len());
        }
        item
    }
}

#[derive(Clone)]
struct FloatSortedMergeHeapItem {
    point: Option<query_point::FloatPoint>,
    itr: Arc<RwLock<Box<dyn FloatIterator + Sync + Send>>>,
    // index for fast shortcut
    fast_idx: usize,
}

/// BoolSortedMergeIter is an iterator that sorts and merges multiple iterators into one.
pub struct BoolSortedMergeIter {
    inputs: Vec<Arc<RwLock<Box<dyn BoolIterator + Sync + Send>>>>,
    heap: BoolSortedMergeHeap,
    init: bool,
}

impl BoolSortedMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn BoolIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut itr = BoolSortedMergeIter {
            inputs: vec![],
            heap: BoolSortedMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
                fast: false,
            },
            init: false,
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));

            itr.inputs.push(Arc::clone(&input));
            itr.heap.items.push(BoolSortedMergeHeapItem {
                point: None,
                itr: input,
                fast_idx: 0,
            });
        }

        itr
    }

    /// Returns the next bool point from the heap.
    /// Reads the next point from item's cursor and puts it back on the heap.
    fn pop(&mut self) -> Option<query_point::BoolPoint> {
        // Initialize the heap: make heap first.
        if !self.init {
            let items = self.heap.items.clone();
            self.heap.items = vec![];
            for mut item in items {
                if let Some(point) = item.itr.write().next() {
                    item.point = Some(point.clone());
                } else {
                    continue;
                }
                self.heap.items.push(item);
            }

            self.heap.detect_fast();
            self.heap.init(); // make heap
            self.init = true;
        }

        if self.heap.items.is_empty() {
            return None;
        }

        // Read the next item from the heap.
        if let Some(mut item) = self.heap.pop() {
            item.point.as_ref()?;

            // Copy the point for return.
            let p = item.point.clone();

            // Read the next item from the cursor. Push back to heap if one exists.
            item.point = item.itr.write().next();
            if item.point.is_some() {
                self.heap.push(item);
            }

            return p;
        }

        None
    }
}

impl QueryIterator for BoolSortedMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Boolean
    }

    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        for input in self.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    fn close(&mut self) {
        for input in self.inputs.iter_mut() {
            input.write().close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl BoolIterator for BoolSortedMergeIter {
    /// Rreturns the next bool points from the iterator.
    fn next(&mut self) -> Option<query_point::BoolPoint> {
        self.pop()
    }
}

// BoolSortedMergeHeap represents a heap of BoolSortedMergeHeapItems.
// Items are sorted with the following priority:
//     - By their measurement name;
//     - By their tag keys/values;
//     - By time; or
//     - By their Aux field values.
//
struct BoolSortedMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<BoolSortedMergeHeapItem>, // FIXME: always single-thread
    // if each input comes from a unique single time series, we can make a shortcut.
    // detection of the shortcut introduces some overhead but it gets significant
    // performance improvement in cases like SELECT * FROM m GROUP BY *
    fast: bool,
}

impl BoolSortedMergeHeap {
    /// Create a new BoolSortedMergeHeap Instance.
    /// Note: the returned instance hasn't established the heap.
    fn new(opt: Arc<MergeOptions>, items: Vec<BoolSortedMergeHeapItem>) -> Self {
        Self {
            opt,
            items,
            fast: false,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            // if j2 < n && self.items[j2] < self.items[j1] {
            //     j = j2;  // = 2*i + 2  // right child
            // }
            // if !(self.items[j] < self.items[i]) {
            //     break;
            // }

            if j2 < n && self.item_less(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.item_less(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&self, i: usize, j: usize) -> bool {
        let (x, y) = (
            self.items[i].point.as_ref().unwrap(),
            self.items[j].point.as_ref().unwrap(),
        );

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.eq(&ytags) {
                    return xtags.id() < ytags.id();
                }
            }

            if x.time != y.time {
                return x.time < y.time;
            }

            if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
                for i in 0..x.aux.len() {
                    let (v1, ok1) = match &x.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    let (v2, ok2) = match &y.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    if !ok1 || !ok2 {
                        // Unsupported types used in Aux fields. Maybe they
                        // need to be added here?
                        return false;
                    } else if v1 == v2 {
                        continue;
                    }

                    return v1 < v2;
                }
            }
            return false; // Times and/or aux fields are equal.
        }

        if x.name != y.name {
            return x.name > y.name;
        } else {
            let (xtags, ytags) = (
                x.tags.subset(&self.opt.dimensions),
                y.tags.subset(&self.opt.dimensions),
            );
            if !xtags.eq(&ytags) {
                return xtags.id() > ytags.id();
            }
        }

        if x.time != y.time {
            return x.time > y.time;
        }

        if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
            for i in 0..x.aux.len() {
                let (v1, ok1) = match &x.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                let (v2, ok2) = match &y.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                if !ok1 || !ok2 {
                    // Unsupported types used in Aux fields. Maybe they
                    // need to be added here?
                    return false;
                } else if v1 == v2 {
                    continue;
                }

                return v1 > v2;
            }
        }

        false // Times and/or aux fields are equal.
    }

    fn detect_fast(&mut self) {
        for item in self.items.iter() {
            if item.itr.read().stats().series_n != 1 {
                return;
            }
        }

        let mut has_deq = false;
        self.items.sort_by(|a, b| {
            let x = a.point.as_ref().unwrap();
            let y = b.point.as_ref().unwrap();
            let mut ret = x.name.cmp(&y.name);
            if ret == Ordering::Equal {
                ret = x.tags.id().cmp(&y.tags.id());
            }

            if ret != Ordering::Equal {
                // TT
                // ret | == -1 | h.opt.Ascending | result
                //  1  | false |  false          | true
                // -1  | true  |  false          | false
                //  1  | false |  true           | false
                // -1  | true  |  true           | true
                if (ret == Ordering::Less) == self.opt.ascending {
                    return Ordering::Less;
                } else {
                    return ret;
                }
            }

            has_deq = true;
            ret
        });

        if !has_deq {
            self.fast = true;
            for (i, item) in self.items.iter_mut().enumerate() {
                item.fast_idx = i;
            }
        }
    }

    fn item_less(&self, i: usize, j: usize) -> bool {
        if self.fast {
            self.items[i].fast_idx < self.items[j].fast_idx
        } else {
            self.less_heap_item(i, j)
        }
    }

    pub fn push(&mut self, item: BoolSortedMergeHeapItem) {
        self.items.push(item);
        let mut child = self.items.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if !self.item_less(child, parent) {
                break;
            }
            self.items.swap(child, parent);
            child = parent;
        }
    }

    /// Removes the least element while retaining heap order.
    pub fn pop(&mut self) -> Option<BoolSortedMergeHeapItem> {
        let last = self.items.len().checked_sub(1)?;
        self.items.swap(0, last);
        let item = self.items.pop();
        if !self.items.is_empty() {
            self.down(0, self.items.len());
        }
        item
    }
}

#[derive(Clone)]
struct BoolSortedMergeHeapItem {
    point: Option<query_point::BoolPoint>,
    itr: Arc<RwLock<Box<dyn BoolIterator + Sync + Send>>>,
    // index for fast shortcut
    fast_idx: usize,
}

/// StringSortedMergeIter is an iterator that sorts and merges multiple iterators into one.
pub struct StringSortedMergeIter {
    inputs: Vec<Arc<RwLock<Box<dyn StringIterator + Sync + Send>>>>,
    heap: StringSortedMergeHeap,
    init: bool,
}

impl StringSortedMergeIter {
    fn new_with_options(
        inputs: Vec<Box<dyn StringIterator + Sync + Send>>,
        opt: Arc<MergeOptions>,
    ) -> Self {
        let mut itr = StringSortedMergeIter {
            inputs: vec![],
            heap: StringSortedMergeHeap {
                opt,
                items: Vec::with_capacity(inputs.len()),
                fast: false,
            },
            init: false,
        };

        // Initialize heap items.
        for input in inputs {
            let input = Arc::new(RwLock::new(input));

            itr.inputs.push(Arc::clone(&input));
            itr.heap.items.push(StringSortedMergeHeapItem {
                point: None,
                itr: input,
                fast_idx: 0,
            });
        }

        itr
    }

    /// Returns the next string point from the heap.
    /// Reads the next point from item's cursor and puts it back on the heap.
    fn pop(&mut self) -> Option<query_point::StringPoint> {
        // Initialize the heap: make heap first.
        if !self.init {
            let items = self.heap.items.clone();
            self.heap.items = vec![];
            for mut item in items {
                if let Some(point) = item.itr.write().next() {
                    item.point = Some(point.clone());
                } else {
                    continue;
                }
                self.heap.items.push(item);
            }

            self.heap.detect_fast();
            self.heap.init(); // make heap
            self.init = true;
        }

        if self.heap.items.is_empty() {
            return None;
        }

        // Read the next item from the heap.
        if let Some(mut item) = self.heap.pop() {
            item.point.as_ref()?;

            // Copy the point for return.
            let p = item.point.clone();

            // Read the next item from the cursor. Push back to heap if one exists.
            item.point = item.itr.write().next();
            if item.point.is_some() {
                self.heap.push(item);
            }

            return p;
        }

        None
    }
}

impl QueryIterator for StringSortedMergeIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::String
    }

    fn stats(&self) -> IteratorStats {
        let mut stats = IteratorStats::default();
        for input in self.inputs.iter() {
            stats.add(&input.read().stats());
        }
        stats
    }

    fn close(&mut self) {
        for input in self.inputs.iter_mut() {
            input.write().close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl StringIterator for StringSortedMergeIter {
    /// Rreturns the next string points from the iterator.
    fn next(&mut self) -> Option<query_point::StringPoint> {
        self.pop()
    }
}

// StringSortedMergeHeap represents a heap of boolSortedMergeHeapItems.
// Items are sorted with the following priority:
//     - By their measurement name;
//     - By their tag keys/values;
//     - By time; or
//     - By their Aux field values.
//
struct StringSortedMergeHeap {
    opt: Arc<MergeOptions>,
    items: Vec<StringSortedMergeHeapItem>, // FIXME: always single-thread
    // if each input comes from a unique single time series, we can make a shortcut.
    // detection of the shortcut introduces some overhead but it gets significant
    // performance improvement in cases like SELECT * FROM m GROUP BY *
    fast: bool,
}

impl StringSortedMergeHeap {
    /// Create a new StringSortedMergeHeap Instance.
    /// Note: the returned instance hasn't established the heap.
    fn new(opt: Arc<MergeOptions>, items: Vec<StringSortedMergeHeapItem>) -> Self {
        Self {
            opt,
            items,
            fast: false,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;

            // if j2 < n && self.items[j2] < self.items[j1] {
            //     j = j2;  // = 2*i + 2  // right child
            // }
            // if !(self.items[j] < self.items[i]) {
            //     break;
            // }

            if j2 < n && self.item_less(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.item_less(j, i) {
                break;
            }

            // swap items
            self.items.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less_heap_item(&self, i: usize, j: usize) -> bool {
        let (x, y) = (
            self.items[i].point.as_ref().unwrap(),
            self.items[j].point.as_ref().unwrap(),
        );

        if self.opt.ascending {
            if x.name != y.name {
                return x.name < y.name;
            } else {
                let (xtags, ytags) = (
                    x.tags.subset(&self.opt.dimensions),
                    y.tags.subset(&self.opt.dimensions),
                );
                if !xtags.eq(&ytags) {
                    return xtags.id() < ytags.id();
                }
            }

            if x.time != y.time {
                return x.time < y.time;
            }

            if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
                for i in 0..x.aux.len() {
                    let (v1, ok1) = match &x.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    let (v2, ok2) = match &y.aux[i] {
                        Val::String(v) => (v.as_str(), true),
                        _ => ("", false),
                    };
                    if !ok1 || !ok2 {
                        // Unsupported types used in Aux fields. Maybe they
                        // need to be added here?
                        return false;
                    } else if v1 == v2 {
                        continue;
                    }

                    return v1 < v2;
                }
            }
            return false; // Times and/or aux fields are equal.
        }

        if x.name != y.name {
            return x.name > y.name;
        } else {
            let (xtags, ytags) = (
                x.tags.subset(&self.opt.dimensions),
                y.tags.subset(&self.opt.dimensions),
            );
            if !xtags.eq(&ytags) {
                return xtags.id() > ytags.id();
            }
        }

        if x.time != y.time {
            return x.time > y.time;
        }

        if !x.aux.is_empty() && x.aux.len() == y.aux.len() {
            for i in 0..x.aux.len() {
                let (v1, ok1) = match &x.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                let (v2, ok2) = match &y.aux[i] {
                    Val::String(v) => (v.as_str(), true),
                    _ => ("", false),
                };
                if !ok1 || !ok2 {
                    // Unsupported types used in Aux fields. Maybe they
                    // need to be added here?
                    return false;
                } else if v1 == v2 {
                    continue;
                }

                return v1 > v2;
            }
        }

        false // Times and/or aux fields are equal.
    }

    fn detect_fast(&mut self) {
        for item in self.items.iter() {
            if item.itr.read().stats().series_n != 1 {
                return;
            }
        }

        let mut has_deq = false;
        self.items.sort_by(|a, b| {
            let x = a.point.as_ref().unwrap();
            let y = b.point.as_ref().unwrap();
            let mut ret = x.name.cmp(&y.name);
            if ret == Ordering::Equal {
                ret = x.tags.id().cmp(&y.tags.id());
            }

            if ret != Ordering::Equal {
                // TT
                // ret | == -1 | h.opt.Ascending | result
                //  1  | false |  false          | true
                // -1  | true  |  false          | false
                //  1  | false |  true           | false
                // -1  | true  |  true           | true
                if (ret == Ordering::Less) == self.opt.ascending {
                    return Ordering::Less;
                } else {
                    return ret;
                }
            }

            has_deq = true;
            ret
        });

        if !has_deq {
            self.fast = true;
            for (i, item) in self.items.iter_mut().enumerate() {
                item.fast_idx = i;
            }
        }
    }

    fn item_less(&self, i: usize, j: usize) -> bool {
        if self.fast {
            self.items[i].fast_idx < self.items[j].fast_idx
        } else {
            self.less_heap_item(i, j)
        }
    }

    pub fn push(&mut self, item: StringSortedMergeHeapItem) {
        self.items.push(item);
        let mut child = self.items.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if !self.item_less(child, parent) {
                break;
            }
            self.items.swap(child, parent);
            child = parent;
        }
    }

    /// Removes the least element while retaining heap order.
    pub fn pop(&mut self) -> Option<StringSortedMergeHeapItem> {
        let last = self.items.len().checked_sub(1)?;
        self.items.swap(0, last);
        let item = self.items.pop();
        if !self.items.is_empty() {
            self.down(0, self.items.len());
        }
        item
    }
}

#[derive(Clone)]
struct StringSortedMergeHeapItem {
    point: Option<query_point::StringPoint>,
    itr: Arc<RwLock<Box<dyn StringIterator + Sync + Send>>>,
    // index for fast shortcut
    fast_idx: usize,
}

#[derive(Default)]
struct Prev {
    name: String,
    tags: query_point::Tags,
}

// ////////////////////////////////////////////////////////////
// /////////////// IntegerLimitIter   /////////////////////////
// ////////////////////////////////////////////////////////////

type DynQueryIterator = Box<dyn QueryIterator + Sync + Send>;

macro_rules! define_typed_converter {
    ($function:ident, $predicate:ident, $trait_name:ident, [$($kind:ty),+ $(,)?]) => {
        fn $predicate(input: &DynQueryIterator) -> bool {
            let id = input.as_any().type_id();
            false $(|| id == any::TypeId::of::<$kind>())+
        }

        fn $function(
            input: DynQueryIterator,
        ) -> std::result::Result<Box<dyn $trait_name + Sync + Send>, DynQueryIterator> {
            let id = input.as_any().type_id();
            $(
                if id == any::TypeId::of::<$kind>() {
                    let input = input.into_any().downcast::<$kind>().expect(
                        "a TypeId-checked query iterator downcast must succeed",
                    );
                    return Ok(input as Box<dyn $trait_name + Sync + Send>);
                }
            )+
            Err(input)
        }
    };
}

define_typed_converter!(
    into_integer_iterator,
    is_integer_iterator,
    IntegerIterator,
    [
        IntegerIter,
        iterator::IntegerLimitIter,
        IntegerMergeIter,
        IntegerSortedMergeIter,
        IntegerLimitIter,
        IntegerSeriesLimitIter,
        IntegerInterruptIter,
    ]
);
define_typed_converter!(
    into_unsigned_iterator,
    is_unsigned_iterator,
    UnsignedIterator,
    [
        UnsignedIter,
        iterator::UnsignedLimitIter,
        UnsignedMergeIter,
        UnsignedSortedMergeIter,
        UnsignedLimitIter,
        UnsignedSeriesLimitIter,
        UnsignedInterruptIter,
    ]
);
define_typed_converter!(
    into_float_iterator,
    is_float_iterator,
    FloatIterator,
    [
        FloatIter,
        iterator::FloatLimitIter,
        FloatMergeIter,
        FloatSortedMergeIter,
        FloatLimitIter,
        FloatSeriesLimitIter,
        FloatInterruptIter,
    ]
);
define_typed_converter!(
    into_bool_iterator,
    is_bool_iterator,
    BoolIterator,
    [
        BoolIter,
        iterator::BoolLimitIter,
        BoolMergeIter,
        BoolSortedMergeIter,
        BoolLimitIter,
        BoolSeriesLimitIter,
        BoolInterruptIter,
    ]
);
define_typed_converter!(
    into_string_iterator,
    is_string_iterator,
    StringIterator,
    [
        StringIter,
        iterator::StringLimitIter,
        StringMergeIter,
        StringSortedMergeIter,
        StringLimitIter,
        StringSeriesLimitIter,
        StringInterruptIter,
    ]
);

pub(crate) fn fold_aggregate_iterator(
    input: DynQueryIterator,
    mut state: AggregateState,
    track_endpoints: bool,
) -> Result<AggregateState> {
    match input.data_type() {
        IteratorDataType::Integer => {
            let mut input = into_integer_iterator(input).map_err(|mut input| {
                input.close();
                Error::StringErr {
                    reason: "unsupported integer aggregate iterator".to_string(),
                }
            })?;
            while let Some(point) = input.next() {
                if track_endpoints {
                    state.add_integer_at(point.time(), point.value());
                } else {
                    state.add_integer(point.value());
                }
            }
            input.close();
        }
        IteratorDataType::Unsigned => {
            let mut input = into_unsigned_iterator(input).map_err(|mut input| {
                input.close();
                Error::StringErr {
                    reason: "unsupported unsigned aggregate iterator".to_string(),
                }
            })?;
            while let Some(point) = input.next() {
                if track_endpoints {
                    state.add_unsigned_at(point.time(), point.value());
                } else {
                    state.add_unsigned(point.value());
                }
            }
            input.close();
        }
        IteratorDataType::Float => {
            let mut input = into_float_iterator(input).map_err(|mut input| {
                input.close();
                Error::StringErr {
                    reason: "unsupported float aggregate iterator".to_string(),
                }
            })?;
            while let Some(point) = input.next() {
                if track_endpoints {
                    state.add_float_at(point.time(), point.value());
                } else {
                    state.add_float(point.value());
                }
            }
            input.close();
        }
        IteratorDataType::Boolean => {
            let mut input = into_bool_iterator(input).map_err(|mut input| {
                input.close();
                Error::StringErr {
                    reason: "unsupported boolean aggregate iterator".to_string(),
                }
            })?;
            while let Some(point) = input.next() {
                if track_endpoints {
                    state.add_boolean_at(point.time(), point.value());
                } else {
                    state.add_count(1);
                }
            }
            input.close();
        }
        IteratorDataType::String => {
            let mut input = into_string_iterator(input).map_err(|mut input| {
                input.close();
                Error::StringErr {
                    reason: "unsupported string aggregate iterator".to_string(),
                }
            })?;
            while let Some(point) = input.next() {
                if track_endpoints {
                    state.add_string_at(point.time(), point.value());
                } else {
                    state.add_count(1);
                }
            }
            input.close();
        }
        IteratorDataType::Unknown => {
            return Err(Error::StringErr {
                reason: "unknown aggregate iterator type".to_string(),
            });
        }
    }
    Ok(state)
}

/// Folds the canonical visibility-aware iterator into fixed-window/tag groups.
pub(crate) fn fold_grouped_aggregate_iterator(
    input: DynQueryIterator,
    field_type: crate::models::FieldType,
    window: GroupedAggregateWindow,
    group_tags: &[String],
    groups: &mut BTreeMap<GroupedAggregateKey, AggregateState>,
    points_raw_decoded: &mut u64,
) -> Result<()> {
    fn key(
        time: i64,
        tags: query_point::Tags,
        window: GroupedAggregateWindow,
        group_tags: &[String],
    ) -> Result<GroupedAggregateKey> {
        let values = tags.key_values();
        Ok(GroupedAggregateKey {
            window_start: window
                .bucket_start(time)
                .map_err(|reason| Error::StringErr {
                    reason: reason.to_string(),
                })?,
            tag_values: group_tags
                .iter()
                .map(|tag| values.get(tag).cloned())
                .collect(),
        })
    }

    macro_rules! fold_typed {
        ($converter:ident, $label:literal, $add:ident) => {{
            let mut input = $converter(input).map_err(|mut input| {
                input.close();
                Error::StringErr {
                    reason: concat!("unsupported ", $label, " grouped aggregate iterator")
                        .to_string(),
                }
            })?;
            let result = (|| -> Result<()> {
                while let Some(point) = input.next() {
                    let key = key(point.time(), point.tags(), window, group_tags)?;
                    groups
                        .entry(key)
                        .or_insert_with(|| AggregateState::empty(field_type))
                        .$add(point.value());
                    *points_raw_decoded = points_raw_decoded.saturating_add(1);
                }
                Ok(())
            })();
            input.close();
            result?;
        }};
    }

    match input.data_type() {
        IteratorDataType::Integer => fold_typed!(into_integer_iterator, "integer", add_integer),
        IteratorDataType::Unsigned => {
            fold_typed!(into_unsigned_iterator, "unsigned", add_unsigned)
        }
        IteratorDataType::Float => fold_typed!(into_float_iterator, "float", add_float),
        IteratorDataType::Boolean => {
            let mut input = into_bool_iterator(input).map_err(|mut input| {
                input.close();
                Error::StringErr {
                    reason: "unsupported boolean grouped aggregate iterator".to_string(),
                }
            })?;
            let result = (|| -> Result<()> {
                while let Some(point) = input.next() {
                    let key = key(point.time(), point.tags(), window, group_tags)?;
                    groups
                        .entry(key)
                        .or_insert_with(|| AggregateState::empty(field_type))
                        .add_count(1);
                    *points_raw_decoded = points_raw_decoded.saturating_add(1);
                }
                Ok(())
            })();
            input.close();
            result?;
        }
        IteratorDataType::String => {
            let mut input = into_string_iterator(input).map_err(|mut input| {
                input.close();
                Error::StringErr {
                    reason: "unsupported string grouped aggregate iterator".to_string(),
                }
            })?;
            let result = (|| -> Result<()> {
                while let Some(point) = input.next() {
                    let key = key(point.time(), point.tags(), window, group_tags)?;
                    groups
                        .entry(key)
                        .or_insert_with(|| AggregateState::empty(field_type))
                        .add_count(1);
                    *points_raw_decoded = points_raw_decoded.saturating_add(1);
                }
                Ok(())
            })();
            input.close();
            result?;
        }
        IteratorDataType::Unknown => {
            return Err(Error::StringErr {
                reason: "unknown grouped aggregate iterator type".to_string(),
            });
        }
    }
    Ok(())
}

/// Folds one aux-only visibility-aware iterator into all requested physical
/// target states. The iterator's timestamp domain is the union of the target
/// and row-domain field cursors, so a group is retained even when every target
/// is NULL at one of its logical rows.
pub(crate) fn fold_grouped_multi_aggregate_iterator(
    input: DynQueryIterator,
    targets: &[GroupedAggregateTarget],
    window: GroupedAggregateWindow,
    group_tags: &[String],
    groups: &mut BTreeMap<GroupedAggregateKey, Vec<AggregateState>>,
    points_raw_decoded: &mut u64,
    #[cfg(feature = "slow-trace")] profile: &mut GroupedAggregateProfileMetrics,
) -> Result<()> {
    let mut input = into_float_iterator(input).map_err(|mut input| {
        input.close();
        Error::StringErr {
            reason: "multi-field grouped aggregate requires an aux-only iterator".to_string(),
        }
    })?;
    let result = (|| -> Result<()> {
        loop {
            #[cfg(feature = "slow-trace")]
            let next_started = Instant::now();
            let point = input.next();
            #[cfg(feature = "slow-trace")]
            {
                profile.iterator_next_us = profile.iterator_next_us.saturating_add(
                    next_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                );
            }
            let Some(point) = point else {
                break;
            };
            #[cfg(feature = "slow-trace")]
            {
                profile.logical_rows = profile.logical_rows.saturating_add(1);
            }
            if point.aux().len() != targets.len() {
                return Err(Error::StringErr {
                    reason: format!(
                        "multi-field grouped aggregate expected {} target values, received {}",
                        targets.len(),
                        point.aux().len()
                    ),
                });
            }
            #[cfg(feature = "slow-trace")]
            let timestamp_domain_started = Instant::now();
            let tag_values = point.tags().key_values();
            let key = GroupedAggregateKey {
                window_start: window.bucket_start(point.time()).map_err(|reason| {
                    Error::StringErr {
                        reason: reason.to_string(),
                    }
                })?,
                tag_values: group_tags
                    .iter()
                    .map(|tag| tag_values.get(tag).cloned())
                    .collect(),
            };
            let states = groups.entry(key).or_insert_with(|| {
                targets
                    .iter()
                    .map(|target| AggregateState::empty(target.expected_field_type))
                    .collect()
            });
            #[cfg(feature = "slow-trace")]
            {
                profile.timestamp_domain_us = profile.timestamp_domain_us.saturating_add(
                    timestamp_domain_started
                        .elapsed()
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64,
                );
            }
            #[cfg(feature = "slow-trace")]
            let aggregate_update_started = Instant::now();
            for ((target, state), value) in targets.iter().zip(states.iter_mut()).zip(point.aux()) {
                match (target.expected_field_type, value) {
                    (_, Val::Empty(_)) => continue,
                    (crate::models::INTEGER, Val::I64(value)) => state.add_integer(*value),
                    (crate::models::UNSIGNED, Val::U64(value)) => state.add_unsigned(*value),
                    (crate::models::FLOAT, Val::F64(value)) => state.add_float(*value),
                    (crate::models::BOOL, Val::Bool(_))
                    | (crate::models::STRING, Val::String(_)) => state.add_count(1),
                    (_, value) => {
                        return Err(Error::StringErr {
                            reason: format!(
                                "grouped aggregate field {} expected type {}, received {value:?}",
                                target.field, target.expected_field_type
                            ),
                        });
                    }
                }
                *points_raw_decoded = points_raw_decoded.saturating_add(1);
                #[cfg(feature = "slow-trace")]
                {
                    profile.aggregate_updates = profile.aggregate_updates.saturating_add(1);
                }
            }
            #[cfg(feature = "slow-trace")]
            {
                profile.aggregate_update_us = profile.aggregate_update_us.saturating_add(
                    aggregate_update_started
                        .elapsed()
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64,
                );
            }
        }
        #[cfg(feature = "slow-trace")]
        {
            profile.groups = groups.len() as u64;
        }
        Ok(())
    })();
    input.close();
    result
}

/// Adds the fixed-window/tag keys present in the complete logical row domain.
/// Existing field aggregate states are preserved; rows missing the projected
/// field create an empty state so SQL COUNT/SUM/MIN/MAX/AVG null semantics are
/// retained.
pub(crate) fn fold_grouped_row_keys(
    input: DynQueryIterator,
    field_type: crate::models::FieldType,
    window: GroupedAggregateWindow,
    group_tags: &[String],
    groups: &mut BTreeMap<GroupedAggregateKey, AggregateState>,
    points_raw_decoded: &mut u64,
) -> Result<()> {
    fold_grouped_row_domain_keys(input, window, group_tags, points_raw_decoded, |key| {
        groups
            .entry(key)
            .or_insert_with(|| AggregateState::empty(field_type));
    })
}

/// Adds canonical row-domain keys to a positional multi-target result.
pub(crate) fn fold_grouped_multi_row_keys(
    input: DynQueryIterator,
    targets: &[GroupedAggregateTarget],
    window: GroupedAggregateWindow,
    group_tags: &[String],
    groups: &mut BTreeMap<GroupedAggregateKey, Vec<AggregateState>>,
    points_raw_decoded: &mut u64,
) -> Result<()> {
    fold_grouped_row_domain_keys(input, window, group_tags, points_raw_decoded, |key| {
        groups.entry(key).or_insert_with(|| {
            targets
                .iter()
                .map(|target| AggregateState::empty(target.expected_field_type))
                .collect()
        });
    })
}

fn fold_grouped_row_domain_keys(
    input: DynQueryIterator,
    window: GroupedAggregateWindow,
    group_tags: &[String],
    points_raw_decoded: &mut u64,
    mut insert_key: impl FnMut(GroupedAggregateKey),
) -> Result<()> {
    fn add_key(
        time: i64,
        tags: query_point::Tags,
        window: GroupedAggregateWindow,
        group_tags: &[String],
        points_raw_decoded: &mut u64,
    ) -> Result<GroupedAggregateKey> {
        let values = tags.key_values();
        let key = GroupedAggregateKey {
            window_start: window
                .bucket_start(time)
                .map_err(|reason| Error::StringErr {
                    reason: reason.to_string(),
                })?,
            tag_values: group_tags
                .iter()
                .map(|tag| values.get(tag).cloned())
                .collect(),
        };
        *points_raw_decoded = points_raw_decoded.saturating_add(1);
        Ok(key)
    }

    macro_rules! fold_typed {
        ($converter:ident, $label:literal) => {{
            let mut input = $converter(input).map_err(|mut input| {
                input.close();
                Error::StringErr {
                    reason: concat!("unsupported ", $label, " grouped row-domain iterator")
                        .to_string(),
                }
            })?;
            let result = (|| -> Result<()> {
                while let Some(point) = input.next() {
                    let key = add_key(
                        point.time(),
                        point.tags(),
                        window,
                        group_tags,
                        points_raw_decoded,
                    )?;
                    insert_key(key);
                }
                Ok(())
            })();
            input.close();
            result?;
        }};
    }

    match input.data_type() {
        IteratorDataType::Integer => fold_typed!(into_integer_iterator, "integer"),
        IteratorDataType::Unsigned => fold_typed!(into_unsigned_iterator, "unsigned"),
        IteratorDataType::Float => fold_typed!(into_float_iterator, "float"),
        IteratorDataType::Boolean => fold_typed!(into_bool_iterator, "boolean"),
        IteratorDataType::String => fold_typed!(into_string_iterator, "string"),
        IteratorDataType::Unknown => {
            return Err(Error::StringErr {
                reason: "unknown grouped row-domain iterator type".to_string(),
            });
        }
    }
    Ok(())
}

/// Counts logical rows produced by the canonical visibility-aware iterator.
///
/// This is the exact fallback for storage row-domain counting. It deliberately
/// consumes points inside ReefsDB instead of exposing them as Arrow batches.
pub(crate) fn count_iterator_rows(input: DynQueryIterator) -> Result<u64> {
    macro_rules! count_typed {
        ($converter:ident, $label:literal) => {{
            let mut input = $converter(input).map_err(|mut input| {
                let _ = input.close();
                Error::StringErr {
                    reason: concat!("unsupported ", $label, " row-count iterator").to_string(),
                }
            })?;
            let mut count = 0_u64;
            while input.next().is_some() {
                let Some(next) = count.checked_add(1) else {
                    let _ = input.close();
                    return Err(Error::StringErr {
                        reason: "row count overflow".to_string(),
                    });
                };
                count = next;
            }
            let _ = input.close();
            Ok(count)
        }};
    }

    match input.data_type() {
        IteratorDataType::Integer => count_typed!(into_integer_iterator, "integer"),
        IteratorDataType::Unsigned => count_typed!(into_unsigned_iterator, "unsigned"),
        IteratorDataType::Float => count_typed!(into_float_iterator, "float"),
        IteratorDataType::Boolean => count_typed!(into_bool_iterator, "boolean"),
        IteratorDataType::String => count_typed!(into_string_iterator, "string"),
        IteratorDataType::Unknown => {
            let mut input = input;
            let _ = input.close();
            Err(Error::StringErr {
                reason: "unknown row-count iterator type".to_string(),
            })
        }
    }
}

macro_rules! define_fallible_typed_merge {
    ($function:ident, $converter:ident, $trait_name:ident, $merge:ident, $sorted:ident) => {
        fn $function(
            inputs: Vec<DynQueryIterator>,
            opt: Arc<MergeOptions>,
        ) -> std::result::Result<DynQueryIterator, String> {
            let mut typed: Vec<Box<dyn $trait_name + Sync + Send>> = Vec::new();
            let mut remaining = inputs.into_iter();
            while let Some(input) = remaining.next() {
                match $converter(input) {
                    Ok(input) => typed.push(input),
                    Err(mut unsupported) => {
                        unsupported.close();
                        for mut input in typed {
                            input.close();
                        }
                        for mut input in remaining {
                            input.close();
                        }
                        return Err(format!(
                            "unsupported iterator wrapper for {:?}",
                            unsupported.data_type()
                        ));
                    }
                }
            }

            if opt.merge_sorted() {
                Ok(Box::new($sorted::new_with_options(typed, opt)))
            } else {
                Ok(Box::new($merge::new_with_options(typed, opt)))
            }
        }
    };
}

define_fallible_typed_merge!(
    merge_integer_iterators,
    into_integer_iterator,
    IntegerIterator,
    IntegerMergeIter,
    IntegerSortedMergeIter
);
define_fallible_typed_merge!(
    merge_unsigned_iterators,
    into_unsigned_iterator,
    UnsignedIterator,
    UnsignedMergeIter,
    UnsignedSortedMergeIter
);
define_fallible_typed_merge!(
    merge_float_iterators,
    into_float_iterator,
    FloatIterator,
    FloatMergeIter,
    FloatSortedMergeIter
);
define_fallible_typed_merge!(
    merge_bool_iterators,
    into_bool_iterator,
    BoolIterator,
    BoolMergeIter,
    BoolSortedMergeIter
);
define_fallible_typed_merge!(
    merge_string_iterators,
    into_string_iterator,
    StringIterator,
    StringMergeIter,
    StringSortedMergeIter
);

pub fn try_merge_storage(
    inputs: Vec<DynQueryIterator>,
    scan: Arc<StorageScan>,
) -> std::result::Result<Option<DynQueryIterator>, String> {
    let merge_options = Arc::new(MergeOptions::from_storage_scan(&scan));
    let interrupt = scan
        .cancellation
        .as_ref()
        .map(|cancellation| cancellation.token());
    try_merge_with_options(inputs, merge_options, interrupt)
}

fn try_merge_with_options(
    inputs: Vec<DynQueryIterator>,
    opt: Arc<MergeOptions>,
    interrupt: Option<CancellationToken>,
) -> std::result::Result<Option<DynQueryIterator>, String> {
    let Some(first) = inputs.first() else {
        return Ok(None);
    };
    let data_type = first.data_type();
    if let Some(actual) = inputs
        .iter()
        .map(|input| input.data_type())
        .find(|actual| *actual != data_type)
    {
        Iterators(inputs).close();
        return Err(format!(
            "iterator data type mismatch: expected {data_type:?}, found {actual:?}"
        ));
    }
    let mut matching = inputs;

    let predicate = match data_type {
        VarRefDataType::Integer => is_integer_iterator,
        VarRefDataType::Unsigned => is_unsigned_iterator,
        VarRefDataType::Float => is_float_iterator,
        VarRefDataType::Boolean => is_bool_iterator,
        VarRefDataType::String => is_string_iterator,
        _ => {
            Iterators(matching).close();
            return Err(format!("unsupported iterator data type: {data_type:?}"));
        }
    };
    if matching.iter().any(|input| !predicate(input)) {
        Iterators(matching).close();
        return Err(format!("unsupported iterator wrapper for {data_type:?}"));
    }

    let mut merged = if matching.len() == 1 {
        matching.pop().expect("one matching iterator")
    } else {
        match data_type {
            VarRefDataType::Integer => merge_integer_iterators(matching, Arc::clone(&opt))?,
            VarRefDataType::Unsigned => merge_unsigned_iterators(matching, Arc::clone(&opt))?,
            VarRefDataType::Float => merge_float_iterators(matching, Arc::clone(&opt))?,
            VarRefDataType::Boolean => merge_bool_iterators(matching, Arc::clone(&opt))?,
            VarRefDataType::String => merge_string_iterators(matching, Arc::clone(&opt))?,
            _ => unreachable!("validated iterator data type"),
        }
    };

    if let Some(interrupt) = interrupt {
        merged = try_new_interrupt_iter(merged, interrupt)?;
    }
    Ok(Some(merged))
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn try_merge_for_test(
    inputs: Vec<Box<dyn QueryIterator + Sync + Send>>,
    scan: Arc<StorageScan>,
) -> std::result::Result<Option<Box<dyn QueryIterator + Sync + Send>>, String> {
    try_merge_storage(inputs, scan)
}

fn try_new_interrupt_iter(
    input: DynQueryIterator,
    closing: CancellationToken,
) -> std::result::Result<DynQueryIterator, String> {
    let data_type = input.data_type();
    macro_rules! wrap {
        ($converter:ident, $wrapper:ident) => {
            match $converter(input) {
                Ok(input) => Ok(Box::new($wrapper::new(input, closing))),
                Err(mut input) => {
                    input.close();
                    Err(format!(
                        "unsupported interrupt iterator wrapper for {data_type:?}"
                    ))
                }
            }
        };
    }
    match data_type {
        VarRefDataType::Integer => wrap!(into_integer_iterator, IntegerInterruptIter),
        VarRefDataType::Unsigned => wrap!(into_unsigned_iterator, UnsignedInterruptIter),
        VarRefDataType::Float => wrap!(into_float_iterator, FloatInterruptIter),
        VarRefDataType::Boolean => wrap!(into_bool_iterator, BoolInterruptIter),
        VarRefDataType::String => wrap!(into_string_iterator, StringInterruptIter),
        _ => {
            let mut input = input;
            input.close();
            Err(format!(
                "unsupported interrupt iterator data type: {data_type:?}"
            ))
        }
    }
}

/// IntegerLimitIter represents an iterator that limits points per group.
pub struct IntegerLimitIter {
    input: Box<dyn IntegerIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,

    prev: Prev,
}

impl IntegerLimitIter {
    pub fn new(input: Box<dyn IntegerIterator + Sync + Send>, opt: StorageLimit) -> Self {
        Self {
            input,
            opt,
            n: 0,
            prev: Prev::default(),
        }
    }
}

impl QueryIterator for IntegerLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Integer
    }

    /// Returns stats from the underlying iterator.
    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    /// Close the underlying iterators.
    fn close(&mut self) {
        self.input.close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl IntegerIterator for IntegerLimitIter {
    fn next(&mut self) -> Option<query_point::IntegerPoint> {
        while let Some(p) = self.input.next() {
            // let p = p.unwrap();
            // Reset window and counter if a new window is encountered.
            if p.name != self.prev.name || !p.tags.equals(&self.prev.tags) {
                self.prev.name = p.name.clone();
                self.prev.tags = p.tags.clone();
                self.n = 0;
            }

            // Increment counter.
            self.n += 1;

            // Read next point if we're beyond the limit.
            if self.n <= self.opt.offset {
                continue;
            }

            // Read next point if not beyond the offset.
            if self.opt.limit > 0 && (self.n - self.opt.offset) > self.opt.limit {
                continue;
            }

            return Some(p.clone());
        }

        None
    }
}

/// UnsignedLimitIter represents an iterator that limits points per group.
pub struct UnsignedLimitIter {
    input: Box<dyn UnsignedIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,

    prev: Prev,
}

impl UnsignedLimitIter {
    pub fn new(input: Box<dyn UnsignedIterator + Sync + Send>, opt: StorageLimit) -> Self {
        Self {
            input,
            opt,
            n: 0,
            prev: Prev::default(),
        }
    }
}

impl QueryIterator for UnsignedLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Unsigned
    }

    /// Returns stats from the underlying iterator.
    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    /// Close the underlying iterators.
    fn close(&mut self) {
        self.input.close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl UnsignedIterator for UnsignedLimitIter {
    fn next(&mut self) -> Option<query_point::UnsignedPoint> {
        while let Some(p) = self.input.next() {
            // let p = p.unwrap();
            // Reset window and counter if a new window is encountered.
            if p.name != self.prev.name || !p.tags.equals(&self.prev.tags) {
                self.prev.name = p.name.clone();
                self.prev.tags = p.tags.clone();
                self.n = 0;
            }

            // Increment counter.
            self.n += 1;

            // Read next point if we're beyond the limit.
            if self.n <= self.opt.offset {
                continue;
            }

            // Read next point if not beyond the offset.
            if self.opt.limit > 0 && (self.n - self.opt.offset) > self.opt.limit {
                continue;
            }

            return Some(p.clone());
        }

        None
    }
}

/// FloatLimitIter represents an iterator that limits points per group.
pub struct FloatLimitIter {
    input: Box<dyn FloatIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,

    prev: Prev,
}

impl FloatLimitIter {
    pub fn new(input: Box<dyn FloatIterator + Sync + Send>, opt: StorageLimit) -> Self {
        Self {
            input,
            opt,
            n: 0,
            prev: Prev::default(),
        }
    }
}

impl QueryIterator for FloatLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Float
    }

    /// Returns stats from the underlying iterator.
    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    /// Close the underlying iterators.
    fn close(&mut self) {
        self.input.close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl FloatIterator for FloatLimitIter {
    fn next(&mut self) -> Option<query_point::FloatPoint> {
        while let Some(p) = self.input.next() {
            // let p = p.unwrap();
            // Reset window and counter if a new window is encountered.
            if p.name != self.prev.name || !p.tags.equals(&self.prev.tags) {
                self.prev.name = p.name.clone();
                self.prev.tags = p.tags.clone();
                self.n = 0;
            }

            // Increment counter.
            self.n += 1;

            // Read next point if we're beyond the limit.
            if self.n <= self.opt.offset {
                continue;
            }

            // Read next point if not beyond the offset.
            if self.opt.limit > 0 && (self.n - self.opt.offset) > self.opt.limit {
                continue;
            }

            return Some(p.clone());
        }

        None
    }
}
/// BoolLimitIter represents an iterator that limits points per group.
pub struct BoolLimitIter {
    input: Box<dyn BoolIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,

    prev: Prev,
}

impl BoolLimitIter {
    pub fn new(input: Box<dyn BoolIterator + Sync + Send>, opt: StorageLimit) -> Self {
        Self {
            input,
            opt,
            n: 0,
            prev: Prev::default(),
        }
    }
}

impl QueryIterator for BoolLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Boolean
    }

    /// Returns stats from the underlying iterator.
    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    /// Close the underlying iterators.
    fn close(&mut self) {
        self.input.close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl BoolIterator for BoolLimitIter {
    fn next(&mut self) -> Option<query_point::BoolPoint> {
        while let Some(p) = self.input.next() {
            // let p = p.unwrap();
            // Reset window and counter if a new window is encountered.
            if p.name != self.prev.name || !p.tags.equals(&self.prev.tags) {
                self.prev.name = p.name.clone();
                self.prev.tags = p.tags.clone();
                self.n = 0;
            }

            // Increment counter.
            self.n += 1;

            // Read next point if we're beyond the limit.
            if self.n <= self.opt.offset {
                continue;
            }

            // Read next point if not beyond the offset.
            if self.opt.limit > 0 && (self.n - self.opt.offset) > self.opt.limit {
                continue;
            }

            return Some(p.clone());
        }

        None
    }
}

/// StringLimitIter represents an iterator that limits points per group.
pub struct StringLimitIter {
    input: Box<dyn StringIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,

    prev: Prev,
}

impl StringLimitIter {
    pub fn new(input: Box<dyn StringIterator + Sync + Send>, opt: StorageLimit) -> Self {
        Self {
            input,
            opt,
            n: 0,
            prev: Prev::default(),
        }
    }
}

impl QueryIterator for StringLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::String
    }

    /// Returns stats from the underlying iterator.
    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    /// Close the underlying iterators.
    fn close(&mut self) {
        self.input.close();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl StringIterator for StringLimitIter {
    fn next(&mut self) -> Option<query_point::StringPoint> {
        while let Some(p) = self.input.next() {
            // let p = p.unwrap();
            // Reset window and counter if a new window is encountered.
            if p.name != self.prev.name || !p.tags.equals(&self.prev.tags) {
                self.prev.name = p.name.clone();
                self.prev.tags = p.tags.clone();
                self.n = 0;
            }

            // Increment counter.
            self.n += 1;

            // Read next point if we're beyond the limit.
            if self.n <= self.opt.offset {
                continue;
            }

            // Read next point if not beyond the offset.
            if self.opt.limit > 0 && (self.n - self.opt.offset) > self.opt.limit {
                continue;
            }

            return Some(p.clone());
        }

        None
    }
}

macro_rules! define_series_limit_iterator {
    ($name:ident, $trait_name:ident, $point:ty, $data_type:expr) => {
        pub struct $name {
            input: Box<dyn $trait_name + Sync + Send>,
            opt: StorageLimit,
            prev: Prev,
            series_n: i64,
        }

        impl $name {
            fn new(input: Box<dyn $trait_name + Sync + Send>, opt: StorageLimit) -> Self {
                Self {
                    input,
                    opt,
                    prev: Prev::default(),
                    series_n: 0,
                }
            }
        }

        impl QueryIterator for $name {
            fn data_type(&self) -> VarRefDataType {
                $data_type
            }

            fn stats(&self) -> IteratorStats {
                self.input.stats()
            }

            fn close(&mut self) {
                self.input.close();
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }

            fn into_any(self: Box<Self>) -> Box<dyn Any> {
                self
            }
        }

        impl $trait_name for $name {
            fn next(&mut self) -> Option<$point> {
                while let Some(point) = self.input.next() {
                    if point.name != self.prev.name || !point.tags.equals(&self.prev.tags) {
                        self.prev.name = point.name.clone();
                        self.prev.tags = point.tags.clone();
                        self.series_n += 1;
                    }
                    if self.series_n <= self.opt.offset {
                        continue;
                    }
                    if self.opt.limit > 0 && self.series_n - self.opt.offset > self.opt.limit {
                        continue;
                    }
                    return Some(point);
                }
                None
            }
        }
    };
}

define_series_limit_iterator!(
    IntegerSeriesLimitIter,
    IntegerIterator,
    query_point::IntegerPoint,
    VarRefDataType::Integer
);
define_series_limit_iterator!(
    UnsignedSeriesLimitIter,
    UnsignedIterator,
    query_point::UnsignedPoint,
    VarRefDataType::Unsigned
);
define_series_limit_iterator!(
    FloatSeriesLimitIter,
    FloatIterator,
    query_point::FloatPoint,
    VarRefDataType::Float
);
define_series_limit_iterator!(
    BoolSeriesLimitIter,
    BoolIterator,
    query_point::BoolPoint,
    VarRefDataType::Boolean
);
define_series_limit_iterator!(
    StringSeriesLimitIter,
    StringIterator,
    query_point::StringPoint,
    VarRefDataType::String
);

pub fn try_new_series_limit_iter(
    input: DynQueryIterator,
    opt: StorageLimit,
) -> std::result::Result<DynQueryIterator, String> {
    let data_type = input.data_type();
    macro_rules! wrap {
        ($converter:ident, $wrapper:ident) => {
            match $converter(input) {
                Ok(input) => Ok(Box::new($wrapper::new(input, opt))),
                Err(mut input) => {
                    input.close();
                    Err(format!(
                        "unsupported series limit iterator for {data_type:?}"
                    ))
                }
            }
        };
    }
    match data_type {
        VarRefDataType::Integer => wrap!(into_integer_iterator, IntegerSeriesLimitIter),
        VarRefDataType::Unsigned => wrap!(into_unsigned_iterator, UnsignedSeriesLimitIter),
        VarRefDataType::Float => wrap!(into_float_iterator, FloatSeriesLimitIter),
        VarRefDataType::Boolean => wrap!(into_bool_iterator, BoolSeriesLimitIter),
        VarRefDataType::String => wrap!(into_string_iterator, StringSeriesLimitIter),
        _ => {
            let mut input = input;
            input.close();
            Err(format!("unsupported series limit data type: {data_type:?}"))
        }
    }
}

pub fn try_new_limit_iter(
    input: DynQueryIterator,
    opt: StorageLimit,
) -> std::result::Result<DynQueryIterator, String> {
    let data_type = input.data_type();
    macro_rules! wrap {
        ($converter:ident, $wrapper:ident) => {
            match $converter(input) {
                Ok(input) => Ok(Box::new($wrapper::new(input, opt))),
                Err(mut input) => {
                    input.close();
                    Err(format!(
                        "unsupported point limit iterator for {data_type:?}"
                    ))
                }
            }
        };
    }
    match data_type {
        VarRefDataType::Integer => wrap!(into_integer_iterator, IntegerLimitIter),
        VarRefDataType::Unsigned => wrap!(into_unsigned_iterator, UnsignedLimitIter),
        VarRefDataType::Float => wrap!(into_float_iterator, FloatLimitIter),
        VarRefDataType::Boolean => wrap!(into_bool_iterator, BoolLimitIter),
        VarRefDataType::String => wrap!(into_string_iterator, StringLimitIter),
        _ => {
            let mut input = input;
            input.close();
            Err(format!("unsupported point limit data type: {data_type:?}"))
        }
    }
}

pub fn new_limit_iter(
    itr: Box<dyn Any>,
    opt: StorageLimit,
) -> Box<dyn QueryIterator + Sync + Send> {
    if (*itr).downcast_ref::<IntegerIter>().is_some() {
        let itr = itr.downcast::<IntegerIter>().unwrap();
        return Box::new(IntegerLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<IntegerMergeIter>().is_some() {
        let itr = itr.downcast::<IntegerMergeIter>().unwrap();
        return Box::new(IntegerLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<IntegerSortedMergeIter>().is_some() {
        let itr = itr.downcast::<IntegerSortedMergeIter>().unwrap();
        return Box::new(IntegerLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<UnsignedIter>().is_some() {
        let itr = itr.downcast::<UnsignedIter>().unwrap();
        return Box::new(UnsignedLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<UnsignedMergeIter>().is_some() {
        let itr = itr.downcast::<UnsignedMergeIter>().unwrap();
        return Box::new(UnsignedLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<UnsignedSortedMergeIter>().is_some() {
        let itr = itr.downcast::<UnsignedSortedMergeIter>().unwrap();
        return Box::new(UnsignedLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<FloatIter>().is_some() {
        let itr = itr.downcast::<FloatIter>().unwrap();
        return Box::new(FloatLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<FloatMergeIter>().is_some() {
        let itr = itr.downcast::<FloatMergeIter>().unwrap();
        return Box::new(FloatLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<FloatSortedMergeIter>().is_some() {
        let itr = itr.downcast::<FloatSortedMergeIter>().unwrap();
        return Box::new(FloatLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<BoolIter>().is_some() {
        let itr = itr.downcast::<BoolIter>().unwrap();
        return Box::new(BoolLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<BoolMergeIter>().is_some() {
        let itr = itr.downcast::<BoolMergeIter>().unwrap();
        return Box::new(BoolLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<BoolSortedMergeIter>().is_some() {
        let itr = itr.downcast::<BoolSortedMergeIter>().unwrap();
        return Box::new(BoolLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<StringIter>().is_some() {
        let itr = itr.downcast::<StringIter>().unwrap();
        return Box::new(StringLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<StringMergeIter>().is_some() {
        let itr = itr.downcast::<StringMergeIter>().unwrap();
        return Box::new(StringLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<StringSortedMergeIter>().is_some() {
        let itr = itr.downcast::<StringSortedMergeIter>().unwrap();
        return Box::new(StringLimitIter::new(itr, opt));
    }

    panic!("unsupported limit iterator type: {:?}", itr);
}

// ////////////////////////////////////////////////////////////
// /////////////// IntegerInterruptIter   /////////////////////////
// ////////////////////////////////////////////////////////////

pub struct IntegerInterruptIter {
    input: Box<dyn IntegerIterator + Sync + Send>,
    closing: CancellationToken,
    count: i64,
    closed: bool,
}

impl IntegerInterruptIter {
    pub fn new(input: Box<dyn IntegerIterator + Sync + Send>, closing: CancellationToken) -> Self {
        Self {
            input,
            closing,
            count: 0,
            closed: false,
        }
    }
}

impl QueryIterator for IntegerInterruptIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Integer
    }

    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.input.close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl IntegerIterator for IntegerInterruptIter {
    fn next(&mut self) -> Option<query_point::IntegerPoint> {
        // Only check if the channel is closed every N points. This
        // intentionally checks on both 0 and N so that if the iterator
        // has been interrupted before the first point is emitted it will
        // not emit any points.
        if self.count & 0xFF == 0xFF {
            // select! {
            //     recv(self.closing.receiver()) -> _ =>  {
            //         self.close();
            //         return None;
            //     },
            //    default => {
            //     // Reset iterator count to zero and fall through to emit the next point.
            //     self.count = 0;
            //    },
            // }
            if self.closing.is_cancelled() {
                self.close();
                return None;
            } else {
                // Reset iterator count to zero and fall through to emit the next point.
                self.count = 0;
            }
        }

        // Increment the counter for every point read.
        self.count += 1;
        self.input.next()
    }
}

pub struct UnsignedInterruptIter {
    input: Box<dyn UnsignedIterator + Sync + Send>,
    closing: CancellationToken,
    count: i64,
    closed: bool,
}

impl UnsignedInterruptIter {
    pub fn new(input: Box<dyn UnsignedIterator + Sync + Send>, closing: CancellationToken) -> Self {
        Self {
            input,
            closing,
            count: 0,
            closed: false,
        }
    }
}

impl QueryIterator for UnsignedInterruptIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Unsigned
    }

    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.input.close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl UnsignedIterator for UnsignedInterruptIter {
    fn next(&mut self) -> Option<query_point::UnsignedPoint> {
        // Only check if the channel is closed every N points. This
        // intentionally checks on both 0 and N so that if the iterator
        // has been interrupted before the first point is emitted it will
        // not emit any points.
        if self.count & 0xFF == 0xFF {
            // select! {
            //     recv(self.closing.receiver()) -> _ =>  {
            //         self.close();
            //         return None;
            //     },
            //    default => {
            //     // Reset iterator count to zero and fall through to emit the next point.
            //     self.count = 0;
            //    },
            // }
            if self.closing.is_cancelled() {
                self.close();
                return None;
            } else {
                // Reset iterator count to zero and fall through to emit the next point.
                self.count = 0;
            }
        }

        // Increment the counter for every point read.
        self.count += 1;
        self.input.next()
    }
}

pub struct FloatInterruptIter {
    input: Box<dyn FloatIterator + Sync + Send>,
    closing: CancellationToken,
    count: i64,
    closed: bool,
}

impl FloatInterruptIter {
    pub fn new(input: Box<dyn FloatIterator + Sync + Send>, closing: CancellationToken) -> Self {
        Self {
            input,
            closing,
            count: 0,
            closed: false,
        }
    }
}

impl QueryIterator for FloatInterruptIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Float
    }

    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.input.close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl FloatIterator for FloatInterruptIter {
    fn next(&mut self) -> Option<query_point::FloatPoint> {
        // Only check if the channel is closed every N points. This
        // intentionally checks on both 0 and N so that if the iterator
        // has been interrupted before the first point is emitted it will
        // not emit any points.
        if self.count & 0xFF == 0xFF {
            // select! {
            //     recv(self.closing.receiver()) -> _ =>  {
            //         self.close();
            //         return None;
            //     },
            //    default => {
            //     // Reset iterator count to zero and fall through to emit the next point.
            //     self.count = 0;
            //    },
            // }
            if self.closing.is_cancelled() {
                self.close();
                return None;
            } else {
                // Reset iterator count to zero and fall through to emit the next point.
                self.count = 0;
            }
        }

        // Increment the counter for every point read.
        self.count += 1;
        self.input.next()
    }
}

pub struct BoolInterruptIter {
    input: Box<dyn BoolIterator + Sync + Send>,
    closing: CancellationToken,
    count: i64,
    closed: bool,
}

impl BoolInterruptIter {
    pub fn new(input: Box<dyn BoolIterator + Sync + Send>, closing: CancellationToken) -> Self {
        Self {
            input,
            closing,
            count: 0,
            closed: false,
        }
    }
}

impl QueryIterator for BoolInterruptIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Boolean
    }

    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.input.close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl BoolIterator for BoolInterruptIter {
    fn next(&mut self) -> Option<query_point::BoolPoint> {
        // Only check if the channel is closed every N points. This
        // intentionally checks on both 0 and N so that if the iterator
        // has been interrupted before the first point is emitted it will
        // not emit any points.
        if self.count & 0xFF == 0xFF {
            // select! {
            //     recv(self.closing.receiver()) -> _ =>  {
            //         self.close();
            //         return None;
            //     },
            //    default => {
            //     // Reset iterator count to zero and fall through to emit the next point.
            //     self.count = 0;
            //    },
            // }
            if self.closing.is_cancelled() {
                self.close();
                return None;
            } else {
                // Reset iterator count to zero and fall through to emit the next point.
                self.count = 0;
            }
        }

        // Increment the counter for every point read.
        self.count += 1;
        self.input.next()
    }
}

pub struct StringInterruptIter {
    input: Box<dyn StringIterator + Sync + Send>,
    closing: CancellationToken,
    count: i64,
    closed: bool,
}

impl StringInterruptIter {
    pub fn new(input: Box<dyn StringIterator + Sync + Send>, closing: CancellationToken) -> Self {
        Self {
            input,
            closing,
            count: 0,
            closed: false,
        }
    }
}

impl QueryIterator for StringInterruptIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::String
    }

    fn stats(&self) -> IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.input.close();
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl StringIterator for StringInterruptIter {
    fn next(&mut self) -> Option<query_point::StringPoint> {
        // Only check if the channel is closed every N points. This
        // intentionally checks on both 0 and N so that if the iterator
        // has been interrupted before the first point is emitted it will
        // not emit any points.
        if self.count & 0xFF == 0xFF {
            // select! {
            //     recv(self.closing.receiver()) -> _ =>  {
            //         self.close();
            //         return None;
            //     },
            //    default => {
            //     // Reset iterator count to zero and fall through to emit the next point.
            //     self.count = 0;
            //    },
            // }
            if self.closing.is_cancelled() {
                self.close();
                return None;
            } else {
                // Reset iterator count to zero and fall through to emit the next point.
                self.count = 0;
            }
        }

        // Increment the counter for every point read.
        self.count += 1;
        self.input.next()
    }
}

#[cfg(test)]
mod stage4_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    macro_rules! dummy_iterator {
        ($name:ident, $trait_name:ident, $point:ty, $data_type:expr) => {
            struct $name;

            impl QueryIterator for $name {
                fn data_type(&self) -> VarRefDataType {
                    $data_type
                }

                fn stats(&self) -> IteratorStats {
                    IteratorStats::default()
                }

                fn close(&mut self) {}

                fn as_any(&self) -> &dyn Any {
                    self
                }

                fn as_any_mut(&mut self) -> &mut dyn Any {
                    self
                }

                fn into_any(self: Box<Self>) -> Box<dyn Any> {
                    self
                }
            }

            impl $trait_name for $name {
                fn next(&mut self) -> Option<$point> {
                    None
                }
            }
        };
    }

    dummy_iterator!(
        DummyInteger,
        IntegerIterator,
        query_point::IntegerPoint,
        VarRefDataType::Integer
    );
    dummy_iterator!(
        DummyUnsigned,
        UnsignedIterator,
        query_point::UnsignedPoint,
        VarRefDataType::Unsigned
    );
    dummy_iterator!(
        DummyFloat,
        FloatIterator,
        query_point::FloatPoint,
        VarRefDataType::Float
    );
    dummy_iterator!(
        DummyBool,
        BoolIterator,
        query_point::BoolPoint,
        VarRefDataType::Boolean
    );
    dummy_iterator!(
        DummyString,
        StringIterator,
        query_point::StringPoint,
        VarRefDataType::String
    );

    struct TrackingInteger {
        closes: Arc<AtomicUsize>,
    }

    impl QueryIterator for TrackingInteger {
        fn data_type(&self) -> VarRefDataType {
            VarRefDataType::Integer
        }

        fn stats(&self) -> IteratorStats {
            IteratorStats::default()
        }

        fn close(&mut self) {
            self.closes.fetch_add(1, AtomicOrdering::SeqCst);
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any> {
            self
        }
    }

    impl IntegerIterator for TrackingInteger {
        fn next(&mut self) -> Option<query_point::IntegerPoint> {
            None
        }
    }

    struct TrackingFloat {
        closes: Arc<AtomicUsize>,
    }

    impl QueryIterator for TrackingFloat {
        fn data_type(&self) -> VarRefDataType {
            VarRefDataType::Float
        }

        fn stats(&self) -> IteratorStats {
            IteratorStats::default()
        }

        fn close(&mut self) {
            self.closes.fetch_add(1, AtomicOrdering::SeqCst);
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any> {
            self
        }
    }

    impl FloatIterator for TrackingFloat {
        fn next(&mut self) -> Option<query_point::FloatPoint> {
            None
        }
    }

    #[test]
    fn try_merge_rejects_incompatible_types_without_dropping_a_child() {
        let closes = Arc::new(AtomicUsize::new(0));
        let inputs = vec![
            Box::new(TrackingInteger {
                closes: Arc::clone(&closes),
            }) as DynQueryIterator,
            Box::new(TrackingFloat {
                closes: Arc::clone(&closes),
            }) as DynQueryIterator,
        ];

        let options = Arc::new(MergeOptions {
            dimensions: Vec::new(),
            start_time: MIN_NANO_TIME_I64,
            end_time: MAX_NANO_TIME_I64,
            ascending: true,
            output_order: OutputOrder::Unspecified,
            interval: None,
        });
        let error = match try_merge_with_options(inputs, options, None) {
            Ok(_) => panic!("heterogeneous merge must fail"),
            Err(error) => error,
        };
        assert!(error.contains("expected Integer, found Float"), "{error}");
        assert_eq!(
            closes.load(AtomicOrdering::SeqCst),
            2,
            "all children must be closed when the merge is rejected"
        );
    }

    macro_rules! aux_comparator_test {
        ($test:ident, $dummy:ident, $trait_name:ident, $point:ty, $heap:ident, $item:ident) => {
            #[test]
            fn $test() {
                let input = || {
                    Arc::new(RwLock::new(
                        Box::new($dummy) as Box<dyn $trait_name + Send + Sync>
                    ))
                };
                let point = |value: &str| {
                    let mut point = <$point>::default();
                    point.aux = vec![Val::String(value.to_string())];
                    point
                };
                let options = MergeOptions {
                    dimensions: Vec::new(),
                    start_time: MIN_NANO_TIME_I64,
                    end_time: MAX_NANO_TIME_I64,
                    ascending: true,
                    output_order: OutputOrder::Unspecified,
                    interval: None,
                };
                let heap = $heap {
                    opt: Arc::new(options),
                    items: vec![
                        $item {
                            point: Some(point("a")),
                            itr: input(),
                            fast_idx: 0,
                        },
                        $item {
                            point: Some(point("b")),
                            itr: input(),
                            fast_idx: 1,
                        },
                    ],
                    fast: false,
                };
                assert!(heap.less_heap_item(0, 1));
                assert!(!heap.less_heap_item(1, 0));

                let mut descending = (*heap.opt).clone();
                descending.ascending = false;
                let heap = $heap {
                    opt: Arc::new(descending),
                    ..heap
                };
                assert!(!heap.less_heap_item(0, 1));
                assert!(heap.less_heap_item(1, 0));
            }
        };
    }

    aux_comparator_test!(
        integer_aux_comparator_uses_x_and_y,
        DummyInteger,
        IntegerIterator,
        query_point::IntegerPoint,
        IntegerSortedMergeHeap,
        IntegerSortedMergeHeapItem
    );
    aux_comparator_test!(
        unsigned_aux_comparator_uses_x_and_y,
        DummyUnsigned,
        UnsignedIterator,
        query_point::UnsignedPoint,
        UnsignedSortedMergeHeap,
        UnsignedSortedMergeHeapItem
    );
    aux_comparator_test!(
        float_aux_comparator_uses_x_and_y,
        DummyFloat,
        FloatIterator,
        query_point::FloatPoint,
        FloatSortedMergeHeap,
        FloatSortedMergeHeapItem
    );
    aux_comparator_test!(
        boolean_aux_comparator_uses_x_and_y,
        DummyBool,
        BoolIterator,
        query_point::BoolPoint,
        BoolSortedMergeHeap,
        BoolSortedMergeHeapItem
    );
    aux_comparator_test!(
        string_aux_comparator_uses_x_and_y,
        DummyString,
        StringIterator,
        query_point::StringPoint,
        StringSortedMergeHeap,
        StringSortedMergeHeapItem
    );
}
