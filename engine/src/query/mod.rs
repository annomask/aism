//! Storage query contracts and iterator implementations.
mod aggregate_sum;

pub(crate) mod metadata;
pub(crate) mod predicate_eval;
pub(crate) mod query_iterator;
pub(crate) mod query_point;
pub(crate) mod record_batch;
pub(crate) mod storage_ir;
pub(crate) mod projected_aggregate;
pub(crate) mod aggregate_windows;
pub(crate) mod numeric_aggregate;
pub(crate) mod streaming_aggregate;

pub use crate::storage::active_cursor::{
    BoolIter as BoolCursorIterator, BoolLimitIter as BoolCursorLimitIterator,
    FloatIter as FloatCursorIterator, FloatLimitIter as FloatCursorLimitIterator,
    IntegerIter as IntegerCursorIterator, IntegerLimitIter as IntegerCursorLimitIterator,
    StringIter as StringCursorIterator, StringLimitIter as StringCursorLimitIterator,
    UnsignedIter as UnsignedCursorIterator, UnsignedLimitIter as UnsignedCursorLimitIterator,
};
pub use query_iterator::{
    BoolInterruptIter, BoolIterator, BoolLimitIter, BoolMergeIter, BoolSeriesLimitIter,
    BoolSortedMergeIter, FloatInterruptIter, FloatIterator, FloatLimitIter, FloatMergeIter,
    FloatSeriesLimitIter, FloatSortedMergeIter, IntegerInterruptIter, IntegerIterator,
    IntegerLimitIter, IntegerMergeIter, IntegerSeriesLimitIter, IntegerSortedMergeIter,
    IteratorDataType, IteratorStats, Iterators, QueryIterator, StringInterruptIter, StringIterator,
    StringLimitIter, StringMergeIter, StringSeriesLimitIter, StringSortedMergeIter,
    UnsignedInterruptIter, UnsignedIterator, UnsignedLimitIter, UnsignedMergeIter,
    UnsignedSeriesLimitIter, UnsignedSortedMergeIter, try_merge_storage, try_new_limit_iter,
    try_new_series_limit_iter,
};
pub use query_point::{BoolPoint, FloatPoint, IntegerPoint, StringPoint, Tags, UnsignedPoint};

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use query_iterator::try_merge_for_test;
