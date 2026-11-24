use std::{
    any::Any,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::models::points::{BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Val};
use parking_lot::Mutex;

use super::{
    cursor::EOF, data_encoding::encoding_values::Values, key_cursor::KeyCursor,
    vlog::key_cursor::VLogKeyCursor,
};
use crate::query::{
    predicate_eval::PreparedStoragePredicate,
    query_iterator::{
        BoolInterruptIter, BoolMergeIter, BoolSortedMergeIter, FloatInterruptIter, FloatMergeIter,
        FloatSortedMergeIter, IntegerInterruptIter, IntegerMergeIter, IntegerSortedMergeIter,
        StringInterruptIter, StringMergeIter, StringSortedMergeIter, UnsignedInterruptIter,
        UnsignedMergeIter, UnsignedSortedMergeIter,
    },
    storage_ir::{ColumnKind, StorageLimit, StorageScan},
};
use crate::query::{
    query_iterator::{
        self, BoolIterator, FloatIterator, IntegerIterator, IteratorDataType as VarRefDataType,
        QueryIterator, UnsignedIterator,
    },
    query_point,
};

/// STATS_BUFFER_COPY_INTERVAL_N is the number of points that are read before
/// copying the stats buffer to the iterator's stats field. This is used to
/// amortize the cost of using a mutex when updating stats.
const STATS_BUFFER_COPY_INTERVAL_N: i32 = 100;

#[derive(Default)]
struct ConditionCursors {
    cursors: Vec<Box<dyn CursorAt + Sync + Send>>,
    values: Vec<Option<Val>>,
    predicate: Option<PreparedStoragePredicate>,
}

impl ConditionCursors {
    fn new(
        predicate: Option<PreparedStoragePredicate>,
        cursors: Vec<Box<dyn CursorAt + Sync + Send>>,
    ) -> Self {
        debug_assert!(
            predicate
                .as_ref()
                .map_or(0, |predicate| predicate.field_dependencies().len())
                <= cursors.len()
        );
        Self {
            values: vec![None; cursors.len()],
            cursors,
            predicate,
        }
    }

    fn matches(&mut self, time: i64) -> bool {
        let Some(predicate) = &self.predicate else {
            return true;
        };

        for (value, cursor) in self.values.iter_mut().zip(&mut self.cursors) {
            *value = cursor
                .next_at(time)
                .filter(|value| !matches!(value, Val::Empty(_)));
        }

        predicate.matches(time, &self.values)
    }

    fn close(&mut self) {
        for cursor in &mut self.cursors {
            cursor.close();
        }
        self.cursors.clear();
        self.values.clear();
        self.predicate = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::points::{FloatValue, IntegerValue, UnsignedValue, Value};
    use crate::models::time::{MAX_NANO_TIME_I64, MIN_NANO_TIME_I64};
    use crate::query::predicate_eval::ColumnResolution;
    use crate::query::storage_ir::{
        ColumnRef, CompareOp, LogicalOp, MeasurementRef, OutputOrder, StorageExecutionOptions,
        StoragePredicate, StorageProjection, StorageValueExpr, TimeOrder, TimeRange,
    };
    use crate::storage::tsm1::data_encoding::encoding_values::Values;
    use crate::storage::tsm1::key_cursor::KeyCursor;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingCursor {
        calls: Arc<AtomicUsize>,
        value: Option<Val>,
    }

    impl CursorAt for CountingCursor {
        fn close(&mut self) {}

        fn peek(&mut self) -> Option<(i64, Val)> {
            None
        }

        fn next_at(&mut self, _seek: i64) -> Option<Val> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.value.clone()
        }
    }

    fn integer_cursor(values: &[(i64, i64)], ascending: bool) -> IntegerCursor {
        let values = Values(
            values
                .iter()
                .map(|(timestamp, value)| {
                    Value::IntegerValue(IntegerValue::new(*timestamp, *value))
                })
                .collect(),
        );
        let seek = if ascending {
            MIN_NANO_TIME_I64
        } else {
            MAX_NANO_TIME_I64
        };
        let key_cursor = KeyCursor::new(&[], b"key", seek, ascending);
        if ascending {
            IntegerCursor::IntegerAscendingCursor(IntegerAscendingCursor::new(
                false, seek, values, key_cursor, None,
            ))
        } else {
            IntegerCursor::IntegerDescendingCursor(IntegerDescendingCursor::new(
                false, seek, values, key_cursor, None,
            ))
        }
    }

    fn unsigned_cursor(values: &[(i64, u64)], ascending: bool) -> UnsignedCursor {
        let values = Values(
            values
                .iter()
                .map(|(timestamp, value)| {
                    Value::UnsignedValue(UnsignedValue::new(*timestamp, *value))
                })
                .collect(),
        );
        let seek = if ascending {
            MIN_NANO_TIME_I64
        } else {
            MAX_NANO_TIME_I64
        };
        let key_cursor = KeyCursor::new(&[], b"key", seek, ascending);
        if ascending {
            UnsignedCursor::UnsignedAscendingCursor(UnsignedAscendingCursor::new(
                false, seek, values, key_cursor, None,
            ))
        } else {
            UnsignedCursor::UnsignedDescendingCursor(UnsignedDescendingCursor::new(
                false, seek, values, key_cursor, None,
            ))
        }
    }

    fn float_cursor(values: &[(i64, f64)], ascending: bool) -> FloatCursor {
        let values = Values(
            values
                .iter()
                .map(|(timestamp, value)| Value::FloatValue(FloatValue::new(*timestamp, *value)))
                .collect(),
        );
        let seek = if ascending {
            MIN_NANO_TIME_I64
        } else {
            MAX_NANO_TIME_I64
        };
        let key_cursor = KeyCursor::new(&[], b"key", seek, ascending);
        if ascending {
            FloatCursor::FloatAscendingCursor(FloatAscendingCursor::new(
                false, seek, values, key_cursor, None,
            ))
        } else {
            FloatCursor::FloatDescendingCursor(FloatDescendingCursor::new(
                false, seek, values, key_cursor, None,
            ))
        }
    }

    #[test]
    fn prepared_native_condition_filters_fields_tags_and_time() {
        let compare = |column: ColumnRef, op, value| StoragePredicate::Compare {
            op,
            lhs: StorageValueExpr::Column(column),
            rhs: StorageValueExpr::Literal(value),
        };
        let predicate = StoragePredicate::Logical {
            op: LogicalOp::And,
            lhs: Box::new(StoragePredicate::Logical {
                op: LogicalOp::And,
                lhs: Box::new(compare(
                    ColumnRef::new("host", ColumnKind::Tag),
                    CompareOp::Equal,
                    Val::String("host_1".to_string()),
                )),
                rhs: Box::new(compare(
                    ColumnRef::new("usage", ColumnKind::AnyField),
                    CompareOp::GreaterThan,
                    Val::I64(10),
                )),
            }),
            rhs: Box::new(StoragePredicate::Logical {
                op: LogicalOp::And,
                lhs: Box::new(compare(
                    ColumnRef::new("aux_field", ColumnKind::AnyField),
                    CompareOp::LessThanOrEqual,
                    Val::U64(99),
                )),
                rhs: Box::new(compare(
                    ColumnRef::new("time", ColumnKind::Time),
                    CompareOp::Equal,
                    Val::I64(42),
                )),
            }),
        };
        let prepared =
            PreparedStoragePredicate::prepare(&predicate, |column| match column.name.as_str() {
                "host" => ColumnResolution::Static(Some(Val::String("host_1".to_string()))),
                "time" => ColumnResolution::Time,
                _ => ColumnResolution::Field(column.clone()),
            });

        assert_eq!(
            prepared
                .field_dependencies()
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["usage", "aux_field"]
        );
        assert!(prepared.matches(42, &[Some(Val::I64(11)), Some(Val::U64(99))]));
        assert!(!prepared.matches(42, &[Some(Val::I64(9)), Some(Val::U64(99))]));
    }

    #[test]
    fn native_condition_cursors_advance_before_short_circuit_evaluation() {
        for (left, op) in [(true, LogicalOp::Or), (false, LogicalOp::And)] {
            let predicate = StoragePredicate::Logical {
                op,
                lhs: Box::new(StoragePredicate::Constant(left)),
                rhs: Box::new(StoragePredicate::Compare {
                    op: CompareOp::GreaterThan,
                    lhs: StorageValueExpr::Column(ColumnRef::new("sparse", ColumnKind::AnyField)),
                    rhs: StorageValueExpr::Literal(Val::I64(10)),
                }),
            };
            let prepared = PreparedStoragePredicate::prepare(&predicate, |column| {
                ColumnResolution::Field(column.clone())
            });
            let calls = Arc::new(AtomicUsize::new(0));
            let cursor = CountingCursor {
                calls: Arc::clone(&calls),
                value: None,
            };
            let mut cursors = ConditionCursors::new(Some(prepared), vec![Box::new(cursor)]);

            for timestamp in [10, 20, 30] {
                assert_eq!(cursors.matches(timestamp), left);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        }
    }

    #[test]
    fn float_cast_integer_cursor_preserves_values_timestamps_and_eof() {
        let mut cursor = FloatCastIntegerCursor::new(integer_cursor(
            &[(10, -1), (20, 0), (30, 9_007_199_254_740_993)],
            true,
        ));
        assert_eq!(
            cursor.by_ref().collect::<Vec<_>>(),
            vec![
                (10, -1.0),
                (20, 0.0),
                (30, 9_007_199_254_740_993_i64 as f64),
            ]
        );
        assert_eq!(cursor.next_float(), (EOF, 0.0));
        assert!(cursor.next().is_none());
    }

    #[test]
    fn float_cast_unsigned_cursor_preserves_values_timestamps_and_eof() {
        let mut cursor = FloatCastUnsignedCursor::new(unsigned_cursor(
            &[(10, 0), (20, 1), (30, u64::MAX)],
            true,
        ));
        assert_eq!(
            cursor.by_ref().collect::<Vec<_>>(),
            vec![(10, 0.0), (20, 1.0), (30, u64::MAX as f64)]
        );
        assert_eq!(cursor.next_float(), (EOF, 0.0));
        assert!(cursor.next().is_none());
    }

    #[test]
    fn integer_cast_float_cursor_uses_saturating_rust_float_casts() {
        let mut cursor = IntegerCastFloatCursor::new(float_cursor(
            &[
                (10, -1.9),
                (20, 1.9),
                (30, f64::NAN),
                (40, f64::INFINITY),
                (50, f64::NEG_INFINITY),
            ],
            true,
        ));
        assert_eq!(
            cursor.by_ref().collect::<Vec<_>>(),
            vec![(10, -1), (20, 1), (30, 0), (40, i64::MAX), (50, i64::MIN),]
        );
        assert_eq!(cursor.next_integer(), (EOF, 0));
        assert!(cursor.next().is_none());
    }

    #[test]
    fn integer_cast_unsigned_cursor_uses_twos_complement_narrowing() {
        let mut cursor = IntegerCastUnsignedCursor::new(unsigned_cursor(
            &[(10, 0), (20, i64::MAX as u64), (30, u64::MAX)],
            true,
        ));
        assert_eq!(
            cursor.by_ref().collect::<Vec<_>>(),
            vec![(10, 0), (20, i64::MAX), (30, -1)]
        );
        assert_eq!(cursor.next_integer(), (EOF, 0));
        assert!(cursor.next().is_none());
    }

    #[test]
    fn unsigned_cast_float_cursor_uses_saturating_rust_float_casts() {
        let mut cursor = UnsignedCastFloatCursor::new(float_cursor(
            &[
                (10, -1.9),
                (20, 1.9),
                (30, f64::NAN),
                (40, f64::INFINITY),
                (50, f64::NEG_INFINITY),
            ],
            true,
        ));
        assert_eq!(
            cursor.by_ref().collect::<Vec<_>>(),
            vec![(10, 0), (20, 1), (30, 0), (40, u64::MAX), (50, 0)]
        );
        assert_eq!(cursor.next_unsigned(), (EOF, 0));
        assert!(cursor.next().is_none());
    }

    #[test]
    fn unsigned_cast_integer_cursor_uses_twos_complement_widening() {
        let mut cursor = UnsignedCastIntegerCursor::new(integer_cursor(
            &[(10, i64::MIN), (20, -1), (30, 0), (40, 1)],
            true,
        ));
        assert_eq!(
            cursor.by_ref().collect::<Vec<_>>(),
            vec![(10, i64::MIN as u64), (20, u64::MAX), (30, 0), (40, 1),]
        );
        assert_eq!(cursor.next_unsigned(), (EOF, 0));
        assert!(cursor.next().is_none());
    }

    #[test]
    fn numeric_cast_cursors_preserve_descending_order() {
        let integer = FloatCastIntegerCursor::new(integer_cursor(&[(10, 1), (20, 2)], false));
        assert_eq!(integer.collect::<Vec<_>>(), vec![(20, 2.0), (10, 1.0)]);

        let unsigned = FloatCastUnsignedCursor::new(unsigned_cursor(&[(10, 1), (20, 2)], false));
        assert_eq!(unsigned.collect::<Vec<_>>(), vec![(20, 2.0), (10, 1.0)]);

        let float = IntegerCastFloatCursor::new(float_cursor(&[(10, 1.5), (20, 2.5)], false));
        assert_eq!(float.collect::<Vec<_>>(), vec![(20, 2), (10, 1)]);

        let unsigned = IntegerCastUnsignedCursor::new(unsigned_cursor(&[(10, 1), (20, 2)], false));
        assert_eq!(unsigned.collect::<Vec<_>>(), vec![(20, 2), (10, 1)]);

        let float = UnsignedCastFloatCursor::new(float_cursor(&[(10, 1.5), (20, 2.5)], false));
        assert_eq!(float.collect::<Vec<_>>(), vec![(20, 2), (10, 1)]);

        let integer = UnsignedCastIntegerCursor::new(integer_cursor(&[(10, 1), (20, 2)], false));
        assert_eq!(integer.collect::<Vec<_>>(), vec![(20, 2), (10, 1)]);
    }

    #[test]
    fn row_domain_cursors_preserve_sparse_timestamps_without_expanding_point_aux() {
        let scan = Arc::new(StorageScan {
            measurement: MeasurementRef::new("m"),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: vec![ColumnRef::new("aaa", ColumnKind::AnyField)],
                row_domain_columns: vec![ColumnRef::new("bbb", ColumnKind::AnyField)],
                ..Default::default()
            },
            time_range: TimeRange::all(),
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::Unspecified,
            execution: StorageExecutionOptions::default(),
            cancellation: None,
        });
        let cursors: Vec<Box<dyn CursorAt + Send + Sync>> = vec![
            Box::new(BufCursor::new(
                Some(Cursor::IntegerCursor(integer_cursor(
                    &[(10, 1), (30, 3)],
                    true,
                ))),
                true,
            )),
            Box::new(BufCursor::new(
                Some(Cursor::IntegerCursor(integer_cursor(&[(20, 2)], true))),
                true,
            )),
        ];
        let mut iter = FloatIter::new_with_storage_predicate(
            "m".to_string(),
            query_point::Tags::default(),
            scan,
            None,
            cursors,
            Vec::new(),
            None,
        );

        let points = std::iter::from_fn(|| iter.next()).collect::<Vec<_>>();
        assert_eq!(
            points.iter().map(|point| point.time()).collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        assert_eq!(
            points
                .iter()
                .map(|point| point.aux.clone())
                .collect::<Vec<_>>(),
            vec![vec![Val::I64(1)], vec![Val::Empty(None)], vec![Val::I64(3)]]
        );
    }

    #[test]
    fn row_domain_only_iterates_union_in_both_directions_with_empty_point_aux() {
        for ascending in [true, false] {
            let scan = Arc::new(StorageScan {
                measurement: MeasurementRef::new("m"),
                projection: StorageProjection {
                    primary_column: None,
                    returned_columns: Vec::new(),
                    row_domain_columns: vec![
                        ColumnRef::new("aaa", ColumnKind::AnyField),
                        ColumnRef::new("bbb", ColumnKind::AnyField),
                    ],
                    ..Default::default()
                },
                time_range: TimeRange::all(),
                predicate: None,
                time_order: if ascending {
                    TimeOrder::Ascending
                } else {
                    TimeOrder::Descending
                },
                output_order: OutputOrder::Unspecified,
                execution: StorageExecutionOptions::default(),
                cancellation: None,
            });
            let cursors: Vec<Box<dyn CursorAt + Send + Sync>> = vec![
                Box::new(BufCursor::new(
                    Some(Cursor::IntegerCursor(integer_cursor(
                        &[(10, 1), (30, 3)],
                        ascending,
                    ))),
                    ascending,
                )),
                Box::new(BufCursor::new(
                    Some(Cursor::IntegerCursor(integer_cursor(&[(20, 2)], ascending))),
                    ascending,
                )),
            ];
            let mut iter = FloatIter::new_with_storage_predicate(
                "m".to_string(),
                query_point::Tags::default(),
                scan,
                None,
                cursors,
                Vec::new(),
                None,
            );

            let points = std::iter::from_fn(|| iter.next()).collect::<Vec<_>>();
            let expected = if ascending {
                vec![10, 20, 30]
            } else {
                vec![30, 20, 10]
            };
            assert_eq!(
                points.iter().map(|point| point.time()).collect::<Vec<_>>(),
                expected
            );
            assert!(points.iter().all(|point| point.aux.is_empty()));
        }
    }
}

// ////////////////////////////////////////////
// //////////// iterator.gen //////////////
// ////////////////////////////////////////////

pub enum Cursor {
    IntegerCursor(IntegerCursor),
    UnsignedCursor(UnsignedCursor),
    FloatCursor(FloatCursor),
    BoolCursor(BoolCursor),
    StringCursor(StringCursor),
}

unsafe impl Sync for Cursor {}
unsafe impl Send for Cursor {}

impl Cursor {
    fn close(&mut self) {
        match self {
            Cursor::IntegerCursor(c) => c.close(),
            Cursor::UnsignedCursor(c) => c.close(),
            Cursor::FloatCursor(c) => c.close(),
            Cursor::BoolCursor(c) => c.close(),
            Cursor::StringCursor(c) => c.close(),
        }
    }
}

impl Iterator for Cursor {
    type Item = (i64, Val);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Cursor::IntegerCursor(c) => c.next().map(|(t, v)| (t, Val::I64(v))),
            Cursor::UnsignedCursor(c) => c.next().map(|(t, v)| (t, Val::U64(v))),
            Cursor::FloatCursor(c) => c.next().map(|(t, v)| (t, Val::F64(v))),
            Cursor::BoolCursor(c) => c.next().map(|(t, v)| (t, Val::Bool(v))),
            Cursor::StringCursor(c) => c.next().map(|(t, v)| (t, Val::String(v))),
        }
    }
}

// CursorAt provides a bufferred cursor interface.
// This required for literal value cursors which don't have a time value.
pub trait CursorAt {
    fn close(&mut self);
    fn peek(&mut self) -> Option<(i64, Val)>;
    fn next_at(&mut self, seek: i64) -> Option<Val>;
}

fn advance_row_domain_cursors(
    cursors: &mut [Box<dyn CursorAt + Sync + Send>],
    materialized_len: usize,
    seek: i64,
) {
    for cursor in cursors.iter_mut().skip(materialized_len) {
        let _ = cursor.next_at(seek);
    }
}

// BufCursor implements a bufferred cursor.
pub struct BufCursor {
    cur: Option<Cursor>,
    buf: Buf,
    ascending: bool,
}

#[derive(Default)]
struct Buf {
    key: i64,
    value: Val,
    filled: bool,
}

impl BufCursor {
    pub fn new(cur: Option<Cursor>, ascending: bool) -> Self {
        Self {
            cur,
            buf: Buf::default(),
            ascending,
        }
    }

    /// Pushes k and v onto the buffer.
    pub fn unread(&mut self, k: i64, v: Val) {
        (self.buf.key, self.buf.value) = (k, v);
        self.buf.filled = true;
    }
}

impl Iterator for BufCursor {
    type Item = (i64, Val);

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.filled {
            let (k, v) = (self.buf.key, self.buf.value.clone());
            self.buf.filled = false;
            return Some((k, v));
        }
        self.cur.as_mut().unwrap().next()
    }
}

impl CursorAt for BufCursor {
    fn close(&mut self) {
        if let Some(cur) = self.cur.as_mut() {
            cur.close();
        }
        self.cur = None;
    }

    /// Reads next next key/value without removing them from the cursor.
    fn peek(&mut self) -> Option<(i64, Val)> {
        match self.next() {
            Some((k, v)) => {
                self.unread(k, v.clone());
                Some((k, v))
            }
            None => None,
        }
    }

    /// Returns the next value where key is equal to seek.
    /// Skips over any keys that are less than seek.
    /// If the key doesn't exist then a None value is returned instead.
    fn next_at(&mut self, seek: i64) -> Option<Val> {
        loop {
            let (k, v) = self.next()?;

            if k != EOF {
                if k == seek {
                    return Some(v);
                } else if self.ascending && k < seek {
                    continue;
                } else if !self.ascending && k > seek {
                    continue;
                }
                self.unread(k, v);
            }

            // Return `None` value for type.
            match self.cur.as_ref().unwrap() {
                Cursor::IntegerCursor(_)
                | Cursor::UnsignedCursor(_)
                | Cursor::FloatCursor(_)
                | Cursor::BoolCursor(_)
                | Cursor::StringCursor(_) => return None,
                _ => panic!("unreachable"),
            }
        }
    }
}

#[derive(Default)]
struct CursorCache {
    values: Values,
    pos: i64,
}

pub struct IntegerIter {
    cur: Option<IntegerCursor>,
    aux: Vec<Box<dyn CursorAt + Sync + Send>>,
    conds: ConditionCursors,
    scan: Arc<StorageScan>,

    point: query_point::IntegerPoint, // reusable buffer

    stats: Mutex<query_iterator::IteratorStats>,
    stats_buf: query_iterator::IteratorStats,
}

impl IntegerIter {
    pub(crate) fn new_with_storage_predicate(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<IntegerCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        Self::new_inner(name, tags, scan, cur, aux, conds, predicate)
    }

    fn new_inner(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<IntegerCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        let aux_len = scan.returned_aux_count();
        let stats = query_iterator::IteratorStats {
            series_n: 1,
            point_n: 0,
        };
        let mut itr = Self {
            cur,
            aux,
            conds: ConditionCursors::new(predicate, conds),
            scan,

            point: query_point::IntegerPoint {
                name,
                tags,
                ..Default::default()
            },
            stats: Mutex::new(stats.clone()),
            stats_buf: stats,
        };

        if aux_len > 0 {
            itr.point.aux = vec![Val::default(); aux_len];
        }
        itr
    }

    /// Copie from the itr stats buffer to the stats under lock.
    fn copy_stats(&self) {
        let mut stats = self.stats.lock();
        *stats = self.stats_buf.clone();
    }
}

impl query_iterator::QueryIterator for IntegerIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Integer
    }

    /// Returns stats on the points processed.
    fn stats(&self) -> query_iterator::IteratorStats {
        self.stats.lock().clone()
    }

    fn close(&mut self) {
        for c in &mut self.aux {
            c.close();
        }
        self.aux = vec![];
        self.conds.close();

        if let Some(c) = &mut self.cur {
            c.close();
        }
        self.cur = None;
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

impl query_iterator::IntegerIterator for IntegerIter {
    fn next(&mut self) -> Option<query_point::IntegerPoint> {
        loop {
            let mut seek = EOF;

            if let Some(cur) = &mut self.cur {
                // Read from the main cursor if we have one.
                (self.point.time, self.point.value) = cur.next_integer();
                seek = self.point.time;
            } else {
                // Otherwise find lowest aux timestamp.
                for c in self.aux.iter_mut() {
                    if let Some((k, _)) = c.peek() {
                        if seek == EOF
                            || (self.scan.ascending() && k < seek)
                            || (!self.scan.ascending() && k > seek)
                        {
                            seek = k;
                        }
                    }
                }
                self.point.time = seek;
            }

            // Exit if we have no more points or we are outside our time range.
            if self.point.time == EOF
                || (self.scan.ascending() && self.point.time > self.scan.time_range.end_time)
                || (!self.scan.ascending() && self.point.time < self.scan.time_range.start_time)
            {
                self.copy_stats();
                return None;
            }

            // Read from each auxiliary cursor.
            for i in 0..self.scan.returned_aux_count() {
                self.point.aux[i] = match self.aux[i].next_at(seek) {
                    Some(val) => val,
                    None => Val::Empty(None),
                }
            }
            advance_row_domain_cursors(&mut self.aux, self.scan.returned_aux_count(), seek);

            if !self.conds.matches(self.point.time) {
                continue;
            }

            // Track points returned.
            self.stats_buf.point_n += 1;

            // Copy buffer to stats periodically.
            if self.stats_buf.point_n % STATS_BUFFER_COPY_INTERVAL_N == 0 {
                self.copy_stats();
            }

            return Some(self.point.clone());
        }
    }
}

unsafe impl Sync for IntegerIter {}
unsafe impl Send for IntegerIter {}

#[inline]
fn integer_to_float(value: i64) -> f64 {
    value as f64
}

#[inline]
fn unsigned_to_float(value: u64) -> f64 {
    value as f64
}

#[inline]
fn float_to_integer(value: f64) -> i64 {
    value as i64
}

#[inline]
fn unsigned_to_integer(value: u64) -> i64 {
    value as i64
}

#[inline]
fn float_to_unsigned(value: f64) -> u64 {
    value as u64
}

#[inline]
fn integer_to_unsigned(value: i64) -> u64 {
    value as u64
}

macro_rules! define_numeric_cast_cursor {
    ($name:ident, $source:ty, $source_next:ident, $target_next:ident, $target:ty, $cast:ident) => {
        pub struct $name {
            cursor: Box<$source>,
        }

        impl $name {
            #[inline]
            pub fn new(cursor: $source) -> Self {
                Self {
                    cursor: Box::new(cursor),
                }
            }

            #[inline]
            fn close(&mut self) {
                self.cursor.close();
            }

            #[inline]
            pub fn $target_next(&mut self) -> (i64, $target) {
                let (timestamp, value) = self.cursor.$source_next();
                (timestamp, $cast(value))
            }
        }

        impl Iterator for $name {
            type Item = (i64, $target);

            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                self.cursor
                    .next()
                    .map(|(timestamp, value)| (timestamp, $cast(value)))
            }
        }
    };
}

define_numeric_cast_cursor!(
    FloatCastIntegerCursor,
    IntegerCursor,
    next_integer,
    next_float,
    f64,
    integer_to_float
);
define_numeric_cast_cursor!(
    FloatCastUnsignedCursor,
    UnsignedCursor,
    next_unsigned,
    next_float,
    f64,
    unsigned_to_float
);
define_numeric_cast_cursor!(
    IntegerCastFloatCursor,
    FloatCursor,
    next_float,
    next_integer,
    i64,
    float_to_integer
);
define_numeric_cast_cursor!(
    IntegerCastUnsignedCursor,
    UnsignedCursor,
    next_unsigned,
    next_integer,
    i64,
    unsigned_to_integer
);
define_numeric_cast_cursor!(
    UnsignedCastFloatCursor,
    FloatCursor,
    next_float,
    next_unsigned,
    u64,
    float_to_unsigned
);
define_numeric_cast_cursor!(
    UnsignedCastIntegerCursor,
    IntegerCursor,
    next_integer,
    next_unsigned,
    u64,
    integer_to_unsigned
);

pub enum IntegerCursor {
    IntegerAscendingCursor(IntegerAscendingCursor),
    IntegerDescendingCursor(IntegerDescendingCursor),
    IntegerCastFloatCursor(IntegerCastFloatCursor),
    IntegerCastUnsignedCursor(IntegerCastUnsignedCursor),
}

impl IntegerCursor {
    fn close(&mut self) {
        match self {
            IntegerCursor::IntegerAscendingCursor(c) => c.close(),
            IntegerCursor::IntegerDescendingCursor(c) => c.close(),
            IntegerCursor::IntegerCastFloatCursor(c) => c.close(),
            IntegerCursor::IntegerCastUnsignedCursor(c) => c.close(),
        }
    }

    pub fn next_integer(&mut self) -> (i64, i64) {
        match self {
            IntegerCursor::IntegerAscendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
            IntegerCursor::IntegerDescendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
            IntegerCursor::IntegerCastFloatCursor(c) => c.next_integer(),
            IntegerCursor::IntegerCastUnsignedCursor(c) => c.next_integer(),
        }
    }
}

impl Iterator for IntegerCursor {
    type Item = (i64, i64);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IntegerCursor::IntegerAscendingCursor(c) => c.next(),
            IntegerCursor::IntegerDescendingCursor(c) => c.next(),
            IntegerCursor::IntegerCastFloatCursor(c) => c.next(),
            IntegerCursor::IntegerCastUnsignedCursor(c) => c.next(),
        }
    }
}

struct IntegerTSM {
    values: Vec<IntegerValue>,
    pos: i64,
    key_cursor: Option<KeyCursor>,
}

struct IntegerVLog {
    values: Vec<IntegerValue>,
    pos: i64,
    key_cursor: Option<VLogKeyCursor>,
}

pub struct IntegerAscendingCursor {
    cache: CursorCache,
    tsm: IntegerTSM,
    vlog_enabled: bool,
    vlog: IntegerVLog,
}

impl IntegerAscendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = IntegerTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = IntegerVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = IntegerAscendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };
        c.cache.values = cache_values;
        c.cache.pos = match c
            .cache
            .values
            .binary_search_by(|v| {
                if v.unix_nano() >= seek {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .or_else(|i| {
                // i is the insertion point
                if i < c.cache.values.len() {
                    Ok(i) // Return the index where seek could be inserted
                } else {
                    Err(i) // Handle case where seek is greater than all elements
                }
            }) {
            Ok(pos) => pos as i64,
            Err(_) => c.cache.values.len() as i64,
        };

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_integer_block()
            .expect("IntegerAscendingCursor read integer block failed");
        c.tsm.pos = match c
            .tsm
            .values
            .binary_search_by(|v| {
                if v.unix_nano >= seek {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .or_else(|i| {
                // i is the insertion point
                if i < c.tsm.values.len() {
                    Ok(i) // Return the index where seek could be inserted
                } else {
                    Err(i) // Handle case where seek is greater than all elements
                }
            }) {
            Ok(pos) => pos as i64,
            Err(_) => c.tsm.values.len() as i64,
        };

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_integer_block()
                .expect("IntegerAscendingCursor read vlog integer block failed");

            c.vlog.pos = match c
                .vlog
                .values
                .binary_search_by(|v| {
                    if v.unix_nano >= seek {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .or_else(|i| {
                    // i is the insertion point
                    if i < c.vlog.values.len() {
                        Ok(i) // Return the index where seek could be inserted
                    } else {
                        Err(i) // Handle case where seek is greater than all elements
                    }
                }) {
                Ok(pos) => pos as i64,
                Err(_) => c.vlog.values.len() as i64,
            };
        }

        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, i64) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.integer_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, i64) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Returns the current time/value from vlog.
    fn peek_vlog(&self) -> (i64, i64) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return;
        }

        self.cache.pos += 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos += 1;
        if self.tsm.pos >= self.tsm.values.len() as i64 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_integer_block()
                .expect("IntegerAscendingCursor read integer block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = 0;
        }
    }

    /// Returns the next value from the VLog files.
    fn next_vlog(&mut self) {
        self.vlog.pos += 1;
        if self.vlog.pos >= self.vlog.values.len() as i64 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_integer_block()
                .expect("IntegerAscendingCursor read vlog integer block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = 0;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, i64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, 0);
        }

        // Both cache and tsm files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey < tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered VLog key precedes that in Cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    fn next_value_with_vlog(&mut self) -> (i64, i64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, 0);
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).min().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for IntegerAscendingCursor {
    type Item = (i64, i64);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct IntegerDescendingCursor {
    cache: CursorCache,
    tsm: IntegerTSM,
    vlog_enabled: bool,
    vlog: IntegerVLog,
}

impl IntegerDescendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = IntegerTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = IntegerVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = IntegerDescendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };
        c.cache.values = cache_values;
        c.cache.pos = match c.cache.values.binary_search_by(|v| {
            if v.unix_nano() >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.cache.values.len() as i64,
        };

        let (t, _) = c.peek_cache();
        if t != seek {
            c.cache.pos -= 1;
        }

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_integer_block()
            .expect("IntegerDescendingCursor read integer block failed");
        c.tsm.pos = match c.tsm.values.binary_search_by(|v| {
            if v.unix_nano >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.tsm.values.len() as i64,
        };

        let (t, _) = c.peek_tsm();
        if t != seek {
            c.tsm.pos -= 1;
        }

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_integer_block()
                .expect("IntegerAscendingCursor read vlog integer block failed");

            c.vlog.pos = match c
                .vlog
                .values
                .binary_search_by(|v| {
                    if v.unix_nano >= seek {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .or_else(|i| {
                    // i is the insertion point
                    if i < c.vlog.values.len() {
                        Ok(i) // Return the index where seek could be inserted
                    } else {
                        Err(i) // Handle case where seek is greater than all elements
                    }
                }) {
                Ok(pos) => pos as i64,
                Err(_) => c.vlog.values.len() as i64,
            };

            let (t, _) = c.peek_vlog();
            if t != seek {
                c.vlog.pos -= 1;
            }
        }

        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, i64) {
        if self.cache.pos < 0 || self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.integer_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, i64) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Returns the current time/value from tsm.
    fn peek_vlog(&self) -> (i64, i64) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos < 0 {
            return;
        }

        self.cache.pos -= 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos -= 1;
        if self.tsm.pos < 0 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_integer_block()
                .expect("IntegerDescendingCursor read integer block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = self.tsm.values.len() as i64 - 1;
        }
    }

    /// Returns the next value from the VLog files.
    fn next_vlog(&mut self) {
        self.vlog.pos -= 1;
        if self.vlog.pos < 0 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_integer_block()
                .expect("IntegerDescendingCursor read vlog integer block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = self.vlog.values.len() as i64 - 1;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, i64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, 0);
        }

        // Both cache and tsm files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey > tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered TSM key precedes that in Cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    fn next_value_with_vlog(&mut self) -> (i64, i64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, 0);
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).max().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for IntegerDescendingCursor {
    type Item = (i64, i64);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct UnsignedIter {
    cur: Option<UnsignedCursor>,
    aux: Vec<Box<dyn CursorAt + Sync + Send>>,
    conds: ConditionCursors,
    scan: Arc<StorageScan>,

    point: query_point::UnsignedPoint, // reusable buffer

    stats: Mutex<query_iterator::IteratorStats>,
    stats_buf: query_iterator::IteratorStats,
}

impl UnsignedIter {
    pub(crate) fn new_with_storage_predicate(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<UnsignedCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        Self::new_inner(name, tags, scan, cur, aux, conds, predicate)
    }

    fn new_inner(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<UnsignedCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        let aux_len = scan.returned_aux_count();
        let stats = query_iterator::IteratorStats {
            series_n: 1,
            point_n: 0,
        };
        let mut itr = Self {
            cur,
            aux,
            conds: ConditionCursors::new(predicate, conds),
            scan,

            point: query_point::UnsignedPoint {
                name,
                tags,
                ..Default::default()
            },
            stats: Mutex::new(stats.clone()),
            stats_buf: stats,
        };

        if aux_len > 0 {
            itr.point.aux = vec![Val::default(); aux_len];
        }

        itr
    }

    /// Copie from the itr stats buffer to the stats under lock.
    fn copy_stats(&self) {
        let mut stats = self.stats.lock();
        *stats = self.stats_buf.clone();
    }
}

impl query_iterator::QueryIterator for UnsignedIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Unsigned
    }

    /// Returns stats on the points processed.
    fn stats(&self) -> query_iterator::IteratorStats {
        self.stats.lock().clone()
    }

    fn close(&mut self) {
        for c in &mut self.aux {
            c.close();
        }
        self.aux = vec![];
        self.conds.close();

        if let Some(c) = &mut self.cur {
            c.close();
        }
        self.cur = None;
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

impl query_iterator::UnsignedIterator for UnsignedIter {
    fn next(&mut self) -> Option<query_point::UnsignedPoint> {
        loop {
            let mut seek = EOF;

            if let Some(cur) = &mut self.cur {
                // Read from the main cursor if we have one.
                (self.point.time, self.point.value) = cur.next_unsigned();
                seek = self.point.time;
            } else {
                // Otherwise find lowest aux timestamp.
                for c in self.aux.iter_mut() {
                    if let Some((k, _)) = c.peek() {
                        if seek == EOF
                            || (self.scan.ascending() && k < seek)
                            || (!self.scan.ascending() && k > seek)
                        {
                            seek = k;
                        }
                    }
                }
                self.point.time = seek;
            }

            // Exit if we have no more points or we are outside our time range.
            if self.point.time == EOF
                || (self.scan.ascending() && self.point.time > self.scan.time_range.end_time)
                || (!self.scan.ascending() && self.point.time < self.scan.time_range.start_time)
            {
                self.copy_stats();
                return None;
            }

            // Read from each auxiliary cursor.
            for i in 0..self.scan.returned_aux_count() {
                self.point.aux[i] = match self.aux[i].next_at(seek) {
                    Some(val) => val,
                    None => Val::Empty(None),
                }
            }
            advance_row_domain_cursors(&mut self.aux, self.scan.returned_aux_count(), seek);

            if !self.conds.matches(self.point.time) {
                continue;
            }

            // Track points returned.
            self.stats_buf.point_n += 1;

            // Copy buffer to stats periodically.
            if self.stats_buf.point_n % STATS_BUFFER_COPY_INTERVAL_N == 0 {
                self.copy_stats();
            }

            return Some(self.point.clone());
        }
    }
}

unsafe impl Sync for UnsignedIter {}
unsafe impl Send for UnsignedIter {}

pub enum UnsignedCursor {
    UnsignedAscendingCursor(UnsignedAscendingCursor),
    UnsignedDescendingCursor(UnsignedDescendingCursor),
    UnsignedCastFloatCursor(UnsignedCastFloatCursor),
    UnsignedCastIntegerCursor(UnsignedCastIntegerCursor),
}

impl UnsignedCursor {
    fn close(&mut self) {
        match self {
            UnsignedCursor::UnsignedAscendingCursor(c) => c.close(),
            UnsignedCursor::UnsignedDescendingCursor(c) => c.close(),
            UnsignedCursor::UnsignedCastFloatCursor(c) => c.close(),
            UnsignedCursor::UnsignedCastIntegerCursor(c) => c.close(),
        }
    }

    pub fn next_unsigned(&mut self) -> (i64, u64) {
        match self {
            UnsignedCursor::UnsignedAscendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
            UnsignedCursor::UnsignedDescendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
            UnsignedCursor::UnsignedCastFloatCursor(c) => c.next_unsigned(),
            UnsignedCursor::UnsignedCastIntegerCursor(c) => c.next_unsigned(),
        }
    }
}

impl Iterator for UnsignedCursor {
    type Item = (i64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            UnsignedCursor::UnsignedAscendingCursor(c) => c.next(),
            UnsignedCursor::UnsignedDescendingCursor(c) => c.next(),
            UnsignedCursor::UnsignedCastFloatCursor(c) => c.next(),
            UnsignedCursor::UnsignedCastIntegerCursor(c) => c.next(),
        }
    }
}

struct UnsignedTSM {
    values: Vec<UnsignedValue>,
    pos: i64,
    key_cursor: Option<KeyCursor>,
}

struct UnsignedVLog {
    values: Vec<UnsignedValue>,
    pos: i64,
    key_cursor: Option<VLogKeyCursor>,
}

pub struct UnsignedAscendingCursor {
    cache: CursorCache,
    tsm: UnsignedTSM,
    vlog_enabled: bool,
    vlog: UnsignedVLog,
}

impl UnsignedAscendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = UnsignedTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = UnsignedVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = UnsignedAscendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };
        c.cache.values = cache_values;
        c.cache.pos = match c
            .cache
            .values
            .binary_search_by(|v| {
                if v.unix_nano() >= seek {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .or_else(|i| {
                // i is the insertion point
                if i < c.cache.values.len() {
                    Ok(i) // Return the index where seek could be inserted
                } else {
                    Err(i) // Handle case where seek is greater than all elements
                }
            }) {
            Ok(pos) => pos as i64,
            Err(_) => c.cache.values.len() as i64,
        };

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_unsigned_block()
            .expect("UnsignedAscendingCursor read unsigned block failed");
        c.tsm.pos = match c
            .tsm
            .values
            .binary_search_by(|v| {
                if v.unix_nano >= seek {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .or_else(|i| {
                // i is the insertion point
                if i < c.tsm.values.len() {
                    Ok(i) // Return the index where seek could be inserted
                } else {
                    Err(i) // Handle case where seek is greater than all elements
                }
            }) {
            Ok(pos) => pos as i64,
            Err(_) => c.tsm.values.len() as i64,
        };

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_unsigned_block()
                .expect("UnsignedAscendingCursor read vlog unsigned block failed");

            c.vlog.pos = match c
                .vlog
                .values
                .binary_search_by(|v| {
                    if v.unix_nano >= seek {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .or_else(|i| {
                    // i is the insertion point
                    if i < c.vlog.values.len() {
                        Ok(i) // Return the index where seek could be inserted
                    } else {
                        Err(i) // Handle case where seek is greater than all elements
                    }
                }) {
                Ok(pos) => pos as i64,
                Err(_) => c.vlog.values.len() as i64,
            };
        }

        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, u64) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.unsigned_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, u64) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Returns the current time/value from vlog.
    fn peek_vlog(&self) -> (i64, u64) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return;
        }

        self.cache.pos += 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos += 1;
        if self.tsm.pos >= self.tsm.values.len() as i64 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_unsigned_block()
                .expect("UnsignedAscendingCursor read unsigned block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = 0;
        }
    }

    /// Returns the next value from the VLog files.
    fn next_vlog(&mut self) {
        self.vlog.pos += 1;
        if self.vlog.pos >= self.vlog.values.len() as i64 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_unsigned_block()
                .expect("UnsignedAscendingCursor read vlog unsigned block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = 0;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, u64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, 0);
        }

        // Both cache and tsm files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey < tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered TSM key precedes that in Cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    fn next_value_with_vlog(&mut self) -> (i64, u64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, 0);
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).min().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for UnsignedAscendingCursor {
    type Item = (i64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct UnsignedDescendingCursor {
    cache: CursorCache,
    tsm: UnsignedTSM,
    vlog_enabled: bool,
    vlog: UnsignedVLog,
}

impl UnsignedDescendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = UnsignedTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = UnsignedVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = UnsignedDescendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };
        c.cache.values = cache_values;
        c.cache.pos = match c.cache.values.binary_search_by(|v| {
            if v.unix_nano() >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.cache.values.len() as i64,
        };

        let (t, _) = c.peek_cache();
        if t != seek {
            c.cache.pos -= 1;
        }

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_unsigned_block()
            .expect("UnsignedDescendingCursor read unsigned block failed");
        c.tsm.pos = match c.tsm.values.binary_search_by(|v| {
            if v.unix_nano >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.tsm.values.len() as i64,
        };

        let (t, _) = c.peek_tsm();
        if t != seek {
            c.tsm.pos -= 1;
        }

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_unsigned_block()
                .expect("UnsignedAscendingCursor read vlog unsigned block failed");

            c.vlog.pos = match c
                .vlog
                .values
                .binary_search_by(|v| {
                    if v.unix_nano >= seek {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .or_else(|i| {
                    // i is the insertion point
                    if i < c.vlog.values.len() {
                        Ok(i) // Return the index where seek could be inserted
                    } else {
                        Err(i) // Handle case where seek is greater than all elements
                    }
                }) {
                Ok(pos) => pos as i64,
                Err(_) => c.vlog.values.len() as i64,
            };

            let (t, _) = c.peek_vlog();
            if t != seek {
                c.vlog.pos -= 1;
            }
        }
        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, u64) {
        if self.cache.pos < 0 || self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.unsigned_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, u64) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Returns the current time/value from vlog.
    fn peek_vlog(&self) -> (i64, u64) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, 0);
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos < 0 {
            return;
        }

        self.cache.pos -= 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos -= 1;
        if self.tsm.pos < 0 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_unsigned_block()
                .expect("UnsignedDescendingCursor read unsigned block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = self.tsm.values.len() as i64 - 1;
        }
    }

    /// Returns the next value from the VLog files.
    fn next_vlog(&mut self) {
        self.vlog.pos -= 1;
        if self.vlog.pos < 0 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_unsigned_block()
                .expect("UnsignedDescendingCursor read unsigned vlog block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = self.vlog.values.len() as i64 - 1;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, u64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, 0);
        }

        // Both cache and tsm files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey > tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered TSM key precedes that in Cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    fn next_value_with_vlog(&mut self) -> (i64, u64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, 0);
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).max().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for UnsignedDescendingCursor {
    type Item = (i64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct FloatIter {
    cur: Option<FloatCursor>,
    aux: Vec<Box<dyn CursorAt + Sync + Send>>,
    conds: ConditionCursors,
    scan: Arc<StorageScan>,

    point: query_point::FloatPoint, // reusable buffer

    stats: Mutex<query_iterator::IteratorStats>,
    stats_buf: query_iterator::IteratorStats,
}

impl FloatIter {
    pub(crate) fn new_with_storage_predicate(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<FloatCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        Self::new_inner(name, tags, scan, cur, aux, conds, predicate)
    }

    fn new_inner(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<FloatCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        let aux_len = scan.returned_aux_count();
        let stats = query_iterator::IteratorStats {
            series_n: 1,
            point_n: 0,
        };
        let mut itr = Self {
            cur,
            aux,
            conds: ConditionCursors::new(predicate, conds),
            scan,

            point: query_point::FloatPoint {
                name,
                tags,
                ..Default::default()
            },
            stats: Mutex::new(stats.clone()),
            stats_buf: stats,
        };

        if aux_len > 0 {
            itr.point.aux = vec![Val::default(); aux_len];
        }

        itr
    }

    /// Copie from the itr stats buffer to the stats under lock.
    fn copy_stats(&self) {
        let mut stats = self.stats.lock();
        *stats = self.stats_buf.clone();
    }
}

impl query_iterator::QueryIterator for FloatIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Float
    }

    /// Returns stats on the points processed.
    fn stats(&self) -> query_iterator::IteratorStats {
        self.stats.lock().clone()
    }

    fn close(&mut self) {
        for c in &mut self.aux {
            c.close();
        }
        self.aux = vec![];
        self.conds.close();

        if let Some(c) = &mut self.cur {
            c.close();
        }
        self.cur = None;
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

impl query_iterator::FloatIterator for FloatIter {
    fn next(&mut self) -> Option<query_point::FloatPoint> {
        loop {
            let mut seek = EOF;

            if let Some(cur) = &mut self.cur {
                // Read from the main cursor if we have one.
                (self.point.time, self.point.value) = cur.next_float();
                seek = self.point.time;
            } else {
                // Otherwise find lowest aux timestamp.
                for c in self.aux.iter_mut() {
                    if let Some((k, _)) = c.peek() {
                        if seek == EOF
                            || (self.scan.ascending() && k < seek)
                            || (!self.scan.ascending() && k > seek)
                        {
                            seek = k;
                        }
                    }
                }
                self.point.time = seek;
            }

            // Exit if we have no more points or we are outside our time range.
            if self.point.time == EOF
                || (self.scan.ascending() && self.point.time > self.scan.time_range.end_time)
                || (!self.scan.ascending() && self.point.time < self.scan.time_range.start_time)
            {
                self.copy_stats();
                return None;
            }

            // Read from each auxiliary cursor.
            for i in 0..self.scan.returned_aux_count() {
                self.point.aux[i] = match self.aux[i].next_at(seek) {
                    Some(val) => val,
                    None => Val::Empty(None),
                }
            }
            advance_row_domain_cursors(&mut self.aux, self.scan.returned_aux_count(), seek);

            if !self.conds.matches(self.point.time) {
                continue;
            }

            // Track points returned.
            self.stats_buf.point_n += 1;

            // Copy buffer to stats periodically.
            if self.stats_buf.point_n % STATS_BUFFER_COPY_INTERVAL_N == 0 {
                self.copy_stats();
            }

            return Some(self.point.clone());
        }
    }
}

unsafe impl Sync for FloatIter {}
unsafe impl Send for FloatIter {}

pub enum FloatCursor {
    FloatAscendingCursor(FloatAscendingCursor),
    FloatDescendingCursor(FloatDescendingCursor),
    FloatCastIntegerCursor(FloatCastIntegerCursor),
    FloatCastUnsignedCursor(FloatCastUnsignedCursor),
}

impl FloatCursor {
    fn close(&mut self) {
        match self {
            FloatCursor::FloatAscendingCursor(c) => c.close(),
            FloatCursor::FloatDescendingCursor(c) => c.close(),
            FloatCursor::FloatCastIntegerCursor(c) => c.close(),
            FloatCursor::FloatCastUnsignedCursor(c) => c.close(),
        }
    }

    pub fn next_float(&mut self) -> (i64, f64) {
        match self {
            FloatCursor::FloatAscendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
            FloatCursor::FloatDescendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
            FloatCursor::FloatCastIntegerCursor(c) => c.next_float(),
            FloatCursor::FloatCastUnsignedCursor(c) => c.next_float(),
        }
    }
}

impl Iterator for FloatCursor {
    type Item = (i64, f64);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FloatCursor::FloatAscendingCursor(c) => c.next(),
            FloatCursor::FloatDescendingCursor(c) => c.next(),
            FloatCursor::FloatCastIntegerCursor(c) => c.next(),
            FloatCursor::FloatCastUnsignedCursor(c) => c.next(),
        }
    }
}

struct FloatTSM {
    values: Vec<FloatValue>,
    pos: i64,
    key_cursor: Option<KeyCursor>,
}

struct FloatVLog {
    values: Vec<FloatValue>,
    pos: i64,
    key_cursor: Option<VLogKeyCursor>,
}

pub struct FloatAscendingCursor {
    cache: CursorCache,
    tsm: FloatTSM,
    vlog_enabled: bool,
    vlog: FloatVLog,
}

impl FloatAscendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = FloatTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = FloatVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = FloatAscendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };

        c.cache.values = cache_values;
        // FIXME: using binary search
        c.cache.pos = c
            .cache
            .values
            .iter()
            .position(|v| v.unix_nano() >= seek)
            .unwrap_or(c.cache.values.len()) as i64;

        // c.cache.pos = match c
        //     .cache
        //     .values
        //     .binary_search_by(|v| {
        //         if v.unix_nano() >= seek {
        //             std::cmp::Ordering::Greater
        //         } else {
        //             std::cmp::Ordering::Less
        //         }
        //     })
        //     .or_else(|i| {
        //         // i is the insertion point
        //         if i < c.cache.values.len() {
        //             Ok(i) // Return the index where seek could be inserted
        //         } else {
        //             Err(i) // Handle case where seek is greater than all elements
        //         }
        //     }) {
        //     Ok(pos) => pos as i64,
        //     Err(_) => c.cache.values.len() as i64,

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_float_block()
            .expect("FloatAscendingCursor read float block failed");
        // FIXME: using binary search
        c.tsm.pos = c
            .tsm
            .values
            .iter()
            .position(|v| v.unix_nano >= seek)
            .unwrap_or(c.tsm.values.len()) as i64;

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_float_block()
                .expect("FloatAscendingCursor read vlog float block failed");
            // FIXME: using binary search
            c.vlog.pos = c
                .vlog
                .values
                .iter()
                .position(|v| v.unix_nano >= seek)
                .unwrap_or(c.tsm.values.len()) as i64;
        }

        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, f64) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, 0.0);
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.float_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, f64) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, 0.0);
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Returns the current time/value from vlog.
    fn peek_vlog(&self) -> (i64, f64) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, 0.0);
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return;
        }

        self.cache.pos += 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos += 1;
        if self.tsm.pos >= self.tsm.values.len() as i64 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_float_block()
                .expect("FloatAscendingCursor read float block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = 0;
        }
    }

    /// Returns the next value from the VLog.
    fn next_vlog(&mut self) {
        self.vlog.pos += 1;
        if self.vlog.pos >= self.vlog.values.len() as i64 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_float_block()
                .expect("FloatAscendingCursor read float vlog block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = 0;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, f64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, 0.0);
        }

        // Cache, tsm files files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey < tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered TSM key precedes that in Cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    fn next_value_with_vlog(&mut self) -> (i64, f64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, 0.0);
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).min().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for FloatAscendingCursor {
    type Item = (i64, f64);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct FloatDescendingCursor {
    cache: CursorCache,
    tsm: FloatTSM,
    vlog_enabled: bool,
    vlog: FloatVLog,
}

impl FloatDescendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = FloatTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = FloatVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = FloatDescendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };
        c.cache.values = cache_values;
        c.cache.pos = match c.cache.values.binary_search_by(|v| {
            if v.unix_nano() >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.cache.values.len() as i64,
        };

        let (t, _) = c.peek_cache();
        if t != seek {
            c.cache.pos -= 1;
        }

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_float_block()
            .expect("FloatDescendingCursor read float block failed");
        c.tsm.pos = match c.tsm.values.binary_search_by(|v| {
            if v.unix_nano >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.tsm.values.len() as i64,
        };

        let (t, _) = c.peek_tsm();
        if t != seek {
            c.tsm.pos -= 1;
        }

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_float_block()
                .expect("FloatAscendingCursor read vlog float block failed");

            c.vlog.pos = match c
                .vlog
                .values
                .binary_search_by(|v| {
                    if v.unix_nano >= seek {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .or_else(|i| {
                    // i is the insertion point
                    if i < c.vlog.values.len() {
                        Ok(i) // Return the index where seek could be inserted
                    } else {
                        Err(i) // Handle case where seek is greater than all elements
                    }
                }) {
                Ok(pos) => pos as i64,
                Err(_) => c.vlog.values.len() as i64,
            };

            let (t, _) = c.peek_vlog();
            if t != seek {
                c.vlog.pos -= 1;
            }
        }

        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, f64) {
        if self.cache.pos < 0 || self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, 0.0);
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.float_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, f64) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, 0.0);
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Returns the current time/value from vlog.
    fn peek_vlog(&self) -> (i64, f64) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, 0.0);
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos < 0 {
            return;
        }

        self.cache.pos -= 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos -= 1;
        if self.tsm.pos < 0 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_float_block()
                .expect("FloatDescendingCursor read float block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = self.tsm.values.len() as i64 - 1;
        }
    }

    /// Returns the next value from the VLog files.
    fn next_vlog(&mut self) {
        self.vlog.pos -= 1;
        if self.vlog.pos < 0 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_float_block()
                .expect("FloatDescendingCursor read vlog float block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = self.vlog.values.len() as i64 - 1;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, f64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, 0.0);
        }

        // Both cache and tsm files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey > tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered TSM key precedes that in cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    /// Returns the next key/value for the cursor.
    fn next_value_with_vlog(&mut self) -> (i64, f64) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, 0.0);
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).max().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for FloatDescendingCursor {
    type Item = (i64, f64);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct BoolIter {
    cur: Option<BoolCursor>,
    aux: Vec<Box<dyn CursorAt + Sync + Send>>,
    conds: ConditionCursors,
    scan: Arc<StorageScan>,

    point: query_point::BoolPoint, // reusable buffer

    stats: Mutex<query_iterator::IteratorStats>,
    stats_buf: query_iterator::IteratorStats,
}

impl BoolIter {
    pub(crate) fn new_with_storage_predicate(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<BoolCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        Self::new_inner(name, tags, scan, cur, aux, conds, predicate)
    }

    fn new_inner(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<BoolCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        let aux_len = scan.returned_aux_count();
        let stats = query_iterator::IteratorStats {
            series_n: 1,
            point_n: 0,
        };
        let mut itr = Self {
            cur,
            aux,
            conds: ConditionCursors::new(predicate, conds),
            scan,

            point: query_point::BoolPoint {
                name,
                tags,
                ..Default::default()
            },
            stats: Mutex::new(stats.clone()),
            stats_buf: stats,
        };

        if aux_len > 0 {
            itr.point.aux = vec![Val::default(); aux_len];
        }

        itr
    }

    /// Copie from the itr stats buffer to the stats under lock.
    fn copy_stats(&self) {
        let mut stats = self.stats.lock();
        *stats = self.stats_buf.clone();
    }
}

impl query_iterator::QueryIterator for BoolIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Boolean
    }

    /// Returns stats on the points processed.
    fn stats(&self) -> query_iterator::IteratorStats {
        self.stats.lock().clone()
    }

    fn close(&mut self) {
        for c in &mut self.aux {
            c.close();
        }
        self.aux = vec![];
        self.conds.close();

        if let Some(c) = &mut self.cur {
            c.close();
        }
        self.cur = None;
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

impl query_iterator::BoolIterator for BoolIter {
    fn next(&mut self) -> Option<query_point::BoolPoint> {
        loop {
            let mut seek = EOF;

            if let Some(cur) = &mut self.cur {
                // Read from the main cursor if we have one.
                (self.point.time, self.point.value) = cur.next_bool();
                seek = self.point.time;
            } else {
                // Otherwise find lowest aux timestamp.
                for c in self.aux.iter_mut() {
                    if let Some((k, _)) = c.peek() {
                        if seek == EOF
                            || (self.scan.ascending() && k < seek)
                            || (!self.scan.ascending() && k > seek)
                        {
                            seek = k;
                        }
                    }
                }
                self.point.time = seek;
            }

            // Exit if we have no more points or we are outside our time range.
            if self.point.time == EOF
                || (self.scan.ascending() && self.point.time > self.scan.time_range.end_time)
                || (!self.scan.ascending() && self.point.time < self.scan.time_range.start_time)
            {
                self.copy_stats();
                return None;
            }

            // Read from each auxiliary cursor.
            for i in 0..self.scan.returned_aux_count() {
                self.point.aux[i] = match self.aux[i].next_at(seek) {
                    Some(val) => val,
                    None => Val::Empty(None),
                }
            }
            advance_row_domain_cursors(&mut self.aux, self.scan.returned_aux_count(), seek);

            if !self.conds.matches(self.point.time) {
                continue;
            }

            // Track points returned.
            self.stats_buf.point_n += 1;

            // Copy buffer to stats periodically.
            if self.stats_buf.point_n % STATS_BUFFER_COPY_INTERVAL_N == 0 {
                self.copy_stats();
            }

            return Some(self.point.clone());
        }
    }
}

unsafe impl Sync for BoolIter {}
unsafe impl Send for BoolIter {}

pub enum BoolCursor {
    BoolAscendingCursor(BoolAscendingCursor),
    BoolDescendingCursor(BoolDescendingCursor),
}

impl BoolCursor {
    fn close(&mut self) {
        match self {
            BoolCursor::BoolAscendingCursor(c) => c.close(),
            BoolCursor::BoolDescendingCursor(c) => c.close(),
        }
    }

    pub fn next_bool(&mut self) -> (i64, bool) {
        match self {
            BoolCursor::BoolAscendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
            BoolCursor::BoolDescendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
        }
    }
}

impl Iterator for BoolCursor {
    type Item = (i64, bool);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BoolCursor::BoolAscendingCursor(c) => c.next(),
            BoolCursor::BoolDescendingCursor(c) => c.next(),
        }
    }
}
struct BoolTSM {
    values: Vec<BoolValue>,
    pos: i64,
    key_cursor: Option<KeyCursor>,
}

struct BoolVLog {
    values: Vec<BoolValue>,
    pos: i64,
    key_cursor: Option<VLogKeyCursor>,
}

pub struct BoolAscendingCursor {
    cache: CursorCache,
    tsm: BoolTSM,
    vlog_enabled: bool,
    vlog: BoolVLog,
}

impl BoolAscendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = BoolTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = BoolVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = BoolAscendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };
        c.cache.values = cache_values;
        c.cache.pos = match c
            .cache
            .values
            .binary_search_by(|v| {
                if v.unix_nano() >= seek {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .or_else(|i| {
                // i is the insertion point
                if i < c.cache.values.len() {
                    Ok(i) // Return the index where seek could be inserted
                } else {
                    Err(i) // Handle case where seek is greater than all elements
                }
            }) {
            Ok(pos) => pos as i64,
            Err(_) => c.cache.values.len() as i64,
        };

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_bool_block()
            .expect("BoolAscendingCursor read bool block failed");
        c.tsm.pos = match c
            .tsm
            .values
            .binary_search_by(|v| {
                if v.unix_nano >= seek {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .or_else(|i| {
                // i is the insertion point
                if i < c.tsm.values.len() {
                    Ok(i) // Return the index where seek could be inserted
                } else {
                    Err(i) // Handle case where seek is greater than all elements
                }
            }) {
            Ok(pos) => pos as i64,
            Err(_) => c.tsm.values.len() as i64,
        };

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_bool_block()
                .expect("BoolAscendingCursor read vlog bool block failed");

            c.vlog.pos = match c
                .vlog
                .values
                .binary_search_by(|v| {
                    if v.unix_nano >= seek {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .or_else(|i| {
                    // i is the insertion point
                    if i < c.vlog.values.len() {
                        Ok(i) // Return the index where seek could be inserted
                    } else {
                        Err(i) // Handle case where seek is greater than all elements
                    }
                }) {
                Ok(pos) => pos as i64,
                Err(_) => c.vlog.values.len() as i64,
            };
        }

        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, bool) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, false);
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.bool_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, bool) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, false);
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Returns the current time/value from vlog.
    fn peek_vlog(&self) -> (i64, bool) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, false);
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return;
        }

        self.cache.pos += 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos += 1;
        if self.tsm.pos >= self.tsm.values.len() as i64 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_bool_block()
                .expect("BoolAscendingCursor read bool block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = 0;
        }
    }

    /// Returns the next value from the TSM files.
    fn next_vlog(&mut self) {
        self.vlog.pos += 1;
        if self.vlog.pos >= self.vlog.values.len() as i64 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_bool_block()
                .expect("BoolAscendingCursor read vlog bool block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = 0;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, bool) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, false);
        }

        // Both cache and tsm files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey < tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered TSM key precedes that in Cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    fn next_value_with_vlog(&mut self) -> (i64, bool) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, false);
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).min().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for BoolAscendingCursor {
    type Item = (i64, bool);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct BoolDescendingCursor {
    cache: CursorCache,
    tsm: BoolTSM,
    vlog_enabled: bool,
    vlog: BoolVLog,
}

impl BoolDescendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = BoolTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = BoolVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = BoolDescendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };
        c.cache.values = cache_values;
        c.cache.pos = match c.cache.values.binary_search_by(|v| {
            if v.unix_nano() >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.cache.values.len() as i64,
        };

        let (t, _) = c.peek_cache();
        if t != seek {
            c.cache.pos -= 1;
        }

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_bool_block()
            .expect("BoolDescendingCursor read bool block failed");
        c.tsm.pos = match c.tsm.values.binary_search_by(|v| {
            if v.unix_nano >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.tsm.values.len() as i64,
        };

        let (t, _) = c.peek_tsm();
        if t != seek {
            c.tsm.pos -= 1;
        }

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_bool_block()
                .expect("BoolAscendingCursor read vlog bool block failed");

            c.vlog.pos = match c
                .vlog
                .values
                .binary_search_by(|v| {
                    if v.unix_nano >= seek {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .or_else(|i| {
                    // i is the insertion point
                    if i < c.vlog.values.len() {
                        Ok(i) // Return the index where seek could be inserted
                    } else {
                        Err(i) // Handle case where seek is greater than all elements
                    }
                }) {
                Ok(pos) => pos as i64,
                Err(_) => c.vlog.values.len() as i64,
            };

            let (t, _) = c.peek_vlog();
            if t != seek {
                c.vlog.pos -= 1;
            }
        }

        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, bool) {
        if self.cache.pos < 0 || self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, false);
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.bool_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, bool) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, false);
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Returns the current time/value from vlog.
    fn peek_vlog(&self) -> (i64, bool) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, false);
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value)
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos < 0 {
            return;
        }

        self.cache.pos -= 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos -= 1;
        if self.tsm.pos < 0 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_bool_block()
                .expect("BoolDescendingCursor read bool block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = self.tsm.values.len() as i64 - 1;
        }
    }

    /// Returns the next value from the TSM files.
    fn next_vlog(&mut self) {
        self.vlog.pos -= 1;
        if self.vlog.pos < 0 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_bool_block()
                .expect("BoolDescendingCursor read vlog bool block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = self.vlog.values.len() as i64 - 1;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, bool) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, false);
        }

        // Both cache and tsm files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey > tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered TSM key precedes that in Cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    fn next_value_with_vlog(&mut self) -> (i64, bool) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, false);
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).max().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for BoolDescendingCursor {
    type Item = (i64, bool);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct StringIter {
    cur: Option<StringCursor>,
    aux: Vec<Box<dyn CursorAt + Sync + Send>>,
    conds: ConditionCursors,
    scan: Arc<StorageScan>,

    point: query_point::StringPoint, // reusable buffer

    stats: Mutex<query_iterator::IteratorStats>,
    stats_buf: query_iterator::IteratorStats,
}

impl StringIter {
    pub(crate) fn new_with_storage_predicate(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<StringCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        Self::new_inner(name, tags, scan, cur, aux, conds, predicate)
    }

    fn new_inner(
        name: String,
        tags: query_point::Tags,
        scan: Arc<StorageScan>,
        cur: Option<StringCursor>,
        aux: Vec<Box<dyn CursorAt + Sync + Send>>,
        conds: Vec<Box<dyn CursorAt + Sync + Send>>,
        predicate: Option<PreparedStoragePredicate>,
    ) -> Self {
        let aux_len = scan.returned_aux_count();
        let stats = query_iterator::IteratorStats {
            series_n: 1,
            point_n: 0,
        };
        let mut itr = Self {
            cur,
            aux,
            conds: ConditionCursors::new(predicate, conds),
            scan,

            point: query_point::StringPoint {
                name,
                tags,
                ..Default::default()
            },
            stats: Mutex::new(stats.clone()),
            stats_buf: stats,
        };

        if aux_len > 0 {
            itr.point.aux = vec![Val::default(); aux_len];
        }

        itr
    }

    /// Copie from the itr stats buffer to the stats under lock.
    fn copy_stats(&self) {
        let mut stats = self.stats.lock();
        *stats = self.stats_buf.clone();
    }
}

impl query_iterator::QueryIterator for StringIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::String
    }

    /// Returns stats on the points processed.
    fn stats(&self) -> query_iterator::IteratorStats {
        self.stats.lock().clone()
    }

    fn close(&mut self) {
        for c in &mut self.aux {
            c.close();
        }
        self.aux = vec![];
        self.conds.close();

        if let Some(c) = &mut self.cur {
            c.close();
        }
        self.cur = None;
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

impl query_iterator::StringIterator for StringIter {
    fn next(&mut self) -> Option<query_point::StringPoint> {
        loop {
            let mut seek = EOF;

            if let Some(cur) = &mut self.cur {
                // Read from the main cursor if we have one.
                (self.point.time, self.point.value) = cur.next_string();
                seek = self.point.time;
            } else {
                // Otherwise find lowest aux timestamp.
                for c in self.aux.iter_mut() {
                    if let Some((k, _)) = c.peek() {
                        if seek == EOF
                            || (self.scan.ascending() && k < seek)
                            || (!self.scan.ascending() && k > seek)
                        {
                            seek = k;
                        }
                    }
                }
                self.point.time = seek;
            }

            // Exit if we have no more points or we are outside our time range.
            if self.point.time == EOF
                || (self.scan.ascending() && self.point.time > self.scan.time_range.end_time)
                || (!self.scan.ascending() && self.point.time < self.scan.time_range.start_time)
            {
                self.copy_stats();
                return None;
            }

            // Read from each auxiliary cursor.
            for i in 0..self.scan.returned_aux_count() {
                self.point.aux[i] = match self.aux[i].next_at(seek) {
                    Some(val) => val,
                    None => Val::Empty(None),
                }
            }
            advance_row_domain_cursors(&mut self.aux, self.scan.returned_aux_count(), seek);

            if !self.conds.matches(self.point.time) {
                continue;
            }

            // Track points returned.
            self.stats_buf.point_n += 1;

            // Copy buffer to stats periodically.
            if self.stats_buf.point_n % STATS_BUFFER_COPY_INTERVAL_N == 0 {
                self.copy_stats();
            }

            return Some(self.point.clone());
        }
    }
}

unsafe impl Sync for StringIter {}
unsafe impl Send for StringIter {}

pub enum StringCursor {
    StringAscendingCursor(StringAscendingCursor),
    StringDescendingCursor(StringDescendingCursor),
}

impl StringCursor {
    fn close(&mut self) {
        match self {
            StringCursor::StringAscendingCursor(c) => c.close(),
            StringCursor::StringDescendingCursor(c) => c.close(),
        }
    }

    pub fn next_string(&mut self) -> (i64, String) {
        match self {
            StringCursor::StringAscendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
            StringCursor::StringDescendingCursor(c) => {
                if c.vlog_enabled {
                    c.next_value_with_vlog()
                } else {
                    c.next_value()
                }
            }
        }
    }
}

impl Iterator for StringCursor {
    type Item = (i64, String);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StringCursor::StringAscendingCursor(c) => c.next(),
            StringCursor::StringDescendingCursor(c) => c.next(),
        }
    }
}

struct StringTSM {
    values: Vec<StringValue>,
    pos: i64,
    key_cursor: Option<KeyCursor>,
}

struct StringVLog {
    values: Vec<StringValue>,
    pos: i64,
    key_cursor: Option<VLogKeyCursor>,
}

pub struct StringAscendingCursor {
    cache: CursorCache,
    tsm: StringTSM,
    vlog_enabled: bool,
    vlog: StringVLog,
}

impl StringAscendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = StringTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = StringVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = StringAscendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };
        c.cache.values = cache_values;
        c.cache.pos = match c
            .cache
            .values
            .binary_search_by(|v| {
                if v.unix_nano() >= seek {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .or_else(|i| {
                // i is the insertion point
                if i < c.cache.values.len() {
                    Ok(i) // Return the index where seek could be inserted
                } else {
                    Err(i) // Handle case where seek is greater than all elements
                }
            }) {
            Ok(pos) => pos as i64,
            Err(_) => c.cache.values.len() as i64,
        };

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_string_block()
            .expect("StringAscendingCursor read string block failed");
        c.tsm.pos = match c
            .tsm
            .values
            .binary_search_by(|v| {
                if v.unix_nano >= seek {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .or_else(|i| {
                // i is the insertion point
                if i < c.tsm.values.len() {
                    Ok(i) // Return the index where seek could be inserted
                } else {
                    Err(i) // Handle case where seek is greater than all elements
                }
            }) {
            Ok(pos) => pos as i64,
            Err(_) => c.tsm.values.len() as i64,
        };

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_string_block()
                .expect("StringAscendingCursor read vlog string block failed");

            c.vlog.pos = match c
                .vlog
                .values
                .binary_search_by(|v| {
                    if v.unix_nano >= seek {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .or_else(|i| {
                    // i is the insertion point
                    if i < c.vlog.values.len() {
                        Ok(i) // Return the index where seek could be inserted
                    } else {
                        Err(i) // Handle case where seek is greater than all elements
                    }
                }) {
                Ok(pos) => pos as i64,
                Err(_) => c.vlog.values.len() as i64,
            };
        }

        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, String) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, String::new());
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.string_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, String) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, String::new());
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value.clone())
    }

    /// Returns the current time/value from vlog.
    fn peek_vlog(&self) -> (i64, String) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, String::new());
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value.clone())
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos >= self.cache.values.len() as i64 {
            return;
        }

        self.cache.pos += 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos += 1;
        if self.tsm.pos >= self.tsm.values.len() as i64 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_string_block()
                .expect("StringAscendingCursor read string block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = 0;
        }
    }

    /// Returns the next value from the TSM files.
    fn next_vlog(&mut self) {
        self.vlog.pos += 1;
        if self.vlog.pos >= self.vlog.values.len() as i64 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_string_block()
                .expect("StringAscendingCursor read vlog string block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = 0;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, String) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, String::new());
        }

        // Both cache and tsm files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey < tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered TSM key precedes that in Cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    fn next_value_with_vlog(&mut self) -> (i64, String) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, String::new());
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).min().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for StringAscendingCursor {
    type Item = (i64, String);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct StringDescendingCursor {
    cache: CursorCache,
    tsm: StringTSM,
    vlog_enabled: bool,
    vlog: StringVLog,
}

impl StringDescendingCursor {
    pub fn new(
        vlog_enabled: bool,
        seek: i64,
        cache_values: Values,
        tsm_key_cursor: KeyCursor,
        vlog_key_cursor: Option<VLogKeyCursor>,
    ) -> Self {
        let tsm = StringTSM {
            values: vec![],
            pos: 0,
            key_cursor: Some(tsm_key_cursor),
        };
        let vlog = StringVLog {
            values: vec![],
            pos: 0,
            key_cursor: vlog_key_cursor,
        };
        let mut c = StringDescendingCursor {
            cache: CursorCache::default(),
            tsm,
            vlog_enabled,
            vlog,
        };
        c.cache.values = cache_values;
        c.cache.pos = match c.cache.values.binary_search_by(|v| {
            if v.unix_nano() >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.cache.values.len() as i64,
        };

        let (t, _) = c.peek_cache();
        if t != seek {
            c.cache.pos -= 1;
        }

        c.tsm.values = c
            .tsm
            .key_cursor
            .as_mut()
            .unwrap()
            .read_string_block()
            .expect("StringDescendingCursor read string block failed");
        c.tsm.pos = match c.tsm.values.binary_search_by(|v| {
            if v.unix_nano >= seek {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(pos) => pos as i64,
            Err(_) => c.tsm.values.len() as i64,
        };

        let (t, _) = c.peek_tsm();
        if t != seek {
            c.tsm.pos -= 1;
        }

        if vlog_enabled {
            c.vlog.values = c
                .vlog
                .key_cursor
                .as_mut()
                .unwrap()
                .read_string_block()
                .expect("StringAscendingCursor read vlog string block failed");

            c.vlog.pos = match c
                .vlog
                .values
                .binary_search_by(|v| {
                    if v.unix_nano >= seek {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .or_else(|i| {
                    // i is the insertion point
                    if i < c.vlog.values.len() {
                        Ok(i) // Return the index where seek could be inserted
                    } else {
                        Err(i) // Handle case where seek is greater than all elements
                    }
                }) {
                Ok(pos) => pos as i64,
                Err(_) => c.vlog.values.len() as i64,
            };

            let (t, _) = c.peek_vlog();
            if t != seek {
                c.vlog.pos -= 1;
            }
        }

        c
    }

    /// Returns the current time/value from the cache.
    fn peek_cache(&self) -> (i64, String) {
        if self.cache.pos < 0 || self.cache.pos >= self.cache.values.len() as i64 {
            return (EOF, String::new());
        }

        let item = &self.cache.values[self.cache.pos as usize];
        (item.unix_nano(), item.string_value().unwrap())
    }

    /// Returns the current time/value from tsm.
    fn peek_tsm(&self) -> (i64, String) {
        if self.tsm.pos < 0 || self.tsm.pos >= self.tsm.values.len() as i64 {
            return (EOF, String::new());
        }

        let item = &self.tsm.values[self.tsm.pos as usize];
        (item.unix_nano, item.value.clone())
    }

    /// Returns the current time/value from vlog.
    fn peek_vlog(&self) -> (i64, String) {
        if self.vlog.pos < 0 || self.vlog.pos >= self.vlog.values.len() as i64 {
            return (EOF, String::new());
        }

        let item = &self.vlog.values[self.vlog.pos as usize];
        (item.unix_nano, item.value.clone())
    }

    /// Close the cursor and any dependent cursors.
    fn close(&mut self) {
        if let Some(c) = &mut self.tsm.key_cursor {
            c.close();
        } else {
            return;
        }

        if let Some(c) = &mut self.vlog.key_cursor {
            c.close();
        } else {
            return;
        }

        self.tsm.key_cursor = None;
        self.cache.values = Values(vec![]);
        self.tsm.values = vec![];

        self.vlog.key_cursor = None;
        self.vlog.values = vec![];
    }

    /// Returns the next value from the cache.
    fn next_cache(&mut self) {
        if self.cache.pos < 0 {
            return;
        }

        self.cache.pos -= 1;
    }

    /// Returns the next value from the TSM files.
    fn next_tsm(&mut self) {
        self.tsm.pos -= 1;
        if self.tsm.pos < 0 {
            let key_cursor = self.tsm.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.tsm.values = key_cursor
                .read_string_block()
                .expect("StringDescendingCursor read string block failed");
            if self.tsm.values.is_empty() {
                return;
            }
            self.tsm.pos = self.tsm.values.len() as i64 - 1;
        }
    }

    /// Returns the next value from the VLog files.
    fn next_vlog(&mut self) {
        self.vlog.pos -= 1;
        if self.vlog.pos < 0 {
            let key_cursor = self.vlog.key_cursor.as_mut().unwrap();
            key_cursor.next();
            self.vlog.values = key_cursor
                .read_string_block()
                .expect("StringDescendingCursor read vlog string block failed");
            if self.vlog.values.is_empty() {
                return;
            }
            self.vlog.pos = self.vlog.values.len() as i64 - 1;
        }
    }

    /// Returns the next key/value for the cursor.
    fn next_value(&mut self) -> (i64, String) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF {
            return (EOF, String::new());
        }

        // Both cache and tsm files have the same key, cache takes precedence.
        if ckey == tkey {
            self.next_cache();
            self.next_tsm();
            return (ckey, cvalue);
        }

        // Buffered cache key precedes that in TSM file.
        if ckey != EOF && (ckey > tkey || tkey == EOF) {
            self.next_cache();
            return (ckey, cvalue);
        }

        // Buffered TSM key precedes that in Cache.
        self.next_tsm();
        (tkey, tvalue)
    }

    fn next_value_with_vlog(&mut self) -> (i64, String) {
        let (ckey, cvalue) = self.peek_cache();
        let (tkey, tvalue) = self.peek_tsm();
        let (vkey, vvalue) = self.peek_vlog();

        // No more data in cache or in TSM files.
        if ckey == EOF && tkey == EOF && vkey == EOF {
            return (EOF, String::new());
        }
        let timestamp = [ckey, tkey, vkey].into_iter()
            .filter(|t| *t != EOF).max().unwrap();
        // Advance every source at this timestamp. Cache wins a two-way or
        // three-way conflict; VLog retains its precedence over legacy TSM.
        let value = if ckey == timestamp { cvalue }
            else if vkey == timestamp { vvalue } else { tvalue };
        if ckey == timestamp { self.next_cache(); }
        if tkey == timestamp { self.next_tsm(); }
        if vkey == timestamp { self.next_vlog(); }
        (timestamp, value)
    }
}

impl Iterator for StringDescendingCursor {
    type Item = (i64, String);

    fn next(&mut self) -> Option<Self::Item> {
        let (t, v) = if self.vlog_enabled {
            self.next_value_with_vlog()
        } else {
            self.next_value()
        };

        if t == EOF { None } else { Some((t, v)) }
    }
}

pub struct LiteralValueCursor {
    value: Val,
}

impl LiteralValueCursor {
    pub fn new(value: Val) -> Self {
        Self { value }
    }

    pub fn next(&mut self) -> (i64, Val) {
        (EOF, self.value.clone())
    }
}

impl Default for LiteralValueCursor {
    fn default() -> Self {
        Self {
            value: Val::Empty(None),
        }
    }
}

impl CursorAt for LiteralValueCursor {
    fn close(&mut self) {}

    fn peek(&mut self) -> Option<(i64, Val)> {
        // (EOF, self.value.clone())
        None
    }

    fn next_at(&mut self, seek: i64) -> Option<Val> {
        Some(self.value.clone())
    }
}

#[derive(Default)]
pub struct NoneIntegerLiteralValueCursor(LiteralValueCursor);

impl NoneIntegerLiteralValueCursor {
    pub fn new(value: Val) -> Self {
        NoneIntegerLiteralValueCursor(LiteralValueCursor::new(value))
    }
}

impl CursorAt for NoneIntegerLiteralValueCursor {
    fn close(&mut self) {}

    fn peek(&mut self) -> Option<(i64, Val)> {
        // (EOF, self.value.clone())
        None
    }

    fn next_at(&mut self, seek: i64) -> Option<Val> {
        Some(self.value.clone())
    }
}

impl Deref for NoneIntegerLiteralValueCursor {
    type Target = LiteralValueCursor;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NoneIntegerLiteralValueCursor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Default)]
pub struct NoneUnsignedLiteralValueCursor(LiteralValueCursor);

impl NoneUnsignedLiteralValueCursor {
    pub fn new(value: Val) -> Self {
        NoneUnsignedLiteralValueCursor(LiteralValueCursor::new(value))
    }
}

impl CursorAt for NoneUnsignedLiteralValueCursor {
    fn close(&mut self) {}

    fn peek(&mut self) -> Option<(i64, Val)> {
        // (EOF, self.value.clone())
        None
    }

    fn next_at(&mut self, seek: i64) -> Option<Val> {
        Some(self.value.clone())
    }
}

impl Deref for NoneUnsignedLiteralValueCursor {
    type Target = LiteralValueCursor;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NoneUnsignedLiteralValueCursor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Default)]
pub struct NoneFloatLiteralValueCursor(LiteralValueCursor);

impl NoneFloatLiteralValueCursor {
    pub fn new(value: Val) -> Self {
        NoneFloatLiteralValueCursor(LiteralValueCursor::new(value))
    }
}

impl CursorAt for NoneFloatLiteralValueCursor {
    fn close(&mut self) {}

    fn peek(&mut self) -> Option<(i64, Val)> {
        // (EOF, self.value.clone())
        None
    }

    fn next_at(&mut self, seek: i64) -> Option<Val> {
        Some(self.value.clone())
    }
}

impl Deref for NoneFloatLiteralValueCursor {
    type Target = LiteralValueCursor;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NoneFloatLiteralValueCursor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Default)]
pub struct NoneBoolLiteralValueCursor(LiteralValueCursor);

impl NoneBoolLiteralValueCursor {
    pub fn new(value: Val) -> Self {
        NoneBoolLiteralValueCursor(LiteralValueCursor::new(value))
    }
}

impl CursorAt for NoneBoolLiteralValueCursor {
    fn close(&mut self) {}

    fn peek(&mut self) -> Option<(i64, Val)> {
        // (EOF, self.value.clone())
        None
    }

    fn next_at(&mut self, seek: i64) -> Option<Val> {
        Some(self.value.clone())
    }
}

impl Deref for NoneBoolLiteralValueCursor {
    type Target = LiteralValueCursor;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NoneBoolLiteralValueCursor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Default)]
pub struct NoneStringLiteralValueCursor(LiteralValueCursor);

impl NoneStringLiteralValueCursor {
    pub fn new(value: Val) -> Self {
        NoneStringLiteralValueCursor(LiteralValueCursor::new(value))
    }
}

impl CursorAt for NoneStringLiteralValueCursor {
    fn close(&mut self) {}

    fn peek(&mut self) -> Option<(i64, Val)> {
        // (EOF, self.value.clone())
        None
    }

    fn next_at(&mut self, seek: i64) -> Option<Val> {
        Some(self.value.clone())
    }
}

impl Deref for NoneStringLiteralValueCursor {
    type Target = LiteralValueCursor;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NoneStringLiteralValueCursor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// // ////////////////////// Iter.gen ////////////////////////////
// pub enum Iter {
//     IntegerIter(IntegerIter),
//     UnsignedIter(UnsignedIter),
//     FloatIter(FloatIter),
//     BoolIter(BoolIter),
//     StringIter(StringIter),
//     IntegerLimitIter(IntegerLimitIter),
//     UnsignedLimitIter(UnsignedLimitIter),
//     FloatLimitIter(FloatLimitIter),
//     BoolLimitIter(BoolLimitIter),
//     StringLimitIter(StringLimitIter),
// }

// pub enum LimitIter {
//     IntegerLimitIter(IntegerLimitIter),
//     UnsignedLimitIter(UnsignedLimitIter),
//     FloatLimitIter(FloatLimitIter),
//     BoolLimitIter(BoolLimitIter),
//     StringLimitIter(StringLimitIter),
// }

pub fn new_limit_iter(
    itr: Box<dyn Any>,
    opt: StorageLimit,
) -> Box<dyn QueryIterator + Sync + Send> {
    if (*itr).downcast_ref::<IntegerIter>().is_some() {
        let itr = itr.downcast::<IntegerIter>().unwrap();
        return Box::new(IntegerLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<IntegerLimitIter>().is_some() {
        let itr = itr.downcast::<IntegerLimitIter>().unwrap();
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

    if (*itr).downcast_ref::<IntegerInterruptIter>().is_some() {
        let itr = itr.downcast::<IntegerInterruptIter>().unwrap();
        return Box::new(IntegerLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<UnsignedIter>().is_some() {
        let itr = itr.downcast::<UnsignedIter>().unwrap();
        return Box::new(UnsignedLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<UnsignedLimitIter>().is_some() {
        let itr = itr.downcast::<UnsignedLimitIter>().unwrap();
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

    if (*itr).downcast_ref::<UnsignedInterruptIter>().is_some() {
        let itr = itr.downcast::<UnsignedInterruptIter>().unwrap();
        return Box::new(UnsignedLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<FloatIter>().is_some() {
        let itr = itr.downcast::<FloatIter>().unwrap();
        return Box::new(FloatLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<FloatLimitIter>().is_some() {
        let itr = itr.downcast::<FloatLimitIter>().unwrap();
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

    if (*itr).downcast_ref::<FloatInterruptIter>().is_some() {
        let itr = itr.downcast::<FloatInterruptIter>().unwrap();
        return Box::new(FloatLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<BoolIter>().is_some() {
        let itr = itr.downcast::<BoolIter>().unwrap();
        return Box::new(BoolLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<BoolLimitIter>().is_some() {
        let itr = itr.downcast::<BoolLimitIter>().unwrap();
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

    if (*itr).downcast_ref::<BoolInterruptIter>().is_some() {
        let itr = itr.downcast::<BoolInterruptIter>().unwrap();
        return Box::new(BoolLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<StringIter>().is_some() {
        let itr = itr.downcast::<StringIter>().unwrap();
        return Box::new(StringLimitIter::new(itr, opt));
    }

    if (*itr).downcast_ref::<StringLimitIter>().is_some() {
        let itr = itr.downcast::<StringLimitIter>().unwrap();
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

    if (*itr).downcast_ref::<StringInterruptIter>().is_some() {
        let itr = itr.downcast::<StringInterruptIter>().unwrap();
        return Box::new(StringLimitIter::new(itr, opt));
    }

    panic!("unsupported limit iterator type: {:?}", itr);
}

/// If we have a LIMIT or OFFSET and the grouping of the outer query
/// is different than the current grouping, FloatLimitIter performs the
/// limit on each of the individual series keys instead to improve
/// performance.
pub struct IntegerLimitIter {
    input: Box<dyn query_iterator::IntegerIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,
}

impl IntegerLimitIter {
    pub fn new(
        input: Box<dyn query_iterator::IntegerIterator + Sync + Send>,
        opt: StorageLimit,
    ) -> Self {
        Self { input, opt, n: 0 }
    }
}

impl query_iterator::QueryIterator for IntegerLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Integer
    }

    fn stats(&self) -> query_iterator::IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        self.input.close()
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

impl query_iterator::IntegerIterator for IntegerLimitIter {
    fn next(&mut self) -> Option<query_point::IntegerPoint> {
        // Check if we are beyond the limit.
        if self.n - self.opt.offset > self.opt.limit {
            return None;
        }

        // Read the next point.
        let p = self.input.next();
        if p.is_none() {
            return None;
        }

        // Increment counter.
        self.n += 1;

        // Offsets are handled by a higher level iterator so return all points.
        p
    }
}

/// If we have a LIMIT or OFFSET and the grouping of the outer query
/// is different than the current grouping, FloatLimitIter performs the
/// limit on each of the individual series keys instead to improve
/// performance.
pub struct UnsignedLimitIter {
    input: Box<dyn query_iterator::UnsignedIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,
}

impl UnsignedLimitIter {
    pub fn new(
        input: Box<dyn query_iterator::UnsignedIterator + Sync + Send>,
        opt: StorageLimit,
    ) -> Self {
        Self { input, opt, n: 0 }
    }
}

impl query_iterator::QueryIterator for UnsignedLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Unsigned
    }

    fn stats(&self) -> query_iterator::IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        self.input.close()
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

impl query_iterator::UnsignedIterator for UnsignedLimitIter {
    fn next(&mut self) -> Option<query_point::UnsignedPoint> {
        // Check if we are beyond the limit.
        if self.n - self.opt.offset > self.opt.limit {
            return None;
        }

        // Read the next point.
        let p = self.input.next();
        if p.is_none() {
            return None;
        }

        // Increment counter.
        self.n += 1;

        // Offsets are handled by a higher level iterator so return all points.
        p
    }
}

/// If we have a LIMIT or OFFSET and the grouping of the outer query
/// is different than the current grouping, FloatLimitIter performs the
/// limit on each of the individual series keys instead to improve
/// performance.
pub struct FloatLimitIter {
    input: Box<dyn query_iterator::FloatIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,
}

impl FloatLimitIter {
    pub fn new(
        input: Box<dyn query_iterator::FloatIterator + Sync + Send>,
        opt: StorageLimit,
    ) -> Self {
        Self { input, opt, n: 0 }
    }
}

impl query_iterator::QueryIterator for FloatLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Float
    }

    fn stats(&self) -> query_iterator::IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        self.input.close()
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

impl query_iterator::FloatIterator for FloatLimitIter {
    fn next(&mut self) -> Option<query_point::FloatPoint> {
        // Check if we are beyond the limit.
        if self.n - self.opt.offset > self.opt.limit {
            return None;
        }

        // Read the next point.
        let p = self.input.next();
        if p.is_none() {
            return None;
        }

        // Increment counter.
        self.n += 1;

        // Offsets are handled by a higher level iterator so return all points.
        p
    }
}

/// If we have a LIMIT or OFFSET and the grouping of the outer query
/// is different than the current grouping, FloatLimitIter performs the
/// limit on each of the individual series keys instead to improve
/// performance.
pub struct BoolLimitIter {
    input: Box<dyn query_iterator::BoolIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,
}

impl BoolLimitIter {
    pub fn new(
        input: Box<dyn query_iterator::BoolIterator + Sync + Send>,
        opt: StorageLimit,
    ) -> Self {
        Self { input, opt, n: 0 }
    }
}

impl query_iterator::QueryIterator for BoolLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::Boolean
    }

    fn stats(&self) -> query_iterator::IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        self.input.close()
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

impl query_iterator::BoolIterator for BoolLimitIter {
    fn next(&mut self) -> Option<query_point::BoolPoint> {
        // Check if we are beyond the limit.
        if self.n - self.opt.offset > self.opt.limit {
            return None;
        }

        // Read the next point.
        let p = self.input.next();
        if p.is_none() {
            return None;
        }

        // Increment counter.
        self.n += 1;

        // Offsets are handled by a higher level iterator so return all points.
        p
    }
}
/// If we have a LIMIT or OFFSET and the grouping of the outer query
/// is different than the current grouping, FloatLimitIter performs the
/// limit on each of the individual series keys instead to improve
/// performance.
pub struct StringLimitIter {
    input: Box<dyn query_iterator::StringIterator + Sync + Send>,
    opt: StorageLimit,
    n: i64,
}

impl StringLimitIter {
    pub fn new(
        input: Box<dyn query_iterator::StringIterator + Sync + Send>,
        opt: StorageLimit,
    ) -> Self {
        Self { input, opt, n: 0 }
    }
}

impl query_iterator::QueryIterator for StringLimitIter {
    fn data_type(&self) -> VarRefDataType {
        VarRefDataType::String
    }

    fn stats(&self) -> query_iterator::IteratorStats {
        self.input.stats()
    }

    fn close(&mut self) {
        self.input.close()
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

impl query_iterator::StringIterator for StringLimitIter {
    fn next(&mut self) -> Option<query_point::StringPoint> {
        // Check if we are beyond the limit.
        if self.n - self.opt.offset > self.opt.limit {
            return None;
        }

        // Read the next point.
        let p = self.input.next();
        if p.is_none() {
            return None;
        }

        // Increment counter.
        self.n += 1;

        // Offsets are handled by a higher level iterator so return all points.
        p
    }
}
