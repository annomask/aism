//! Streaming Arrow batches assembled at the storage query boundary.

use std::{
    cmp::Ordering,
    collections::HashMap,
    error::Error as StdError,
    fmt,
    ops::Range,
    sync::{Arc, mpsc},
    time::Instant,
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, RecordBatchOptions,
    StringArray, TimestampNanosecondArray, UInt64Array,
    builder::{
        BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
        UInt64Builder,
    },
};
use arrow_schema::{ArrowError, DataType, SchemaRef};

#[cfg(feature = "slow-trace")]
use crate::query::storage_ir::StorageScanProfile;
use crate::{
    BOOL, FLOAT, FieldType, INTEGER, STRING, UNSIGNED, Val,
    db::{LeasedFloatIterator, LeasedQueryIterator, ReefsDbOperationLease},
    query::{
        FloatIterator,
        metadata::{field_type_to_arrow, time_arrow_type},
        query_iterator,
        query_point::{FloatPoint, Tags},
        storage_ir::{ColumnKind, StorageScan},
    },
    runtime::execution::{
        RuntimeExecutorExt, StorageQueryCpuBudget, StorageQueryCpuPermit, TaskHandle,
    },
    storage::{
        active_cursor,
        tsm1::engine::{
            DirectTsmBlock, DirectTsmBlockValues, DirectTsmScan, DirectTsmScanSet,
            DirectTsmSeriesScan,
        },
    },
};
use tokio_util::sync::CancellationToken;

use super::metadata::TIME_COLUMN_NAME;

/// One storage-native request for a streaming Arrow scan.
#[derive(Clone)]
pub struct RecordBatchRequest {
    scan: StorageScan,
    schema: SchemaRef,
    batch_rows: usize,
    max_batch_bytes: usize,
}

impl RecordBatchRequest {
    pub fn new(
        scan: StorageScan,
        schema: SchemaRef,
        batch_rows: usize,
        max_batch_bytes: usize,
    ) -> Self {
        Self {
            scan,
            schema,
            batch_rows: batch_rows.max(1),
            max_batch_bytes: max_batch_bytes.max(1),
        }
    }

    pub fn scan(&self) -> &StorageScan {
        &self.scan
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn batch_rows(&self) -> usize {
        self.batch_rows
    }

    pub fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }
}

/// Lightweight counters for one batch reader.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecordBatchMetrics {
    pub batches_emitted: u64,
    pub rows_emitted: u64,
    pub iterator_fallback_rows: u64,
    pub direct_batch_rows: u64,
    pub direct_blocks: u64,
    pub partial_direct_blocks: u64,
    pub value_decoded_blocks: u64,
    pub timestamp_only_blocks: u64,
    pub value_decoded_points: u64,
    pub timestamp_only_points: u64,
    pub multi_field_direct_rows: u64,
    pub multi_field_direct_batches: u64,
    pub multi_field_streams: u64,
    pub alignment_steps: u64,
    pub null_values_emitted: u64,
    pub estimated_bytes_emitted: u64,
    pub batch_build_nanos: u64,
}

impl RecordBatchMetrics {
    fn merge(&mut self, other: Self) {
        macro_rules! merge {
            ($($field:ident),+ $(,)?) => {
                $(self.$field = self.$field.saturating_add(other.$field);)+
            };
        }
        merge!(
            batches_emitted,
            rows_emitted,
            iterator_fallback_rows,
            direct_batch_rows,
            direct_blocks,
            partial_direct_blocks,
            value_decoded_blocks,
            timestamp_only_blocks,
            value_decoded_points,
            timestamp_only_points,
            multi_field_direct_rows,
            multi_field_direct_batches,
            multi_field_streams,
            alignment_steps,
            null_values_emitted,
            estimated_bytes_emitted,
            batch_build_nanos,
        );
    }
}

/// Error surfaced while constructing or draining a storage batch reader.
#[derive(Debug)]
pub enum RecordBatchError {
    Storage {
        source: Box<dyn StdError + Send + Sync>,
    },
    InvalidRequest {
        reason: String,
    },
    Arrow {
        source: ArrowError,
    },
}

impl RecordBatchError {
    pub(crate) fn storage(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Storage {
            source: Box::new(source),
        }
    }

    fn invalid(reason: impl Into<String>) -> Self {
        Self::InvalidRequest {
            reason: reason.into(),
        }
    }
}

impl fmt::Display for RecordBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage { source } => write!(f, "storage batch scan failed: {source}"),
            Self::InvalidRequest { reason } => {
                write!(f, "invalid storage batch request: {reason}")
            }
            Self::Arrow { source } => write!(f, "Arrow batch construction failed: {source}"),
        }
    }
}

impl StdError for RecordBatchError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Storage { source } => Some(source.as_ref()),
            Self::Arrow { source } => Some(source),
            Self::InvalidRequest { .. } => None,
        }
    }
}

impl From<ArrowError> for RecordBatchError {
    fn from(source: ArrowError) -> Self {
        Self::Arrow { source }
    }
}

struct FieldSlot {
    slot: usize,
    field_type: FieldType,
}

enum ColumnBuilder {
    Time(TimestampNanosecondBuilder),
    Tag {
        name: String,
        builder: StringBuilder,
    },
    Float {
        slot: usize,
        builder: Float64Builder,
    },
    Integer {
        slot: usize,
        builder: Int64Builder,
    },
    Unsigned {
        slot: usize,
        builder: UInt64Builder,
    },
    Boolean {
        slot: usize,
        builder: BooleanBuilder,
    },
    String {
        slot: usize,
        builder: StringBuilder,
    },
}

impl ColumnBuilder {
    fn append(&mut self, point: &FloatPoint) -> Result<usize, RecordBatchError> {
        match self {
            Self::Time(builder) => {
                builder.append_value(point.time);
                Ok(size_of::<i64>())
            }
            Self::Tag { name, builder } => match point.tags.value_ref(name) {
                Some(value) => {
                    builder.append_value(value);
                    Ok(value.len())
                }
                None => {
                    builder.append_null();
                    Ok(0)
                }
            },
            Self::Float { slot, builder } => match point.aux.get(*slot) {
                Some(Val::F64(value)) => {
                    builder.append_value(*value);
                    Ok(size_of::<f64>())
                }
                Some(Val::Empty(_)) => {
                    builder.append_null();
                    Ok(0)
                }
                value => Err(aux_type_error(*slot, "Float64", value)),
            },
            Self::Integer { slot, builder } => match point.aux.get(*slot) {
                Some(Val::I64(value)) => {
                    builder.append_value(*value);
                    Ok(size_of::<i64>())
                }
                Some(Val::Empty(_)) => {
                    builder.append_null();
                    Ok(0)
                }
                value => Err(aux_type_error(*slot, "Int64", value)),
            },
            Self::Unsigned { slot, builder } => match point.aux.get(*slot) {
                Some(Val::U64(value)) => {
                    builder.append_value(*value);
                    Ok(size_of::<u64>())
                }
                Some(Val::Empty(_)) => {
                    builder.append_null();
                    Ok(0)
                }
                value => Err(aux_type_error(*slot, "UInt64", value)),
            },
            Self::Boolean { slot, builder } => match point.aux.get(*slot) {
                Some(Val::Bool(value)) => {
                    builder.append_value(*value);
                    Ok(size_of::<bool>())
                }
                Some(Val::Empty(_)) => {
                    builder.append_null();
                    Ok(0)
                }
                value => Err(aux_type_error(*slot, "Boolean", value)),
            },
            Self::String { slot, builder } => match point.aux.get(*slot) {
                Some(Val::String(value)) => {
                    builder.append_value(value);
                    Ok(value.len())
                }
                Some(Val::Empty(_)) => {
                    builder.append_null();
                    Ok(0)
                }
                value => Err(aux_type_error(*slot, "Utf8", value)),
            },
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Time(builder) => Arc::new(builder.finish()),
            Self::Tag { builder, .. } | Self::String { builder, .. } => Arc::new(builder.finish()),
            Self::Float { builder, .. } => Arc::new(builder.finish()),
            Self::Integer { builder, .. } => Arc::new(builder.finish()),
            Self::Unsigned { builder, .. } => Arc::new(builder.finish()),
            Self::Boolean { builder, .. } => Arc::new(builder.finish()),
        }
    }

    fn append_batch_row(
        &mut self,
        array: &ArrayRef,
        row: usize,
    ) -> Result<usize, RecordBatchError> {
        macro_rules! append_primitive {
            ($array_type:ty, $builder:expr, $size:expr, $expected:literal) => {{
                let array = array
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .ok_or_else(|| direct_type_error($expected))?;
                if array.is_null(row) {
                    $builder.append_null();
                    Ok(0)
                } else {
                    $builder.append_value(array.value(row));
                    Ok($size)
                }
            }};
        }

        match self {
            Self::Time(builder) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| direct_type_error("TimestampNanosecond"))?;
                if array.is_null(row) {
                    return Err(RecordBatchError::invalid(
                        "direct merge received a null timestamp",
                    ));
                }
                builder.append_value(array.value(row));
                Ok(size_of::<i64>())
            }
            Self::Tag { builder, .. } | Self::String { builder, .. } => {
                let array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| direct_type_error("Utf8"))?;
                if array.is_null(row) {
                    builder.append_null();
                    Ok(0)
                } else {
                    let value = array.value(row);
                    builder.append_value(value);
                    Ok(value.len())
                }
            }
            Self::Float { builder, .. } => {
                append_primitive!(Float64Array, builder, size_of::<f64>(), "Float64")
            }
            Self::Integer { builder, .. } => {
                append_primitive!(Int64Array, builder, size_of::<i64>(), "Int64")
            }
            Self::Unsigned { builder, .. } => {
                append_primitive!(UInt64Array, builder, size_of::<u64>(), "UInt64")
            }
            Self::Boolean { builder, .. } => {
                append_primitive!(BooleanArray, builder, size_of::<bool>(), "Boolean")
            }
        }
    }

    fn direct_row_size(
        &self,
        block: &DirectTsmBlock,
        index: usize,
    ) -> Result<usize, RecordBatchError> {
        match self {
            Self::Time(_) => Ok(size_of::<i64>()),
            Self::Tag { name, .. } => Ok(block.tags.value_ref(name).map_or(0, str::len)),
            Self::Float { slot: 0, .. } => match &block.values {
                DirectTsmBlockValues::Float(_) => Ok(size_of::<f64>()),
                _ => Err(direct_type_error("Float64")),
            },
            Self::Integer { slot: 0, .. } => match &block.values {
                DirectTsmBlockValues::Integer(_) => Ok(size_of::<i64>()),
                _ => Err(direct_type_error("Int64")),
            },
            Self::Unsigned { slot: 0, .. } => match &block.values {
                DirectTsmBlockValues::Unsigned(_) => Ok(size_of::<u64>()),
                _ => Err(direct_type_error("UInt64")),
            },
            Self::Boolean { slot: 0, .. } => match &block.values {
                DirectTsmBlockValues::Boolean(_) => Ok(size_of::<bool>()),
                _ => Err(direct_type_error("Boolean")),
            },
            Self::String { slot: 0, .. } => match &block.values {
                DirectTsmBlockValues::String(values) => Ok(values[index].len()),
                _ => Err(direct_type_error("Utf8")),
            },
            Self::Float { slot, .. }
            | Self::Integer { slot, .. }
            | Self::Unsigned { slot, .. }
            | Self::Boolean { slot, .. }
            | Self::String { slot, .. } => Err(RecordBatchError::invalid(format!(
                "direct TSM block expected field slot 0, found slot {slot}"
            ))),
        }
    }

    fn append_direct_range(
        &mut self,
        block: &DirectTsmBlock,
        range: Range<usize>,
    ) -> Result<usize, RecordBatchError> {
        let rows = range.len();
        match self {
            Self::Time(builder) => {
                builder.append_slice(&block.timestamps[range]);
                Ok(rows * size_of::<i64>())
            }
            Self::Tag { name, builder } => match block.tags.value_ref(name) {
                Some(value) => {
                    for _ in range {
                        builder.append_value(value);
                    }
                    Ok(rows * value.len())
                }
                None => {
                    for _ in range {
                        builder.append_null();
                    }
                    Ok(0)
                }
            },
            Self::Float { slot: 0, builder } => match &block.values {
                DirectTsmBlockValues::Float(values) => {
                    builder.append_slice(&values[range]);
                    Ok(rows * size_of::<f64>())
                }
                _ => Err(direct_type_error("Float64")),
            },
            Self::Integer { slot: 0, builder } => match &block.values {
                DirectTsmBlockValues::Integer(values) => {
                    builder.append_slice(&values[range]);
                    Ok(rows * size_of::<i64>())
                }
                _ => Err(direct_type_error("Int64")),
            },
            Self::Unsigned { slot: 0, builder } => match &block.values {
                DirectTsmBlockValues::Unsigned(values) => {
                    builder.append_slice(&values[range]);
                    Ok(rows * size_of::<u64>())
                }
                _ => Err(direct_type_error("UInt64")),
            },
            Self::Boolean { slot: 0, builder } => match &block.values {
                DirectTsmBlockValues::Boolean(values) => {
                    builder.append_slice(&values[range]);
                    Ok(rows * size_of::<bool>())
                }
                _ => Err(direct_type_error("Boolean")),
            },
            Self::String { slot: 0, builder } => match &block.values {
                DirectTsmBlockValues::String(values) => {
                    let mut bytes = 0;
                    for value in &values[range] {
                        builder.append_value(value);
                        bytes += value.len();
                    }
                    Ok(bytes)
                }
                _ => Err(direct_type_error("Utf8")),
            },
            Self::Float { slot, .. }
            | Self::Integer { slot, .. }
            | Self::Unsigned { slot, .. }
            | Self::Boolean { slot, .. }
            | Self::String { slot, .. } => Err(RecordBatchError::invalid(format!(
                "direct TSM block expected field slot 0, found slot {slot}"
            ))),
        }
    }

    fn append_multi_direct(
        &mut self,
        timestamp: i64,
        tags: &Tags,
        streams: &[DirectFieldStream],
    ) -> Result<(usize, bool), RecordBatchError> {
        match self {
            Self::Time(builder) => {
                builder.append_value(timestamp);
                Ok((size_of::<i64>(), false))
            }
            Self::Tag { name, builder } => match tags.value_ref(name) {
                Some(value) => {
                    builder.append_value(value);
                    Ok((value.len(), false))
                }
                None => {
                    builder.append_null();
                    Ok((0, true))
                }
            },
            Self::Float { slot, builder } => match direct_value_at(streams, *slot, timestamp)? {
                Some((DirectTsmBlockValues::Float(values), index)) => {
                    builder.append_value(values[index]);
                    Ok((size_of::<f64>(), false))
                }
                Some(_) => Err(direct_type_error("Float64")),
                None => {
                    builder.append_null();
                    Ok((0, true))
                }
            },
            Self::Integer { slot, builder } => match direct_value_at(streams, *slot, timestamp)? {
                Some((DirectTsmBlockValues::Integer(values), index)) => {
                    builder.append_value(values[index]);
                    Ok((size_of::<i64>(), false))
                }
                Some(_) => Err(direct_type_error("Int64")),
                None => {
                    builder.append_null();
                    Ok((0, true))
                }
            },
            Self::Unsigned { slot, builder } => match direct_value_at(streams, *slot, timestamp)? {
                Some((DirectTsmBlockValues::Unsigned(values), index)) => {
                    builder.append_value(values[index]);
                    Ok((size_of::<u64>(), false))
                }
                Some(_) => Err(direct_type_error("UInt64")),
                None => {
                    builder.append_null();
                    Ok((0, true))
                }
            },
            Self::Boolean { slot, builder } => match direct_value_at(streams, *slot, timestamp)? {
                Some((DirectTsmBlockValues::Boolean(values), index)) => {
                    builder.append_value(values[index]);
                    Ok((size_of::<bool>(), false))
                }
                Some(_) => Err(direct_type_error("Boolean")),
                None => {
                    builder.append_null();
                    Ok((0, true))
                }
            },
            Self::String { slot, builder } => match direct_value_at(streams, *slot, timestamp)? {
                Some((DirectTsmBlockValues::String(values), index)) => {
                    builder.append_value(&values[index]);
                    Ok((values[index].len(), false))
                }
                Some(_) => Err(direct_type_error("Utf8")),
                None => {
                    builder.append_null();
                    Ok((0, true))
                }
            },
        }
    }
}

fn aux_type_error(slot: usize, expected: &str, actual: Option<&Val>) -> RecordBatchError {
    RecordBatchError::invalid(format!(
        "aux slot {slot} expected {expected}, received {actual:?}"
    ))
}

fn direct_type_error(expected: &str) -> RecordBatchError {
    RecordBatchError::invalid(format!(
        "direct TSM block does not contain expected {expected} values"
    ))
}

struct BatchBuilder {
    schema: SchemaRef,
    columns: Vec<ColumnBuilder>,
    expected_aux_values: usize,
    rows: usize,
    estimated_bytes: usize,
}

impl BatchBuilder {
    fn try_new(request: &RecordBatchRequest) -> Result<Self, RecordBatchError> {
        if request.scan.projection.primary_column.is_some() {
            return Err(RecordBatchError::invalid(
                "RecordBatchReader currently requires an aux-only StorageScan",
            ));
        }

        let mut fields = HashMap::with_capacity(request.scan.projection.returned_columns.len());
        for (slot, column) in request.scan.projection.returned_columns.iter().enumerate() {
            let ColumnKind::Field(Some(field_type)) = column.kind else {
                return Err(RecordBatchError::invalid(format!(
                    "returned column `{}` is not a typed field",
                    column.name
                )));
            };
            if fields
                .insert(column.name.as_str(), FieldSlot { slot, field_type })
                .is_some()
            {
                return Err(RecordBatchError::invalid(format!(
                    "returned field `{}` appears more than once",
                    column.name
                )));
            }
        }

        let mut columns = Vec::with_capacity(request.schema.fields().len());
        for field in request.schema.fields() {
            let name = field.name();
            let column = if name == TIME_COLUMN_NAME {
                require_arrow_type(name, field.data_type(), &time_arrow_type())?;
                ColumnBuilder::Time(TimestampNanosecondBuilder::new())
            } else if let Some(field_slot) = fields.get(name.as_str()) {
                require_arrow_type(
                    name,
                    field.data_type(),
                    &field_type_to_arrow(field_slot.field_type),
                )?;
                field_builder(field_slot.slot, field_slot.field_type)?
            } else if request
                .scan
                .projection
                .series
                .output_tag_keys
                .iter()
                .any(|tag| tag == name)
            {
                require_arrow_type(name, field.data_type(), &DataType::Utf8)?;
                ColumnBuilder::Tag {
                    name: name.clone(),
                    builder: StringBuilder::new(),
                }
            } else {
                return Err(RecordBatchError::invalid(format!(
                    "Arrow column `{name}` is not time, a returned field, or an output tag"
                )));
            };
            columns.push(column);
        }

        Ok(Self {
            schema: Arc::clone(&request.schema),
            columns,
            expected_aux_values: request.scan.projection.returned_columns.len(),
            rows: 0,
            estimated_bytes: 0,
        })
    }

    fn append(&mut self, point: &FloatPoint) -> Result<(), RecordBatchError> {
        if point.aux.len() != self.expected_aux_values {
            return Err(RecordBatchError::invalid(format!(
                "point has {} aux values, expected {}",
                point.aux.len(),
                self.expected_aux_values
            )));
        }
        for column in &mut self.columns {
            self.estimated_bytes = self.estimated_bytes.saturating_add(column.append(point)?);
        }
        self.rows += 1;
        Ok(())
    }

    fn append_batch_row(
        &mut self,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<(), RecordBatchError> {
        if batch.schema() != self.schema {
            return Err(RecordBatchError::invalid(
                "direct merge batch schema does not match the requested schema",
            ));
        }
        if row >= batch.num_rows() {
            return Err(RecordBatchError::invalid(format!(
                "direct merge row {row} is outside a {} row batch",
                batch.num_rows()
            )));
        }
        for (column, array) in self.columns.iter_mut().zip(batch.columns()) {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(column.append_batch_row(array, row)?);
        }
        self.rows += 1;
        Ok(())
    }

    fn direct_rows_to_append(
        &self,
        block: &DirectTsmBlock,
        start: usize,
        end: usize,
        batch_rows: usize,
        max_batch_bytes: usize,
    ) -> Result<usize, RecordBatchError> {
        let row_capacity = batch_rows.saturating_sub(self.rows).max(1);
        let mut rows = 0;
        let mut bytes = self.estimated_bytes;
        for index in start..end.min(start.saturating_add(row_capacity)) {
            for column in &self.columns {
                bytes = bytes.saturating_add(column.direct_row_size(block, index)?);
            }
            rows += 1;
            if bytes >= max_batch_bytes {
                break;
            }
        }
        Ok(rows)
    }

    fn append_direct_range(
        &mut self,
        block: &DirectTsmBlock,
        range: Range<usize>,
    ) -> Result<(), RecordBatchError> {
        if self.expected_aux_values != 1 {
            return Err(RecordBatchError::invalid(format!(
                "direct TSM block requires one returned field, found {}",
                self.expected_aux_values
            )));
        }
        let rows = range.len();
        for column in &mut self.columns {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(column.append_direct_range(block, range.clone())?);
        }
        self.rows += rows;
        Ok(())
    }

    fn append_multi_direct(
        &mut self,
        timestamp: i64,
        tags: &Tags,
        streams: &[DirectFieldStream],
    ) -> Result<usize, RecordBatchError> {
        if streams.len() < self.expected_aux_values {
            return Err(RecordBatchError::invalid(format!(
                "multi-field direct scan has {} streams for {} returned fields",
                streams.len(),
                self.expected_aux_values
            )));
        }
        let mut nulls = 0;
        for column in &mut self.columns {
            let (bytes, is_null) = column.append_multi_direct(timestamp, tags, streams)?;
            self.estimated_bytes = self.estimated_bytes.saturating_add(bytes);
            nulls += usize::from(is_null);
        }
        self.rows += 1;
        Ok(nulls)
    }

    fn finish(&mut self) -> Result<(RecordBatch, usize), RecordBatchError> {
        let rows = self.rows;
        let estimated_bytes = self.estimated_bytes;
        let columns = self.columns.iter_mut().map(ColumnBuilder::finish).collect();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&self.schema), columns, &options)?;
        self.rows = 0;
        self.estimated_bytes = 0;
        Ok((batch, estimated_bytes))
    }
}

fn require_arrow_type(
    name: &str,
    actual: &DataType,
    expected: &DataType,
) -> Result<(), RecordBatchError> {
    if actual != expected {
        return Err(RecordBatchError::invalid(format!(
            "Arrow column `{name}` has type {actual:?}, expected {expected:?}"
        )));
    }
    Ok(())
}

fn field_builder(slot: usize, field_type: FieldType) -> Result<ColumnBuilder, RecordBatchError> {
    match field_type {
        FLOAT => Ok(ColumnBuilder::Float {
            slot,
            builder: Float64Builder::new(),
        }),
        INTEGER => Ok(ColumnBuilder::Integer {
            slot,
            builder: Int64Builder::new(),
        }),
        UNSIGNED => Ok(ColumnBuilder::Unsigned {
            slot,
            builder: UInt64Builder::new(),
        }),
        BOOL => Ok(ColumnBuilder::Boolean {
            slot,
            builder: BooleanBuilder::new(),
        }),
        STRING => Ok(ColumnBuilder::String {
            slot,
            builder: StringBuilder::new(),
        }),
        other => Err(RecordBatchError::invalid(format!(
            "unsupported storage field type {other}"
        ))),
    }
}

enum PointSource {
    Storage(LeasedFloatIterator),
    Empty,
    #[cfg(test)]
    Test(std::collections::VecDeque<FloatPoint>),
}

struct DirectFieldStream {
    scan: DirectTsmScan,
    pending: Option<DirectTsmBlock>,
    offset: usize,
}

struct DirectBlockLoad {
    partial: bool,
    timestamp_only: bool,
    points: usize,
}

impl DirectFieldStream {
    fn new(scan: DirectTsmScan) -> Self {
        Self {
            scan,
            pending: None,
            offset: 0,
        }
    }

    fn ensure_current(&mut self) -> Result<Option<DirectBlockLoad>, RecordBatchError> {
        if self.pending.is_some() {
            return Ok(None);
        }
        let Some(block) = self.scan.next_block().map_err(RecordBatchError::storage)? else {
            return Ok(None);
        };
        self.offset = block.start;
        let loaded = DirectBlockLoad {
            partial: block.partial,
            timestamp_only: matches!(block.values, DirectTsmBlockValues::TimestampOnly),
            points: block.timestamps.len(),
        };
        self.pending = Some(block);
        Ok(Some(loaded))
    }

    fn current_timestamp(&self) -> Option<i64> {
        let block = self.pending.as_ref()?;
        Some(block.timestamps[self.offset])
    }

    fn current_value(&self) -> Option<(&DirectTsmBlockValues, usize, i64)> {
        let block = self.pending.as_ref()?;
        Some((&block.values, self.offset, block.timestamps[self.offset]))
    }

    fn advance_if_at(&mut self, timestamp: i64) {
        let Some(block) = self.pending.as_ref() else {
            return;
        };
        if block.timestamps[self.offset] != timestamp {
            return;
        }
        self.offset += 1;
        if self.offset == block.end {
            self.pending = None;
        }
    }
}

fn direct_value_at(
    streams: &[DirectFieldStream],
    slot: usize,
    timestamp: i64,
) -> Result<Option<(&DirectTsmBlockValues, usize)>, RecordBatchError> {
    let stream = streams.get(slot).ok_or_else(|| {
        RecordBatchError::invalid(format!(
            "multi-field direct scan is missing field slot {slot}"
        ))
    })?;
    Ok(stream
        .current_value()
        .and_then(|(values, index, current)| (current == timestamp).then_some((values, index))))
}

struct DirectSource {
    scan: DirectTsmScan,
    pending: Option<DirectTsmBlock>,
    offset: usize,
    _lease: ReefsDbOperationLease,
}

struct DirectAppend {
    rows: usize,
    new_block: bool,
    partial_block: bool,
    timestamp_only_block: bool,
    decoded_points: usize,
    #[cfg(feature = "slow-trace")]
    arrow_append_us: u64,
}

impl DirectSource {
    fn append(
        &mut self,
        builder: &mut BatchBuilder,
        batch_rows: usize,
        max_batch_bytes: usize,
    ) -> Result<Option<DirectAppend>, RecordBatchError> {
        let mut new_block = false;
        let mut timestamp_only_block = false;
        let mut decoded_points = 0;
        if self.pending.is_none() {
            self.pending = self.scan.next_block().map_err(RecordBatchError::storage)?;
            let Some(block) = self.pending.as_ref() else {
                return Ok(None);
            };
            self.offset = block.start;
            new_block = true;
            timestamp_only_block = matches!(block.values, DirectTsmBlockValues::TimestampOnly);
            decoded_points = block.timestamps.len();
        }

        let block = self
            .pending
            .as_ref()
            .expect("pending block was initialized");
        let partial_block = block.partial;
        let rows = builder.direct_rows_to_append(
            block,
            self.offset,
            block.end,
            batch_rows,
            max_batch_bytes,
        )?;
        let end = self.offset + rows;
        #[cfg(feature = "slow-trace")]
        let append_started = Instant::now();
        builder.append_direct_range(block, self.offset..end)?;
        #[cfg(feature = "slow-trace")]
        let arrow_append_us = append_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        self.offset = end;
        if self.offset == block.end {
            self.pending = None;
        }
        Ok(Some(DirectAppend {
            rows,
            new_block,
            partial_block,
            timestamp_only_block,
            decoded_points,
            #[cfg(feature = "slow-trace")]
            arrow_append_us,
        }))
    }
}

struct MultiDirectSource {
    streams: Vec<DirectFieldStream>,
    tags: Arc<Tags>,
    _lease: ReefsDbOperationLease,
}

struct MultiDirectAppend {
    direct_blocks: usize,
    partial_direct_blocks: usize,
    null_values: usize,
    value_decoded_blocks: usize,
    timestamp_only_blocks: usize,
    value_decoded_points: usize,
    timestamp_only_points: usize,
    #[cfg(feature = "slow-trace")]
    arrow_append_us: u64,
    #[cfg(feature = "slow-trace")]
    row_domain_merge_us: u64,
}

impl MultiDirectSource {
    fn append(
        &mut self,
        builder: &mut BatchBuilder,
    ) -> Result<Option<MultiDirectAppend>, RecordBatchError> {
        #[cfg(feature = "slow-trace")]
        let merge_started = Instant::now();
        let mut direct_blocks = 0;
        let mut partial_direct_blocks = 0;
        let mut value_decoded_blocks = 0;
        let mut timestamp_only_blocks = 0;
        let mut value_decoded_points = 0;
        let mut timestamp_only_points = 0;
        for stream in &mut self.streams {
            if let Some(loaded) = stream.ensure_current()? {
                direct_blocks += 1;
                partial_direct_blocks += usize::from(loaded.partial);
                if loaded.timestamp_only {
                    timestamp_only_blocks += 1;
                    timestamp_only_points += loaded.points;
                } else {
                    value_decoded_blocks += 1;
                    value_decoded_points += loaded.points;
                }
            }
        }

        let Some(timestamp) = self
            .streams
            .iter()
            .filter_map(DirectFieldStream::current_timestamp)
            .min()
        else {
            return Ok(None);
        };
        #[cfg(feature = "slow-trace")]
        let append_started = Instant::now();
        let null_values =
            builder.append_multi_direct(timestamp, self.tags.as_ref(), &self.streams)?;
        #[cfg(feature = "slow-trace")]
        let arrow_append_us = append_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        for stream in &mut self.streams {
            stream.advance_if_at(timestamp);
        }
        #[cfg(feature = "slow-trace")]
        let row_domain_merge_us = merge_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        Ok(Some(MultiDirectAppend {
            direct_blocks,
            partial_direct_blocks,
            null_values,
            value_decoded_blocks,
            timestamp_only_blocks,
            value_decoded_points,
            timestamp_only_points,
            #[cfg(feature = "slow-trace")]
            arrow_append_us,
            #[cfg(feature = "slow-trace")]
            row_domain_merge_us: row_domain_merge_us.saturating_sub(arrow_append_us),
        }))
    }
}

struct DirectSeriesInput {
    source_id: usize,
    scan: DirectTsmSeriesScan,
}

struct DirectSeriesPartition {
    series: Vec<DirectSeriesInput>,
}

enum DirectSeriesWorkerEvent {
    Batch {
        source_id: usize,
        batch: RecordBatch,
    },
    Done {
        source_id: usize,
        metrics: RecordBatchMetrics,
    },
}

struct DirectSeriesWorker {
    commands: mpsc::SyncSender<usize>,
    events: mpsc::Receiver<Result<DirectSeriesWorkerEvent, RecordBatchError>>,
    handle: TaskHandle<()>,
}

struct DirectMergeSource {
    worker_index: usize,
    tag_set_index: usize,
    merge_identity: String,
    output_identity: String,
    fast_rank: usize,
    batch: Option<RecordBatch>,
    row: usize,
}

#[derive(Default)]
struct DirectMergeHeap {
    items: Vec<usize>,
}

impl DirectMergeHeap {
    fn init_with_mode(
        &mut self,
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) {
        let n = self.items.len();
        if n < 2 {
            return;
        }
        for index in (0..=(n / 2 - 1)).rev() {
            self.down(index, n, sources, time_index, aux_indices, fast);
        }
    }

    fn less(
        &self,
        left: usize,
        right: usize,
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) -> bool {
        Self::source_less(
            self.items[left],
            self.items[right],
            sources,
            time_index,
            aux_indices,
            fast,
        )
    }

    fn source_less(
        left: usize,
        right: usize,
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) -> bool {
        let left = &sources[left];
        let right = &sources[right];
        if fast {
            return left.fast_rank < right.fast_rank;
        }
        if left.merge_identity != right.merge_identity {
            return left.merge_identity < right.merge_identity;
        }

        let left_batch = left.batch.as_ref().expect("merge source has a batch");
        let right_batch = right.batch.as_ref().expect("merge source has a batch");
        let left_time = left_batch.column(time_index);
        let right_time = right_batch.column(time_index);
        let left_time = left_time
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("validated direct merge timestamp array")
            .value(left.row);
        let right_time = right_time
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("validated direct merge timestamp array")
            .value(right.row);
        if left_time != right_time {
            return left_time < right_time;
        }

        // Match FloatSortedMergeHeap exactly: only string aux values
        // participate in an equal-time tie break. Any non-string or null aux
        // makes the two heads equivalent for heap ordering.
        for index in aux_indices {
            let Some(left_values) = left_batch
                .column(*index)
                .as_any()
                .downcast_ref::<StringArray>()
            else {
                return false;
            };
            let Some(right_values) = right_batch
                .column(*index)
                .as_any()
                .downcast_ref::<StringArray>()
            else {
                return false;
            };
            if left_values.is_null(left.row) || right_values.is_null(right.row) {
                return false;
            }
            match left_values
                .value(left.row)
                .cmp(right_values.value(right.row))
            {
                Ordering::Less => return true,
                Ordering::Greater => return false,
                Ordering::Equal => {}
            }
        }
        false
    }

    fn down(
        &mut self,
        start: usize,
        n: usize,
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) {
        let mut index = start;
        loop {
            let left = 2 * index + 1;
            if left >= n {
                break;
            }
            let right = left + 1;
            let mut child = left;
            if right < n && self.less(right, left, sources, time_index, aux_indices, fast) {
                child = right;
            }
            if !self.less(child, index, sources, time_index, aux_indices, fast) {
                break;
            }
            self.items.swap(index, child);
            index = child;
        }
    }

    fn push(
        &mut self,
        source_id: usize,
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) {
        self.items.push(source_id);
        let mut child = self.items.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if !self.less(child, parent, sources, time_index, aux_indices, fast) {
                break;
            }
            self.items.swap(child, parent);
            child = parent;
        }
    }

    fn pop(
        &mut self,
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) -> Option<usize> {
        let last = self.items.len().checked_sub(1)?;
        self.items.swap(0, last);
        let item = self.items.pop();
        if !self.items.is_empty() {
            self.down(0, self.items.len(), sources, time_index, aux_indices, fast);
        }
        item
    }
}

struct DirectMergeTagSet {
    source_ids: Vec<usize>,
    heap: DirectMergeHeap,
    fast: bool,
    fast_rank: usize,
    physical_series_count: usize,
}

#[derive(Default)]
struct DirectTagSetHeap {
    items: Vec<usize>,
}

impl DirectTagSetHeap {
    fn current_source(tag_sets: &[DirectMergeTagSet], tag_set_index: usize) -> usize {
        tag_sets[tag_set_index].heap.items[0]
    }

    fn less(
        &self,
        left: usize,
        right: usize,
        tag_sets: &[DirectMergeTagSet],
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) -> bool {
        let left_tag_set = self.items[left];
        let right_tag_set = self.items[right];
        if fast {
            return tag_sets[left_tag_set].fast_rank < tag_sets[right_tag_set].fast_rank;
        }
        let left_source = Self::current_source(tag_sets, left_tag_set);
        let right_source = Self::current_source(tag_sets, right_tag_set);
        DirectMergeHeap::source_less(
            left_source,
            right_source,
            sources,
            time_index,
            aux_indices,
            false,
        )
    }

    fn init(
        &mut self,
        tag_sets: &[DirectMergeTagSet],
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) {
        let n = self.items.len();
        if n < 2 {
            return;
        }
        for index in (0..=(n / 2 - 1)).rev() {
            self.down(index, n, tag_sets, sources, time_index, aux_indices, fast);
        }
    }

    fn down(
        &mut self,
        start: usize,
        n: usize,
        tag_sets: &[DirectMergeTagSet],
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) {
        let mut index = start;
        loop {
            let left = 2 * index + 1;
            if left >= n {
                break;
            }
            let right = left + 1;
            let mut child = left;
            if right < n
                && self.less(
                    right,
                    left,
                    tag_sets,
                    sources,
                    time_index,
                    aux_indices,
                    fast,
                )
            {
                child = right;
            }
            if !self.less(
                child,
                index,
                tag_sets,
                sources,
                time_index,
                aux_indices,
                fast,
            ) {
                break;
            }
            self.items.swap(index, child);
            index = child;
        }
    }

    fn push(
        &mut self,
        tag_set_index: usize,
        tag_sets: &[DirectMergeTagSet],
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) {
        self.items.push(tag_set_index);
        let mut child = self.items.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if !self.less(
                child,
                parent,
                tag_sets,
                sources,
                time_index,
                aux_indices,
                fast,
            ) {
                break;
            }
            self.items.swap(child, parent);
            child = parent;
        }
    }

    fn pop(
        &mut self,
        tag_sets: &[DirectMergeTagSet],
        sources: &[DirectMergeSource],
        time_index: usize,
        aux_indices: &[usize],
        fast: bool,
    ) -> Option<usize> {
        let last = self.items.len().checked_sub(1)?;
        self.items.swap(0, last);
        let item = self.items.pop();
        if !self.items.is_empty() {
            self.down(
                0,
                self.items.len(),
                tag_sets,
                sources,
                time_index,
                aux_indices,
                fast,
            );
        }
        item
    }
}

enum MultiSeriesDirectEvent {
    Batch(RecordBatch, RecordBatchMetrics),
    Finished(RecordBatchMetrics),
}

struct MultiSeriesDirectSource {
    workers: Vec<DirectSeriesWorker>,
    sources: Vec<DirectMergeSource>,
    tag_sets: Vec<DirectMergeTagSet>,
    outer_heap: DirectTagSetHeap,
    outer_fast: bool,
    initialized: bool,
    builder: BatchBuilder,
    batch_rows: usize,
    max_batch_bytes: usize,
    time_index: usize,
    aux_indices: Vec<usize>,
    multi_field: bool,
    pending_metrics: RecordBatchMetrics,
    cancellation: CancellationToken,
    request_cancellation: Option<crate::query::storage_ir::ScanCancellation>,
    #[cfg(feature = "slow-trace")]
    profile: Option<Arc<StorageScanProfile>>,
}

impl MultiSeriesDirectSource {
    fn try_new(
        request: RecordBatchRequest,
        scan_set: DirectTsmScanSet,
        lease: ReefsDbOperationLease,
    ) -> Result<Self, RecordBatchError> {
        let DirectTsmScanSet { tag_sets, executor } = scan_set;
        let mut inputs = Vec::new();
        let mut sources = Vec::new();
        let mut merge_tag_sets = Vec::with_capacity(tag_sets.len());
        for (tag_set_index, tag_set) in tag_sets.into_iter().enumerate() {
            let mut source_ids = Vec::with_capacity(tag_set.series.len());
            for scan in tag_set.series {
                let source_id = sources.len();
                source_ids.push(source_id);
                sources.push(DirectMergeSource {
                    worker_index: usize::MAX,
                    tag_set_index,
                    merge_identity: scan.merge_identity.clone(),
                    output_identity: scan.output_identity.clone(),
                    fast_rank: usize::MAX,
                    batch: None,
                    row: 0,
                });
                inputs.push(DirectSeriesInput { source_id, scan });
            }
            merge_tag_sets.push(DirectMergeTagSet {
                physical_series_count: source_ids.len(),
                source_ids,
                heap: DirectMergeHeap::default(),
                fast: false,
                fast_rank: usize::MAX,
            });
        }
        if inputs.is_empty() {
            return Err(RecordBatchError::invalid(
                "multi-series direct scan has no physical series",
            ));
        }
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &request.scan.execution.profile {
            profile.record_direct_merge_inputs(inputs.len() as u64);
        }

        let budget = executor.storage_query_cpu_budget();
        let worker_limit = inputs.len().min(budget.capacity()).max(1);
        let request_cancellation = request.scan.cancellation.clone();
        let first_permit = match budget.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                let request_cancellation = request_cancellation
                    .as_ref()
                    .map(|cancellation| cancellation.token());
                futures::executor::block_on(async {
                    if let Some(request_cancellation) = request_cancellation {
                        tokio::select! {
                            permit = budget.acquire() => Ok(permit),
                            _ = request_cancellation.cancelled() => {
                                Err(RecordBatchError::storage(std::io::Error::new(
                                    std::io::ErrorKind::Interrupted,
                                    "storage scan cancelled while waiting for CPU admission",
                                )))
                            }
                        }
                    } else {
                        Ok(budget.acquire().await)
                    }
                })?
            }
        };
        let mut permits = Vec::with_capacity(worker_limit);
        permits.push(first_permit);
        while permits.len() < worker_limit {
            let Ok(permit) = budget.try_acquire() else {
                break;
            };
            permits.push(permit);
        }

        let partitions = Self::partition_series(inputs, permits.len());
        let cancellation = CancellationToken::new();
        let builder = BatchBuilder::try_new(&request)?;
        let time_index = request
            .schema
            .index_of(TIME_COLUMN_NAME)
            .map_err(RecordBatchError::from)?;
        let aux_indices = request
            .scan
            .projection
            .returned_columns
            .iter()
            .skip(usize::from(
                request.scan.projection.primary_column.is_some(),
            ))
            .map(|column| {
                request
                    .schema
                    .index_of(&column.name)
                    .map_err(RecordBatchError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut source = Self {
            workers: Vec::with_capacity(partitions.len()),
            sources,
            tag_sets: merge_tag_sets,
            outer_heap: DirectTagSetHeap::default(),
            outer_fast: false,
            initialized: false,
            builder,
            batch_rows: request.batch_rows,
            max_batch_bytes: request.max_batch_bytes,
            time_index,
            aux_indices,
            multi_field: request.scan.projection.returned_columns.len()
                + request.scan.projection.row_domain_columns.len()
                > 1,
            pending_metrics: RecordBatchMetrics::default(),
            cancellation: cancellation.clone(),
            request_cancellation,
            #[cfg(feature = "slow-trace")]
            profile: request.scan.execution.profile.clone(),
        };

        for (worker_index, (partition, permit)) in partitions.into_iter().zip(permits).enumerate() {
            for input in &partition.series {
                source.sources[input.source_id].worker_index = worker_index;
            }
            let (commands, command_receiver) = mpsc::sync_channel(1);
            let (event_sender, events) = mpsc::sync_channel(1);
            #[cfg(feature = "slow-trace")]
            let worker_request = {
                let mut worker_request = request.clone();
                // DirectTsmScan owns the physical decode profile. Suppress
                // intermediate Arrow-batch accounting; only merged output is final.
                worker_request.scan.execution.profile = None;
                worker_request
            };
            #[cfg(not(feature = "slow-trace"))]
            let worker_request = request.clone();
            let worker_lease = lease.clone();
            let worker_cancellation = cancellation.clone();
            let worker_budget = Arc::clone(&budget);
            let handle = executor.spawn_storage_query_cpu(move || {
                Self::execute_partition(
                    partition,
                    worker_request,
                    worker_lease,
                    worker_cancellation,
                    command_receiver,
                    event_sender,
                    worker_budget,
                    permit,
                );
            });
            match handle {
                Ok(handle) => source.workers.push(DirectSeriesWorker {
                    commands,
                    events,
                    handle,
                }),
                Err(error) => {
                    source.close();
                    return Err(RecordBatchError::storage(error));
                }
            }
        }
        Ok(source)
    }

    fn partition_series(
        series: Vec<DirectSeriesInput>,
        worker_count: usize,
    ) -> Vec<DirectSeriesPartition> {
        let worker_count = series.len().min(worker_count.max(1));
        let series_per_worker = series.len() / worker_count;
        let mut series = series.into_iter();
        (0..worker_count)
            .map(|index| {
                let series = if index + 1 < worker_count {
                    series.by_ref().take(series_per_worker).collect()
                } else {
                    series.by_ref().collect()
                };
                DirectSeriesPartition { series }
            })
            .collect()
    }

    fn execute_partition(
        partition: DirectSeriesPartition,
        request: RecordBatchRequest,
        lease: ReefsDbOperationLease,
        cancellation: CancellationToken,
        commands: mpsc::Receiver<usize>,
        events: mpsc::SyncSender<Result<DirectSeriesWorkerEvent, RecordBatchError>>,
        budget: Arc<StorageQueryCpuBudget>,
        initial_permit: StorageQueryCpuPermit,
    ) {
        let mut readers = Vec::with_capacity(partition.series.len());
        for input in partition.series {
            let mut scans = input.scan.scans;
            let reader = if request.scan.projection.returned_columns.len() == 1
                && request.scan.projection.row_domain_columns.is_empty()
            {
                let scan = scans
                    .pop()
                    .expect("single-field direct scan has one field stream");
                RecordBatchReader::try_new_direct(request.clone(), scan, lease.clone())
            } else {
                RecordBatchReader::try_new_multi_direct(request.clone(), scans, lease.clone())
            };
            match reader {
                Ok(reader) => readers.push((input.source_id, reader, false)),
                Err(error) => {
                    let _ = events.send(Err(error));
                    return;
                }
            }
        }

        let mut permit = Some(initial_permit);
        let mut remaining = readers.len();
        for index in 0..readers.len() {
            let event = Self::read_worker_source(
                &mut readers[index],
                &budget,
                &cancellation,
                request.scan.cancellation.as_ref(),
                &mut permit,
            );
            if matches!(event, Ok(DirectSeriesWorkerEvent::Done { .. })) {
                remaining -= 1;
            }
            if events.send(event).is_err() {
                return;
            }
        }

        while remaining > 0 {
            let Ok(source_id) = commands.recv() else {
                return;
            };
            let Some(index) = readers
                .iter()
                .position(|(candidate, _, done)| *candidate == source_id && !*done)
            else {
                let _ = events.send(Err(RecordBatchError::invalid(format!(
                    "direct merge requested inactive source {source_id}"
                ))));
                return;
            };
            let event = Self::read_worker_source(
                &mut readers[index],
                &budget,
                &cancellation,
                request.scan.cancellation.as_ref(),
                &mut permit,
            );
            if matches!(event, Ok(DirectSeriesWorkerEvent::Done { .. })) {
                remaining -= 1;
            }
            if events.send(event).is_err() {
                return;
            }
        }
    }

    fn read_worker_source(
        source: &mut (usize, RecordBatchReader, bool),
        budget: &Arc<StorageQueryCpuBudget>,
        cancellation: &CancellationToken,
        request_cancellation: Option<&crate::query::storage_ir::ScanCancellation>,
        permit: &mut Option<StorageQueryCpuPermit>,
    ) -> Result<DirectSeriesWorkerEvent, RecordBatchError> {
        let admitted = match permit.take() {
            Some(permit) => permit,
            None => Self::acquire_worker_permit(budget, cancellation, request_cancellation)?,
        };
        let active = admitted.start();
        let batch = source.1.next();
        drop(active);
        if cancellation.is_cancelled()
            || request_cancellation.is_some_and(|cancellation| cancellation.is_cancelled())
        {
            return Err(Self::cancelled_error());
        }
        match batch {
            Some(Ok(batch)) => Ok(DirectSeriesWorkerEvent::Batch {
                source_id: source.0,
                batch,
            }),
            Some(Err(error)) => Err(error),
            None => {
                source.2 = true;
                let mut metrics = source.1.metrics();
                // These describe intermediate per-physical-series batches, not
                // the merged request output.
                metrics.batches_emitted = 0;
                metrics.rows_emitted = 0;
                metrics.multi_field_direct_batches = 0;
                metrics.estimated_bytes_emitted = 0;
                Ok(DirectSeriesWorkerEvent::Done {
                    source_id: source.0,
                    metrics,
                })
            }
        }
    }

    fn acquire_worker_permit(
        budget: &Arc<StorageQueryCpuBudget>,
        cancellation: &CancellationToken,
        request_cancellation: Option<&crate::query::storage_ir::ScanCancellation>,
    ) -> Result<StorageQueryCpuPermit, RecordBatchError> {
        if cancellation.is_cancelled()
            || request_cancellation.is_some_and(|cancellation| cancellation.is_cancelled())
        {
            return Err(Self::cancelled_error());
        }
        if let Ok(permit) = budget.try_acquire() {
            return Ok(permit);
        }
        let cancellation = cancellation.clone();
        let request_cancellation = request_cancellation.map(|cancellation| cancellation.token());
        futures::executor::block_on(async {
            if let Some(request_cancellation) = request_cancellation {
                tokio::select! {
                    permit = budget.acquire() => Ok(permit),
                    _ = cancellation.cancelled() => Err(Self::cancelled_error()),
                    _ = request_cancellation.cancelled() => Err(Self::cancelled_error()),
                }
            } else {
                tokio::select! {
                    permit = budget.acquire() => Ok(permit),
                    _ = cancellation.cancelled() => Err(Self::cancelled_error()),
                }
            }
        })
    }

    fn cancelled_error() -> RecordBatchError {
        RecordBatchError::storage(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "storage scan cancelled",
        ))
    }

    fn receive_worker_event(
        &mut self,
        worker_index: usize,
    ) -> Result<DirectSeriesWorkerEvent, RecordBatchError> {
        self.workers[worker_index]
            .events
            .recv()
            .map_err(RecordBatchError::storage)?
    }

    fn apply_worker_event(
        &mut self,
        expected_source_id: usize,
        event: DirectSeriesWorkerEvent,
    ) -> Result<bool, RecordBatchError> {
        match event {
            DirectSeriesWorkerEvent::Batch { source_id, batch } => {
                if source_id != expected_source_id || batch.num_rows() == 0 {
                    return Err(RecordBatchError::invalid(format!(
                        "direct merge expected non-empty source {expected_source_id}, received source {source_id} with {} rows",
                        batch.num_rows()
                    )));
                }
                let source = &mut self.sources[source_id];
                source.batch = Some(batch);
                source.row = 0;
                Ok(true)
            }
            DirectSeriesWorkerEvent::Done { source_id, metrics } => {
                if source_id != expected_source_id {
                    return Err(RecordBatchError::invalid(format!(
                        "direct merge expected source {expected_source_id}, completed source {source_id}"
                    )));
                }
                let source = &mut self.sources[source_id];
                source.batch = None;
                self.pending_metrics.merge(metrics);
                Ok(false)
            }
        }
    }

    fn initialize(&mut self) -> Result<(), RecordBatchError> {
        if self.initialized {
            return Ok(());
        }
        for source_id in 0..self.sources.len() {
            let worker_index = self.sources[source_id].worker_index;
            let event = self.receive_worker_event(worker_index)?;
            self.apply_worker_event(source_id, event)?;
        }

        // Reproduce the generic iterator's two-level sorted merge: one heap
        // per tag set, followed by an outer tag-set heap. Its detect_fast path
        // sorts unique projected tag identities and drains them in that order.
        for tag_set in &mut self.tag_sets {
            tag_set
                .source_ids
                .retain(|source_id| self.sources[*source_id].batch.is_some());
            tag_set.source_ids.sort_by(|left, right| {
                self.sources[*left]
                    .output_identity
                    .cmp(&self.sources[*right].output_identity)
            });
            tag_set.fast = !tag_set.source_ids.windows(2).any(|pair| {
                self.sources[pair[0]].output_identity == self.sources[pair[1]].output_identity
            });
            for (rank, source_id) in tag_set.source_ids.iter().copied().enumerate() {
                self.sources[source_id].fast_rank = rank;
            }
            tag_set.heap.items.clone_from(&tag_set.source_ids);
            tag_set.heap.init_with_mode(
                &self.sources,
                self.time_index,
                self.aux_indices.as_slice(),
                tag_set.fast,
            );
        }

        let mut active_tag_sets = self
            .tag_sets
            .iter()
            .enumerate()
            .filter_map(|(index, tag_set)| (!tag_set.heap.items.is_empty()).then_some(index))
            .collect::<Vec<_>>();
        let outer_fast_candidate = active_tag_sets
            .iter()
            .all(|index| self.tag_sets[*index].physical_series_count == 1);
        if outer_fast_candidate {
            active_tag_sets.sort_by(|left, right| {
                let left_source = DirectTagSetHeap::current_source(&self.tag_sets, *left);
                let right_source = DirectTagSetHeap::current_source(&self.tag_sets, *right);
                self.sources[left_source]
                    .output_identity
                    .cmp(&self.sources[right_source].output_identity)
            });
        }
        self.outer_fast = outer_fast_candidate
            && !active_tag_sets.windows(2).any(|pair| {
                let left_source = DirectTagSetHeap::current_source(&self.tag_sets, pair[0]);
                let right_source = DirectTagSetHeap::current_source(&self.tag_sets, pair[1]);
                self.sources[left_source].output_identity
                    == self.sources[right_source].output_identity
            });
        for (rank, tag_set_index) in active_tag_sets.iter().copied().enumerate() {
            self.tag_sets[tag_set_index].fast_rank = rank;
        }
        self.outer_heap.items = active_tag_sets;
        self.outer_heap.init(
            &self.tag_sets,
            &self.sources,
            self.time_index,
            self.aux_indices.as_slice(),
            self.outer_fast,
        );
        self.initialized = true;
        Ok(())
    }

    fn advance_source(&mut self, source_id: usize) -> Result<bool, RecordBatchError> {
        let exhausted = {
            let source = &mut self.sources[source_id];
            source.row += 1;
            source.row
                == source
                    .batch
                    .as_ref()
                    .expect("active direct merge source has a batch")
                    .num_rows()
        };
        if !exhausted {
            return Ok(true);
        }

        self.sources[source_id].batch = None;
        let worker_index = self.sources[source_id].worker_index;
        self.workers[worker_index]
            .commands
            .send(source_id)
            .map_err(RecordBatchError::storage)?;
        let event = self.receive_worker_event(worker_index)?;
        self.apply_worker_event(source_id, event)
    }

    fn finish_batch(&mut self) -> Result<RecordBatch, RecordBatchError> {
        let started = Instant::now();
        let (batch, estimated_bytes) = self.builder.finish()?;
        let elapsed = started.elapsed();
        self.pending_metrics.batches_emitted =
            self.pending_metrics.batches_emitted.saturating_add(1);
        self.pending_metrics.rows_emitted = self
            .pending_metrics
            .rows_emitted
            .saturating_add(batch.num_rows() as u64);
        if self.multi_field {
            self.pending_metrics.multi_field_direct_batches = self
                .pending_metrics
                .multi_field_direct_batches
                .saturating_add(1);
        }
        self.pending_metrics.estimated_bytes_emitted = self
            .pending_metrics
            .estimated_bytes_emitted
            .saturating_add(estimated_bytes as u64);
        self.pending_metrics.batch_build_nanos = self
            .pending_metrics
            .batch_build_nanos
            .saturating_add(elapsed.as_nanos().min(u128::from(u64::MAX)) as u64);
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &self.profile {
            profile.record_arrow_batch(
                batch.num_rows() as u64,
                estimated_bytes as u64,
                0,
                elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
            );
        }
        Ok(batch)
    }

    fn next_event(&mut self) -> Result<MultiSeriesDirectEvent, RecordBatchError> {
        if self
            .request_cancellation
            .as_ref()
            .is_some_and(|cancellation| cancellation.is_cancelled())
        {
            return Err(Self::cancelled_error());
        }
        self.initialize()?;
        #[cfg(feature = "slow-trace")]
        let mut merge_nanos = 0_u128;
        let mut merged_rows = 0_u64;
        while self.builder.rows < self.batch_rows
            && self.builder.estimated_bytes < self.max_batch_bytes
        {
            #[cfg(feature = "slow-trace")]
            let merge_step_started = Instant::now();
            let Some(tag_set_index) = self.outer_heap.pop(
                &self.tag_sets,
                &self.sources,
                self.time_index,
                self.aux_indices.as_slice(),
                self.outer_fast,
            ) else {
                break;
            };
            let tag_set_fast = self.tag_sets[tag_set_index].fast;
            let source_id = self.tag_sets[tag_set_index]
                .heap
                .pop(
                    &self.sources,
                    self.time_index,
                    self.aux_indices.as_slice(),
                    tag_set_fast,
                )
                .expect("active direct tag set has a physical source");
            {
                let source = &self.sources[source_id];
                debug_assert_eq!(source.tag_set_index, tag_set_index);
                self.builder.append_batch_row(
                    source
                        .batch
                        .as_ref()
                        .expect("active direct merge source has a batch"),
                    source.row,
                )?;
            }
            merged_rows = merged_rows.saturating_add(1);
            #[cfg(feature = "slow-trace")]
            {
                merge_nanos = merge_nanos.saturating_add(merge_step_started.elapsed().as_nanos());
            }
            if self.advance_source(source_id)? {
                #[cfg(feature = "slow-trace")]
                let heap_update_started = Instant::now();
                let tag_set = &mut self.tag_sets[tag_set_index];
                tag_set.heap.push(
                    source_id,
                    &self.sources,
                    self.time_index,
                    self.aux_indices.as_slice(),
                    tag_set.fast,
                );
                #[cfg(feature = "slow-trace")]
                {
                    merge_nanos =
                        merge_nanos.saturating_add(heap_update_started.elapsed().as_nanos());
                }
            }
            if !self.tag_sets[tag_set_index].heap.items.is_empty() {
                #[cfg(feature = "slow-trace")]
                let heap_update_started = Instant::now();
                self.outer_heap.push(
                    tag_set_index,
                    &self.tag_sets,
                    &self.sources,
                    self.time_index,
                    self.aux_indices.as_slice(),
                    self.outer_fast,
                );
                #[cfg(feature = "slow-trace")]
                {
                    merge_nanos =
                        merge_nanos.saturating_add(heap_update_started.elapsed().as_nanos());
                }
            }
        }
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &self.profile {
            profile.record_direct_series_merge(
                merged_rows,
                (merge_nanos / 1_000).min(u128::from(u64::MAX)) as u64,
            );
        }
        if self.builder.rows > 0 {
            let batch = self.finish_batch()?;
            return Ok(MultiSeriesDirectEvent::Batch(
                batch,
                std::mem::take(&mut self.pending_metrics),
            ));
        }
        Ok(MultiSeriesDirectEvent::Finished(std::mem::take(
            &mut self.pending_metrics,
        )))
    }

    fn close(&mut self) {
        self.cancellation.cancel();
        let workers = std::mem::take(&mut self.workers);
        let handles = workers
            .into_iter()
            .map(|worker| {
                drop(worker.commands);
                drop(worker.events);
                worker.handle
            })
            .collect::<Vec<_>>();
        for handle in handles {
            let _ = futures::executor::block_on(handle);
        }
    }
}

enum BatchSource {
    Points(PointSource),
    Direct(DirectSource),
    MultiDirect(MultiDirectSource),
    MultiSeriesDirect(MultiSeriesDirectSource),
    Empty,
}

impl BatchSource {
    fn close(&mut self) {
        match self {
            Self::Points(points) => points.close(),
            Self::MultiSeriesDirect(source) => source.close(),
            Self::Direct(_) | Self::MultiDirect(_) | Self::Empty => {}
        }
        *self = Self::Empty;
    }
}

impl PointSource {
    fn next(&mut self) -> Option<FloatPoint> {
        match self {
            Self::Storage(iterator) => iterator.next(),
            Self::Empty => None,
            #[cfg(test)]
            Self::Test(points) => points.pop_front(),
        }
    }

    fn close(&mut self) {
        if let Self::Storage(iterator) = self {
            iterator.close();
        }
        *self = Self::Empty;
    }
}

/// Streaming RecordBatch iterator backed by the authoritative logical point stream.
pub struct RecordBatchReader {
    source: BatchSource,
    builder: BatchBuilder,
    batch_rows: usize,
    max_batch_bytes: usize,
    metrics: RecordBatchMetrics,
    finished: bool,
    #[cfg(feature = "slow-trace")]
    profile: Option<Arc<StorageScanProfile>>,
    #[cfg(feature = "slow-trace")]
    arrow_append_us: u64,
}

impl RecordBatchReader {
    pub fn try_new(
        request: RecordBatchRequest,
        iterator: Option<LeasedQueryIterator>,
    ) -> Result<Self, RecordBatchError> {
        #[cfg(feature = "slow-trace")]
        let profile = request.scan.execution.profile.clone();
        let builder = BatchBuilder::try_new(&request)?;
        let source = match iterator {
            Some(iterator) => BatchSource::Points(PointSource::Storage(downcast_float(iterator)?)),
            None => BatchSource::Empty,
        };
        Ok(Self {
            source,
            builder,
            batch_rows: request.batch_rows,
            max_batch_bytes: request.max_batch_bytes,
            metrics: RecordBatchMetrics::default(),
            finished: false,
            #[cfg(feature = "slow-trace")]
            profile,
            #[cfg(feature = "slow-trace")]
            arrow_append_us: 0,
        })
    }

    pub(crate) fn try_new_direct(
        request: RecordBatchRequest,
        scan: DirectTsmScan,
        lease: ReefsDbOperationLease,
    ) -> Result<Self, RecordBatchError> {
        #[cfg(feature = "slow-trace")]
        let profile = request.scan.execution.profile.clone();
        let builder = BatchBuilder::try_new(&request)?;
        Ok(Self {
            source: BatchSource::Direct(DirectSource {
                scan,
                pending: None,
                offset: 0,
                _lease: lease,
            }),
            builder,
            batch_rows: request.batch_rows,
            max_batch_bytes: request.max_batch_bytes,
            metrics: RecordBatchMetrics::default(),
            finished: false,
            #[cfg(feature = "slow-trace")]
            profile,
            #[cfg(feature = "slow-trace")]
            arrow_append_us: 0,
        })
    }

    pub(crate) fn try_new_multi_direct(
        request: RecordBatchRequest,
        scans: Vec<DirectTsmScan>,
        lease: ReefsDbOperationLease,
    ) -> Result<Self, RecordBatchError> {
        #[cfg(feature = "slow-trace")]
        let profile = request.scan.execution.profile.clone();
        let builder = BatchBuilder::try_new(&request)?;
        let expected_streams = request
            .scan
            .projection
            .returned_columns
            .len()
            .saturating_add(request.scan.projection.row_domain_columns.len());
        if scans.len() != expected_streams {
            return Err(RecordBatchError::invalid(format!(
                "multi-field direct scan has {} streams, expected {expected_streams}",
                scans.len()
            )));
        }
        let tags = scans
            .first()
            .ok_or_else(|| RecordBatchError::invalid("multi-field direct scan has no streams"))?
            .tags();
        let stream_count = scans.len() as u64;
        Ok(Self {
            source: BatchSource::MultiDirect(MultiDirectSource {
                streams: scans.into_iter().map(DirectFieldStream::new).collect(),
                tags,
                _lease: lease,
            }),
            builder,
            batch_rows: request.batch_rows,
            max_batch_bytes: request.max_batch_bytes,
            metrics: RecordBatchMetrics {
                multi_field_streams: stream_count,
                ..Default::default()
            },
            finished: false,
            #[cfg(feature = "slow-trace")]
            profile,
            #[cfg(feature = "slow-trace")]
            arrow_append_us: 0,
        })
    }

    pub(crate) fn try_new_multi_series_direct(
        request: RecordBatchRequest,
        scan_set: DirectTsmScanSet,
        lease: ReefsDbOperationLease,
    ) -> Result<Self, RecordBatchError> {
        #[cfg(feature = "slow-trace")]
        let profile = request.scan.execution.profile.clone();
        let builder = BatchBuilder::try_new(&request)?;
        let source = MultiSeriesDirectSource::try_new(request.clone(), scan_set, lease)?;
        Ok(Self {
            source: BatchSource::MultiSeriesDirect(source),
            builder,
            batch_rows: request.batch_rows,
            max_batch_bytes: request.max_batch_bytes,
            metrics: RecordBatchMetrics::default(),
            finished: false,
            #[cfg(feature = "slow-trace")]
            profile,
            #[cfg(feature = "slow-trace")]
            arrow_append_us: 0,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.builder.schema
    }

    pub fn metrics(&self) -> RecordBatchMetrics {
        self.metrics
    }

    fn finish_batch(&mut self, multi_field_direct: bool) -> Result<RecordBatch, RecordBatchError> {
        let started = Instant::now();
        let (batch, estimated_bytes) = self.builder.finish()?;
        #[cfg(feature = "slow-trace")]
        let finish_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.metrics.batches_emitted = self.metrics.batches_emitted.saturating_add(1);
        if multi_field_direct {
            self.metrics.multi_field_direct_batches =
                self.metrics.multi_field_direct_batches.saturating_add(1);
        }
        self.metrics.rows_emitted = self
            .metrics
            .rows_emitted
            .saturating_add(batch.num_rows() as u64);
        self.metrics.estimated_bytes_emitted = self
            .metrics
            .estimated_bytes_emitted
            .saturating_add(estimated_bytes as u64);
        self.metrics.batch_build_nanos = self
            .metrics
            .batch_build_nanos
            .saturating_add(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &self.profile {
            profile.record_arrow_batch(
                batch.num_rows() as u64,
                estimated_bytes as u64,
                std::mem::take(&mut self.arrow_append_us),
                finish_us,
            );
        }
        Ok(batch)
    }

    fn fail(&mut self, error: RecordBatchError) -> Option<Result<RecordBatch, RecordBatchError>> {
        self.finished = true;
        self.source.close();
        Some(Err(error))
    }

    #[cfg(test)]
    fn from_points(
        request: RecordBatchRequest,
        points: Vec<FloatPoint>,
    ) -> Result<Self, RecordBatchError> {
        let builder = BatchBuilder::try_new(&request)?;
        Ok(Self {
            source: BatchSource::Points(PointSource::Test(points.into())),
            builder,
            batch_rows: request.batch_rows,
            max_batch_bytes: request.max_batch_bytes,
            metrics: RecordBatchMetrics::default(),
            finished: false,
            #[cfg(feature = "slow-trace")]
            profile: request.scan.execution.profile.clone(),
            #[cfg(feature = "slow-trace")]
            arrow_append_us: 0,
        })
    }
}

impl Iterator for RecordBatchReader {
    type Item = Result<RecordBatch, RecordBatchError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if let BatchSource::MultiSeriesDirect(source) = &mut self.source {
                match source.next_event() {
                    Ok(MultiSeriesDirectEvent::Batch(batch, metrics)) => {
                        self.metrics.merge(metrics);
                        return Some(Ok(batch));
                    }
                    Ok(MultiSeriesDirectEvent::Finished(metrics)) => {
                        self.metrics.merge(metrics);
                        self.finished = true;
                        self.source.close();
                        return None;
                    }
                    Err(error) => return self.fail(error),
                }
            }

            if let BatchSource::Direct(source) = &mut self.source {
                match source.append(&mut self.builder, self.batch_rows, self.max_batch_bytes) {
                    Ok(Some(appended)) => {
                        #[cfg(feature = "slow-trace")]
                        {
                            self.arrow_append_us = self
                                .arrow_append_us
                                .saturating_add(appended.arrow_append_us);
                        }
                        self.metrics.direct_batch_rows = self
                            .metrics
                            .direct_batch_rows
                            .saturating_add(appended.rows as u64);
                        if appended.new_block {
                            self.metrics.direct_blocks =
                                self.metrics.direct_blocks.saturating_add(1);
                            if appended.partial_block {
                                self.metrics.partial_direct_blocks =
                                    self.metrics.partial_direct_blocks.saturating_add(1);
                            }
                            if appended.timestamp_only_block {
                                self.metrics.timestamp_only_blocks =
                                    self.metrics.timestamp_only_blocks.saturating_add(1);
                                self.metrics.timestamp_only_points = self
                                    .metrics
                                    .timestamp_only_points
                                    .saturating_add(appended.decoded_points as u64);
                            } else {
                                self.metrics.value_decoded_blocks =
                                    self.metrics.value_decoded_blocks.saturating_add(1);
                                self.metrics.value_decoded_points = self
                                    .metrics
                                    .value_decoded_points
                                    .saturating_add(appended.decoded_points as u64);
                            }
                        }
                        if self.builder.rows >= self.batch_rows
                            || self.builder.estimated_bytes >= self.max_batch_bytes
                        {
                            return Some(self.finish_batch(false));
                        }
                        continue;
                    }
                    Ok(None) => {
                        self.finished = true;
                        self.source.close();
                        if self.builder.rows == 0 {
                            return None;
                        }
                        return Some(self.finish_batch(false));
                    }
                    Err(error) => return self.fail(error),
                }
            }

            if let BatchSource::MultiDirect(source) = &mut self.source {
                match source.append(&mut self.builder) {
                    Ok(Some(appended)) => {
                        #[cfg(feature = "slow-trace")]
                        {
                            self.arrow_append_us = self
                                .arrow_append_us
                                .saturating_add(appended.arrow_append_us);
                            if let Some(profile) = &self.profile {
                                profile.record_row_domain_merge(appended.row_domain_merge_us);
                            }
                        }
                        self.metrics.direct_batch_rows =
                            self.metrics.direct_batch_rows.saturating_add(1);
                        self.metrics.multi_field_direct_rows =
                            self.metrics.multi_field_direct_rows.saturating_add(1);
                        self.metrics.alignment_steps =
                            self.metrics.alignment_steps.saturating_add(1);
                        self.metrics.direct_blocks = self
                            .metrics
                            .direct_blocks
                            .saturating_add(appended.direct_blocks as u64);
                        self.metrics.partial_direct_blocks = self
                            .metrics
                            .partial_direct_blocks
                            .saturating_add(appended.partial_direct_blocks as u64);
                        self.metrics.value_decoded_blocks = self
                            .metrics
                            .value_decoded_blocks
                            .saturating_add(appended.value_decoded_blocks as u64);
                        self.metrics.timestamp_only_blocks = self
                            .metrics
                            .timestamp_only_blocks
                            .saturating_add(appended.timestamp_only_blocks as u64);
                        self.metrics.value_decoded_points = self
                            .metrics
                            .value_decoded_points
                            .saturating_add(appended.value_decoded_points as u64);
                        self.metrics.timestamp_only_points = self
                            .metrics
                            .timestamp_only_points
                            .saturating_add(appended.timestamp_only_points as u64);
                        self.metrics.null_values_emitted = self
                            .metrics
                            .null_values_emitted
                            .saturating_add(appended.null_values as u64);
                        if self.builder.rows >= self.batch_rows
                            || self.builder.estimated_bytes >= self.max_batch_bytes
                        {
                            return Some(self.finish_batch(true));
                        }
                        continue;
                    }
                    Ok(None) => {
                        self.finished = true;
                        self.source.close();
                        if self.builder.rows == 0 {
                            return None;
                        }
                        return Some(self.finish_batch(true));
                    }
                    Err(error) => return self.fail(error),
                }
            }

            #[cfg(feature = "slow-trace")]
            let iterator_next_started = Instant::now();
            let point = match &mut self.source {
                BatchSource::Points(source) => source.next(),
                BatchSource::Empty => None,
                BatchSource::Direct(_)
                | BatchSource::MultiDirect(_)
                | BatchSource::MultiSeriesDirect(_) => unreachable!(),
            };
            #[cfg(feature = "slow-trace")]
            if let Some(profile) = &self.profile {
                profile.record_iterator_next(
                    iterator_next_started
                        .elapsed()
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64,
                );
            }
            match point {
                Some(point) => {
                    #[cfg(feature = "slow-trace")]
                    let append_started = Instant::now();
                    if let Err(error) = self.builder.append(&point) {
                        return self.fail(error);
                    }
                    #[cfg(feature = "slow-trace")]
                    {
                        self.arrow_append_us = self.arrow_append_us.saturating_add(
                            append_started
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                    }
                    self.metrics.iterator_fallback_rows =
                        self.metrics.iterator_fallback_rows.saturating_add(1);
                    if self.builder.rows >= self.batch_rows
                        || self.builder.estimated_bytes >= self.max_batch_bytes
                    {
                        return Some(self.finish_batch(false));
                    }
                }
                None => {
                    self.finished = true;
                    self.source.close();
                    if self.builder.rows == 0 {
                        return None;
                    }
                    return Some(self.finish_batch(false));
                }
            }
        }
    }
}

impl Drop for RecordBatchReader {
    fn drop(&mut self) {
        self.source.close();
    }
}

macro_rules! try_downcast_float {
    ($iterator:expr, $ty:ty) => {
        match $iterator.try_downcast_float::<$ty>() {
            Ok(iterator) => return Ok(iterator),
            Err(iterator) => iterator,
        }
    };
}

fn downcast_float(iterator: LeasedQueryIterator) -> Result<LeasedFloatIterator, RecordBatchError> {
    let iterator = try_downcast_float!(iterator, query_iterator::FloatMergeIter);
    let iterator = try_downcast_float!(iterator, query_iterator::FloatInterruptIter);
    let iterator = try_downcast_float!(iterator, query_iterator::FloatLimitIter);
    let iterator = try_downcast_float!(iterator, query_iterator::FloatSeriesLimitIter);
    let iterator = try_downcast_float!(iterator, query_iterator::FloatSortedMergeIter);
    let iterator = try_downcast_float!(iterator, active_cursor::FloatIter);
    let iterator = try_downcast_float!(iterator, active_cursor::FloatLimitIter);
    drop(iterator);
    Err(RecordBatchError::invalid(
        "unknown aux-only FloatIterator concrete type",
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
        UInt64Array,
    };
    use arrow_schema::{Field, Schema};

    use super::*;
    use crate::query::{
        query_point::Tags,
        storage_ir::{
            ColumnRef, MeasurementRef, OutputOrder, SeriesProjection, StorageProjection, TimeOrder,
            TimeRange,
        },
    };

    #[derive(Clone, Debug, PartialEq)]
    struct Row {
        time: i64,
        host: Option<String>,
        float: Option<f64>,
        integer: Option<i64>,
        unsigned: Option<u64>,
        boolean: Option<bool>,
        string: Option<String>,
    }

    fn typed_scan() -> StorageScan {
        StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: vec![
                    ColumnRef::new("float", ColumnKind::Field(Some(FLOAT))),
                    ColumnRef::new("integer", ColumnKind::Field(Some(INTEGER))),
                    ColumnRef::new("unsigned", ColumnKind::Field(Some(UNSIGNED))),
                    ColumnRef::new("boolean", ColumnKind::Field(Some(BOOL))),
                    ColumnRef::new("string", ColumnKind::Field(Some(STRING))),
                ],
                row_domain_columns: Vec::new(),
                series: SeriesProjection::new(vec!["host".into()], vec!["host".into()]),
            },
            time_range: TimeRange::all(),
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        }
    }

    fn typed_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(TIME_COLUMN_NAME, time_arrow_type(), false),
            Field::new("host", DataType::Utf8, true),
            Field::new("float", DataType::Float64, true),
            Field::new("integer", DataType::Int64, true),
            Field::new("unsigned", DataType::UInt64, true),
            Field::new("boolean", DataType::Boolean, true),
            Field::new("string", DataType::Utf8, true),
        ]))
    }

    fn request(batch_rows: usize, max_batch_bytes: usize) -> RecordBatchRequest {
        RecordBatchRequest::new(typed_scan(), typed_schema(), batch_rows, max_batch_bytes)
    }

    fn point(time: i64, host: Option<&str>, aux: Vec<Val>) -> FloatPoint {
        let tags = host.map_or_else(Tags::default, |host| {
            Tags::new(HashMap::from([("host".to_string(), host.to_string())]))
        });
        FloatPoint {
            name: "cpu".into(),
            tags,
            time,
            value: 0.0,
            aux,
            aggregated: 0,
            none: false,
        }
    }

    fn row_from_point(point: &FloatPoint) -> Row {
        Row {
            time: point.time,
            host: point.tags.value("host"),
            float: match &point.aux[0] {
                Val::F64(value) => Some(*value),
                Val::Empty(_) => None,
                value => panic!("unexpected float value {value:?}"),
            },
            integer: match &point.aux[1] {
                Val::I64(value) => Some(*value),
                Val::Empty(_) => None,
                value => panic!("unexpected integer value {value:?}"),
            },
            unsigned: match &point.aux[2] {
                Val::U64(value) => Some(*value),
                Val::Empty(_) => None,
                value => panic!("unexpected unsigned value {value:?}"),
            },
            boolean: match &point.aux[3] {
                Val::Bool(value) => Some(*value),
                Val::Empty(_) => None,
                value => panic!("unexpected boolean value {value:?}"),
            },
            string: match &point.aux[4] {
                Val::String(value) => Some(value.clone()),
                Val::Empty(_) => None,
                value => panic!("unexpected string value {value:?}"),
            },
        }
    }

    fn value<T: Copy>(array: &impl Array, index: usize, get: impl FnOnce() -> T) -> Option<T> {
        (!array.is_null(index)).then(get)
    }

    fn flatten(batches: &[RecordBatch]) -> Vec<Row> {
        let mut rows = Vec::new();
        for batch in batches {
            let time = batch
                .column_by_name(TIME_COLUMN_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let host = batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let float = batch
                .column_by_name("float")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let integer = batch
                .column_by_name("integer")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let unsigned = batch
                .column_by_name("unsigned")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let boolean = batch
                .column_by_name("boolean")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let string = batch
                .column_by_name("string")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for index in 0..batch.num_rows() {
                rows.push(Row {
                    time: time.value(index),
                    host: (!host.is_null(index)).then(|| host.value(index).to_string()),
                    float: value(float, index, || float.value(index)),
                    integer: value(integer, index, || integer.value(index)),
                    unsigned: value(unsigned, index, || unsigned.value(index)),
                    boolean: value(boolean, index, || boolean.value(index)),
                    string: (!string.is_null(index)).then(|| string.value(index).to_string()),
                });
            }
        }
        rows
    }

    #[test]
    fn record_batch_reader_builds_all_types_and_nulls() {
        let points = vec![
            point(
                10,
                Some("a"),
                vec![
                    Val::F64(1.5),
                    Val::I64(-2),
                    Val::U64(3),
                    Val::Bool(true),
                    Val::String("x".into()),
                ],
            ),
            point(
                20,
                None,
                vec![
                    Val::Empty(None),
                    Val::Empty(None),
                    Val::Empty(None),
                    Val::Empty(None),
                    Val::Empty(None),
                ],
            ),
        ];
        let expected = points.iter().map(row_from_point).collect::<Vec<_>>();
        let mut reader = RecordBatchReader::from_points(request(1024, usize::MAX), points).unwrap();
        let batches = reader.by_ref().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(flatten(&batches), expected);
        assert_eq!(batches.len(), 1);
        assert_eq!(
            reader.metrics(),
            RecordBatchMetrics {
                batches_emitted: 1,
                rows_emitted: 2,
                iterator_fallback_rows: 2,
                direct_batch_rows: 0,
                direct_blocks: 0,
                partial_direct_blocks: 0,
                value_decoded_blocks: 0,
                timestamp_only_blocks: 0,
                value_decoded_points: 0,
                timestamp_only_points: 0,
                multi_field_direct_rows: 0,
                multi_field_direct_batches: 0,
                multi_field_streams: 0,
                alignment_steps: 0,
                null_values_emitted: 0,
                estimated_bytes_emitted: 43,
                batch_build_nanos: reader.metrics().batch_build_nanos,
            }
        );
    }

    #[test]
    fn record_batch_reader_honors_row_and_byte_bounds() {
        let points = (0..5)
            .map(|time| {
                point(
                    time,
                    Some("a"),
                    vec![
                        Val::F64(time as f64),
                        Val::I64(time),
                        Val::U64(time as u64),
                        Val::Bool(time % 2 == 0),
                        Val::String(time.to_string()),
                    ],
                )
            })
            .collect::<Vec<_>>();

        let row_batches = RecordBatchReader::from_points(request(2, usize::MAX), points.clone())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            row_batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );

        let byte_batches = RecordBatchReader::from_points(request(1024, 1), points)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(byte_batches.len(), 5);
        assert!(byte_batches.iter().all(|batch| batch.num_rows() == 1));
    }

    #[test]
    fn record_batch_reader_preserves_zero_column_row_count() {
        let mut scan = typed_scan();
        scan.projection.returned_columns.clear();
        scan.projection.series.output_tag_keys.clear();
        let schema = Arc::new(Schema::empty());
        let request = RecordBatchRequest::new(scan, schema, 2, usize::MAX);
        let points = (0..3).map(|time| point(time, None, Vec::new())).collect();
        let batches = RecordBatchReader::from_points(request, points)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 0);
    }

    #[test]
    fn record_batch_reader_rejects_schema_and_value_type_mismatches() {
        let wrong_schema = Arc::new(Schema::new(vec![Field::new(
            "float",
            DataType::Int64,
            true,
        )]));
        let invalid_request = RecordBatchRequest::new(typed_scan(), wrong_schema, 1, 1);
        assert!(matches!(
            RecordBatchReader::from_points(invalid_request, Vec::new()),
            Err(RecordBatchError::InvalidRequest { .. })
        ));

        let points = vec![point(
            1,
            Some("a"),
            vec![
                Val::I64(1),
                Val::I64(2),
                Val::U64(3),
                Val::Bool(true),
                Val::String("x".into()),
            ],
        )];
        let mut reader = RecordBatchReader::from_points(request(1, usize::MAX), points).unwrap();
        assert!(matches!(
            reader.next(),
            Some(Err(RecordBatchError::InvalidRequest { .. }))
        ));
        assert!(reader.next().is_none());
    }

    #[test]
    fn fixed_seed_iterator_to_batch_differential() {
        let mut seed = 0x5eed_cafe_f00d_u64;
        let mut next = || {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            seed
        };
        let mut points = Vec::new();
        for time in 0..257_i64 {
            let bits = next();
            let empty = |shift| ((bits >> shift) & 7_u64) == 0_u64;
            points.push(point(
                time,
                Some(if bits & 1 == 0 { "a" } else { "b" }),
                vec![
                    if empty(1) {
                        Val::Empty(None)
                    } else {
                        Val::F64((bits as i64 % 10_000) as f64 / 10.0)
                    },
                    if empty(4) {
                        Val::Empty(None)
                    } else {
                        Val::I64(bits as i64)
                    },
                    if empty(7) {
                        Val::Empty(None)
                    } else {
                        Val::U64(bits)
                    },
                    if empty(10) {
                        Val::Empty(None)
                    } else {
                        Val::Bool(bits & 1 != 0)
                    },
                    if empty(13) {
                        Val::Empty(None)
                    } else {
                        Val::String(format!("value-{}", bits % 31))
                    },
                ],
            ));
        }

        let expected = points.iter().map(row_from_point).collect::<Vec<_>>();
        let batches = RecordBatchReader::from_points(request(17, 384), points)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(flatten(&batches), expected);
        assert!(batches.len() > 1);
        assert!(batches.iter().all(|batch| batch.num_rows() <= 17));
    }
}
