// ReefsDB: An Efficient Design and Implementation of Time-Series Store on SSDs
//
// Metadata helpers that translate the engine's native field metadata into
// Arrow types, so the values handed back by local `Shard` metadata can be plugged
// straight into a DataFusion `TableProvider`.
//
// Tags and fields are merged into a single flat column space; callers tell them
// apart by name (tag and field names are guaranteed not to collide). When the
// distinction itself is needed, use `field_dimensions`, which keeps the tag
// keys and field keys in separate sets.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub use crate::models::TIME_COLUMN_NAME;
use crate::models::{BOOL, FLOAT, FieldType, INTEGER, STRING, UNSIGNED};
use arrow_schema::{DataType as ArrowType, Field, Schema, SchemaRef, TimeUnit};

/// Arrow data type used for the timestamp column (nanoseconds since the epoch).
pub fn time_arrow_type() -> ArrowType {
    ArrowType::Timestamp(TimeUnit::Nanosecond, None)
}

/// Maps a storage field type to its Arrow equivalent.
///
/// Anything unrecognised falls back to `Utf8` so an unexpected type never
/// aborts schema construction.
pub fn field_type_to_arrow(typ: FieldType) -> ArrowType {
    match typ {
        FLOAT => ArrowType::Float64,
        INTEGER => ArrowType::Int64,
        UNSIGNED => ArrowType::UInt64,
        BOOL => ArrowType::Boolean,
        STRING => ArrowType::Utf8,
        _ => ArrowType::Utf8,
    }
}

/// Builds an Arrow schema from a measurement's fields (name -> type) and tag
/// keys (dimensions).
///
/// Layout: a non-nullable `time` column first, then the tag keys as nullable
/// `Utf8` columns, then the field keys with their mapped types (nullable).
/// Tags and fields share one flat column space and are sorted by name within
/// each group so the schema is deterministic. The result is a [`SchemaRef`],
/// ready for `TableProvider::schema()`.
pub fn build_arrow_schema(
    fields: &HashMap<String, FieldType>,
    dimensions: &HashSet<String>,
) -> SchemaRef {
    let mut cols: Vec<Field> = Vec::with_capacity(fields.len() + dimensions.len() + 1);

    // Time column (non-nullable).
    cols.push(Field::new(TIME_COLUMN_NAME, time_arrow_type(), false));

    // Tag columns (always Utf8), sorted for a stable schema.
    let mut tag_keys: Vec<&String> = dimensions.iter().collect();
    tag_keys.sort();
    for k in tag_keys {
        cols.push(Field::new(k, ArrowType::Utf8, true));
    }

    // Field columns, sorted for a stable schema.
    let mut field_keys: Vec<(&String, &FieldType)> = fields.iter().collect();
    field_keys.sort_by(|a, b| a.0.cmp(b.0));
    for (k, &typ) in field_keys {
        cols.push(Field::new(k, field_type_to_arrow(typ), true));
    }

    Arc::new(Schema::new(cols))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_primitive_types() {
        assert_eq!(field_type_to_arrow(FLOAT), ArrowType::Float64);
        assert_eq!(field_type_to_arrow(INTEGER), ArrowType::Int64);
        assert_eq!(field_type_to_arrow(UNSIGNED), ArrowType::UInt64);
        assert_eq!(field_type_to_arrow(BOOL), ArrowType::Boolean);
        assert_eq!(field_type_to_arrow(STRING), ArrowType::Utf8);
        // Unknown falls back to Utf8.
        assert_eq!(field_type_to_arrow(u8::MAX), ArrowType::Utf8);
    }

    #[test]
    fn builds_schema_with_time_tags_then_fields() {
        let mut fields = HashMap::new();
        fields.insert("usage".to_string(), FLOAT);
        fields.insert("count".to_string(), INTEGER);

        let mut dims = HashSet::new();
        dims.insert("host".to_string());
        dims.insert("region".to_string());

        let schema = build_arrow_schema(&fields, &dims);

        // time, host, region (tags, sorted), count, usage (fields, sorted)
        let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["time", "host", "region", "count", "usage"]);

        let time = schema.field(0);
        assert!(!time.is_nullable());
        assert_eq!(time.data_type(), &time_arrow_type());

        // Tags are Utf8.
        let host = schema.field_with_name("host").unwrap();
        assert_eq!(host.data_type(), &ArrowType::Utf8);
        assert!(host.is_nullable());

        // Fields keep their mapped type.
        let usage = schema.field_with_name("usage").unwrap();
        assert_eq!(usage.data_type(), &ArrowType::Float64);
    }
}
