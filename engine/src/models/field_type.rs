//! Storage field types used by line protocol values and persisted field metadata.

/// Numeric representation persisted in measurement field metadata.
pub type FieldType = u8;

/// A floating-point field.
pub const FLOAT: FieldType = 0;
/// A signed integer field.
pub const INTEGER: FieldType = 1;
/// An unsigned integer field.
pub const UNSIGNED: FieldType = 2;
/// A string field.
pub const STRING: FieldType = 3;
/// A boolean field.
pub const BOOL: FieldType = 4;
/// Parser sentinel indicating that no field value is present.
pub const EMPTY: FieldType = 5;

/// Returns whether `field_type` represents a concrete storage field value.
pub const fn is_valid_field_type(field_type: FieldType) -> bool {
    matches!(field_type, FLOAT | INTEGER | UNSIGNED | STRING | BOOL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_field_type_discriminants_are_stable() {
        assert_eq!(FLOAT, 0);
        assert_eq!(INTEGER, 1);
        assert_eq!(UNSIGNED, 2);
        assert_eq!(STRING, 3);
        assert_eq!(BOOL, 4);
        assert_eq!(EMPTY, 5);
    }

    #[test]
    fn only_concrete_storage_types_are_valid() {
        for field_type in [FLOAT, INTEGER, UNSIGNED, STRING, BOOL] {
            assert!(is_valid_field_type(field_type));
        }
        assert!(!is_valid_field_type(EMPTY));
        assert!(!is_valid_field_type(u8::MAX));
    }
}
