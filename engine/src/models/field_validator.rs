use crate::models::{FieldType, field_type::is_valid_field_type};

/// Returns the concrete storage field type, or `None` for parser sentinels and invalid values.
pub const fn validate_field_type(field_type: FieldType) -> Option<FieldType> {
    if is_valid_field_type(field_type) {
        Some(field_type)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::models::{BOOL, EMPTY, FLOAT, INTEGER, STRING, UNSIGNED};

    use super::*;

    #[test]
    fn validates_all_supported_storage_field_types() {
        for field_type in [FLOAT, INTEGER, UNSIGNED, STRING, BOOL] {
            assert_eq!(validate_field_type(field_type), Some(field_type));
        }
        assert_eq!(validate_field_type(EMPTY), None);
        assert_eq!(validate_field_type(u8::MAX), None);
    }
}
