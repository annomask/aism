// Statistics gathered for one database.
pub const STAT_DATABASE_SERIES: &str = "num_series"; // number of series in a database
pub const STAT_DATABASE_MEASUREMENTS: &str = "num_measurements"; // number of measurements in a database

// SERIES_FILE_DIRECTORY is the name of the directory containing series files for
// a database.
pub const SERIES_FILE_DIRECTORY: &str = "_series";

pub use crate::models::{DatabaseId, InvalidDatabaseId};

pub(crate) mod measurement_fields;
pub mod measurement_schema;
pub(crate) mod meta;
pub mod series_file;
mod series_index;
pub(crate) mod series_partition;
pub(crate) mod series_segment;
pub mod series_set;

pub(crate) use measurement_fields::Error as MeasurementFieldsError;
pub use measurement_schema::{
    Error as MeasurementSchemaError, MEASUREMENT_SCHEMA_FILE_NAME, MeasurementSchema,
    MeasurementSchemaRegistry,
};
