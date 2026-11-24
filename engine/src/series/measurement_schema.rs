//! Database-scoped measurement schema metadata.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use parking_lot::{Mutex, RwLock};
use prost::Message;

use crate::{
    common::slow_trace,
    models::{FieldType, TIME_COLUMN_NAME, field_type::is_valid_field_type},
};

use super::meta;

pub const MEASUREMENT_SCHEMA_FILE_NAME: &str = "measurement_schema.idx";
const MEASUREMENT_SCHEMA_MAGIC: [u8; 4] = *b"RMS1";
const MEASUREMENT_SCHEMA_VERSION: u32 = 1;
const MAX_SCHEMA_NAME_LENGTH: usize = 65_535;
const RESERVED_COLUMN_NAMES: [&str; 5] =
    [TIME_COLUMN_NAME, "_field", "_measurement", "\0", "\u{ff}"];

// Generations only need to be unique for the lifetime of the in-process query
// plan cache. Allocating from one process-wide sequence also distinguishes a
// registry that is closed and reopened while that cache is still alive.
static NEXT_SCHEMA_GENERATION: AtomicU64 = AtomicU64::new(1);

fn next_schema_generation() -> u64 {
    NEXT_SCHEMA_GENERATION
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .expect("measurement schema generation space exhausted")
}

#[derive(Debug)]
pub enum Error {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    InvalidMeasurementName {
        reason: String,
    },
    InvalidColumnName {
        measurement: String,
        column: String,
        reason: String,
    },
    DuplicateTagKey {
        measurement: String,
        key: String,
    },
    DuplicateFieldKey {
        measurement: String,
        key: String,
    },
    ColumnNameConflict {
        measurement: String,
        key: String,
    },
    InvalidFieldType {
        measurement: String,
        field: String,
        field_type: FieldType,
    },
    MeasurementSchemaConflict {
        measurement: String,
    },
    FieldTypeConflict {
        measurement: String,
        field: String,
        existing: FieldType,
        incoming: FieldType,
    },
    LegacyBootstrapIncomplete,
    InvalidFormat {
        reason: String,
    },
    UnsupportedVersion {
        version: u32,
    },
    Decode {
        source: prost::DecodeError,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(
                    formatter,
                    "measurement schema metadata I/O at {}: {source}",
                    path.display()
                )
            }
            Self::InvalidMeasurementName { reason } => {
                write!(formatter, "invalid measurement name: {reason}")
            }
            Self::InvalidColumnName {
                measurement,
                column,
                reason,
            } => write!(
                formatter,
                "invalid column {column:?} in measurement {measurement:?}: {reason}"
            ),
            Self::DuplicateTagKey { measurement, key } => write!(
                formatter,
                "duplicate tag key {key:?} in measurement {measurement:?}"
            ),
            Self::DuplicateFieldKey { measurement, key } => write!(
                formatter,
                "duplicate field key {key:?} in measurement {measurement:?}"
            ),
            Self::ColumnNameConflict { measurement, key } => write!(
                formatter,
                "column {key:?} is both a tag and field in measurement {measurement:?}"
            ),
            Self::InvalidFieldType {
                measurement,
                field,
                field_type,
            } => write!(
                formatter,
                "invalid field type {field_type} for field {field:?} in measurement {measurement:?}"
            ),
            Self::MeasurementSchemaConflict { measurement } => {
                write!(formatter, "measurement schema conflict for {measurement:?}")
            }
            Self::FieldTypeConflict {
                measurement,
                field,
                existing,
                incoming,
            } => write!(
                formatter,
                "field type conflict for {measurement:?}.{field}: incoming type {incoming}, existing type {existing}"
            ),
            Self::LegacyBootstrapIncomplete => {
                write!(
                    formatter,
                    "legacy measurement schema bootstrap is not complete"
                )
            }
            Self::InvalidFormat { reason } => {
                write!(formatter, "invalid measurement schema metadata: {reason}")
            }
            Self::UnsupportedVersion { version } => write!(
                formatter,
                "unsupported measurement schema metadata version {version}"
            ),
            Self::Decode { source } => {
                write!(formatter, "invalid measurement schema protobuf: {source}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Decode { source } => Some(source),
            _ => None,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeasurementSchema {
    name: String,
    tag_keys: BTreeSet<String>,
    fields: BTreeMap<String, FieldType>,
}

impl MeasurementSchema {
    pub fn new(
        name: impl Into<String>,
        tag_keys: Vec<String>,
        fields: Vec<(String, FieldType)>,
    ) -> Result<Self> {
        let name = name.into();
        validate_measurement_name(&name)?;

        let mut canonical_tags = BTreeSet::new();
        for key in tag_keys {
            validate_column_name(&name, &key)?;
            if !canonical_tags.insert(key.clone()) {
                return Err(Error::DuplicateTagKey {
                    measurement: name,
                    key,
                });
            }
        }

        let mut canonical_fields = BTreeMap::new();
        for (key, field_type) in fields {
            validate_column_name(&name, &key)?;
            if !is_valid_field_type(field_type) {
                return Err(Error::InvalidFieldType {
                    measurement: name,
                    field: key,
                    field_type,
                });
            }
            if canonical_fields.insert(key.clone(), field_type).is_some() {
                return Err(Error::DuplicateFieldKey {
                    measurement: name,
                    key,
                });
            }
        }

        if let Some(key) = canonical_tags
            .iter()
            .find(|key| canonical_fields.contains_key(*key))
        {
            return Err(Error::ColumnNameConflict {
                measurement: name,
                key: key.clone(),
            });
        }

        Ok(Self {
            name,
            tag_keys: canonical_tags,
            fields: canonical_fields,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tag_keys(&self) -> &BTreeSet<String> {
        &self.tag_keys
    }

    pub fn fields(&self) -> &BTreeMap<String, FieldType> {
        &self.fields
    }

    fn merge_compatible(&mut self, incoming: &Self) -> Result<bool> {
        if self.name != incoming.name {
            return Err(Error::MeasurementSchemaConflict {
                measurement: incoming.name.clone(),
            });
        }

        let mut changed = false;
        for key in &incoming.tag_keys {
            if self.fields.contains_key(key) {
                return Err(Error::ColumnNameConflict {
                    measurement: self.name.clone(),
                    key: key.clone(),
                });
            }
            changed |= self.tag_keys.insert(key.clone());
        }
        for (key, incoming_type) in &incoming.fields {
            if self.tag_keys.contains(key) {
                return Err(Error::ColumnNameConflict {
                    measurement: self.name.clone(),
                    key: key.clone(),
                });
            }
            match self.fields.get(key) {
                Some(existing) if existing != incoming_type => {
                    return Err(Error::FieldTypeConflict {
                        measurement: self.name.clone(),
                        field: key.clone(),
                        existing: *existing,
                        incoming: *incoming_type,
                    });
                }
                Some(_) => {}
                None => {
                    self.fields.insert(key.clone(), *incoming_type);
                    changed = true;
                }
            }
        }
        Ok(changed)
    }

    fn contains_compatible(&self, incoming: &Self) -> Result<bool> {
        if self.name != incoming.name {
            return Err(Error::MeasurementSchemaConflict {
                measurement: incoming.name.clone(),
            });
        }

        let mut unchanged = true;
        for key in &incoming.tag_keys {
            if self.fields.contains_key(key) {
                return Err(Error::ColumnNameConflict {
                    measurement: self.name.clone(),
                    key: key.clone(),
                });
            }
            unchanged &= self.tag_keys.contains(key);
        }
        for (key, incoming_type) in &incoming.fields {
            if self.tag_keys.contains(key) {
                return Err(Error::ColumnNameConflict {
                    measurement: self.name.clone(),
                    key: key.clone(),
                });
            }
            match self.fields.get(key) {
                Some(existing) if existing != incoming_type => {
                    return Err(Error::FieldTypeConflict {
                        measurement: self.name.clone(),
                        field: key.clone(),
                        existing: *existing,
                        incoming: *incoming_type,
                    });
                }
                Some(_) => {}
                None => unchanged = false,
            }
        }
        Ok(unchanged)
    }
}

pub(crate) fn schemas_from_points(
    points: &[crate::models::Point],
) -> Result<Vec<MeasurementSchema>> {
    #[derive(Default)]
    struct PendingSchema {
        tag_keys: BTreeSet<String>,
        fields: BTreeMap<String, FieldType>,
    }

    let mut pending = BTreeMap::<String, PendingSchema>::new();
    for point in points {
        let name =
            String::from_utf8(point.name()).map_err(|source| Error::InvalidMeasurementName {
                reason: source.to_string(),
            })?;
        let schema = pending.entry(name.clone()).or_default();
        let mut tag_error = None;
        point.for_each_tag(|key, _value| {
            match std::str::from_utf8(key) {
                Ok(key) => {
                    schema.tag_keys.insert(key.to_string());
                }
                Err(source) => {
                    tag_error = Some(Error::InvalidColumnName {
                        measurement: name.clone(),
                        column: String::from_utf8_lossy(key).into_owned(),
                        reason: source.to_string(),
                    });
                }
            }
            tag_error.is_none()
        });
        if let Some(error) = tag_error {
            return Err(error);
        }

        for field in point.field_iter() {
            if field.field_key() == TIME_COLUMN_NAME.as_bytes() {
                continue;
            }
            let key = std::str::from_utf8(field.field_key()).map_err(|source| {
                Error::InvalidColumnName {
                    measurement: name.clone(),
                    column: String::from_utf8_lossy(field.field_key()).into_owned(),
                    reason: source.to_string(),
                }
            })?;
            let field_type = field.field_type();
            if !is_valid_field_type(field_type) {
                return Err(Error::InvalidFieldType {
                    measurement: name.clone(),
                    field: key.to_string(),
                    field_type,
                });
            }
            match schema.fields.get(key) {
                Some(existing) if *existing != field_type => {
                    return Err(Error::FieldTypeConflict {
                        measurement: name,
                        field: key.to_string(),
                        existing: *existing,
                        incoming: field_type,
                    });
                }
                Some(_) => {}
                None => {
                    schema.fields.insert(key.to_string(), field_type);
                }
            }
        }
    }

    pending
        .into_iter()
        .map(|(name, schema)| {
            MeasurementSchema::new(
                name,
                schema.tag_keys.into_iter().collect(),
                schema.fields.into_iter().collect(),
            )
        })
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryData {
    legacy_bootstrap_complete: bool,
    measurements: BTreeMap<String, MeasurementSchema>,
}

pub struct MeasurementSchemaRegistry {
    path: PathBuf,
    commit_lock: Mutex<()>,
    current: RwLock<Arc<RegistryData>>,
    generation: AtomicU64,
}

impl std::fmt::Debug for MeasurementSchemaRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MeasurementSchemaRegistry")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl MeasurementSchemaRegistry {
    /// Opens the authoritative registry, creating a completed empty registry when absent.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_missing_state(path.into(), true)
    }

    /// Opens an existing registry or creates a resumable legacy-bootstrap registry when absent.
    pub fn open_for_legacy_bootstrap(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_missing_state(path.into(), false)
    }

    fn open_with_missing_state(path: PathBuf, legacy_bootstrap_complete: bool) -> Result<Self> {
        let loaded = load_registry(&path)?;
        let was_missing = loaded.is_none();
        let mut data = loaded.unwrap_or(RegistryData {
            legacy_bootstrap_complete,
            measurements: BTreeMap::new(),
        });
        if !data.legacy_bootstrap_complete {
            // Persisted BOOTSTRAPPING content is never authoritative. Startup
            // reconstructs a fresh candidate from the complete local shard set.
            data.measurements.clear();
        }
        let registry = Self {
            path,
            commit_lock: Mutex::new(()),
            current: RwLock::new(Arc::new(data)),
            generation: AtomicU64::new(next_schema_generation()),
        };
        if was_missing {
            let current = Arc::clone(&registry.current.read());
            registry.persist(current.as_ref())?;
        }
        Ok(registry)
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn needs_legacy_bootstrap(&self) -> bool {
        !self.current.read().legacy_bootstrap_complete
    }

    /// Returns an in-process identity for the currently published schema.
    ///
    /// The value changes after every successful planning-relevant schema
    /// publication and also differs when a registry is reopened. It is not a
    /// persisted file-format field; in-memory consumers must discard it across
    /// process restarts.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Rebuilds and atomically publishes the complete legacy schema snapshot.
    pub fn complete_legacy_bootstrap(
        &self,
        schemas: impl IntoIterator<Item = MeasurementSchema>,
    ) -> Result<bool> {
        let schemas = schemas.into_iter().collect::<Vec<_>>();
        let _commit = self.commit_lock.lock();
        let current = Arc::clone(&self.current.read());
        if current.legacy_bootstrap_complete {
            return Ok(false);
        }

        let mut next = RegistryData {
            legacy_bootstrap_complete: false,
            measurements: BTreeMap::new(),
        };
        for schema in &schemas {
            match next.measurements.get_mut(schema.name()) {
                Some(existing) => {
                    existing.merge_compatible(schema)?;
                }
                None => {
                    next.measurements
                        .insert(schema.name().to_string(), schema.clone());
                }
            }
        }
        next.legacy_bootstrap_complete = true;
        self.persist(&next)?;
        *self.current.write() = Arc::new(next);
        self.generation
            .store(next_schema_generation(), Ordering::Release);
        Ok(true)
    }

    pub fn create_measurement_schema(&self, schema: MeasurementSchema) -> Result<bool> {
        self.commit(|current| {
            ensure_bootstrap_complete(current)?;
            if let Some(existing) = current.measurements.get(schema.name()) {
                if existing == &schema {
                    return Ok((current.clone(), false));
                }
                return Err(Error::MeasurementSchemaConflict {
                    measurement: schema.name().to_string(),
                });
            }
            let mut next = current.clone();
            next.measurements.insert(schema.name.clone(), schema);
            Ok((next, true))
        })
    }

    /// Atomically validates and merges one write batch's measurement-wide schemas.
    pub fn ensure_measurement_schemas(
        &self,
        schemas: impl IntoIterator<Item = MeasurementSchema>,
    ) -> Result<bool> {
        let schemas = schemas.into_iter().collect::<Vec<_>>();
        let _commit = self.commit_lock.lock();
        let current = Arc::clone(&self.current.read());
        ensure_bootstrap_complete(current.as_ref())?;
        if measurement_schemas_unchanged(current.as_ref(), &schemas)? {
            return Ok(false);
        }
        let (next, changed) = merge_measurement_schemas(current.as_ref(), &schemas)?;
        self.persist(&next)?;
        *self.current.write() = Arc::new(next);
        self.generation
            .store(next_schema_generation(), Ordering::Release);
        Ok(changed)
    }

    pub fn measurement_schema(&self, name: &str) -> Option<MeasurementSchema> {
        #[cfg(feature = "slow-trace")]
        let lock_started = slow_trace::now();
        let current = self.current.read();
        #[cfg(feature = "slow-trace")]
        let lock_wait = slow_trace::elapsed(lock_started);
        let schema = current.measurements.get(name).cloned();
        #[cfg(feature = "slow-trace")]
        {
            drop(current);
            slow_trace::log_planner_lock_wait("measurement_schema.current.read", lock_wait);
        }
        schema
    }

    pub fn measurement_names(&self) -> Vec<Vec<u8>> {
        self.current
            .read()
            .measurements
            .keys()
            .map(|name| name.as_bytes().to_vec())
            .collect()
    }

    /// Removes a schema idempotently. DROP execution is intentionally not wired here.
    pub fn remove_measurement_schema(&self, name: &str) -> Result<bool> {
        self.commit(|current| {
            ensure_bootstrap_complete(current)?;
            if !current.measurements.contains_key(name) {
                return Ok((current.clone(), false));
            }
            let mut next = current.clone();
            next.measurements.remove(name);
            Ok((next, true))
        })
    }

    fn commit<T>(
        &self,
        mutate: impl FnOnce(&RegistryData) -> Result<(RegistryData, T)>,
    ) -> Result<T> {
        let _commit = self.commit_lock.lock();
        let current = Arc::clone(&self.current.read());
        let (next, result) = mutate(current.as_ref())?;
        if next == *current {
            return Ok(result);
        }
        self.persist(&next)?;
        *self.current.write() = Arc::new(next);
        self.generation
            .store(next_schema_generation(), Ordering::Release);
        Ok(result)
    }

    fn persist(&self, data: &RegistryData) -> Result<()> {
        let bytes = encode_registry(data);
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|source| io_error(parent, source))?;
        let temp_path = temporary_path(&self.path);
        let result = (|| {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp_path)
                .map_err(|source| io_error(&temp_path, source))?;
            file.write_all(&bytes)
                .map_err(|source| io_error(&temp_path, source))?;
            file.sync_all()
                .map_err(|source| io_error(&temp_path, source))?;
            drop(file);
            fs::rename(&temp_path, &self.path).map_err(|source| io_error(&self.path, source))?;
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|source| io_error(parent, source))?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result
    }
}

fn ensure_bootstrap_complete(data: &RegistryData) -> Result<()> {
    if data.legacy_bootstrap_complete {
        Ok(())
    } else {
        Err(Error::LegacyBootstrapIncomplete)
    }
}

fn merge_measurement_schemas(
    current: &RegistryData,
    schemas: &[MeasurementSchema],
) -> Result<(RegistryData, bool)> {
    let mut next = current.clone();
    let mut changed = false;
    for schema in schemas {
        match next.measurements.get_mut(schema.name()) {
            Some(existing) => changed |= existing.merge_compatible(schema)?,
            None => {
                next.measurements
                    .insert(schema.name().to_string(), schema.clone());
                changed = true;
            }
        }
    }
    Ok((next, changed))
}

fn measurement_schemas_unchanged(
    current: &RegistryData,
    schemas: &[MeasurementSchema],
) -> Result<bool> {
    let mut unchanged = true;
    for schema in schemas {
        match current.measurements.get(schema.name()) {
            Some(existing) => unchanged &= existing.contains_compatible(schema)?,
            None => unchanged = false,
        }
    }
    Ok(unchanged)
}

fn validate_measurement_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidMeasurementName {
            reason: "name is empty".to_string(),
        });
    }
    if name.len() > MAX_SCHEMA_NAME_LENGTH {
        return Err(Error::InvalidMeasurementName {
            reason: format!("name exceeds {MAX_SCHEMA_NAME_LENGTH} bytes"),
        });
    }
    if !crate::models::points::valid_token(name.as_bytes()) {
        return Err(Error::InvalidMeasurementName {
            reason: "name is not valid UTF-8".to_string(),
        });
    }
    Ok(())
}

fn validate_column_name(measurement: &str, column: &str) -> Result<()> {
    let reason = if column.is_empty() {
        Some("name is empty".to_string())
    } else if column.len() > MAX_SCHEMA_NAME_LENGTH {
        Some(format!("name exceeds {MAX_SCHEMA_NAME_LENGTH} bytes"))
    } else if RESERVED_COLUMN_NAMES.contains(&column) {
        Some("name is reserved".to_string())
    } else if !crate::models::points::valid_token(column.as_bytes()) {
        Some("name is not valid UTF-8".to_string())
    } else {
        None
    };
    if let Some(reason) = reason {
        return Err(Error::InvalidColumnName {
            measurement: measurement.to_string(),
            column: column.to_string(),
            reason,
        });
    }
    Ok(())
}

fn encode_registry(data: &RegistryData) -> Vec<u8> {
    let envelope = meta::MeasurementSchemaRegistryEnvelope {
        schema_version: MEASUREMENT_SCHEMA_VERSION,
        legacy_bootstrap_complete: data.legacy_bootstrap_complete,
        measurements: data
            .measurements
            .values()
            .map(|schema| meta::MeasurementSchemaEntry {
                name: schema.name.clone(),
                tag_keys: schema.tag_keys.iter().cloned().collect(),
                fields: schema
                    .fields
                    .iter()
                    .map(|(name, typ)| meta::MeasurementSchemaField {
                        name: name.clone(),
                        typ: i32::from(*typ),
                    })
                    .collect(),
            })
            .collect(),
    };
    let mut bytes = Vec::with_capacity(MEASUREMENT_SCHEMA_MAGIC.len() + envelope.encoded_len());
    bytes.extend_from_slice(&MEASUREMENT_SCHEMA_MAGIC);
    envelope
        .encode(&mut bytes)
        .expect("encoding a protobuf into Vec cannot fail");
    bytes
}

fn load_registry(path: &Path) -> Result<Option<RegistryData>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error(path, source)),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| io_error(path, source))?;
    if bytes.len() < MEASUREMENT_SCHEMA_MAGIC.len()
        || bytes[..MEASUREMENT_SCHEMA_MAGIC.len()] != MEASUREMENT_SCHEMA_MAGIC
    {
        return Err(Error::InvalidFormat {
            reason: "missing RMS1 magic".to_string(),
        });
    }
    let envelope =
        meta::MeasurementSchemaRegistryEnvelope::decode(&bytes[MEASUREMENT_SCHEMA_MAGIC.len()..])
            .map_err(|source| Error::Decode { source })?;
    if envelope.schema_version != MEASUREMENT_SCHEMA_VERSION {
        return Err(Error::UnsupportedVersion {
            version: envelope.schema_version,
        });
    }

    let mut measurements = BTreeMap::new();
    for persisted in envelope.measurements {
        let schema = MeasurementSchema::new(
            persisted.name,
            persisted.tag_keys,
            persisted
                .fields
                .into_iter()
                .map(|field| {
                    let typ = u8::try_from(field.typ).unwrap_or(u8::MAX);
                    (field.name, typ)
                })
                .collect(),
        )?;
        let name = schema.name.clone();
        if measurements.insert(name.clone(), schema).is_some() {
            return Err(Error::InvalidFormat {
                reason: format!("duplicate measurement schema {name:?}"),
            });
        }
    }
    Ok(Some(RegistryData {
        legacy_bootstrap_complete: envelope.legacy_bootstrap_complete,
        measurements,
    }))
}

fn temporary_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(MEASUREMENT_SCHEMA_FILE_NAME);
    path.with_file_name(format!("{file_name}.tmp"))
}

fn io_error(path: impl AsRef<Path>, source: std::io::Error) -> Error {
    Error::Io {
        path: path.as_ref().to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Barrier},
        thread,
    };

    use tempfile::tempdir;

    use crate::models::{BOOL, FLOAT, INTEGER, STRING, UNSIGNED};

    use super::*;

    fn schema() -> MeasurementSchema {
        MeasurementSchema::new(
            "cpu",
            vec!["host".to_string(), "region".to_string()],
            vec![
                ("usage_system".to_string(), FLOAT),
                ("count".to_string(), INTEGER),
            ],
        )
        .unwrap()
    }

    #[test]
    fn empty_measurement_schema_round_trips_without_series_or_tag_values() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = MeasurementSchemaRegistry::open(&path).unwrap();
        assert!(registry.create_measurement_schema(schema()).unwrap());
        assert_eq!(registry.measurement_names(), vec![b"cpu".to_vec()]);
        assert_eq!(registry.measurement_schema("cpu"), Some(schema()));

        drop(registry);
        let reopened = MeasurementSchemaRegistry::open(&path).unwrap();
        let persisted = reopened.measurement_schema("cpu").unwrap();
        assert_eq!(persisted.tag_keys(), schema().tag_keys());
        assert_eq!(persisted.fields(), schema().fields());
    }

    #[test]
    fn exact_create_is_idempotent_and_incompatible_create_conflicts() {
        let directory = tempdir().unwrap();
        let registry =
            MeasurementSchemaRegistry::open(directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME))
                .unwrap();
        assert!(registry.create_measurement_schema(schema()).unwrap());
        assert!(!registry.create_measurement_schema(schema()).unwrap());
        let incompatible = MeasurementSchema::new(
            "cpu",
            vec!["host".to_string()],
            vec![("value".to_string(), STRING)],
        )
        .unwrap();
        assert!(matches!(
            registry.create_measurement_schema(incompatible),
            Err(Error::MeasurementSchemaConflict { .. })
        ));
        assert_eq!(registry.measurement_schema("cpu"), Some(schema()));
    }

    #[test]
    fn schema_validation_rejects_duplicates_reserved_names_and_collisions() {
        assert!(matches!(
            MeasurementSchema::new("cpu", vec!["host".to_string(), "host".to_string()], vec![]),
            Err(Error::DuplicateTagKey { .. })
        ));
        assert!(matches!(
            MeasurementSchema::new(
                "cpu",
                vec![],
                vec![("value".to_string(), FLOAT), ("value".to_string(), FLOAT)]
            ),
            Err(Error::DuplicateFieldKey { .. })
        ));
        assert!(matches!(
            MeasurementSchema::new(
                "cpu",
                vec!["host".to_string()],
                vec![("host".to_string(), FLOAT)]
            ),
            Err(Error::ColumnNameConflict { .. })
        ));
        for reserved in [TIME_COLUMN_NAME, "_field", "_measurement", "\0", "\u{ff}"] {
            assert!(matches!(
                MeasurementSchema::new("cpu", vec![reserved.to_string()], vec![]),
                Err(Error::InvalidColumnName { .. })
            ));
            assert!(matches!(
                MeasurementSchema::new("cpu", vec![], vec![(reserved.to_string(), FLOAT)]),
                Err(Error::InvalidColumnName { .. })
            ));
        }
    }

    #[test]
    fn all_storage_field_types_persist_without_time_column() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = MeasurementSchemaRegistry::open(&path).unwrap();
        let fields = [FLOAT, INTEGER, UNSIGNED, STRING, BOOL]
            .into_iter()
            .enumerate()
            .map(|(index, typ)| (format!("field_{index}"), typ))
            .collect::<Vec<_>>();
        let all_types = MeasurementSchema::new("types", vec![], fields).unwrap();
        registry
            .create_measurement_schema(all_types.clone())
            .unwrap();
        drop(registry);

        let reopened = MeasurementSchemaRegistry::open(path).unwrap();
        let persisted = reopened.measurement_schema("types").unwrap();
        assert_eq!(persisted, all_types);
        assert!(!persisted.fields().contains_key(TIME_COLUMN_NAME));
    }

    #[test]
    fn compatible_evolution_is_atomic_and_type_conflict_preserves_old_schema() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = MeasurementSchemaRegistry::open(&path).unwrap();
        registry
            .create_measurement_schema(
                MeasurementSchema::new(
                    "cpu",
                    vec!["host".to_string()],
                    vec![("a".to_string(), FLOAT)],
                )
                .unwrap(),
            )
            .unwrap();
        assert!(
            registry
                .ensure_measurement_schemas([MeasurementSchema::new(
                    "cpu",
                    vec!["region".to_string()],
                    vec![("b".to_string(), INTEGER)],
                )
                .unwrap()])
                .unwrap()
        );
        let evolved = registry.measurement_schema("cpu").unwrap();
        assert_eq!(evolved.tag_keys().len(), 2);
        assert_eq!(evolved.fields().len(), 2);

        let conflict = MeasurementSchema::new(
            "cpu",
            vec!["rack".to_string()],
            vec![("a".to_string(), STRING)],
        )
        .unwrap();
        assert!(matches!(
            registry.ensure_measurement_schemas([conflict]),
            Err(Error::FieldTypeConflict { .. })
        ));
        assert_eq!(registry.measurement_schema("cpu"), Some(evolved.clone()));
        drop(registry);

        assert_eq!(
            MeasurementSchemaRegistry::open(path)
                .unwrap()
                .measurement_schema("cpu"),
            Some(evolved)
        );
    }

    #[test]
    fn multi_measurement_conflict_does_not_publish_earlier_batch_schema() {
        let directory = tempdir().unwrap();
        let registry =
            MeasurementSchemaRegistry::open(directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME))
                .unwrap();
        registry
            .create_measurement_schema(
                MeasurementSchema::new("cpu", vec![], vec![("value".to_string(), FLOAT)]).unwrap(),
            )
            .unwrap();

        let error = registry.ensure_measurement_schemas([
            MeasurementSchema::new(
                "memory",
                vec!["host".to_string()],
                vec![("free".to_string(), UNSIGNED)],
            )
            .unwrap(),
            MeasurementSchema::new("cpu", vec![], vec![("value".to_string(), STRING)]).unwrap(),
        ]);
        assert!(matches!(error, Err(Error::FieldTypeConflict { .. })));
        assert!(registry.measurement_schema("memory").is_none());
        assert_eq!(
            registry
                .measurement_schema("cpu")
                .unwrap()
                .fields()
                .get("value"),
            Some(&FLOAT)
        );
    }

    #[test]
    fn concurrent_compatible_evolution_does_not_lose_fields() {
        let directory = tempdir().unwrap();
        let registry = Arc::new(
            MeasurementSchemaRegistry::open(directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME))
                .unwrap(),
        );
        let mut workers = Vec::new();
        for index in 0..16 {
            let registry = Arc::clone(&registry);
            workers.push(thread::spawn(move || {
                registry
                    .ensure_measurement_schemas([MeasurementSchema::new(
                        "cpu",
                        vec![format!("tag_{index}")],
                        vec![(format!("field_{index}"), FLOAT)],
                    )
                    .unwrap()])
                    .unwrap();
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let schema = registry.measurement_schema("cpu").unwrap();
        assert_eq!(schema.tag_keys().len(), 16);
        assert_eq!(schema.fields().len(), 16);
    }

    #[test]
    fn remove_is_idempotent_and_persistent() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = MeasurementSchemaRegistry::open(&path).unwrap();
        registry.create_measurement_schema(schema()).unwrap();
        assert!(registry.remove_measurement_schema("cpu").unwrap());
        assert!(!registry.remove_measurement_schema("cpu").unwrap());
        drop(registry);
        assert!(
            MeasurementSchemaRegistry::open(path)
                .unwrap()
                .measurement_names()
                .is_empty()
        );
    }

    #[test]
    fn generation_changes_only_after_schema_publication_and_reopen() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = MeasurementSchemaRegistry::open(&path).unwrap();
        let opened = registry.generation();

        assert!(registry.create_measurement_schema(schema()).unwrap());
        let created = registry.generation();
        assert_ne!(created, opened);

        assert!(!registry.ensure_measurement_schemas([schema()]).unwrap());
        assert_eq!(registry.generation(), created);

        assert!(registry.remove_measurement_schema("cpu").unwrap());
        let removed = registry.generation();
        assert_ne!(removed, created);
        assert!(!registry.remove_measurement_schema("cpu").unwrap());
        assert_eq!(registry.generation(), removed);

        assert!(registry.create_measurement_schema(schema()).unwrap());
        let recreated = registry.generation();
        assert_ne!(recreated, removed);

        drop(registry);
        let reopened = MeasurementSchemaRegistry::open(path).unwrap();
        assert_ne!(reopened.generation(), recreated);
    }

    #[test]
    fn incomplete_bootstrap_rejects_mutation_until_atomic_completion() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = MeasurementSchemaRegistry::open_for_legacy_bootstrap(&path).unwrap();
        assert!(registry.needs_legacy_bootstrap());
        assert!(matches!(
            registry.create_measurement_schema(schema()),
            Err(Error::LegacyBootstrapIncomplete)
        ));
        assert!(matches!(
            registry.ensure_measurement_schemas([schema()]),
            Err(Error::LegacyBootstrapIncomplete)
        ));
        assert!(matches!(
            registry.remove_measurement_schema("cpu"),
            Err(Error::LegacyBootstrapIncomplete)
        ));
        drop(registry);

        let reopened = MeasurementSchemaRegistry::open_for_legacy_bootstrap(&path).unwrap();
        assert!(reopened.needs_legacy_bootstrap());
        assert!(reopened.measurement_names().is_empty());
        assert!(reopened.complete_legacy_bootstrap([schema()]).unwrap());
        assert!(!reopened.needs_legacy_bootstrap());
        assert!(
            !reopened
                .complete_legacy_bootstrap([MeasurementSchema::new(
                    "cpu",
                    vec![],
                    vec![("value".to_string(), STRING)],
                )
                .unwrap()])
                .unwrap()
        );
        assert_eq!(reopened.measurement_schema("cpu"), Some(schema()));
        drop(reopened);
        assert!(
            !MeasurementSchemaRegistry::open_for_legacy_bootstrap(path)
                .unwrap()
                .needs_legacy_bootstrap()
        );
    }

    #[test]
    fn persisted_partial_bootstrap_is_hidden_and_rebuilt_from_a_fresh_candidate() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = MeasurementSchemaRegistry::open_for_legacy_bootstrap(&path).unwrap();
        let partial = RegistryData {
            legacy_bootstrap_complete: false,
            measurements: BTreeMap::from([("cpu".to_string(), schema())]),
        };
        registry.persist(&partial).unwrap();
        drop(registry);

        let reopened = MeasurementSchemaRegistry::open_for_legacy_bootstrap(&path).unwrap();
        assert!(reopened.needs_legacy_bootstrap());
        assert!(reopened.measurement_names().is_empty());
        assert!(reopened.measurement_schema("cpu").is_none());

        let memory =
            MeasurementSchema::new("memory", vec![], vec![("used".to_string(), UNSIGNED)]).unwrap();
        assert!(
            reopened
                .complete_legacy_bootstrap([memory.clone()])
                .unwrap()
        );
        assert!(reopened.measurement_schema("cpu").is_none());
        assert_eq!(reopened.measurement_schema("memory"), Some(memory));
    }

    #[test]
    fn invalid_or_truncated_existing_registry_is_rejected() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        fs::write(&path, []).unwrap();
        assert!(matches!(
            MeasurementSchemaRegistry::open(&path),
            Err(Error::InvalidFormat { .. })
        ));
        fs::write(&path, MEASUREMENT_SCHEMA_MAGIC).unwrap();
        assert!(matches!(
            MeasurementSchemaRegistry::open(&path),
            Err(Error::UnsupportedVersion { version: 0 })
        ));
        let mut future = MEASUREMENT_SCHEMA_MAGIC.to_vec();
        meta::MeasurementSchemaRegistryEnvelope {
            schema_version: MEASUREMENT_SCHEMA_VERSION + 1,
            legacy_bootstrap_complete: true,
            measurements: vec![],
        }
        .encode(&mut future)
        .unwrap();
        fs::write(&path, future).unwrap();
        assert!(matches!(
            MeasurementSchemaRegistry::open(&path),
            Err(Error::UnsupportedVersion { version: 2 })
        ));

        let invalid_utf8_measurement = [
            MEASUREMENT_SCHEMA_MAGIC.as_slice(),
            &[0x08, 0x01, 0x10, 0x01, 0x1a, 0x03, 0x0a, 0x01, 0xff],
        ]
        .concat();
        fs::write(&path, invalid_utf8_measurement).unwrap();
        assert!(matches!(
            MeasurementSchemaRegistry::open(path),
            Err(Error::Decode { .. })
        ));
    }

    #[test]
    fn corrupt_registry_entries_are_rejected() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let write = |envelope: meta::MeasurementSchemaRegistryEnvelope| {
            let mut bytes = MEASUREMENT_SCHEMA_MAGIC.to_vec();
            envelope.encode(&mut bytes).unwrap();
            fs::write(&path, bytes).unwrap();
        };
        let persisted = meta::MeasurementSchemaEntry {
            name: "cpu".to_string(),
            tag_keys: vec!["host".to_string()],
            fields: vec![meta::MeasurementSchemaField {
                name: "value".to_string(),
                typ: i32::from(FLOAT),
            }],
        };

        write(meta::MeasurementSchemaRegistryEnvelope {
            schema_version: MEASUREMENT_SCHEMA_VERSION,
            legacy_bootstrap_complete: true,
            measurements: vec![persisted.clone(), persisted.clone()],
        });
        assert!(matches!(
            MeasurementSchemaRegistry::open(&path),
            Err(Error::InvalidFormat { .. })
        ));

        let mut invalid_type = persisted.clone();
        invalid_type.fields[0].typ = 999;
        write(meta::MeasurementSchemaRegistryEnvelope {
            schema_version: MEASUREMENT_SCHEMA_VERSION,
            legacy_bootstrap_complete: true,
            measurements: vec![invalid_type],
        });
        assert!(matches!(
            MeasurementSchemaRegistry::open(&path),
            Err(Error::InvalidFieldType { .. })
        ));

        let mut duplicate_field = persisted;
        duplicate_field.fields.push(meta::MeasurementSchemaField {
            name: "value".to_string(),
            typ: i32::from(STRING),
        });
        write(meta::MeasurementSchemaRegistryEnvelope {
            schema_version: MEASUREMENT_SCHEMA_VERSION,
            legacy_bootstrap_complete: true,
            measurements: vec![duplicate_field],
        });
        assert!(matches!(
            MeasurementSchemaRegistry::open(path),
            Err(Error::DuplicateFieldKey { .. })
        ));
    }

    #[test]
    fn failed_durable_replace_does_not_publish_in_memory_schema() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = MeasurementSchemaRegistry::open(&path).unwrap();
        registry.create_measurement_schema(schema()).unwrap();
        let canonical_before = fs::read(&path).unwrap();
        let temp_path = temporary_path(&path);
        fs::create_dir(&temp_path).unwrap();

        assert!(matches!(
            registry.create_measurement_schema(
                MeasurementSchema::new("memory", vec![], vec![("used".to_string(), UNSIGNED)],)
                    .unwrap()
            ),
            Err(Error::Io { .. })
        ));
        assert_eq!(registry.measurement_schema("cpu"), Some(schema()));
        assert!(registry.measurement_schema("memory").is_none());
        assert_eq!(fs::read(&path).unwrap(), canonical_before);

        fs::remove_dir(&temp_path).unwrap();
        drop(registry);
        let reopened = MeasurementSchemaRegistry::open(path).unwrap();
        assert_eq!(reopened.measurement_schema("cpu"), Some(schema()));
        assert!(reopened.measurement_schema("memory").is_none());
    }

    #[test]
    fn unchanged_schema_fast_path_performs_no_persistence_io() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = MeasurementSchemaRegistry::open(&path).unwrap();
        registry.create_measurement_schema(schema()).unwrap();
        let canonical_before = fs::read(&path).unwrap();
        let temp_path = temporary_path(&path);
        fs::create_dir(&temp_path).unwrap();

        assert!(!registry.ensure_measurement_schemas([schema()]).unwrap());
        assert_eq!(fs::read(&path).unwrap(), canonical_before);
        fs::remove_dir(temp_path).unwrap();
    }

    #[test]
    fn concurrent_conflicting_evolution_publishes_one_canonical_type() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(MEASUREMENT_SCHEMA_FILE_NAME);
        let registry = Arc::new(MeasurementSchemaRegistry::open(&path).unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let workers = [FLOAT, STRING]
            .into_iter()
            .map(|typ| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    registry.ensure_measurement_schemas([MeasurementSchema::new(
                        "cpu",
                        vec![],
                        vec![("value".to_string(), typ)],
                    )
                    .unwrap()])
                })
            })
            .collect::<Vec<_>>();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(Error::FieldTypeConflict { .. })))
                .count(),
            1
        );
        let canonical = registry.measurement_schema("cpu").unwrap().fields()["value"];
        assert!(canonical == FLOAT || canonical == STRING);
        drop(registry);
        assert_eq!(
            MeasurementSchemaRegistry::open(path)
                .unwrap()
                .measurement_schema("cpu")
                .unwrap()
                .fields()["value"],
            canonical
        );
    }
}
