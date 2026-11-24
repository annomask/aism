use std::{
    collections::HashMap,
    fs,
    io::{ErrorKind, Read, Write},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::Arc,
};

use crate::common::to_string;
use crate::models::FieldType;
use parking_lot::RwLock;
use prost::Message;
// use bytes::{Buf, BufMut};
// use integer_encoding::VarInt;
// use crate::models;
// // use parking_lot::{RwLock, RwLockWriteGuard, RwLockReadGuard};
use snafu::{ResultExt, Snafu, ensure};

use super::meta;

// FIELDS_INDEX_MAGIC_NUMBER is the file magic number for the fields index file.
const FIELDS_INDEX_MAGIC_NUMBER: [u8; 4] = [0, 6, 1, 3];

/// Error type for Shard.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("integer encode/decode error: {}", source))]
    IOError { source: std::io::Error },

    /// FieldOverflow is returned when too many fields are created on a measurement.
    #[snafu(display("field overflow"))]
    FieldOverflow,

    // FieldTypeConflict is returned when a new field already exists with a different type.
    #[snafu(display("field type conflict"))]
    FieldTypeConflict,

    // ErrFieldNotFound is returned when a field cannot be found.
    #[snafu(display("field not found"))]
    FieldNotFound,

    // ErrFieldUnmappedID is returned when the system is presented, during decode, with a field ID
    // there is no mapping for.
    #[snafu(display("field ID not mapped"))]
    FieldUnmappedID,

    // ErrUnknownFieldsFormat is returned when the fields index file is not identifiable by
    // the file's magic number.
    #[snafu(display("unknown field index format"))]
    UnknownFieldsFormat,

    // ErrUnknownFieldType is returned when the type of a field cannot be determined.
    #[snafu(display("unknown field type"))]
    UnknownFieldType,

    /// Prost encode error.
    #[snafu(display("prost error: {}", source))]
    ProstEncode { source: prost::EncodeError },

    /// Prost decode error.
    #[snafu(display("prost error: {}", source))]
    ProstDecode { source: prost::DecodeError },
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError { source: err }
    }
}

/// Result type for Shard.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// MeasurementFields holds the fields of a measurement and their stable ordinals.
#[derive(Default)]
pub struct MeasurementFields {
    fields: RwLock<HashMap<String, Field>>,
}

impl MeasurementFields {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field_keys(&self) -> Vec<String> {
        let mut keys = {
            let fields = self.fields.read();
            fields.keys().cloned().collect::<Vec<_>>()
        };
        keys.sort();
        keys
    }

    /// Estimates the memory footprint of this MeasurementFields, in bytes.
    pub fn bytes(&self) -> i64 {
        todo!("implement me");
    }

    /// Creates a field with a stable, nonzero ordinal, or verifies its type.
    pub fn create_field_if_not_exists(&self, name: &[u8], typ: FieldType) -> Result<()> {
        let name = std::str::from_utf8(name).map_err(|_| Error::UnknownFieldsFormat)?;
        {
            let fields = self.fields.read();
            if let Some(field) = fields.get(name) {
                ensure!(field.typ == typ, FieldTypeConflictSnafu);
                return Ok(());
            }
        }

        let mut fields = self.fields.write();
        // Recheck after releasing the read guard; another writer may have inserted it.
        if let Some(field) = fields.get(name) {
            ensure!(field.typ == typ, FieldTypeConflictSnafu);
            return Ok(());
        }
        let id = fields
            .values()
            .map(|field| field.id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(Error::FieldOverflow)?;
        fields.insert(
            name.to_owned(),
            Field {
                id,
                name: name.to_owned(),
                typ,
            },
        );
        Ok(())
    }

    pub fn field_n(&self) -> i64 {
        self.fields.read().len() as i64
    }

    pub fn field(&self, name: &str) -> Option<Field> {
        self.fields.read().get(name).cloned()
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.fields.read().contains_key(name)
    }

    pub fn field_bytes(&self, name: &[u8]) -> Option<Field> {
        self.field(std::str::from_utf8(name).ok()?)
    }

    pub fn ordinal(&self, name: &[u8]) -> Option<u8> {
        self.fields
            .read()
            .get(std::str::from_utf8(name).ok()?)
            .map(|field| field.id)
    }

    pub fn field_set(&self) -> HashMap<String, FieldType> {
        self.fields
            .read()
            .iter()
            .map(|(name, field)| (name.clone(), field.typ))
            .collect()
    }

    fn snapshot(&self) -> Vec<Field> {
        self.fields.read().values().cloned().collect()
    }

    pub fn for_each_field<F>(&self, mut f: F)
    where
        F: FnMut(&str, FieldType) -> bool,
    {
        // Callbacks can access the field set again; never invoke them under its lock.
        for field in self.snapshot() {
            if !f(&field.name, field.typ) {
                return;
            }
        }
    }
}
// MeasurementFieldSet represents a collection of fields by measurement.
// This safe for concurrent use.
#[derive(Default)]
pub struct MeasurementFieldSet {
    // path is the location to persist field sets
    path: PathBuf,
    inner: RwLock<MeasurementFieldSetInner>,
}

#[derive(Default)]
struct MeasurementFieldSetInner {
    fields: HashMap<String, Arc<MeasurementFields>>,
    // ephemeral counters for updating the file on disk
    memory_version: u64,
    written_version: u64,
}

impl MeasurementFieldSet {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let fs = Self {
            path: path.into(),
            ..Default::default()
        };

        fs.load()?;

        Ok(fs)
    }

    /// Estimates the memory footprint of this MeasurementFieldSet, in bytes.
    pub fn bytes(&self) -> usize {
        todo!("implement me");
    }

    /// Returns fields for a measurement by name.
    pub fn fields(&self, name: &[u8]) -> Option<Arc<MeasurementFields>> {
        let inner = self.inner.read();
        inner
            .fields
            .get(unsafe { std::str::from_utf8_unchecked(name) })
            .cloned()
    }

    /// Returns fields for a measurement by name.
    pub fn fields_by_string(&self, name: &str) -> Option<Arc<MeasurementFields>> {
        let inner = self.inner.read();
        inner.fields.get(name).cloned()
    }

    pub fn ordinal(&self, measurement: &[u8], field: &[u8]) -> Option<u8> {
        self.fields(measurement)?.ordinal(field)
    }

    /// Returns fields for a measurement by name.
    pub fn create_fields_if_not_exists(&self, name: &[u8]) -> Arc<MeasurementFields> {
        {
            let inner = self.inner.read();
            if let Some(mf) = inner
                .fields
                .get(unsafe { std::str::from_utf8_unchecked(name) })
            {
                return Arc::clone(mf);
            }
        }

        let mut inner = self.inner.write();
        let mf = {
            if let Some(mf) = inner
                .fields
                .get(unsafe { std::str::from_utf8_unchecked(name) })
            {
                Arc::clone(&mf)
            } else {
                let mf = Arc::new(MeasurementFields::new());
                inner.fields.insert(to_string!(name), Arc::clone(&mf));
                mf
            }
        };

        mf
    }

    /// Removes a field set for a measurement.
    pub fn delete(&self, name: &str) {
        let mut inner = self.inner.write();
        let _ = inner.fields.remove(name);
    }

    /// Executes fn and removes a field set from a measurement under lock.
    pub fn delete_with_lock<F, E>(&self, name: &[u8], mut f: F) -> Result<(), E>
    where
        F: FnMut() -> Result<(), E>,
    {
        let mut inner = self.inner.write();

        f()?;

        let _ = inner
            .fields
            .remove(unsafe { std::str::from_utf8_unchecked(name) });

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().fields.is_empty()
    }

    pub fn save(&self) -> Result<()> {
        // current version
        let mut v = 0;
        // Is the MeasurementFieldSet empty?
        let mut is_empty = false;
        // marshaled MeasurementFieldSet
        let b = {
            let mut inner = self.inner.write();

            inner.memory_version += 1;
            v = inner.memory_version;
            // If no fields left, remove the fields index file
            if inner.fields.is_empty() {
                is_empty = true;
                if self.path.exists() {
                    fs::remove_file(&self.path)?;
                }
                inner.written_version = inner.memory_version;
                Ok(vec![])
            } else {
                self.marshal_measurement_field_set_no_lock(&inner.fields)
            }
        }?;

        if is_empty {
            return Ok(());
        }

        // Write the new index to a temp file and rename when it's sync'd
        // if it is still the most recent memoryVersion of the MeasurementFields
        let path = self.path.to_str().unwrap().to_string() + "." + &v.to_string() + ".tmp";

        let mut fd = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let stat = fd.metadata()?;
        stat.permissions().set_mode(0o666);
        if let Err(e) = fd.write_all(&FIELDS_INDEX_MAGIC_NUMBER) {
            let _ = fs::remove_file(&path);
            return Err(e.into());
        }

        if let Err(e) = fd.write_all(&b) {
            let _ = fs::remove_file(&path);
            return Err(e.into());
        }

        if let Err(e) = fd.sync_all() {
            let _ = fs::remove_file(&path);
            return Err(e.into());
        }

        //close file handle before renaming to support Windows
        drop(fd);

        let mut inner = self.inner.write();
        // Check if a later modification and save of fields has superseded ours
        // If so, we are successfully done! We were beaten by a later call
        // to this function
        if inner.written_version > v {
            return Ok(());
        }

        if let Err(e) = fs::rename(&path, &self.path) {
            let _ = fs::remove_file(&path);
            return Err(e.into());
        }

        // TODO: SyncDir(self.path) ?

        // Update the written version to the current version
        inner.written_version = v;

        Ok(())
    }

    fn marshal_measurement_field_set_no_lock(
        &self,
        fields: &HashMap<String, Arc<MeasurementFields>>,
    ) -> Result<Vec<u8>> {
        let mut pb = meta::MeasurementFieldSet {
            measurements: Vec::with_capacity(fields.len()),
        };

        for (name, mf) in fields.iter() {
            let mut imf = meta::MeasurementFields {
                name: name.as_bytes().to_vec(),
                fields: Vec::with_capacity(mf.field_n() as usize),
            };

            for field in mf.snapshot() {
                imf.fields.push(meta::Field {
                    name: field.name.into_bytes(),
                    typ: i32::from(field.typ),
                    id: u32::from(field.id),
                });
            }

            pb.measurements.push(imf);
        }

        Ok(pb.encode_to_vec())
    }

    fn load(&self) -> Result<()> {
        let mut fd = match std::fs::OpenOptions::new().read(true).open(&self.path) {
            Ok(fd) => fd,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Ok(());
                } else {
                    return Err(e.into());
                }
            }
        };

        let mut magic = [0u8; 4];
        fd.read_exact(&mut magic)?;

        ensure!(magic == FIELDS_INDEX_MAGIC_NUMBER, UnknownFieldsFormatSnafu);

        let mut b = Vec::new();
        let _ = fd.read_to_end(&mut b)?;

        let pb = meta::MeasurementFieldSet::decode(&mut std::io::Cursor::new(&mut b))
            .context(ProstDecodeSnafu)?;

        drop(fd);
        let mut measurements = HashMap::new();
        let mut upgraded = false;
        for mut measurement in pb.measurements {
            ensure!(
                measurement.fields.len() <= u8::MAX as usize,
                FieldOverflowSnafu
            );
            let mut used = [false; 256];
            for field in &measurement.fields {
                ensure!(field.id <= u32::from(u8::MAX), UnknownFieldsFormatSnafu);
                if field.id != 0 {
                    ensure!(!used[field.id as usize], UnknownFieldsFormatSnafu);
                    used[field.id as usize] = true;
                }
            }
            // Old protobuf records omit id. Assign deterministically and preserve
            // any existing IDs when loading a partially migrated record set.
            measurement.fields.sort_by(|a, b| a.name.cmp(&b.name));
            let mut fields = HashMap::new();
            for mut field in measurement.fields {
                if field.id == 0 {
                    let id = (1..256).find(|&id| !used[id]).ok_or(Error::FieldOverflow)?;
                    used[id] = true;
                    field.id = id as u32;
                    upgraded = true;
                }
                let name = String::from_utf8(field.name).map_err(|_| Error::UnknownFieldsFormat)?;
                let f = Field {
                    id: field.id as u8,
                    name: name.clone(),
                    typ: field.typ as FieldType,
                };
                ensure!(fields.insert(name, f).is_none(), UnknownFieldsFormatSnafu);
            }
            let name =
                String::from_utf8(measurement.name).map_err(|_| Error::UnknownFieldsFormat)?;
            let set = Arc::new(MeasurementFields {
                fields: RwLock::new(fields),
            });
            ensure!(
                measurements.insert(name, set).is_none(),
                UnknownFieldsFormatSnafu
            );
        }
        {
            let mut inner = self.inner.write();
            inner.fields = measurements;
        }
        // save() takes inner itself. Migration must run after releasing its guard.
        if upgraded {
            self.save()?;
        }
        Ok(())
    }
}

/// Field represents a series field. All of the fields must be hashable.
#[derive(Clone)]
pub struct Field {
    pub id: u8,
    pub name: String,
    pub typ: FieldType,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::models::{BOOL, FLOAT, INTEGER, STRING, UNSIGNED};
    use prost::Message;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn field_type_conflict_semantics_are_unchanged() {
        let fields = MeasurementFields::new();
        fields.create_field_if_not_exists(b"value", FLOAT).unwrap();
        fields.create_field_if_not_exists(b"value", FLOAT).unwrap();
        fields
            .create_field_if_not_exists(b"count", INTEGER)
            .unwrap();

        assert!(matches!(
            fields.create_field_if_not_exists(b"value", INTEGER),
            Err(Error::FieldTypeConflict)
        ));
    }

    #[test]
    fn persisted_field_type_discriminants_round_trip_unchanged() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("fields.idx");
        let field_set = MeasurementFieldSet::new(&path).unwrap();
        let fields = field_set.create_fields_if_not_exists(b"cpu");
        let expected = [
            ("float", FLOAT),
            ("integer", INTEGER),
            ("unsigned", UNSIGNED),
            ("string", STRING),
            ("boolean", BOOL),
        ];

        for (name, field_type) in expected {
            fields
                .create_field_if_not_exists(name.as_bytes(), field_type)
                .unwrap();
        }
        field_set.save().unwrap();

        let bytes = fs::read(&path).unwrap();
        assert_eq!(
            &bytes[..FIELDS_INDEX_MAGIC_NUMBER.len()],
            &FIELDS_INDEX_MAGIC_NUMBER
        );
        let persisted = meta::MeasurementFieldSet::decode(&mut std::io::Cursor::new(
            &bytes[FIELDS_INDEX_MAGIC_NUMBER.len()..],
        ))
        .unwrap();
        let persisted_types = persisted.measurements[0]
            .fields
            .iter()
            .map(|field| (to_string!(field.name), field.typ))
            .collect::<HashMap<_, _>>();
        for (name, field_type) in expected {
            assert_eq!(persisted_types[name], i32::from(field_type));
        }

        let reopened = MeasurementFieldSet::new(&path).unwrap();
        let reopened_fields = reopened.fields(b"cpu").unwrap();
        for (name, field_type) in expected {
            assert_eq!(reopened_fields.field(name).unwrap().typ, field_type);
            assert_eq!(
                reopened_fields.ordinal(name.as_bytes()),
                fields.ordinal(name.as_bytes())
            );
        }
    }

    #[test]
    fn legacy_field_ordinals_are_upgraded_once_in_name_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("fields.idx");
        let legacy = meta::MeasurementFieldSet {
            measurements: vec![meta::MeasurementFields {
                name: b"cpu".to_vec(),
                fields: vec![
                    meta::Field {
                        name: b"z".to_vec(),
                        typ: i32::from(FLOAT),
                        id: 0,
                    },
                    meta::Field {
                        name: b"a".to_vec(),
                        typ: i32::from(INTEGER),
                        id: 0,
                    },
                ],
            }],
        };
        let mut bytes = FIELDS_INDEX_MAGIC_NUMBER.to_vec();
        bytes.extend(legacy.encode_to_vec());
        fs::write(&path, bytes).unwrap();
        let loaded = MeasurementFieldSet::new(&path).unwrap();
        assert_eq!(loaded.ordinal(b"cpu", b"a"), Some(1));
        assert_eq!(loaded.ordinal(b"cpu", b"z"), Some(2));
        let upgraded = fs::read(&path).unwrap();
        let reopened = MeasurementFieldSet::new(&path).unwrap();
        assert_eq!(reopened.ordinal(b"cpu", b"a"), Some(1));
        assert_eq!(upgraded, fs::read(&path).unwrap());
        reopened
            .fields(b"cpu")
            .unwrap()
            .create_field_if_not_exists(b"b", FLOAT)
            .unwrap();
        reopened.save().unwrap();
        let reopened = MeasurementFieldSet::new(&path).unwrap();
        assert_eq!(reopened.ordinal(b"cpu", b"z"), Some(2));
        assert_eq!(reopened.ordinal(b"cpu", b"b"), Some(3));
    }

    #[test]
    fn field_ordinals_reject_overflow_and_reuse_existing_ids() {
        let fields = MeasurementFields::new();
        for id in 1..=255 {
            let name = format!("field_{id}");
            fields
                .create_field_if_not_exists(name.as_bytes(), FLOAT)
                .unwrap();
            assert_eq!(fields.ordinal(name.as_bytes()), Some(id));
        }
        fields
            .create_field_if_not_exists(b"field_1", FLOAT)
            .unwrap();
        assert!(matches!(
            fields.create_field_if_not_exists(b"overflow", FLOAT),
            Err(Error::FieldOverflow)
        ));
        assert_eq!(fields.field_n(), 255);
    }

    #[test]
    fn concurrent_field_creation_preserves_unique_stable_ordinals() {
        let fields = MeasurementFields::new();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for n in 0..64 {
                        let name = format!("field_{n}");
                        fields
                            .create_field_if_not_exists(name.as_bytes(), INTEGER)
                            .unwrap();
                        assert!(fields.has_field(&name));
                    }
                });
            }
        });
        let mut ids = fields
            .snapshot()
            .iter()
            .map(|field| field.id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (1..=64).collect::<Vec<_>>());
        fields.for_each_field(|name, typ| {
            fields
                .create_field_if_not_exists(name.as_bytes(), typ)
                .unwrap();
            true
        });
    }

    #[test]
    fn invalid_persisted_ordinals_are_rejected_without_rewriting_metadata() {
        for ids in [[1, 1], [1, 256]] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("fields.idx");
            let pb = meta::MeasurementFieldSet {
                measurements: vec![meta::MeasurementFields {
                    name: b"cpu".to_vec(),
                    fields: ids
                        .into_iter()
                        .enumerate()
                        .map(|(n, id)| meta::Field {
                            name: format!("field_{n}").into_bytes(),
                            typ: i32::from(INTEGER),
                            id,
                        })
                        .collect(),
                }],
            };
            let mut bytes = FIELDS_INDEX_MAGIC_NUMBER.to_vec();
            bytes.extend(pb.encode_to_vec());
            fs::write(&path, &bytes).unwrap();
            assert!(matches!(
                MeasurementFieldSet::new(&path),
                Err(Error::UnknownFieldsFormat)
            ));
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }
}
