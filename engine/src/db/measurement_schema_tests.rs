use std::{path::Path, sync::Arc, thread};

use crate::{
    DefaultTokioExecutor, DefaultTokioExecutorOptions, EngineOptions, MEASUREMENT_SCHEMA_FILE_NAME,
    MeasurementSchema, MeasurementSchemaError, MeasurementSchemaRegistry, ReefsDb, SeriesFile,
    StorageOptions,
    models::{FLOAT, INTEGER, UNSIGNED},
};

fn test_executor() -> crate::ExecutorRef {
    Arc::new(DefaultTokioExecutor::new(DefaultTokioExecutorOptions::default()).unwrap())
}

fn open_series_file(path: &Path, executor: crate::ExecutorRef) -> Arc<SeriesFile> {
    let mut series_file = SeriesFile::new_with_executor(path, executor);
    series_file.open().unwrap();
    Arc::new(series_file)
}

fn new_shard(
    root: &Path,
    shard_id: u64,
    series_file: Arc<SeriesFile>,
    schemas: Arc<MeasurementSchemaRegistry>,
    executor: crate::ExecutorRef,
) -> Arc<ReefsDb> {
    let shard_root = root.join(format!("shard-{shard_id}"));
    let mut options =
        EngineOptions::new_with_executor(StorageOptions::default(), Arc::clone(&executor));
    options.shard_id = shard_id;
    options.compaction_disabled = true;
    options.monitor_disabled = true;
    options.config.wal_dir = shard_root.join("wal").to_string_lossy().into_owned();
    Arc::new(
        ReefsDb::new_with_executor(
            shard_root.join("data"),
            shard_root.join("wal"),
            shard_root.join("vlog"),
            shard_root.join("vlog-index-wal"),
            series_file,
            schemas,
            options,
            executor,
        )
        .unwrap(),
    )
}

fn open_shard(
    root: &Path,
    shard_id: u64,
    series_file: Arc<SeriesFile>,
    schemas: Arc<MeasurementSchemaRegistry>,
    executor: crate::ExecutorRef,
) -> Arc<ReefsDb> {
    let shard = new_shard(root, shard_id, series_file, schemas, executor);
    shard.open().unwrap();
    shard
}

fn canonical_registry(series_file: &SeriesFile) -> Arc<MeasurementSchemaRegistry> {
    Arc::new(
        MeasurementSchemaRegistry::open(series_file.path().join(MEASUREMENT_SCHEMA_FILE_NAME))
            .unwrap(),
    )
}

#[test]
fn explicit_empty_measurement_is_visible_with_zero_series_and_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_path = root.path().join("_series");
    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard(
        root.path(),
        1,
        Arc::clone(&series_file),
        Arc::clone(&schemas),
        Arc::clone(&executor),
    );
    let declared = MeasurementSchema::new(
        "cpu",
        vec!["host".to_string(), "region".to_string()],
        vec![("usage".to_string(), FLOAT)],
    )
    .unwrap();

    assert!(shard.create_measurement_schema(declared.clone()).unwrap());
    assert!(!shard.create_measurement_schema(declared.clone()).unwrap());
    assert_eq!(shard.series_n(), 0);
    assert_eq!(shard.measurement_names().unwrap(), vec![b"cpu".to_vec()]);
    let (fields, dimensions) = shard.field_dimensions(&[b"cpu"]).unwrap();
    assert_eq!(fields.get("usage"), Some(&FLOAT));
    assert_eq!(dimensions.len(), 2);
    assert!(dimensions.contains("host"));
    assert!(dimensions.contains("region"));

    shard.close().unwrap();
    drop(shard);
    drop(schemas);
    series_file.close().unwrap();
    drop(series_file);

    let reopened_series = open_series_file(&series_path, Arc::clone(&executor));
    let reopened_schemas = canonical_registry(&reopened_series);
    let reopened = open_shard(
        root.path(),
        1,
        Arc::clone(&reopened_series),
        reopened_schemas,
        executor,
    );
    assert_eq!(reopened.series_n(), 0);
    assert_eq!(reopened.measurement_schema("cpu").unwrap(), Some(declared));
    assert_eq!(reopened.measurement_names().unwrap(), vec![b"cpu".to_vec()]);

    reopened.close().unwrap();
    reopened_series.close().unwrap();
}

#[test]
fn declared_schema_and_write_first_evolution_converge_without_partial_conflict() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_path = root.path().join("_series");
    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard(
        root.path(),
        1,
        Arc::clone(&series_file),
        Arc::clone(&schemas),
        Arc::clone(&executor),
    );
    shard
        .create_measurement_schema(
            MeasurementSchema::new(
                "cpu",
                vec!["host".to_string()],
                vec![("a".to_string(), FLOAT)],
            )
            .unwrap(),
        )
        .unwrap();

    shard
        .write_points_string(&["cpu,host=a,region=west a=1.0,b=2i 10"])
        .unwrap();
    assert_eq!(shard.series_n(), 1);
    let evolved = shard.measurement_schema("cpu").unwrap().unwrap();
    assert_eq!(evolved.tag_keys().len(), 2);
    assert_eq!(evolved.fields().get("a"), Some(&FLOAT));
    assert_eq!(evolved.fields().get("b"), Some(&INTEGER));
    let persisted = std::fs::read(schemas.path()).unwrap();
    assert!(
        !persisted
            .windows(b"west".len())
            .any(|window| window == b"west"),
        "concrete tag values must not be stored in the schema registry"
    );

    assert!(
        shard
            .write_points_string(&["cpu,host=a,rack=r1 a=\"wrong\",c=3i 20"])
            .is_err()
    );
    assert_eq!(
        shard.measurement_schema("cpu").unwrap(),
        Some(evolved.clone())
    );

    shard.close().unwrap();
    drop(shard);
    drop(schemas);
    series_file.close().unwrap();
    drop(series_file);

    let reopened_series = open_series_file(&series_path, Arc::clone(&executor));
    let reopened_schemas = canonical_registry(&reopened_series);
    let reopened = open_shard(
        root.path(),
        1,
        Arc::clone(&reopened_series),
        reopened_schemas,
        executor,
    );
    assert_eq!(reopened.measurement_schema("cpu").unwrap(), Some(evolved));
    reopened.close().unwrap();
    reopened_series.close().unwrap();
}

#[test]
fn write_batch_merges_series_schema_and_keeps_measurements_isolated() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard(root.path(), 1, Arc::clone(&series_file), schemas, executor);

    shard
        .write_points_string(&[
            "cpu,host=a a=1.0 10",
            "cpu,host=b,region=west b=2i 20",
            "memory,zone=z free=3u 30",
        ])
        .unwrap();
    assert_eq!(shard.series_n(), 3);
    assert_eq!(
        shard.measurement_names().unwrap(),
        vec![b"cpu".to_vec(), b"memory".to_vec()]
    );
    let cpu = shard.measurement_schema("cpu").unwrap().unwrap();
    assert_eq!(cpu.tag_keys().len(), 2);
    assert_eq!(cpu.fields().get("a"), Some(&FLOAT));
    assert_eq!(cpu.fields().get("b"), Some(&INTEGER));
    let memory = shard.measurement_schema("memory").unwrap().unwrap();
    assert_eq!(
        memory.tag_keys().iter().cloned().collect::<Vec<_>>(),
        vec!["zone"]
    );
    assert_eq!(memory.fields().get("free"), Some(&UNSIGNED));
    assert!(!memory.fields().contains_key("a"));

    shard.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn peer_shards_share_one_registry_and_concurrent_evolution_loses_no_updates() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard_one = open_shard(
        root.path(),
        1,
        Arc::clone(&series_file),
        Arc::clone(&schemas),
        Arc::clone(&executor),
    );
    let shard_two = open_shard(
        root.path(),
        2,
        Arc::clone(&series_file),
        Arc::clone(&schemas),
        executor,
    );
    assert!(Arc::ptr_eq(
        &shard_one.measurement_schema_registry(),
        &shard_two.measurement_schema_registry()
    ));

    let first = {
        let shard = Arc::clone(&shard_one);
        thread::spawn(move || shard.write_points_string(&["cpu,host=a a=1.0 10"]).unwrap())
    };
    let second = {
        let shard = Arc::clone(&shard_two);
        thread::spawn(move || {
            shard
                .write_points_string(&["cpu,region=west b=2i 20"])
                .unwrap()
        })
    };
    first.join().unwrap();
    second.join().unwrap();

    let schema = schemas.measurement_schema("cpu").unwrap();
    assert_eq!(schema.tag_keys().len(), 2);
    assert_eq!(schema.fields().get("a"), Some(&FLOAT));
    assert_eq!(schema.fields().get("b"), Some(&INTEGER));

    shard_one.close().unwrap();
    shard_two.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn absent_registry_bootstraps_and_merges_existing_metadata_from_all_shards() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_path = root.path().join("_series");
    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let discarded = Arc::new(
        MeasurementSchemaRegistry::open(root.path().join("discarded-schema.idx")).unwrap(),
    );
    let shard_one = open_shard(
        root.path(),
        1,
        Arc::clone(&series_file),
        Arc::clone(&discarded),
        Arc::clone(&executor),
    );
    let shard_two = open_shard(
        root.path(),
        2,
        Arc::clone(&series_file),
        Arc::clone(&discarded),
        Arc::clone(&executor),
    );
    let shard_three = open_shard(
        root.path(),
        3,
        Arc::clone(&series_file),
        discarded,
        Arc::clone(&executor),
    );
    shard_one
        .write_points_string(&["cpu,host=a a=1.0 10"])
        .unwrap();
    shard_two
        .write_points_string(&["cpu,region=west b=2i 20"])
        .unwrap();
    shard_three
        .write_points_string(&["memory used=3u 30"])
        .unwrap();
    shard_one.close().unwrap();
    shard_two.close().unwrap();
    shard_three.close().unwrap();
    drop(shard_one);
    drop(shard_two);
    drop(shard_three);
    series_file.close().unwrap();
    drop(series_file);

    let registry_path = series_path.join(MEASUREMENT_SCHEMA_FILE_NAME);
    assert!(!registry_path.exists());
    let reopened_series = open_series_file(&series_path, Arc::clone(&executor));
    let bootstrapping =
        Arc::new(MeasurementSchemaRegistry::open_for_legacy_bootstrap(&registry_path).unwrap());
    let reopened_one = open_shard(
        root.path(),
        1,
        Arc::clone(&reopened_series),
        Arc::clone(&bootstrapping),
        Arc::clone(&executor),
    );
    let reopened_two = open_shard(
        root.path(),
        2,
        Arc::clone(&reopened_series),
        Arc::clone(&bootstrapping),
        Arc::clone(&executor),
    );
    let reopened_three = open_shard(
        root.path(),
        3,
        Arc::clone(&reopened_series),
        Arc::clone(&bootstrapping),
        executor,
    );
    assert!(bootstrapping.needs_legacy_bootstrap());
    assert!(bootstrapping.measurement_names().is_empty());
    let mut discovered = Vec::new();
    for shard in [&reopened_one, &reopened_two, &reopened_three] {
        discovered.extend(shard.legacy_measurement_schemas_for_bootstrap().unwrap());
        assert!(bootstrapping.measurement_names().is_empty());
    }
    assert!(bootstrapping.complete_legacy_bootstrap(discovered).unwrap());
    let schema = bootstrapping.measurement_schema("cpu").unwrap();
    assert_eq!(schema.tag_keys().len(), 2);
    assert_eq!(schema.fields().get("a"), Some(&FLOAT));
    assert_eq!(schema.fields().get("b"), Some(&INTEGER));
    assert_eq!(
        bootstrapping
            .measurement_schema("memory")
            .unwrap()
            .fields()
            .get("used"),
        Some(&UNSIGNED)
    );
    assert!(!bootstrapping.needs_legacy_bootstrap());

    reopened_one.close().unwrap();
    reopened_two.close().unwrap();
    reopened_three.close().unwrap();
    reopened_series.close().unwrap();
    drop(bootstrapping);
    let reopened_registry =
        MeasurementSchemaRegistry::open_for_legacy_bootstrap(registry_path).unwrap();
    assert!(!reopened_registry.needs_legacy_bootstrap());
    assert_eq!(reopened_registry.measurement_schema("cpu"), Some(schema));
}

#[test]
fn legacy_cross_shard_field_type_conflict_is_rejected_without_completion() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_path = root.path().join("_series");
    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let float_registry =
        Arc::new(MeasurementSchemaRegistry::open(root.path().join("float-schema.idx")).unwrap());
    let string_registry =
        Arc::new(MeasurementSchemaRegistry::open(root.path().join("string-schema.idx")).unwrap());
    let float_shard = open_shard(
        root.path(),
        1,
        Arc::clone(&series_file),
        float_registry,
        Arc::clone(&executor),
    );
    let string_shard = open_shard(
        root.path(),
        2,
        Arc::clone(&series_file),
        string_registry,
        Arc::clone(&executor),
    );
    float_shard
        .write_points_string(&["cpu,host=a value=1.0 10"])
        .unwrap();
    string_shard
        .write_points_string(&["cpu,host=b value=\"bad\" 20"])
        .unwrap();
    float_shard.close().unwrap();
    string_shard.close().unwrap();
    drop(float_shard);
    drop(string_shard);
    series_file.close().unwrap();
    drop(series_file);

    let registry_path = series_path.join(MEASUREMENT_SCHEMA_FILE_NAME);
    let reopened_series = open_series_file(&series_path, Arc::clone(&executor));
    let bootstrapping =
        Arc::new(MeasurementSchemaRegistry::open_for_legacy_bootstrap(&registry_path).unwrap());
    let reopened_float = open_shard(
        root.path(),
        1,
        Arc::clone(&reopened_series),
        Arc::clone(&bootstrapping),
        Arc::clone(&executor),
    );
    let reopened_string = open_shard(
        root.path(),
        2,
        Arc::clone(&reopened_series),
        Arc::clone(&bootstrapping),
        executor,
    );
    let mut discovered = reopened_float
        .legacy_measurement_schemas_for_bootstrap()
        .unwrap();
    discovered.extend(
        reopened_string
            .legacy_measurement_schemas_for_bootstrap()
            .unwrap(),
    );
    assert!(matches!(
        bootstrapping.complete_legacy_bootstrap(discovered),
        Err(MeasurementSchemaError::FieldTypeConflict { .. })
    ));
    assert!(bootstrapping.needs_legacy_bootstrap());
    assert!(bootstrapping.measurement_names().is_empty());

    reopened_float.close().unwrap();
    reopened_string.close().unwrap();
    reopened_series.close().unwrap();
}
