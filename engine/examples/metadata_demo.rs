// End-to-end demo of the metadata-retrieval interfaces (DataFusion-oriented).
//
// Opens a ReefsDb under a data directory (default: $HOME/.cache/reefsdb-meta-test),
// writes a few points across two measurements, then exercises every metadata
// API: measurement listing, Arrow schema, field/dimension sets, tag keys/values,
// series keys, and predicate pushdown.
//
// Run with: cargo run -p reefsdb --example metadata_demo [data_dir]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use reefsdb::{
    ColumnKind, ColumnRef, CompareOp, EngineOptions, Fields, MEASUREMENT_SCHEMA_FILE_NAME,
    MeasurementSchemaRegistry, Point, RawVal, ReefsDb, SeriesFile, StoragePredicate,
    StorageValueExpr, Tags, Time, Val, must_new_point,
};

fn to_str(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn strs(items: &[Vec<u8>]) -> Vec<String> {
    items.iter().map(|b| to_str(b)).collect()
}

fn point(name: &str, tags: &[(&str, &str)], fields: Vec<(&str, RawVal)>, ts: i64) -> Point {
    let tag_map: HashMap<Vec<u8>, Vec<u8>> = tags
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect();
    let field_map: HashMap<String, RawVal> = fields
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    must_new_point(
        name.to_string(),
        Tags::from(tag_map),
        Fields(field_map),
        Time::from_timestamp(ts, 0),
    )
}

fn tag_eq(key: &str, value: &str) -> StoragePredicate {
    StoragePredicate::Compare {
        op: CompareOp::Equal,
        lhs: StorageValueExpr::Column(ColumnRef::new(key, ColumnKind::Tag)),
        rhs: StorageValueExpr::Literal(Val::String(value.to_string())),
    }
}

fn main() {
    let base = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".cache/reefsdb-meta-test")
        });

    // Fresh data dir each run.
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    println!("== data dir: {} ==\n", base.display());

    // --- open a standalone ReefsDb ---
    let mut sfile = SeriesFile::new(base.join("_series"));
    sfile.open().unwrap();
    let sfile = Arc::new(sfile);
    let measurement_schemas = Arc::new(
        MeasurementSchemaRegistry::open(base.join("_series").join(MEASUREMENT_SCHEMA_FILE_NAME))
            .unwrap(),
    );

    let mut opt = EngineOptions::default();
    opt.shard_id = 1;
    opt.config.wal_dir = base.join("wal").to_str().unwrap().to_string();

    let reefsdb = Arc::new(
        ReefsDb::new(
            base.join("data/mydb/1"),
            base.join("wal"),
            base.join("vlog"),
            base.join("vlog_index_wal"),
            Arc::clone(&sfile),
            measurement_schemas,
            opt,
        )
        .unwrap(),
    );
    reefsdb.open().unwrap();

    // --- write points: cpu (2 series) + mem (1 series) ---
    let mut pts = vec![
        point(
            "cpu",
            &[("host", "a"), ("region", "east")],
            vec![("usage", RawVal::F64(1.0)), ("count", RawVal::I64(2))],
            100,
        ),
        point(
            "cpu",
            &[("host", "b"), ("region", "west")],
            vec![("usage", RawVal::F64(2.0))],
            101,
        ),
        point(
            "mem",
            &[("host", "a")],
            vec![("used", RawVal::I64(10))],
            102,
        ),
    ];
    reefsdb.write_points(&mut pts).unwrap();
    println!("wrote {} points; series_n = {}\n", 3, reefsdb.series_n());

    // --- 1. SHOW MEASUREMENTS ---
    println!(
        "measurement_names: {:?}",
        strs(&reefsdb.measurement_names().unwrap())
    );

    // --- 2. Arrow schema (DataFusion TableProvider::schema) ---
    println!("\narrow_schema(cpu):");
    for f in reefsdb.arrow_schema(b"cpu").unwrap().fields() {
        println!(
            "  {:<8} {:?} (nullable={})",
            f.name(),
            f.data_type(),
            f.is_nullable()
        );
    }

    // --- 3. field_dimensions (fields + tag dimensions) ---
    let (fields, dims) = reefsdb.field_dimensions(&[b"cpu"]).unwrap();
    let mut fs: Vec<_> = fields.iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
    fs.sort();
    let mut ds: Vec<_> = dims.iter().cloned().collect();
    ds.sort();
    println!(
        "\nfield_dimensions(cpu): fields={:?} dimensions={:?}",
        fs, ds
    );

    // --- 4. SHOW TAG KEYS / TAG VALUES ---
    println!("\ntag_keys(cpu): {:?}", reefsdb.tag_keys(b"cpu").unwrap());
    println!(
        "tag_values(cpu, [host,region]): {:?}",
        reefsdb.tag_values(b"cpu", &["host", "region"]).unwrap()
    );

    // --- 5. SHOW SERIES ---
    println!(
        "\nmeasurement_series_keys(cpu): {:?}",
        strs(&reefsdb.measurement_series_keys(b"cpu").unwrap())
    );

    // --- 6. predicate pushdown ---
    let p = tag_eq("host", "a");
    println!(
        "\nseries_keys_by_expr(cpu, host='a'): {:?}",
        strs(
            &reefsdb
                .series_keys_by_storage_predicate(b"cpu", Some(&p))
                .unwrap()
        )
    );
    println!(
        "measurement_names_by_predicate(host='a'): {:?}",
        strs(
            &reefsdb
                .measurement_names_by_storage_predicate(Some(&p))
                .unwrap()
        )
    );
    let pr = tag_eq("region", "east");
    println!(
        "series_keys_by_expr(cpu, region='east'): {:?}",
        strs(
            &reefsdb
                .series_keys_by_storage_predicate(b"cpu", Some(&pr))
                .unwrap()
        )
    );

    println!("\nOK");
}
