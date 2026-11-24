use std::{
    collections::BTreeMap,
    io::{BufWriter, Write},
};

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use reefsdb::{
    index::tsi1::tag_block::{self, *},
    series::series_set,
};

async fn encode_key_and_value<'a, W: Write>(
    enc: &mut TagBlockEncoder<'a, W>,
    kvs: &'a BTreeMap<String, Vec<String>>,
) {
    // Write block.
    for (k, values) in kvs.iter() {
        if let Err(e) = enc.encode_key(k.as_bytes(), false) {
            panic!("encode_key failed: {:?}", e);
        }
        for v in values.iter() {
            if let Err(e) = enc.encode_value(
                v.as_bytes(),
                false,
                &series_set::SeriesIDSet::new(vec![1].as_slice()),
            ) {
                panic!("encode_value failed: {:?}", e);
            }
        }
    }

    // FLush encoder.
    enc.close().unwrap();
}

fn key_value_pairs(tag_n: i32, value_n: i32) -> BTreeMap<String, Vec<String>> {
    let mut kvs = BTreeMap::new();
    for i in 0..tag_n {
        let key = format!("{:08}", i);
        let mut values = vec![];
        for j in 0..value_n {
            values.push(format!("{:08}", j));
        }
        kvs.insert(key, values);
    }
    kvs
}

fn run_benchmarks(c: &mut Criterion, tag_n: i32, value_n: i32) {
    let kvs = key_value_pairs(tag_n, value_n);

    let mut bw = BufWriter::new(Vec::new());
    let mut enc = tag_block::TagBlockEncoder::new(&mut bw);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(encode_key_and_value(&mut enc, &kvs));

    // Unmarshal into a block.
    let mut blk = tag_block::TagBlock::default();
    let inner_data = bw.into_inner().unwrap();
    if let Err(e) = blk.unmarshal_binary(&Bytes::from(inner_data)) {
        panic!("unmarshal_binary failed: {:?}", e);
    }

    // Benchmark lookups.
    let (key, value) = (format!("{:08}", 0), format!("{:08}", 0));
    c.bench_function(
        &format!(" tag_block_series_n_{}_{}", tag_n, value_n),
        move |b| {
            b.iter(|| {
                if let Some(val) = blk.tag_value_elem(key.as_bytes(), value.as_bytes()) {
                    assert_eq!(
                        val.series_n(),
                        1,
                        "unexpected series ids: {:?}",
                        val.series_n()
                    );
                } else {
                    panic!("expected element");
                }
            })
        },
    );
}

// golang: 35.04 ns/op
// 24.649 ns
// 191.52 ns (use Bytes)
// 21.209 ns (use Bytes, but iter use &[u8])
fn tag_block_series_n_10_1000(c: &mut Criterion) {
    run_benchmarks(c, 10, 1000);
}

// golang: 34.99 ns/op
// 69.811 ns
// 451.06 ns (use Bytes)
// 62.200 ns (use Bytes, but iter use &[u8])
fn tag_block_series_n_100_1000(c: &mut Criterion) {
    run_benchmarks(c, 100, 1000);
}

// golang: 34.49 ns/op
// 24.282 ns
// 195.25 ns (use Bytes)
// 21.692 ns (use Bytes, but iter use &[u8])
fn tag_block_series_n_1000_1000(c: &mut Criterion) {
    run_benchmarks(c, 1000, 1000);
}
// golang: 34.00 ns/op
// 24.161 ns
// 192.22 ns (use Bytes)
// 21.586 ns (use Bytes, but iter use &[u8])
fn tag_block_series_n_1_1000000(c: &mut Criterion) {
    run_benchmarks(c, 1, 1000000);
}

criterion_group!(
    benches,
    tag_block_series_n_10_1000,
    tag_block_series_n_100_1000,
    tag_block_series_n_1000_1000,
    tag_block_series_n_1_1000000,
);
criterion_main!(benches);
