use std::sync::Arc;

use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use reefsdb::{
    index::tsi1::index_file::tests::must_generate_index_file,
    series::series_file::tests::must_open_series_file,
};

fn bench_series_file_tag_value_series(c: &mut Criterion) {
    let mut g = c.benchmark_group("bench_series_file_tag_value_series");
    // golang: 200 ns/op
    // 44.451 ns
    // 207.16 ns (use Bytes)
    // 43.150 ns (use Bytes, but iter use &[u8])
    run_tag_value_series(&mut g, 1, 2, 3);
    // golang: 328.3 ns/op
    // 46.151 ns
    // 221.21 ns (use Bytes)
    // 47.402 ns (use Bytes, but iter use &[u8])
    run_tag_value_series(&mut g, 10, 5, 5);
    // golang:
    // 69.285 ns
    //  (use Bytes)  // TODO: implement me
    run_tag_value_series(&mut g, 10, 7, 7);
}

fn run_tag_value_series(
    g: &mut BenchmarkGroup<WallTime>,
    measurement_n: i32,
    tag_n: i32,
    value_n: i32,
) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let idx = rt.block_on(async {
        let tmp_sfile = must_open_series_file();
        let sfile = Arc::new(tmp_sfile.into());

        must_generate_index_file(&sfile, measurement_n, tag_n, value_n)
    });

    g.bench_function(
        &format!(
            "measurement_n={}/tag_n={}/value_n={}",
            measurement_n, tag_n, value_n
        ),
        move |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    let e = idx.tag_value_elem(
                        "measurement0".as_bytes(),
                        "key0".as_bytes(),
                        "value0".as_bytes(),
                    );
                    assert!(e.is_some(), "expected element");
                    assert_ne!(e.unwrap().series_n(), 0, "expected series");
                })
        },
    );
}

criterion_group!(benches, bench_series_file_tag_value_series);
criterion_main!(benches);
