use std::{collections::HashMap, sync::Arc};

use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use reefsdb::{
    index::tsi1::log_file::tests::must_open_log_file,
    series::{series_file::tests::must_open_series_file, series_set},
};
use tokio_util::sync::CancellationToken;

fn run_log_file_add_series_list_benchmarks(
    g: &mut BenchmarkGroup<WallTime>,
    measurement_n: usize,
    series_key_n: usize,
    series_value_n: usize,
) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let f = rt.block_on(async {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());
        must_open_log_file(&sfile)
    });

    let series_set = Arc::new(series_set::SeriesIDSet::new(&[]));

    struct Datum {
        name: Vec<u8>,
        tags: reefsdb::models::points::Tags,
    }

    // Pre-generate everything.
    let mut data = Vec::<Datum>::new();

    let tag_value_n = series_value_n.pow(series_key_n as u32);

    // add series to all measurements
    for i in 0..measurement_n {
        let name = format!("measurement{}", i);

        for j in 0..tag_value_n {
            let mut tags = reefsdb::models::points::Tags::default();

            for k in 0..series_key_n {
                let key = format!("key{}", k);
                let value = format!(
                    "value{}",
                    j / (series_value_n.pow(k as u32)) % series_value_n
                );
                tags.push(reefsdb::models::points::Tag::new(
                    key.into_bytes(),
                    value.into_bytes(),
                ));
            }

            data.push(Datum {
                name: name.as_bytes().to_vec(),
                tags,
            });
        }
    }

    g.bench_function(
        &format!(
            "measurement_n: {}, series_key_n: {}, series_value_n: {}",
            measurement_n, series_key_n, series_value_n
        ),
        move |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter_with_large_drop(|| async {
                    for d in &data {
                        let _ = f.add_series_list(Arc::clone(&series_set), &[&d.name], &[&d.tags]);
                    }
                })
        },
    );
}

fn run_measurement_has_series_benchmarks(
    g: &mut BenchmarkGroup<WallTime>,
    series_key_n: usize,
    series_value_n: usize,
) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let (f, series_set) = rt.block_on(async {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());
        let f = must_open_log_file(&sfile);

        let measurement_n = 2;
        let tag_value_n = series_value_n.pow(series_key_n as u32);
        let series_set = Arc::new(series_set::SeriesIDSet::new(&[])); //  all series in all measurements
        let mut series_ids = vec![]; // all series ids in measurement0

        // add series to all measurements
        for i in 0..measurement_n {
            let name = format!("measurement{}", i);

            let mut names = Vec::<String>::with_capacity(tag_value_n);
            let mut tags = Vec::<reefsdb::models::points::Tags>::with_capacity(tag_value_n);
            for j in 0..tag_value_n {
                let mut tag = reefsdb::models::points::Tags::default();

                for k in 0..series_key_n {
                    let key = format!("key{}", k);
                    let value = format!(
                        "value{}",
                        j / (series_value_n.pow(k as u32)) % series_value_n
                    );
                    tag.push(reefsdb::models::points::Tag::new(
                        key.into_bytes(),
                        value.into_bytes(),
                    ));
                }

                names.push(name.clone()); // names[j] = name;
                tags.push(tag); // tags[j] = tag;
            }
            let tags_slice_of_refs: Vec<&reefsdb::models::points::Tags> = tags.iter().collect();
            let tags = tags_slice_of_refs.as_slice();
            let names: Vec<&[u8]> = names.iter().map(|v| v.as_bytes()).collect();
            let ids = f
                .add_series_list(Arc::clone(&series_set), &names, tags)
                .unwrap();
            if i == 0 {
                series_ids = ids;
            }
        }
        // remove some series in measurement0
        for i in 0..50 {
            assert!(f.delete_series_id(series_ids[i]).is_ok());
            series_set.remove(series_ids[i]);
        }

        (f, series_set)
    });

    let name = "measurement0";
    g.bench_function(
        &format!(
            "series_key: {}, series_value_{}",
            series_key_n, series_value_n
        ),
        move |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    assert!(
                        f.measurement_has_series(&*series_set, name.as_bytes()),
                        "expect true, got false"
                    );
                })
        },
    );
}

// Returns an estimated bit count and hash count given the element count and false positive rate.
fn estimate(n: u64, p: f64) -> (u64, u64) {
    let m = ((-1f64 * (n as f64) * p.ln() / (2f64.ln().powf(2f64))).ceil()) as u64;
    let k = ((2f64.ln() * (m as f64) / (n as f64)).ceil()) as u64;
    (m, k)
}

fn run_write_to_benchmarks(g: &mut BenchmarkGroup<WallTime>, series_n: usize) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let f = rt.block_on(async {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());
        let f = must_open_log_file(&sfile);

        let series_set = Arc::new(series_set::SeriesIDSet::new(&[])); //  all series in all measurements

        // Initialize log file with series data.
        for i in 0..series_n {
            let _ = f
                .add_series_list(
                    Arc::clone(&series_set),
                    &["cpu".as_bytes()],
                    &[&reefsdb::models::points::Tags::from(HashMap::from([
                        (
                            "host".as_bytes().to_vec(),
                            format!("server-{}", i).into_bytes(),
                        ),
                        (
                            "location".as_bytes().to_vec(),
                            "us-west".as_bytes().to_vec(),
                        ),
                    ]))],
                )
                .unwrap();
        }

        f
    });

    // Estimate bloom filter size.
    let (m, k) = estimate(series_n as u64, 0.02);

    // // Create cpu profile for each subtest.
    // MustStartCPUProfile(name)
    // defer pprof.StopCPUProfile()

    // Compact log file.
    g.bench_function(&format!("series_n: {}", series_n), move |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let mut buf = vec![];
                assert!(
                    f.compact_to(&mut buf, m, k, CancellationToken::new())
                        .is_ok()
                );
            })
    });
}

fn bench_log_file_write_to(c: &mut Criterion) {
    let mut g = c.benchmark_group("bench_log_file_write_to");
    // set configure
    // g.sample_size(10); // default sample size is 100, the min size is 10
    // g.warm_up_time(Duration::from_secs(xxx)); // complete 100 samples is about xxx

    // golang: 1982557 ns/op=1.98ms, 58088b
    //
    // 1.0555 ms (use Bytes)
    run_write_to_benchmarks(&mut g, 1000);

    // golang: 16637630 ns/op=16.6ms, 506153b
    //
    // 11.872 ms (use Bytes)
    run_write_to_benchmarks(&mut g, 10000);
    // golang: 203232054 ns/op=203ms, 4552732b
    //
    // (use Bytes)
    run_write_to_benchmarks(&mut g, 100000);
    // golang: 2158033459 ns/op=2.15s, 51781954b
    //
    // (use Bytes)
    run_write_to_benchmarks(&mut g, 1000000);
}

fn bench_log_file_add_series_list(c: &mut Criterion) {
    let mut g = c.benchmark_group("bench_log_file_add_series_list");
    // set configure
    // g.sample_size(10); // default sample size is 100, the min size is 10
    // g.warm_up_time(std::time::Duration::from_secs(300)); // complete 100 samples is about xxx

    // golang: 364845 ns/op=0.36ms
    // 3.4010 ms
    run_log_file_add_series_list_benchmarks(&mut g, 100, 1, 1); // 100 series

    // golang: 4727051 ns/op=4.7ms
    // 33.643 ms
    run_log_file_add_series_list_benchmarks(&mut g, 1000, 1, 1); // 1000 series

    // golang: 37336846 ns/op=37ms
    // 336.84 ms
    run_log_file_add_series_list_benchmarks(&mut g, 10000, 1, 1); // 10000 series

    // golang: 39927351 ns/op=40ms
    // 349.46 ms
    run_log_file_add_series_list_benchmarks(&mut g, 100, 2, 10); // ~20k series

    // golang: 526549180 ns/op=526ms
    // 3.5404 s
    run_log_file_add_series_list_benchmarks(&mut g, 100000, 1, 1);

    // golang: 136489665 ns/op=136ms
    // 1.1849 s
    run_log_file_add_series_list_benchmarks(&mut g, 100, 3, 7); // ~100k series

    // golang: 316680805 ns/op=316ms
    // 2.3999 s
    run_log_file_add_series_list_benchmarks(&mut g, 200, 3, 7); // ~200K series

    // golang: 2762829611 ns/op=2762ms=2.76s
    // 16.767 s
    run_log_file_add_series_list_benchmarks(&mut g, 200, 4, 7); // ~1.9M series

    g.finish();
}

fn bench_log_file_measurement_has_series(c: &mut Criterion) {
    let mut g = c.benchmark_group("bench_log_file_measurement_has_series");
    // set configure
    // g.sample_size(10); // default sample size is 100, the min size is 10
    // g.warm_up_time(Duration::from_secs(xxx)); // complete 100 samples is about xxx

    // golang: 38.16 ns/op
    // 91.993 ns
    // (use Bytes)
    run_measurement_has_series_benchmarks(&mut g, 2, 10); // 100 series

    // golang: 38.18 ns/op
    // 674.62 ns
    // (use Bytes)
    run_measurement_has_series_benchmarks(&mut g, 3, 10); // 1k series

    // golang: 37.35 ns/op
    // 575.31 ns
    // (use Bytes)
    run_measurement_has_series_benchmarks(&mut g, 4, 10); // 10k series

    // golang: 37.24 ns/op
    // 1.1089 µs
    // (use Bytes)
    run_measurement_has_series_benchmarks(&mut g, 5, 10); // 100k series

    g.finish();
}

criterion_group!(
    name = log_file_benches;
    config = Criterion::default();
    targets = bench_log_file_add_series_list, bench_log_file_measurement_has_series, bench_log_file_write_to
);
criterion_main!(log_file_benches);
