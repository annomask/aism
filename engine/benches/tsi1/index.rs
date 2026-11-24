use std::{
    io::{BufReader, Read},
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{
    BenchmarkGroup, Criterion, black_box, criterion_group, criterion_main, measurement::WallTime,
};
use flate2::bufread;
use reefsdb::models::points::Tags;
use reefsdb::{common::wait_group::WaitGroup, runtime::global::spawn_blocking_global};
use reefsdb::{
    config,
    index::{
        SeriesIDIterator,
        tsi1::{
            self,
            index::{self, tests::must_open_index},
        },
    },
    series::series_file,
};
#[allow(dead_code)]
fn run_benchmark_tag_value_series_id_iter(
    g: &mut BenchmarkGroup<WallTime>,
    id: &str,
    cache_size: i64,
) {
    let sfile = Arc::new(series_file::tests::new_series_file().into());

    let idx = {
        let path = std::env::current_dir().unwrap();
        // Load index
        let idx = index::Index::new(
            Arc::clone(&sfile),
            "foo".to_string(),
            path.join("src/index/tsi1/testdata/index-file-index"),
            config::DEFAULT_MAX_INDEX_LOG_FILE_SIZE,
            cache_size,
        );
        assert!(idx.open().is_ok());
        idx
    };

    g.bench_function(id, move |b| {
        b.iter(|| {
            let tsiditr = idx.tag_value_series_id_iter(
                "m4".as_bytes(),
                "tag0".as_bytes(),
                "value4".as_bytes(),
            );
            assert!(tsiditr.is_ok());
            assert!(tsiditr.unwrap().is_some(), "got None iterator");
        })
    });

    // sfile.close();
    // idx.close();
}

// Calling TagValueSeriesIDIterator on the index involves merging several
// SeriesIDSets together.BenchmarkIndex_TagValueSeriesIDIterator, which can have
// a non trivial cost. In the case of `tsi` files, the mmapd sets are merged
// together. In the case of tsl files the sets need to are cloned and then merged.
//
// Typical results on an i7 laptop
// BenchmarkIndex_IndexFile_TagValueSeriesIDIterator/78888_series_TagValueSeriesIDIterator/cache-8   	 2000000	       643 ns/op	     744 B/op	      13 allocs/op
// BenchmarkIndex_IndexFile_TagValueSeriesIDIterator/78888_series_TagValueSeriesIDIterator/no_cache-8      10000	    130749 ns/op	  124952 B/op	     350 allocs/op

// go:
//    cache:    30.052 µs/op
//    no_cache: 113.244 µs/op
// rust:
//    cache: 372.77 µs/op
//    no_cache: 372.05 µs/op
#[allow(dead_code)]
fn bench_index_index_file_tag_value_series_id_iter(c: &mut Criterion) {
    let mut g = c.benchmark_group("78888 series tag_value_series_id_iter");

    run_benchmark_tag_value_series_id_iter(
        &mut g,
        "cache",
        config::DEFAULT_SERIES_ID_SET_CACHE_SIZE,
    );

    run_benchmark_tag_value_series_id_iter(&mut g, "no_cache", 0);
}

// Typical results on an i7 laptop
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_1-8         	       1	4004452124 ns/op	2381998144 B/op	21686990 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_2-8         	       1	2625853773 ns/op	2368913968 B/op	21765385 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_4-8         	       1	2127205189 ns/op	2338013584 B/op	21908381 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_8-8         	       1	2331960889 ns/op	2332643248 B/op	22191763 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_16-8        	       1	2398489751 ns/op	2299551824 B/op	22670465 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_1-8        	       1	3404683972 ns/op	2387236504 B/op	21600671 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_2-8        	       1	2173772186 ns/op	2329237224 B/op	21631104 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_4-8        	       1	1729089575 ns/op	2299161840 B/op	21699878 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_8-8        	       1	1644295339 ns/op	2161473200 B/op	21796469 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_16-8       	       1	1683275418 ns/op	2171872432 B/op	21925974 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_1-8       	       1	3330508160 ns/op	2333250904 B/op	21574887 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_2-8       	       1	2278604285 ns/op	2292600808 B/op	21628966 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_4-8       	       1	1760098762 ns/op	2243730672 B/op	21684608 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_8-8       	       1	1693312924 ns/op	2166924112 B/op	21753079 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_16-8      	       1	1663610452 ns/op	2131177160 B/op	21806209 allocs/op
#[allow(dead_code)]
fn bench_index_create_series_list_if_not_exists(c: &mut Criterion) {
    let mut g = c.benchmark_group("bench_index_create_series_list_if_not_exists");
    g.sample_size(10); //  Collecting 100 samples in estimated 9388.7 s (100 iterations)
    g.warm_up_time(Duration::from_secs(1000)); //  Collecting 10 samples in estimated 934.32 s (10 iterations)
    let points = read_line_protocol_test_data();

    let mut keys = vec![vec![]; 1e6 as usize];
    let mut names = vec![vec![]; 1e6 as usize];
    let mut tags = vec![Tags::default(); 1e6 as usize];

    for mut pt in points {
        keys.push(pt.key().to_vec());
        names.push(pt.name());
        tags.push(pt.tags());
    }

    let batch_sizes = vec![1000, 10000, 100000];
    let partitions = vec![1, 2, 4, 8, 16];
    for sz in batch_sizes {
        for p in &partitions {
            let keys = keys.clone();
            let names = names.clone();
            let tags = tags.clone();
            g.bench_function(&format!("batch_size_{}/partition_{}", sz, p), move |b| {
                b.iter_custom(|iters| {
                    let keys = keys.clone();
                    let names = names.clone();
                    let tags = tags.clone();
                    let mut total_elapsed = Duration::ZERO;
                    for _i in 0..iters {
                        black_box({
                            let mut i = 0;
                            while i < keys.len() {
                                let k = keys
                                    .get(i..i + sz)
                                    .unwrap()
                                    .iter()
                                    .map(|k| &k[..])
                                    .collect::<Vec<&[u8]>>();
                                let n = names.get(i..i + sz).unwrap().to_vec();
                                let t = tags.get(i..i + sz).unwrap().to_vec();

                                let mut idx = must_open_index(*p);

                                let start = Instant::now();
                                if let Err(e) = idx.idx.create_series_list_if_not_exists(&k, n, t) {
                                    panic!("{}", e);
                                }
                                total_elapsed += start.elapsed();

                                // Reset the index
                                idx.close();
                                i += sz;
                            }
                        });
                    }
                    total_elapsed
                })
            });
        }
    }
}

// Run concurrent iterator.
fn run_iter_task(index: Arc<index::tests::Index>, query_n: i32) {
    let keys = vec![
        vec!["m0".to_string(), "tag2".to_string(), "value4".to_string()],
        vec!["m1".to_string(), "tag3".to_string(), "value5".to_string()],
        vec!["m2".to_string(), "tag4".to_string(), "value6".to_string()],
        vec!["m3".to_string(), "tag0".to_string(), "value8".to_string()],
        vec!["m4".to_string(), "tag5".to_string(), "value0".to_string()],
    ];

    for _i in 0..query_n / 5 {
        for key in keys.iter() {
            let itr = index.idx.tag_value_series_id_iter(
                key[0].as_bytes(),
                key[1].as_bytes(),
                key[2].as_bytes(),
            );
            assert!(itr.is_ok());
            let itr = itr.unwrap();
            assert!(itr.is_some(), "got None iter");
            assert!(itr.unwrap().close().is_ok())
        }
    }
}
fn run_benchmark_concurrent_write_query(
    query_n: i32,
    partitions: u64,
    cache_size: i64,
    keys: Vec<Vec<u8>>,
    names: Vec<Vec<u8>>,
    tags: Vec<Tags>,
) -> Duration {
    let mut total_elapsed = Duration::ZERO;
    let start = Instant::now();

    let mut idx = Arc::new({
        let sfile = Arc::new(series_file::tests::new_series_file().into());
        let mut idx = index::Index::new(
            Arc::clone(&sfile),
            "foo".to_string(),
            tsi1::tests::must_temp_tsi_dir(),
            config::DEFAULT_MAX_INDEX_LOG_FILE_SIZE,
            cache_size,
        );
        idx.partition_n = partitions;

        let mut idx = index::tests::Index { idx, sfile };

        assert!(idx.open().is_ok());
        idx
    });

    let wg = WaitGroup::new();
    wg.add(1);

    let worker = wg.clone();
    let idx2 = Arc::clone(&idx);
    spawn_blocking_global(move || {
        run_iter_task(idx2, query_n);
        worker.done();
    });

    let batch_size = 10000;

    total_elapsed += start.elapsed();
    let mut i = 0;
    while i < keys.len() {
        let start = Instant::now();
        let k = keys
            .get(i..i + batch_size)
            .unwrap()
            .iter()
            .map(|k| &k[..])
            .collect::<Vec<&[u8]>>();
        let n = names.get(i..i + batch_size).unwrap().to_vec();
        let t = tags.get(i..i + batch_size).unwrap().to_vec();

        if let Err(e) = idx.idx.create_series_list_if_not_exists(&k, n, t) {
            panic!("{}", e);
        }

        // Wait for queries to finish
        wg.wait();

        total_elapsed += start.elapsed();
        // Reset the index...
        Arc::get_mut(&mut idx)
            .expect("query task retained an index reference")
            .close();

        idx = Arc::new({
            let sfile = Arc::new(series_file::tests::new_series_file().into());
            let mut idx = index::Index::new(
                Arc::clone(&sfile),
                "foo".to_string(),
                tsi1::tests::must_temp_tsi_dir(),
                config::DEFAULT_MAX_INDEX_LOG_FILE_SIZE,
                cache_size,
            );
            idx.partition_n = partitions;

            let mut idx = index::tests::Index { idx, sfile };

            assert!(idx.open().is_ok());
            idx
        });

        wg.add(1);

        let idx2 = Arc::clone(&idx);
        let worker = wg.clone();
        spawn_blocking_global(move || {
            run_iter_task(idx2, query_n);
            worker.done();
        });

        i += batch_size;
    }

    total_elapsed

    // sfile.close();
    // idx.close();
}

// This benchmark concurrently writes series to the index and fetches cached bitsets.
// The idea is to emphasize the performance difference when bitset caching is on and off.
//
// Typical results for an i7 laptop
// BenchmarkIndex_ConcurrentWriteQuery/partition_1/queries_100000/cache-8   	       	1	3836451407 ns/op	2453296232 B/op		22648482 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_4/queries_100000/cache-8            	1	1836598730 ns/op	2435668224 B/op		22908705 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_8/queries_100000/cache-8            	1	1714771527 ns/op	2341518456 B/op		23450621 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_16/queries_100000/cache-8           	1	1810658403 ns/op	2401239408 B/op		23868079 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_1/queries_100000/no_cache-8           	1	4044478305 ns/op	4414915048 B/op		27292357 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_4/queries_100000/no_cache-8         	1	18663345153 ns/op	23035974472 B/op	54015704 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_8/queries_100000/no_cache-8         	1	22242979152 ns/op	28178915600 B/op	80156305 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_16/queries_100000/no_cache-8        	1	24817283922 ns/op	34613960984 B/op	150356327 allocs/op

// bench_concurrent_write_query/partition_1/querys_100000/cache
// rust: 27.462 s
// bench_concurrent_write_query/partition_1/querys_100000/no_cache
// rust: 37.263 s
// bench_concurrent_write_query/partition_4/querys_100000/cache
// rust: 101.73 s
fn bench_index_concurrent_write_query(c: &mut Criterion) {
    // Init global runtime.
    let runtime_opt = reefsdb::runtime::global::RuntimeOptions::default();
    reefsdb::runtime::global::init_global_runtimes(&runtime_opt);

    let points = read_line_protocol_test_data();

    let mut keys = vec![vec![]; 1e6 as usize];
    let mut names = vec![vec![]; 1e6 as usize];
    let mut tags = vec![Tags::default(); 1e6 as usize];

    for mut pt in points {
        keys.push(pt.key().to_vec());
        names.push(pt.name());
        tags.push(pt.tags());
    }

    let partitions = vec![1, 4, 8, 16];
    let querys = vec![1e5 as i32];
    for partition_n in partitions {
        let mut g = c.benchmark_group(format!(
            "bench_concurrent_write_query/partition_{}",
            partition_n
        ));
        for query_n in &querys {
            let keys1 = keys.clone();
            let names1 = names.clone();
            let tags1 = tags.clone();
            g.bench_function(&format!("querys_{}/cache", query_n), move |b| {
                b.iter_custom(|iters| {
                    let keys = keys1.clone();
                    let names = names1.clone();
                    let tags = tags1.clone();
                    let mut total_elapsed = Duration::ZERO;
                    for _i in 0..iters {
                        black_box({
                            total_elapsed += run_benchmark_concurrent_write_query(
                                *query_n,
                                partition_n,
                                config::DEFAULT_SERIES_ID_SET_CACHE_SIZE,
                                keys.clone(),
                                names.clone(),
                                tags.clone(),
                            );
                        });
                    }
                    total_elapsed
                })
            });

            let keys2 = keys.clone();
            let names2 = names.clone();
            let tags2 = tags.clone();
            g.bench_function(&format!("querys_{}/no_cache", query_n), move |b| {
                b.iter_custom(|iters| {
                    let keys = keys2.clone();
                    let names = names2.clone();
                    let tags = tags2.clone();
                    let mut total_elapsed = Duration::ZERO;
                    for _i in 0..iters {
                        black_box({
                            total_elapsed += run_benchmark_concurrent_write_query(
                                *query_n,
                                partition_n,
                                0,
                                keys.clone(),
                                names.clone(),
                                tags.clone(),
                            );
                        });
                    }
                    total_elapsed
                })
            });
        }
    }
}

fn read_line_protocol_test_data() -> Vec<reefsdb::models::points::Point> {
    // Read line-protocol and coerce into tsdb format.
    // 1M series generated with:
    // $inch -b 10000 -c 1 -t 10,10,10,10,10,10 -f 1 -m 5 -p 1
    let path = std::env::current_dir()
        .unwrap()
        .join("src/index/tsi1/testdata/line-protocol-1M.txt.gz");
    let input = BufReader::new(std::fs::File::open(&path).unwrap());
    let mut decoder = bufread::GzDecoder::new(input);
    let mut data = vec![];
    let _n = decoder.read_to_end(&mut data);
    let (points, _) = reefsdb::models::points::parse_points(&data).unwrap();
    points
}

criterion_group!(
    benches,
    // bench_index_index_file_tag_value_series_id_iter,
    bench_index_concurrent_write_query
);
criterion_main!(benches);
