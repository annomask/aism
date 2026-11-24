use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use rand::Rng;
use reefsdb::series::series_set::SeriesIDSet;

// Contains should be typically a constant time lookup. Example results on a laptop:
//
// BenchmarkSeriesIDSet_Contains/1-4 	        87984150	        11.87 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/2-4 	        97690736	        12.01 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/10-4         	88153453	        12.88 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/100-4        	89330898	        12.67 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/1000-4       	82865253	        13.58 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/10000-4      	97940937	        11.36 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/100000-4     	98251282	        12.35 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/1000000-4    	80216841	        13.71 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/10000000-4   	96675100	        12.60 ns/op	       0 B/op	       0 allocs/op
//
// Rust:
// 12ns ~ 13ns
fn bench_series_id_set_contains(c: &mut Criterion) {
    let mut g = c.benchmark_group("series_id_set_contains");

    let cardinalities: [u64; 9] = [1, 2, 10, 100, 1000, 10000, 100000, 1000000, 10000000];
    for cardinality in cardinalities {
        run_series_id_set_contains_benchmarks(&mut g, cardinality);
    }

    g.finish();
}

fn run_series_id_set_contains_benchmarks(g: &mut BenchmarkGroup<WallTime>, cardinality: u64) {
    let set = SeriesIDSet::new(&[] as &[u64]);
    for i in 0u64..cardinality {
        let _ = set.add(i);
    }

    let lookup = cardinality / 2;
    g.bench_function(&format!("cardinality: {}", cardinality), |b| {
        b.iter(|| {
            let _ = set.contains(lookup);
        })
    });
}

// Adding to a larger bitset shouldn't be significantly more expensive than adding
// to a smaller one. This benchmark adds a value to different cardinality sets.
//
// Example results from a laptop:
//
// series_id_set_add_more/1           time:   10.273 ns
// series_id_set_add_more/2           time:   10.544 ns
// series_id_set_add_more/10          time:   11.113 ns
// series_id_set_add_more/100         time:   12.280 ns
// series_id_set_add_more/1000        time:   13.198 ns
// series_id_set_add_more/10000       time:   10.593 ns
// series_id_set_add_more/100000      time:   10.542 ns
// series_id_set_add_more/1000000     time:   11.896 ns
// series_id_set_add_more/10000000    time:   13.205 ns
fn series_id_set_add_more(c: &mut Criterion) {
    let mut group = c.benchmark_group("series_id_set_add_more");

    let cardinalities: [u64; 9] = [1, 2, 10, 100, 1000, 10000, 100000, 1000000, 10000000];
    for cardinality in cardinalities {
        let set = SeriesIDSet::new(&[] as &[u64]);
        for i in 0u64..cardinality {
            let _ = set.add(i);
        }

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(cardinality),
            &cardinality,
            |b, &cardinality| {
                b.iter(|| {
                    // Add next value
                    let _ = set.add(cardinality);
                });
                let _ = set.remove(cardinality);
            },
        );
    }
}

// Add benchmarks the cost of adding the same element to a set versus the
// cost of checking if it exists before adding it.
//
// Typical benchmarks from a laptop:
//
// cardinality_1000000_add/same                        time:   11.953 ns
// cardinality_1000000_add/random                      time:   12.986 ns
// cardinality_1000000_add/same_no_lock                time:   3.2353 ns
// cardinality_1000000_add/random_no_lock              time:   4.5472 ns
//
// cardinality_1000000_check_add/same_no_lock          time:   2.4257 ns
// cardinality_1000000_check_add/random_no_lock        time:   3.8877 ns
// cardinality_1000000_check_add/same_global_lock      time:   9.6983 ns
// cardinality_1000000_check_add/random_global_lock    time:   10.941 ns
// cardinality_1000000_check_add/same_multi_lock       time:   [23.697 ns 23.896 ns 24.184 ns]
fn series_id_set_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("cardinality_1000000_add");

    let set = SeriesIDSet::new(&[] as &[u64]);
    for i in 0u64..1000000 {
        let _ = set.add(i);
    }

    let lookup = 300032u64;

    // Add the same value over and over.
    group.bench_with_input(
        BenchmarkId::from_parameter("same"),
        &lookup,
        |b, &lookup| {
            b.iter(|| {
                let _ = set.add(lookup);
            });
        },
    );

    let x = rand::thread_rng().gen_range(0..i32::MAX);
    group.bench_with_input(BenchmarkId::from_parameter("random"), &x, |b, &x| {
        b.iter(|| {
            let _ = set.add(x as u64);
        });
    });

    group.bench_with_input(
        BenchmarkId::from_parameter("same_no_lock"),
        &lookup,
        |b, &lookup| {
            b.iter(|| {
                let _ = set.add_no_lock(lookup);
            });
        },
    );

    let x = rand::thread_rng().gen_range(0..i32::MAX);
    group.bench_with_input(
        BenchmarkId::from_parameter("random_no_lock"),
        &x,
        |b, &x| {
            b.iter(|| {
                let _ = set.add_no_lock(x as u64);
            });
        },
    );

    drop(group);
    let mut group = c.benchmark_group("cardinality_1000000_check_add");
    // Add the same value over and over with no lock
    group.bench_with_input(
        BenchmarkId::from_parameter("same_no_lock"),
        &lookup,
        |b, &lookup| {
            b.iter(|| {
                if !set.contains_no_lock(lookup) {
                    let _ = set.add_no_lock(lookup);
                }
            });
        },
    );

    let x = rand::thread_rng().gen_range(0..i32::MAX);
    group.bench_with_input(
        BenchmarkId::from_parameter("random_no_lock"),
        &x,
        |b, &x| {
            b.iter(|| {
                if !set.contains_no_lock(x as u64) {
                    let _ = set.add_no_lock(x as u64);
                }
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::from_parameter("same_global_lock"),
        &lookup,
        |b, &lookup| {
            b.iter(|| {
                let _guard = set.bitmap.write();
                if !set.contains_no_lock(lookup) {
                    let _ = set.add_no_lock(lookup);
                }
            });
        },
    );

    let x = rand::thread_rng().gen_range(0..i32::MAX);
    group.bench_with_input(
        BenchmarkId::from_parameter("random_global_lock"),
        &x,
        |b, &x| {
            b.iter(|| {
                let _guard = set.bitmap.write();
                if !set.contains_no_lock(x as u64) {
                    let _ = set.add_no_lock(x as u64);
                }
            });
        },
    );

    let x = rand::thread_rng().gen_range(0..i32::MAX);
    group.bench_with_input(
        BenchmarkId::from_parameter("same_multi_lock"),
        &x,
        |b, &x| {
            b.iter(|| {
                if !set.contains(x as u64) {
                    let _ = set.add(x as u64);
                }
            });
        },
    );
}

fn run_benchmarks<F: Fn() -> SeriesIDSet>(
    group: &mut BenchmarkGroup<WallTime>,
    cardinality: i32,
    other: SeriesIDSet,
    init: F,
) {
    group.bench_with_input(
        format!("cardinality_{}/clone", cardinality),
        &other,
        |b, other| {
            b.iter(|| {
                let _ = other.clone();
            })
        },
    );

    let ss_result = init();
    let other_1 = other.clone();
    group.bench_with_input(
        format!("cardinality_{}/merge", cardinality),
        &ss_result,
        |b, ss_result| {
            b.iter(|| {
                ss_result.merge(&[&other_1]);
            })
        },
    );

    let ss_result = init();
    let other_2 = other.clone();
    group.bench_with_input(
        format!("cardinality_{}/merge_in_place", cardinality),
        &ss_result,
        |b, ss_result| {
            b.iter(|| {
                ss_result.merge_in_place(&other_2);
            })
        },
    );

    let ss_result = init();
    group.bench_with_input(
        format!("cardinality_{}/add", cardinality),
        &ss_result,
        |b, ss_result| {
            b.iter(|| {
                let itr = other.iterator_no_lock();
                let _guard = ss_result.bitmap.write();
                for id in itr {
                    let _ = ss_result.add_no_lock(id as u64);
                }
            })
        },
    );

    let ss_result = init();
    let mut buf = vec![];
    group.bench_with_input(
        format!("cardinality_{}/write_to", cardinality),
        &ss_result,
        |b, ss_result| {
            b.iter(|| {
                // ss_result.merge_in_place(&other);
                other.write_to(&mut buf).unwrap();
                ss_result.unmarshal_binary_unsafe(&buf[..])
            })
        },
    );
}

// Benchmark various ways of creating a copy of a bitmap. Note, Clone_COW will result
// in a bitmap where future modifications will involve copies.
//
fn series_id_set_clone(c: &mut Criterion) {
    let to_add_cardinalities: [i32; 4] = [1_000, 1_0_000, 1_00_000, 1_000_000];

    for to_add_cardinality in to_add_cardinalities {
        let mut ids = vec![0u64; to_add_cardinality as usize];
        for i in 0..to_add_cardinality {
            ids[i as usize] = rand::thread_rng().gen_range(0..200000000) as u64;
        }

        let other = SeriesIDSet::new(&ids);
        let mut group = c.benchmark_group("series_id_set_clone/re-use");
        run_benchmarks(&mut group, to_add_cardinality, other, || {
            let base = SeriesIDSet::new(&[] as &[u64]);
            base.clear();
            base
        });
        drop(group);

        let other = SeriesIDSet::new(&ids);
        let mut group = c.benchmark_group("series_id_set_clone/don't re-use");
        run_benchmarks(&mut group, to_add_cardinality, other, || {
            SeriesIDSet::new(&[] as &[u64])
        });
    }
}

fn series_id_set_add_many(c: &mut Criterion) {
    let cardinalities = [1, 1_000, 1_0_000, 1_00_000, 1_000_000];
    let to_add_cardinalities: [i32; 3] = [1_000, 1_0_000, 1_00_000];

    let mut group = c.benchmark_group("add_many");
    for cardinality in cardinalities {
        let mut ids = vec![0u64; cardinality as usize];
        for i in 0..cardinality {
            ids[i as usize] = rand::thread_rng().gen_range(0..200000000) as u64;
        }
        let set = SeriesIDSet::new(&ids);

        for to_add_cardinality in to_add_cardinalities {
            let clone = set.clone();
            let bench_id = format!(
                "cardinality_{}/adding_{}/add_no_lock",
                cardinality, to_add_cardinality
            );
            group.bench_with_input(BenchmarkId::from_parameter(bench_id), &clone, |b, clone| {
                b.iter(|| {
                    for id in &ids {
                        let _ = clone.add_no_lock(*id);
                    }
                })
            });

            let clone = set.clone();
            let bench_id = format!(
                "cardinality_{}/add_{}/add_many",
                cardinality, to_add_cardinality
            );
            group.bench_with_input(BenchmarkId::from_parameter(bench_id), &clone, |b, clone| {
                b.iter(|| {
                    clone.add_many(&ids);
                })
            });

            // Merge will involve a new bitmap being allocated.
            let clone = set.clone();
            let bench_id = format!(
                "cardinality_{}/add_{}/merge",
                cardinality, to_add_cardinality
            );
            group.bench_with_input(BenchmarkId::from_parameter(bench_id), &clone, |b, clone| {
                b.iter(|| {
                    let other = SeriesIDSet::new(&ids);
                    clone.merge(&[&other]);
                })
            });

            let clone = set.clone();
            let bench_id = format!(
                "cardinality_{}/add_{}/merge_in_place",
                cardinality, to_add_cardinality
            );
            group.bench_with_input(BenchmarkId::from_parameter(bench_id), &clone, |b, clone| {
                b.iter(|| {
                    let other = SeriesIDSet::new(&ids);
                    clone.merge_in_place(&other);
                })
            });
        }
    }
}

// Remove benchmarks the cost of removing the same element in a set versus the
// cost of checking if it exists before removing it.
//
// Typical benchmarks from a laptop:
//
// remove/cardinality_1000000_remove_same                 time:   19.528 ns
// remove/cardinality_1000000_check_remove_global_lock    time:   16.062 ns
// remove/cardinality_1000000_check_remove_multi_lock     time:   20.345 ns
fn series_id_set_add_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("remove");
    let set = SeriesIDSet::new(&[] as &[u64]);
    for i in 0u64..1000000u64 {
        let _ = set.add(i);
    }
    let lookup = 300032u64;
    // Remove the same value over and over.
    group.bench_function("cardinality_1000000_remove_same", |b| {
        b.iter(|| {
            set.remove(lookup);
        })
    });

    // Check if the value exists before adding it. Subsequent repeats of the code
    // will result in contains checks.
    group.bench_function("cardinality_1000000_check_remove_global_lock", |b| {
        b.iter(|| {
            let _bitmap = set.bitmap.write();
            if set.contains_no_lock(lookup) {
                let _ = set.remove_no_lock(lookup);
            }
        })
    });

    // Check if the value exists before adding it under two locks.
    group.bench_function("cardinality_1000000_check_remove_multi_lock", |b| {
        b.iter(|| {
            if set.contains(lookup) {
                let _ = set.remove(lookup);
            }
        })
    });
}

// Typical benchmarks for a laptop:
//
// merge_duplicates/cardinality_1/shards_1             time:   53.837 ns
// merge_duplicates/cardinality_1/shards_10            time:   259.47 ns
// merge_duplicates/cardinality_1/shards_100           time:   2.2299 µs
// merge_duplicates/cardinality_10000/shards_1         time:   638.35 ns
// merge_duplicates/cardinality_10000/shards_10        time:   5.5891 µs
// merge_duplicates/cardinality_10000/shards_100       time:   55.200 µs
// merge_duplicates/cardinality_1000000/shards_1       time:   11.466 µs
// merge_duplicates/cardinality_1000000/shards_10      time:   93.236 µs
// merge_duplicates/cardinality_1000000/shards_100     time:   924.26 µs
// merge_duplicates/cardinality_10000000/shards_1      time:   136.43 µs
// merge_duplicates/cardinality_10000000/shards_10     time:   1.0542 ms
// merge_duplicates/cardinality_10000000/shards_100    time:   13.261 ms
fn series_id_set_merge_duplicates(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_duplicates");

    let cardinalities = [1, 1_0_000, 1_000_000, 1_0_000_000];
    let shards = [1i32, 10i32, 100i32];

    for cardinality in cardinalities {
        let set = SeriesIDSet::new(&[] as &[u64]);
        for i in 0u64..cardinality {
            let _ = set.add(i);
        }

        for shard in shards {
            let mut others = vec![];
            for _s in 0..shard {
                others.push(set.clone());
            }

            let bench_id = format!("cardinality_{}/shards_{}", cardinality, shard);
            let base = set.clone();
            group.bench_function(bench_id, |b| {
                b.iter(|| {
                    let _ = &base.merge(
                        others
                            .iter()
                            .map(|ss| &*ss)
                            .collect::<Vec<&SeriesIDSet>>()
                            .as_slice(),
                    );
                })
            });
        }
    }
}

// Typical benchmarks for a laptop:
//
// merge_unique/cardinality_1/shards_1             time:   52.744 ns
// merge_unique/cardinality_1/shards_10            time:   246.72 ns
// merge_unique/cardinality_1/shards_100           time:   2.1687 µs
// merge_unique/cardinality_10000/shards_1         time:   650.65 ns
// merge_unique/cardinality_10000/shards_10        time:   5.4915 µs
// merge_unique/cardinality_10000/shards_100       time:   54.824 µs
// merge_unique/cardinality_1000000/shards_1       time:   11.308 µs
// merge_unique/cardinality_1000000/shards_10      time:   91.625 µs
// merge_unique/cardinality_1000000/shards_100     time:   935.39 µs
// merge_unique/cardinality_10000000/shards_1      time:   139.70 µs
// merge_unique/cardinality_10000000/shards_10     time:   1.0587 ms
// merge_unique/cardinality_10000000/shards_100    time:   15.069 ms
fn series_id_set_merge_unique(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_unique");

    let cardinalities = [1, 1_0_000, 1_000_000, 1_0_000_000];
    let shards = [1i32, 10i32, 100i32];

    for cardinality in cardinalities {
        let set = SeriesIDSet::new(&[] as &[u64]);
        for i in 0u64..cardinality {
            let _ = set.add(i);
        }

        for shard in shards {
            let mut others = vec![];
            for s in 1i32..=shard {
                let other = SeriesIDSet::new(&[] as &[u64]);
                for i in 0..cardinality {
                    other.add(i + (s as u64 * cardinality));
                }
                others.push(set.clone());
            }

            let bench_id = format!("cardinality_{}/shards_{}", cardinality, shard);
            let base = set.clone();
            group.bench_function(bench_id, |b| {
                b.iter(|| {
                    let _ = &base.merge(
                        others
                            .iter()
                            .map(|ss| &*ss)
                            .collect::<Vec<&SeriesIDSet>>()
                            .as_slice(),
                    );
                })
            });
        }
    }
}

criterion_group!(
    benches,
    bench_series_id_set_contains,
    series_id_set_add_more,
    series_id_set_add,
    series_id_set_clone,
    series_id_set_add_many,
    series_id_set_add_remove,
    series_id_set_merge_duplicates,
    series_id_set_merge_unique,
);
criterion_main!(benches);
