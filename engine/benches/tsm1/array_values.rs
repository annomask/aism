use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use reefsdb::storage::tsm1::cursor::array_values::{IntegerArray, tests::make_integer_array};

fn run_exclude_bench(
    group: &mut BenchmarkGroup<WallTime>,
    mut vals: IntegerArray,
    min: i64,
    max: i64,
) {
    group.bench_function(&format!("min_{}/max_{}", min, max), |b| {
        b.iter(|| {
            vals.exclude(min, max);
        })
    });
}

fn bench_integer_array_exclude(c: &mut Criterion) {
    let mut g = c.benchmark_group("bench_integer_array_exclude");

    // golang: 4.542 ns/op
    // rust:   3.3027 ns
    run_exclude_bench(&mut g, make_integer_array(1000, 1000, 2000), 0, 500);

    // golang: 35.89 ns/op
    // rust:   29.594 ns
    run_exclude_bench(&mut g, make_integer_array(1000, 1000, 2000), 1250, 1750);

    // golang: 4.426 ns/op
    // rust:   3.3099 ns
    run_exclude_bench(&mut g, make_integer_array(1000, 1000, 2000), 0, 1000);

    // golang: 4.597 ns/op
    // rust:   3.6555 ns
    run_exclude_bench(&mut g, make_integer_array(1000, 1000, 2000), 1999, 2000);

    // golang: 4.540 ns/op
    // rust:   3.2885 ns
    run_exclude_bench(&mut g, make_integer_array(10000, 10000, 20000), 0, 5000);

    // golang: 43.18 ns/op
    // rust:   37.585 ns
    run_exclude_bench(
        &mut g,
        make_integer_array(10000, 10000, 20000),
        12500,
        17500,
    );

    // golang: 4.502 ns/op
    // rust:   3.3138 ns
    run_exclude_bench(&mut g, make_integer_array(10000, 10000, 20000), 0, 10000);

    // golang: 4.529 ns/op
    // rust:   3.6368 ns
    run_exclude_bench(
        &mut g,
        make_integer_array(10000, 10000, 20000),
        19999,
        20000,
    );
}

fn run_include_bench(
    group: &mut BenchmarkGroup<WallTime>,
    mut vals: IntegerArray,
    min: i64,
    max: i64,
) {
    group.bench_function(&format!("min_{}/max_{}", min, max), |b| {
        b.iter(|| {
            vals.include(min, max);
        })
    });
}

fn bench_integer_array_include(c: &mut Criterion) {
    let mut g = c.benchmark_group("bench_integer_array_include");

    // golang: 4.038 ns/op
    // rust:   2.9605 ns
    run_include_bench(&mut g, make_integer_array(1000, 1000, 2000), 0, 500);

    // golang: 34.36 ns/op
    // rust:   26.625 ns
    run_include_bench(&mut g, make_integer_array(1000, 1000, 2000), 1250, 1750);

    // golang: 12.23 ns/op
    // rust:   12.034 ns
    run_include_bench(&mut g, make_integer_array(1000, 1000, 2000), 0, 1000);

    // golang: 11.96 ns/op
    // rust:   11.576 ns
    run_include_bench(&mut g, make_integer_array(1000, 1000, 2000), 1999, 2000);

    // golang: 4.445 ns/op
    // rust:   2.9792 ns
    run_include_bench(&mut g, make_integer_array(10000, 10000, 20000), 0, 5000);

    // golang: 39.39 ns/op
    // rust:   29.684 ns
    run_include_bench(
        &mut g,
        make_integer_array(10000, 10000, 20000),
        12500,
        17500,
    );

    // golang: 12.45 ns/op
    // rust:   11.786 ns
    run_include_bench(&mut g, make_integer_array(10000, 10000, 20000), 0, 10000);

    // golang: 12.68 ns/op
    // rust:   11.349 ns
    run_include_bench(
        &mut g,
        make_integer_array(10000, 10000, 20000),
        19999,
        20000,
    );
}

criterion_group!(
    benches,
    bench_integer_array_exclude,
    bench_integer_array_include
);
criterion_main!(benches);
