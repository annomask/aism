use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use reefsdb::storage::tsm1::data_encoding::encoding_values::{
    IntegerValues, tests::make_integer_values,
};

fn run_exclude_bench(
    group: &mut BenchmarkGroup<WallTime>,
    mut vals: IntegerValues,
    min: i64,
    max: i64,
) {
    group.bench_function(&format!("min_{}/max_{}", min, max), |b| {
        b.iter(|| {
            vals.exclude(min, max);
        })
    });
}

fn bench_integer_values_exclude(c: &mut Criterion) {
    let mut g = c.benchmark_group("bench_integer_values_exclude");

    // golang: 5.257 ns/op
    // rust:   3.6752 ns
    run_exclude_bench(&mut g, make_integer_values(1000, 1000, 2000), 0, 500);

    // golang: 162.0 ns/op
    // rust:   330.19 ns
    run_exclude_bench(&mut g, make_integer_values(1000, 1000, 2000), 1250, 1750);

    // golang: 5.256 ns/op
    // rust:   3.6518 ns
    run_exclude_bench(&mut g, make_integer_values(1000, 1000, 2000), 0, 1000);

    // golang: 26.20 ns/op
    // rust:   4.1423 ns
    run_exclude_bench(&mut g, make_integer_values(1000, 1000, 2000), 1999, 2000);

    // golang: 5.269 ns/op
    // rust:   3.6772 ns
    run_exclude_bench(&mut g, make_integer_values(10000, 10000, 20000), 0, 5000);

    // golang: 1.125 µs/op
    // rust:   3.0310 µs
    run_exclude_bench(
        &mut g,
        make_integer_values(10000, 10000, 20000),
        12500,
        17500,
    );

    // golang: 5.265 ns/op
    // rust:   3.7165 ns
    run_exclude_bench(&mut g, make_integer_values(10000, 10000, 20000), 0, 10000);

    // golang: 33.77 ns/op
    // rust:   4.0880 ns
    run_exclude_bench(
        &mut g,
        make_integer_values(10000, 10000, 20000),
        19999,
        20000,
    );
}

fn run_include_bench(
    group: &mut BenchmarkGroup<WallTime>,
    mut vals: IntegerValues,
    min: i64,
    max: i64,
) {
    group.bench_function(&format!("min_{}/max_{}", min, max), |b| {
        b.iter(|| {
            vals.include(min, max);
        })
    });
}

fn bench_integer_values_include(c: &mut Criterion) {
    let mut g = c.benchmark_group("bench_integer_values_include");

    // golang: 4.862 ns/op
    // rust:   3.6932 ns
    run_include_bench(&mut g, make_integer_values(1000, 1000, 2000), 0, 500);

    // golang: 30.85 ns/op
    // rust:   373.49 ns
    run_include_bench(&mut g, make_integer_values(1000, 1000, 2000), 1250, 1750);

    // golang: 35.83 ns/op
    // rust:   29.527 ns
    run_include_bench(&mut g, make_integer_values(1000, 1000, 2000), 0, 1000);

    // golang: 30.36 ns/op
    // rust:   29.589 ns
    run_include_bench(&mut g, make_integer_values(1000, 1000, 2000), 1999, 2000);

    // golang: 4.799 ns/op
    // rust:   3.6748 ns
    run_include_bench(&mut g, make_integer_values(10000, 10000, 20000), 0, 5000);

    // golang: 39.55 ns/op
    // rust:    2.9281 µs
    run_include_bench(
        &mut g,
        make_integer_values(10000, 10000, 20000),
        12500,
        17500,
    );

    // golang: 44.23 ns/op
    // rust:   29.365 ns
    run_include_bench(&mut g, make_integer_values(10000, 10000, 20000), 0, 10000);

    // golang: 37.14 ns/op
    // rust:   29.232 ns
    run_include_bench(
        &mut g,
        make_integer_values(10000, 10000, 20000),
        19999,
        20000,
    );
}

criterion_group!(
    benches,
    bench_integer_values_exclude,
    bench_integer_values_include
);
criterion_main!(benches);
