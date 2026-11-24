use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::Rng;
use reefsdb::models::points::{BoolValue, FloatValue, IntegerValue, StringValue, Value};
use reefsdb::storage::tsm1::data_encoding::{
    encoding::{
        decode_block, decode_bool_block, decode_float_block, decode_integer_block,
        decode_string_block, tests::get_times,
    },
    encoding_values::Values,
};

// rust: 19.500 µs
fn bench_decode_block_integer(c: &mut Criterion) {
    let value_count = 1000;
    let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
    let mut values = vec![];
    for (i, t) in times.iter().enumerate() {
        values.push(Value::IntegerValue(IntegerValue::new(*t, i as i64)))
    }

    let bytes = Values::encode(&values).unwrap();
    c.bench_function("bench_decode_block_integer", |b| {
        b.iter_with_large_drop(|| decode_block(&bytes))
    });
}

// rust: 40.099 µs
fn bench_decode_block_float(c: &mut Criterion) {
    let value_count = 1000;
    let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
    let mut values = vec![];
    for (i, t) in times.iter().enumerate() {
        values.push(Value::FloatValue(FloatValue::new(*t, i as f64)))
    }

    let bytes = Values::encode(&values).unwrap();
    c.bench_function("bench_decode_block_float_empty", |b| {
        b.iter_with_large_drop(|| decode_block(&bytes))
    });
}

// rust: 18.155 µs
fn bench_decode_block_bool(c: &mut Criterion) {
    let value_count = 1000;
    let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
    let mut values = vec![];
    for t in times.iter() {
        values.push(Value::BoolValue(BoolValue::new(*t, true)))
    }

    let bytes = Values::encode(&values).unwrap();
    c.bench_function("bench_decode_block_bool", |b| {
        b.iter_with_large_drop(|| decode_block(&bytes))
    });
}

// rust: 58.040 µs
fn bench_decode_block_string(c: &mut Criterion) {
    let value_count = 1000;
    let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
    let mut values = vec![];
    for (i, t) in times.iter().enumerate() {
        values.push(Value::StringValue(StringValue::new(
            *t,
            format!("value {}", i),
        )))
    }

    let bytes = Values::encode(&values).unwrap();
    c.bench_function("bench_decode_block_string", |b| {
        b.iter_with_large_drop(|| decode_block(&bytes))
    });
}

// bench_decode_integer_block/rle/size_5
// golang: 1.398 µs/op
// rust:   179.25 ns
// bench_decode_integer_block/rle/size_55
// golang: 1.840 µs/op
// rust:   606.83 ns
// bench_decode_integer_block/rle/size_555
// golang: 1.659 µs/op
// rust:   4.6798 µs
// bench_decode_integer_block/rle/size_1000
// golang: 1.702 µs/op
// rust:   8.6573 µs µs
// bench_decode_integer_block/s8b/size_5
// golang: 1.963 µs/op
// rust:   232.43 ns
// bench_decode_integer_block/s8b/size_55
// golang: 2.084 µs/op
// rust:   902.56 ns
// bench_decode_integer_block/s8b/size_555
// golang: 1.863 µs/op
// rust:   8.5446 µs
// bench_decode_integer_block/s8b/size_1000
// golang: 1.879 µs/op
// rust:   14.287 µs µs µs
fn bench_decode_integer_block(c: &mut Criterion) {
    let cases = vec![
        ("rle", 5),
        ("rle", 55),
        ("rle", 555),
        ("rle", 1000),
        ("s8b", 5),
        ("s8b", 55),
        ("s8b", 555),
        ("s8b", 1000),
    ];

    let mut g = c.benchmark_group("bench_decode_integer_block");
    for (typ, n) in cases {
        let value_count = n;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            let v = match typ {
                "rle" => i as i64,
                "s8b" => (i + rand::thread_rng().gen_range(0..10)) as i64,
                _ => unreachable!(),
            };
            values.push(Value::IntegerValue(IntegerValue::new(*t, v)))
        }

        let bytes = Values::encode(&values).unwrap();
        // TODO: RunParallel
        g.bench_function(&format!("{}/size_{}", typ, n), |b| {
            b.iter_with_large_drop(|| decode_integer_block(&bytes))
        });
    }
}

// bench_decode_float_block/size_5
// golang: 1.285 µs/op
// rust:   315.03 ns
// bench_decode_float_block/size_55
// golang: 1.656 µs/op
// rust:   1.5803 µs
// bench_decode_float_block/size_555
// golang: 5.624 µs/op
// rust:   13.950 µs
// bench_decode_float_block/size_1000
// golang: 10.032 µs/op
// rust:   27.403 µs
fn bench_decode_float_block(c: &mut Criterion) {
    let cases = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_decode_float_block");
    for n in cases {
        let value_count = n;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            values.push(Value::FloatValue(FloatValue::new(*t, i as f64)))
        }

        let bytes = Values::encode(&values).unwrap();
        // TODO: RunParallel
        g.bench_function(&format!("size_{}", n), |b| {
            b.iter_with_large_drop(|| decode_float_block(&bytes))
        });
    }
}

// bench_decode_bool_block/size_5
// golang: 1513 ns/op
// rust:   123.64 ns
// bench_decode_bool_block/size_55
// golang: 1.826 µs/op
// rust:   404.79 ns
// bench_decode_bool_block/size_555
// golang: 1.840 µs/op
// rust:   2.5720 µs
// bench_decode_bool_block/size_1000
// golang: 1.711 µs/op
// rust:   4.5108 µs
fn bench_decode_bool_block(c: &mut Criterion) {
    let cases = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_decode_bool_block");
    for n in cases {
        let value_count = n;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for t in times.iter() {
            values.push(Value::BoolValue(BoolValue::new(*t, true)))
        }

        let bytes = Values::encode(&values).unwrap();
        // TODO: RunParallel
        g.bench_function(&format!("size_{}", n), |b| {
            b.iter_with_large_drop(|| decode_bool_block(&bytes))
        });
    }
}

// bench_decode_string_block/size_5
// golang: 1.624 µs/op
// rust:   417.95 ns
// bench_decode_string_block/size_55
// golang: 1.490 µs/op
// rust:   3.8284 µs
// bench_decode_string_block/size_555
// golang: 10.098 µs/op
// rust:   38.221 µs
// bench_decode_string_block/size_1000
// golang: 18.244 µs/op
// rust:   65.908 µs
fn bench_decode_string_block(c: &mut Criterion) {
    let cases = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_decode_string_block");
    for n in cases {
        let value_count = n;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            values.push(Value::StringValue(StringValue::new(
                *t,
                format!("value {}", i),
            )))
        }

        let bytes = Values::encode(&values).unwrap();
        // TODO: RunParallel
        g.bench_function(&format!("size_{}", n), |b| {
            b.iter_with_large_drop(|| decode_string_block(&bytes))
        });
    }
}

// bench_values_dedeplicate
// golang: 46.588 µs/op
// rust:   21.412 µs
fn bench_values_dedeplicate(c: &mut Criterion) {
    let value_count = 1000;
    let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
    let mut values = vec![];
    for (i, t) in times.iter().enumerate() {
        values.push(Value::StringValue(StringValue::new(
            *t,
            format!("value {}", i),
        )))
    }

    let mut new_values = vec![Value::StringValue(StringValue::default()); times.len()];
    new_values.append(&mut values);

    // TODO: RunParallel
    c.bench_function("bench_values_dedeplicate", |b| {
        b.iter_custom(|iters| {
            let mut exec_time = std::time::Duration::ZERO;
            for _i in 0..iters {
                let values = new_values.clone();
                let start = std::time::Instant::now();
                let mut values = Values(values);
                black_box(values.deduplicate());
                exec_time += start.elapsed();
            }
            exec_time
        })
    });
}

// /////////////////////////////////////
// TODO: bench merge
// /////////////////////////////////////

// bench_values_encode_integer
// golang: 50.588 µs/op
// rust:   1.3816 µs
fn bench_values_encode_integer(c: &mut Criterion) {
    let value_count = 1024;
    let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
    let mut values = vec![];
    for (i, t) in times.iter().enumerate() {
        values.push(Value::IntegerValue(IntegerValue::new(*t, i as i64)))
    }

    c.bench_function("bench_values_encode_integer", |b| {
        b.iter_custom(|iters| {
            let mut exec_time = std::time::Duration::ZERO;
            for _i in 0..iters {
                let values = values.clone();
                let start = std::time::Instant::now();
                black_box(Values::encode(&values).unwrap());
                exec_time += start.elapsed();
            }
            exec_time
        })
    });
}

// bench_values_encode_float
// golang: 56.790 µs/op
// rust:   1.3784 µs
fn bench_values_encode_float(c: &mut Criterion) {
    let value_count = 1024;
    let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
    let mut values = vec![];
    for (i, t) in times.iter().enumerate() {
        values.push(Value::FloatValue(FloatValue::new(*t, i as f64)))
    }

    c.bench_function("bench_values_encode_float", |b| {
        b.iter_custom(|iters| {
            let mut exec_time = std::time::Duration::ZERO;
            for _i in 0..iters {
                let values = values.clone();
                let start = std::time::Instant::now();
                black_box(Values::encode(&values).unwrap());
                exec_time += start.elapsed();
            }
            exec_time
        })
    });
}

// bench_values_encode_bool
// golang: 20.490 µs/op
// rust:   1.3734 µs
fn bench_values_encode_bool(c: &mut Criterion) {
    let value_count = 1024;
    let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
    let mut values = vec![];
    for (i, t) in times.iter().enumerate() {
        let v = if i % 2 == 0 { true } else { false };
        values.push(Value::BoolValue(BoolValue::new(*t, v)))
    }

    c.bench_function("bench_values_encode_bool", |b| {
        b.iter_custom(|iters| {
            let mut exec_time = std::time::Duration::ZERO;
            for _i in 0..iters {
                let values = values.clone();
                let start = std::time::Instant::now();
                black_box(Values::encode(&values).unwrap());
                exec_time += start.elapsed();
            }
            exec_time
        })
    });
}

// bench_values_encode_string
// golang: 74.132 µs/op
// rust:   13.181 µs
fn bench_values_encode_string(c: &mut Criterion) {
    let value_count = 1024;
    let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
    let mut values = vec![];
    for (i, t) in times.iter().enumerate() {
        values.push(Value::StringValue(StringValue::new(*t, i.to_string())))
    }

    c.bench_function("bench_values_encode_string", |b| {
        b.iter_custom(|iters| {
            let mut exec_time = std::time::Duration::ZERO;
            for _i in 0..iters {
                let values = values.clone();
                let start = std::time::Instant::now();
                black_box(Values::encode(&values).unwrap());
                exec_time += start.elapsed();
            }
            exec_time
        })
    });
}

criterion_group!(
    benches,
    bench_decode_block_integer,
    bench_decode_block_float,
    bench_decode_block_bool,
    bench_decode_block_string,
    bench_decode_integer_block,
    bench_decode_float_block,
    bench_decode_bool_block,
    bench_decode_string_block,
    bench_values_dedeplicate,
    bench_values_encode_integer,
    bench_values_encode_float,
    bench_values_encode_bool,
    bench_values_encode_string
);
criterion_main!(benches);
