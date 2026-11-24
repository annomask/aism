use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use reefsdb::models::points::{BoolValue, FloatValue, IntegerValue, StringValue, Value};
use reefsdb::storage::tsm1::{
    cursor::array_values::{BoolArray, FloatArray, IntegerArray, StringArray},
    data_encoding::{
        array_encoding::{
            decode_bool_array_block, decode_float_array_block, decode_integer_array_block,
            decode_string_array_block,
        },
        encoding::tests::get_times,
        encoding_values::Values,
    },
};

// bench_array_encoding_decode_integer_array_block/rle/size_5
// golang: 56.26 ns/op
// rust:   162.49 ns
// bench_array_encoding_decode_integer_array_block/rle/size_55
// golang: 62.83 ns/op
// rust:   395.10 ns
// bench_array_encoding_decode_integer_array_block/rle/size_5555
// golang: 108.3 ns/op
// rust:   746.44 ns
// bench_array_encoding_decode_integer_array_block/rle/size_1000
// golang: 174.5 ns/op
// rust:   1.0282 µs
// bench_array_encoding_decode_integer_array_block/s8b/size_5
// golang: 73.42 ns/op
// rust:   259.02 ns
// bench_array_encoding_decode_integer_array_block/s8b/size_55
// golang: 69.69 ns/op
// rust:   497.35 ns
// bench_array_encoding_decode_integer_array_block/s8b/size_555
// golang: 236.1 ns/op
// rust:   1.4560 µs
// bench_array_encoding_decode_integer_array_block/s8b/size_1000
// golang: 376.2 ns/op
// rust:   2.3913 µs
fn bench_array_encoding_decode_integer_array_block(c: &mut Criterion) {
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

    let mut g = c.benchmark_group("bench_array_encoding_decode_integer_array_block");
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

        let array_len = values.len();
        let bytes = Values::encode(&values).unwrap();
        // TODO: RunParallel
        g.bench_function(&format!("{}/size_{}", typ, n), |b| {
            b.iter(|| {
                let mut decode_values = IntegerArray::new(array_len);
                decode_integer_array_block(&bytes, &mut decode_values).unwrap();
            })
        });
    }
}

// bench_array_encoding_decode_float_array_block/5
// golang: 60.34 ns/op
// rust:   273.98 ns
// bench_array_encoding_decode_float_array_block/55
// golang: 90.81 ns/op
// rust:   1.4944 µs
// bench_array_encoding_decode_float_array_block/555
// golang: 495.0 ns/op
// rust:   10.076 µs
// bench_array_encoding_decode_float_array_block/1000
// golang: 881.0 ns/op
// rust:   16.759 µs
fn bench_array_encoding_decode_float_array_block(c: &mut Criterion) {
    let cases = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_array_encoding_decode_float_array_block");
    for n in cases {
        let value_count = n;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for t in times.iter() {
            values.push(Value::FloatValue(FloatValue::new(*t, *t as f64)))
        }

        let array_len = values.len();
        let bytes = Values::encode(&values).unwrap();
        // TODO: RunParallel
        g.bench_function(&format!("{}", n), |b| {
            b.iter(|| {
                let mut decode_values = FloatArray::new(array_len);
                decode_float_array_block(&bytes, &mut decode_values).unwrap();
            })
        });
    }
}

// bench_array_encoding_decode_bool_array_block/5
// golang: 54.31 ns/op
// rust:   154.02 ns
// bench_array_encoding_decode_bool_array_block/55
// golang: 66.85 ns/o
// rust:   332.52 ns
// bench_array_encoding_decode_bool_array_block/555
// golang: 156.4 ns/op
// rust:   1.1030 µs
// bench_array_encoding_decode_bool_array_block/1000
// golang: 265.2 ns/op
// rust:   1.5893 µs
fn bench_array_encoding_decode_bool_array_block(c: &mut Criterion) {
    let cases = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_array_encoding_decode_bool_array_block");
    for n in cases {
        let value_count = n;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for t in times.iter() {
            values.push(Value::BoolValue(BoolValue::new(*t, true)))
        }

        let array_len = values.len();
        let bytes = Values::encode(&values).unwrap();
        // TODO: RunParallel
        g.bench_function(&format!("{}", n), |b| {
            b.iter(|| {
                let mut decode_values = BoolArray::new(array_len);
                decode_bool_array_block(&bytes, &mut decode_values).unwrap();
            })
        });
    }
}

// bench_array_encoding_decode_string_array_block/5
// golang: 78.44 ns/op
// rust:   684.91 ns
// bench_array_encoding_decode_string_array_block/55
// golang: 396.7 ns/op
// rust:   4.9017 µs
// bench_array_encoding_decode_string_array_block/555
// golang: 3729 ns/op
// rust:   44.245 µs
// bench_array_encoding_decode_string_array_block/1000
// golang: 6150 ns/op
// rust:   80.640 µs
fn bench_array_encoding_decode_string_array_block(c: &mut Criterion) {
    let cases = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_array_encoding_decode_string_array_block");
    for n in cases {
        let value_count = n;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for t in times.iter() {
            values.push(Value::StringValue(StringValue::new(
                *t,
                format!("value {}", *t),
            )))
        }

        let array_len = values.len();
        let bytes = Values::encode(&values).unwrap();
        // TODO: RunParallel
        g.bench_function(&format!("{}", n), |b| {
            b.iter(|| {
                let mut decode_values = StringArray::new(array_len);
                decode_string_array_block(&bytes, &mut decode_values).unwrap();
            })
        });
    }
}

criterion_group!(
    benches,
    bench_array_encoding_decode_integer_array_block,
    bench_array_encoding_decode_float_array_block,
    bench_array_encoding_decode_bool_array_block,
    bench_array_encoding_decode_string_array_block
);
criterion_main!(benches);
