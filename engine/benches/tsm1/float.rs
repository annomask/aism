use criterion::{Criterion, criterion_group, criterion_main};
use reefsdb::storage::tsm1::data_encoding::float::{
    FloatDecoder, FloatEncoder, tests::TWO_HOURS_DATA,
};

// golang: 11889 ns/op
// rust:   798.81 ns
fn bench_float_encoder(c: &mut Criterion) {
    c.bench_function("bench_float_encoder", |b| {
        b.iter(|| {
            let mut enc = FloatEncoder::new();
            for v in TWO_HOURS_DATA.iter() {
                enc.write(*v);
            }
        })
    });
}

// golang: 650.8 ns/op
// rust:   816.93 ns
fn bench_float_decoder(c: &mut Criterion) {
    let mut enc = FloatEncoder::new();
    for p in TWO_HOURS_DATA.iter() {
        enc.write(*p);
    }
    enc.flush();

    let bytes = enc.bytes().unwrap();

    c.bench_function("bench_float_decoder", |b| {
        b.iter(|| {
            let mut dec = FloatDecoder::new();
            dec.set_bytes(bytes).unwrap();
            for _ in 0..TWO_HOURS_DATA.len() {
                let _ = dec.next();
            }
        })
    });
}

// bench_float_decoder_decode_all/size=1
// golang: 22.56 ns/op
// rust:   52.765 ns
// bench_float_decoder_decode_all/size=55
// golang: 325.8 ns/op
// rust:   428.37 ns
// bench_float_decoder_decode_all/size=550
// golang: 2.890 µs/op
// rust:   3.8537 µs
// bench_float_decoder_decode_all/size=1000
// golang: 5.241 µs/op
// rust:   8.4070 µs
fn bench_float_decoder_decode_all(c: &mut Criterion) {
    let benchmarks = vec![1, 55, 550, 1000];

    let mut g = c.benchmark_group("bench_float_decoder_decode_all");
    for size in benchmarks {
        let mut enc = FloatEncoder::new();
        for c in 0..size {
            enc.write(TWO_HOURS_DATA[c % TWO_HOURS_DATA.len()]);
        }
        enc.flush();

        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("size={}", size), |b| {
            b.iter(|| {
                let mut dec = FloatDecoder::new();
                let _ = dec.set_bytes(bytes);
                let mut count = 0;
                for _ in dec {
                    count += 1;
                }
                assert_eq!(
                    size, count,
                    "unexpected value count, exp {}, got {}",
                    size, count
                );
            })
        });
    }
}

criterion_group!(
    benches,
    bench_float_encoder,
    bench_float_decoder,
    bench_float_decoder_decode_all,
);
criterion_main!(benches);
