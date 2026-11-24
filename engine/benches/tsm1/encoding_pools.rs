use criterion::{Criterion, black_box, criterion_group, criterion_main};
use reefsdb::storage::tsm1::data_encoding::{
    encoding_pools::*,
    float::{FloatDecoder, FloatEncoder},
    timestamp::{TimeDecoder, TimeEncoder},
};

// 177.59 ns
fn bench_float_encoding_pool(c: &mut Criterion) {
    // fill encoder pools first.
    for _ in 0..num_cpus::get() {
        TIME_ENCODER_POOLS.put(TimeEncoder::new(1024));
        FLOAT_ENCODER_POOLS.put(FloatEncoder::new());
    }
    c.bench_function("bench_float_encoding_pool", |b| {
        b.iter(|| {
            let tenc = TIME_ENCODER_POOLS.get(1024);
            let venc = FLOAT_ENCODER_POOLS.get(1024);

            TIME_ENCODER_POOLS.put(tenc);
            FLOAT_ENCODER_POOLS.put(venc);
        })
    });
}

// 324.94 ns
// 83.475 ns  // without put
fn bench_float_decoding_pool(c: &mut Criterion) {
    // fill decoder pools first.
    for _ in 0..num_cpus::get() {
        TIME_DECODER_POOLS.put(TimeDecoder::new());
        FLOAT_DECODER_POOLS.put(FloatDecoder::new());
    }

    c.bench_function("bench_float_decoding_pool", |b| {
        b.iter(|| {
            let tdec = TIME_DECODER_POOLS.get(1024);
            let vdec = FLOAT_DECODER_POOLS.get(1024);
            TIME_DECODER_POOLS.put(tdec);
            FLOAT_DECODER_POOLS.put(vdec);
        })
    });
}

// 319.83 ns
fn bench_float_encoder(c: &mut Criterion) {
    c.bench_function("bench_float_encoder", |b| {
        b.iter(|| {
            black_box(TimeEncoder::new(1024));
            black_box(FloatEncoder::new());
        })
    });
}

// 0.0000 ps
fn bench_float_decoder(c: &mut Criterion) {
    c.bench_function("bench_float_decoder", |b| {
        b.iter(|| {
            black_box(TimeDecoder::new());
            black_box(FloatDecoder::new());
        })
    });
}

criterion_group!(
    benches,
    bench_float_encoding_pool,
    bench_float_decoding_pool,
    bench_float_encoder,
    bench_float_decoder,
);
criterion_main!(benches);
