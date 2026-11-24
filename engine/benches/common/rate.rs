use std::time::Duration;

use reefsdb::common::rate::*;
use criterion::{Criterion, criterion_group, criterion_main};

// fn allow_n_in_parallel(c: &mut Criterion) {

// }

//
fn wait_n_no_delay(c: &mut Criterion) {
    c.bench_function("wait_n_no_delay", |b| {
        b.iter(|| {
            let lim = Limiter::new(Limit::new(7405723.0), 3);
            let _ = lim.wait_n(1, Duration::ZERO);
        })
    });
}

criterion_group!(benches, wait_n_no_delay);
criterion_main!(benches);
