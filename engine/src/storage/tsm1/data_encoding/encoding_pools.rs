use crossbeam_channel::{Receiver, Sender, bounded};
use once_cell::sync::Lazy;

use super::{
    bool::BoolEncoder, float::FloatEncoder, int::IntegerEncoder, string::StringEncoder,
    timestamp::TimeEncoder,
};
use crate::{
    config::DEFAULT_MAX_POINTS_PER_BLOCK,
    storage::tsm1::data_encoding::{
        bool::BoolDecoder, float::FloatDecoder, int::IntegerDecoder, string::StringDecoder,
        timestamp::TimeDecoder,
    },
};

pub struct TimeEncoderPools {
    pool: (Sender<TimeEncoder>, Receiver<TimeEncoder>),
    f: fn(usize) -> TimeEncoder,
}

impl TimeEncoderPools {
    fn new(max: usize, f: fn(usize) -> TimeEncoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> TimeEncoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: TimeEncoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

pub struct TimeDecoderPools {
    pool: (Sender<TimeDecoder>, Receiver<TimeDecoder>),
    f: fn(usize) -> TimeDecoder,
}

impl TimeDecoderPools {
    fn new(max: usize, f: fn(usize) -> TimeDecoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> TimeDecoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: TimeDecoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

pub struct IntegerEncoderPools {
    pool: (Sender<IntegerEncoder>, Receiver<IntegerEncoder>),
    f: fn(usize) -> IntegerEncoder,
}

impl IntegerEncoderPools {
    fn new(max: usize, f: fn(usize) -> IntegerEncoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> IntegerEncoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: IntegerEncoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

pub struct IntegerDecoderPools {
    pool: (Sender<IntegerDecoder>, Receiver<IntegerDecoder>),
    f: fn(usize) -> IntegerDecoder,
}

impl IntegerDecoderPools {
    fn new(max: usize, f: fn(usize) -> IntegerDecoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> IntegerDecoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: IntegerDecoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

pub struct FloatEncoderPools {
    pool: (Sender<FloatEncoder>, Receiver<FloatEncoder>),
    f: fn(usize) -> FloatEncoder,
}

impl FloatEncoderPools {
    fn new(max: usize, f: fn(usize) -> FloatEncoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> FloatEncoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: FloatEncoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

pub struct FloatDecoderPools {
    pool: (Sender<FloatDecoder>, Receiver<FloatDecoder>),
    f: fn(usize) -> FloatDecoder,
}

impl FloatDecoderPools {
    fn new(max: usize, f: fn(usize) -> FloatDecoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> FloatDecoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: FloatDecoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

pub struct BoolEncoderPools {
    pool: (Sender<BoolEncoder>, Receiver<BoolEncoder>),
    f: fn(usize) -> BoolEncoder,
}

impl BoolEncoderPools {
    fn new(max: usize, f: fn(usize) -> BoolEncoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> BoolEncoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: BoolEncoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

pub struct BoolDecoderPools {
    pool: (Sender<BoolDecoder>, Receiver<BoolDecoder>),
    f: fn(usize) -> BoolDecoder,
}

impl BoolDecoderPools {
    fn new(max: usize, f: fn(usize) -> BoolDecoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> BoolDecoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: BoolDecoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

pub struct StringEncoderPools {
    pool: (Sender<StringEncoder>, Receiver<StringEncoder>),
    f: fn(usize) -> StringEncoder,
}

impl StringEncoderPools {
    fn new(max: usize, f: fn(usize) -> StringEncoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> StringEncoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: StringEncoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

pub struct StringDecoderPools {
    pool: (Sender<StringDecoder>, Receiver<StringDecoder>),
    f: fn(usize) -> StringDecoder,
}

impl StringDecoderPools {
    fn new(max: usize, f: fn(usize) -> StringDecoder) -> Self {
        let (sender, receiver) = bounded(max);
        Self {
            pool: (sender, receiver),
            f,
        }
    }

    /// Returns a item from the pool or a new instance if the pool is empty.
    /// Items returned may not be in the zero state and should be reset by the caller.
    pub fn get(&self, sz: usize) -> StringDecoder {
        crossbeam_channel::select! {
            recv(self.pool.1) -> c => c.unwrap(),
            default => (self.f)(sz),
        }
    }

    /// Puts an item back to the pool.  If the pool is full, the item is discarded.
    pub fn put(&self, c: StringDecoder) {
        crossbeam_channel::select! {
            send(self.pool.0, c) -> _ => {},
            default => {},
        }
    }
}

// func init() {
// 	// Prime the pools with one encoder/decoder for each available CPU.
// 	vals := make([]interface{}, 0, runtime.NumCPU())
// 	for _, p := range []*pool.Generic{
// 		timeEncoderPool, timeDecoderPool,
// 		integerEncoderPool, integerDecoderPool,
// 		floatDecoderPool, floatDecoderPool,
// 		stringEncoderPool, stringEncoderPool,
// 		booleanEncoderPool, booleanDecoderPool,
// 	} {
// 		vals = vals[:0]
// 		// Check one out to force the allocation now and hold onto it
// 		for i := 0; i < runtime.NumCPU(); i++ {
// 			v := p.Get(tsdb.DefaultMaxPointsPerBlock)
// 			vals = append(vals, v)
// 		}
// 		// Add them all back
// 		for _, v := range vals {
// 			p.Put(v)
// 		}
// 	}
// }
// var (
// 	// encoder pools

// 	timeEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return NewTimeEncoder(sz)
// 	})
// 	integerEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return NewIntegerEncoder(sz)
// 	})
// 	floatEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return NewFloatEncoder()
// 	})
// 	stringEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return NewStringEncoder(sz)
// 	})
// 	booleanEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return NewBooleanEncoder(sz)
// 	})

// 	// decoder pools

// 	timeDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return &TimeDecoder{}
// 	})
// 	integerDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return &IntegerDecoder{}
// 	})
// 	floatDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return &FloatDecoder{}
// 	})
// 	stringDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return &StringDecoder{}
// 	})
// 	booleanDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
// 		return &BooleanDecoder{}
// 	})
// )

pub fn init_encoding_pools() {
    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = TIME_ENCODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        TIME_ENCODER_POOLS.put(v);
    }

    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = INTEGER_ENCODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        INTEGER_ENCODER_POOLS.put(v);
    }

    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = FLOAT_ENCODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        FLOAT_ENCODER_POOLS.put(v);
    }

    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = BOOL_ENCODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        BOOL_ENCODER_POOLS.put(v);
    }

    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = STRING_ENCODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        STRING_ENCODER_POOLS.put(v);
    }

    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = TIME_DECODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        TIME_DECODER_POOLS.put(v);
    }

    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = INTEGER_DECODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        INTEGER_DECODER_POOLS.put(v);
    }

    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = FLOAT_DECODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        FLOAT_DECODER_POOLS.put(v);
    }

    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = BOOL_DECODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        BOOL_DECODER_POOLS.put(v);
    }

    let mut vals = vec![];
    for _ in 0..num_cpus::get() {
        let v = STRING_DECODER_POOLS.get(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
        vals.push(v);
    }
    for v in vals {
        STRING_DECODER_POOLS.put(v);
    }
}

// Encoder pools
pub static TIME_ENCODER_POOLS: Lazy<TimeEncoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_time_encoder(sz: usize) -> TimeEncoder {
        TimeEncoder::new(sz as i64)
    }
    TimeEncoderPools::new(sz, new_time_encoder)
});

pub static INTEGER_ENCODER_POOLS: Lazy<IntegerEncoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_integer_encoder(sz: usize) -> IntegerEncoder {
        IntegerEncoder::new(sz as i64)
    }
    IntegerEncoderPools::new(sz, new_integer_encoder)
});

pub static FLOAT_ENCODER_POOLS: Lazy<FloatEncoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_float_encoder(sz: usize) -> FloatEncoder {
        FloatEncoder::new()
    }
    FloatEncoderPools::new(sz, new_float_encoder)
});

pub static BOOL_ENCODER_POOLS: Lazy<BoolEncoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_bool_encoder(sz: usize) -> BoolEncoder {
        BoolEncoder::new(sz as i64)
    }
    BoolEncoderPools::new(sz, new_bool_encoder)
});

pub static STRING_ENCODER_POOLS: Lazy<StringEncoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_string_encoder(sz: usize) -> StringEncoder {
        StringEncoder::new(sz as i64)
    }
    StringEncoderPools::new(sz, new_string_encoder)
});

// Decoder pools
pub static TIME_DECODER_POOLS: Lazy<TimeDecoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_time_decoder(sz: usize) -> TimeDecoder {
        TimeDecoder::new()
    }
    TimeDecoderPools::new(sz, new_time_decoder)
});

pub static INTEGER_DECODER_POOLS: Lazy<IntegerDecoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_integer_decoder(sz: usize) -> IntegerDecoder {
        IntegerDecoder::new()
    }
    IntegerDecoderPools::new(sz, new_integer_decoder)
});

pub static FLOAT_DECODER_POOLS: Lazy<FloatDecoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_float_decoder(sz: usize) -> FloatDecoder {
        FloatDecoder::new()
    }
    FloatDecoderPools::new(sz, new_float_decoder)
});

pub static BOOL_DECODER_POOLS: Lazy<BoolDecoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_bool_decoder(sz: usize) -> BoolDecoder {
        BoolDecoder::new()
    }
    BoolDecoderPools::new(sz, new_bool_decoder)
});

pub static STRING_DECODER_POOLS: Lazy<StringDecoderPools> = Lazy::new(|| {
    let sz = num_cpus::get();
    fn new_string_decoder(sz: usize) -> StringDecoder {
        StringDecoder::new()
    }
    StringDecoderPools::new(sz, new_string_decoder)
});

pub fn get_time_encoder(sz: usize) -> TimeEncoder {
    let mut x = TIME_ENCODER_POOLS.get(sz);
    x.reset();
    x
}

pub fn put_time_encoder(enc: TimeEncoder) {
    TIME_ENCODER_POOLS.put(enc);
}

pub fn get_integer_encoder(sz: usize) -> IntegerEncoder {
    let mut x = INTEGER_ENCODER_POOLS.get(sz);
    x.reset();
    x
}

pub fn put_integer_encoder(enc: IntegerEncoder) {
    INTEGER_ENCODER_POOLS.put(enc);
}

pub fn get_unsigned_encoder(sz: usize) -> IntegerEncoder {
    let mut x = INTEGER_ENCODER_POOLS.get(sz);
    x.reset();
    x
}

pub fn put_unsigned_encoder(enc: IntegerEncoder) {
    INTEGER_ENCODER_POOLS.put(enc);
}

pub fn get_float_encoder(sz: usize) -> FloatEncoder {
    let mut x = FLOAT_ENCODER_POOLS.get(sz);
    x.reset();
    x
}

pub fn put_float_encoder(enc: FloatEncoder) {
    FLOAT_ENCODER_POOLS.put(enc);
}

pub fn get_bool_encoder(sz: usize) -> BoolEncoder {
    let mut x = BOOL_ENCODER_POOLS.get(sz);
    x.reset();
    x
}

pub fn put_bool_encoder(enc: BoolEncoder) {
    BOOL_ENCODER_POOLS.put(enc);
}

pub fn get_string_encoder(sz: usize) -> StringEncoder {
    let mut x = STRING_ENCODER_POOLS.get(sz);
    x.reset();
    x
}

pub fn put_string_encoder(enc: StringEncoder) {
    STRING_ENCODER_POOLS.put(enc);
}
