pub mod array_encoding;
pub mod batch_bool;
pub mod batch_float;
pub mod batch_int;
pub mod batch_string;
pub mod batch_timestamp;
pub mod bit_reader;
pub mod bit_stream;
pub mod bool;
pub mod encoding;
pub mod encoding_pools;
pub mod encoding_values;
pub mod float;
pub mod int;
pub mod simple8b;
pub(crate) mod streaming_int;
pub mod string;
pub mod timestamp;

#[cfg(test)]
mod compatibility_tests;
