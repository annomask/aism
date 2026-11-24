use std::ops::{Deref, DerefMut};

// use snafu::{ensure, ResultExt, Snafu};

const PRIME64: u64 = 1099511628211;
const OFFSET64: u64 = 14695981039346656037;

// InlineFNV64a is an alloc-free port of the standard library's fnv64a.
// See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function.
pub struct InlineFnv64a(u64);

impl Deref for InlineFnv64a {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        self
    }
}

impl DerefMut for InlineFnv64a {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self
    }
}

// /// Error type for query iterator.
// #[derive(Debug, Snafu)]
// #[snafu(visibility(pub(crate)))]
// pub enum Error {
//     /// IO error.
//     #[snafu(display("{}", source))]
//     IOErr { source: std::io::Error },

//     /// String error.
//     #[snafu(display("{}", reason))]
//     StringErr { reason: String },
// }

// pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Returns a new instance of InlineFNV64a.
pub fn new_inline_fnv_64a() -> InlineFnv64a {
    InlineFnv64a(OFFSET64)
}

impl InlineFnv64a {
    // write adds data to the running hash.
    pub fn write(&mut self, data: &[u8]) -> usize {
        let mut hash = **self as u64;
        for c in data {
            hash ^= *c as u64;
            hash *= PRIME64;
        }
        *self = InlineFnv64a(hash);

        data.len()
    }

    // sum64 returns the uint64 of the current resulting hash.
    pub fn sum64(&self) -> u64 {
        **self
    }
}
