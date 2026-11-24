use std::ops::{Deref, DerefMut};

use bytes::Buf;
use snafu::Snafu;
/// Error type for WalSegmentReader.
#[derive(Debug, Snafu)]
pub enum Error {
    /// Unmarshal binary error.
    #[snafu(display("unmarshal binary error"))]
    UnmarshalBinary,
}

/// Result type for compressed.
pub type Result<T, E = Error> = std::result::Result<T, E>;

trait Iterable {
    fn decode(&self, i: usize, last: u32) -> (u32, usize);
    fn len(&self) -> usize;
    // fn iter(&self) -> Iter<I>;
}

pub(crate) struct Iter<'a> {
    i: usize,
    last: u32,
    v: &'a (dyn Iterable + 'a),
}

// pub struct PeekableIter<'a>(std::iter::Peekable<Iter<'a>>);

// impl<'a> PeekableIter<'a> {
//     pub fn has_next(&self) -> bool {
//         let x = self.0;
//         self.0.i < self.0.v.len()
//     }
// }

impl<'a> Iterator for Iter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.v.len() {
            return None;
        }

        let (n, i) = self.v.decode(self.i, self.last);
        self.last = n;
        self.i = i;
        Some(n)
    }

    // fn peek(&self) -> u32 {
    //     let (n, _) = self.v.decode(self.i, self.last);
    //     n
    // }

    // fn has_next(&self) -> bool {
    //     self.i < self.v.len()
    // }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CompressedList {
    pub(crate) count: u32,
    pub(crate) last: u32,
    pub(crate) b: VariableLengthList,
}

impl CompressedList {
    pub fn len(&self) -> usize {
        self.b.len()
    }

    pub fn capacity(&self) -> usize {
        self.b.capacity()
    }

    pub fn marshal_binary(&self) -> Result<Vec<u8>> {
        // Marshal the variableLengthList
        let mut bdata = self.b.marshal_binary()?; // FIXME: b.len 1876 -> 1880

        // At least 4 bytes for the two fixed sized values plus the size of bdata.
        let mut data = Vec::<u8>::with_capacity(4 + 4 + bdata.len());

        // Marshal the count and last values.
        data.append(&mut vec![
            // Number of items in the list.
            (self.count >> 24) as u8,
            (self.count >> 16) as u8,
            (self.count >> 8) as u8,
            self.count as u8,
            // The last item in the list
            (self.last >> 24) as u8,
            (self.last >> 16) as u8,
            (self.last >> 8) as u8,
            self.last as u8,
        ]);

        // Append the list
        data.append(&mut bdata);
        Ok(data)
    }

    pub fn unmarshal_binary(&mut self, data: &[u8]) -> Result<()> {
        // Set the count.
        self.count = (&data[..4]).get_u32(); // BigEndian
        let data = &data[4..];

        // Set the last value.
        self.last = (&data[..4]).get_u32();
        let data = &data[4..];

        // Set the list.
        let sz = (&data[..4]).get_u32();
        let data = &data[4..];
        self.b = VariableLengthList(vec![0u8; sz as usize]);
        for i in 0u32..sz {
            self.b[i as usize] = data[i as usize];
        }
        Ok(())
    }

    pub fn append(&mut self, x: u32) {
        self.count += 1;
        self.b.append(x.wrapping_sub(self.last));
        self.last = x;
    }

    pub fn iter(&self) -> Iter {
        Iter {
            i: 0usize,
            last: 0u32,
            v: self,
        }
    }
}

impl Iterable for CompressedList {
    fn len(&self) -> usize {
        self.b.len()
    }

    fn decode(&self, i: usize, last: u32) -> (u32, usize) {
        let (n, i) = self.b.decode(i, last);
        (n + last, i)
    }
}

pub(crate) fn new_compressed_list(size: usize) -> CompressedList {
    CompressedList {
        count: 0u32,
        last: 0u32,
        b: VariableLengthList(Vec::<u8>::with_capacity(size)),
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct VariableLengthList(Vec<u8>);

impl<'a> VariableLengthList {
    fn marshal_binary(&self) -> Result<Vec<u8>> {
        // 4 bytes for the size of the list, and a byte for each element in the list.
        let mut data = Vec::<u8>::with_capacity(4 + self.0.len());

        // Length of the list. We only need 32 bits because the size of the set
        // couldn't exceed that on 32 bit architectures.
        let sz = self.0.len();
        data.append(&mut vec![
            (sz >> 24) as u8,
            (sz >> 16) as u8,
            (sz >> 8) as u8,
            sz as u8,
        ]);

        // Marshal each element in the list.
        for i in 0..sz {
            data.push(self.0[i]);
        }

        Ok(data)
    }

    fn iter(&'a self) -> Iter<'a> {
        Iter {
            i: 0,
            last: 0,
            v: self,
        }
    }

    fn append(&mut self, x: u32) {
        let mut x = x;
        while x & 0xffffff80 != 0 {
            self.0.push(((x & 0x7f) | 0x80) as u8);
            x >>= 7;
        }
        self.0.push((x & 0x7f) as u8);
    }
}

impl Iterable for VariableLengthList {
    fn decode(&self, i: usize, _last: u32) -> (u32, usize) {
        let mut x = 0u32;
        let mut j = i;
        while self.0[j] & 0x80 != 0 {
            x |= ((self.0[j] & 0x7f) as u32) << ((j - i) * 7);
            j += 1usize;
        }
        x |= (self.0[j] as u32) << ((j - i) * 7);
        (x, j + 1)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl Deref for VariableLengthList {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VariableLengthList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
