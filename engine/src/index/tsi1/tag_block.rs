use std::io::{BufWriter, Write};

use crate::common::rhh;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, Bytes};
use integer_encoding::VarInt;
use snafu::{ResultExt, Snafu, ensure};

use super::index_file::INDEX_FILE_VERSION;
use crate::{
    index::tsi1::{
        LOAD_FACTOR, TagKeyElement, TagValueElement, decode_var, write_to, write_u_vint_to,
        write_u8_to, write_u64_to,
    },
    series::{series_file::SERIES_ID_SIZE, series_set},
};

// TAG_BLOCK_VERSION is the version of the tag block.
const TAG_BLOCK_VERSION: i32 = 1;

// Tag key flag constants.
const TAG_KEY_TOMBSTONE_FLAG: u8 = 0x01;

// Tag value flag constants.
const TAG_VALUE_TOMBSTONE_FLAG: u8 = 0x01;
const TAG_VALUE_SERIES_ID_SET_FLAG: u8 = 0x02;

// TagBlock variable size constants.
// TagBlock key block fields.
const TAG_KEY_N_SIZE: usize = 8;
const TAG_KEY_OFFSET_SIZE: usize = 8;
// TagBlock value block fields.
const TAG_VALUE_N_SIZE: usize = 8;
const TAG_VALUE_OFFSET_SIZE: usize = 8;

/// Error type for TagBlock.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOError { reason: std::io::Error },

    /// UnsupportedTagBlockVersion.
    #[snafu(display("unsupported tag block version"))]
    UnsupportedTagBlockVersion,

    /// TagBlockSizeMismatch.
    #[snafu(display("tag block size mismatch"))]
    TagBlockSizeMismatch,

    /// Series set error.
    #[snafu(display("{}", source))]
    SeriesSet {
        source: crate::series::series_set::Error,
    },

    /// TSI error.
    #[snafu(display("{}", source))]
    TSI { source: crate::index::tsi1::Error },

    /// String error.
    #[snafu(display("tsi: {}", reason))]
    StringErr { reason: String },

    /// DecodeVarIntFailed.
    #[snafu(display("decode var integer failed"))]
    DecodeVarIntFailed,
}

/// Result type for TagBlock.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

/// TagBlock represents tag key/value block for a single measurement.
#[derive(Default)]
pub struct TagBlock {
    data: Bytes,

    // TODO: 其实不需要，只需要保存offset size即可？
    value_data: Bytes,
    key_data: Bytes,
    hash_data: Bytes,

    version: i32, // tag block version
}

impl TagBlock {
    /// Returns the encoding version parsed from the data.
    /// Only valid after unmarshal_binary() has been successfully invoked.
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Unpacks data into the tag block. Tag block is not copied so data
    /// should be retained and unchanged after being passed into this function.
    pub fn unmarshal_binary(&mut self, data: &Bytes) -> Result<()> {
        // Read trailer.
        let t = read_tag_block_trailer(data)?;

        // Verify data size is correct.
        ensure!((data.len() as i64) == t.size, TagBlockSizeMismatchSnafu);

        // Save data section.
        self.value_data = data.slice(
            t.value_data.offset as usize..(t.value_data.offset + t.value_data.size) as usize,
        );

        // Save key data section.
        self.key_data =
            data.slice(t.key_data.offset as usize..(t.key_data.offset + t.key_data.size) as usize);

        // Save hash index block.
        self.hash_data = data.slice(
            t.hash_index.offset as usize..(t.hash_index.offset + t.hash_index.size) as usize,
        );

        // Save entire block.
        self.data = data.slice(..);

        Ok(())
    }

    /// Returns an element for a tag key.
    /// Returns an element with a nil key if not found.
    pub fn tag_key_elem(&self, key: &'_ [u8]) -> Option<TagBlockKeyElem> {
        let mut elem = TagBlockKeyElem::default();
        if !self.decode_tag_key_elem(key, &mut elem) {
            return None;
        }

        Some(elem)
    }

    pub fn decode_tag_key_elem(&self, key: &[u8], elem: &mut TagBlockKeyElem) -> bool {
        let key_n = (&self.hash_data[..TAG_KEY_N_SIZE])
            .read_u64::<BigEndian>()
            .unwrap() as i64;
        let hash = rhh::hash_key(key);
        let mut pos = hash % key_n;

        // Track current distance.
        let mut d = 0i64;
        loop {
            // Find offset of tag key.
            let offset = (&self.hash_data[TAG_KEY_N_SIZE + (pos as usize * TAG_KEY_OFFSET_SIZE)..])
                .read_u64::<BigEndian>()
                .unwrap();
            if offset == 0 {
                return false;
            }

            // Parse into element.
            elem.unmarshal(&self.data[offset as usize..], &self.data);

            // Return if keys match.
            if elem.key == key {
                return true;
            }

            // Check if we've exceeded the probe distance.
            if d > rhh::dist(rhh::hash_key(&elem.key), pos, key_n) {
                return false;
            }

            // Move position forward.
            pos = (pos + 1) % key_n;
            d += 1;

            if d > key_n {
                return false;
            }
        }
    }

    /// Returns an element for a tag value.
    pub fn tag_value_elem(&self, key: &[u8], value: &[u8]) -> Option<TagBlockValueElem> {
        let mut value_elem = TagBlockValueElem::default();

        if !self.decode_tag_value_elem(key, value, &mut value_elem) {
            return None;
        }

        Some(value_elem)
    }

    /// Returns an element for a tag value.
    pub fn decode_tag_value_elem(
        &self,
        key: &[u8],
        value: &[u8],
        value_elem: &mut TagBlockValueElem,
    ) -> bool {
        // Find key element, exit if not found.
        let mut key_elem = TagBlockKeyElem::default();
        if !self.decode_tag_key_elem(key, &mut key_elem) {
            // TODO: 4ms
            return false;
        }

        // Slice hash index data.
        let hash_data = key_elem.hash_index.buf;

        let value_n = (&hash_data[..TAG_VALUE_N_SIZE])
            .read_u64::<BigEndian>()
            .unwrap() as i64;
        let hash = rhh::hash_key(value); // TODO: 600 ns
        let mut pos = hash % value_n;

        // Track current distance
        let mut d = 0i64;
        loop {
            // Find offset of tag key.
            let offset = (&hash_data[TAG_VALUE_N_SIZE + (pos as usize * TAG_VALUE_OFFSET_SIZE)..])
                .read_u64::<BigEndian>()
                .unwrap();
            if offset == 0 {
                return false;
            }

            // Parse into element.
            value_elem.unmarshal(self.data.slice(offset as usize..));

            // Return if values match.
            if value_elem.value == value {
                return true;
            }

            // Check if we've exceeded the probe distance.
            if d > rhh::dist(rhh::hash_key(&value_elem.value), pos, value_n) {
                return false;
            }

            // Move position forward.
            pos = (pos + 1) % value_n;
            d += 1;

            if d > value_n {
                return false;
            }
        }
    }

    /// Returns an iterator over all the keys in the block.
    pub fn tag_key_iterator(&self) -> TagBlockKeyIter {
        TagBlockKeyIter {
            blk_data: self.data.slice(..),
            key_data: self.key_data.slice(..),
            // e: TagBlockKeyElem::default(),
        }
    }
}

// TagBlockKeyIter represents an iterator over all keys in a TagBlock.
#[derive(Clone)]
pub struct TagBlockKeyIter {
    blk_data: Bytes,
    key_data: Bytes,
    // e: TagBlockKeyElem,
}

impl Iterator for TagBlockKeyIter {
    type Item = TagBlockKeyElem;

    fn next(&mut self) -> Option<Self::Item> {
        // Exit when there is no data left.
        if self.key_data.is_empty() {
            return None;
        }

        // Unmarshal next element & move data forward.
        let mut e = TagBlockKeyElem::default();
        e.unmarshal(&self.key_data, &self.blk_data);
        self.key_data = self.key_data.slice(e.size as usize..);

        assert!(!e.key().is_empty(), "invalid zero-length tag key");
        Some(e)
    }
}

// TagBlockValueIter represents an iterator over all values for a tag key.
#[derive(Clone)]
pub struct TagBlockValueIter {
    data: Bytes,
}

impl Iterator for TagBlockValueIter {
    type Item = TagBlockValueElem;

    fn next(&mut self) -> Option<Self::Item> {
        // Exit when there is no data left.
        if self.data.is_empty() {
            return None;
        }

        // Unmarshal next element & move data forward.
        let mut e = TagBlockValueElem::default();
        e.unmarshal(Bytes::clone(&self.data));
        self.data = self.data.slice(e.size as usize..);

        assert!(!e.value().is_empty(), "invalid zero-length tag value");
        Some(e)
    }
}

#[derive(Default, Clone)]
struct Data {
    offset: u64,
    size: u64,
    buf: Bytes, // how to optimize to reference to &[u8] ?
}

#[derive(Default, Clone)]
struct HashIndexData {
    offset: u64,
    size: u64,
    buf: Bytes, // how to optimize to reference to &[u8] ?
}

// TODO: how to optimize to reference &[u8], other than copy to Vec ?
// TagBlockKeyElem represents a tag key element in a TagBlock.
#[derive(Default, Clone)]
pub struct TagBlockKeyElem {
    flag: u8,
    key: Bytes,

    // Value data
    data: Data,

    // Value hash index data
    hash_index: HashIndexData,

    size: i32,
}

/// TagBlockKeyElem represents a tag key element in a TagBlock.
impl TagBlockKeyElem {
    pub fn unmarshal(&mut self, buf: &[u8], data: &Bytes) {
        let mut buf = buf;
        let start = buf.len();

        // Parse flag data.
        (self.flag, buf) = (buf[0], &buf[1..]);

        // Parse data offset/size.
        (self.data.offset, buf) = ((&buf[..8]).get_u64(), &buf[8..]);
        (self.data.size, buf) = ((&buf[..8]).get_u64(), &buf[8..]);

        // // Slice data.
        self.data.buf =
            data.slice(self.data.offset as usize..(self.data.offset + self.data.size) as usize);

        // Parse hash index offset/size.
        (self.hash_index.offset, buf) = ((&buf[..8]).get_u64(), &buf[8..]);
        (self.hash_index.size, buf) = ((&buf[..8]).get_u64(), &buf[8..]);

        // Slice hash index data.
        self.hash_index.buf = data.slice(
            self.hash_index.offset as usize
                ..(self.hash_index.offset + self.hash_index.size) as usize,
        );

        // Parse key.
        if let Some((n, sz)) = u64::decode_var(buf) {
            (self.key, buf) = (
                data.slice_ref(&buf[sz..sz + n as usize]),
                &buf[sz + n as usize..],
            );
        }

        // Save length of elem.
        self.size = (start - buf.len()) as i32;
    }

    pub fn tag_value_iterator(&self) -> TagBlockValueIter {
        // if self.data.buf.is_empty() {
        //     return None;
        // }

        TagBlockValueIter {
            data: self.data.buf.slice(..),
        }
    }
}

impl TagKeyElement for TagBlockKeyElem {
    fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns true if the key has been tombstoned.
    fn deleted(&self) -> bool {
        (self.flag & TAG_KEY_TOMBSTONE_FLAG) != 0
    }

    // fn tag_value_iterator(&self) -> Option<Box<dyn TagValueIterator>> {
    //     if self.data.buf.is_empty() {
    //         return None;
    //     }
    //     Some(Box::new(TagBlockValueIter::new(self.data.buf)))
    // }
}

#[derive(Clone, Default, Debug)]
struct Series {
    n: u64,      // series count
    data: Bytes, // raw series data
}

#[derive(Clone, Default, Debug)]
pub struct TagBlockValueElem {
    flag: u8,
    value: Bytes,

    // Legacy uvarint-encoded series data.
    // Mutually exclusive with seriesIDSetData field.
    series: Series,

    // Roaring bitmap encoded series data.
    // Mutually exclusive with series.data field.
    series_id_set_data: Bytes,

    size: u64,
}

impl TagBlockValueElem {
    /// Returns the series count.
    pub fn series_n(&self) -> u64 {
        self.series.n
    }

    /// Returns the raw series data.
    pub fn series_data(&self) -> &[u8] {
        &self.series.data
    }

    /// Returns series ID at an index.
    pub fn series_id(&self, i: usize) -> u64 {
        (&self.series.data[i * SERIES_ID_SIZE..]).get_u64()
    }

    /// Returns a list decoded series ids.
    pub fn series_ids(&self) -> Result<Vec<u64>> {
        if !self.series_id_set_data.is_empty() {
            let ss = self.series_id_set()?;
            return Ok(ss.slice());
        }

        let mut a = Vec::<u64>::with_capacity(self.series.n as usize);
        let mut prev = 0u64;
        let mut data = self.series.data.slice(..);
        while !data.is_empty() {
            if let Some((delta, n)) = decode_var(&data) {
                data = data.slice(n..);

                let series_id = prev + delta;
                a.push(series_id);
                prev = series_id;
            } else {
                return Err(Error::DecodeVarIntFailed);
            }
        }

        Ok(a)
    }

    // SeriesIDSet returns a set of series ids.
    pub fn series_id_set(&self) -> Result<series_set::SeriesIDSet> {
        let ss = series_set::SeriesIDSet::new(&[]);

        // Read bitmap data directly from mmap, if available.
        if !self.series_id_set_data.is_empty() {
            ss.unmarshal_binary_unsafe(&self.series_id_set_data)
                .context(SeriesSetSnafu)?;
            return Ok(ss);
        }

        // Otherwise decode series ids from uvarint encoding.
        let mut prev = 0u64;
        let mut data = self.series.data.slice(..);
        while !data.is_empty() {
            if let Some((delta, n)) = decode_var(&data) {
                data = data.slice(n..);

                let series_id = prev + delta;
                ss.add_no_lock(series_id);
                prev = series_id;
            } else {
                return Err(Error::DecodeVarIntFailed);
            }
        }

        Ok(ss)
    }

    /// Returns the size of the element.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Unmarshal unmarshals buf into e.
    fn unmarshal(&mut self, mut buf: Bytes) {
        // let mut buf = buf;
        let start = buf.len();

        // Parse flag data.
        (self.flag, buf) = (buf.as_ref()[0], buf.slice(1..));

        let (mut sz, mut n) = (0u64, 0usize);
        // Parse value.
        if let Some((value, size)) = u64::decode_var(&buf) {
            (sz, n) = (value, size);
        }
        (self.value, buf) = (buf.slice(n..n + sz as usize), buf.slice(n + sz as usize..));

        // Parse series count
        if let Some((value, size)) = u64::decode_var(&buf) {
            (self.series.n, n) = (value, size);
        }
        buf = buf.slice(n..);

        // Parse data block size.
        if let Some((value, size)) = u64::decode_var(&buf) {
            (sz, n) = (value, size);
        }
        buf = buf.slice(n..);

        // Parse series data (original uvarint encoded or roaring bitmap).
        if self.flag & TAG_VALUE_SERIES_ID_SET_FLAG == 0 {
            (self.series.data, buf) = (buf.slice(..sz as usize), buf.slice(sz as usize..));
        } else {
            (self.series_id_set_data, buf) = (buf.slice(..), buf.slice(sz as usize..));
        }

        // Save length of elem.
        self.size = (start - buf.len()) as u64;
    }
}

impl TagValueElement for TagBlockValueElem {
    fn value(&self) -> &[u8] {
        &self.value
    }

    fn deleted(&self) -> bool {
        (self.flag & TAG_VALUE_TOMBSTONE_FLAG) != 0
    }

    // fn as_any(&self) -> &dyn Any {
    //     self
    // }
}

// TagBlockTrailerSize is the total size of the on-disk trailer.
const TAG_BLOCK_TRAILER_SIZE: usize = 8 + 8 + // value data offset/size
	8 + 8 + // key data offset/size
	8 + 8 + // hash index offset/size
	8 + // size
	2; // version

// TagBlockTrailer represents meta data at the end of a TagBlock.
#[derive(Default)]
pub struct TagBlockTrailer {
    version: i32, // Encoding version
    size: i64,    // Total size w/ trailer

    value_data: ValueData,
    key_data: KeyData,
    hash_index: HashIndex,
}

#[derive(Default)]
// Offset & size of value data section.
struct ValueData {
    offset: i64,
    size: i64,
}

#[derive(Default)]
// Offset & size of key data section.
struct KeyData {
    offset: i64,
    size: i64,
}

#[derive(Default)]
// Offset & size of hash map section.
struct HashIndex {
    offset: i64,
    size: i64,
}

impl TagBlockTrailer {
    /// Writes the trailer to w.
    pub fn write_to(&self, w: &mut impl Write) -> Result<i64> {
        let mut n = 0usize;

        // Write data info.
        w.write_u64::<BigEndian>(self.value_data.offset as u64)?;
        n += 8;
        w.write_u64::<BigEndian>(self.value_data.size as u64)?;
        n += 8;

        // Write key data info.
        w.write_u64::<BigEndian>(self.key_data.offset as u64)?;
        n += 8;
        w.write_u64::<BigEndian>(self.key_data.size as u64)?;
        n += 8;

        // Write hash index info.
        w.write_u64::<BigEndian>(self.hash_index.offset as u64)?;
        n += 8;
        w.write_u64::<BigEndian>(self.hash_index.size as u64)?;
        n += 8;

        // Write total size & encoding version.
        w.write_u64::<BigEndian>(self.size as u64)?;
        n += 8;
        w.write_u16::<BigEndian>(INDEX_FILE_VERSION)?;
        n += 2;

        Ok(n as i64)
    }
}

pub fn read_tag_block_trailer(data: &[u8]) -> Result<TagBlockTrailer> {
    let mut t = TagBlockTrailer {
        version: (&data[data.len() - 2..]).read_u16::<BigEndian>().unwrap() as i32, // Read version.
        ..Default::default()
    };

    ensure!(
        t.version == TAG_BLOCK_VERSION,
        UnsupportedTagBlockVersionSnafu
    );

    // Slicetrailer data.
    let mut buf = &data[(data.len() - TAG_BLOCK_TRAILER_SIZE)..];

    // Read data section info.
    (t.value_data.offset, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );
    (t.value_data.size, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );

    // Read key section info.
    (t.key_data.offset, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );
    (t.key_data.size, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );

    // Read hash section info.
    (t.hash_index.offset, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );
    (t.hash_index.size, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );

    // Read total size.
    t.size = (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64;

    Ok(t)
}

/// TagBlockEncoder encodes a tags to a TagBlock section.
pub struct TagBlockEncoder<'a, W: std::io::Write> {
    w: &'a mut BufWriter<W>,
    buf: Vec<u8>,

    // Track value offsets.
    offsets: rhh::HashMap<Vec<u8>, i64>,

    // Track bytes written, sections.
    n: i64,
    trailer: TagBlockTrailer,

    // Track tag keys.
    keys: Vec<TagKeyEncodeEntry>,
    prev_value: Vec<u8>,
}

impl<'a, W: Write> TagBlockEncoder<'a, W> {
    /// Returns a new TagBlockEncoder.
    pub fn new(w: &'a mut BufWriter<W>) -> TagBlockEncoder<'a, W> {
        TagBlockEncoder {
            w,
            buf: vec![],
            offsets: rhh::HashMap::<Vec<u8>, i64>::new(rhh::Options {
                capacity: 0i64,
                load_factor: LOAD_FACTOR,
            }),
            n: 0i64,
            trailer: TagBlockTrailer {
                version: TAG_BLOCK_VERSION,
                ..Default::default()
            },
            keys: vec![],
            prev_value: vec![],
        }
    }

    /// Returns the number of bytes written.
    pub fn n(&self) -> i64 {
        self.n
    }

    /// Writes a tag key to the underlying writer.
    pub fn encode_key(&mut self, key: &[u8], deleted: bool) -> Result<()> {
        // An initial empty byte must be written.
        self.ensure_header_written()?;

        // Verify key is lexicographically after previous key.
        if !self.keys.is_empty() {
            let prev = self.keys[self.keys.len() - 1].key.as_slice();
            match prev.cmp(key) {
                std::cmp::Ordering::Greater => {
                    return Err(Error::StringErr {
                        reason: format!("tag key out of order: prev={:?}, new={:?}", prev, key),
                    });
                }
                std::cmp::Ordering::Equal => {
                    return Err(Error::StringErr {
                        reason: format!("tag key already encoded: {:?}", key),
                    });
                }
                _ => {}
            }
        }

        // Flush values section for key.
        self.flush_value_hash_index()?;

        // Append key on to the end of the key list.
        let mut entry = TagKeyEncodeEntry {
            key: key.to_vec(),
            deleted,
            ..Default::default()
        };
        entry.data.offset = self.n;

        self.keys.push(entry);

        // Clear previous value.
        self.prev_value = vec![];

        Ok(())
    }

    /// Writes a tag value to the underlying writer.
    /// The tag key must be lexicographical sorted after the previous encoded tag key.
    pub fn encode_value(
        &mut self,
        value: &[u8],
        deleted: bool,
        ss: &series_set::SeriesIDSet,
    ) -> Result<()> {
        ensure!(
            !self.keys.is_empty(),
            StringErrSnafu {
                reason: "tag key must be encoded before encoding values".to_string(),
            }
        );
        ensure!(
            !value.is_empty(),
            StringErrSnafu {
                reason: "zero length tag value not allowed".to_string(),
            }
        );

        // Validate that keys are in-order.
        match self.prev_value.as_slice().cmp(value) {
            std::cmp::Ordering::Greater => {
                return Err(Error::StringErr {
                    reason: format!(
                        "tag value out of order: prev={:?}, new={:?}",
                        self.prev_value, value
                    ),
                });
            }
            std::cmp::Ordering::Equal => {
                return Err(Error::StringErr {
                    reason: "tag key must be encoded before encoding values".to_string(),
                });
            }
            _ => {}
        }

        // Save offset to hash map.
        self.offsets.put(value.to_vec(), self.n);

        // Write flag.
        let c = write_u8_to(self.w, encode_tag_value_flag(deleted)).context(TSISnafu)?;
        self.n += c as i64;

        // Write value.
        let c = write_u_vint_to(self.w, value.len() as u64).context(TSISnafu)?;
        self.n += c as i64;
        let c = write_to(self.w, value).context(TSISnafu)?;
        self.n += c as i64;

        // Build series data in buffer.
        self.buf.truncate(0);
        ss.write_to(&mut self.buf).context(SeriesSetSnafu)?;

        // Write series count.
        let c = write_u_vint_to(self.w, ss.cardinality()).context(TSISnafu)?;
        self.n += c as i64;

        // Write data size & buffer.
        let c = write_u_vint_to(self.w, self.buf.len() as u64).context(TSISnafu)?;
        self.n += c as i64;

        // Word align bitmap data.
        // if offset := (enc.n) % 8; offset != 0 {
        // 	if err := writeTo(enc.w, make([]byte, 8-offset), &enc.n); err != nil {
        // 		return err
        // 	}
        // }

        let c = write_to(self.w, self.buf.as_slice()).context(TSISnafu)?;
        self.n += c as i64;

        // Save previous value.
        self.prev_value = value.to_vec();

        Ok(())
    }

    /// Close flushes the trailer of the encoder to the writer.
    pub fn close(&mut self) -> Result<()> {
        // Flush last value set.
        self.ensure_header_written()?;
        self.flush_value_hash_index()?;

        // Save ending position of entire data block.
        self.trailer.value_data.size = self.n - self.trailer.value_data.offset;

        // Write key block to point to value blocks.
        self.encode_tag_key_block()?;

        // Compute total size w/ trailer.
        self.trailer.size = self.n + TAG_BLOCK_TRAILER_SIZE as i64;

        // Write trailer.
        let nn = self.trailer.write_to(self.w)?;
        self.n += nn;

        self.w.flush()?;

        Ok(())
    }

    /// Writes a single byte to offset the rest of the block.
    pub fn ensure_header_written(&mut self) -> Result<()> {
        if self.n > 0 {
            return Ok(());
        }
        let _ = self.w.write(&[0u8])?;

        self.n += 1;
        self.trailer.value_data.offset = self.n;

        Ok(())
    }

    /// Builds writes the hash map at the end of a value set.
    pub fn flush_value_hash_index(&mut self) -> Result<()> {
        // Ignore if no keys have been written.
        if self.keys.is_empty() {
            return Ok(());
        }
        let keys_len = self.keys.len();
        let key = &mut self.keys[keys_len - 1];

        // Save size of data section.
        key.data.size = self.n - key.data.offset;

        // Encode hash map length.
        key.hash_index.offset = self.n;
        match write_u64_to(&mut self.w, self.offsets.capacity() as u64) {
            Ok(n) => self.n += n as i64,
            Err(e) => {
                return Err(Error::StringErr {
                    reason: format!("encode hash map length failed: {:?}", e),
                });
            }
        }

        // Encode hash map offset entries.
        for i in 0i64..self.offsets.capacity() {
            let (_k, v) = self.offsets.elem(i);
            // let offset = i64::from_be_bytes(<[u8; 8]>::try_from(v.unwrap()).unwrap());
            let offset = match v {
                Some(v) => *v,
                None => 0,
            };

            match write_u64_to(&mut self.w, offset as u64) {
                Ok(n) => self.n += n as i64,
                Err(e) => {
                    return Err(Error::StringErr {
                        reason: format!("encode hash map offset entries failed: {:?}", e),
                    });
                }
            }
        }
        key.hash_index.size = self.n - key.hash_index.offset;

        // Clear offsets.
        self.offsets = rhh::HashMap::<Vec<u8>, i64>::new(rhh::Options {
            capacity: 0i64,
            load_factor: LOAD_FACTOR,
        });

        Ok(())
    }

    /// Encodes the keys section to the writer.
    pub fn encode_tag_key_block(&mut self) -> Result<()> {
        let mut offsets = rhh::HashMap::<Vec<u8>, i64>::new(rhh::Options {
            capacity: self.keys.len() as i64,
            load_factor: LOAD_FACTOR,
        });

        // Encode key list in sorted order.
        self.trailer.key_data.offset = self.n;
        for entry in self.keys.iter() {
            // Save current offset so we can use it in the hash index.
            offsets.put(entry.key.to_vec(), self.n); // TODO: avoid copy ?

            match write_u8_to(self.w, encode_tag_key_flag(entry.deleted)) {
                Ok(n) => self.n += n as i64,
                Err(e) => {
                    return Err(Error::StringErr {
                        reason: format!("save current offset failed: {:?}", e),
                    });
                }
            }

            // Write value data offset & size.
            match write_u64_to(&mut self.w, entry.data.offset as u64) {
                Ok(n) => self.n += n as i64,
                Err(e) => {
                    return Err(Error::StringErr {
                        reason: format!("write value data offset failed: {:?}", e),
                    });
                }
            }

            match write_u64_to(&mut self.w, entry.data.size as u64) {
                Ok(n) => self.n += n as i64,
                Err(e) => {
                    return Err(Error::StringErr {
                        reason: format!("write value data size failed: {:?}", e),
                    });
                }
            }

            // Write value hash index offset & size.
            match write_u64_to(&mut self.w, entry.hash_index.offset as u64) {
                Ok(n) => self.n += n as i64,
                Err(e) => {
                    return Err(Error::StringErr {
                        reason: format!("write hash data offset failed: {:?}", e),
                    });
                }
            }

            match write_u64_to(&mut self.w, entry.hash_index.size as u64) {
                Ok(n) => self.n += n as i64,
                Err(e) => {
                    return Err(Error::StringErr {
                        reason: format!("write hash data size failed: {:?}", e),
                    });
                }
            }

            // Write key length and data.
            match write_u_vint_to(self.w, entry.key.len() as u64) {
                Ok(n) => self.n += n as i64,
                Err(e) => {
                    return Err(Error::StringErr {
                        reason: format!("write key length failed: {:?}", e),
                    });
                }
            }

            match write_to(self.w, entry.key.as_slice()) {
                Ok(n) => self.n += n as i64,
                Err(e) => {
                    return Err(Error::StringErr {
                        reason: format!("write key data failed: {:?}", e),
                    });
                }
            }
        }
        self.trailer.key_data.size = self.n - self.trailer.key_data.offset;

        // Encode hash map length.
        self.trailer.hash_index.offset = self.n;
        match write_u64_to(&mut self.w, offsets.capacity() as u64) {
            Ok(n) => self.n += n as i64,
            Err(e) => {
                return Err(Error::StringErr {
                    reason: format!("encode hash map length failed: {:?}", e),
                });
            }
        }

        // Encode hash map offset entries.
        for i in 0i64..offsets.capacity() {
            let (_k, v) = offsets.elem(i);
            // let offset = i64::from_be_bytes(<[u8; 8]>::try_from(v.unwrap()).unwrap());
            let offset = match v {
                Some(v) => *v,
                None => 0,
            };
            // let offset = *v.unwrap();
            match write_u64_to(&mut self.w, offset as u64) {
                Ok(n) => self.n += n as i64,
                Err(e) => {
                    return Err(Error::StringErr {
                        reason: format!("encode hash map offset entries failed: {:?}", e),
                    });
                }
            }
        }
        self.trailer.hash_index.size = self.n - self.trailer.hash_index.offset;

        Ok(())
    }
}

#[derive(Default)]
struct TagKeyEncodeEntry {
    key: Vec<u8>,
    deleted: bool,

    // data struct {
    // 	offset int64
    // 	size   int64
    // }
    data: KeyData,
    hash_index: HashIndex,
}

fn encode_tag_key_flag(deleted: bool) -> u8 {
    let mut flag = 0u8;
    if deleted {
        flag |= TAG_KEY_TOMBSTONE_FLAG;
    }

    flag
}

fn encode_tag_value_flag(deleted: bool) -> u8 {
    let mut flag = TAG_VALUE_SERIES_ID_SET_FLAG;
    if deleted {
        flag |= TAG_VALUE_TOMBSTONE_FLAG;
    }
    flag
}

#[cfg(test)]
mod tests {
    use crate::index::tsi1::tag_block::*;

    // Ensure tag blocks can be written and opened.
    #[test]
    fn test_tag_block_writer() {
        // Write 3 series to writer.
        let mut bw = BufWriter::new(Vec::new());
        let enc_n: i64; // encoded byte size
        {
            let mut enc = TagBlockEncoder::new(&mut bw);

            enc.encode_key("host".as_bytes(), false).unwrap();
            enc.encode_value(
                "server0".as_bytes(),
                false,
                &series_set::SeriesIDSet::new(vec![1].as_slice()),
            )
            .unwrap();
            enc.encode_value(
                "server1".as_bytes(),
                false,
                &series_set::SeriesIDSet::new(vec![2].as_slice()),
            )
            .unwrap();
            enc.encode_value(
                "server2".as_bytes(),
                false,
                &series_set::SeriesIDSet::new(vec![3].as_slice()),
            )
            .unwrap();

            enc.encode_key("region".as_bytes(), false).unwrap();
            enc.encode_value(
                "us-east".as_bytes(),
                false,
                &series_set::SeriesIDSet::new(vec![1, 2].as_slice()),
            )
            .unwrap();
            enc.encode_value(
                "us-west".as_bytes(),
                false,
                &series_set::SeriesIDSet::new(vec![3].as_slice()),
            )
            .unwrap();

            // Flush encoder.
            enc.close().unwrap();
            enc_n = enc.n();
        }
        let written = bw.get_ref().len() as i64;
        assert_eq!(
            enc_n, written,
            "bytes written mismatch: {}, expected: {}",
            enc_n, written
        );

        // Unmarshal into a block.
        let mut blk = TagBlock::default();
        let inner_data = bw.into_inner().unwrap();
        blk.unmarshal_binary(&Bytes::from(inner_data)).unwrap();

        // Verify data.
        if let Some(val) = blk.tag_value_elem("region".as_bytes(), "us-east".as_bytes()) {
            if let Ok(ids) = val.series_ids() {
                assert_eq!(ids, vec![1, 2], "unexpected series ids: {:?}", ids);
            } else {
                panic!("unexpected error");
            }
        } else {
            panic!("expected element");
        }

        if let Some(val) = blk.tag_value_elem("region".as_bytes(), "us-west".as_bytes()) {
            if let Ok(ids) = val.series_ids() {
                assert_eq!(ids, vec![3], "unexpected series ids: {:?}", ids);
            } else {
                panic!("unexpected error");
            }
        } else {
            panic!("expected element");
        }

        if let Some(val) = blk.tag_value_elem("host".as_bytes(), "server0".as_bytes()) {
            if let Ok(ids) = val.series_ids() {
                assert_eq!(ids, vec![1], "unexpected series ids: {:?}", ids);
            } else {
                panic!("unexpected error");
            }
        } else {
            panic!("expected element");
        }

        if let Some(val) = blk.tag_value_elem("host".as_bytes(), "server1".as_bytes()) {
            if let Ok(ids) = val.series_ids() {
                assert_eq!(ids, vec![2], "unexpected series ids: {:?}", ids);
            } else {
                panic!("unexpected error");
            }
        } else {
            panic!("expected element");
        }

        if let Some(val) = blk.tag_value_elem("host".as_bytes(), "server2".as_bytes()) {
            if let Ok(ids) = val.series_ids() {
                assert_eq!(ids, vec![3], "unexpected series ids: {:?}", ids);
            } else {
                panic!("unexpected error");
            }
        } else {
            panic!("expected element");
        }
    }
}
