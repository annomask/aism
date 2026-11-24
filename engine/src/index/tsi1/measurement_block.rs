use std::{collections::HashMap, io::Write, sync::Arc};

use crate::common::{hll, rhh};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use integer_encoding::VarInt;
use snafu::{ResultExt, Snafu, ensure};

use crate::{
    index::{
        SeriesIDElem, SeriesIDIter, SeriesIDIterator, new_series_id_set_iter,
        tsi1::{
            LOAD_FACTOR, MeasurementElement, decode_var, write_to, write_u_vint_to, write_u8_to,
            write_u64_to,
        },
    },
    series::series_set,
};

// MeasurementBlockVersion is the version of the measurement block.
const MEASUREMENT_BLOCK_VERSION: i32 = 1;

// Measurement flag constants.
const MEASUREMENT_TOMBSTONE_FLAG: u8 = 0x01;
const MEASUREMENT_SERIES_ID_SET_FLAG: u8 = 0x02;

// Measurement field size constants.
// 1 byte offset for the block to ensure non-zero offsets.
const MEASUREMENT_FILL_SIZE: usize = 1;
// Measurement trailer fields.
const MEASUREMENT_TRAILER_SIZE: usize = 2 +  // version
    8 + 8 +  // data offset/size
    8 + 8 +  // hash index offset/size
    8 + 8 +  // measurement sketch offset/size
    8 + 8; // tombstone measurement sketch offset/size

// Measurement key block fields.
const MEASUREMENT_N_SIZE: usize = 8;
const MEASUREMENT_OFFSET_SIZE: usize = 8;
const SERIES_ID_SIZE: usize = 8;

/// Error type for TagBlock.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOError { reason: std::io::Error },

    /// UnsupportedMeasurementBlockVersion.
    #[snafu(display("unsupported measurement block version"))]
    UnsupportedMeasurementBlockVersion,

    /// MeasurementBlockSizeMismatch.
    #[snafu(display("measurement block size mismatch"))]
    MeasurementBlockSizeMismatch,

    /// Series set error.
    #[snafu(display("{}", source))]
    SeriesSet {
        source: crate::series::series_set::Error,
    },

    /// HLL error.
    #[snafu(display("{}", source))]
    HLL { source: crate::common::hll::Error },

    /// TSI error.
    #[snafu(display("{}", source))]
    TSI { source: crate::index::tsi1::Error },

    /// DecodeVarIntFailed.
    #[snafu(display("decode var integer failed"))]
    DecodeVarIntFailed,

    /// String error.
    #[snafu(display("tsi: {}", reason))]
    StringErr { reason: String },
}

/// Result type for TagBlock.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

/// MeasurementBlock represents a collection of all measurements in an index.
#[derive(Default, Debug)]
pub struct MeasurementBlock {
    data: Bytes,
    hash_data: Bytes,

    // Measurement sketch and tombstone sketch for cardinality estimation.
    sketch_data: Bytes,
    tsketch_data: Bytes,

    version: i32, // measurement block version
}

impl MeasurementBlock {
    /// bytes estimates the memory footprint of this MeasurementBlock, in bytes.
    pub fn bytes(&self) -> i32 {
        // Do not count contents of blk.data or blk.hashData because they reference into an external []byte
        let mut b = 0i32;
        b += self.sketch_data.len() as i32;
        b += self.tsketch_data.len() as i32;
        b += 4; // version size
        b
    }

    /// Returns the encoding version parsed from the data.
    /// Only valid after UnmarshalBinary() has been successfully invoked.
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Returns an element for a measurement.
    pub fn elem(&self, name: &[u8]) -> Option<MeasurementBlockElem> {
        let n = (&self.hash_data[..MEASUREMENT_N_SIZE])
            .read_u64::<BigEndian>()
            .unwrap() as i64;
        let hash = rhh::hash_key(name);
        let mut pos = hash % n;
        let mut d = 0i64;
        loop {
            // Find offset of measurement.
            let offset = (&self.hash_data
                [MEASUREMENT_N_SIZE + (pos as usize * MEASUREMENT_OFFSET_SIZE)..])
                .read_u64::<BigEndian>()
                .unwrap();
            if offset == 0 {
                return None;
            }

            // Evaluate name if offset is not empty.
            if offset > 0 {
                // Parse into element.
                let mut e = MeasurementBlockElem::default();
                e.unmarshal_binary(self.data.slice(offset as usize..)).ok();

                // Return if name match.
                if e.name.as_ref().cmp(name) == std::cmp::Ordering::Equal {
                    return Some(e);
                }

                // Check if we've exceeded the probe distance.
                if d > rhh::dist(rhh::hash_key(e.name.as_ref()), pos, n) {
                    return None;
                }
            }

            // Move position forward.
            pos = (pos + 1) % n;
            d += 1;

            if d > n {
                return None;
            }
        }
    }

    /// Unpacks data into the block. Block is not copied so data
    /// should be retained and unchanged after being passed into this function.
    pub fn unmarshal_binary(&mut self, data: Bytes) -> Result<()> {
        // Read trailer.
        let t = read_measurement_block_trailer(&data)?;

        // Save data section.
        self.data = data.slice(t.data.offset as usize..(t.data.offset + t.data.size) as usize);

        // Save hash index block.
        self.hash_data = data.slice(
            t.hash_index.offset as usize..(t.hash_index.offset + t.hash_index.size) as usize,
        );

        // Initialise sketch/tsketch data.
        self.sketch_data =
            data.slice(t.sketch.offset as usize..(t.sketch.offset + t.sketch.size) as usize);
        self.tsketch_data =
            data.slice(t.tsketch.offset as usize..(t.tsketch.offset + t.tsketch.size) as usize);

        Ok(())
    }

    /// Returns an iterator over all measurements.
    pub fn measurement_iterator(&self) -> BlockMeasurementIter {
        BlockMeasurementIter {
            // elem: MeasurementBlockElem::default(),
            data: self.data.slice(MEASUREMENT_FILL_SIZE..),
        }
    }

    /// Returns an iterator for all series ids in a measurement.
    pub fn series_id_iterator(&self, name: &[u8]) -> Option<SeriesIDIter> {
        // Find measurement element.
        if let Some(e) = self.elem(name) {
            if e.series_id_set.is_some() {
                return Some(SeriesIDIter::SeriesIDSetIter(new_series_id_set_iter(
                    e.series_id_set.unwrap(), // TODO: optimize me.
                )));
            } else {
                return Some(SeriesIDIter::MeasurementRawSeriesIDIter(RawSeriesIDIter {
                    prev: 0u64,
                    n: e.series.n,
                    data: e.series.data,
                }));
            }
        }
        None
    }

    /// Returns existence and tombstone measurement sketches.
    pub fn sketches(&self) -> Result<(hll::HLL, hll::HLL)> {
        let mut sketch = hll::HLL::default();
        sketch
            .unmarshal_binary(&self.sketch_data)
            .context(HLLSnafu)?;

        let mut tsketch = hll::HLL::default();
        tsketch
            .unmarshal_binary(&self.tsketch_data)
            .context(HLLSnafu)?;

        Ok((sketch, tsketch))
    }
}

/// BlockMeasurementIter iterates over a list measurements in a block.
#[derive(Clone)]
pub struct BlockMeasurementIter {
    // elem: MeasurementBlockElem<'a>,
    data: Bytes,
}

impl Iterator for BlockMeasurementIter {
    type Item = MeasurementBlockElem;

    fn next(&mut self) -> Option<Self::Item> {
        // Return nil when we run out of data.
        if self.data.is_empty() {
            return None;
        }

        // Unmarshal the element at the current position.
        let mut e = MeasurementBlockElem::default();
        e.unmarshal_binary(self.data.clone()).ok();

        // Move the data forward past the record.
        self.data = self.data.slice(e.size as usize..);

        Some(e)
    }
}

/// RawSeriesIDIter iterates over a list of raw series data.
pub struct RawSeriesIDIter {
    prev: u64,
    n: u64,
    data: Bytes,
}

impl Iterator for RawSeriesIDIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }

        if let Some((delta, n)) = decode_var(&self.data) {
            self.data = self.data.slice(n..);
            let series_id = self.prev + delta;
            self.prev = series_id;
            Some(SeriesIDElem { series_id })
        } else {
            None
        }
    }
}

impl SeriesIDIterator for RawSeriesIDIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl RawSeriesIDIter {
    pub fn series_id_set(&self) -> Arc<series_set::SeriesIDSet> {
        let ss: series_set::SeriesIDSet = series_set::SeriesIDSet::new(&[]);
        // Otherwise decode series ids from uvarint encoding.
        let mut prev = 0u64;
        let mut data = self.data.slice(..);
        while !data.is_empty() {
            if let Some((delta, n)) = decode_var(&data) {
                data = data.slice(n..);

                let series_id = prev + delta;
                prev = series_id;
                ss.add_no_lock(series_id);
            } else {
                break;
            }
        }
        Arc::new(ss)
    }
}

// impl<'a> SeriesIDIterator for RawSeriesIDIter<'a> {
//     fn close(&self) -> crate::index::Result<()> {
//         Ok(())
//     }
// }

// impl<'a> crate::index::SeriesIDSetIterator for RawSeriesIDIter<'a> {
//     fn series_id_set(&self) -> Arc<series_set::SeriesIDSet> {
//         let ss: series_set::SeriesIDSet = series_set::SeriesIDSet::new(&[]);
//         // Otherwise decode series ids from uvarint encoding.
//         let mut prev = 0u64;
//         let mut data = self.data;
//         while !data.is_empty() {
//             if let Some((delta, n)) = decode_var(data) {
//                 data = &data[n..];

//                 let series_id = prev + delta;
//                 prev = series_id;
//                 ss.add_no_lock(series_id);
//             } else {
//                 break;
//             }
//         }
//         Arc::new(ss)
//     }
// }

#[derive(Debug, Default)]
pub(crate) struct Data {
    offset: i64,
    size: i64,
}

#[derive(Debug, Default)]
pub(crate) struct HashIndex {
    offset: i64,
    size: i64,
}

#[derive(Debug, Default)]
pub(crate) struct Sketch {
    offset: i64,
    size: i64,
}

#[derive(Debug, Default)]
pub(crate) struct TSketch {
    offset: i64,
    size: i64,
}

// MeasurementBlockTrailer represents meta data at the end of a MeasurementBlock.
#[derive(Debug, Default)]
pub struct MeasurementBlockTrailer {
    version: i32, // Encoding version

    // Offset & size of data section.
    data: Data,

    // Offset & size of hash map section.
    hash_index: HashIndex,

    // Offset and size of cardinality sketch for measurements.
    sketch: Sketch,

    // Offset and size of cardinality sketch for tombstoned measurements.
    tsketch: TSketch,
}

impl MeasurementBlockTrailer {
    /// Writes the trailer to w.
    pub fn write_to(&self, w: &mut impl Write) -> Result<i64> {
        let mut n = 0usize;

        // Write data info.
        let _ = w.write_u64::<BigEndian>(self.data.offset as u64);
        n += 8;
        let _ = w.write_u64::<BigEndian>(self.data.size as u64);
        n += 8;

        // Write hash index info.
        let _ = w.write_u64::<BigEndian>(self.hash_index.offset as u64);
        n += 8;
        let _ = w.write_u64::<BigEndian>(self.hash_index.size as u64);
        n += 8;

        // Write measurement sketch info.
        let _ = w.write_u64::<BigEndian>(self.sketch.offset as u64);
        n += 8;
        let _ = w.write_u64::<BigEndian>(self.sketch.size as u64);
        n += 8;

        // Write measurement sketch info.
        let _ = w.write_u64::<BigEndian>(self.tsketch.offset as u64);
        n += 8;
        let _ = w.write_u64::<BigEndian>(self.tsketch.size as u64);
        n += 8;

        // Write measurement block version.
        let _ = w.write_u16::<BigEndian>(MEASUREMENT_BLOCK_VERSION as u16);
        n += 2;

        Ok(n as i64)
    }
}

// Returns the block trailer from data.
pub fn read_measurement_block_trailer(data: &[u8]) -> Result<MeasurementBlockTrailer> {
    let mut t = MeasurementBlockTrailer {
        version: (&data[data.len() - 2..]).read_u16::<BigEndian>().unwrap() as i32, // Read version.
        ..Default::default()
    };

    ensure!(
        t.version == MEASUREMENT_BLOCK_VERSION,
        UnsupportedMeasurementBlockVersionSnafu
    );

    // Slicetrailer data.
    let mut buf = &data[(data.len() - MEASUREMENT_TRAILER_SIZE)..];

    // Read data section info.
    (t.data.offset, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );
    (t.data.size, buf) = (
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

    // Read measurement sketch info.
    (t.sketch.offset, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );
    (t.sketch.size, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );

    // Read tomb measurement sketch info.
    (t.tsketch.offset, buf) = (
        (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64,
        &buf[8..],
    );
    t.tsketch.size = (&buf[0..8]).read_u64::<BigEndian>().unwrap() as i64;

    Ok(t)
}

#[derive(Default, Clone)]
pub(crate) struct TagBlock {
    pub(crate) offset: i64,
    pub(crate) size: i64,
}

#[derive(Default, Clone)]
pub(crate) struct Series {
    pub(crate) n: u64,
    pub(crate) data: Bytes,
}

// MeasurementBlockElem represents an internal measurement element.
#[derive(Default, Clone)]
pub struct MeasurementBlockElem {
    flag: u8,               // flag
    pub(crate) name: Bytes, // measurement name

    pub(crate) tag_block: TagBlock,
    pub(crate) series: Series,

    series_id_set: Option<series_set::SeriesIDSet>,

    // size in bytes, set after unmarshalling.
    size: i32,
}

impl MeasurementBlockElem {
    /// Returns the offset of the measurement's tag block.
    pub fn tag_block_offset(&self) -> i64 {
        self.tag_block.offset
    }

    /// Returns the size of the measurement's tag block.
    pub fn tag_block_size(&self) -> i64 {
        self.tag_block.size
    }

    /// Returns the raw series data.
    pub fn series_data(&self) -> &[u8] {
        &self.series.data
    }

    /// Returns the number of series associated with the measurement.
    pub fn series_n(&self) -> u64 {
        self.series.n
    }

    /// Returns series ID at an index.
    pub fn series_id(&self, i: i32) -> u64 {
        (&self.series.data[(i as usize) * SERIES_ID_SIZE..])
            .read_u64::<BigEndian>()
            .unwrap()
    }

    pub fn has_series(&self) -> bool {
        self.series.n > 0
    }

    /// Returns a list of decoded series ids.
    ///
    /// NOTE: This should be used for testing and diagnostics purposes only.
    /// It requires loading the entire list of series in-memory.
    pub fn series_ids(&self) -> Vec<u64> {
        let mut a = Vec::<u64>::with_capacity(self.series.n as usize);
        self.for_each_series_id::<_, Error>(|id: u64| -> Result<()> {
            a.push(id);
            Ok(())
        })
        .ok();

        a
    }

    pub fn for_each_series_id<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(u64) -> Result<(), E>,
    {
        // Read from roaring, if available.
        if self.series_id_set.is_some() {
            let itr = self.series_id_set.as_ref().unwrap().iterator_no_lock();
            for id in itr {
                f(id as u64)?;
            }
        }

        // Read from uvarint encoded data, if available.
        let mut prev = 0u64;
        let mut data = self.series.data.as_ref();
        while !data.is_empty() {
            if let Some((delta, n)) = decode_var(data) {
                data = &data[n..];

                let series_id = prev + delta;
                f(series_id)?;
                prev = series_id;
            } else {
                // TODO: 多个crate 调用for_each_series_id，且调用时 E 不同，如何在这里return error ？
                // return Err(crate::tsi::index_file::Error::DecodeVarIntFailed);
                // return Err(Error::DecodeVarIntFailed);
                // return Error(E::new());
                // FIXME: this should return decode var int failed error.
                return Ok(());
            }
        }

        Ok(())
    }

    /// Returns the size of the element.
    pub fn size(&self) -> i32 {
        self.size
    }

    /// Unmarshals data into e.
    pub fn unmarshal_binary(&mut self, data: Bytes) -> Result<()> {
        let start = data.len();
        let mut data = data;
        // Parse flag data.
        (self.flag, data) = (data[0], data.slice(1..));

        // Parse tag block offset.
        (self.tag_block.offset, data) = (
            (&data[..8]).read_u64::<BigEndian>().unwrap() as i64,
            data.slice(8..),
        );
        (self.tag_block.size, data) = (
            (&data[..8]).read_u64::<BigEndian>().unwrap() as i64,
            data.slice(8..),
        );

        let mut sz = 0u64;
        // Parse name.
        if let Some((c, n)) = u64::decode_var(&data) {
            sz = c;
            self.name = data.slice(n..n + (sz as usize));
            data = data.slice(n + (sz as usize)..);
        } else {
            return Err(Error::DecodeVarIntFailed);
        }

        // Parse series count.
        if let Some((v, n)) = u64::decode_var(&data) {
            self.series.n = v;
            data = data.slice(n..);
        } else {
            return Err(Error::DecodeVarIntFailed);
        }

        // Parse series data size.

        if let Some((c, n)) = u64::decode_var(&data) {
            sz = c;
            data = data.slice(n..);
        } else {
            return Err(Error::DecodeVarIntFailed);
        }

        // Parse series data (original uvarint encoded or roaring bitmap).
        if self.flag & MEASUREMENT_SERIES_ID_SET_FLAG == 0 {
            (self.series.data, data) = (data.slice(..sz as usize), data.slice(sz as usize..));
        } else {
            // data = memalign(data)
            let ss = series_set::SeriesIDSet::new(&[]);
            ss.unmarshal_binary_unsafe(&data[..sz as usize])
                .context(SeriesSetSnafu)?;

            self.series_id_set = Some(ss);
            data = data.slice(sz as usize..);
        }

        // Save length of elem.
        self.size = (start - data.len()) as i32;

        Ok(())
    }
}

impl MeasurementElement for MeasurementBlockElem {
    fn name(&self) -> Option<&[u8]> {
        Some(&self.name)
    }

    fn deleted(&self) -> bool {
        (self.flag & MEASUREMENT_TOMBSTONE_FLAG) != 0
    }
}

/// MeasurementBlockWriter writes a measurement block.
pub(crate) struct MeasurementBlockWriter {
    // w: &'a mut BufWriter<W>,
    buf: Vec<u8>,
    mms: HashMap<String, Measurement>,

    // Measurement sketch and tombstoned measurement sketch.
    sketch: Option<hll::HLL>,
    tsketch: Option<hll::HLL>,
}

impl MeasurementBlockWriter {
    /// Returns a new MeasurementBlockWriter.
    pub fn new(/*w: &'a mut BufWriter<W>*/) -> MeasurementBlockWriter {
        MeasurementBlockWriter {
            // w,
            buf: vec![],
            mms: HashMap::new(),
            sketch: Some(hll::HLL::default()),
            tsketch: Some(hll::HLL::default()),
        }
    }

    /// Adds a measurement with series and tag set offset/size.
    pub fn add(&mut self, name: &[u8], deleted: bool, offset: i64, size: i64, series_ids: &[u64]) {
        if let Some(mm) = self
            .mms
            .get_mut(unsafe { std::str::from_utf8_unchecked(name) })
        {
            mm.deleted = deleted;
            mm.tag_block.offset = offset;
            mm.tag_block.size = size;

            for id in series_ids {
                (*mm.series_id_set).as_ref().unwrap().add_no_lock(*id);
            }
        } else {
            let mut mm = Measurement {
                deleted,
                tag_block: TagBlock { offset, size },
                series_id_set: Arc::new(Some(series_set::SeriesIDSet::new(&[]))),
                offset: 0i64,
            };

            let ss = series_set::SeriesIDSet::new(&[]);
            for id in series_ids {
                ss.add_no_lock(*id);
            }

            mm.series_id_set = Arc::new(Some(ss));

            self.mms
                .insert(unsafe { String::from_utf8_unchecked(name.to_vec()) }, mm);
        }

        if deleted {
            self.tsketch.as_mut().unwrap().add(name);
        } else {
            self.sketch.as_mut().unwrap().add(name);
        }
    }

    /// Encodes the measurements to w.
    pub fn write_to(&mut self, w: &mut impl Write) -> Result<i64> {
        let mut t = MeasurementBlockTrailer::default();

        // The sketches must be set before calling write_to.
        ensure!(
            self.sketch.is_some(),
            StringErrSnafu {
                reason: "measurement sketch not set".to_string()
            }
        );
        ensure!(
            self.tsketch.is_some(),
            StringErrSnafu {
                reason: "measurement tombstone sketch not set".to_string()
            }
        );

        // Sort names.
        let mut names = Vec::<String>::with_capacity(self.mms.len());
        for name in self.mms.keys() {
            names.push(name.clone());
        }
        names.sort();

        let mut n = 0i64;

        // Begin data section.
        t.data.offset = n;

        // Write padding byte so no offsets are zero.
        let c = write_u8_to(w, 0).context(TSISnafu)?;
        n += c as i64;

        // Encode key list.
        for name in &names {
            // Retrieve measurement and save offset.
            self.mms.get_mut(name).unwrap().offset = n;

            // Write measurement
            self.write_measurement_to(w, name.as_bytes(), &mut n /* mm, */)?;
        }
        t.data.size = n - t.data.offset;

        // Build key hash map.
        let mut m = rhh::HashMap::<Vec<u8>, &Measurement>::new(rhh::Options {
            capacity: names.len() as i64,
            load_factor: LOAD_FACTOR,
        });
        for (name, measurement) in self.mms.iter() {
            m.put(name.as_bytes().to_vec(), measurement);
        }

        t.hash_index.offset = n;

        // Encode hash map length.
        let c = write_u64_to(w, m.capacity() as u64).context(TSISnafu)?;
        n += c as i64;

        // Encode hash map offset entries.
        for i in 0i64..m.capacity() {
            let (_k, v) = m.elem(i);
            // let offset = i64::from_be_bytes(<[u8; 8]>::try_from(v.unwrap()).unwrap());
            let offset = match v {
                Some(v) => v.offset,
                None => 0,
            };

            let c = write_u64_to(w, offset as u64).context(TSISnafu)?;
            n += c as i64;
        }
        t.hash_index.size = n - t.hash_index.offset;

        // Write the sketches out.
        t.sketch.offset = n;
        let c = write_sketch_to(w, self.sketch.as_ref().unwrap())?;
        n += c;
        t.sketch.size = n - t.sketch.offset;

        t.tsketch.offset = n;
        let c = write_sketch_to(w, self.tsketch.as_ref().unwrap())?;
        n += c;
        t.tsketch.size = n - t.tsketch.offset;

        // Write trailer.
        let nn = t.write_to(w)?;
        n += nn;

        Ok(n)
    }

    /// Encodes a single measurement entry into w.
    pub fn write_measurement_to(
        &mut self,
        w: &mut impl Write,
        name: &[u8], /* mm: &Measurement, */
        n: &mut i64,
    ) -> Result<()> {
        let mm = self
            .mms
            .get(unsafe { std::str::from_utf8_unchecked(name) })
            .unwrap();

        // Write flag & tag block offset.
        let c = write_u8_to(w, mm.flag()).context(TSISnafu)?;
        *n += c as i64;
        let c = write_u64_to(w, mm.tag_block.offset as u64).context(TSISnafu)?;
        *n += c as i64;
        let c = write_u64_to(w, mm.tag_block.size as u64).context(TSISnafu)?;
        *n += c as i64;

        // Write measurement name.
        let c = write_u_vint_to(w, name.len() as u64).context(TSISnafu)?;
        *n += c as i64;
        let c = write_to(w, name).context(TSISnafu)?;
        *n += c as i64;

        // Write series data to buffer.
        self.buf.truncate(0);
        (*mm.series_id_set)
            .as_ref()
            .unwrap()
            .write_to(&mut self.buf)
            .context(SeriesSetSnafu)?;

        // Write series count.
        let c = write_u_vint_to(w, (*mm.series_id_set).as_ref().unwrap().cardinality())
            .context(TSISnafu)?;
        *n += c as i64;

        // Write data size & buffer.
        let c = write_u_vint_to(w, self.buf.len() as u64).context(TSISnafu)?;
        *n += c as i64;

        // Word align bitmap data.
        // if offset := (*n) % 8; offset != 0 {
        // 	if err := writeTo(w, make([]byte, 8-offset), n); err != nil {
        // 		return err
        // 	}
        // }

        let c = write_to(w, self.buf.as_slice()).context(TSISnafu)?;
        *n += c as i64;

        Ok(())
    }
}

fn write_sketch_to(w: &mut impl Write, h: &hll::HLL) -> Result<i64> {
    let data = h.marshal_binary().context(HLLSnafu)?;
    let nn = w.write(data.as_slice())?;
    Ok(nn as i64)
}

#[derive(Default)]
struct Measurement {
    deleted: bool,
    tag_block: TagBlock,
    series_id_set: Arc<Option<series_set::SeriesIDSet>>,
    offset: i64,
}

impl Measurement {
    pub fn flag(&self) -> u8 {
        let mut flag = MEASUREMENT_SERIES_ID_SET_FLAG;
        if self.deleted {
            flag |= MEASUREMENT_TOMBSTONE_FLAG;
        }
        flag
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufWriter, Write};

    use bytes::BufMut;

    use super::*;

    #[test]
    fn test_read_measurement_block_trailer() {
        // Build a trailer
        let mut data = vec![0u8; MEASUREMENT_TRAILER_SIZE];
        let block_version = 1u16;
        let (block_offset, block_size) = (1u64, 2500u64);
        let (hash_idx_offset, hash_idx_size) = (2501u64, 1000u64);
        let (sketch_offset, sketch_size) = (3501u64, 250u64);
        let (tsketch_offset, tsketch_size) = (3751u64, 250u64);

        let _ = data.write_u64::<BigEndian>(block_offset);
        let _ = data.write_u64::<BigEndian>(block_size);
        let _ = data.write_u64::<BigEndian>(hash_idx_offset);
        let _ = data.write_u64::<BigEndian>(hash_idx_size);
        let _ = data.write_u64::<BigEndian>(sketch_offset);
        let _ = data.write_u64::<BigEndian>(sketch_size);
        let _ = data.write_u64::<BigEndian>(tsketch_offset);
        let _ = data.write_u64::<BigEndian>(tsketch_size);
        let _ = data.write_u16::<BigEndian>(block_version);

        let trailer = read_measurement_block_trailer(&data).unwrap();

        let ok = trailer.version == block_version as i32
            && trailer.data.offset == block_offset as i64
            && trailer.data.size == block_size as i64
            && trailer.hash_index.offset == hash_idx_offset as i64
            && trailer.hash_index.size == hash_idx_size as i64
            && trailer.sketch.offset == sketch_offset as i64
            && trailer.sketch.size == sketch_size as i64
            && trailer.tsketch.offset == tsketch_offset as i64
            && trailer.tsketch.size == tsketch_size as i64;

        assert!(
            ok,
            "got {:?}
which does not match expected",
            trailer
        );
    }

    #[test]
    fn test_measurement_block_trailer_write_to() {
        let mut trailer = MeasurementBlockTrailer {
            version: 1,
            data: Data { offset: 1, size: 2 },
            hash_index: HashIndex { offset: 3, size: 4 },
            sketch: Sketch { offset: 5, size: 6 },
            tsketch: TSketch { offset: 7, size: 8 },
        };

        let mut bw = BufWriter::new(Vec::new());
        let n = trailer.write_to(&mut bw).unwrap();
        bw.flush().unwrap();

        assert_eq!(
            n, MEASUREMENT_TRAILER_SIZE as i64,
            "got: {}, expect: {}",
            n, MEASUREMENT_TRAILER_SIZE as i64
        );

        // Verify trailer written correctly.
        let mut exp = Vec::new();
        for value in 1u64..=8 {
            exp.extend_from_slice(&value.to_be_bytes());
        }
        exp.extend_from_slice(&1u16.to_be_bytes()); // version, as u16

        let got = bw.get_ref().as_slice();
        assert_eq!(got, exp.as_slice(), "got {:?}, exp: {:?}", got, exp);
    }

    // Ensure measurement blocks can be written and opened.
    #[test]
    fn test_measurement_block_writer() {
        let ms = vec![
            Measurement {
                name: "foo".as_bytes().to_vec(),
                deleted: false,
                offset: 100,
                size: 10,
                ids: vec![1, 3, 4],
            },
            Measurement {
                name: "bar".as_bytes().to_vec(),
                deleted: false,
                offset: 200,
                size: 20,
                ids: vec![2],
            },
            Measurement {
                name: "baz".as_bytes().to_vec(),
                deleted: false,
                offset: 300,
                size: 30,
                ids: vec![5, 6],
            },
        ];

        // Write the measurements to writer.
        let mut mw = MeasurementBlockWriter::new();
        for m in ms {
            mw.add(
                m.name.as_slice(),
                m.deleted,
                m.offset,
                m.size,
                m.ids.as_slice(),
            )
        }

        // Encode into buffer.
        let mut bw = BufWriter::new(Vec::new());
        let n = mw.write_to(&mut bw).unwrap();
        bw.flush().unwrap();
        assert_ne!(n, 0, "expected bytes written");

        // Unmarshal into a block.
        let mut blk = MeasurementBlock::default();
        let inner_data = bw.into_inner().unwrap();
        blk.unmarshal_binary(Bytes::from(inner_data)).unwrap();

        // Verify data in block.
        if let Some(e) = blk.elem("foo".as_bytes()) {
            assert!(
                !(e.tag_block_offset() != 100 || e.tag_block_size() != 10),
                "unexpected offset/size: {}/{}",
                e.tag_block_offset(),
                e.tag_block_size()
            );
            assert_eq!(
                e.series_ids(),
                vec![1, 3, 4],
                "unexpected series data: {:?}",
                e.series_ids()
            );
        } else {
            panic!("expected element");
        }

        if let Some(e) = blk.elem("bar".as_bytes()) {
            assert!(
                !(e.tag_block_offset() != 200 || e.tag_block_size() != 20),
                "unexpected offset/size: {}/{}",
                e.tag_block_offset(),
                e.tag_block_size()
            );
            assert_eq!(
                e.series_ids(),
                vec![2],
                "unexpected series data: {:?}",
                e.series_ids()
            );
        } else {
            panic!("expected element");
        }

        if let Some(e) = blk.elem("baz".as_bytes()) {
            assert!(
                !(e.tag_block_offset() != 300 || e.tag_block_size() != 30),
                "unexpected offset/size: {}/{}",
                e.tag_block_offset(),
                e.tag_block_size()
            );
            assert_eq!(
                e.series_ids(),
                vec![5, 6],
                "unexpected series data: {:?}",
                e.series_ids()
            );
        } else {
            panic!("expected element");
        }

        // Verify non-existent measurement doesn't exist.
        assert!(
            blk.elem("BAD_MEASUREMENT".as_bytes()).is_none(),
            "expected no element"
        );
    }

    struct Measurement {
        name: Vec<u8>,
        deleted: bool,
        offset: i64,
        size: i64,
        ids: Vec<u64>,
    }

    struct Measurements(Vec<Measurement>);
}
