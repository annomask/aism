use core::slice;
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use crate::common::{
    hll,
    mmap::{self, Mmap},
    to_str, to_string,
    wait_group::WaitGroup,
};
use bytes::{Buf, Bytes};
use snafu::{ResultExt, Snafu, ensure};

use super::{
    measurement_block::{BlockMeasurementIter, MeasurementBlockElem},
    partition::parse_file_name,
    tag_block::{TagBlock, TagBlockKeyElem, TagBlockKeyIter, TagBlockValueElem, TagBlockValueIter},
};
use crate::index::{
    SeriesIDIter, new_series_id_set_iter,
    tsi1::{self, TagValueElement, write_u16_to, write_u64_to},
};

// IndexFileVersion is the current TSI1 index file version.
pub(crate) const INDEX_FILE_VERSION: u16 = 1;

// FileSignature represents a magic number at the header of the index file.
pub(crate) const FILE_SIGNATURE: &str = "TSI1";

// IndexFile trailer fields
pub(crate) const INDEX_FILE_VERSION_SIZE: u8 = 2;

// IndexFileTrailerSize is the size of the trailer. Currently 82 bytes.
pub(crate) const INDEX_FILE_TRAILER_SIZE: u8 = INDEX_FILE_VERSION_SIZE +
		8 + 8 + // measurement block offset + size
		8 + 8 + // series id set offset + size
		8 + 8 + // tombstone series id set offset + size
		8 + 8 + // series sketch offset + size
		8 + 8;

/// Error type for IndexFile.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOErr { reason: std::io::Error },

    /// ShortBuffer
    #[snafu(display("buffer too short"))]
    ShortBuffer,

    /// InvalidIndexFile.
    #[snafu(display("invalid index file"))]
    InvalidIndexFile,

    /// UnsupportedIndexFileVersion.
    #[snafu(display("unsupported index file version"))]
    UnsupportedIndexFileVersion,

    /// Measurement block error.
    #[snafu(display("{}", source))]
    MeasurementBlock {
        source: crate::index::tsi1::measurement_block::Error,
    },

    /// Tag block error.
    #[snafu(display("{}", source))]
    TagBlock {
        source: crate::index::tsi1::tag_block::Error,
    },

    /// Series id set error.
    #[snafu(display("{}", source))]
    SeriesIDSet {
        source: crate::series::series_set::Error,
    },

    /// HLL error.
    #[snafu(display("{}", source))]
    HLL { source: crate::common::hll::Error },

    /// String error.
    #[snafu(display("tsi: {}", reason))]
    StringErr { reason: String },

    /// DecodeVarIntFailed.
    #[snafu(display("decode var integer failed"))]
    DecodeVarIntFailed,

    /// TSI error.
    #[snafu(display("{}", source))]
    TSI { source: crate::index::tsi1::Error },
}

/// Result type for IndexFile.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOErr { reason: err }
    }
}

unsafe impl Sync for IndexFile {}
unsafe impl Send for IndexFile {}
// IndexFile represents a collection of measurement, tag, and series data.
pub struct IndexFile {
    wg: WaitGroup,      // ref count
    mmap: Option<Mmap>, // memoryu maped index file
    data: *const u8,    // reference to mmaped data
    len: usize,         // length of  mmaped data

    // Components
    sfile: Option<Arc<crate::series::series_file::SeriesFile>>,
    tblks: HashMap<String, super::tag_block::TagBlock>,
    mblk: super::measurement_block::MeasurementBlock,

    // Raw series set data.
    series_id_set_data: Bytes,
    tombstone_series_id_set_data: Bytes,

    // Series sketch data.
    sketch_data: Bytes,
    tsketch_data: Bytes,

    // Sortable identifier & filepath to the log file.
    level: i64,
    id: i64,

    // // Compacting tracking.
    // compacting: std::sync::RwLock<bool>,

    // Path to data file.
    path: PathBuf,
}

impl IndexFile {
    /// Returns a new instance of IndexFile.
    pub fn new(sfile: Arc<crate::series::series_file::SeriesFile>) -> Self {
        Self {
            wg: WaitGroup::new(),
            sfile: Some(sfile),
            ..Default::default()
        }
    }

    // // FIXME: this meathod is incorrect
    // // Estimates the memory footprint of this IndexFile, in bytes.
    // pub fn bytes(&self) -> i32 {
    //     let mut b = 0;
    //     // FIXME:
    //     // let wg = self.wg.add(1);
    //     b += 8; // wg WaitGroup is 8 bytes
    //     b += std::mem::size_of_val(&self.mmap);
    //     // Do not count f.data contents because it is mmap'd
    //     b += std::mem::size_of_val(&self.sfile);
    //     // Do not count SeriesFile because it belongs to the code that constructed this IndexFile.
    //     b += std::mem::size_of_val(&self.tblks);
    //     for (k, v) in self.tblks.iter() {
    //         // Do not count TagBlock contents, they all reference f.data
    //         b += std::mem::size_of_val(k) + k.len();
    //         b += std::mem::size_of_val(v);
    //     }
    //     b += std::mem::size_of_val(&self.mblk) + (self.mblk.bytes() as usize);
    //     b += std::mem::size_of_val(&self.series_id_set_data)
    //         + std::mem::size_of_val(&self.tombstone_series_id_set_data);
    //     // Do not count contents of series_id_set_data or tombstone_series_id_set_Data: references f.data
    //     b += std::mem::size_of_val(&self.level) + std::mem::size_of_val(&self.id);
    //     // b += 24;  // mu RwMutex is 24 bytes
    //     // b += std::mem::size_of_val(&self.compacting);
    //     b += std::mem::size_of_val(&self.path) + self.path.as_path().to_str().unwrap().len();

    //     // FIXME:
    //     // self.wg.done();

    //     b as i32
    // }

    /// Open memory maps the data file at the file's path.
    pub fn open(&mut self) -> Result<()> {
        // defer func() {
        //     if err := recover(); err != nil {
        //         err = fmt.Errorf("[Index file: %s] %v", f.path, err)
        //         panic(err)
        //     }
        // }()

        // Extract identifier from path name.
        (self.id, self.level) = parse_file_name(self.path().as_os_str().to_str().unwrap());

        // open file
        match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&*self.path)
        {
            Ok(file) => {
                let mmap = unsafe {
                    mmap::MmapOptions::new()
                        // .len(stat.size() as usize)
                        .map(&file)
                        .expect("read-only memory map failed")
                };
                self.mmap = Some(mmap);
                self.data = self.mmap.as_ref().unwrap().as_ptr();
                self.len = self.mmap.as_ref().unwrap().len();
            }
            Err(e) => {
                let _ = self.close();
                return IOErrSnafu { reason: e }.fail();
            }
        }

        self.unmarshal_binary(None)
    }

    // Close unmaps the data file.
    pub fn close(&mut self) -> Result<()> {
        // Wait until all references are released.
        self.wg.wait();

        self.sfile = None;
        // drop(self.tblks);
        self.tblks = HashMap::<String, super::tag_block::TagBlock>::default();
        self.mblk = super::measurement_block::MeasurementBlock::default();
        self.mmap = None;

        Ok(())
    }

    /// Returns the file sequence identifier.
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Returns the file path.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Sets the file's path.
    pub fn set_path(&mut self, path: &Path) {
        self.path = path.into();
    }

    /// Returns the compaction level for the file.
    pub fn level(&self) -> i64 {
        self.level
    }

    #[cfg(test)]
    pub(crate) fn set_level_for_test(&mut self, level: i64) {
        self.level = level;
    }

    /// Adds a reference count to the file.
    pub fn retain(&self) {
        self.wg.add(1);
    }

    /// Removes a reference count from the file.
    pub fn release(&self) {
        self.wg.done();
    }

    /// Returns the size of the index file, in bytes.
    pub fn size(&self) -> i64 {
        self.mmap.as_ref().unwrap().len() as i64
    }

    /// Opens an index from data.
    /// The byte slice is retained so it must be kept open.
    pub fn unmarshal_binary(&mut self, data: Option<Bytes>) -> Result<()> {
        // let data = if let Some(d) = data {
        //     d
        // } else {
        //     self.mmap.as_ref().unwrap().as_ref()
        // };

        let data: Bytes = if let Some(d) = data {
            d
        } else {
            unsafe { Bytes::from(slice::from_raw_parts(self.data, self.len)) }
        };

        // Ensure magic number exists at the beginning.
        if data.len() < FILE_SIGNATURE.len() {
            return Err(Error::ShortBuffer);
        }
        ensure!(
            &data[..FILE_SIGNATURE.len()] == FILE_SIGNATURE.as_bytes(),
            InvalidIndexFileSnafu
        );

        // Read index file trailer.
        let t = read_index_file_trailer(&data)?;
        // Slice series sketch data.
        self.sketch_data = data.slice(
            t.series_sketch.offset as usize
                ..t.series_sketch.offset as usize + t.series_sketch.size as usize,
        );
        self.tsketch_data = data.slice(
            t.tombstone_series_sketch.offset as usize
                ..t.tombstone_series_sketch.offset as usize
                    + t.tombstone_series_sketch.size as usize,
        );

        // Slice series set data.
        self.series_id_set_data = data.slice(
            t.series_id_set.offset as usize
                ..t.series_id_set.offset as usize + t.series_id_set.size as usize,
        );
        self.tombstone_series_id_set_data = data.slice(
            t.tombstone_series_id_set.offset as usize
                ..t.tombstone_series_id_set.offset as usize
                    + t.tombstone_series_id_set.size as usize,
        );

        // Unmarshal measurement block.
        self.mblk
            .unmarshal_binary(data.slice(
                t.measurement_block.offset as usize
                    ..t.measurement_block.offset as usize + t.measurement_block.size as usize,
            ))
            .context(MeasurementBlockSnafu)?;

        // Unmarshal each tag block.
        self.tblks = HashMap::<String, TagBlock>::default();
        let itr = self.mblk.measurement_iterator();
        for m in itr {
            // Slice measurement block data.
            let buf = data.slice(
                m.tag_block.offset as usize..(m.tag_block.offset + m.tag_block.size) as usize,
            );

            // Unmarshal measurement block.
            let mut tblk = TagBlock::default();
            tblk.unmarshal_binary(&buf).context(TagBlockSnafu)?;
            self.tblks.insert(to_string!(&m.name), tblk);
        }

        Ok(())
    }

    pub fn series_id_set(&self) -> Result<crate::series::series_set::SeriesIDSet> {
        let ss = crate::series::series_set::SeriesIDSet::new(&[]);
        ss.unmarshal_binary(&self.series_id_set_data)
            .context(SeriesIDSetSnafu)?;
        Ok(ss)
    }

    pub fn tombstone_series_id_set(&self) -> Result<crate::series::series_set::SeriesIDSet> {
        let ss = crate::series::series_set::SeriesIDSet::new(&[]);
        ss.unmarshal_binary_unsafe(&self.tombstone_series_id_set_data)
            .context(SeriesIDSetSnafu)?;
        Ok(ss)
    }

    /// Returns a measurement element.
    pub fn measurement(&self, name: &[u8]) -> Option<MeasurementBlockElem> {
        self.mblk.elem(name)
    }

    /// Returns the number of measurements in the file.
    pub fn measurement_n(&self) -> u64 {
        // let mut n = 0;
        // let mitr = self.mblk.measurement_iterator();
        // for _bme in mitr {
        //     n += 1;
        // }
        // n
        self.mblk.measurement_iterator().count() as u64
    }

    /// Returns true if a measurement has any non-tombstoned series.
    pub fn measurement_has_series(
        &self,
        ss: &crate::series::series_set::SeriesIDSet,
        name: &[u8],
    ) -> bool {
        if let Some(e) = self.mblk.elem(name) {
            let mut exists = false;
            e.for_each_series_id::<_, Error>(|id: u64| -> Result<()> {
                if ss.contains(id) {
                    exists = true;
                    return Err(Error::StringErr {
                        reason: "done".to_string(),
                    });
                }
                Ok(())
            })
            .ok();

            return exists;
        }
        false
    }

    /// Returns a value iterator for a tag key and a flag indicating
    /// if a tombstone exists on the measurement or key.
    pub fn tag_value_iterator(&self, name: &[u8], key: &[u8]) -> Option<TagBlockValueIter> {
        if let Some(tblk) = self.tblks.get(to_str!(name)) {
            // Find key element.
            if let Some(ke) = tblk.tag_key_elem(key) {
                // Merge all value series iterators together.
                return Some(ke.tag_value_iterator());
            }
            return None;
        }
        None
    }

    /// Returns a series iterator for a tag key and a flag indicating
    /// if a tombstone exists on the measurement or key.
    pub fn tag_key_series_id_iterator(
        &self,
        name: &[u8],
        key: &[u8],
    ) -> Result<Option<SeriesIDIter>> {
        if let Some(tblk) = self.tblks.get(to_str!(&name)) {
            // Find key element.
            if let Some(ke) = tblk.tag_key_elem(key) {
                // Merge all value series iterators together.
                let vitr = ke.tag_value_iterator();
                let mut itrs = Vec::new();
                for ve in vitr {
                    let ss = ve.series_id_set().context(TagBlockSnafu)?;
                    itrs.push(SeriesIDIter::SeriesIDSetIter(new_series_id_set_iter(ss)));
                }
                return Ok(crate::index::merge_series_id_iterators(itrs));
            }
            return Ok(None);
        }
        Ok(None)
    }

    /// Returns a series id set for a tag value.
    pub fn tag_value_series_id_set(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<crate::series::series_set::SeriesIDSet>> {
        if let Some(tblk) = self.tblks.get(to_str!(&name)) {
            // Find value element.
            let mut value_elem = TagBlockValueElem::default();
            if !tblk.decode_tag_value_elem(key, value, &mut value_elem)
                || value_elem.series_n() == 0
            {
                return Ok(None);
            }
            // else if value_elem.series_n() == 0 {
            //     return Ok(None);
            // }

            let ss = value_elem.series_id_set().context(TagBlockSnafu)?;
            return Ok(Some(ss));
        }

        Ok(None)
    }

    /// Returns a tag key.
    pub fn tag_key(&self, name: &[u8], key: &[u8]) -> Option<TagBlockKeyElem> {
        if let Some(tblk) = self.tblks.get(to_str!(&name)) {
            return tblk.tag_key_elem(key);
        }
        None
    }

    /// Returns a tag value.
    pub fn tag_value(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Option<impl TagValueElement + '_ + use<'_>> {
        if let Some(tblk) = self.tblks.get(to_str!(&name)) {
            return tblk.tag_value_elem(key, value);
        }
        None
    }

    /// Returns flags indicating if the series exists and if it is tombstoned.
    /// Used for tests.
    pub fn has_series(&self, name: &[u8], tags: &crate::models::points::Tags) -> (bool, bool) {
        (self.sfile.as_ref().unwrap().has_series(name, tags), false)
    }

    /// Returns an element for a measurement/tag/value.
    pub fn tag_value_elem(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Option<TagBlockValueElem> {
        if let Some(tblk) = self.tblks.get(to_str!(&name)) {
            return tblk.tag_value_elem(key, value);
        }
        None
    }

    pub fn measurement_iterator(&self) -> BlockMeasurementIter {
        self.mblk.measurement_iterator()
    }

    /// Returns an iterator over all tag keys for a measurement.
    pub fn tag_key_iterator(&self, name: &[u8]) -> Option<TagBlockKeyIter> {
        if let Some(tblk) = self.tblks.get(to_str!(&name)) {
            return Some(tblk.tag_key_iterator());
        }
        None
    }

    /// Returns an iterator over a measurement's series.
    pub fn measurement_series_id_iterator(&self, name: &[u8]) -> Option<SeriesIDIter> {
        self.mblk.series_id_iterator(name)
    }

    /// Returns existence and tombstone sketches for measurements.
    pub fn measurement_sketches(&self) -> Result<(hll::HLL, hll::HLL)> {
        self.mblk.sketches().context(MeasurementBlockSnafu)
    }

    /// Returns existence and tombstone sketches for series.
    pub fn series_sketches(&self) -> Result<(hll::HLL, hll::HLL)> {
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

impl Default for IndexFile {
    fn default() -> Self {
        Self {
            wg: WaitGroup::new(),
            mmap: None,
            data: std::ptr::null(),
            len: 0, // length of  mmaped data

            // Components
            sfile: None,
            tblks: HashMap::default(),
            mblk: super::measurement_block::MeasurementBlock::default(),

            // Raw series set data.
            series_id_set_data: Bytes::new(),
            tombstone_series_id_set_data: Bytes::new(),

            // Series sketch data.
            sketch_data: Bytes::new(),
            tsketch_data: Bytes::new(),

            // Sortable identifier & filepath to the log file.
            level: 0,
            id: 0,

            // // Compacting tracking.
            // compacting: std::sync::RwLock<bool>,

            // Path to data file.
            path: PathBuf::default(),
        }
    }
}

impl PartialEq for IndexFile {
    fn eq(&self, other: &Self) -> bool {
        self.mmap == other.mmap
            && self.level == other.level
            && self.id == other.id
            && self.path == other.path
    }
}

/// Returns the index file trailer from data.
fn read_index_file_trailer(data: &[u8]) -> Result<IndexFileTrailer> {
    let mut t = IndexFileTrailer {
        // Read version.
        version: (&data[data.len() - INDEX_FILE_VERSION_SIZE as usize..]).get_u16() as i32,
        ..Default::default()
    };

    ensure!(
        t.version == INDEX_FILE_VERSION as i32,
        UnsupportedIndexFileVersionSnafu
    );

    // Slice trailer data.
    let mut buf = &data[data.len() - INDEX_FILE_TRAILER_SIZE as usize..];

    // Read measurement block info.
    (t.measurement_block.offset, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);
    (t.measurement_block.size, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);

    // Read series id set info.
    (t.series_id_set.offset, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);
    (t.series_id_set.size, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);

    // Read series tombstone id set info.
    (t.tombstone_series_id_set.offset, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);
    (t.tombstone_series_id_set.size, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);

    // Read series sketch set info.
    (t.series_sketch.offset, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);
    (t.series_sketch.size, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);

    // Read series tombstone sketch info.
    (t.tombstone_series_sketch.offset, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);
    (t.tombstone_series_sketch.size, buf) = ((&buf[0..8]).get_u64() as i64, &buf[8..]);

    ensure!(
        buf.len() == 2,
        StringErrSnafu {
            reason: format!("unread {} bytes left unread in trailer", buf.len() - 2),
        }
    ); // version field still in buffer.

    Ok(t)
}

// IndexFileTrailer represents meta data written to the end of the index file.
#[derive(Default)]
pub(crate) struct IndexFileTrailer {
    pub(crate) version: i32,
    pub(crate) measurement_block: MeasurementBlock,
    pub(crate) series_id_set: SeriesIDSet,
    pub(crate) tombstone_series_id_set: TombstoneSeriesIDSet,
    pub(crate) series_sketch: SeriesSketch,
    pub(crate) tombstone_series_sketch: TombstoneSeriesSketch,
}

impl IndexFileTrailer {
    /// Writes the trailer to w.
    pub fn write_to(&self, w: &mut impl Write) -> Result<usize> {
        let mut n = 0usize;
        // Write measurement block info.
        let c = write_u64_to(w, self.measurement_block.offset as u64).context(TSISnafu)?;
        n += c;
        let c = write_u64_to(w, self.measurement_block.size as u64).context(TSISnafu)?;
        n += c;

        // Write series id set info.
        let c = write_u64_to(w, self.series_id_set.offset as u64).context(TSISnafu)?;
        n += c;
        let c = write_u64_to(w, self.series_id_set.size as u64).context(TSISnafu)?;
        n += c;
        // Write tombstone series id set info.
        let c = write_u64_to(w, self.tombstone_series_id_set.offset as u64).context(TSISnafu)?;
        n += c;
        let c = write_u64_to(w, self.tombstone_series_id_set.size as u64).context(TSISnafu)?;
        n += c;

        // Write series sketch info.
        let c = write_u64_to(w, self.series_sketch.offset as u64).context(TSISnafu)?;
        n += c;
        let c = write_u64_to(w, self.series_sketch.size as u64).context(TSISnafu)?;
        n += c;

        // Write series tombstone sketch info.
        let c = write_u64_to(w, self.tombstone_series_sketch.offset as u64).context(TSISnafu)?;
        n += c;
        let c = write_u64_to(w, self.tombstone_series_sketch.size as u64).context(TSISnafu)?;
        n += c;

        // Write index file encoding version.
        let c = write_u16_to(w, INDEX_FILE_VERSION).context(TSISnafu)?;
        n += c;

        Ok(n)
    }
}

#[derive(Default)]
pub(crate) struct MeasurementBlock {
    pub(crate) offset: i64,
    pub(crate) size: i64,
}

#[derive(Default)]
pub(crate) struct SeriesIDSet {
    pub(crate) offset: i64,
    pub(crate) size: i64,
}

#[derive(Default)]
pub(crate) struct TombstoneSeriesIDSet {
    pub(crate) offset: i64,
    pub(crate) size: i64,
}

#[derive(Default)]
pub(crate) struct SeriesSketch {
    pub(crate) offset: i64,
    pub(crate) size: i64,
}

#[derive(Default)]
pub(crate) struct TombstoneSeriesSketch {
    pub(crate) offset: i64,
    pub(crate) size: i64,
}

// Generates an index filename for the given index.
pub(crate) fn formant_index_file_name(id: i64, level: i64) -> String {
    format!("L{}-{:08}.{}", level, id, tsi1::partition::INDEX_FILE_EXT) // .tsi
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    // Bloom filter settings used in tests.
    pub const M: u64 = 4096;
    pub const K: u64 = 6;

    use crate::common::to_string;

    #[test]
    fn test_index_file_create_index_file() {
        let mut sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        // let mut buf = vec![];
        let f = create_index_file(
            &sfile,
            vec![
                Series {
                    name: "cpu".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "east".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
                Series {
                    name: "cpu".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "west".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
                Series {
                    name: "mem".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "east".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
            ],
        );

        let e = f.tag_value_elem("cpu".as_bytes(), "region".as_bytes(), "west".as_bytes());
        assert!(e.is_some(), "expected element");
        let e = e.unwrap();
        assert_eq!(e.series_n(), 1, "unexpected series count: {}", e.series_n());
    }

    #[test]
    fn test_index_file_tag_key_series_id_iterator() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        // let mut buf = vec![];
        let f = create_index_file(
            &sfile,
            vec![
                Series {
                    name: "mem".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "east".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
                Series {
                    name: "cpu".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "east".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
                Series {
                    name: "cpu".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "west".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
            ],
        );

        // itr
        let mut itr = f
            .tag_key_series_id_iterator("cpu".as_bytes(), "region".as_bytes())
            .unwrap()
            .unwrap();

        // NOTE(edd): the series keys end up being emitted in this order because the
        // series were written to different partitons in the _series file_. As such,
        // the key with region=west ends up with a lower series ID than the region=east
        // series, even though it was written later. When the series id sets for each
        // tag block in the index file are merged together and iterated, the roaring
        // bitmap library sorts the series ids, resulting the the series keys being
        // emitted in a different order to that which they were written.
        let exp = vec!["cpu,region=west".to_string(), "cpu,region=east".to_string()];
        let mut got = vec![];
        loop {
            if let Some(e) = itr.next() {
                let series_id: u64 = e.series_id;
                // the first iter: name=cpu, tags=[{region: west}]
                // the second iter: name=cpu, tags=[{region: east}]
                let (name, tags) =
                    parse_series_key(sfile.series_key(series_id).as_slice()).unwrap();
                let k = crate::models::points::must_new_point(
                    to_string!(name),
                    tags,
                    crate::models::points::Fields(HashMap::from([(
                        "a".to_string(),
                        RawVal::String("a".to_string()),
                    )])),
                    time::Time::from_timestamp(0, 0),
                )
                .key()
                .to_vec();
                got.push(to_string!(k));
            } else {
                break;
            }
        }

        assert_eq!(exp, got);
    }

    // Ensure index file generation can be successfully built.
    #[test]
    fn test_index_file_generate_index_file() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        // Build generated index file.
        // let mut buf = vec![];
        let f = generate_index_file(&sfile, 10, 3, 4);

        // Verify that tag/value series can be fetched.
        let e = f.tag_value_elem(
            "measurement0".as_bytes(),
            "key0".as_bytes(),
            "value0".as_bytes(),
        );
        assert!(e.is_some());
        let e = e.unwrap();
        assert_ne!(e.series_n(), 0, "expected series");
    }

    // Ensure index file generated with uvarint encoding can be loaded.
    // Note: must use xxhash, don't use xxhash3 !
    #[test]
    fn test_index_file_generate_index_file_uvarint() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let sfile = Arc::new(series_file::SeriesFile::new(
            path.join("src/index/tsi1/testdata/uvarint/_series"),
        ));

        // Load legacy index file from buffer.
        let mut f = IndexFile::new(sfile);
        f.set_path(path.join("src/index/tsi1/testdata/uvarint/index").as_path());
        let _ = f.open().unwrap();

        // Verify that tag/value series can be fetched.
        let e = f.tag_value_elem(
            "measurement0".as_bytes(),
            "key0".as_bytes(),
            "value0".as_bytes(),
        );
        assert!(e.is_some());
        let e = e.unwrap();
        assert_ne!(e.series_n(), 0, "expected series");
    }

    // Ensure a MeasurementHashSeries returns false when all series are tombstoned.
    #[test]
    fn test_index_file_measurement_has_series_tombstoned() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        // let mut buf = vec![];
        let f = create_index_file(
            &sfile,
            vec![Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "east".as_bytes().to_vec(),
                )])),
                delete: false,
            }],
        );

        // Simulate all series are tombstoned
        let ss = crate::series::series_set::SeriesIDSet::new(&[]);
        if f.measurement_has_series(&ss, "cpu".as_bytes()) {
            panic!("measurement_has_series got true, exp false");
        }
    }

    // //////////// index file test util functions //////////////
    use std::{
        collections::HashMap,
        fs,
        ops::{Deref, DerefMut},
        sync::Arc,
    };

    use crate::common::time;
    use crate::models::RawVal;
    use bytes::Bytes;
    use crossbeam_channel;
    use tokio::io::AsyncWrite;
    use tokio_util::sync::CancellationToken;

    use crate::{
        index::tsi1::{
            index_file::{self, IndexFile},
            log_file::tests::*,
        },
        series::{
            series_file::{self, parse_series_key, tests::*},
            series_set,
        },
    };

    /// Creates an index file with a given set of series.
    pub fn create_index_file<'a>(
        sfile: &'a Arc<series_file::SeriesFile>,
        series: Vec<Series>,
    ) -> index_file::IndexFile {
        let lf = create_log_file(&sfile, series);

        let cancel = CancellationToken::new();
        let mut buf = vec![];
        // Write index file to buffer.
        assert!(lf.compact_to(&mut buf, M, K, cancel).is_ok());

        // Load index file from buffer.
        let mut f = index_file::IndexFile::new(Arc::clone(sfile));
        assert!(f.unmarshal_binary(Some(Bytes::from(buf))).is_ok());

        f
    }

    /// Generates an index file from a set of series based on the count arguments.
    /// Total series returned will equal measurement_n * tag_n * value_n.
    pub fn generate_index_file<'a>(
        // buf: &'a mut Vec<u8>,
        sfile: &'a Arc<series_file::SeriesFile>,
        measurement_n: i32,
        tag_n: i32,
        value_n: i32,
    ) -> index_file::IndexFile {
        // Generate a new log file first.
        let lf = generate_log_file(&sfile, measurement_n, tag_n, value_n);

        let cancel = CancellationToken::new();
        let mut buf = vec![];
        // Compact log file to buffer.
        assert!(lf.compact_to(&mut buf, M, K, cancel).is_ok());

        // Load index file from buffer.
        let mut f = index_file::IndexFile::new(Arc::clone(sfile));
        assert!(f.unmarshal_binary(Some(Bytes::from(buf))).is_ok());

        f
    }

    pub fn must_generate_index_file<'a>(
        // buf: &'a mut Vec<u8>,
        sfile: &'a Arc<series_file::SeriesFile>,
        measurement_n: i32,
        tag_n: i32,
        value_n: i32,
    ) -> index_file::IndexFile {
        generate_index_file(sfile, measurement_n, tag_n, value_n)
    }
}
