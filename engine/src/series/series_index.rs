#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::MetadataExt; // metadata.size
#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt;
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    // fs::{self, OpenOptions},
    // io::{self, Write},
    path::PathBuf,
    sync::Arc,
};
use std::{
    fs::{self, File},
    io,
};

use crate::common::{
    mmap::{self, Mmap},
    rhh, to_str,
};
use bytes::{Buf, BufMut};
use parking_lot::RwLock;
use snafu::{Snafu, ensure};

use super::{series_file::*, series_segment::*};

const SERIES_INDEX_VERSION: u8 = 1;
const SERIES_INDEX_MAGIC: &str = "SIDX";

pub(crate) const SERIES_INDEX_ELEM_SIZE: i32 = 16; // offset + id
pub(crate) const SERIES_INDEX_LOAD_FACTOR: i32 = 90; // rhh load factor
pub(crate) const SERIES_INDEX_HEADER_SIZE: i32 = 4 + 1 // magic + version
    + 8 + 8 // max series + max offset
    + 8 + 8 // count + capacity
    + 8 + 8 // key/id map offset & size
    + 8 + 8; // id/offset map offset & size

/// Error type for SeriesIndex.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOErr { reason: io::Error },

    /// File is empty.
    #[snafu(display("the file to mmap is empty"))]
    FileIsEmpty,

    /// Invalid series index.
    #[snafu(display("invalid series index"))]
    InvalidSeriesIndex,

    /// File is not exist.
    #[snafu(display("the file to mmap is not exist"))]
    FileIsNotExist,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOErr { reason: err }
    }
}

/// Result type for SeriesIndex.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// SeriesIndex represents an index of key-to-id & id-to-offset mappings.
#[derive(Debug, Default)]
pub struct SeriesIndex {
    path: Arc<PathBuf>,

    count: u64,
    capacity: i64,
    mask: i64, // hash mask

    pub max_series_id: u64,
    pub max_offset: i64,

    mmap: Option<Mmap>, // _series/xx/index file data, maybe not exists
    // slice index of key_id_data in mmap
    key_id_data_start: usize,
    key_id_data_end: usize,
    // slice index of id_offset_data in mmap
    id_offset_data_start: usize,
    id_offset_data_end: usize,

    // In-memory data since rebuild (contains all segments data).
    key_id_map: Option<rhh::HashMap<Vec<u8>, u64>>,
    id_offset_map: HashMap<u64, i64>,
    tombstones: HashSet<u64>,
}

pub fn new_series_index(path: impl Into<PathBuf>) -> SeriesIndex {
    SeriesIndex {
        path: Arc::new(path.into()),
        count: 0,
        capacity: 0,
        mask: 0,

        max_series_id: 0,
        max_offset: 0,

        mmap: None,
        key_id_data_start: 0,
        key_id_data_end: 0,
        id_offset_data_start: 0,
        id_offset_data_end: 0,

        key_id_map: None,
        id_offset_map: HashMap::<u64, i64>::new(),
        tombstones: HashSet::<u64>::new(),
    }
}

impl SeriesIndex {
    /// Open memory-maps the index file.
    pub fn open(&mut self) -> Result<()> {
        // Map index data file, if it exists.
        let stat = fs::metadata(&*self.path);
        if stat.is_err() {
            let e = stat.err().unwrap();
            // ensure!(e.kind() != io::ErrorKind::NotFound, Err(e.into()));
            if e.kind() != io::ErrorKind::NotFound {
                // _series/xx/index file exists, but open failed
                return Err(e.into());
            }
        } else if stat.is_ok() {
            // _series/xx/index file exists, open success
            // check if file is empty
            let stat = stat?;
            ensure!(stat.size() > 0, FileIsEmptySnafu);

            // open file
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&*self.path)
            {
                Ok(file) => {
                    // _series/xx/index file not exists
                    // memory map file data
                    let mmap = unsafe {
                        mmap::MmapOptions::new()
                            .len(stat.size() as usize)
                            .map(&file)
                            .expect("read-only memory map failed")
                    };
                    self.mmap = Some(mmap);

                    match read_series_index_header(self.mmap.as_ref().unwrap()) {
                        Ok(hdr) => {
                            self.count = hdr.count;
                            self.capacity = hdr.capacity;
                            self.mask = hdr.capacity - 1;
                            self.max_series_id = hdr.max_series_id;
                            self.max_offset = hdr.max_offset;

                            self.key_id_data_start = hdr.key_id_map.offset as usize;
                            self.key_id_data_end =
                                (hdr.key_id_map.offset + hdr.key_id_map.size) as usize;
                            self.id_offset_data_start = hdr.id_offset_map.offset as usize;
                            self.id_offset_data_end =
                                (hdr.id_offset_map.offset + hdr.id_offset_map.size) as usize;
                        }
                        Err(e) => {
                            self.close();
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    self.close();
                    return IOErrSnafu { reason: e }.fail();
                }
            }
        }

        self.key_id_map = Some(rhh::HashMap::<Vec<u8>, u64>::new(rhh::Options::default()));
        self.id_offset_map = HashMap::<u64, i64>::new();
        self.tombstones = HashSet::<u64>::new();

        Ok(())
    }

    pub fn path(&self) -> PathBuf {
        self.path.to_path_buf()
    }

    /// Close unmaps the index file.
    pub fn close(&mut self) {
        self.mmap = None;

        self.key_id_data_start = 0;
        self.key_id_data_end = 0;
        self.id_offset_data_start = 0;
        self.id_offset_data_end = 0;

        self.key_id_map = None;
        self.id_offset_map = HashMap::<u64, i64>::new();
        self.tombstones = HashSet::<u64>::new();
    }

    /// Rebuilds the in-memory index for all new entries.
    pub fn recover(&mut self, segments: &[Arc<RwLock<SeriesSegment>>]) -> Result<()> {
        // allocate new in-memory maps
        self.key_id_map = Some(rhh::HashMap::<Vec<u8>, u64>::new(rhh::Options::default()));
        self.id_offset_map = HashMap::<u64, i64>::new();
        self.tombstones = HashSet::<u64>::new();

        // process all entries since the maximum offset in the on-disk index
        let (min_segment_id, _) = split_series_offset(self.max_offset);
        for segment in segments {
            let segment = segment.read();
            if segment.id() < min_segment_id {
                continue;
            }

            let _e = segment.for_each_entry::<_, Error>(
                |flag: u8, id: u64, offset: i64, key: &[u8]| -> Result<()> {
                    if offset <= self.max_offset {
                        return Ok(());
                    }
                    self.exec_entry(flag, id, offset, Some(key.to_vec()));
                    Ok(())
                },
            );
        }

        Ok(())
    }

    fn key_id_data(&self) -> &[u8] {
        &self.mmap.as_ref().unwrap().as_ref()[self.key_id_data_start..self.key_id_data_end]
    }

    fn id_offset_data(&self) -> &[u8] {
        &self.mmap.as_ref().unwrap().as_ref()[self.id_offset_data_start..self.id_offset_data_end]
    }

    /// Returns the number of series in the  index.
    pub fn count(&self) -> u64 {
        self.on_disk_count() + self.in_mem_count()
    }

    /// Returns the number of series in the on-disk index.
    pub fn on_disk_count(&self) -> u64 {
        self.count
    }

    /// Returns the number of series in the in-memory index.
    pub fn in_mem_count(&self) -> u64 {
        self.id_offset_map.len() as u64
    }

    pub fn insert(&mut self, key: Vec<u8>, id: u64, offset: i64) {
        self.exec_entry(SERIES_ENTRY_INSERT_FLAG, id, offset, Some(key))
    }

    /// Marks the series id as deleted.
    pub fn delete(&mut self, id: u64) {
        self.exec_entry(SERIES_ENTRY_TOMBSTONE_FLAG, id, 0, None)
    }

    /// Returns true if series id has been deleted.
    pub fn is_deleted(&self, id: u64) -> bool {
        if self.tombstones.contains(&id) {
            return true;
        }

        self.find_offset_by_id(id) == 0
    }

    fn exec_entry(&mut self, flag: u8, id: u64, offset: i64, key: Option<Vec<u8>>) {
        match flag {
            SERIES_ENTRY_INSERT_FLAG => {
                self.key_id_map.as_mut().unwrap().put(key.unwrap(), id);
                self.id_offset_map.insert(id, offset);

                if id > self.max_series_id {
                    self.max_series_id = id;
                }
                if offset > self.max_offset {
                    self.max_offset = offset;
                }
            }
            SERIES_ENTRY_TOMBSTONE_FLAG => {
                let _ = self.tombstones.insert(id);
            }
            _ => unreachable!("unreachable"),
        }
    }

    pub fn find_id_by_series_key(
        &self,
        segments: &[Arc<RwLock<SeriesSegment>>],
        key: &[u8],
    ) -> u64 {
        if let Some(id) = self.key_id_map.as_ref().unwrap().get(key) {
            if *id != 0 && !self.is_deleted(*id) {
                return *id;
            }
        }

        if self.mmap.is_none() {
            return 0;
        }

        // query from _series/xx/index
        let key_id_data =
            &self.mmap.as_ref().unwrap()[self.key_id_data_start..self.key_id_data_end];

        let hash = rhh::hash_key(key);
        let mut d = 0i64;
        let mut pos = hash & self.mask;
        loop {
            let elem = &key_id_data[(pos * SERIES_INDEX_ELEM_SIZE as i64) as usize..];
            let elem_offset = (&elem[..8]).get_u64() as i64;

            if elem_offset == 0 {
                return 0;
            }

            let elem_key = read_series_key_from_segments(
                segments,
                elem_offset + SERIES_ENTRY_HEADER_SIZE as i64,
            )
            .unwrap();
            let elem_hash = rhh::hash_key(&elem_key);
            if d > rhh::dist(elem_hash, pos, self.capacity) {
                return 0;
            } else if elem_hash == hash && elem_key.eq(key) {
                let id = (&elem[8..]).get_u64();
                if self.is_deleted(id) {
                    return 0;
                }
                return id;
            }

            d += 1;
            pos = (pos + 1) & self.mask;
        }
    }

    pub fn find_id_by_name_tags(
        &self,
        segments: &[Arc<RwLock<SeriesSegment>>],
        name: &[u8],
        tags: crate::models::points::Tags,
    ) -> u64 {
        let id = self.find_id_by_series_key(segments, encode_series_key(name, &tags).as_slice());
        if self.tombstones.contains(&id) {
            return 0;
        }

        id
    }

    pub fn find_offset_by_id(&self, id: u64) -> i64 {
        if let Some(offset) = self.id_offset_map.get(&id) {
            if *offset != 0 {
                return *offset;
            }
        }

        if self.mmap.is_none() {
            return 0;
        }

        // query from _series/xx/index
        let id_offset_data =
            &self.mmap.as_ref().unwrap()[self.id_offset_data_start..self.id_offset_data_end];

        let hash = rhh::hash_u64(id);
        let mut d = 0i64;
        let mut pos = hash & self.mask;
        loop {
            let elem = &id_offset_data[(pos * SERIES_INDEX_ELEM_SIZE as i64) as usize..];
            let elem_id = (&elem[..8]).get_u64();
            if elem_id == id {
                return (&elem[8..]).get_u64() as i64;
            } else if elem_id == 0 || d > rhh::dist(rhh::hash_u64(elem_id), pos, self.capacity) {
                return 0;
            }

            d += 1;
            pos = (pos + 1) & self.mask;
        }
    }

    /// Returns a copy of idx for use during compaction. In-memory maps are not cloned.
    pub fn clone_series_index(&self) -> Result<SeriesIndex> {
        let mut tombstones = HashSet::<u64>::with_capacity(self.tombstones.len());
        for id in self.tombstones.iter() {
            let _ = tombstones.insert(*id);
        }

        let mut id_offset_map = HashMap::<u64, i64>::new();
        for (k, v) in self.id_offset_map.iter() {
            let _ = id_offset_map.insert(*k, *v);
        }

        Ok(SeriesIndex {
            path: self.path.clone(),

            count: self.count,
            capacity: self.capacity,
            mask: self.mask,

            max_series_id: self.max_series_id,
            max_offset: self.max_offset,

            mmap: self.mmap.clone(),
            key_id_data_start: self.key_id_data_start,
            key_id_data_end: self.key_id_data_end,
            id_offset_data_start: self.id_offset_data_start,
            id_offset_data_end: self.id_offset_data_end,

            key_id_map: None, // needn't clone
            id_offset_map: self.id_offset_map.clone(),
            tombstones,
        })
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct KeyIDMap {
    pub offset: i64,
    pub size: i64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct IDOffsetMap {
    pub offset: i64,
    pub size: i64,
}
/// SeriesIndexHeader represents the header of a series index.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SeriesIndexHeader {
    version: u8,

    pub max_series_id: u64,
    pub max_offset: i64,

    pub count: u64,
    pub capacity: i64,

    pub key_id_map: KeyIDMap,
    pub id_offset_map: IDOffsetMap,
}

pub fn new_series_index_header() -> SeriesIndexHeader {
    SeriesIndexHeader {
        version: SERIES_INDEX_VERSION,
        ..Default::default()
    }
}

pub fn read_series_index_header(data: &[u8]) -> Result<SeriesIndexHeader> {
    let mut hdr = new_series_index_header();

    let mut n = 0;
    // read magic number
    let mut magic: [u8; SERIES_INDEX_MAGIC.len()] = [0; SERIES_INDEX_MAGIC.len()];
    magic.copy_from_slice(&data[..SERIES_INDEX_MAGIC.len()]);

    ensure!(
        to_str!(&magic).eq(SERIES_INDEX_MAGIC),
        InvalidSeriesIndexSnafu
    );
    n += SERIES_INDEX_MAGIC.len();

    // read version
    hdr.version = (&data[n..n + 1]).get_u8();
    n += 1;

    // read max offset
    hdr.max_series_id = (&data[n..n + 8]).get_u64();
    n += 8;
    hdr.max_offset = (&data[n..n + 8]).get_i64();
    n += 8;

    // read count & capacity
    hdr.count = (&data[n..n + 8]).get_u64();
    n += 8;
    hdr.capacity = (&data[n..n + 8]).get_i64();
    n += 8;

    // read key/id map position
    hdr.key_id_map.offset = (&data[n..n + 8]).get_i64();
    n += 8;
    hdr.key_id_map.size = (&data[n..n + 8]).get_i64();
    n += 8;

    // read offset/id map position
    hdr.id_offset_map.offset = (&data[n..n + 8]).get_i64();
    n += 8;
    hdr.id_offset_map.size = (&data[n..n + 8]).get_i64();

    Ok(hdr)
}

impl SeriesIndexHeader {
    /// Writes the header to w.
    pub fn write_to(&self, w: &mut impl Write) -> Result<usize> {
        let mut buf = vec![];
        buf.put_slice(SERIES_INDEX_MAGIC.as_bytes());
        buf.put_u8(self.version);
        buf.put_u64(self.max_series_id); // BigEndian
        buf.put_i64(self.max_offset);
        buf.put_u64(self.count);
        buf.put_i64(self.capacity);
        buf.put_i64(self.key_id_map.offset);
        buf.put_i64(self.key_id_map.size);
        buf.put_i64(self.id_offset_map.offset);
        buf.put_i64(self.id_offset_map.size);

        let sz = w.write(&buf)?;
        Ok(sz)
    }
}

#[cfg(any(test, feature = "benchmarking"))]
mod tests {
    use crate::{db::tests::must_temp_reefsdb_dir, series::series_index::*};

    #[test]
    fn test_series_index_count() {
        let dir = must_temp_reefsdb_dir();

        let mut idx = new_series_index(dir.path().join("index"));
        idx.open().unwrap();

        let key0 = encode_series_key("m0".as_bytes(), &crate::models::points::Tags::new());
        idx.insert(key0, 1, 10);
        let key1 = encode_series_key("m1".as_bytes(), &crate::models::points::Tags::new());
        idx.insert(key1, 2, 20);

        let n = idx.count();
        if n != 2 {
            panic!("unexpected count: {}", n);
        }
    }

    #[test]
    fn test_series_index_delete() {
        let dir = must_temp_reefsdb_dir();

        let mut idx = new_series_index(dir.path().join("index"));
        idx.open().unwrap();

        let key0 = encode_series_key("m0".as_bytes(), &crate::models::points::Tags::new());
        idx.insert(key0, 1, 10);
        let key1 = encode_series_key("m1".as_bytes(), &crate::models::points::Tags::new());
        idx.insert(key1, 2, 20);
        idx.delete(1); // delete series_id=1

        assert!(idx.is_deleted(1), "expected deletion");
        assert!(!idx.is_deleted(2), "expected series to exist");
    }

    #[test]
    fn test_series_index_find_id_by_series_key() {
        let dir = must_temp_reefsdb_dir();

        let mut idx = new_series_index(dir.path().join("index"));
        idx.open().unwrap();

        let key0 = encode_series_key("m0".as_bytes(), &crate::models::points::Tags::new());
        idx.insert(key0.clone(), 1, 10);
        let key1 = encode_series_key("m1".as_bytes(), &crate::models::points::Tags::new());
        idx.insert(key1.clone(), 2, 20);
        let bad_key =
            encode_series_key("not_found".as_bytes(), &crate::models::points::Tags::new());

        let id = idx.find_id_by_series_key(&[], key0.as_slice());
        if id != 1 {
            panic!("unexpected id(0): {}", id);
        }

        let id = idx.find_id_by_series_key(&[], key1.as_slice());
        if id != 2 {
            panic!("unexpected id(1): {}", id);
        }

        let id = idx.find_id_by_series_key(&[], bad_key.as_slice());
        if id != 0 {
            panic!("unexpected id(2): {}", id);
        }

        let id = idx.find_id_by_name_tags(&[], "m0".as_bytes(), crate::models::points::Tags::new());
        if id != 1 {
            panic!("unexpected id(0): {}", id);
        }

        let id = idx.find_id_by_name_tags(&[], "m1".as_bytes(), crate::models::points::Tags::new());
        if id != 2 {
            panic!("unexpected id(1): {}", id);
        }

        let id = idx.find_id_by_name_tags(
            &[],
            "not_found".as_bytes(),
            crate::models::points::Tags::new(),
        );
        if id != 0 {
            panic!("unexpected id(2): {}", id);
        }
    }

    #[test]
    fn test_series_index_find_offset_by_id() {
        let dir = must_temp_reefsdb_dir();

        let mut idx = new_series_index(dir.path().join("index"));
        idx.open().unwrap();

        let key0 = encode_series_key("m0".as_bytes(), &crate::models::points::Tags::new());
        idx.insert(key0, 1, 10);
        let key1 = encode_series_key("m1".as_bytes(), &crate::models::points::Tags::new());
        idx.insert(key1, 2, 20);

        let offset = idx.find_offset_by_id(1);
        if offset != 10 {
            panic!("unexpected offset(0): {}", offset);
        }

        let offset = idx.find_offset_by_id(2);
        if offset != 20 {
            panic!("unexpected offset(0): {}", offset);
        }

        let offset = idx.find_offset_by_id(3);
        if offset != 0 {
            panic!("unexpected offset(0): {}", offset);
        }
    }

    #[test]
    fn test_series_index_header() {
        let mut hdr = new_series_index_header();
        assert!(
            hdr.version == SERIES_INDEX_VERSION,
            "unexpected version: {}",
            hdr.version
        );

        hdr.max_series_id = 10;
        hdr.max_offset = 20;
        hdr.count = 30;
        hdr.capacity = 40;
        (hdr.key_id_map.offset, hdr.key_id_map.size) = (50, 60);
        (hdr.id_offset_map.offset, hdr.id_offset_map.size) = (70, 80);

        // marshal/unmarshal
        let mut buf = Vec::<u8>::new();
        assert!(hdr.write_to(&mut buf).is_ok());
        let other = read_series_index_header(buf.as_slice()).unwrap();
        assert!(hdr == other);
    }
}
