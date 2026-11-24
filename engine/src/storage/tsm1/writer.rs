/*!
A TSM file is composed for four sections: header, blocks, index and the footer.

┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
│ Header │               Blocks               │    Index    │    Footer    │
│5 bytes │              N bytes               │   N bytes   │   4 bytes    │
└────────┴────────────────────────────────────┴─────────────┴──────────────┘

Header is composed of a magic number to identify the file type and a version
number.

┌───────────────────┐
│      Header       │
├─────────┬─────────┤
│  Magic  │ Version │
│ 4 bytes │ 1 byte  │
└─────────┴─────────┘

Blocks are sequences of pairs of CRC32 and data.  The block data is opaque to the
file.  The CRC32 is used for block level error detection.  The length of the blocks
is stored in the index.

┌───────────────────────────────────────────────────────────┐
│                          Blocks                           │
├───────────────────┬───────────────────┬───────────────────┤
│      Block 1      │      Block 2      │      Block N      │
├─────────┬─────────┼─────────┬─────────┼─────────┬─────────┤
│  CRC    │  Data   │  CRC    │  Data   │  CRC    │  Data   │
│ 4 bytes │ N bytes │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
└─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘

Following the blocks is the index for the blocks in the file.  The index is
composed of a sequence of index entries ordered lexicographically by key and
then by time.  Each index entry starts with a key length and key followed by a
count of the number of blocks in the file.  Each block entry is composed of
the min and max time for the block, the offset into the file where the block
is located and the the size of the block.

The index structure can provide efficient access to all blocks as well as the
ability to determine the cost associated with accessing a given key.  Given a key
and timestamp, we can determine whether a file contains the block for that
timestamp as well as where that block resides and how much data to read to
retrieve the block.  If we know we need to read all or multiple blocks in a
file, we can use the size to determine how much to read in a given IO.

┌────────────────────────────────────────────────────────────────────────────┐
│                                   Index                                    │
├─────────┬─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───┤
│ Key Len │   Key   │ Type │ Count │Min Time │Max Time │ Offset │  Size  │...│
│ 2 bytes │ N bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │4 bytes │   │
└─────────┴─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───┘

The last section is the footer that stores the offset of the start of the index.

┌─────────┐
│ Footer  │
├─────────┤
│Index Ofs│
│ 8 bytes │
└─────────┘
*/

use std::{
    fmt::Display,
    fs::File,
    io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use crate::common::{time::Time, to_str};
use crate::models::points::Value;
use bytes::{Buf, BufMut};
use crc32fast;
use snafu::{ResultExt, Snafu, ensure};

use crate::storage::{
    limiter::SyncWrite,
    tsm1::{
        block_statistics::{BLOCK_STATISTICS_SIZE, BlockStatistics},
        data_encoding::{encoding, encoding::block_type, encoding_values::Values},
    },
};

// MagicNumber is written as the first 4 bytes of a data file to
// identify the file as a tsm1 formatted file
pub const MAGIC_NUMBER: u32 = 0x16D116D1;

/// Current TSM format version. Version 2 includes fixed-size FIRST/LAST values
/// in each block-statistics record.
pub const VERSION: u8 = 2;

// Size in bytes of a version 2 index entry.
pub const INDEX_ENTRY_SIZE: usize = 28 + BLOCK_STATISTICS_SIZE;

// Size in bytes used to store the count of index entries for a key
pub const INDEX_COUNT_SIZE: usize = 2;

// Size in bytes used to store the type of block encoded
pub const INDEX_TYPE_SIZE: usize = 1;
// FIXME： compact vlog 时，很容易达到65535
// Max number of blocks for a given key that can exist in a single file
pub const MAX_INDEX_ENTRIES: i32 = (1 << (INDEX_COUNT_SIZE * 8)) - 1;

// max length of a key in an index entry (measurement + tags)
pub const MAX_KEY_LENGTH: i32 = (1 << (2 * 8)) - 1;

// The threshold amount data written before we periodically fsync a TSM file.  This helps avoid
// long pauses due to very large fsyncs at the end of writing a TSM file.
pub const FSYNC_EVERY: i64 = 25 * 1024 * 1024;

/// Error type for tsm writer.
#[derive(Debug, Snafu)]
pub enum Error {
    /// NoValues is returned when TSMWriter.WriteIndex is called and there are no values to write.
    #[snafu(display("no values written"))]
    NoValues,

    /// TSMClosed is returned when performing an operation against a closed TSM file.
    #[snafu(display("tsm file closed"))]
    TSMClosed,

    /// MaxKeyLengthExceeded is returned when attempting to write a key that is too long.
    #[snafu(display("max key length exceeded"))]
    MaxKeyLengthExceeded,

    /// MaxBlocksExceeded is returned when attempting to write a block past the allowed number.
    #[snafu(display("max blocks exceeded"))]
    MaxBlocksExceeded { offset: i64, size: i64 },

    /// IO error.
    #[snafu(display("tsm writer error: {reason}"))]
    IO { reason: std::io::Error },

    /// EncodingValues error.
    #[snafu(display("{}", source))]
    EncodingValues {
        source: crate::storage::tsm1::data_encoding::encoding_values::Error,
    },

    /// Encoding error.
    #[snafu(display("{}", source))]
    Encoding {
        source: crate::storage::tsm1::data_encoding::encoding::Error,
    },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for TSI.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO { reason: err }
    }
}

/// IndexEntry is the index information for a given block in a TSM file.
#[derive(Default, Debug, Clone)]
pub struct IndexEntry {
    // The min and max time of all points stored in the block.
    pub min_time: i64,
    pub max_time: i64,

    // The absolute position in the file where this block is located.
    pub offset: i64,

    // The size in bytes of the block in the file.
    pub size: u32,

    /// None only for explicitly unavailable metadata.
    pub statistics: Option<BlockStatistics>,
}

impl IndexEntry {
    /// Decodes an IndexEntry from a byte slice.
    pub fn unmarshal_binary(&mut self, b: &[u8], block_type: u8) -> Result<()> {
        ensure!(
            b.len() >= INDEX_ENTRY_SIZE,
            StringErrSnafu {
                reason: format!(
                    "unmarshalBinary: short buf: {} < {}",
                    b.len(),
                    INDEX_ENTRY_SIZE
                ),
            }
        );

        self.min_time = (&b[..8]).get_u64() as i64;
        self.max_time = (&b[8..16]).get_u64() as i64;
        self.offset = (&b[16..24]).get_u64() as i64;
        self.size = (&b[24..28]).get_u32();
        self.statistics = BlockStatistics::decode(block_type, &b[28..INDEX_ENTRY_SIZE])
            .map_err(|reason| Error::StringErr { reason })?;

        Ok(())
    }

    /// Writes a binary-encoded version of IndexEntry to b.
    /// Note: the slice's length must be INDEX_ENTRY_SIZE
    pub fn append_to(&self, b: &mut [u8]) {
        // ensure!(b.len() == INDEX_ENTRY_SIZE, StringErrSnafu {
        //     reason: format!("appendTo: invalid buf length: exp {}, got {}", INDEX_ENTRY_SIZE, b.len()),
        // });

        (&mut b[0..8]).put_u64(self.min_time as u64);
        (&mut b[8..16]).put_u64(self.max_time as u64);
        (&mut b[16..24]).put_u64(self.offset as u64);
        (&mut b[24..28]).put_u32(self.size);
        BlockStatistics::encode_into(self.statistics, &mut b[28..INDEX_ENTRY_SIZE]);
    }

    /// Returns true if this IndexEntry may contain values for the given time.
    /// The min and max times are inclusive.
    pub fn contains(&self, t: i64) -> bool {
        self.min_time <= t && t <= self.max_time
    }

    /// Returns true if the given time ranges are completely within the entry's time bounds.
    pub fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        self.min_time <= max && min <= self.max_time
    }
}

impl Display for IndexEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "min={} max={} ofs={} siz={}",
            Time::from_timestamp(0, self.min_time as u32),
            Time::from_timestamp(0, self.max_time as u32),
            self.offset,
            self.size
        )
    }
}

pub enum TSMIndex {
    InmemIndex(InmemIndex), // DirectIndex
    DiskIndex(DiskIndex),   //
}

impl TSMIndex {
    pub fn entries(&self, key: &[u8]) -> &[IndexEntry] {
        match self {
            TSMIndex::InmemIndex(idx) => idx.entries(key),
            TSMIndex::DiskIndex(idx) => idx.entries(key),
        }
    }

    pub fn key_count(&self) -> i32 {
        match self {
            TSMIndex::InmemIndex(idx) => idx.key_count(),
            TSMIndex::DiskIndex(idx) => idx.key_count(),
        }
    }

    pub fn size(&self) -> u32 {
        match self {
            TSMIndex::InmemIndex(idx) => idx.size(),
            TSMIndex::DiskIndex(idx) => idx.size(),
        }
    }

    pub fn add(
        &mut self,
        key: &[u8],
        block_type: u8,
        min_time: i64,
        max_time: i64,
        offset: i64,
        size: u32,
        statistics: Option<BlockStatistics>,
    ) {
        match self {
            TSMIndex::InmemIndex(idx) => idx.add(
                key, block_type, min_time, max_time, offset, size, statistics,
            ),
            TSMIndex::DiskIndex(idx) => idx.add(
                key, block_type, min_time, max_time, offset, size, statistics,
            ),
        }
    }

    fn write_to(&mut self, w: &mut impl SyncWrite) -> Result<i64> {
        match self {
            TSMIndex::InmemIndex(idx) => idx.write_to(w),
            TSMIndex::DiskIndex(idx) => idx.write_to(w),
        }
    }

    pub fn reset(&mut self) {
        match self {
            TSMIndex::InmemIndex(idx) => idx.reset(),
            TSMIndex::DiskIndex(idx) => idx.reset(),
        }
    }

    pub fn close(&mut self) -> Result<()> {
        match self {
            TSMIndex::InmemIndex(idx) => idx.close(),
            TSMIndex::DiskIndex(idx) => idx.close(),
        }
    }

    /// Removes the index from any temporary storage
    pub fn remove(&mut self) -> Result<()> {
        match self {
            TSMIndex::InmemIndex(idx) => idx.remove(),
            TSMIndex::DiskIndex(idx) => idx.remove(),
        }
    }
}

// InmemIndex is a simple in-memory index implementation for a TSM file.  The full index
// must fit in memory.
pub struct InmemIndex {
    key_count: i32,
    size: u32,

    w: Vec<u8>,

    key: Vec<u8>,
    index_entries: Option<IndexEntries>,
}

impl InmemIndex {
    pub fn new() -> Self {
        Self {
            key_count: 0,
            size: 0,

            w: Vec::<u8>::with_capacity(1024 * 1024),

            key: Vec::new(),
            index_entries: None,
        }
    }

    pub fn entries(&self, key: &[u8]) -> &[IndexEntry] {
        if self.key.is_empty() {
            return &[];
        }

        if self.key.as_slice().eq(key) {
            return &self.index_entries.as_ref().unwrap().entries;
        }

        &[]
    }

    pub fn entry(&self, key: &[u8], t: i64) -> Option<&IndexEntry> {
        let entries = self.entries(key);
        if !entries.is_empty() {
            for e in entries {
                if e.contains(t) {
                    return Some(e);
                }
            }
        }

        None
    }

    pub fn key_count(&self) -> i32 {
        self.key_count
    }

    pub fn size(&self) -> u32 {
        self.size
    }

    // TODO:
    // // copyBuffer is the actual implementation of Copy and CopyBuffer.
    // // if buf is nil, one is allocated.  This is copied from the Go stdlib
    // // in order to remove the fast path WriteTo calls which circumvent any
    // // IO throttling as well as to add periodic fsyncs to avoid long stalls.
    // fn copyBuffer(f syncer, dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {

    // }

    pub fn add(
        &mut self,
        key: &[u8],
        block_type: u8,
        min_time: i64,
        max_time: i64,
        offset: i64,
        size: u32,
        statistics: Option<BlockStatistics>,
    ) {
        // Is this the first block being added?
        if self.key.is_empty() {
            // size of the key stored in the index
            self.size += (2 + key.len()) as u32;
            // size of the count of entries stored in the index
            self.size += INDEX_COUNT_SIZE as u32;

            self.key = key.to_vec();
            if self.index_entries.is_none() {
                self.index_entries = Some(IndexEntries::default());
            }
            self.index_entries.as_mut().unwrap().typ = block_type;
            self.index_entries
                .as_mut()
                .unwrap()
                .entries
                .push(IndexEntry {
                    min_time,
                    max_time,
                    offset,
                    size,
                    statistics,
                });

            // size of the encoded index entry
            self.size += INDEX_ENTRY_SIZE as u32;
            self.key_count += 1;
            return;
        }

        // See if were still adding to the same series key.
        match self.key.as_slice().cmp(key) {
            std::cmp::Ordering::Equal => {
                // The last block is still this key
                self.index_entries
                    .as_mut()
                    .unwrap()
                    .entries
                    .push(IndexEntry {
                        min_time,
                        max_time,
                        offset,
                        size,
                        statistics,
                    });

                // size of the encoded index entry
                self.size += INDEX_ENTRY_SIZE as u32;
            }
            std::cmp::Ordering::Less => {
                let _ = self.flush_buffered();

                // We have a new key that is greater than the last one so we need to add
                // a new index block section.

                // size of the key stored in the index
                self.size += (2 + key.len()) as u32;
                // size of the count of entries stored in the index
                self.size += INDEX_COUNT_SIZE as u32;

                self.key = key.to_vec();
                self.index_entries.as_mut().unwrap().typ = block_type;
                self.index_entries
                    .as_mut()
                    .unwrap()
                    .entries
                    .push(IndexEntry {
                        min_time,
                        max_time,
                        offset,
                        size,
                        statistics,
                    });

                // size of the encoded index entry
                self.size += INDEX_ENTRY_SIZE as u32;
                self.key_count += 1;
            }
            std::cmp::Ordering::Greater => {
                // Keys can't be added out of order.
                panic!(
                    "keys must be added in sorted order: {} < {}",
                    to_str!(key),
                    to_str!(&self.key)
                );
            }
        }
    }

    fn copy_buf(dst: &mut impl SyncWrite, mut src: &[u8]) -> Result<i64> {
        let mut buf = vec![0u8; 32 * 1024];

        let mut written = 0i64;
        let mut last_sync = 0i64;
        loop {
            match src.read(&mut buf) {
                Ok(nr) => {
                    match nr.cmp(&0) {
                        std::cmp::Ordering::Greater => {
                            let nw = dst.write(&buf[..nr])?;
                            if nw > 0 {
                                written += nw as i64;
                            }

                            if written - last_sync > FSYNC_EVERY {
                                dst.sync_all()?;
                                last_sync = written;
                            }

                            if nr != nw {
                                return Err(Error::StringErr {
                                    reason: "written bytes is less than readed".to_string(),
                                });
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            // EOF
                            return Ok(written);
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    if e.kind() != ErrorKind::UnexpectedEof {
                        return Err(e.into());
                    }
                    return Ok(written);
                }
            }
        }
    }

    pub fn write_to(&mut self, w: &mut impl SyncWrite) -> Result<i64> {
        self.flush_buffered()?;
        self.w.flush()?;

        Self::copy_buf(w, &self.w)
    }

    fn flush_buffered(&mut self) -> Result<i64> {
        let mut n = 0;
        let mut buf = [0u8; 5];

        if self.key.is_empty() {
            return Ok(0);
        }

        // For each key, individual entries are sorted by time
        let key = &mut self.key;
        let entries = self.index_entries.as_mut().unwrap();

        // FIXME: MAX_INDEX_ENTRIES donot panic ?
        // ensure!(entries.len() as i32 <= MAX_INDEX_ENTRIES, StringErrSnafu {
        //     reason: format!(
        //         "key '{:?}' exceeds max index entries: {} > {}",
        //         key,
        //         entries.len(),
        //         MAX_INDEX_ENTRIES
        //     ),
        // });

        if !entries.is_sorted() {
            entries.sort();
        }

        (&mut buf[0..2]).put_u16(key.len() as u16);
        buf[2] = entries.typ;
        (&mut buf[3..5]).put_u16(entries.len() as u16);

        // Append the key length and key
        let sz = self.w.write(&buf[0..2])?;
        n += sz;

        let sz = self.w.write(key)?;
        n += sz;

        // Append the block type and count
        let sz = self.w.write(&buf[2..5])?;
        n += sz;

        // Append each index entry for all blocks for this key
        let sz = entries.write_to(&mut self.w)?; // tsm writer write entries error
        n += sz as usize;

        self.key.clear();
        self.index_entries.as_mut().unwrap().typ = 0;
        self.index_entries.as_mut().unwrap().entries.clear();

        Ok(n as i64)
    }

    pub fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        self.write_to(&mut buf)?;
        Ok(buf)
    }

    pub fn reset(&mut self) {
        self.key_count = 0;
        self.size = 0;
        self.w.clear();
        self.key.clear();
        self.index_entries = None;
    }

    pub fn close(&mut self) -> Result<()> {
        // Flush anything remaining in the index
        self.w.flush()?;
        Ok(())
    }

    /// Removes the index from any temporary storage
    pub fn remove(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Default for InmemIndex {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DiskIndex {
    key_count: i32,
    size: u32,

    // The bytes written count of when we last fsync'd
    last_sync: u32,

    path: PathBuf,
    // f syncer
    w: BufWriter<File>,

    key: Vec<u8>,
    index_entries: Option<IndexEntries>,
}

impl DiskIndex {
    pub fn new(w: File, path: impl AsRef<Path>) -> Self {
        Self {
            key_count: 0,
            size: 0,
            last_sync: 0,

            path: path.as_ref().to_path_buf(),

            w: BufWriter::new(w),

            key: Vec::new(),
            index_entries: None,
        }
    }

    pub fn entries(&self, key: &[u8]) -> &[IndexEntry] {
        if self.key.is_empty() {
            return &[];
        }

        if self.key.as_slice().eq(key) {
            return &self.index_entries.as_ref().unwrap().entries;
        }

        &[]
    }

    pub fn entry(&self, key: &[u8], t: i64) -> Option<&IndexEntry> {
        let entries = self.entries(key);
        if !entries.is_empty() {
            for e in entries {
                if e.contains(t) {
                    return Some(e);
                }
            }
        }

        None
    }

    pub fn key_count(&self) -> i32 {
        self.key_count
    }

    pub fn size(&self) -> u32 {
        self.size
    }

    // TODO:
    // // copyBuffer is the actual implementation of Copy and CopyBuffer.
    // // if buf is nil, one is allocated.  This is copied from the Go stdlib
    // // in order to remove the fast path WriteTo calls which circumvent any
    // // IO throttling as well as to add periodic fsyncs to avoid long stalls.
    // fn copyBuffer(f syncer, dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {

    // }

    pub fn add(
        &mut self,
        key: &[u8],
        block_type: u8,
        min_time: i64,
        max_time: i64,
        offset: i64,
        size: u32,
        statistics: Option<BlockStatistics>,
    ) {
        // Is this the first block being added?
        if self.key.is_empty() {
            // size of the key stored in the index
            self.size += (2 + key.len()) as u32;
            // size of the count of entries stored in the index
            self.size += INDEX_COUNT_SIZE as u32;

            self.key = key.to_vec();
            if self.index_entries.is_none() {
                self.index_entries = Some(IndexEntries::default());
            }
            self.index_entries.as_mut().unwrap().typ = block_type;
            self.index_entries
                .as_mut()
                .unwrap()
                .entries
                .push(IndexEntry {
                    min_time,
                    max_time,
                    offset,
                    size,
                    statistics,
                });

            // size of the encoded index entry
            self.size += INDEX_ENTRY_SIZE as u32;
            self.key_count += 1;
            return;
        }

        // See if were still adding to the same series key.
        match self.key.as_slice().cmp(key) {
            std::cmp::Ordering::Equal => {
                // The last block is still this key
                self.index_entries
                    .as_mut()
                    .unwrap()
                    .entries
                    .push(IndexEntry {
                        min_time,
                        max_time,
                        offset,
                        size,
                        statistics,
                    });

                // size of the encoded index entry
                self.size += INDEX_ENTRY_SIZE as u32;
            }
            std::cmp::Ordering::Less => {
                let _ = self.flush_buffered();

                // We have a new key that is greater than the last one so we need to add
                // a new index block section.

                // size of the key stored in the index
                self.size += (2 + key.len()) as u32;
                // size of the count of entries stored in the index
                self.size += INDEX_COUNT_SIZE as u32;

                self.key = key.to_vec();
                self.index_entries.as_mut().unwrap().typ = block_type;
                self.index_entries
                    .as_mut()
                    .unwrap()
                    .entries
                    .push(IndexEntry {
                        min_time,
                        max_time,
                        offset,
                        size,
                        statistics,
                    });

                // size of the encoded index entry
                self.size += INDEX_ENTRY_SIZE as u32;
                self.key_count += 1;
            }
            std::cmp::Ordering::Greater => {
                // Keys can't be added out of order.
                panic!(
                    "keys must be added in sorted order: {} < {}",
                    to_str!(key),
                    to_str!(&self.key)
                );
            }
        }
    }

    pub fn write_to(&mut self, w: &mut impl SyncWrite) -> Result<i64> {
        self.flush_buffered()?;

        self.w.flush()?;
        self.w.get_mut().flush()?;

        self.w.get_mut().seek(SeekFrom::Start(0))?;

        let mut n = 0;
        let mut buffer = [0u8; 1024 * 1024];
        loop {
            let bytes_read = self.w.get_mut().read(&mut buffer)?;
            if bytes_read == 0 {
                break; // End of file
            }
            w.write_all(&buffer[..bytes_read])?;
            n += bytes_read;
        }

        // TODO: need sync ?
        w.sync_all()?;

        Ok(n as i64)
    }

    fn flush_buffered(&mut self) -> Result<i64> {
        let mut n = 0;
        let mut buf = [0u8; 5];

        if self.key.is_empty() {
            return Ok(0);
        }

        // For each key, individual entries are sorted by time
        let key = &mut self.key;
        let entries = self.index_entries.as_mut().unwrap();

        // FIXME: MAX_INDEX_ENTRIES donot panic ?
        // ensure!(entries.len() as i32 <= MAX_INDEX_ENTRIES, StringErrSnafu {
        //     reason: format!(
        //         "key '{:?}' exceeds max index entries: {} > {}",
        //         key,
        //         entries.len(),
        //         MAX_INDEX_ENTRIES
        //     ),
        // });

        if !entries.is_sorted() {
            entries.sort();
        }

        (&mut buf[0..2]).put_u16(key.len() as u16);
        buf[2] = entries.typ;
        (&mut buf[3..5]).put_u16(entries.len() as u16);

        // let w = self.w.as_mut().unwrap();
        // Append the key length and key
        let sz = self.w.write(&buf[0..2])?;
        n += sz;

        let sz = self.w.write(key)?;
        n += sz;

        // Append the block type and count
        let sz = self.w.write(&buf[2..5])?;
        n += sz;

        // Append each index entry for all blocks for this key
        let sz = entries.write_to(&mut self.w)?; // tsm writer write entries error
        n += sz as usize;

        self.key.clear();
        self.index_entries.as_mut().unwrap().typ = 0;
        self.index_entries.as_mut().unwrap().entries.clear();

        // If this is a disk based index and we've written more than the fsync threshold,
        // fsync the data to avoid long pauses later on.
        if self.size - self.last_sync > FSYNC_EVERY as u32 {
            self.w.flush()?;
            self.w.get_mut().sync_all()?;
            self.last_sync = self.size;
        }

        Ok(n as i64)
    }

    pub fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        self.write_to(&mut buf)?;
        Ok(buf)
    }

    pub fn reset(&mut self) {
        self.key_count = 0;
        self.size = 0;
        self.last_sync = 0;
        self.path = PathBuf::new();
        // self.w = None;
        self.key.clear();
        self.index_entries = None;
    }

    pub fn close(&mut self) -> Result<()> {
        // Flush anything remaining in the index
        self.w.flush()?;

        std::fs::remove_file(&self.path)?;

        Ok(())
    }

    /// Removes the index from any temporary storage
    pub fn remove(&mut self) -> Result<()> {
        // Close the file handle to prevent leaking.  We ignore the error because
        // we just want to cleanup and remove the file.

        // FIXME: donot need to flush and sync ?
        // self.w.flush()?;
        // let _ = self.w.get_mut().sync_all();

        std::fs::remove_file(&self.path)?;

        Ok(())
    }
}

impl SyncWrite for Vec<u8> {
    fn path(&self) -> &std::path::Path {
        std::path::Path::new("")
    }

    fn close(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_all(&self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<W: SyncWrite> SyncWrite for std::io::BufWriter<W> {
    fn path(&self) -> &std::path::Path {
        self.get_ref().path()
    }

    fn close(&self) -> std::io::Result<()> {
        self.get_ref().close()
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.get_ref().sync_all()
    }
}

/// TSMWriter writes keys and values in the TSM format
// pub struct TSMWriter<W: SyncWrite> {
pub struct TSMWriter<W: Write> {
    // wrapped: W,
    // w: BufWriter<W>,
    w: BufWriter<W>,
    generation: i64,
    seq: i64,
    path: PathBuf,

    index: TSMIndex,
    n: i64,

    // The bytes written count of when we last fsync'd
    last_sync: i64,
}

impl<W> TSMWriter<W>
where
    W: SyncWrite,
{
    /// Returns a new TSMWriter writing to w.
    pub fn new(w: W, generation: i64, seq: i64, path: impl AsRef<Path>) -> Self {
        // w always contains File
        let index = TSMIndex::InmemIndex(InmemIndex::new());
        Self {
            // wrapped: w,
            w: BufWriter::with_capacity(1024 * 1024, w),
            generation,
            seq,
            path: path.as_ref().to_path_buf(),
            index,
            n: 0,
            last_sync: 0,
        }
    }

    /// Returns a new TSMWriter writing to w and will use a disk
    /// based buffer for the TSM index if possible.
    pub fn with_disk_buffer(w: W, path: &Path, generation: i64, seq: i64) -> Result<Self> {
        let path = path.as_os_str().to_str().unwrap();
        let fname = match path.strip_suffix(".tsm.tmp") {
            Some(striped_path) => &format!("{}{}", striped_path, ".idx.tmp"),
            None => path,
        };

        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(fname)?;
        f.metadata()?.permissions().set_mode(0o666);
        let index = TSMIndex::DiskIndex(DiskIndex::new(f, fname));
        Ok(Self {
            // wrapped: w,
            w: BufWriter::new(w),
            generation,
            seq,
            path: PathBuf::from(fname),
            index,
            n: 0,
            last_sync: 0,
        })
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Returns the generation and sequence number.
    pub fn get_id(&self) -> (i64, i64) {
        (self.generation, self.seq)
    }

    pub fn get_ref(&self) -> &W {
        self.w.get_ref()
    }

    pub fn get_mut(&mut self) -> &mut W {
        self.w.get_mut()
    }

    // pub fn into_inner(self) -> Result<Box<dyn SyncWrite>> {
    //     self.w.into_inner()
    // }

    fn write_header(&mut self) -> Result<()> {
        let mut buf = [0u8; 5];
        (&mut buf[0..4]).put_u32(MAGIC_NUMBER);
        buf[4] = VERSION;

        let n = self.w.write(&buf)?;
        self.n = n as i64;
        Ok(())
    }

    /// Writes a new block containing key and values.
    pub fn write(&mut self, key: &[u8], values: &[Value]) -> Result<()> {
        ensure!(
            key.len() as i32 <= MAX_KEY_LENGTH,
            MaxKeyLengthExceededSnafu
        );

        // Nothing to write
        if values.is_empty() {
            return Ok(());
        }

        let (mut offset, mut size) = (0, 0);

        // Write header only after we have some data to write.
        if self.n == 0 {
            self.write_header()?;
        }
        offset = self.n;

        let block = Values::encode(values).context(EncodingValuesSnafu)?;
        let block_type = block_type(&block).context(EncodingSnafu)?;
        let statistics = BlockStatistics::from_values(values);

        let checksum = crc32fast::hash(&block).to_be_bytes();
        self.w.write_all(&checksum)?;
        size += checksum.len(); // checksum length

        self.w.write_all(&block)?;
        size += block.len();

        // Record this block in index
        self.index.add(
            key,
            block_type,
            values[0].unix_nano(),
            values[values.len() - 1].unix_nano(),
            offset,
            size as u32,
            statistics,
        );

        // Increment file position pointer
        self.n += size as i64;

        let entrys = self.index.entries(key);
        ensure!(
            (entrys.len() as i32) < MAX_INDEX_ENTRIES,
            MaxBlocksExceededSnafu {
                offset,
                size: size as i64,
            }
        );

        Ok(())
    }

    /// Writes block for the given key and time range to the TSM file.  If the write
    /// exceeds max entries for a given key, ErrMaxBlocksExceeded is returned.  This indicates
    /// that the index is now full for this key and no future writes to this key will succeed.
    pub fn write_block(
        &mut self,
        key: &[u8],
        min_time: i64,
        max_time: i64,
        block: &[u8],
    ) -> Result<(i64, i64)> {
        ensure!(
            key.len() as i32 <= MAX_KEY_LENGTH,
            MaxKeyLengthExceededSnafu
        );

        if block.is_empty() {
            return Ok((0, 0));
        }
        let values = encoding::decode_block(block).context(EncodingSnafu)?;
        let statistics = BlockStatistics::from_values(&values);
        self.write_block_with_statistics(key, min_time, max_time, block, statistics)
    }

    /// Writes an already encoded block and its statistics. The caller must
    /// calculate statistics from the exact logical values represented by
    /// `block` after duplicate and tombstone processing.
    pub fn write_block_with_statistics(
        &mut self,
        key: &[u8],
        min_time: i64,
        max_time: i64,
        block: &[u8],
        statistics: Option<BlockStatistics>,
    ) -> Result<(i64, i64)> {
        ensure!(
            key.len() as i32 <= MAX_KEY_LENGTH,
            MaxKeyLengthExceededSnafu
        );

        let (mut offset, mut size) = (0, 0);
        // Nothing to write
        if block.is_empty() {
            return Ok((0, 0));
        }

        let block_type = block_type(block).context(EncodingSnafu)?;
        ensure!(
            statistics.is_none_or(|statistics| statistics.supports_block_type(block_type)),
            StringErrSnafu {
                reason: format!("block statistics do not match encoded block type {block_type}"),
            }
        );

        // Write header only after we have some data to write.
        if self.n == 0 {
            self.write_header()?;
        }
        offset = self.n;

        let checksum = crc32fast::hash(block).to_be_bytes();
        self.w.write_all(&checksum)?;
        size += checksum.len(); // checksum length

        self.w.write_all(block)?;
        size += block.len();

        // Record this block in index
        self.index.add(
            key,
            block_type,
            min_time,
            max_time,
            offset,
            size as u32,
            statistics,
        );

        // Increment file position pointer (checksum + block len)
        self.n += size as i64;

        // fsync the file periodically to avoid long pauses with very big files.
        if self.n - self.last_sync > FSYNC_EVERY {
            self.sync()?;
            self.last_sync = self.n;
        }

        let entrys = self.index.entries(key);
        ensure!(
            (entrys.len() as i32) < MAX_INDEX_ENTRIES,
            MaxBlocksExceededSnafu {
                offset,
                size: size as i64,
            }
        );

        Ok((offset, size as i64))
    }

    /// Writes the index section of the file.
    /// If there are no index entries to write, this returns ErrNoValues.
    pub fn write_index(&mut self) -> Result<()> {
        let pos = self.n;

        ensure!(self.index.key_count() != 0, NoValuesSnafu);

        // Set the destination file on the index so we can periodically
        // fsync while writing the index.
        // if f, ok := t.wrapped.(syncer); ok {
        //     t.index.(*directIndex).f = f
        // }

        // Write the index
        self.index.write_to(&mut self.w)?;

        let mut buf = [0u8; 8];
        (&mut buf[0..8]).put_u64(pos as u64);

        // Write the index index position
        self.w.write_all(&buf)?;
        Ok(())
    }

    pub fn reset_index(&mut self) {
        self.index.reset()
    }

    pub fn flush(&mut self) -> Result<()> {
        self.w.flush()?;

        self.sync()
    }

    fn sync(&mut self) -> Result<()> {
        // self.w always contais File
        self.w.get_mut().sync_all()?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.flush()?;
        self.index.close()?;
        self.w.get_mut().close()?;

        Ok(())
    }

    pub fn remove(&mut self) -> Result<()> {
        self.index.remove()?;

        // Close the file handle to prevent leaking.  We ignore the error because
        // we just want to cleanup and remove the file.
        self.w.get_mut().close()?;
        std::fs::remove_file(self.path())?;

        Ok(())
    }

    pub fn size(&self) -> u32 {
        self.n as u32 + self.index.size()
    }
}

#[derive(Default, Clone)]
pub struct IndexEntries {
    pub typ: u8,
    pub entries: Vec<IndexEntry>,
}

impl IndexEntries {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_sorted(&self) -> bool {
        self.entries.is_sorted_by_key(|e| e.min_time)
    }

    pub fn sort(&mut self) {
        self.entries.sort_by_key(|e| e.min_time);
    }

    pub fn marshal_binary(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; self.entries.len() * INDEX_ENTRY_SIZE];

        for (i, entry) in self.entries.iter().enumerate() {
            entry.append_to(&mut buf[INDEX_ENTRY_SIZE * i..]);
        }

        Ok(buf)
    }

    pub fn write_to(&self, w: &mut impl Write) -> Result<i64> {
        let mut buf = vec![0u8; INDEX_ENTRY_SIZE];
        let mut n = 0;

        for entry in self.entries.iter() {
            entry.append_to(&mut buf);
            let sz = w.write(&buf)?;
            n += sz;
        }

        Ok(n as i64)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{Read, Seek, SeekFrom},
        sync::Arc,
    };

    use crate::models::points::{
        BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Value,
    };
    use bytes::Buf;
    use tempfile::{NamedTempFile, TempDir};

    use super::{Error, INDEX_ENTRY_SIZE, IndexEntry, TSMWriter, VERSION};
    use crate::storage::tsm1::{
        block_statistics::{BlockStatisticValue, BlockStatistics},
        cache::tests::{must_temp_tsm_dir, must_temp_tsm_file},
        data_encoding::encoding_values::Values,
        reader::TSMReader,
        writer::{MAGIC_NUMBER, SyncWrite},
    };

    #[test]
    fn test_tsm_writer_write_empty() {
        let b = Vec::<u8>::new();
        let mut w = TSMWriter::new(b, 1, 1, "");

        let got = w.write_index();
        assert!(got.is_err());
        assert_eq!(got.unwrap_err().to_string(), Error::NoValues.to_string());
    }

    #[test]
    fn test_tsm_writer_write_no_values() {
        let b = Vec::<u8>::new();
        let mut w = TSMWriter::new(b, 1, 1, "");

        w.write("foo".as_bytes(), &[]).unwrap();

        let got = w.write_index();
        assert!(got.is_err());
        assert_eq!(got.unwrap_err().to_string(), Error::NoValues.to_string());

        let got = w.get_ref().len();
        assert_eq!(0, got, "file size mismatch: exp {}, got {}", 0, got);
    }

    #[test]
    fn test_tsm_writer_write_single() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let values = vec![Value::FloatValue(FloatValue::new(0, 1.0))];

        w.write("cpu".as_bytes(), &values).unwrap();

        w.write_index().unwrap();

        w.close().unwrap();

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();

        let mut b = vec![];
        f.read_to_end(&mut b).unwrap();

        assert!(
            b.len() >= 5,
            "file size mismatch: exp {}, got {}",
            5,
            b.len()
        );

        let got = (&b[0..4]).get_u32();
        assert_eq!(
            MAGIC_NUMBER, got,
            "magic number mismatch: exp {}, got {}",
            MAGIC_NUMBER, got
        );
        assert_eq!(b[4], VERSION);
        assert_eq!(VERSION, 2);

        f.seek(std::io::SeekFrom::Start(0)).unwrap();

        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let read_values = r.read_all("cpu".as_bytes()).unwrap();

        assert!(
            read_values.len() == values.len(),
            "read values length mismatch: got {}, exp {}",
            read_values.len(),
            values.len()
        );

        for (i, v) in values.iter().enumerate() {
            if v.value() != read_values[i].value() {
                panic!(
                    "read value mismatch({}): got {:?}, exp {:?}",
                    i,
                    v.value(),
                    read_values[i].value()
                );
            }
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn block_statistics_survive_tsm_close_and_reopen() {
        let dir = must_temp_tsm_dir();
        let (file, path) = must_temp_tsm_file(dir.path()).keep().unwrap();
        let values = vec![
            Value::IntegerValue(IntegerValue::new(10, i64::MAX)),
            Value::IntegerValue(IntegerValue::new(20, 1)),
            Value::IntegerValue(IntegerValue::new(30, -4)),
        ];

        let mut writer = TSMWriter::new(file, 1, 1, "");
        writer.write(b"cpu#!~#value", &values).unwrap();
        writer.write_index().unwrap();
        writer.close().unwrap();

        let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        let reader = TSMReader::new(file, &path, 1, 1).unwrap();
        let entries = reader.entries(b"cpu#!~#value");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].statistics, BlockStatistics::from_values(&values));
        assert_eq!(
            entries[0].statistics.unwrap().value,
            BlockStatisticValue::Integer {
                sum: i64::MAX.wrapping_add(1).wrapping_add(-4),
                min: -4,
                max: i64::MAX,
                first: i64::MAX,
                last: -4,
            }
        );
    }

    #[test]
    fn block_statistics_round_trip_all_types_and_multiple_blocks() {
        let dir = must_temp_tsm_dir();
        let (file, path) = must_temp_tsm_file(dir.path()).keep().unwrap();
        let mut cases = vec![
            (
                b"cpu#!~#integer".as_slice(),
                vec![
                    vec![
                        Value::IntegerValue(IntegerValue::new(1, -3)),
                        Value::IntegerValue(IntegerValue::new(2, 7)),
                    ],
                    vec![
                        Value::IntegerValue(IntegerValue::new(3, 11)),
                        Value::IntegerValue(IntegerValue::new(4, 13)),
                    ],
                ],
            ),
            (
                b"cpu#!~#unsigned".as_slice(),
                vec![vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 2)),
                    Value::UnsignedValue(UnsignedValue::new(2, u64::MAX)),
                ]],
            ),
            (
                b"cpu#!~#float".as_slice(),
                vec![vec![
                    Value::FloatValue(FloatValue::new(1, -0.0)),
                    Value::FloatValue(FloatValue::new(2, 3.5)),
                    Value::FloatValue(FloatValue::new(3, -2.0)),
                ]],
            ),
            (
                b"cpu#!~#bool".as_slice(),
                vec![vec![
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(2, false)),
                ]],
            ),
            (
                b"cpu#!~#string".as_slice(),
                vec![vec![
                    Value::StringValue(StringValue::new(1, "a".to_string())),
                    Value::StringValue(StringValue::new(2, "z".to_string())),
                ]],
            ),
        ];
        cases.sort_by(|left, right| left.0.cmp(right.0));

        let mut writer = TSMWriter::new(file, 1, 1, "");
        for (key, blocks) in &cases {
            for values in blocks {
                writer.write(key, values).unwrap();
            }
        }
        writer.write_index().unwrap();
        writer.close().unwrap();

        let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        let reader = TSMReader::new(file, &path, 1, 1).unwrap();
        for (key, blocks) in cases {
            let entries = reader.entries(key);
            assert_eq!(entries.len(), blocks.len());
            for (entry, values) in entries.iter().zip(blocks) {
                assert_eq!(entry.statistics, BlockStatistics::from_values(&values));
            }
        }
    }

    #[test]
    fn index_entry_rejects_truncated_or_type_mismatched_statistics() {
        let mut entry = IndexEntry::default();
        let error = entry
            .unmarshal_binary(&[0; INDEX_ENTRY_SIZE - 1], crate::models::INTEGER)
            .unwrap_err();
        assert!(error.to_string().contains("short buf"));

        let source = IndexEntry {
            min_time: 1,
            max_time: 2,
            offset: 5,
            size: 8,
            statistics: BlockStatistics::from_integer([10, 20]),
        };
        let mut encoded = [0; INDEX_ENTRY_SIZE];
        source.append_to(&mut encoded);
        encoded[28] = 4;
        let error = entry
            .unmarshal_binary(&encoded, crate::models::INTEGER)
            .unwrap_err();
        assert!(error.to_string().contains("does not match block type"));

        encoded[28] = 2;
        encoded[29..37].fill(0);
        let error = entry
            .unmarshal_binary(&encoded, crate::models::INTEGER)
            .unwrap_err();
        assert!(error.to_string().contains("count must be non-zero"));

        encoded[29..37].copy_from_slice(&1_u64.to_be_bytes());
        encoded[45..53].copy_from_slice(&(20_i64 as u64).to_be_bytes());
        encoded[53..61].copy_from_slice(&(10_i64 as u64).to_be_bytes());
        let error = entry
            .unmarshal_binary(&encoded, crate::models::INTEGER)
            .unwrap_err();
        assert!(error.to_string().contains("minimum exceeds maximum"));
    }

    #[test]
    fn test_tsm_writer_write_multi() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        struct Data {
            key: &'static str,
            values: Vec<Value>,
        }

        let data = vec![
            Data {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            Data {
                key: "mem",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
        ];

        for d in &data {
            w.write(d.key.as_bytes(), &d.values).unwrap();
        }

        w.write_index().unwrap();

        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        for d in &data {
            let read_values = r.read_all(d.key.as_bytes()).unwrap();

            assert!(
                read_values.len() == d.values.len(),
                "read values length mismatch: got {}, exp {}",
                read_values.len(),
                d.values.len()
            );

            for (i, v) in d.values.iter().enumerate() {
                if v.value() != read_values[i].value() {
                    panic!(
                        "read value mismatch({}): got {:?}, exp {:?}",
                        i,
                        v.value(),
                        read_values[i].value()
                    );
                }
            }
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_writer_write_multi_key_values() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        struct Data {
            key: &'static str,
            values: Vec<Value>,
        }

        let data = vec![
            Data {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(0, 1.0)),
                    Value::FloatValue(FloatValue::new(1, 2.0)),
                ],
            },
            Data {
                key: "mem",
                values: vec![
                    Value::FloatValue(FloatValue::new(0, 1.5)),
                    Value::FloatValue(FloatValue::new(1, 2.5)),
                ],
            },
        ];

        for d in &data {
            w.write(d.key.as_bytes(), &d.values).unwrap();
        }

        w.write_index().unwrap();

        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        for d in &data {
            let read_values = r.read_all(d.key.as_bytes()).unwrap();

            assert!(
                read_values.len() == d.values.len(),
                "read values length mismatch: got {}, exp {}",
                read_values.len(),
                d.values.len()
            );

            for (i, v) in d.values.iter().enumerate() {
                if v.value() != read_values[i].value() {
                    panic!(
                        "read value mismatch({}): got {:?}, exp {:?}",
                        i,
                        v.value(),
                        read_values[i].value()
                    );
                }
            }
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    // Tests that writing keys in reverse is able to read them back.
    #[test]
    fn test_tsm_writer_write_same_key() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        struct Data {
            key: &'static str,
            values: Vec<Value>,
        }

        let data = vec![
            Data {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(0, 1.0)),
                    Value::FloatValue(FloatValue::new(1, 2.0)),
                ],
            },
            Data {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(2, 3.0)),
                    Value::FloatValue(FloatValue::new(3, 4.0)),
                ],
            },
        ];

        for d in &data {
            w.write(d.key.as_bytes(), &d.values).unwrap();
        }

        w.write_index().unwrap();

        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let mut values = Vec::from(data[0].values.clone());
        values.extend_from_slice(data[1].values.as_slice());

        let read_values = r.read_all("cpu".as_bytes()).unwrap();
        assert!(
            read_values.len() == values.len(),
            "read values length mismatch: got {}, exp {}",
            read_values.len(),
            values.len()
        );

        for (i, v) in values.iter().enumerate() {
            if v.value() != read_values[i].value() {
                panic!(
                    "read value mismatch({}): got {:?}, exp {:?}",
                    i,
                    v.value(),
                    read_values[i].value()
                );
            }
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    // Tests that calling Read returns all the values for block matching the key
    // and timestamp
    #[test]
    fn test_tsm_writer_read_multi() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        struct Data {
            key: &'static str,
            values: Vec<Value>,
        }

        let data = vec![
            Data {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(0, 1.0)),
                    Value::FloatValue(FloatValue::new(1, 2.0)),
                ],
            },
            Data {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(2, 3.0)),
                    Value::FloatValue(FloatValue::new(3, 4.0)),
                ],
            },
        ];

        for d in &data {
            w.write(d.key.as_bytes(), &d.values).unwrap();
        }

        w.write_index().unwrap();

        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        for d in &data {
            // Try the first timestamp
            let read_values = r.read("cpu".as_bytes(), d.values[0].unix_nano()).unwrap();

            assert!(
                read_values.len() == d.values.len(),
                "read values length mismatch: got {}, exp {}",
                read_values.len(),
                d.values.len()
            );

            for (i, v) in d.values.iter().enumerate() {
                if v.value() != read_values[i].value() {
                    panic!(
                        "read value mismatch({}): got {:?}, exp {:?}",
                        i,
                        v.value(),
                        read_values[i].value()
                    );
                }
            }
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_writer_write_block_empty() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        w.write_block("cpu".as_bytes(), 0, 0, &[]).unwrap();

        let err = w.write_index();
        assert_eq!(err.err().unwrap().to_string(), Error::NoValues.to_string());

        w.close().unwrap();

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let mut b = vec![];
        let _ = f.read_to_end(&mut b).unwrap();

        assert!(b.is_empty(), "file size mismatch: got {}, exp 0", b.len());

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_writer_write_block_multi() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");
        struct Data {
            key: &'static str,
            values: Vec<Value>,
        }

        let data = vec![
            Data {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            Data {
                key: "mem",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
        ];

        for d in &data {
            w.write(d.key.as_bytes(), &d.values).unwrap();
        }

        w.write_index().unwrap();

        w.close().unwrap();

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let mut b = vec![];
        let _ = f.read_to_end(&mut b).unwrap();

        assert!(b.len() >= 5, "file size mismatch: got {}, exp 5", b.len());

        let got = (&b.as_slice()[..4]).get_u32();
        assert_eq!(
            got, MAGIC_NUMBER,
            "magic number mismatch: got {}, exp {}",
            got, MAGIC_NUMBER
        );

        f.seek(SeekFrom::Start(0)).unwrap();

        // Create reader for that file
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        // Create new file in the same tmp dir.
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let itr = TSMReader::block_iter(Arc::clone(&r));
        for item in itr {
            w.write_block_with_statistics(
                item.key.as_slice(),
                item.min_time,
                item.max_time,
                item.block.as_slice(),
                item.statistics,
            )
            .unwrap();
        }

        w.write_index().unwrap();

        w.close().unwrap();

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();

        // Now create a reader to verify the written blocks matches the originally
        // written file using Write
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();
        for d in &data {
            // Try the first timestamp
            let read_values = r.read_all(d.key.as_bytes()).unwrap();
            assert!(
                read_values.len() == d.values.len(),
                "read values length mismatch: got {}, exp {}",
                read_values.len(),
                d.values.len()
            );

            for (i, v) in d.values.iter().enumerate() {
                if v.value() != read_values[i].value() {
                    panic!(
                        "read value mismatch({}): got {:?}, exp {:?}",
                        i,
                        v.value(),
                        read_values[i].value()
                    );
                }
            }
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_writer_write_block_max_key() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let key = "a".repeat(100000);

        let res = w.write_block(key.as_bytes(), 0, 0, &[]);
        assert_eq!(
            res.err().unwrap().to_string(),
            Error::MaxKeyLengthExceeded.to_string()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_writer_write_max_key() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let key = "a".repeat(100000);

        let res = w.write(
            key.as_bytes(),
            &[Value::FloatValue(FloatValue::new(0, 1.0))],
        );
        assert_eq!(
            res.err().unwrap().to_string(),
            Error::MaxKeyLengthExceeded.to_string()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }
}
