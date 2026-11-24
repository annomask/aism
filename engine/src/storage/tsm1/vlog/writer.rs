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
        data_encoding::{encoding::block_type, encoding_values::Values},
        vlog::compact::MAX_VLOG_FILE_SIZE,
    },
};

// MagicNumber is written as the first 4 bytes of a data file to
// identify the file as a tsm1 formatted file
pub const MAGIC_NUMBER: u32 = 0x16D116D1;

// Version indicates the version of the TSM file format.
pub const VERSION: u8 = 1;

// Size in bytes of an index entry
pub const INDEX_ENTRY_SIZE: usize = 28;

// Size in bytes used to store the type of block encoded
pub const INDEX_TYPE_SIZE: usize = 1;

// max length of a key in an index entry (measurement + tags)
pub const MAX_KEY_LENGTH: i32 = (1 << (2 * 8)) - 1;

// The threshold amount data written before we periodically fsync a TSM file.  This helps avoid
// long pauses due to very large fsyncs at the end of writing a TSM file.
pub const FSYNC_EVERY: i64 = 25 * 1024 * 1024;

/// Error type for tsm writer.
#[derive(Debug, Snafu)]
pub enum Error {
    /// NoValues is returned when VLogWriter.WriteIndex is called and there are no values to write.
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

/// VLogWriter writes keys and values in the TSM format
// pub struct VLogWriter<W: SyncWrite> {
pub struct VLogWriter<W: Write> {
    // wrapped: W,
    // w: BufWriter<W>,
    w: BufWriter<W>,
    generation: i64,
    seq: i64,
    path: PathBuf,

    n: i64,

    block_count: i32, // block count writen to file

    // The bytes written count of when we last fsync'd
    last_sync: i64,
}

impl<W> VLogWriter<W>
where
    W: SyncWrite,
{
    /// Returns a new VLogWriter writing to w.
    pub fn new(w: W, generation: i64, seq: i64, path: impl AsRef<Path>) -> Self {
        // w always contains File
        Self {
            // wrapped: w,
            w: BufWriter::with_capacity(1024 * 1024, w),
            generation,
            seq,
            path: path.as_ref().to_path_buf(),

            block_count: 0,

            n: 0,
            last_sync: 0,
        }
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

        let checksum = crc32fast::hash(&block).to_be_bytes();
        self.w.write_all(&checksum)?;
        size += checksum.len(); // checksum length

        self.w.write_all(&block)?;
        size += block.len();

        // Increment file position pointer
        self.n += size as i64;

        self.block_count += 1;

        ensure!(
            self.n < MAX_VLOG_FILE_SIZE,
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

        let (mut offset, mut size) = (0, 0);
        // Nothing to write
        if block.is_empty() {
            return Ok((0, 0));
        }

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

        // Increment file position pointer (checksum + block len)
        self.n += size as i64;

        // fsync the file periodically to avoid long pauses with very big files.
        if self.n - self.last_sync > FSYNC_EVERY {
            self.sync()?;
            self.last_sync = self.n;
        }

        self.block_count += 1;

        ensure!(
            self.n < MAX_VLOG_FILE_SIZE,
            MaxBlocksExceededSnafu {
                offset,
                size: size as i64,
            }
        );

        Ok((offset, size as i64))
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
        self.w.get_mut().close()?;

        Ok(())
    }

    pub fn remove(&mut self) -> Result<()> {
        // Close the file handle to prevent leaking.  We ignore the error because
        // we just want to cleanup and remove the file.
        self.w.get_mut().close()?;
        std::fs::remove_file(self.path())?;

        Ok(())
    }

    pub fn size(&self) -> u32 {
        self.n as u32
    }
}
