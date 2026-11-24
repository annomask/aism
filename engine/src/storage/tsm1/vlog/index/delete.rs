//! Legacy cross-file journal reader and bounded staging-row codec.

use super::reader::pread;
use super::{Result, SeqNo, invalid};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};
#[cfg(test)]
use std::{fs::OpenOptions, io::Write};

const HEADER: usize = 16;
pub(super) const ROW: usize = 32;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DeleteRange {
    pub(crate) seq: SeqNo,
    pub(crate) min: i64,
    pub(crate) max: i64,
    pub(crate) cutoff: u32,
}

pub(super) fn encode_row(range: DeleteRange) -> [u8; ROW] {
    let mut bytes = [0; ROW];
    bytes[..8].copy_from_slice(&range.seq.get().to_be_bytes());
    bytes[8..16].copy_from_slice(&range.min.to_be_bytes());
    bytes[16..24].copy_from_slice(&range.max.to_be_bytes());
    bytes[24..28].copy_from_slice(&range.cutoff.to_be_bytes());
    let crc = crc32fast::hash(&bytes[..28]);
    bytes[28..].copy_from_slice(&crc.to_be_bytes());
    bytes
}

pub(super) fn decode_row(bytes: &[u8]) -> Result<DeleteRange> {
    if bytes.len() != ROW || crc32fast::hash(&bytes[..28]).to_be_bytes() != bytes[28..] {
        return Err(invalid("delete row checksum mismatch"));
    }
    let row = DeleteRange {
        seq: SeqNo::from_raw(u64::from_be_bytes(bytes[..8].try_into().unwrap()))?,
        min: i64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        max: i64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        cutoff: u32::from_be_bytes(bytes[24..28].try_into().unwrap()),
    };
    if row.min > row.max {
        return Err(invalid("reversed delete range"));
    }
    Ok(row)
}

pub(crate) struct DeleteFile {
    pub(crate) path: PathBuf,
    file: File,
    count: u64,
}
impl DeleteFile {
    #[cfg(test)]
    pub(crate) fn write(
        path: &Path,
        batch: &super::delete_batch::DeleteBatch,
        cutoff: u32,
    ) -> Result<()> {
        if batch.len() == 0 {
            return Err(invalid("empty delete file"));
        }
        let mut file =
            std::io::BufWriter::new(OpenOptions::new().write(true).create_new(true).open(path)?);
        let mut header = Vec::from(*b"AGGD");
        header.extend_from_slice(&batch.len().to_be_bytes());
        header.extend_from_slice(&crc32fast::hash(&header).to_be_bytes());
        file.write_all(&header)?;
        let mut previous = None;
        for range in batch.rows() {
            let mut range = range?;
            range.cutoff = cutoff;
            if range.min > range.max || previous.is_some_and(|seq| seq > range.seq) {
                return Err(invalid("invalid delete range order"));
            }
            previous = Some(range.seq);
            file.write_all(&encode_row(range))?;
        }
        file.flush()?;
        file.get_ref().sync_all()?;
        Ok(())
    }
    pub(crate) fn open(path: &Path) -> Result<Arc<Self>> {
        let file = File::open(path)?;
        let mut bytes = [0; HEADER];
        pread(&file, 0, &mut bytes)?;
        if &bytes[..4] != b"AGGD" || crc32fast::hash(&bytes[..12]).to_be_bytes() != bytes[12..] {
            return Err(invalid("invalid delete header"));
        }
        let count = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
        if count == 0
            || count
                .checked_mul(ROW as u64)
                .and_then(|n| n.checked_add(HEADER as u64))
                != Some(file.metadata()?.len())
        {
            return Err(invalid("invalid delete file length"));
        }
        let result = Self {
            path: path.into(),
            file,
            count,
        };
        let mut previous = None;
        for i in 0..count {
            let row = result.row(i)?;
            if previous.is_some_and(|seq| seq > row.seq) {
                return Err(invalid("unsorted delete file"));
            }
            previous = Some(row.seq);
        }
        Ok(Arc::new(result))
    }
    fn row(&self, index: u64) -> Result<DeleteRange> {
        let mut bytes = [0; ROW];
        pread(&self.file, HEADER as u64 + index * ROW as u64, &mut bytes)?;
        decode_row(&bytes)
    }

    /// Iterate deletion candidates with one small IO page in flight.
    pub(crate) fn pages(&self) -> impl Iterator<Item = Result<Vec<DeleteRange>>> + '_ {
        (0..self.count).step_by(256).map(|start| {
            let count = (self.count - start).min(256) as usize;
            let mut bytes = vec![0; count * ROW];
            pread(&self.file, HEADER as u64 + start * ROW as u64, &mut bytes)?;
            bytes.chunks_exact(ROW).map(decode_row).collect()
        })
    }
    #[cfg(test)]
    pub(crate) fn lookup(&self, seq: SeqNo) -> Result<Vec<DeleteRange>> {
        let (mut lo, mut hi) = (0, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.row(mid)?.seq < seq {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut result = Vec::new();
        while lo < self.count {
            let row = self.row(lo)?;
            if row.seq != seq {
                break;
            }
            result.push(row);
            lo += 1;
        }
        Ok(result)
    }
}
