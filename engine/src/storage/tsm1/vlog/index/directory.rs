use super::{
    Level, Result, SeqNo, encode as enc, invalid, page_group::valid_type, segment::levels_from_bits,
};
use crate::models::FieldType;

pub(crate) const PAGE: usize = 4096;
pub(crate) const DIR_ENTRY_SIZE: usize = 40;
pub(crate) const DIR_PER_PAGE: usize = PAGE / DIR_ENTRY_SIZE;
pub(crate) const INDEX_PER_PAGE: usize = PAGE / 8;
pub(crate) const HEADER_SIZE: usize = 32;
pub(crate) const FOOTER_SIZE: usize = 80;
pub(crate) const MAGIC: [u8; 4] = *b"AGG5";
pub(crate) const FORMAT_VERSION: u16 = IndexFormat::CURRENT.version();
pub(crate) const MAX_INDEX_FILE_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Recognized file formats, separate from the shard directory's mode marker.
/// New versions must explicitly declare their supported reader capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexFormat {
    V2,
    V3,
    V4,
}

impl IndexFormat {
    pub(crate) const CURRENT: Self = Self::V4;

    pub(crate) const fn version(self) -> u16 {
        match self {
            Self::V2 => 2,
            Self::V3 => 3,
            Self::V4 => 4,
        }
    }

    pub(crate) fn from_version(version: u16) -> Result<Self> {
        match version {
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            4 => Ok(Self::V4),
            _ => Err(invalid(
                "unsupported aggregate index version; explicit migration is required",
            )),
        }
    }

    pub(crate) fn supports_column_checksums(self) -> bool {
        match self {
            Self::V2 | Self::V3 => false,
            Self::V4 => true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirEntry {
    pub(crate) seq_no: SeqNo,
    pub(crate) seg_offset: u64,
    pub(crate) seg_len: u32,
    pub(crate) typ: FieldType,
    pub(crate) top_level: Level,
    pub(crate) level_bits: u8,
    pub(crate) min_start: i64,
    pub(crate) max_end: i64,
}
impl DirEntry {
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.seq_no.get().to_be_bytes());
        out.extend_from_slice(&self.seg_offset.to_be_bytes());
        out.extend_from_slice(&self.seg_len.to_be_bytes());
        out.extend_from_slice(&[self.typ, self.top_level, self.level_bits, 0]);
        out.extend_from_slice(&self.min_start.to_be_bytes());
        out.extend_from_slice(&self.max_end.to_be_bytes());
    }
    pub(crate) fn decode(mut bytes: &[u8], index_end: u64) -> Result<Self> {
        if bytes.len() != DIR_ENTRY_SIZE {
            return Err(invalid("directory entry size mismatch"));
        }
        let seq_no = SeqNo::from_raw(enc::u64(&mut bytes)?)?;
        let seg_offset = enc::u64(&mut bytes)?;
        let seg_len = enc::u32(&mut bytes)?;
        let flags = enc::take(&mut bytes, 4)?;
        let min_start = enc::i64(&mut bytes)?;
        let max_end = enc::i64(&mut bytes)?;
        if !valid_type(flags[0])
            || flags[3] != 0
            || levels_from_bits(flags[2])?[0] != flags[1]
            || min_start > max_end
            || seg_offset < HEADER_SIZE as u64
            || seg_len < 32
            || seg_offset
                .checked_add(u64::from(seg_len))
                .is_none_or(|end| end > index_end)
        {
            return Err(invalid("invalid directory entry"));
        }
        Ok(Self {
            seq_no,
            seg_offset,
            seg_len,
            typ: flags[0],
            top_level: flags[1],
            level_bits: flags[2],
            min_start,
            max_end,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TopIndex(pub(crate) Vec<u64>);
impl TopIndex {
    pub(crate) fn locate(&self, seq_no: SeqNo) -> Option<usize> {
        self.0
            .partition_point(|s| *s <= seq_no.get())
            .checked_sub(1)
    }
    pub(crate) fn encode(&self) -> Vec<u8> {
        self.0.iter().flat_map(|v| v.to_be_bytes()).collect()
    }
    pub(crate) fn decode(mut bytes: &[u8], footer: &Footer) -> Result<Self> {
        let count = footer.directory_pages().div_ceil(INDEX_PER_PAGE);
        if bytes.len() != count * 8 {
            return Err(invalid("top index length mismatch"));
        }
        let mut values = Vec::with_capacity(count);
        while !bytes.is_empty() {
            let v = enc::u64(&mut bytes)?;
            SeqNo::from_raw(v)?;
            values.push(v);
        }
        if values.first() != Some(&footer.min_seq_no)
            || values.last().is_none_or(|s| *s > footer.max_seq_no)
            || values.windows(2).any(|p| p[0] >= p[1])
        {
            return Err(invalid("invalid top index ordering"));
        }
        Ok(Self(values))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Footer {
    pub(crate) index_offset: u64,
    pub(crate) page_index_offset: u64,
    pub(crate) dir_offset: u64,
    pub(crate) top_index_offset: u64,
    pub(crate) min_seq_no: u64,
    pub(crate) max_seq_no: u64,
    pub(crate) series_count: u32,
    pub(crate) flags: u16,
}
impl Footer {
    pub(crate) fn directory_pages(&self) -> usize {
        (self.series_count as usize).div_ceil(DIR_PER_PAGE)
    }
    pub(crate) fn directory_len(count: usize) -> usize {
        if count == 0 {
            return 0;
        }
        let full = (count - 1) / DIR_PER_PAGE;
        full * PAGE + (count - full * DIR_PER_PAGE) * DIR_ENTRY_SIZE
    }
    pub(crate) fn directory_bytes(count: usize) -> u64 {
        let pages = count.div_ceil(DIR_PER_PAGE);
        (pages * 8 + Self::directory_len(count) + pages.div_ceil(INDEX_PER_PAGE) * 8 + FOOTER_SIZE)
            as u64
    }
    pub(crate) fn encode(&self, top: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(FOOTER_SIZE);
        for v in [
            self.index_offset,
            self.page_index_offset,
            self.dir_offset,
            self.top_index_offset,
            self.min_seq_no,
            self.max_seq_no,
        ] {
            out.extend_from_slice(&v.to_be_bytes());
        }
        out.extend_from_slice(&self.series_count.to_be_bytes());
        out.extend_from_slice(&self.flags.to_be_bytes());
        out.extend_from_slice(&[0; 18]);
        let mut crc = crc32fast::Hasher::new();
        crc.update(&out);
        crc.update(top);
        out.extend_from_slice(&crc.finalize().to_be_bytes());
        out.extend_from_slice(&MAGIC);
        out
    }
    pub(crate) fn decode(bytes: &[u8], file_len: u64) -> Result<Self> {
        if bytes.len() != FOOTER_SIZE || bytes[76..] != MAGIC {
            return Err(invalid("invalid footer magic"));
        }
        let mut input = bytes;
        let footer = Self {
            index_offset: enc::u64(&mut input)?,
            page_index_offset: enc::u64(&mut input)?,
            dir_offset: enc::u64(&mut input)?,
            top_index_offset: enc::u64(&mut input)?,
            min_seq_no: enc::u64(&mut input)?,
            max_seq_no: enc::u64(&mut input)?,
            series_count: enc::u32(&mut input)?,
            flags: enc::u16(&mut input)?,
        };
        SeqNo::from_raw(footer.min_seq_no)?;
        SeqNo::from_raw(footer.max_seq_no)?;
        if footer.index_offset != HEADER_SIZE as u64
            || footer.page_index_offset <= footer.index_offset
            || footer.series_count == 0
            || footer.flags != 0
            || footer.min_seq_no > footer.max_seq_no
            || enc::take(&mut input, 18)?.iter().any(|b| *b != 0)
            || file_len > MAX_INDEX_FILE_SIZE
        {
            return Err(invalid("invalid footer fields"));
        }
        let pages = footer.directory_pages();
        if footer.page_index_offset.checked_add((pages * 8) as u64) != Some(footer.dir_offset)
            || footer
                .dir_offset
                .checked_add(Self::directory_len(footer.series_count as usize) as u64)
                != Some(footer.top_index_offset)
            || footer
                .top_index_offset
                .checked_add((pages.div_ceil(INDEX_PER_PAGE) * 8 + FOOTER_SIZE) as u64)
                != Some(file_len)
        {
            return Err(invalid("invalid directory boundaries"));
        }
        Ok(footer)
    }
    pub(crate) fn verify_checksum(bytes: &[u8], top: &[u8]) -> Result<()> {
        if bytes.len() != FOOTER_SIZE {
            return Err(invalid("invalid footer size"));
        }
        let mut crc = crc32fast::Hasher::new();
        crc.update(&bytes[..72]);
        crc.update(top);
        if crc.finalize() != u32::from_be_bytes(bytes[72..76].try_into().unwrap()) {
            return Err(invalid("footer checksum mismatch"));
        }
        Ok(())
    }
}
