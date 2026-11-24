use super::{
    LEVEL_DIRTY, Level, MAX_LEVELS, Result, encode as enc, invalid, page_group::valid_type,
};
use crate::models::FieldType;

pub(crate) const LEVEL_HEADER_SIZE: usize = 32;

pub(super) fn level_bit(level: Level) -> Result<u8> {
    match level {
        1..=MAX_LEVELS => Ok(1 << (level - 1)),
        LEVEL_DIRTY => Ok(0x80),
        _ => Err(invalid("unsupported aggregate level")),
    }
}
pub(super) fn levels_from_bits(bits: u8) -> Result<Vec<Level>> {
    if bits == 0 || bits & 0x60 != 0 {
        return Err(invalid("invalid level bitmap"));
    }
    let mut levels = Vec::new();
    if bits & 0x80 != 0 {
        levels.push(LEVEL_DIRTY);
    }
    for level in (1..=MAX_LEVELS).rev() {
        if bits & level_bit(level)? != 0 {
            levels.push(level);
        }
    }
    Ok(levels)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LevelHeader {
    pub(crate) count: u32,
    pub(crate) level_offset: u32,
    pub(crate) group_count: u32,
    /// Byte offset of the group table relative to the start of this level.
    pub(crate) table_offset: u32,
    pub(crate) min_start: i64,
    pub(crate) max_end: i64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentHeader {
    pub(crate) typ: FieldType,
    pub(crate) flags: u8,
    pub(crate) level_bits: u8,
    pub(crate) levels: Vec<(Level, LevelHeader)>,
}
impl SegmentHeader {
    pub(crate) fn size(&self) -> usize {
        8 + LEVEL_HEADER_SIZE * self.levels.len()
    }
    pub(crate) fn level(&self, level: Level) -> Option<&LevelHeader> {
        self.levels
            .iter()
            .find(|(l, _)| *l == level)
            .map(|(_, h)| h)
    }
    pub(crate) fn validate(&self, seg_len: u32) -> Result<()> {
        let levels = levels_from_bits(self.level_bits)?;
        if !valid_type(self.typ) || self.flags & !1 != 0 || levels.len() != self.levels.len() {
            return Err(invalid("invalid segment header"));
        }
        let mut previous = self.size() as u32;
        for (i, ((level, h), expected)) in self.levels.iter().zip(levels).enumerate() {
            if *level != expected
                || h.count == 0
                || h.group_count == 0
                || h.group_count > h.count
                || u64::from(h.count) > u64::from(h.group_count) * super::PAGE_GROUP as u64
                || h.min_start > h.max_end
                || h.level_offset < previous
                || h.level_offset >= seg_len
                || (i > 0 && h.level_offset == previous)
            {
                return Err(invalid("invalid level header"));
            }
            let end = self
                .levels
                .get(i + 1)
                .map_or(seg_len, |(_, next)| next.level_offset);
            if h.table_offset == 0
                || u64::from(h.level_offset)
                    + u64::from(h.table_offset)
                    + u64::from(h.group_count) * super::page_group::PageGroupEntry::SIZE as u64
                    != u64::from(end)
            {
                return Err(invalid("invalid level group table bounds"));
            }
            previous = h.level_offset;
        }
        Ok(())
    }
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&[self.typ, self.flags, self.level_bits, 0, 0, 0, 0, 0]);
        for (_, h) in &self.levels {
            out.extend_from_slice(&h.count.to_be_bytes());
            out.extend_from_slice(&h.level_offset.to_be_bytes());
            out.extend_from_slice(&h.group_count.to_be_bytes());
            out.extend_from_slice(&h.table_offset.to_be_bytes());
            out.extend_from_slice(&h.min_start.to_be_bytes());
            out.extend_from_slice(&h.max_end.to_be_bytes());
        }
    }
    pub(crate) fn decode(mut bytes: &[u8], seg_len: u32) -> Result<Self> {
        let fixed = enc::take(&mut bytes, 8)?;
        if fixed[3..].iter().any(|b| *b != 0) {
            return Err(invalid("nonzero segment reserved bytes"));
        }
        let mut header = Self {
            typ: fixed[0],
            flags: fixed[1],
            level_bits: fixed[2],
            levels: Vec::new(),
        };
        for level in levels_from_bits(header.level_bits)? {
            header.levels.push((
                level,
                LevelHeader {
                    count: enc::u32(&mut bytes)?,
                    level_offset: enc::u32(&mut bytes)?,
                    group_count: enc::u32(&mut bytes)?,
                    table_offset: enc::u32(&mut bytes)?,
                    min_start: enc::i64(&mut bytes)?,
                    max_end: enc::i64(&mut bytes)?,
                },
            ));
        }
        if !bytes.is_empty() {
            return Err(invalid("trailing segment header bytes"));
        }
        header.validate(seg_len)?;
        Ok(header)
    }
}
