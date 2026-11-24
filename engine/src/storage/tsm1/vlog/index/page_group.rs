pub(crate) use super::column_layout::{ALL_COLUMNS, Column};
use super::column_layout::{COLUMN_COUNT, ColumnReadPlan, PART_CHECKSUM_SIZE, PART_COUNT};
use super::{
    AggScalar, AggSum, AggValues, BlockLoc, FAN_OUT, LEVEL_BLOCK, LEVEL_DIRTY, Level, MAX_LEVELS,
    MIN_CHILDREN, PAGE_GROUP, Position, Record, Result, SeqNo, encode as enc, invalid,
};
use crate::models::{BOOL, FLOAT, FieldType, INTEGER, STRING, UNSIGNED};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ColumnValue {
    Count(u64),
    Sum(AggSum),
    Float(f64),
    Scalar(AggScalar),
    Time(i64),
}

/// Absent columns stay absent rather than becoming fabricated zero aggregates.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SelectedRecord {
    pub(crate) seq_no: SeqNo,
    pub(crate) level: Level,
    pub(crate) pos: Position,
    pub(crate) values: [Option<ColumnValue>; COLUMN_COUNT],
}
impl SelectedRecord {
    pub(crate) fn into_record(self) -> Result<Record> {
        use ColumnValue::*;
        let [
            Some(Count(count)),
            Some(Sum(sum)),
            Some(Float(sumsq)),
            Some(Scalar(min)),
            Some(Time(min_at)),
            Some(Scalar(max)),
            Some(Time(max_at)),
            Some(Scalar(first)),
            Some(Scalar(last)),
        ] = self.values
        else {
            return Err(invalid("incomplete aggregate projection"));
        };
        Ok(Record {
            seq_no: self.seq_no,
            level: self.level,
            pos: self.pos,
            values: AggValues {
                count,
                sum,
                sumsq,
                min,
                min_at,
                max,
                max_at,
                first,
                last,
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageGroupEntry {
    pub(crate) first_start: i64,
    pub(crate) last_end: i64,
    pub(crate) group_offset: u32,
    pub(crate) first_row: u32,
    pub(crate) row_count: u16,
    pub(crate) position_len: u16,
    pub(crate) column_lens: [u16; COLUMN_COUNT],
    pub(crate) payload_crc: u32,
    pub(crate) column_checksums: bool,
}
impl PageGroupEntry {
    // Fixed v2-v4 table offsets, independent of in-memory structure layout.
    pub(crate) const SIZE: usize = 64;
    const CHECKSUM_FLAG_OFFSET: usize = 46;
    const TABLE_CRC_OFFSET: usize = 52;
    const RESERVED_OFFSET: usize = 56;
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        let start = out.len();
        out.extend_from_slice(&self.first_start.to_be_bytes());
        out.extend_from_slice(&self.last_end.to_be_bytes());
        out.extend_from_slice(&self.group_offset.to_be_bytes());
        out.extend_from_slice(&self.first_row.to_be_bytes());
        out.extend_from_slice(&self.row_count.to_be_bytes());
        out.extend_from_slice(&self.position_len.to_be_bytes());
        for len in self.column_lens {
            out.extend_from_slice(&len.to_be_bytes());
        }
        out.extend_from_slice(&[u8::from(self.column_checksums), 0]);
        out.extend_from_slice(&self.payload_crc.to_be_bytes());
        let crc = crc32fast::hash(&out[start..]);
        out.extend_from_slice(&crc.to_be_bytes());
        out.extend_from_slice(&[0; 8]);
    }
    pub(crate) fn decode(mut input: &[u8]) -> Result<Self> {
        if input.len() != Self::SIZE {
            return Err(invalid("invalid group entry size"));
        }
        if crc32fast::hash(&input[..Self::TABLE_CRC_OFFSET])
            != u32::from_be_bytes(
                input[Self::TABLE_CRC_OFFSET..Self::RESERVED_OFFSET]
                    .try_into()
                    .unwrap(),
            )
            || input[Self::CHECKSUM_FLAG_OFFSET] > 1
            || input[Self::CHECKSUM_FLAG_OFFSET + 1] != 0
            || input[Self::RESERVED_OFFSET..].iter().any(|byte| *byte != 0)
        {
            return Err(invalid("invalid group entry checksum or reserved bytes"));
        }
        let first_start = enc::i64(&mut input)?;
        let last_end = enc::i64(&mut input)?;
        let group_offset = enc::u32(&mut input)?;
        let first_row = enc::u32(&mut input)?;
        let row_count = enc::u16(&mut input)?;
        let position_len = enc::u16(&mut input)?;
        let mut column_lens = [0; COLUMN_COUNT];
        for len in &mut column_lens {
            *len = enc::u16(&mut input)?;
        }
        let column_checksums = enc::take(&mut input, 2)?[0] == 1;
        let payload_crc = enc::u32(&mut input)?;
        let checksum_len = if column_checksums {
            PART_CHECKSUM_SIZE as u16
        } else {
            0
        };
        if position_len < 10 + checksum_len
            || column_lens.iter().any(|len| *len <= checksum_len)
            || first_start > last_end
            || !(1..=PAGE_GROUP).contains(&(row_count as usize))
            || first_row.checked_add(u32::from(row_count)).is_none()
        {
            return Err(invalid("invalid group columns, range, or row count"));
        }
        Ok(Self {
            first_start,
            last_end,
            group_offset,
            first_row,
            row_count,
            position_len,
            column_lens,
            payload_crc,
            column_checksums,
        })
    }
    pub(crate) fn byte_len(&self, cols: &[Column]) -> usize {
        let end = cols.iter().map(|c| *c as usize + 1).max().unwrap_or(0);
        self.position_len as usize
            + self.column_lens[..end]
                .iter()
                .map(|n| *n as usize)
                .sum::<usize>()
    }
    pub(crate) fn read_range(&self, level_offset: u64, cols: &[Column]) -> Result<(u64, usize)> {
        Ok((
            level_offset
                .checked_add(u64::from(self.group_offset))
                .ok_or(invalid("group offset overflow"))?,
            self.byte_len(cols),
        ))
    }
    pub(crate) fn verify_payload(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() != self.byte_len(&ALL_COLUMNS) || crc32fast::hash(bytes) != self.payload_crc
        {
            return Err(invalid("page group payload checksum mismatch"));
        }
        Ok(())
    }
    pub(crate) fn part_lens(&self) -> [usize; PART_COUNT] {
        std::array::from_fn(|i| {
            if i == 0 {
                self.position_len as usize
            } else {
                self.column_lens[i - 1] as usize
            }
        })
    }
    pub(super) fn read_plan(&self, columns: &[Column]) -> ColumnReadPlan {
        ColumnReadPlan::new(self.position_len, &self.column_lens, columns)
    }
}

fn checksum_part(bytes: &[u8]) -> Vec<u8> {
    let mut out = crc32fast::hash(bytes).to_be_bytes().to_vec();
    out.extend_from_slice(bytes);
    out
}

fn verify_part(bytes: &[u8], checksummed: bool) -> Result<&[u8]> {
    if !checksummed {
        return Ok(bytes);
    }
    if bytes.len() < PART_CHECKSUM_SIZE
        || crc32fast::hash(&bytes[PART_CHECKSUM_SIZE..])
            != u32::from_be_bytes(bytes[..PART_CHECKSUM_SIZE].try_into().unwrap())
    {
        return Err(invalid("index column checksum mismatch"));
    }
    Ok(&bytes[PART_CHECKSUM_SIZE..])
}

/// A verified compressed group. Relocation changes only its external table entry.
/// Construction is restricted to this index implementation.
pub(crate) struct EncodedPageGroup {
    pub(super) seq_no: SeqNo,
    pub(super) level: Level,
    pub(super) typ: FieldType,
    pub(super) entry: PageGroupEntry,
    pub(super) bytes: Vec<u8>,
}
impl EncodedPageGroup {
    /// Add column checksums to verified legacy bytes without decoding codecs.
    pub(super) fn with_column_checksums(mut self) -> Result<Self> {
        if self.entry.column_checksums {
            return Ok(self);
        }
        self.entry.verify_payload(&self.bytes)?;
        let mut input = self.bytes.as_slice();
        let mut out = Vec::with_capacity(self.bytes.len() + PART_COUNT * PART_CHECKSUM_SIZE);
        for len in self.entry.part_lens() {
            out.extend(checksum_part(enc::take(&mut input, len)?));
        }
        self.entry.position_len = self
            .entry
            .position_len
            .checked_add(PART_CHECKSUM_SIZE as u16)
            .ok_or(invalid("position checksum exceeds length limit"))?;
        for len in &mut self.entry.column_lens {
            *len = len
                .checked_add(PART_CHECKSUM_SIZE as u16)
                .ok_or(invalid("column checksum exceeds length limit"))?;
        }
        self.entry.column_checksums = true;
        self.entry.payload_crc = crc32fast::hash(&out);
        self.bytes = out;
        Ok(self)
    }
    #[cfg(test)]
    pub(super) fn without_column_checksums(mut self) -> Result<Self> {
        if !self.entry.column_checksums {
            return Ok(self);
        }
        self.entry.verify_payload(&self.bytes)?;
        let mut input = self.bytes.as_slice();
        let mut out = Vec::new();
        for len in self.entry.part_lens() {
            out.extend_from_slice(verify_part(enc::take(&mut input, len)?, true)?);
        }
        self.entry.position_len -= PART_CHECKSUM_SIZE as u16;
        for len in &mut self.entry.column_lens {
            *len -= PART_CHECKSUM_SIZE as u16;
        }
        self.entry.column_checksums = false;
        self.entry.payload_crc = crc32fast::hash(&out);
        self.bytes = out;
        Ok(self)
    }
}

pub(super) fn valid_type(typ: FieldType) -> bool {
    matches!(typ, INTEGER | UNSIGNED | FLOAT | BOOL | STRING)
}
fn scalar_type(value: AggScalar) -> FieldType {
    match value {
        AggScalar::Int(_) => INTEGER,
        AggScalar::Uint(_) => UNSIGNED,
        AggScalar::Float(_) => FLOAT,
        AggScalar::Bool(_) => BOOL,
        AggScalar::None => STRING,
    }
}
pub(super) fn validate_record(record: &Record, typ: FieldType) -> Result<()> {
    let v = record.values;
    if !valid_type(typ) || record.pos.start > record.pos.end || record.pos.flags & !1 != 0 {
        return Err(invalid("invalid record position"));
    }
    validate_children(record.level, record.pos.child_count)?;
    if record.level == LEVEL_DIRTY {
        if record.pos.block != BlockLoc::default() || record.pos.block_size != 0 || v.count != 0 {
            return Err(invalid("invalid dirty record"));
        }
    } else {
        if !(1..=MAX_LEVELS).contains(&record.level) || v.count == 0 {
            return Err(invalid("invalid record level or count"));
        }
        if record.level == LEVEL_BLOCK {
            record.to_vlog_index()?;
        } else if record.pos.block != BlockLoc::default() || record.pos.version != 0 {
            return Err(invalid("summary has a block location"));
        }
        if typ != STRING
            && (!(record.pos.start..=record.pos.end).contains(&v.min_at)
                || !(record.pos.start..=record.pos.end).contains(&v.max_at))
        {
            return Err(invalid("extremum outside record interval"));
        }
    }
    if [v.min, v.max, v.first, v.last]
        .into_iter()
        .any(|s| scalar_type(s) != typ)
    {
        return Err(invalid("inconsistent scalar types"));
    }
    if !matches!(
        (typ, v.sum),
        (INTEGER, AggSum::Int(_))
            | (UNSIGNED | BOOL, AggSum::Uint(_))
            | (FLOAT, AggSum::Float(_))
            | (STRING, AggSum::CountOnly)
    ) {
        return Err(invalid("inconsistent sum type"));
    }
    Ok(())
}

fn validate_children(level: Level, child_count: u8) -> Result<()> {
    let valid = match level {
        LEVEL_BLOCK | LEVEL_DIRTY => child_count == 0,
        2..=MAX_LEVELS => (MIN_CHILDREN..=FAN_OUT).contains(&(child_count as usize)),
        _ => false,
    };
    if !valid {
        return Err(invalid("invalid direct child count"));
    }
    Ok(())
}

fn encode_scalars(typ: FieldType, values: Vec<AggScalar>) -> Result<Vec<u8>> {
    Ok(match typ {
        INTEGER => enc::ints(
            values
                .into_iter()
                .map(|v| {
                    if let AggScalar::Int(v) = v {
                        v
                    } else {
                        unreachable!()
                    }
                })
                .collect(),
        )?,
        UNSIGNED => enc::uints(
            values
                .into_iter()
                .map(|v| {
                    if let AggScalar::Uint(v) = v {
                        v
                    } else {
                        unreachable!()
                    }
                })
                .collect(),
        )?,
        FLOAT => enc::floats(
            values
                .into_iter()
                .map(|v| {
                    if let AggScalar::Float(v) = v {
                        v
                    } else {
                        unreachable!()
                    }
                })
                .collect(),
        )?,
        BOOL => enc::bools(
            &values
                .into_iter()
                .map(|v| {
                    if let AggScalar::Bool(v) = v {
                        v
                    } else {
                        unreachable!()
                    }
                })
                .collect::<Vec<_>>(),
        )?,
        STRING => enc::ints(vec![0; values.len()])?,
        _ => return Err(invalid("unknown field type")),
    })
}
fn decode_scalars(typ: FieldType, bytes: &[u8], n: usize) -> Result<Vec<AggScalar>> {
    Ok(match typ {
        INTEGER => enc::decode_ints(bytes, n)?
            .into_iter()
            .map(AggScalar::Int)
            .collect(),
        UNSIGNED => enc::decode_uints(bytes, n)?
            .into_iter()
            .map(AggScalar::Uint)
            .collect(),
        FLOAT => enc::decode_floats(bytes, n)?
            .into_iter()
            .map(AggScalar::Float)
            .collect(),
        BOOL => enc::decode_bools(bytes, n)?
            .into_iter()
            .map(AggScalar::Bool)
            .collect(),
        STRING => {
            if enc::decode_ints(bytes, n)?.iter().any(|v| *v != 0) {
                return Err(invalid("nonzero string aggregate"));
            }
            vec![AggScalar::None; n]
        }
        _ => return Err(invalid("unknown field type")),
    })
}

pub(crate) fn encode_group(
    typ: FieldType,
    records: &[Record],
    group_offset: u32,
) -> Result<(PageGroupEntry, Vec<u8>)> {
    if records.is_empty() || records.len() > PAGE_GROUP {
        return Err(invalid("invalid group count"));
    }
    for r in records {
        validate_record(r, typ)?;
    }
    for pair in records.windows(2) {
        if pair[0].seq_no != pair[1].seq_no
            || pair[0].level != pair[1].level
            || pair[0].pos.end >= pair[1].pos.start
        {
            return Err(invalid("unordered or overlapping group records"));
        }
    }
    let positions = enc::pack_parts(&[
        enc::times(records.iter().map(|r| r.pos.start).collect())?,
        enc::ints(
            records
                .iter()
                .map(|r| r.pos.end.wrapping_sub(r.pos.start))
                .collect(),
        )?,
        enc::pack_parts(&[
            enc::uints(
                records
                    .iter()
                    .map(|r| u64::from(r.pos.block.file_no))
                    .collect(),
            )?,
            enc::uints(
                records
                    .iter()
                    .map(|r| u64::from(r.pos.block.offset))
                    .collect(),
            )?,
        ])?,
        enc::uints(
            records
                .iter()
                .map(|r| u64::from(r.pos.block_size))
                .collect(),
        )?,
        enc::uints(
            records
                .iter()
                .map(|r| {
                    u64::from(r.pos.flags)
                        | (u64::from(r.pos.child_count) << 8)
                        | (u64::from(r.pos.version) << 16)
                })
                .collect(),
        )?,
    ])?;
    let sums = records
        .iter()
        .map(|r| match r.values.sum {
            AggSum::Int(v) => v as u128,
            AggSum::Uint(v) => v,
            AggSum::Float(v) => u128::from(v.to_bits()),
            AggSum::CountOnly => 0,
        })
        .collect::<Vec<_>>();
    let columns = [
        enc::uints(records.iter().map(|r| r.values.count).collect())?,
        enc::pack_parts(&[
            enc::uints(sums.iter().map(|v| (v >> 64) as u64).collect())?,
            enc::uints(sums.iter().map(|v| *v as u64).collect())?,
        ])?,
        enc::floats(records.iter().map(|r| r.values.sumsq).collect())?,
        encode_scalars(typ, records.iter().map(|r| r.values.min).collect())?,
        enc::times(records.iter().map(|r| r.values.min_at).collect())?,
        encode_scalars(typ, records.iter().map(|r| r.values.max).collect())?,
        enc::times(records.iter().map(|r| r.values.max_at).collect())?,
        encode_scalars(typ, records.iter().map(|r| r.values.first).collect())?,
        encode_scalars(typ, records.iter().map(|r| r.values.last).collect())?,
    ];
    let positions = checksum_part(&positions);
    let columns = columns.map(|c| checksum_part(&c));
    let mut entry = PageGroupEntry {
        first_start: records[0].pos.start,
        last_end: records.last().unwrap().pos.end,
        group_offset,
        first_row: 0,
        row_count: records.len() as u16,
        position_len: u16::try_from(positions.len())
            .map_err(|_| invalid("position page too large"))?,
        column_lens: [0; COLUMN_COUNT],
        payload_crc: 0,
        column_checksums: true,
    };
    let mut out = positions;
    for (i, col) in columns.iter().enumerate() {
        entry.column_lens[i] =
            u16::try_from(col.len()).map_err(|_| invalid("value column too large"))?;
        out.extend_from_slice(col);
    }
    entry.payload_crc = crc32fast::hash(&out);
    Ok((entry, out))
}

pub(crate) fn decode_group(
    seq_no: SeqNo,
    level: Level,
    typ: FieldType,
    mut bytes: &[u8],
    entry: &PageGroupEntry,
    n: usize,
    cols: &[Column],
) -> Result<Vec<SelectedRecord>> {
    super::segment::level_bit(level)?;
    if !valid_type(typ)
        || bytes.len() != entry.byte_len(cols)
        || n != entry.row_count as usize
        || !(1..=PAGE_GROUP).contains(&n)
    {
        return Err(invalid("invalid group read length or type"));
    }
    if bytes.len() == entry.byte_len(&ALL_COLUMNS) {
        entry.verify_payload(bytes)?;
    }
    let positions = enc::take(&mut bytes, entry.position_len as usize)?;
    let mut parts = [None; COLUMN_COUNT];
    let last = cols.iter().map(|c| *c as usize + 1).max().unwrap_or(0);
    for (i, col) in ALL_COLUMNS.iter().enumerate().take(last) {
        let data = enc::take(&mut bytes, entry.column_lens[i] as usize)?;
        if cols.contains(col) {
            parts[i] = Some(data);
        }
    }
    decode_parts(seq_no, level, typ, entry, positions, parts)
}

/// Decode independently checked parts; absent columns remain absent.
pub(crate) fn decode_parts(
    seq_no: SeqNo,
    level: Level,
    typ: FieldType,
    entry: &PageGroupEntry,
    positions: &[u8],
    parts: [Option<&[u8]>; COLUMN_COUNT],
) -> Result<Vec<SelectedRecord>> {
    Ok(decode_columns(seq_no, level, typ, entry, positions, parts)?.records())
}

pub(crate) fn decode_group_columns(
    seq_no: SeqNo,
    level: Level,
    typ: FieldType,
    mut bytes: &[u8],
    entry: &PageGroupEntry,
    cols: &[Column],
) -> Result<super::projected_batch::ProjectedGroup> {
    if bytes.len() != entry.byte_len(cols) {
        return Err(invalid("invalid group read length"));
    }
    if bytes.len() == entry.byte_len(&ALL_COLUMNS) {
        entry.verify_payload(bytes)?;
    }
    let positions = enc::take(&mut bytes, entry.position_len as usize)?;
    let mut parts = [None; COLUMN_COUNT];
    let last = cols.iter().map(|c| *c as usize + 1).max().unwrap_or(0);
    for (i, col) in ALL_COLUMNS.iter().enumerate().take(last) {
        let data = enc::take(&mut bytes, entry.column_lens[i] as usize)?;
        if cols.contains(col) {
            parts[i] = Some(data);
        }
    }
    decode_columns(seq_no, level, typ, entry, positions, parts)
}

/// Keep verified columns in their decoded order until a consumer needs a row.
pub(crate) fn decode_columns(
    seq_no: SeqNo,
    level: Level,
    typ: FieldType,
    entry: &PageGroupEntry,
    positions: &[u8],
    parts: [Option<&[u8]>; COLUMN_COUNT],
) -> Result<super::projected_batch::ProjectedGroup> {
    super::segment::level_bit(level)?;
    let n = entry.row_count as usize;
    if !valid_type(typ)
        || !(1..=PAGE_GROUP).contains(&n)
        || positions.len() != entry.position_len as usize
    {
        return Err(invalid("invalid projected group type or length"));
    }
    let [starts, durations, blocks, sizes, flags] =
        enc::unpack_parts::<5>(verify_part(positions, entry.column_checksums)?)?;
    let starts = enc::decode_times(starts, n)?;
    let durations = enc::decode_ints(durations, n)?;
    let [files, offsets] = enc::unpack_parts::<2>(blocks)?;
    let files = enc::decode_uints(files, n)?;
    let offsets = enc::decode_uints(offsets, n)?;
    let sizes = enc::decode_uints(sizes, n)?;
    let flags = enc::decode_uints(flags, n)?;
    let mut records: Vec<Position> = Vec::with_capacity(n);
    let mut columns = std::array::from_fn(|_| None);
    for i in 0..n {
        let pos = Position {
            start: starts[i],
            end: starts[i].wrapping_add(durations[i]),
            block: BlockLoc {
                file_no: u32::try_from(files[i]).map_err(|_| invalid("file number overflow"))?,
                offset: u32::try_from(offsets[i]).map_err(|_| invalid("block offset overflow"))?,
            },
            block_size: u32::try_from(sizes[i]).map_err(|_| invalid("block size overflow"))?,
            flags: flags[i] as u8,
            child_count: ((flags[i] >> 8) & 255) as u8,
            version: u32::try_from(flags[i] >> 16)
                .map_err(|_| invalid("write version overflow"))?,
        };
        validate_children(level, pos.child_count)?;
        if pos.start > pos.end || pos.flags & !1 != 0 || (i > 0 && pos.start <= records[i - 1].end)
        {
            return Err(invalid("invalid group time range"));
        }
        if level == LEVEL_BLOCK {
            BlockLoc::from_vlog(
                pos.block.generation(),
                pos.block.seq(),
                i64::from(pos.block.offset),
            )?;
            if pos.block_size < 5 || (pos.version != 0 && pos.version >> 8 == 0) {
                return Err(invalid("invalid block size"));
            }
        } else if pos.block != BlockLoc::default()
            || pos.version != 0
            || (level == LEVEL_DIRTY && pos.block_size != 0)
        {
            return Err(invalid("non-block has a location"));
        }
        records.push(pos);
    }
    if starts[0] != entry.first_start || records.last().unwrap().end != entry.last_end {
        return Err(invalid("group timestamp bounds mismatch"));
    }
    for (i, col) in ALL_COLUMNS.iter().enumerate() {
        let Some(data) = parts[i] else {
            continue;
        };
        if data.len() != entry.column_lens[i] as usize {
            return Err(invalid("invalid projected column length"));
        }
        let data = verify_part(data, entry.column_checksums)?;
        let values: Vec<ColumnValue> = match col {
            Column::Count => enc::decode_uints(data, n)?
                .into_iter()
                .map(ColumnValue::Count)
                .collect(),
            Column::Sum => {
                let [high, low] = enc::unpack_parts::<2>(data)?;
                let high = enc::decode_uints(high, n)?;
                let low = enc::decode_uints(low, n)?;
                high.into_iter()
                    .zip(low)
                    .map(|(hi, lo)| {
                        let bits = (u128::from(hi) << 64) | u128::from(lo);
                        Ok(ColumnValue::Sum(match typ {
                            INTEGER => AggSum::Int(bits as i128),
                            UNSIGNED | BOOL => AggSum::Uint(bits),
                            FLOAT if hi == 0 => AggSum::Float(f64::from_bits(lo)),
                            STRING if bits == 0 => AggSum::CountOnly,
                            _ => return Err(invalid("invalid sum bits")),
                        }))
                    })
                    .collect::<Result<_>>()?
            }
            Column::SumSq => enc::decode_floats(data, n)?
                .into_iter()
                .map(ColumnValue::Float)
                .collect(),
            Column::MinAt | Column::MaxAt => enc::decode_times(data, n)?
                .into_iter()
                .map(ColumnValue::Time)
                .collect(),
            Column::Min | Column::Max | Column::First | Column::Last => {
                decode_scalars(typ, data, n)?
                    .into_iter()
                    .map(ColumnValue::Scalar)
                    .collect()
            }
        };
        for value in &values {
            if let ColumnValue::Count(count) = value {
                if (level == LEVEL_DIRTY) != (*count == 0) {
                    return Err(invalid("invalid aggregate count"));
                }
            }
        }
        columns[i] = Some(values);
    }
    let group = super::projected_batch::ProjectedGroup {
        seq_no,
        level,
        positions: records,
        columns,
    };
    if parts.iter().all(Option::is_some) {
        for row in 0..group.len() {
            validate_record(&group.record(row).into_record()?, typ)?;
        }
    }
    Ok(group)
}
