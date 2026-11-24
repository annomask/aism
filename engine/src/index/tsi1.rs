//! Time Series Index.
use std::{
    io,
    io::Write,
    ops::{Deref, DerefMut},
};

use bytes::BufMut;
use file_set::{MeasurementIter, TagKeyIter, TagValueIter};
use integer_encoding::VarInt;
use log_file::{LogMeasurement, LogTagKey, LogTagValue};
use measurement_block::MeasurementBlockElem;
use snafu::Snafu;
use tag_block::{TagBlockKeyElem, TagBlockValueElem};
/// LOAD_FACTOR is the fill percent for RHH indexes.
pub const LOAD_FACTOR: i32 = 80;

pub mod cache;
pub mod file_set;
pub mod index;
pub mod index_file;
pub mod index_files;
pub mod log_file;
pub mod measurement_block;
pub mod partition;
pub mod tag_block;

/// Error type for TSI.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOError { reason: io::Error },
}

/// Result type for TSI.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

// //////////////////////////////////////////////////////////////////////////
// //////////////////////////////// Measurement /////////////////////////////
// //////////////////////////////////////////////////////////////////////////

pub trait MeasurementElement {
    /// Returns the measurement name.
    fn name(&self) -> Option<&[u8]>;

    /// Returns true if the tombstone flag is set.
    fn deleted(&self) -> bool;
}

// Returns an iterator that merges a set of tsi files and tsl files iterators.
// Iterators that are first in the list take precedence and a deletion by those
// early iterators will invalidate elements by later iterators.
pub fn merge_measurement_iters(itrs: Vec<MeasurementIter>) -> Option<MeasurementMergeIter> {
    if itrs.is_empty() {
        return None;
    }

    Some(MeasurementMergeIter {
        e: MeasurementMergeElem(vec![MeasurementElem::default(); itrs.len()]),
        buf: vec![None; itrs.len()],
        itrs,
    })
}

#[derive(Clone)]
pub struct MeasurementMergeIter {
    e: MeasurementMergeElem,
    buf: Vec<Option<MeasurementElem>>, // itrs[i].next: MeasurementBlockElem<'a> and LogMeasurement
    itrs: Vec<MeasurementIter>,        // Vec<LogMeasurementIter and BlockMeasurementIter>
}

impl Iterator for MeasurementMergeIter {
    type Item = MeasurementMergeElem;

    // Next returns the element with the next lowest name across the iterators.
    //
    // If multiple iterators contain the same name then the first is returned
    // and the remaining ones are skipped.
    fn next(&mut self) -> Option<Self::Item> {
        // Find next lowest name amongst the buffers.
        let mut name = vec![];
        for (i, buf) in self.buf.iter_mut().enumerate() {
            if buf.is_none() {
                match self.itrs[i].next() {
                    Some(b) => {
                        *buf = Some(b);
                    }
                    _ => {
                        continue;
                    }
                }
            }

            // Find next lowest name.
            if name.is_empty()
                || buf.as_ref().unwrap().name().unwrap().cmp(&name) == std::cmp::Ordering::Less
            {
                name = buf.as_ref().unwrap().name().unwrap().to_vec();
            }
        }

        // Return nil if no elements remaining.
        if name.is_empty() {
            return None;
        }

        // Merge all elements together and clear buffers.
        self.e.clear();
        for buf in self.buf.iter_mut() {
            if buf.is_none() || !buf.as_ref().unwrap().name().unwrap().eq(&name) {
                continue;
            }
            self.e.push(buf.as_ref().unwrap().clone());
            *buf = None;
        }
        Some(self.e.clone())
    }
}

// Represents a merged measurement element.
#[derive(Clone)]
pub struct MeasurementMergeElem(Vec<MeasurementElem>);

impl MeasurementElement for MeasurementMergeElem {
    // Returns the name of the first element.
    fn name(&self) -> Option<&[u8]> {
        if self.is_empty() {
            return None;
        }

        self.first().unwrap().name()
    }

    // Returns the deleted flag of the first element.
    fn deleted(&self) -> bool {
        if self.is_empty() {
            return false;
        }

        self.first().unwrap().deleted()
    }
}

impl Deref for MeasurementMergeElem {
    type Target = Vec<MeasurementElem>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MeasurementMergeElem {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Default, Clone)]
pub enum MeasurementElem {
    MeasurementBlockElem(MeasurementBlockElem),
    LogMeasurement(LogMeasurement),
    #[default]
    None,
}

impl MeasurementElement for MeasurementElem {
    fn name(&self) -> Option<&[u8]> {
        match self {
            MeasurementElem::MeasurementBlockElem(e) => e.name(),
            MeasurementElem::LogMeasurement(e) => {
                let name = e.name();
                if name.is_empty() {
                    return None;
                }
                Some(name)
            }
            _ => None,
        }
    }

    fn deleted(&self) -> bool {
        match self {
            MeasurementElem::MeasurementBlockElem(e) => e.deleted(),
            MeasurementElem::LogMeasurement(e) => e.deleted(),
            _ => false,
        }
    }
}

impl Iterator for MeasurementIter {
    type Item = MeasurementElem;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MeasurementIter::BlockMeasurementIter(itr) => {
                if let Some(e) = itr.next() {
                    return Some(MeasurementElem::MeasurementBlockElem(e));
                }
                None
            }
            MeasurementIter::LogMeasurementIter(itr) => {
                if let Some(e) = itr.next() {
                    return Some(MeasurementElem::LogMeasurement(e));
                }
                None
            }
        }
    }
}

#[derive(Clone)]
pub struct TSDBMeasurementIterAdapter {
    itr: MeasurementMergeIter, // MeasurementMergeIter
}

impl TSDBMeasurementIterAdapter {
    pub fn new(itr: MeasurementMergeIter) -> TSDBMeasurementIterAdapter {
        TSDBMeasurementIterAdapter { itr }
    }

    pub fn close(&self) -> Result<()> {
        Ok(())
    }
}

impl Iterator for TSDBMeasurementIterAdapter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let e = self.itr.next();
            if let Some(e) = e {
                if e.deleted() {
                    continue;
                }
                if let Some(name) = e.name() {
                    return Some(name.to_vec());
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
    }
}

// //////////////////////////////////////////////////////////////////////////
// //////////////////////////////// TagKey //////////////////////////////////
// //////////////////////////////////////////////////////////////////////////

pub trait TagKeyElement {
    fn key(&self) -> &[u8];
    fn deleted(&self) -> bool;
}

// TSDBTagKeyIterAdapter wraps TagKeyIterator to match the TSDB interface.
// This is needed because TSDB doesn't have a concept of "deleted" tag keys.
pub struct TSDBTagKeyIterAdapter {
    itr: TagKeyMergeIter,
}

impl TSDBTagKeyIterAdapter {
    pub fn new(itr: TagKeyMergeIter) -> TSDBTagKeyIterAdapter {
        TSDBTagKeyIterAdapter { itr }
    }

    pub fn close(&self) -> Result<()> {
        Ok(())
    }
}

impl Iterator for TSDBTagKeyIterAdapter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let e = self.itr.next();
            if let Some(e) = e {
                if e.deleted() {
                    continue;
                }
                return Some(e.key().to_vec());
            } else {
                return None;
            }
        }
    }
}

// TSDBTagValueIterAdapter wraps TagValueIterator to match the TSDB interface.
// This is needed because TSDB doesn't have a concept of "deleted" tag values.
pub struct TSDBTagValueIterAdapter {
    itr: TagValueMergeIter,
}

impl TSDBTagValueIterAdapter {
    pub fn new(itr: TagValueMergeIter) -> TSDBTagValueIterAdapter {
        TSDBTagValueIterAdapter { itr }
    }

    pub fn close(&self) -> Result<()> {
        Ok(())
    }
}

impl Iterator for TSDBTagValueIterAdapter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let e = self.itr.next();
            if let Some(e) = e {
                if e.deleted() {
                    continue;
                }
                return Some(e.value().to_vec());
            } else {
                return None;
            }
        }
    }
}

// Returns an iterator that merges a set of tsi/tsl files iterators.
// Iterators that are first in the list take precedence and a deletion by those
// early iterators will invalidate elements by later iterators.
pub fn merge_tag_key_iters(itrs: Vec<TagKeyIter>) -> Option<TagKeyMergeIter> {
    if itrs.is_empty() {
        return None;
    }

    Some(TagKeyMergeIter {
        e: TagKeyMergeElem(vec![TagKeyElem::default(); itrs.len()]),
        buf: vec![None; itrs.len()],
        itrs,
    })
}

pub struct TagKeyMergeIter {
    e: TagKeyMergeElem,
    buf: Vec<Option<TagKeyElem>>, // TagBlockKeyElem or LogTagKey
    itrs: Vec<TagKeyIter>,        // TagBlockKeyIter or LogTagKeyIter
}

impl Iterator for TagKeyMergeIter {
    type Item = TagKeyMergeElem;

    fn next(&mut self) -> Option<Self::Item> {
        // Find next lowest key amongst the buffers.
        let mut key = vec![];
        for (i, buf) in self.buf.iter_mut().enumerate() {
            if buf.is_none() {
                match self.itrs[i].next() {
                    Some(b) => {
                        // self.buf[i] = Some(Box::new(buf));
                        *buf = Some(b);
                    }
                    _ => {
                        continue;
                    }
                }
            }

            // Find next lowest key.
            if key.is_empty() || buf.as_ref().unwrap().key().cmp(&key) == std::cmp::Ordering::Less {
                key = buf.as_ref().unwrap().key().to_vec();
            }
        }

        // Return nil if no elements remaining.
        if key.is_empty() {
            return None;
        }

        // Merge all elements together and clear buffers.
        self.e.clear();
        for buf in self.buf.iter_mut() {
            if buf.is_none() || !buf.as_ref().unwrap().key().eq(&key) {
                continue;
            }
            self.e.push(buf.as_ref().unwrap().clone());
            *buf = None;
        }
        Some(self.e.clone())
    }
}

// Represents a merged tag key element.
#[derive(Default, Clone)]
pub struct TagKeyMergeElem(Vec<TagKeyElem>); // NOTE: TagKeyElement: logTagKey

impl TagKeyMergeElem {
    /// Returns a merge iterator for all elements until a tombstone occurs.
    pub fn tag_value_iterator(&self) -> Option<TagValueMergeIter> {
        if self.0.is_empty() {
            return None;
        }

        let mut a = Vec::with_capacity(self.0.len());
        for e in self.0.iter() {
            // if let Some(itr) = e.tag_value_iterator() {
            //     a.push(itr);
            // }
            let itr = e.tag_value_iter();
            a.push(itr);

            if e.deleted() {
                break;
            }
        }
        merge_tag_value_iters(a)
    }
}

impl TagKeyElement for TagKeyMergeElem {
    /// Returns the key of the first element.
    fn key(&self) -> &[u8] {
        if self.0.is_empty() {
            return &[];
        }
        self.0[0].key()
    }

    /// Returns the deleted flag of the first element.
    fn deleted(&self) -> bool {
        if self.0.is_empty() {
            return false;
        }
        self.0[0].deleted()
    }
}

impl Deref for TagKeyMergeElem {
    type Target = Vec<TagKeyElem>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TagKeyMergeElem {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone)]
pub enum TagKeyElem {
    TagBlockKeyElem(TagBlockKeyElem),
    LogTagKey(LogTagKey),
}

impl TagKeyElem {
    pub fn tag_value_iter(&self) -> TagValueIter {
        match self {
            TagKeyElem::TagBlockKeyElem(e) => {
                TagValueIter::TagBlockValueIter(e.tag_value_iterator())
            }
            TagKeyElem::LogTagKey(e) => TagValueIter::LogTagValueIter(e.tag_value_iterator()),
        }
    }
}

impl Default for TagKeyElem {
    fn default() -> Self {
        TagKeyElem::TagBlockKeyElem(TagBlockKeyElem::default())
    }
}

impl TagKeyElement for TagKeyElem {
    fn key(&self) -> &[u8] {
        match self {
            TagKeyElem::TagBlockKeyElem(e) => e.key(),
            TagKeyElem::LogTagKey(e) => e.key(),
        }
    }

    fn deleted(&self) -> bool {
        match self {
            TagKeyElem::TagBlockKeyElem(e) => e.deleted(),
            TagKeyElem::LogTagKey(e) => e.deleted(),
        }
    }
}

impl Iterator for TagKeyIter {
    type Item = TagKeyElem;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TagKeyIter::TagBlockKeyIter(itr) => {
                if let Some(e) = itr.next() {
                    return Some(TagKeyElem::TagBlockKeyElem(e));
                }
                None
            }
            TagKeyIter::LogTagKeyIter(itr) => {
                if let Some(e) = itr.next() {
                    return Some(TagKeyElem::LogTagKey(e));
                }
                None
            }
        }
    }
}

// //////////////////////////////////////////////////////////////////////////
// //////////////////////////////// TagValue ////////////////////////////////
// //////////////////////////////////////////////////////////////////////////

pub trait TagValueElement {
    fn value(&self) -> &[u8];
    fn deleted(&self) -> bool;
}

// Returns an iterator that merges a set of tag block value in tsi/tsl files iterators.
// Iterators that are first in the list take precedence and a deletion by those
// early iterators will invalidate elements by later iterators.
pub fn merge_tag_value_iters(itrs: Vec<TagValueIter>) -> Option<TagValueMergeIter> {
    if itrs.is_empty() {
        return None;
    }

    Some(TagValueMergeIter {
        e: TagValueMergeElem(vec![TagValueElem::default(); itrs.len()]),
        buf: vec![None; itrs.len()],
        itrs,
    })
}

pub struct TagValueMergeIter {
    e: TagValueMergeElem,
    buf: Vec<Option<TagValueElem>>,
    itrs: Vec<TagValueIter>,
}

impl Iterator for TagValueMergeIter {
    type Item = TagValueMergeElem;

    fn next(&mut self) -> Option<Self::Item> {
        // Find next lowest value amongst the buffers.
        let mut value = vec![];
        for (i, buf) in self.buf.iter_mut().enumerate() {
            if buf.is_none() {
                match self.itrs[i].next() {
                    Some(b) => {
                        // self.buf[i] = Some(Box::new(buf));
                        *buf = Some(b);
                    }
                    _ => {
                        continue;
                    }
                }
            }

            // Find next lowest value.
            if value.is_empty()
                || buf.as_ref().unwrap().value().cmp(&value) == std::cmp::Ordering::Less
            {
                value = buf.as_ref().unwrap().value().to_vec();
            }
        }

        // Return nil if no elements remaining.
        if value.is_empty() {
            return None;
        }

        // Merge all elements together and clear buffers.
        self.e.clear();
        for buf in self.buf.iter_mut() {
            if buf.is_none() || !buf.as_ref().unwrap().value().eq(&value) {
                continue;
            }
            self.e.push(buf.as_ref().unwrap().clone());
            *buf = None;
        }
        Some(self.e.clone())
    }
}

// Represents a merged tag value element.
#[derive(Clone)]
pub struct TagValueMergeElem(Vec<TagValueElem>);

impl Deref for TagValueMergeElem {
    type Target = Vec<TagValueElem>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TagValueMergeElem {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TagValueElement for TagValueMergeElem {
    fn value(&self) -> &[u8] {
        if self.0.is_empty() {
            return &[];
        }

        self.0[0].value()
    }

    fn deleted(&self) -> bool {
        if self.0.is_empty() {
            return false;
        }
        self.0[0].deleted()
    }
}

#[derive(Clone)]
pub enum TagValueElem {
    TagBlockValueElem(TagBlockValueElem),
    LogTagValue(LogTagValue),
}

impl Default for TagValueElem {
    fn default() -> Self {
        TagValueElem::TagBlockValueElem(TagBlockValueElem::default())
    }
}

impl TagValueElement for TagValueElem {
    fn value(&self) -> &[u8] {
        match self {
            TagValueElem::TagBlockValueElem(v) => v.value(),
            TagValueElem::LogTagValue(v) => v.value(),
        }
    }

    fn deleted(&self) -> bool {
        match self {
            TagValueElem::TagBlockValueElem(v) => v.deleted(),
            TagValueElem::LogTagValue(v) => v.deleted(),
        }
    }
}

impl Iterator for TagValueIter {
    type Item = TagValueElem;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TagValueIter::TagBlockValueIter(itr) => {
                if let Some(e) = itr.next() {
                    return Some(TagValueElem::TagBlockValueElem(e));
                }
                None
            }
            TagValueIter::LogTagValueIter(itr) => {
                if let Some(e) = itr.next() {
                    return Some(TagValueElem::LogTagValue(e));
                }
                None
            }
        }
    }
}

pub trait TagValueIterator: Iterator<Item = Box<dyn TagValueElement>> {}

impl<T> TagValueIterator for T where T: Iterator<Item = Box<dyn TagValueElement>> {}

// //////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////

pub fn write_to(w: &mut impl Write, v: &[u8]) -> Result<usize> {
    let n = w.write(v)?;
    Ok(n)
}

pub fn write_u8_to(w: &mut impl Write, v: u8) -> Result<usize> {
    let n = w.write(std::slice::from_ref(&v))?;
    Ok(n)
}

/// Writes write v into w using big endian encoding.
/// returning how many bytes were written.
pub fn write_u16_to(w: &mut impl Write, v: u16) -> Result<usize> {
    let mut buf = vec![0u8; 2];
    (&mut buf[..]).put_u16(v);

    let n = w.write(buf.as_slice())?;

    Ok(n)
}

/// Writes write v into w using big endian encoding.
/// returning how many bytes were written.
pub fn write_u64_to(w: &mut impl Write, v: u64) -> Result<usize> {
    let mut buf = vec![0u8; 8];
    (&mut buf[..]).put_u64(v);

    let n = w.write(buf.as_slice())?;

    Ok(n)
}

/// Writes write v into w using variable length encoding.
/// returning how many bytes were written.
pub fn write_u_vint_to(w: &mut impl Write, v: u64) -> Result<usize> {
    let mut buf = vec![0u8; i64::MAX.required_space()];
    let i = v.encode_var(&mut buf);

    let n = w.write(&buf[..i])?;

    Ok(n)
}

pub fn decode_var(data: &[u8]) -> Option<(u64, usize)> {
    if data.is_empty() {
        return None;
    }
    if let Some((value, n)) = u64::decode_var(data) {
        if n == 0 || n > data.len() {
            return None;
        }
        return Some((value, n));
    }
    None
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::path::PathBuf;

    pub fn must_temp_tsi_dir() -> PathBuf {
        let mut dir_path = std::env::temp_dir();
        dir_path.push(format!("tsi-{:x}", rand::random::<u64>())); // Add the prefix
        std::fs::create_dir_all(&dir_path)
            .expect("unable to create tsi temporary working directory");
        dir_path
    }
}
