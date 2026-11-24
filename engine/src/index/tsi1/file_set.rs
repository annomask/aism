//! A collections of seriesfiles, tsi file and tsl file.
use core::panic;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::common::hll::{self, HLL};
use futures::SinkExt;
use parking_lot::{Mutex, RwLock};
use serde_json::to_vec;
use snafu::{ResultExt, Snafu, ensure};

use super::{
    index_file::IndexFile,
    log_file::{LogFile, LogMeasurementIter, LogTagKeyIter, LogTagValueIter},
    measurement_block::BlockMeasurementIter,
    partition::CompactionLevel,
    tag_block::{TagBlockKeyIter, TagBlockValueIter},
};
use crate::{
    common::slow_trace,
    index::{
        SeriesIDElem, SeriesIDIter, SeriesIDIterator, SeriesIDSetIter, merge_series_id_iterators,
        new_series_id_set_iter,
        tsi1::{
            MeasurementElement, MeasurementMergeIter, TSDBMeasurementIterAdapter,
            TSDBTagKeyIterAdapter, TSDBTagValueIterAdapter, TagKeyElement, TagKeyMergeIter,
            TagValueElement, TagValueMergeIter, merge_measurement_iters, merge_tag_key_iters,
            merge_tag_value_iters,
        },
    },
    series::{series_file::SeriesFile, series_set},
};

/// Error type for FileSet.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("{}", reason))]
    IOErr { reason: io::Error },

    /// Index file error.
    #[snafu(display("{}", source))]
    IndexFile {
        source: crate::index::tsi1::index_file::Error,
    },

    /// Log file error.
    #[snafu(display("{}", source))]
    LogFile {
        source: crate::index::tsi1::log_file::Error,
    },

    /// HLL error.
    #[snafu(display("{}", source))]
    HLL { source: crate::common::hll::Error },

    /// TSI error.
    #[snafu(display("{}", source))]
    TSI { source: crate::index::tsi1::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for FileSet.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// FileSet represents a collection of files.
#[derive(Default)]
pub struct FileSet {
    levels: Vec<CompactionLevel>,
    sfile: Arc<SeriesFile>,
    pub files: VecDeque<File>, // .tsi and .tsl
    manifest_size: i64,        // Size of the manifest file in bytes.
}

impl FileSet {
    /// Returns a new instance of FileSet.
    pub fn new(
        levels: Vec<CompactionLevel>,
        sfile: Arc<SeriesFile>,
        files: VecDeque<File>,
    ) -> FileSet {
        FileSet {
            levels,
            sfile,
            files,
            manifest_size: 0,
        }
    }

    /// Returns the active files in the file set.
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Estimates the memory footprint of this FileSet, in bytes.
    pub fn bytes(&self) -> usize {
        unimplemented!("bytes not implemented yet")
    }

    /// Closes all the files in the file set.
    pub fn close(&self) -> Result<()> {
        let mut err: Option<Error> = None;
        for f in &self.files {
            if let Err(e) = f.close() {
                if err.is_none() {
                    err = Some(e);
                }
            }
        }

        if let Some(e) = err { Err(e) } else { Ok(()) }
    }

    /// Adds a reference count to all files.
    pub fn retain(&self) {
        for f in &self.files {
            f.retain();
        }
    }

    pub(crate) fn retain_for_query_measurement_exists(&self) {
        for f in &self.files {
            f.retain_for_query_measurement_exists();
        }
    }

    /// Removes a reference count from all files.
    pub fn release(&self) {
        for f in &self.files {
            f.release();
        }
    }

    #[cfg(test)]
    /// Returns the attached series file.
    pub fn series_file(&self) -> Arc<SeriesFile> {
        Arc::clone(&self.sfile)
    }

    /// Returns a new file set with f added at the beginning.
    /// Filters do not need to be rebuilt because log files have no bloom filter.
    // pub fn prepend_log_file(&mut self, f: Arc<RwLock<LogFile>>) {
    //     self.files
    //         .as_mut()
    //         .unwrap()
    //         .push_front(File::LogFile(f));
    //     self.manifest_size = 0;
    // }
    pub fn prepend_log_file(&self, f: Arc<RwLock<LogFile>>) -> FileSet {
        let mut files = self.files.clone();
        files.push_front(File::LogFile(f));

        Self {
            levels: self.levels.clone(),
            sfile: Arc::clone(&self.sfile),
            files,
            manifest_size: 0,
        }
    }

    /// Returns the on-disk size of the FileSet.
    pub fn size(&self) -> i64 {
        let mut total = 0i64;
        for f in &self.files {
            total += f.size();
        }

        total + self.manifest_size
    }

    /// Swaps a list of files for a single file and returns a new file set.
    /// The caller should always guarantee that the files exist and are contiguous.
    pub fn must_replace(&self, old_files: VecDeque<File>, new_file: File) -> FileSet {
        assert!(!old_files.is_empty(), "cannot replace empty files");

        // Find index of first old file.
        let mut i = 0;
        while i < self.files.len() {
            if self.files[i] == old_files[0] {
                break;
            } else if i == self.files.len() - 1 {
                panic!("first replacement file not found");
            }
            i += 1;
        }

        // Ensure all old files are contiguous.
        for (j, _) in old_files.iter().enumerate() {
            if self.files[i + j] != old_files[j] {
                // TODO: implement me
                // panic!(
                //     "cannot replace non-contiguous files: subset={:?}, fileset={:?}",
                //     Files(&old_files).ids(),
                //     Files(&self.files).ids()
                // );
                panic!("cannot replace non-contiguous files");
            }
        }

        // Copy to new fileset.
        let len = self.files.len() - old_files.len() + 1;
        let mut other = VecDeque::with_capacity(len);
        for f in &self.files {
            other.push_back(f.clone());
        }
        other.push_back(new_file);
        for idx in i + old_files.len()..len {
            other.push_back(self.files[idx].clone());
        }

        Self {
            levels: self.levels.clone(),
            sfile: Arc::clone(&self.sfile),
            files: other,
            manifest_size: 0,
        }
        // self.files.as_mut().unwrap().insert(i, new_file);
    }

    /// Returns the highest file identifier.
    pub fn max_id(&self) -> i64 {
        let mut max = 0;
        for f in self.files.iter() {
            let i = f.id();
            if i > max {
                max = i;
            }
        }
        max
    }

    // TODO: used for tests
    // /// Returns all files in the set.
    // pub fn files(&self) -> Vec<Arc<File>> {
    //     self.files.clon
    // }

    // Return a slice of reference to index files
    /// Returns the last contiguous files by level.
    /// These can be used by the compaction scheduler.
    pub fn last_contiguous_index_files_by_evel(
        &self,
        level: i64,
    ) -> Option<VecDeque<Arc<RwLock<IndexFile>>>> {
        // 1 <= level <= partition.levels.len()-2, all files in this level are index files.
        if level == 0 {
            return None;
        }

        let mut a = VecDeque::new();
        for f in self.files.iter().rev() {
            // let f = &*f.read();
            // Ignore files above level, stop on files below level.
            let flevel = f.level();
            match level.cmp(&flevel) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => break,
                _ => {}
            }

            if let File::IndexFile(f) = f {
                a.push_front(Arc::clone(f));
            }
        }

        Some(a)
    }

    /// Returns true if a measurement exists.
    pub fn measurement_exists(&self, name: &[u8]) -> bool {
        let total_start = slow_trace::now();
        for f in self.files.iter() {
            match f {
                File::IndexFile(idx_file) => {
                    let file_start = slow_trace::now();
                    let lock_start = slow_trace::now();
                    let idx_file = idx_file.read();
                    slow_trace::log_lock_wait(
                        "index_file.read",
                        lock_start,
                        "stage=query_wait_for_index_lock component=measurement_exists",
                    );
                    let measurement = idx_file.measurement(name);
                    slow_trace::log_query_stage(
                        "query_index_lookup",
                        file_start,
                        "component=file_set_measurement_exists_index_file",
                    );
                    if let Some(m) = measurement {
                        slow_trace::log_query_stage(
                            "query_index_lookup",
                            total_start,
                            "component=file_set_measurement_exists_total",
                        );
                        return !m.deleted();
                    }
                }
                File::LogFile(log_file) => {
                    let file_start = slow_trace::now();
                    let lock_start = slow_trace::now();
                    let log_file = log_file.read();
                    slow_trace::log_lock_wait(
                        "log_file.read",
                        lock_start,
                        "stage=query_wait_for_index_lock component=measurement_exists",
                    );
                    let measurement = log_file.measurement(name);
                    slow_trace::log_query_stage(
                        "query_index_lookup",
                        file_start,
                        "component=file_set_measurement_exists_log_file",
                    );
                    if let Some(m) = measurement {
                        slow_trace::log_query_stage(
                            "query_index_lookup",
                            total_start,
                            "component=file_set_measurement_exists_total",
                        );
                        return !m.deleted();
                    }
                }
            };
        }
        slow_trace::log_query_stage(
            "query_index_lookup",
            total_start,
            "component=file_set_measurement_exists_total",
        );
        false
    }

    /// Returns an iterator over all measurement names and deleted in the index.
    pub fn measurement_iter(&self) -> Option<MeasurementMergeIter> {
        let mut a = vec![];
        for f in self.files.iter() {
            let itr = f.measurement_iter();
            a.push(itr);
        }
        merge_measurement_iters(a)
    }

    /// Returns an iterator over all tag keys for a measurement.
    pub fn tag_key_iter(&self, name: &[u8]) -> Option<TagKeyMergeIter> {
        let mut a = vec![];
        for f in self.files.iter() {
            if let Some(itr) = f.tag_key_iter(name) {
                a.push(itr);
            }
        }
        merge_tag_key_iters(a)
    }

    /// Returns a series iterator for a measurement.
    pub fn measurement_series_id_iter(&self, name: &[u8]) -> Option<SeriesIDIter> {
        let mut a = vec![];
        for f in self.files.iter() {
            if let Some(itr) = f.measurement_series_id_iter(name) {
                a.push(itr);
            }
        }
        crate::index::merge_series_id_iterators(a)
    }

    /// Returns the tag keys for the measurement.
    pub fn measurement_tag_keys(&self, name: &[u8]) -> HashSet<String> {
        let mut m = HashSet::new();
        if let Some(itr) = self.tag_key_iter(name) {
            unsafe {
                for e in itr {
                    m.insert(String::from_utf8_unchecked(e.key().to_vec()));
                }
            }
        }

        m
    }

    /// Returns a series iterator for all values across a single key.
    pub fn tag_key_series_id_iter(&self, name: &[u8], key: &[u8]) -> Result<Option<SeriesIDIter>> {
        let mut a = vec![];
        for f in self.files.iter() {
            if let Some(itr) = f.tag_key_series_id_iter(name, key)? {
                a.push(itr);
            }
        }
        Ok(crate::index::merge_series_id_iterators(a))
    }

    /// Returns true if the tag key exists.
    pub fn has_tag_key(&self, name: &[u8], key: &[u8]) -> bool {
        for f in self.files.iter() {
            if !f.has_tag_key(name, key) {
                continue;
            }
            return true;
        }
        false
    }

    /// Returns true if the tag value exists.
    pub fn has_tag_value(&self, name: &[u8], key: &[u8], value: &[u8]) -> bool {
        for f in self.files.iter() {
            if !f.has_tag_value(name, key, value) {
                continue;
            }
            return true;
        }
        false
    }

    /// Returns a value iterator for a tag key.
    pub fn tag_value_iter(&self, name: &[u8], key: &[u8]) -> Option<TagValueMergeIter> {
        let mut a = vec![];
        for f in self.files.iter() {
            if let Some(itr) = f.tag_value_iter(name, key) {
                a.push(itr);
            }
        }
        merge_tag_value_iters(a)
    }

    /// Returns a series iterator for a single tag value.
    pub fn tag_value_series_id_iter(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<SeriesIDSetIter> {
        let mut ss = series_set::SeriesIDSet::new(&[]);

        let mut ftss = Arc::new(series_set::SeriesIDSet::new(&[]));
        for f in self.files.iter().rev() {
            // Remove tombstones set in previous file.
            if !ftss.is_empty() && ftss.cardinality() > 0 {
                ss = ss.and_not(&ftss);
            }

            // Fetch tag value series set for this file and merge into overall set.
            if let Some(fss) = f.tag_value_series_id_set(name, key, value)? {
                if !fss.is_empty() {
                    ss.merge(&[&fss]);
                }
            }

            // Fetch tombstone set to be processed on next file.
            ftss = f.tombstone_series_id_set()?;
        }

        Ok(new_series_id_set_iter(ss))
    }

    /// Returns the merged measurement sketches for the FileSet.
    pub fn measurements_sketches(&self) -> Result<(HLL, HLL)> {
        let (mut sketch, mut tsketch) = (HLL::default(), HLL::default());
        for f in self.files.iter() {
            let (s, t) = f.measurements_sketches()?;
            sketch.merge(Some(&s)).context(HLLSnafu)?;
            tsketch.merge(Some(&t)).context(HLLSnafu)?;
        }
        Ok((sketch, tsketch))
    }

    /// Returns the merged measurement sketches for the FileSet.
    pub fn series_sketches(&self) -> Result<(HLL, HLL)> {
        let (mut sketch, mut tsketch) = (HLL::default(), HLL::default());
        for f in self.files.iter() {
            let (s, t) = f.series_sketches()?;
            sketch.merge(Some(&s)).context(HLLSnafu)?;
            tsketch.merge(Some(&t)).context(HLLSnafu)?;
        }
        Ok((sketch, tsketch))
    }
}

#[derive(Clone)]
pub enum File {
    IndexFile(Arc<RwLock<IndexFile>>), // .tsi
    LogFile(Arc<RwLock<LogFile>>),     // .tsl
}

impl File {
    pub fn close(&self) -> Result<()> {
        match self {
            File::IndexFile(idx_file) => {
                let mut idx_file = idx_file.write();
                idx_file.close().context(IndexFileSnafu)
            }

            File::LogFile(log_file) => {
                let mut log_file = log_file.write();
                log_file.close().context(LogFileSnafu)
            }
        }
    }

    pub fn retain(&self) {
        match self {
            File::IndexFile(idx_file) => idx_file.read().retain(),
            File::LogFile(log_file) => log_file.read().retain(),
        }
    }

    pub(crate) fn retain_for_query_measurement_exists(&self) {
        match self {
            File::IndexFile(idx_file) => {
                let lock_start = slow_trace::now();
                let idx_file = idx_file.read();
                slow_trace::log_lock_wait(
                    "index_file.read",
                    lock_start,
                    "stage=query_wait_for_index_lock component=file_set_retain",
                );
                idx_file.retain();
            }
            File::LogFile(log_file) => {
                let lock_start = slow_trace::now();
                let log_file = log_file.read();
                slow_trace::log_lock_wait(
                    "log_file.read",
                    lock_start,
                    "stage=query_wait_for_index_lock component=file_set_retain",
                );
                log_file.retain();
            }
        }
    }

    pub fn release(&self) {
        match self {
            File::IndexFile(idx_file) => idx_file.read().release(),
            File::LogFile(log_file) => log_file.read().release(),
        }
    }

    fn size(&self) -> i64 {
        match self {
            File::IndexFile(idx_file) => idx_file.read().size(),
            File::LogFile(log_file) => log_file.read().size(),
        }
    }

    fn id(&self) -> i64 {
        match self {
            File::IndexFile(idx_file) => idx_file.read().id(),
            File::LogFile(log_file) => log_file.read().id(),
        }
    }

    fn level(&self) -> i64 {
        match self {
            File::IndexFile(idx_file) => idx_file.read().level(),
            File::LogFile(log_file) => log_file.read().level(),
        }
    }

    pub fn measurement_has_series(&self, ss: &series_set::SeriesIDSet, name: &[u8]) -> bool {
        match self {
            File::IndexFile(idx_file) => idx_file.read().measurement_has_series(ss, name),
            File::LogFile(log_file) => log_file.read().measurement_has_series(ss, name),
        }
    }

    fn measurement_iter(&self) -> MeasurementIter {
        match self {
            File::IndexFile(idx_file) => {
                let idx_file = idx_file.read();
                MeasurementIter::BlockMeasurementIter(idx_file.measurement_iterator())
            }
            File::LogFile(log_file) => {
                MeasurementIter::LogMeasurementIter(log_file.read().measurement_iterator())
            }
        }
    }

    pub fn measurements_sketches(&self) -> Result<(HLL, HLL)> {
        let (mut sketch, mut tsketch) = (HLL::default(), HLL::default());

        match self {
            File::IndexFile(idx_file) => {
                let (s, t) = idx_file
                    .read()
                    .measurement_sketches()
                    .context(IndexFileSnafu)?;
                sketch.merge(Some(&s)).context(HLLSnafu)?;
                tsketch.merge(Some(&t)).context(HLLSnafu)?;
            }
            File::LogFile(log_file) => {
                let (s, t) = log_file
                    .read()
                    .measurement_sketches()
                    .context(LogFileSnafu)?;
                sketch.merge(Some(&s)).context(HLLSnafu)?;
                tsketch.merge(Some(&t)).context(HLLSnafu)?;
            }
        }

        Ok((sketch, tsketch))
    }

    fn tag_key_iter(&self, name: &[u8]) -> Option<TagKeyIter> {
        match self {
            File::IndexFile(idx_file) => {
                if let Some(itr) = idx_file.read().tag_key_iterator(name) {
                    return Some(TagKeyIter::TagBlockKeyIter(itr));
                }
                None
            }
            File::LogFile(log_file) => {
                if let Some(itr) = log_file.read().tag_key_iterator(name) {
                    return Some(TagKeyIter::LogTagKeyIter(itr));
                }
                None
            }
        }
    }

    fn measurement_series_id_iter(&self, name: &[u8]) -> Option<SeriesIDIter> {
        match self {
            File::IndexFile(idx_file) => idx_file.read().measurement_series_id_iterator(name),
            File::LogFile(log_file) => log_file.read().measurement_series_id_iterator(name),
        }
    }

    fn tag_key_series_id_iter(&self, name: &[u8], key: &[u8]) -> Result<Option<SeriesIDIter>> {
        match self {
            File::IndexFile(idx_file) => {
                if let Some(itr) = idx_file
                    .read()
                    .tag_key_series_id_iterator(name, key)
                    .context(IndexFileSnafu)?
                {
                    return Ok(Some(itr));
                }
                Ok(None)
            }
            File::LogFile(log_file) => Ok(log_file.read().tag_key_series_id_iterator(name, key)),
        }
    }

    fn tag_value_series_id_set(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<crate::series::series_set::SeriesIDSet>> {
        match self {
            File::IndexFile(idx_file) => idx_file
                .read()
                .tag_value_series_id_set(name, key, value)
                .context(IndexFileSnafu),
            File::LogFile(log_file) => {
                Ok(log_file.read().tag_value_series_id_set(name, key, value))
            }
        }
    }

    pub fn series_id_set(&self) -> Result<Arc<crate::series::series_set::SeriesIDSet>> {
        match self {
            File::IndexFile(idx_file) => Ok(Arc::new(
                idx_file.read().series_id_set().context(IndexFileSnafu)?,
            )),
            File::LogFile(log_file) => Ok(log_file.read().series_id_set()),
        }
    }

    pub fn series_sketches(&self) -> Result<(HLL, HLL)> {
        let (mut sketch, mut tsketch) = (HLL::default(), HLL::default());
        match self {
            File::IndexFile(idx_file) => {
                let (s, t) = idx_file.read().series_sketches().context(IndexFileSnafu)?;
                sketch.merge(Some(&s)).context(HLLSnafu)?;
                tsketch.merge(Some(&t)).context(HLLSnafu)?;
            }
            File::LogFile(log_file) => {
                let (s, t) = log_file.read().series_sketches().context(LogFileSnafu)?;
                sketch.merge(Some(&s)).context(HLLSnafu)?;
                tsketch.merge(Some(&t)).context(HLLSnafu)?;
            }
        }
        Ok((sketch, tsketch))
    }

    pub fn tombstone_series_id_set(&self) -> Result<Arc<crate::series::series_set::SeriesIDSet>> {
        match self {
            File::IndexFile(idx_file) => Ok(Arc::new(
                idx_file
                    .read()
                    .tombstone_series_id_set()
                    .context(IndexFileSnafu)?,
            )),
            File::LogFile(log_file) => Ok(log_file.read().tombstone_series_id_set()),
        }
    }

    fn has_tag_key(&self, name: &[u8], key: &[u8]) -> bool {
        match self {
            File::IndexFile(idx_file) => {
                if let Some(tk) = idx_file.read().tag_key(name, key) {
                    return !tk.deleted();
                }
                false
            }
            File::LogFile(log_file) => {
                if let Some(tk) = log_file.read().tag_key(name, key) {
                    return !tk.deleted();
                }
                false
            }
        }
    }

    fn has_tag_value(&self, name: &[u8], key: &[u8], value: &[u8]) -> bool {
        match self {
            File::IndexFile(idx_file) => {
                if let Some(tv) = idx_file.read().tag_value(name, key, value) {
                    return !tv.deleted();
                }
                false
            }
            File::LogFile(log_file) => {
                if let Some(tv) = log_file.read().tag_value(name, key, value) {
                    return !tv.deleted();
                }
                false
            }
        }
    }

    fn tag_value_iter(&self, name: &[u8], key: &[u8]) -> Option<TagValueIter> {
        match self {
            File::IndexFile(idx_file) => {
                if let Some(itr) = idx_file.read().tag_value_iterator(name, key) {
                    return Some(TagValueIter::TagBlockValueIter(itr));
                }
                None
            }
            File::LogFile(log_file) => {
                if let Some(itr) = log_file.read().tag_value_iter(name, key) {
                    return Some(TagValueIter::LogTagValueIter(itr));
                }
                None
            }
        }
    }

    pub fn path(&self) -> PathBuf {
        match self {
            File::IndexFile(idx_file) => idx_file.read().path().to_path_buf(),
            File::LogFile(log_file) => log_file.read().path().to_path_buf(),
        }
    }
}

impl PartialEq for File {
    fn eq(&self, other: &Self) -> bool {
        match self {
            File::IndexFile(f) => {
                if let File::IndexFile(o) = other {
                    return *f.read() == *o.read();
                }
                false
            }
            File::LogFile(f) => {
                if let File::LogFile(o) = other {
                    return *f.read() == *o.read();
                }
                false
            }
        }
    }
}

impl From<Arc<RwLock<IndexFile>>> for File {
    fn from(value: Arc<RwLock<IndexFile>>) -> Self {
        File::IndexFile(value)
    }
}

impl From<Arc<RwLock<LogFile>>> for File {
    fn from(value: Arc<RwLock<LogFile>>) -> Self {
        File::LogFile(value)
    }
}

struct Files<'a>(&'a VecDeque<File>);

impl<'a> Files<'a> {
    fn ids(&self) -> Vec<i64> {
        let mut ids = vec![];
        for f in self.0.iter() {
            ids.push(f.id());
        }
        ids
    }
}

#[derive(Clone)]
pub enum MeasurementIter {
    BlockMeasurementIter(BlockMeasurementIter),
    LogMeasurementIter(LogMeasurementIter),
}

#[derive(Clone)]
pub enum TagKeyIter {
    TagBlockKeyIter(TagBlockKeyIter), // Item = TagBlockKeyElem<'a>
    LogTagKeyIter(LogTagKeyIter),     // Item = LogTagKey
}

#[derive(Clone)]
pub enum TagValueIter {
    TagBlockValueIter(TagBlockValueIter), // Item = TagBlockValueElem
    LogTagValueIter(LogTagValueIter),     // Item = LogTagValue
}

// /////////////////////////////////////////////////////////////
// ////////////////// FileSet      /////////////////////////////
// /////////////////////////////////////////////////////////////

pub enum FileSetSeriesIDIter {
    OneFileSetSeriesIDIter(OneFileSetSeriesIDIter),
    OneFileSetSeriesIDSetIter(OneFileSetSeriesIDSetIter),
}

impl FileSetSeriesIDIter {
    pub fn series_id_set(&self) -> Option<Arc<series_set::SeriesIDSet>> {
        match self {
            FileSetSeriesIDIter::OneFileSetSeriesIDIter(itr) => itr.series_id_set(),
            FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr) => itr.series_id_set(),
            _ => None,
        }
    }
}

impl SeriesIDIterator for FileSetSeriesIDIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        match self {
            FileSetSeriesIDIter::OneFileSetSeriesIDIter(itr) => itr.close(),
            FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr) => itr.close(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for FileSetSeriesIDIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FileSetSeriesIDIter::OneFileSetSeriesIDIter(itr) => itr.next(),
            FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr) => itr.next(),
        }
    }
}

pub fn new_file_set_series_id_iter(
    fs: Arc<FileSet>,
    itr: Option<SeriesIDIter>,
) -> Option<FileSetSeriesIDIter> {
    if itr.is_none() {
        fs.release();
        return None;
    }

    let itr = itr.unwrap();
    if let SeriesIDIter::SeriesIDSetIter(itr) = itr {
        return Some(FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(
            OneFileSetSeriesIDSetIter {
                once: std::sync::Once::new(),
                fs,
                itr,
            },
        ));
    }

    Some(FileSetSeriesIDIter::OneFileSetSeriesIDIter(
        OneFileSetSeriesIDIter {
            once: std::sync::Once::new(),
            fs,
            itr,
        },
    ))
}
/// OneFileSetSeriesIDIter attaches a fileset to an iterator that is released on close.
pub struct OneFileSetSeriesIDIter {
    once: std::sync::Once,
    fs: Arc<FileSet>,
    itr: SeriesIDIter,
}

impl OneFileSetSeriesIDIter {
    pub fn series_id_set(&self) -> Option<Arc<series_set::SeriesIDSet>> {
        self.itr.series_id_set()
    }
}

impl SeriesIDIterator for OneFileSetSeriesIDIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.once.call_once(|| {
            self.fs.release();
        });

        Ok(self.itr.close()?)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for OneFileSetSeriesIDIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        self.itr.next()
    }
}

/// OneFileSetSeriesIDSetIter attaches a fileset to an iterator that is released on close.
pub struct OneFileSetSeriesIDSetIter {
    once: std::sync::Once,
    fs: Arc<FileSet>,
    itr: SeriesIDSetIter,
}

impl OneFileSetSeriesIDSetIter {
    pub fn series_id_set(&self) -> Option<Arc<series_set::SeriesIDSet>> {
        Some(self.itr.series_id_set())
    }
}

impl SeriesIDIterator for OneFileSetSeriesIDSetIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.once.call_once(|| {
            self.fs.release();
        });

        self.itr.close()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for OneFileSetSeriesIDSetIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        self.itr.next()
    }
}

// OneFileSetMeasurementIter attaches a fileset to an iterator that is released on close.
pub struct OneFileSetMeasurementIter {
    once: std::sync::Once,
    fs: Arc<FileSet>,
    itr: TSDBMeasurementIterAdapter,
}

pub fn new_one_file_set_measurement_iter(
    fs: Arc<FileSet>,
    itr: TSDBMeasurementIterAdapter,
) -> OneFileSetMeasurementIter {
    OneFileSetMeasurementIter {
        once: std::sync::Once::new(),
        fs,
        itr,
    }
}

impl OneFileSetMeasurementIter {
    pub fn close(&self) -> Result<()> {
        self.once.call_once(|| {
            self.fs.release();
        });

        Ok(())
    }
}

impl Iterator for OneFileSetMeasurementIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        self.itr.next()
    }
}

// OneFileSetTagKeyIter attaches a fileset to an iterator that is released on close.
pub struct OneFileSetTagKeyIter {
    once: std::sync::Once,
    fs: Arc<FileSet>,
    itr: TSDBTagKeyIterAdapter,
}

pub fn new_one_file_set_tag_key_iter(
    fs: Arc<FileSet>,
    itr: TSDBTagKeyIterAdapter,
) -> OneFileSetTagKeyIter {
    OneFileSetTagKeyIter {
        once: std::sync::Once::new(),
        fs,
        itr,
    }
}

impl OneFileSetTagKeyIter {
    pub fn close(&self) -> Result<()> {
        self.once.call_once(|| {
            self.fs.release();
        });

        Ok(())
    }
}

impl Iterator for OneFileSetTagKeyIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        self.itr.next()
    }
}

// OneFileSetTagValueIter attaches a fileset to an iterator that is released on close.
pub struct OneFileSetTagValueIter {
    once: std::sync::Once,
    fs: Arc<FileSet>,
    itr: TSDBTagValueIterAdapter,
}

pub fn new_one_file_set_tag_value_iter(
    fs: Arc<FileSet>,
    itr: TSDBTagValueIterAdapter,
) -> OneFileSetTagValueIter {
    OneFileSetTagValueIter {
        once: std::sync::Once::new(),
        fs,
        itr,
    }
}

impl OneFileSetTagValueIter {
    pub fn close(&self) -> Result<()> {
        self.once.call_once(|| {
            self.fs.release();
        });

        Ok(())
    }
}

impl Iterator for OneFileSetTagValueIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        self.itr.next()
    }
}

#[cfg(test)]
pub mod tests {

    use std::{collections::HashMap, panic, sync::Arc};

    use crate::common::to_str;
    use scopeguard::defer;

    use super::FileSet;
    use crate::{
        index::tsi1::{MeasurementElement, TagKeyElement, index},
        series::series_file::{compare_series_keys, parse_series_key, tests::Series},
    };

    // Ensure fileset can return an iterator over all series in the index.
    #[test]
    fn test_file_setfile_set_series_id_iterator() {
        let mut idx = index::tests::must_open_index(1);

        // 1. Create initial set of series.
        idx.create_series_slice_if_not_exists(vec![
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
        ])
        .unwrap();

        // 2. Verify initial set of series.
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_initial_series_series_id(fs);
        }

        // 3. Add more series.
        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "disk".as_bytes().to_vec(),
                ..Default::default()
            },
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "north".as_bytes().to_vec(),
                )])),
                delete: false,
            },
        ])
        .unwrap();

        // 4. Verify additional series.
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_additional_series_series_id(fs);
        }
    }

    // Ensure fileset can return an iterator over all series for one measurement.
    #[test]
    fn test_file_setfile_set_measurement_series_id_iterator() {
        let mut idx = index::tests::must_open_index(1);

        // 1. Create initial set of series.
        idx.create_series_slice_if_not_exists(vec![
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
        ])
        .unwrap();

        // 2. Verify initial set of series.
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_initial_series_measurement_series_id(fs);
        }

        // 3. Add more series.
        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "disk".as_bytes().to_vec(),
                ..Default::default()
            },
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "north".as_bytes().to_vec(),
                )])),
                delete: false,
            },
        ])
        .unwrap();

        // 4. Verify additional series.
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_additional_series_measurement_series_id(fs);
        }
    }

    // Ensure fileset can return an iterator over all measurements for the index.
    #[test]
    fn test_file_setfile_set_measurement_iterator() {
        let mut idx = index::tests::must_open_index(1);

        // 1. Create initial set of series.
        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "cpu".as_bytes().to_vec(),
                ..Default::default()
            },
            Series {
                name: "mem".as_bytes().to_vec(),
                ..Default::default()
            },
        ])
        .unwrap();

        // 2. Verify initial set of series.
        // 2.1 Invoke immediately.
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_initial_series_measurement_iterator(fs);
        }

        // 2.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_initial_series_measurement_iterator(fs);
        }

        // TODO: 2.3 Request a compaction.
        // if err := idx.Compact(); err != nil {
        // 	t.Fatalf("compact error: %s", err)
        // }
        // t.Run("state=post-compaction", fn)

        // 2.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "post-compaction reopen failed");
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_initial_series_measurement_iterator(fs);
        }

        // 3. Add more series.
        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "disk".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([
                    ("region".as_bytes().to_vec(), "north".as_bytes().to_vec()),
                    ("x".as_bytes().to_vec(), "y".as_bytes().to_vec()),
                ])),
                delete: false,
            },
        ])
        .unwrap();

        // 4. Verify additional series.
        // 4.1 Invoke immediately.
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_additional_series_measurement_iterator(fs);
        }

        // 4.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_additional_series_measurement_iterator(fs);
        }

        // TODO: 4.3 Request a compaction.
        // if err := idx.Compact(); err != nil {
        // 	t.Fatalf("compact error: %s", err)
        // }
        // t.Run("state=post-compaction", fn)

        // 4.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "post-compaction reopen failed");
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_additional_series_measurement_iterator(fs);
        }
    }

    // Ensure fileset can return an iterator over all keys for one measurement.
    #[test]
    fn test_file_setfile_set_tag_key_iterator() {
        let mut idx = index::tests::must_open_index(1);

        // 1. Create initial set of series.
        idx.create_series_slice_if_not_exists(vec![
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
                tags: crate::models::points::Tags::from(HashMap::from([
                    ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                    ("type".as_bytes().to_vec(), "gpu".as_bytes().to_vec()),
                ])),
                delete: false,
            },
            Series {
                name: "mem".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([
                    ("region".as_bytes().to_vec(), "east".as_bytes().to_vec()),
                    ("misc".as_bytes().to_vec(), "other".as_bytes().to_vec()),
                ])),
                delete: false,
            },
        ])
        .unwrap();

        // 2. Verify initial set of series.
        // 2.1 Invoke immediately.
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_initial_series_tag_key_iterator(fs);
        }

        // 2.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_initial_series_tag_key_iterator(fs);
        }

        // TODO: 2.3 Request a compaction.
        // if err := idx.Compact(); err != nil {
        // 	t.Fatalf("compact error: %s", err)
        // }
        // t.Run("state=post-compaction", fn)

        // 2.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "post-compaction reopen failed");
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_initial_series_tag_key_iterator(fs);
        }

        // 3. Add more series.
        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "disk".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([
                    ("region".as_bytes().to_vec(), "north".as_bytes().to_vec()),
                    ("x".as_bytes().to_vec(), "y".as_bytes().to_vec()),
                ])),
                delete: false,
            },
        ])
        .unwrap();

        // 4. Verify additional series.
        // 4.1 Invoke immediately.
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_additional_series_tag_key_iterator(fs);
        }

        // 4.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_additional_series_tag_key_iterator(fs);
        }

        // TODO: 4.3 Request a compaction.
        // if err := idx.Compact(); err != nil {
        // 	t.Fatalf("compact error: %s", err)
        // }
        // t.Run("state=post-compaction", fn)

        // 4.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "post-compaction reopen failed");
        {
            let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();
            vertify_additional_series_tag_key_iterator(fs);
        }
    }

    fn vertify_initial_series_series_id(fs: Arc<FileSet>) {
        defer! {
            fs.release();
        }

        let itr = fs.series_file().series_id_iterator();
        let res = must_read_all_series_id_iter_string(fs.series_file(), itr);
        assert_eq!(
            res,
            vec![
                "cpu,[{region east}]".to_string(),
                "cpu,[{region west}]".to_string(),
                "mem,[{region east}]".to_string()
            ],
            "unexpected keys: {:?}",
            res
        );
    }

    fn vertify_additional_series_series_id(fs: Arc<FileSet>) {
        defer! {
            fs.release();
        }

        let itr = fs.series_file().series_id_iterator();

        let res = must_read_all_series_id_iter_string(fs.series_file(), itr);
        assert_eq!(
            res,
            vec![
                "cpu,[{region east}]".to_string(),
                "cpu,[{region north}]".to_string(),
                "cpu,[{region west}]".to_string(),
                "disk,[]".to_string(),
                "mem,[{region east}]".to_string()
            ],
            "unexpected keys: {:?}",
            res
        );
    }

    fn vertify_initial_series_measurement_series_id(fs: Arc<FileSet>) {
        defer! {
            fs.release();
        }

        let itr = fs.measurement_series_id_iter("cpu".as_bytes());
        assert!(itr.is_some(), "expected iterator");
        let itr = itr.unwrap();
        let res = must_read_all_series_id_iter_string(fs.series_file(), itr);
        assert_eq!(
            res,
            vec![
                "cpu,[{region east}]".to_string(),
                "cpu,[{region west}]".to_string()
            ],
            "unexpected keys: {:?}",
            res
        );
    }

    fn vertify_additional_series_measurement_series_id(fs: Arc<FileSet>) {
        defer! {
            fs.release();
        }

        let itr = fs.measurement_series_id_iter("cpu".as_bytes());
        assert!(itr.is_some(), "expected iterator");
        let itr = itr.unwrap();
        let res = must_read_all_series_id_iter_string(fs.series_file(), itr);
        assert_eq!(
            res,
            vec![
                "cpu,[{region east}]".to_string(),
                "cpu,[{region north}]".to_string(),
                "cpu,[{region west}]".to_string()
            ],
            "unexpected keys: {:?}",
            res
        );
    }

    fn vertify_initial_series_measurement_iterator(fs: Arc<FileSet>) {
        defer! {
            fs.release();
        }

        let itr = fs.measurement_iter();
        assert!(itr.is_some(), "expected iterator");
        let mut itr = itr.unwrap();

        let expected_names = vec!["cpu", "mem", ""];
        for name in expected_names {
            let e = itr.next();
            if name == "" && e.is_some() {
                panic!(
                    "got measurement {:?}, expected None measurement",
                    e.unwrap().name().unwrap()
                );
            } else if e.is_none() && name != "" {
                panic!("got None measurement, expected {:?}", name);
            } else if e.is_some() && e.as_ref().unwrap().name().unwrap() != name.as_bytes() {
                panic!(
                    "got measurement {:?}, expected {:?}",
                    e.unwrap().name().unwrap(),
                    name
                );
            }
        }
    }

    fn vertify_additional_series_measurement_iterator(fs: Arc<FileSet>) {
        defer! {
            fs.release();
        }

        let itr = fs.measurement_iter();
        assert!(itr.is_some(), "expected iterator");
        let mut itr = itr.unwrap();

        let expected_names = vec!["cpu", "disk", "mem", ""];
        for name in expected_names {
            let e = itr.next();
            if name == "" && e.is_some() {
                panic!(
                    "got measurement {:?}, expected None measurement",
                    e.unwrap().name().unwrap()
                );
            } else if e.is_none() && name != "" {
                panic!("got None measurement, expected {:?}", name);
            } else if e.is_some() && e.as_ref().unwrap().name().unwrap() != name.as_bytes() {
                panic!(
                    "got measurement {:?}, expected {:?}",
                    e.unwrap().name().unwrap(),
                    name
                );
            }
        }
    }

    fn vertify_initial_series_tag_key_iterator(fs: Arc<FileSet>) {
        defer! {
            fs.release();
        }

        let itr = fs.tag_key_iter("cpu".as_bytes());
        assert!(itr.is_some(), "expected iterator");
        let mut itr = itr.unwrap();

        if let Some(e) = itr.next() {
            let k = e.key();
            assert_eq!(k, "region".as_bytes(), "unexpected key: {:?}", k);
        } else {
            panic!("expected tag key");
        }

        if let Some(e) = itr.next() {
            let k = e.key();
            assert_eq!(k, "type".as_bytes(), "unexpected key: {:?}", to_str!(k));
        } else {
            panic!("expected tag key");
        }

        let e = itr.next();
        assert!(e.is_none());
    }

    fn vertify_additional_series_tag_key_iterator(fs: Arc<FileSet>) {
        defer! {
            fs.release();
        }

        let itr = fs.tag_key_iter("cpu".as_bytes());
        assert!(itr.is_some(), "expected iterator");
        let mut itr = itr.unwrap();

        if let Some(e) = itr.next() {
            let k = e.key();
            assert_eq!(k, "region".as_bytes(), "unexpected key: {:?}", k);
        } else {
            panic!("expected tag key");
        }

        if let Some(e) = itr.next() {
            let k = e.key();
            assert_eq!(k, "type".as_bytes(), "unexpected key: {:?}", k);
        } else {
            panic!("expected tag key");
        }

        if let Some(e) = itr.next() {
            let k = e.key();
            assert_eq!(k, "x".as_bytes(), "unexpected key: {:?}", k);
        } else {
            panic!("expected tag key");
        }

        let e = itr.next();
        assert!(e.is_none());
    }

    fn must_read_all_series_id_iter_string(
        sfile: Arc<crate::series::series_file::SeriesFile>,
        itr: crate::index::SeriesIDIter,
    ) -> Vec<String> {
        // Read all ids.
        let ids = read_all_series_id_iter(itr); // FIXME: 这里应该是从1开始：1、2、3

        // Convert to keys and sort.
        let mut keys = sfile.series_keys(&ids);
        keys.sort_by(|a, b| compare_series_keys(a, b).unwrap());

        // Convert to strings.
        let mut a = vec![];
        unsafe {
            for k in keys.iter() {
                let (name, tags) = parse_series_key(k).unwrap();
                a.push(format!(
                    "{},{}",
                    String::from_utf8_unchecked(name),
                    tags.to_string()
                ));
            }
        }
        a
    }

    // Returns all ids from the iterator.
    fn read_all_series_id_iter(itr: crate::index::SeriesIDIter) -> Vec<u64> {
        let mut a = vec![];
        for elem in itr {
            a.push(elem.series_id);
        }
        a
    }
}
