//! Time Series Index.
use std::{
    collections::{HashMap, HashSet},
    fs,
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use crate::common::{hll::HLL, to_str, to_string};
use crate::models::{
    Val,
    points::{append_make_key, make_tags_key},
};
use scopeguard::{defer, guard};
use snafu::{OptionExt, ResultExt, Snafu, ensure};
use tsi1::{index::Index as Tsi1Index, partition::VERSION};

use crate::{
    config::{DEFAULT_MAX_INDEX_LOG_FILE_SIZE, DEFAULT_SERIES_ID_SET_CACHE_SIZE},
    index::tsi1::{
        file_set::{
            self, OneFileSetMeasurementIter, OneFileSetSeriesIDSetIter, OneFileSetTagKeyIter,
            OneFileSetTagValueIter,
        },
        measurement_block::RawSeriesIDIter,
    },
    query::{
        query_iterator::{self, QueryIterator},
        query_point,
        storage_ir::{ScanCancellation, StoragePredicate},
    },
    runtime::execution::ExecutorRef,
    series::{
        measurement_fields::MeasurementFieldSet,
        series_file::{self, SeriesFile, compare_series_keys, parse_series_key},
        series_set::{self, SeriesIDSet},
    },
    storage::EngineOptions,
};

pub const TSI1_INDEX_NAME: &str = "tsi1";

mod storage_predicate;
#[cfg(test)]
mod storage_predicate_tests;
pub mod tsi1;

/// Statically selected index implementation exposed through the capabilities used by upper layers.
pub(crate) struct ActiveIndex {
    implementation: Tsi1Index,
}

impl ActiveIndex {
    #[cfg(test)]
    fn from_implementation(implementation: Tsi1Index) -> Self {
        Self { implementation }
    }

    pub(crate) fn new_with_executor(
        sfile: Arc<SeriesFile>,
        database: String,
        path: impl Into<std::path::PathBuf>,
        max_index_log_file_size: i64,
        series_idset_cache_size: i64,
        executor: ExecutorRef,
    ) -> Self {
        Self {
            implementation: Tsi1Index::new_with_executor(
                sfile,
                database,
                path,
                max_index_log_file_size,
                series_idset_cache_size,
                executor,
            ),
        }
    }

    fn implementation(&self) -> &Tsi1Index {
        &self.implementation
    }

    pub(crate) fn open(&self) -> Result<()> {
        self.implementation.open().map_err(Error::from)
    }

    pub(crate) fn close(&self) -> Result<()> {
        self.implementation.close().map_err(Error::from)
    }

    pub(crate) fn index_type(&self) -> &str {
        self.implementation.index_type()
    }

    pub(crate) fn series_file(&self) -> Arc<SeriesFile> {
        self.implementation.series_file()
    }

    pub(crate) fn series_id_set(&self) -> series_set::SeriesIDSet {
        self.implementation.series_id_set()
    }

    pub(crate) fn set_field_set(&self, fields: Arc<MeasurementFieldSet>) {
        self.implementation.set_field_set(fields);
    }

    pub(crate) fn measurement_exists(&self, name: &[u8]) -> Result<bool> {
        self.implementation
            .measurement_exists(name)
            .map_err(Error::from)
    }

    pub(crate) fn has_tag_key(&self, name: &[u8], key: &[u8]) -> Result<bool> {
        self.implementation
            .has_tag_key(name, key)
            .map_err(Error::from)
    }

    pub(crate) fn measurement_tag_keys(&self, name: &[u8]) -> Result<HashSet<String>> {
        self.implementation
            .measurement_tag_keys(name)
            .map_err(Error::from)
    }

    #[cfg(feature = "slow-trace")]
    pub(crate) fn measurement_tag_keys_profiled(
        &self,
        name: &[u8],
        diagnostics: &Arc<MeasurementTagKeysDiagnostics>,
    ) -> Result<HashSet<String>> {
        self.implementation
            .measurement_tag_keys_profiled(name, Arc::clone(diagnostics))
            .map_err(Error::from)
    }

    pub(crate) fn tag_key_cardinality(&self, name: &[u8], key: &[u8]) -> i64 {
        self.implementation.tag_key_cardinality(name, key)
    }

    pub(crate) fn series_n(&self) -> i64 {
        self.implementation.series_n()
    }

    pub(crate) fn measurements_sketches(&self) -> Result<(HLL, HLL)> {
        self.implementation
            .measurements_sketches()
            .map_err(Error::from)
    }

    pub(crate) fn series_sketches(&self) -> Result<(HLL, HLL)> {
        self.implementation.series_sketches().map_err(Error::from)
    }

    pub(crate) fn drop_series(&self, series_id: u64, key: &[u8], cascade: bool) -> Result<()> {
        self.implementation
            .drop_series(series_id, key, cascade)
            .map_err(Error::from)
    }

    pub(crate) fn drop_measurement(&self, name: &[u8]) -> Result<()> {
        self.implementation
            .drop_measurement(name)
            .map_err(Error::from)
    }

    pub(crate) fn measurement_iter(&self) -> Result<Option<MeasurementMergeIter>> {
        self.implementation.measurement_iter().map_err(Error::from)
    }

    pub(crate) fn measurement_series_id_iter(&self, name: &[u8]) -> Result<Option<FileSetSeriesIDIter>> {
        self.implementation.measurement_series_id_iter(name).map_err(Error::from)
    }
    pub(crate) fn drop_series_if(&self, series_id: u64, key: &[u8], unchanged: impl FnOnce() -> bool) -> Result<bool> {
        self.implementation.drop_series_if(series_id, key, unchanged).map_err(Error::from)
    }
    pub(crate) fn measurement_has_series(&self, name: &[u8]) -> Result<bool> {
        self.implementation.measurement_has_series(name).map_err(Error::from)
    }

    pub(crate) fn contains_series_id(&self, series_id: u64, key: &[u8]) -> bool {
        self.implementation.contains_series_id(series_id, key)
    }

    pub(crate) fn drop_measurement_if_series_not_exist(&self, name: &[u8]) -> Result<bool> {
        self.implementation
            .drop_measurement_if_series_not_exist(name)
            .map_err(Error::from)
    }

    pub(crate) fn create_series_if_not_exists(
        &self,
        key: &[u8],
        name: &[u8],
        tags: crate::models::Tags,
    ) -> Result<()> {
        self.implementation
            .create_series_if_not_exists(key, name, tags)
            .map_err(Error::from)
    }

    pub(crate) fn for_each_measurement_name<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(&[u8]),
    {
        self.implementation
            .for_each_measurement_name(f)
            .map_err(Error::from)
    }

    pub(crate) fn create_series_list_if_not_exists(
        &self,
        keys: &[&[u8]],
        names: Vec<Vec<u8>>,
        tags: Vec<crate::models::Tags>,
    ) -> Result<()> {
        self.implementation
            .create_series_list_if_not_exists(keys, names, tags)
            .map_err(Error::from)
    }

    pub(crate) fn lru_series_keys_exists(&self, key: &str) -> bool {
        self.implementation.lru_series_keys_exists(key)
    }

    pub(crate) fn lru_series_keys_is_empty(&self) -> bool {
        self.implementation.lru_series_keys_is_empty()
    }

    pub(crate) fn retain_for_delete(self: &Arc<Self>) -> Result<IndexDeleteGuard> {
        self.implementation.disable_compactions();
        self.implementation.wait();
        match self.implementation.retain_file_set() {
            Ok(file_set) => Ok(IndexDeleteGuard {
                index: Arc::clone(self),
                file_set: Some(file_set),
            }),
            Err(source) => {
                self.implementation.enable_compactions();
                Err(Error::from(source))
            }
        }
    }
}

/// Keeps index files stable while a storage delete operation installs tombstones.
pub(crate) struct IndexDeleteGuard {
    index: Arc<ActiveIndex>,
    file_set: Option<tsi1::file_set::FileSet>,
}

impl IndexDeleteGuard {
    pub(crate) fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if let Some(file_set) = self.file_set.take() {
            file_set.release();
            self.index.implementation.enable_compactions();
        }
    }
}

impl Drop for IndexDeleteGuard {
    fn drop(&mut self) {
        self.release_inner();
    }
}

/// Error type for Index.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", source))]
    IOError { source: std::io::Error },

    /// Index closing.
    #[snafu(display("index is closing"))]
    IndexClosing,

    /// Index error.
    #[snafu(display("tsdb index error: {}", source))]
    Index {
        source: crate::index::tsi1::index::Error,
    },

    /// Series file error.
    #[snafu(display("tsdb series file error: {}", source))]
    SeriesFile {
        source: crate::series::series_file::Error,
    },

    /// Measurement block error.
    #[snafu(display("tsdb measurement error: {}", source))]
    MeasurementBlock {
        source: crate::index::tsi1::measurement_block::Error,
    },

    /// Storage-native predicate filtering was cancelled.
    #[snafu(display("storage predicate filtering cancelled"))]
    StoragePredicateCancelled,

    /// Storage-native scan planning was cancelled.
    #[snafu(display("storage scan cancelled"))]
    StorageScanCancelled,

    /// String error.
    #[snafu(display("tsi: {}", reason))]
    StringErr { reason: String },
}

/// Result type for Index.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<tsi1::index::Error> for Error {
    fn from(source: tsi1::index::Error) -> Self {
        Self::Index { source }
    }
}

/// SeriesElem represents a generic series element.
pub trait SeriesElem {
    fn name(&self) -> &[u8];
    fn tags(&self) -> &crate::models::points::Tags;
    fn deleted(&self) -> bool;

    // Query IR expression associated with series during filtering.
}

pub trait SeriesIterator: Iterator<Item = Box<dyn SeriesElem>> {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>>;
}

pub struct SeriesIterAdapter {
    sfile: Arc<series_file::SeriesFile>,
    itr: FileSetSeriesIDIter,
}

pub fn new_series_iterator_adapter(
    sfile: Arc<series_file::SeriesFile>,
    itr: FileSetSeriesIDIter,
) -> SeriesIterAdapter {
    SeriesIterAdapter { sfile, itr }
}

impl SeriesIterator for SeriesIterAdapter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.itr.close()
    }
}

impl Iterator for SeriesIterAdapter {
    type Item = Box<dyn SeriesElem>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(elem) = self.itr.next() {
                if elem.series_id == 0 {
                    return None;
                }
                // skip if this key has been tombstoned
                let key = self.sfile.series_key(elem.series_id);
                if !key.is_empty() {
                    let (name, tags) = series_file::parse_series_key(key.as_slice()).unwrap();
                    let deleted = self.sfile.is_deleted(elem.series_id);

                    return Some(Box::new(SeriesElemAdapter {
                        name,
                        tags,
                        deleted,
                    }));
                } else {
                    continue;
                }
            }

            return None;
        }
    }
}

/// An adapter for converting series ids to series.
pub struct FilterUndeletedSeriesIterAdapter {
    sfile: Arc<series_file::SeriesFile>,
    itr: FilterUndeletedSeriesIDIter,
}

pub fn new_filter_undeleted_series_iterator_adapter(
    sfile: Arc<series_file::SeriesFile>,
    itr: FilterUndeletedSeriesIDIter,
) -> FilterUndeletedSeriesIterAdapter {
    FilterUndeletedSeriesIterAdapter { sfile, itr }
}

impl SeriesIterator for FilterUndeletedSeriesIterAdapter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.itr.close()
    }
}

impl Iterator for FilterUndeletedSeriesIterAdapter {
    type Item = Box<dyn SeriesElem>;

    // type Item = SeriesElemAdapter;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(elem) = self.itr.next() {
                if elem.series_id == 0 {
                    return None;
                }
                // skip if this key has been tombstoned
                let key = self.sfile.series_key(elem.series_id);
                if !key.is_empty() {
                    let (name, tags) = series_file::parse_series_key(key.as_slice()).unwrap();
                    let deleted = self.sfile.is_deleted(elem.series_id);

                    return Some(Box::new(SeriesElemAdapter {
                        name,
                        tags,
                        deleted,
                    }));
                } else {
                    continue;
                }
            }

            return None;
        }
    }
}

pub struct SeriesElemAdapter {
    name: Vec<u8>,
    tags: crate::models::points::Tags,
    deleted: bool,
    // expr    query IR expression
}

impl SeriesElem for SeriesElemAdapter {
    fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    fn tags(&self) -> &crate::models::points::Tags {
        &self.tags
    }

    fn deleted(&self) -> bool {
        self.deleted
    }
}

/// Represents a iterator over a list of series ids.
pub trait SeriesIDIterator: Iterator<Item = SeriesIDElem> {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>>;
    fn as_any(&self) -> &dyn std::any::Any;
}

// /// Represents an iterator that can produce a SeriesIDSet.
// pub trait SeriesIDSetIterator: Iterator {
//     fn series_id_set(&self) -> Arc<series_set::SeriesIDSet>;
// }

pub enum SeriesIDIter {
    SeriesIDSetIter(SeriesIDSetIter),
    SeriesIDSliceIter(SeriesIDSliceIter),
    MeasurementRawSeriesIDIter(RawSeriesIDIter),
    SeriesIDMergeIter(SeriesIDMergeIter),
    // TODO: belows are used for query iterator in engine
    // SeriesIDIntersectIter(SeriesIDIntersectIter),
    // SeriesIDUnionIter(SeriesIDUnionIter),
}

impl SeriesIDIter {
    pub fn series_id_set(&self) -> Option<Arc<series_set::SeriesIDSet>> {
        match self {
            SeriesIDIter::SeriesIDSetIter(itr) => Some(itr.series_id_set()),
            SeriesIDIter::SeriesIDSliceIter(itr) => Some(itr.series_id_set()),
            SeriesIDIter::MeasurementRawSeriesIDIter(itr) => Some(itr.series_id_set()),
            _ => None,
        }
    }
}

impl SeriesIDIterator for SeriesIDIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        match self {
            SeriesIDIter::SeriesIDSetIter(itr) => itr.close(),
            SeriesIDIter::SeriesIDSliceIter(itr) => itr.close(),
            SeriesIDIter::MeasurementRawSeriesIDIter(itr) => itr.close(),
            SeriesIDIter::SeriesIDMergeIter(itr) => itr.close(),
            // SeriesIDIter::SeriesIDIntersectIter(itr) => itr.close(),
            // SeriesIDIter::SeriesIDUnionIter(itr) => itr.close(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        match self {
            SeriesIDIter::SeriesIDSetIter(itr) => itr,
            SeriesIDIter::SeriesIDSliceIter(itr) => itr,
            SeriesIDIter::MeasurementRawSeriesIDIter(itr) => itr,
            SeriesIDIter::SeriesIDMergeIter(itr) => itr,
            // SeriesIDIter::SeriesIDIntersectIter(itr) => itr.close(),
            // SeriesIDIter::SeriesIDUnionIter(itr) => itr.close(),
        }
    }
}

impl Iterator for SeriesIDIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SeriesIDIter::SeriesIDSetIter(itr) => itr.next(),
            SeriesIDIter::SeriesIDSliceIter(itr) => itr.next(),
            SeriesIDIter::MeasurementRawSeriesIDIter(itr) => itr.next(),
            SeriesIDIter::SeriesIDMergeIter(itr) => itr.next(),
            // SeriesIDIter::SeriesIDIntersectIter(itr) => itr.next(),
            // SeriesIDIter::SeriesIDUnionIter(itr) => itr.next(),
        }
    }
}
pub struct SeriesIDSetIter {
    ss: Arc<series_set::SeriesIDSet>,
    itr: Box<dyn Iterator<Item = SeriesIDElem>>,
}

pub fn new_series_id_set_iter(ss: series_set::SeriesIDSet) -> SeriesIDSetIter {
    let itr = ss.iterator_no_lock().map(|x| SeriesIDElem {
        series_id: x as u64,
    });
    SeriesIDSetIter {
        ss: Arc::new(ss),
        itr: Box::new(itr),
    }
}

impl SeriesIDSetIter {
    pub fn series_id_set(&self) -> Arc<series_set::SeriesIDSet> {
        Arc::clone(&self.ss)
    }
}

impl SeriesIDIterator for SeriesIDSetIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for SeriesIDSetIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        self.itr.next()
    }
}

/// Returns a slice of SeriesIDSetIterator if all itrs can be type casted.
/// Otherwise returns None.
pub fn new_series_id_set_iters(itrs: &[SeriesIDIter]) -> Option<Vec<&SeriesIDSetIter>> {
    if itrs.is_empty() {
        return None;
    }

    let mut a = Vec::with_capacity(itrs.len());
    for itr in itrs {
        match itr {
            SeriesIDIter::SeriesIDSetIter(itr) => {
                a.push(itr);
            }
            _ => {
                return None;
            }
        }
    }

    Some(a)
}

pub fn new_series_id_set_iters2(
    itrs: &[Box<dyn SeriesIDIterator>],
) -> Option<Vec<&SeriesIDSetIter>> {
    if itrs.is_empty() {
        return None;
    }

    let mut a = Vec::with_capacity(itrs.len());
    for itr in itrs {
        // if let Some(itr) = itr.as_any().downcast_ref::<SeriesIDSetIter>() {
        // if let Some(itr) = (*itr).as_any().downcast_ref::<SeriesIDSetIter>() {
        if let Some(itr) = (*itr)
            .as_any()
            .downcast_ref::<crate::index::FileSetSeriesIDIter>()
        {
            if let crate::index::FileSetSeriesIDIter::SeriesIDSetIter(itr) = itr {
                a.push(itr)
            }
        } else if let Some(itr) = (*itr).as_any().downcast_ref::<SeriesIDSetIter>() {
            a.push(itr)
        } else {
            return None;
        }
    }

    Some(a)
}

/// Returns all ids from the iterator.
pub fn read_all_series_id_iterator(
    itr: &mut impl Iterator<Item = SeriesIDElem>,
) -> Option<Vec<SeriesIDElem>> {
    let mut a = Vec::<SeriesIDElem>::new();
    loop {
        match itr.next() {
            Some(id) => {
                if id.series_id == 0 {
                    break;
                }
                a.push(id);
            }
            _ => {
                return None;
            }
        }
    }
    Some(a)
}

/// Iterates over a slice of series ids.
pub struct SeriesIDSliceIter {
    ids: Vec<u64>,
    iter_i: usize,
}

pub fn new_series_id_slice_iterator(ids: Vec<u64>) -> SeriesIDIter {
    SeriesIDIter::SeriesIDSliceIter(SeriesIDSliceIter {
        ids,
        iter_i: 0usize,
    })
}

impl Iterator for SeriesIDSliceIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ids.is_empty() || self.iter_i == self.ids.len() {
            return None;
        }
        let curr_iter_i = self.iter_i;
        self.iter_i += 1;

        Some(SeriesIDElem {
            series_id: self.ids[curr_iter_i],
        })
    }
}

impl SeriesIDIterator for SeriesIDSliceIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl SeriesIDSliceIter {
    fn series_id_set(&self) -> Arc<series_set::SeriesIDSet> {
        let s = series_set::SeriesIDSet::new(&[]);
        for id in &self.ids {
            s.add_no_lock(*id);
        }
        Arc::new(s)
    }
}

struct SeriesIDIters<'a>(&'a [SeriesIDIter]);

// impl SeriesIDIterators {
//     fn filter_non_none(&self) -> Self {
//         let mut other = Vec::<Box<dyn SeriesIDIterator>>::with_capacity(self.0.len());
//         for itr in self.0 {
//             if itr.is_none() {
//                 continue;
//             }
//             other.push(itr)
//         }
//         SeriesIDIterators(other)
//     }
// }

impl<'a> SeriesIDIters<'a> {
    fn close(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut err: Option<Box<dyn std::error::Error>> = None;
        let siiv = self.0.iter();
        for itr in siiv {
            if let Err(e) = itr.close() {
                if err.is_none() {
                    err = Some(e);
                }
            }
        }

        if let Some(e) = err { Err(e) } else { Ok(()) }
    }
}

impl<'a> Deref for SeriesIDIters<'a> {
    type Target = &'a [SeriesIDIter];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SeriesIDIters<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// SeriesIDElem represents a single series and optional expression.
#[derive(Clone, Default, Debug)]
pub struct SeriesIDElem {
    pub series_id: u64,
}

impl PartialEq for SeriesIDElem {
    fn eq(&self, other: &Self) -> bool {
        self.series_id == other.series_id
    }
}

impl PartialOrd for SeriesIDElem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.series_id.partial_cmp(&other.series_id)
    }
}

pub struct SeriesIDElems(Vec<SeriesIDElem>);

impl SeriesIDElems {
    pub fn less(&mut self, i: usize, j: usize) -> bool {
        self.0[i] < self.0[j]
    }
}

impl Deref for SeriesIDElems {
    type Target = Vec<SeriesIDElem>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SeriesIDElems {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Returns an iterator that merges a set of iterators.
/// Iterators that are first in the list take precedence and a deletion by those
/// early iterators will invalidate elements by later iterators.
pub fn merge_series_id_iterators(mut itrs: Vec<SeriesIDIter>) -> Option<SeriesIDIter> {
    if itrs.is_empty() {
        return None;
    } else if itrs.len() == 1 {
        return itrs.pop();
    }

    // There is no need to filter non None.
    // let itrs = SeriesIDIterators(itrs)/*.filter_non_none()*/;

    // Merge as series id sets, if available.
    if let Some(a) = new_series_id_set_iters(&itrs) {
        let mut sets = Vec::with_capacity(itrs.len());
        for itr in a {
            sets.push(itr.series_id_set());
        }

        let ss = series_set::SeriesIDSet::new(&[]);
        ss.merge(
            sets.iter()
                .map(|item| item.as_ref())
                .collect::<Vec<&SeriesIDSet>>()
                .as_slice(),
        );
        SeriesIDIters(&itrs).close().ok();

        return Some(SeriesIDIter::SeriesIDSetIter(new_series_id_set_iter(ss)));
    }

    Some(SeriesIDIter::SeriesIDMergeIter(SeriesIDMergeIter {
        buf: Vec::<SeriesIDElem>::with_capacity(itrs.len()),
        itrs,
    }))
}

pub struct SeriesIDMergeIter {
    buf: Vec<SeriesIDElem>,
    itrs: Vec<SeriesIDIter>,
}

impl SeriesIDIterator for SeriesIDMergeIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        SeriesIDIters(self.itrs.as_slice()).close()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for SeriesIDMergeIter {
    type Item = SeriesIDElem;

    // Returns the element with the next lowest name/tags across the iterators.
    fn next(&mut self) -> Option<Self::Item> {
        let mut elem = SeriesIDElem::default();
        for (i, buf) in self.buf.iter_mut().enumerate() {
            // Fill buffer.
            if buf.series_id == 0 {
                let itrs = &mut self.itrs;
                match itrs[i].next() {
                    Some(id) => {
                        if id.series_id == 0 {
                            continue;
                        }
                        elem = id;
                    }
                    _ => {
                        return None;
                    }
                }
                *buf = elem.clone();
            }

            if elem.series_id == 0 || *buf < elem {
                elem = buf.clone();
            }
        }

        // Return EOF if no elements remaining.
        if elem.series_id == 0 {
            return None;
        }

        // Clear matching buffers.
        for sie in self.buf.iter_mut() {
            if *sie == elem {
                *sie = SeriesIDElem::default();
            }
        }

        Some(elem)
    }
}

// /////////////////// merge file set measurement iters ////////

/// Returns a slice of SeriesIDSetIterator if all itrs can be type casted.
/// Otherwise returns None.
pub fn new_file_set_series_id_set_iters(
    itrs: &[crate::index::tsi1::file_set::FileSetSeriesIDIter],
) -> Option<Vec<&OneFileSetSeriesIDSetIter>> {
    if itrs.is_empty() {
        return None;
    }

    let mut a = Vec::with_capacity(itrs.len());
    for itr in itrs {
        match itr {
            crate::index::tsi1::file_set::FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr) => {
                a.push(itr);
            }
            _ => {
                return None;
            }
        }
    }

    Some(a)
}

/// Returns an iterator that merges a set of iterators.
/// Iterators that are first in the list take precedence and a deletion by those
/// early iterators will invalidate elements by later iterators.
pub fn merge_file_set_series_id_iters(
    mut itrs: Vec<crate::index::tsi1::file_set::FileSetSeriesIDIter>,
) -> Option<FileSetSeriesIDIter> {
    if itrs.is_empty() {
        return None;
    } else if itrs.len() == 1 {
        // return Some(FileSetSeriesIDIter::FileSetSeriesIDMergeIter(
        //     FileSetSeriesIDMergeIter {
        //         buf: Vec::<SeriesIDElem>::with_capacity(1usize),
        //         itrs,
        //     },
        // ));
        let itr = match itrs.pop().unwrap() {
            crate::index::tsi1::file_set::FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr) => {
                itr
            }
            _ => unreachable!("merge_file_set_series_id_iters invalid itr"),
        };
        return Some(FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr));
    }

    // There is no need to filter non None.
    // let itrs = SeriesIDIterators(itrs)/*.filter_non_none()*/;

    // Merge as series id sets, if available.
    if let Some(a) = new_file_set_series_id_set_iters(itrs.as_slice()) {
        let mut sets = Vec::with_capacity(itrs.len());
        for itr in a {
            sets.push(itr.series_id_set());
        }

        let ss = series_set::SeriesIDSet::new(&[]);
        ss.merge(
            sets.iter()
                .map(|item| &**item.as_ref().unwrap())
                .collect::<Vec<&SeriesIDSet>>()
                .as_slice(),
        );
        FileSetSeriesIDIters(&itrs).close().ok();

        return Some(FileSetSeriesIDIter::SeriesIDSetIter(
            new_series_id_set_iter(ss),
        ));
    }

    Some(FileSetSeriesIDIter::FileSetSeriesIDMergeIter(
        FileSetSeriesIDMergeIter {
            buf: Vec::<SeriesIDElem>::with_capacity(itrs.len()),
            itrs,
        },
    ))
}

pub enum FileSetSeriesIDIter {
    SeriesIDSetIter(SeriesIDSetIter),
    OneFileSetSeriesIDSetIter(OneFileSetSeriesIDSetIter),
    FileSetSeriesIDMergeIter(FileSetSeriesIDMergeIter),
    // FilterUndeletedSeriesIDIter(FilterUndeletedSeriesIDIter),
}

impl SeriesIDIterator for FileSetSeriesIDIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        match self {
            FileSetSeriesIDIter::SeriesIDSetIter(itr) => itr.close(),
            FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr) => itr.close(),
            FileSetSeriesIDIter::FileSetSeriesIDMergeIter(itr) => itr.close(),
            // FileSetSeriesIDIter::FilterUndeletedSeriesIDIter(itr) => itr.close(),
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
            FileSetSeriesIDIter::SeriesIDSetIter(itr) => itr.next(),
            FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr) => itr.next(),
            FileSetSeriesIDIter::FileSetSeriesIDMergeIter(itr) => itr.next(),
            // FileSetSeriesIDIter::FilterUndeletedSeriesIDIter(itr) => itr.next(),
        }
    }
}

pub struct FileSetSeriesIDMergeIter {
    buf: Vec<SeriesIDElem>,
    itrs: Vec<crate::index::tsi1::file_set::FileSetSeriesIDIter>,
}

impl SeriesIDIterator for FileSetSeriesIDMergeIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        Ok(FileSetSeriesIDIters(self.itrs.as_slice()).close()?)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for FileSetSeriesIDMergeIter {
    type Item = SeriesIDElem;

    // Returns the element with the next lowest name/tags across the iterators.
    fn next(&mut self) -> Option<Self::Item> {
        let mut elem = SeriesIDElem::default();
        for (i, buf) in self.buf.iter_mut().enumerate() {
            // Fill buffer.
            if buf.series_id == 0 {
                let itrs = &mut self.itrs;
                match itrs[i].next() {
                    Some(id) => {
                        if id.series_id == 0 {
                            continue;
                        }
                        elem = id;
                    }
                    _ => {
                        return None;
                    }
                }
                *buf = elem.clone();
            }

            if elem.series_id == 0 || *buf < elem {
                elem = buf.clone();
            }
        }

        // Return EOF if no elements remaining.
        if elem.series_id == 0 {
            return None;
        }

        // Clear matching buffers.
        for sie in self.buf.iter_mut() {
            if *sie == elem {
                *sie = SeriesIDElem::default();
            }
        }

        Some(elem)
    }
}

struct FileSetSeriesIDIters<'a>(&'a [crate::index::tsi1::file_set::FileSetSeriesIDIter]);

impl<'a> FileSetSeriesIDIters<'a> {
    fn close(&self) -> Result<()> {
        let mut err = None;
        let itrs = self.0.iter();
        for itr in itrs {
            if let Err(e) = itr.close() {
                if err.is_none() {
                    err = Some(e);
                }
            }
        }

        if let Some(e) = err {
            StringErrSnafu {
                reason: format!("{:?}", e),
            }
            .fail()
        } else {
            Ok(())
        }
    }
}

impl<'a> Deref for FileSetSeriesIDIters<'a> {
    type Target = &'a [crate::index::tsi1::file_set::FileSetSeriesIDIter];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FileSetSeriesIDIters<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// /////////////////////////////////////////////
// ///////////// measurement  //////////////////
// /////////////////////////////////////////////

/// Returns an iterator that merges a set of iterators (iterators in all index
/// partitions under `engine/data/<database>/<shard-id>/index/0~7`).
/// Iterators that are first in the list take precedence and a deletion by those
/// early iterators will invalidate elements by later iterators.
pub fn merge_measurement_iters(
    itrs: Vec<OneFileSetMeasurementIter>,
) -> Option<MeasurementMergeIter> {
    if itrs.is_empty() {
        return None;
    }

    Some(MeasurementMergeIter {
        buf: vec![None; itrs.len()],
        itrs,
    })
}

pub struct MeasurementMergeIter {
    buf: Vec<Option<Vec<u8>>>,
    itrs: Vec<OneFileSetMeasurementIter>,
}

impl MeasurementMergeIter {
    pub fn close(&self) -> Result<(), file_set::Error> {
        let mut err = None;
        for itr in self.itrs.iter() {
            if let Err(e) = itr.close() {
                if err.is_none() {
                    err = Some(e);
                }
            }
        }

        if let Some(e) = err {
            return Err(e);
        }
        Ok(())
    }
}

impl Iterator for MeasurementMergeIter {
    type Item = Vec<u8>;

    // Next returns the element with the next lowest name across the iterators.
    //
    // If multiple iterators contain the same name then the first is returned
    // and the remaining ones are skipped.
    fn next(&mut self) -> Option<Self::Item> {
        // Find next lowest name amongst the buffers.
        let mut name = vec![];
        for (i, buf) in self.buf.iter_mut().enumerate() {
            // Fill buffer if empty.
            if buf.is_none() {
                if let Some(b) = self.itrs[i].next() {
                    *buf = Some(b);
                } else {
                    continue;
                }
            }

            // Find next lowest name.
            if name.is_empty()
                || buf.as_ref().unwrap().as_slice().cmp(&name) == std::cmp::Ordering::Less
            {
                name.clone_from(buf.as_ref().unwrap());
            }
        }

        // Return nil if no elements remaining.
        if name.is_empty() {
            return None;
        }

        // Merge all elements together and clear buffers.
        for buf in self.buf.iter_mut() {
            if buf.is_none() || !buf.as_ref().unwrap().as_slice().eq(&name) {
                continue;
            }
            *buf = None;
        }
        Some(name)
    }
}

// /////////////////////////////////////////////
// ///////////// tag key  //////////////////////
// /////////////////////////////////////////////

/// Rreturns an iterator that merges a set of iterators.
pub fn merge_tag_key_iters(itrs: Vec<OneFileSetTagKeyIter>) -> Option<TagKeyMergeIter> {
    if itrs.is_empty() {
        return None;
    }

    Some(TagKeyMergeIter {
        buf: vec![None; itrs.len()],
        itrs,
    })
}

pub struct TagKeyMergeIter {
    buf: Vec<Option<Vec<u8>>>,
    itrs: Vec<OneFileSetTagKeyIter>,
}

impl TagKeyMergeIter {
    pub fn close(&self) -> Result<()> {
        for itr in self.itrs.iter() {
            let _ = itr.close();
        }

        Ok(())
    }
}

impl Iterator for TagKeyMergeIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // Find next lowest key amongst the buffers.
        let mut key = vec![];
        for (i, buf) in self.buf.iter_mut().enumerate() {
            if buf.is_none() {
                if let Some(b) = self.itrs[i].next() {
                    // self.buf[i] = Some(Box::new(buf));
                    *buf = Some(b);
                } else {
                    continue;
                }
            }

            // Find next lowest key.
            if key.is_empty()
                || buf.as_ref().unwrap().as_slice().cmp(&key) == std::cmp::Ordering::Less
            {
                key = buf.as_ref().unwrap().as_slice().to_vec();
            }
        }

        // Return nil if no elements remaining.
        if key.is_empty() {
            return None;
        }

        // Merge all elements together and clear buffers.
        for buf in self.buf.iter_mut() {
            if buf.is_none() || !buf.as_ref().unwrap().as_slice().eq(&key) {
                continue;
            }
            *buf = None;
        }
        Some(key)
    }
}

// /////////////////////////////////////////////
// ///////////// tag value  ////////////////////
// /////////////////////////////////////////////

/// Returns an iterator that merges a set of iterators.
pub fn merge_tag_value_iters(itrs: Vec<OneFileSetTagValueIter>) -> Option<TagValueMergeIter> {
    if itrs.is_empty() {
        return None;
    }

    Some(TagValueMergeIter {
        buf: vec![None; itrs.len()],
        itrs,
    })
}

pub struct TagValueMergeIter {
    buf: Vec<Option<Vec<u8>>>,
    itrs: Vec<OneFileSetTagValueIter>,
}

impl TagValueMergeIter {
    pub fn close(&self) -> Result<()> {
        for itr in self.itrs.iter() {
            let _ = itr.close();
        }

        Ok(())
    }
}

impl Iterator for TagValueMergeIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // Find next lowest value amongst the buffers.
        let mut value = vec![];
        for (i, buf) in self.buf.iter_mut().enumerate() {
            if buf.is_none() {
                if let Some(b) = self.itrs[i].next() {
                    // self.buf[i] = Some(Box::new(buf));
                    *buf = Some(b);
                } else {
                    continue;
                }
            }

            // Find next lowest value.
            if value.is_empty()
                || buf.as_ref().unwrap().as_slice().cmp(&value) == std::cmp::Ordering::Less
            {
                value = buf.as_ref().unwrap().as_slice().to_vec();
            }
        }

        // Return nil if no elements remaining.
        if value.is_empty() {
            return None;
        }

        // Merge all elements together and clear buffers.
        for buf in self.buf.iter_mut() {
            if buf.is_none() || !buf.as_ref().unwrap().as_slice().eq(&value) {
                continue;
            }
            *buf = None;
        }
        Some(value)
    }
}

pub struct FilterUndeletedSeriesIDIter {
    sfile: Arc<SeriesFile>,
    // itr: FileSetSeriesIDIter,
    itr: Box<dyn SeriesIDIterator>,
}

impl SeriesIDIterator for FilterUndeletedSeriesIDIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.itr.close()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for FilterUndeletedSeriesIDIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        for e in self.itr.by_ref() {
            if self.sfile.is_deleted(e.series_id) {
                continue;
            }
            return Some(e);
        }

        None
    }
}

/// IndexSet represents a list of indexes, all belonging to one database.
pub struct IndexSet {
    index: Arc<ActiveIndex>,                      // The engine's index.
    series_file: Arc<SeriesFile>, // // The Series File associated with the db for this set.
    field_sets: Option<Arc<MeasurementFieldSet>>, // field sets for _all_ indexes in this set's DB.
}

/// Request-local, observer-only breakdown of one grouped TSI selection.
#[cfg(feature = "slow-trace")]
#[derive(Debug, Default)]
pub(crate) struct TsiSelectionDiagnostics {
    predicate_selection_us: std::sync::atomic::AtomicU64,
    series_key_materialization_us: std::sync::atomic::AtomicU64,
    tag_value_cache_lock_acquisitions: std::sync::atomic::AtomicU64,
    tag_value_cache_lock_wait_us: std::sync::atomic::AtomicU64,
    tag_value_cache_lock_hold_us: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "slow-trace")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TsiSelectionDiagnosticsSnapshot {
    pub(crate) predicate_selection_us: u64,
    pub(crate) series_key_materialization_us: u64,
    pub(crate) tag_value_cache_lock_acquisitions: u64,
    pub(crate) tag_value_cache_lock_wait_us: u64,
    pub(crate) tag_value_cache_lock_hold_us: u64,
}

/// Request-local, observer-only breakdown of measurement tag-key discovery.
#[cfg(feature = "slow-trace")]
#[derive(Debug, Default)]
pub(crate) struct MeasurementTagKeysDiagnostics {
    wall_us: std::sync::atomic::AtomicU64,
    partition_count: std::sync::atomic::AtomicU64,
    tasks_submitted: std::sync::atomic::AtomicU64,
    tasks_started: std::sync::atomic::AtomicU64,
    tasks_completed: std::sync::atomic::AtomicU64,
    queue_wait_us_total: std::sync::atomic::AtomicU64,
    queue_wait_us_max: std::sync::atomic::AtomicU64,
    execution_us_total: std::sync::atomic::AtomicU64,
    execution_us_max: std::sync::atomic::AtomicU64,
    caller_wait_all_us: std::sync::atomic::AtomicU64,
    grouped_execution: Option<Arc<crate::query::storage_ir::GroupedExecutionDiagnostics>>,
}

#[cfg(feature = "slow-trace")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MeasurementTagKeysDiagnosticsSnapshot {
    pub(crate) wall_us: u64,
    pub(crate) partition_count: u64,
    pub(crate) tasks_submitted: u64,
    pub(crate) tasks_started: u64,
    pub(crate) tasks_completed: u64,
    pub(crate) queue_wait_us_total: u64,
    pub(crate) queue_wait_us_max: u64,
    pub(crate) execution_us_total: u64,
    pub(crate) execution_us_max: u64,
    pub(crate) caller_wait_all_us: u64,
}

#[cfg(feature = "slow-trace")]
impl MeasurementTagKeysDiagnostics {
    pub(crate) fn new(
        grouped_execution: Option<Arc<crate::query::storage_ir::GroupedExecutionDiagnostics>>,
    ) -> Self {
        Self {
            grouped_execution,
            ..Self::default()
        }
    }

    pub(crate) fn elapsed_us(started: std::time::Instant) -> u64 {
        started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
    }

    fn record_max(value: &std::sync::atomic::AtomicU64, sample: u64) {
        let _ = value.fetch_max(sample, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn record_wall(&self, started: std::time::Instant) {
        self.wall_us.fetch_add(
            Self::elapsed_us(started),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) fn record_partition_count(&self, count: usize) {
        self.partition_count
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn record_task_submitted(&self) {
        self.tasks_submitted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn record_task_started(&self, submitted_at: std::time::Instant) {
        self.tasks_started
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let elapsed_us = Self::elapsed_us(submitted_at);
        self.queue_wait_us_total
            .fetch_add(elapsed_us, std::sync::atomic::Ordering::Relaxed);
        Self::record_max(&self.queue_wait_us_max, elapsed_us);
    }

    pub(crate) fn record_task_completed(&self, started: std::time::Instant) {
        self.tasks_completed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let elapsed_us = Self::elapsed_us(started);
        self.execution_us_total
            .fetch_add(elapsed_us, std::sync::atomic::Ordering::Relaxed);
        Self::record_max(&self.execution_us_max, elapsed_us);
    }

    pub(crate) fn record_caller_wait_all(&self, started: std::time::Instant) {
        self.caller_wait_all_us.fetch_add(
            Self::elapsed_us(started),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) fn record_general_blocking_task(
        &self,
        partition: usize,
        diagnostics: Arc<crate::runtime::execution::GeneralBlockingTaskDiagnostics>,
    ) {
        if let Some(grouped_execution) = &self.grouped_execution {
            grouped_execution.record_general_blocking_task(partition, diagnostics);
        }
    }

    pub(crate) fn snapshot(&self) -> MeasurementTagKeysDiagnosticsSnapshot {
        use std::sync::atomic::Ordering;

        MeasurementTagKeysDiagnosticsSnapshot {
            wall_us: self.wall_us.load(Ordering::Relaxed),
            partition_count: self.partition_count.load(Ordering::Relaxed),
            tasks_submitted: self.tasks_submitted.load(Ordering::Relaxed),
            tasks_started: self.tasks_started.load(Ordering::Relaxed),
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            queue_wait_us_total: self.queue_wait_us_total.load(Ordering::Relaxed),
            queue_wait_us_max: self.queue_wait_us_max.load(Ordering::Relaxed),
            execution_us_total: self.execution_us_total.load(Ordering::Relaxed),
            execution_us_max: self.execution_us_max.load(Ordering::Relaxed),
            caller_wait_all_us: self.caller_wait_all_us.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "slow-trace")]
impl TsiSelectionDiagnostics {
    fn elapsed_us(started: std::time::Instant) -> u64 {
        started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
    }

    pub(crate) fn record_predicate_selection(&self, started: std::time::Instant) {
        self.predicate_selection_us.fetch_add(
            Self::elapsed_us(started),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) fn record_series_key_materialization(&self, started: std::time::Instant) {
        self.series_key_materialization_us.fetch_add(
            Self::elapsed_us(started),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) fn record_tag_value_cache_lock(&self, wait_us: u64, hold_us: u64) {
        use std::sync::atomic::Ordering;

        self.tag_value_cache_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
        self.tag_value_cache_lock_wait_us
            .fetch_add(wait_us, Ordering::Relaxed);
        self.tag_value_cache_lock_hold_us
            .fetch_add(hold_us, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> TsiSelectionDiagnosticsSnapshot {
        use std::sync::atomic::Ordering;

        TsiSelectionDiagnosticsSnapshot {
            predicate_selection_us: self.predicate_selection_us.load(Ordering::Relaxed),
            series_key_materialization_us: self
                .series_key_materialization_us
                .load(Ordering::Relaxed),
            tag_value_cache_lock_acquisitions: self
                .tag_value_cache_lock_acquisitions
                .load(Ordering::Relaxed),
            tag_value_cache_lock_wait_us: self.tag_value_cache_lock_wait_us.load(Ordering::Relaxed),
            tag_value_cache_lock_hold_us: self.tag_value_cache_lock_hold_us.load(Ordering::Relaxed),
        }
    }
}

impl IndexSet {
    pub fn new(
        index: Arc<ActiveIndex>,
        series_file: Arc<SeriesFile>,
        field_sets: Option<Arc<MeasurementFieldSet>>,
    ) -> Self {
        let field_sets = if field_sets.is_none() {
            index.implementation().field_set()
        } else {
            field_sets
        };

        Self {
            index,
            series_file,
            field_sets,
        }
    }

    /// Returns the database name of the first index.
    pub fn database(&self) -> &str {
        // if self.index.is_empty() {
        //     return "";
        // }
        self.index.implementation().database()
    }

    /// Determines if any of the field sets on the set of indexes in the
    /// IndexSet have the provided field for the provided measurement.
    pub fn has_field(&self, measurement: &[u8], field: &str) -> bool {
        // if self.index.is_empty() {
        //     return false;
        // }

        // if self.field_sets.is_none() {
        //     self.field_sets = self.index.field_set();
        // }

        // for fs in self.field_sets.iter() {
        if let Some(fs) = &self.field_sets {
            if let Some(fields) = fs.fields(measurement) {
                if fields.has_field(field) {
                    return true;
                }
            }
        }
        // }

        false
    }

    /// Returns true if the tag key exists in any index for the provided measurement.
    pub fn has_tag_key(&self, name: &[u8], key: &[u8]) -> Result<bool> {
        self.index
            .implementation()
            .has_tag_key(name, key)
            .context(IndexSnafu)
    }

    /// Returns true if the tag value exists in any index for the provided measurement and tag key.
    pub fn has_tag_value(&self, name: &[u8], key: &[u8], value: &[u8]) -> Result<bool> {
        self.index
            .implementation()
            .hash_tag_value(name, key, value)
            .context(IndexSnafu)
    }

    // TODO:
    // MeasurementNamesByExpr
    // measurementNamesByExpr
    // measurementNamesByNameFilter
    // MeasurementNamesByPredicate
    // measurementNamesByPredicate
    // measurementNamesByTagFilter
    // measurementNamesByTagPredicate
    // measurementAuthorizedSeries
    // measurementHasTagValue
    // measurementHasEmptyTagValue
    // measurementHasTagValueRegex

    /// Returns a slice of measurement names.
    pub fn measurement_names(&self) -> Result<Vec<Vec<u8>>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        // Iterate over all measurements if no condition exists.
        let mut names = vec![];
        if let Some(itr) = self.measurement_iter()? {
            for e in itr {
                names.push(e);
            }
        }

        Ok(names)
    }

    /// Returns an iterator over all non-tombstoned series for the provided measurement.
    pub fn measurement_series_id_iter(
        &self,
        name: &[u8],
        // ) -> Result<Option<FilterUndeletedSeriesIDIter>> {
    ) -> Result<Option<FileSetSeriesIDIter>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        // if let Some(itr) = self
        //     .index
        //     .measurement_series_id_iter(name)
        //     .context(IndexSnafu)?
        // {
        //     return Ok(Some(FilterUndeletedSeriesIDIter {
        //         sfile: Arc::clone(&self.series_file),
        //         itr,
        //     }));
        // }

        // Ok(None)
        if let Some(itr) = self
            .index
            .implementation()
            .measurement_series_id_iter(name)
            .context(IndexSnafu)?
        {
            return Ok(Some(itr));
        }

        Ok(None)
    }

    pub fn measurement_series_id_iter2(&self, name: &[u8]) -> Result<Option<FileSetSeriesIDIter>> {
        self.index
            .implementation()
            .measurement_series_id_iter(name)
            .context(IndexSnafu)
    }

    /// Returns an iterator over all measurements in the index.
    pub fn measurement_iter(&self) -> Result<Option<MeasurementMergeIter>> {
        self.index
            .implementation()
            .measurement_iter()
            .context(IndexSnafu)
    }

    /// Returns a key iterator for a measurement.
    pub fn tag_key_iter(&self, name: &[u8]) -> Result<Option<TagKeyMergeIter>> {
        self.index
            .implementation()
            .tag_key_iter(name)
            .context(IndexSnafu)
    }

    /// Returns a value iterator for a tag key.
    pub fn tag_value_iter(&self, name: &[u8], key: &[u8]) -> Result<Option<TagValueMergeIter>> {
        self.index
            .implementation()
            .tag_value_iter(name, key)
            .context(IndexSnafu)
    }

    /// Iterates over all tag keys in a measurement and applies the provided function.
    pub fn for_each_measurement_tag_key<F, E>(&self, name: &[u8], mut f: F) -> Result<(), E>
    where
        F: FnMut(Vec<u8>) -> Result<(), E>,
    {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        if let Ok(Some(mut itr)) = self.tag_key_iter(name) {
            for key in itr.by_ref() {
                if let Err(e) = f(key) {
                    let _ = itr.close();
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Returns the tag keys for the measurement.
    pub fn measurement_tag_keys(&self, name: &[u8]) -> Result<HashSet<String>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        self.index
            .implementation()
            .measurement_tag_keys(name)
            .context(IndexSnafu)
    }

    /// Returns a series iterator for all values across a single key.
    pub fn tag_key_series_id_iter(
        &self,
        name: &[u8],
        key: &[u8],
    ) -> Result<Option<FilterUndeletedSeriesIDIter>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        self.tag_key_series_id_iter2(name, key)
    }

    pub fn tag_key_series_id_iter2(
        &self,
        name: &[u8],
        key: &[u8],
    ) -> Result<Option<FilterUndeletedSeriesIDIter>> {
        if let Some(itr) = self
            .index
            .implementation()
            .tag_key_series_id_iter(name, key)
            .context(IndexSnafu)?
        {
            return Ok(Some(FilterUndeletedSeriesIDIter {
                sfile: Arc::clone(&self.series_file),
                itr: Box::new(itr),
            }));
        }

        Ok(None)
    }

    /// Returns a series iterator for a single tag value.
    pub fn tag_value_series_id_iter(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<FilterUndeletedSeriesIDIter>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        if let Some(itr) = self.tag_value_series_id_iter2(name, key, value)? {
            return Ok(Some(FilterUndeletedSeriesIDIter {
                sfile: Arc::clone(&self.series_file),
                itr: Box::new(itr),
            }));
        }

        Ok(None)
    }

    pub fn tag_value_series_id_iter2(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<crate::index::FileSetSeriesIDIter>> {
        self.index
            .implementation()
            .tag_value_series_id_iter(name, key, value)
            .context(IndexSnafu)
    }

    #[cfg(feature = "slow-trace")]
    pub(crate) fn tag_value_series_id_iter2_profiled(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
        diagnostics: &TsiSelectionDiagnostics,
    ) -> Result<Option<crate::index::FileSetSeriesIDIter>> {
        self.index
            .implementation()
            .tag_value_series_id_iter_profiled(name, key, value, diagnostics)
            .context(IndexSnafu)
    }

    /// Returns a series iterator for a measurement.
    /// Note, this call is equivalent to measurement_series_id_iter.
    pub fn measurement_series_iter(
        &self,
        name: &[u8],
    ) -> Result<Option<FilterUndeletedSeriesIDIter>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }
        self.measurement_series_iter2(name)
    }

    pub fn measurement_series_iter2(
        &self,
        name: &[u8],
    ) -> Result<Option<FilterUndeletedSeriesIDIter>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        if let Some(itr) = self.measurement_series_id_iter(name)? {
            return Ok(Some(FilterUndeletedSeriesIDIter {
                sfile: Arc::clone(&self.series_file),
                itr: Box::new(itr),
            }));
        }

        Ok(None)
    }

    // TODO:
    // MeasurementSeriesKeysByExpr

    /// Returns a list of series keys for a measurement.
    pub fn measurement_series_keys(&self, name: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        // Create iterator for all matching series.
        let itr = self.measurement_series_iter(name)?;
        if let Some(itr) = itr {
            let mut itr = guard(itr, |itr| {
                let _ = itr.close();
            });
            let mut keys = vec![];
            // Iterate over all series and generate keys.
            for e in itr.by_ref() {
                let series_key = self.series_file.series_key(e.series_id);
                if series_key.is_empty() {
                    continue;
                }

                let (names, tags) = parse_series_key(&series_key).context(SeriesFileSnafu)?;
                keys.push(crate::models::points::make_key(&names, &tags));
            }

            keys.sort();

            return Ok(keys);
        }

        Ok(vec![])
    }

    /// Returns series keys selected by a storage-native predicate.
    pub fn series_keys_by_storage_predicate(
        &self,
        name: &[u8],
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        let itr = if let Some(predicate) = predicate {
            let filtered = self.filter_series_by_storage_predicate(name, predicate)?;
            Some(FilterUndeletedSeriesIDIter {
                sfile: Arc::clone(&self.series_file),
                itr: Box::new(new_series_id_set_iter(filtered.candidate_series)),
            })
        } else {
            self.measurement_series_iter2(name)?
        };
        let Some(itr) = itr else {
            return Ok(Vec::new());
        };
        let mut itr = guard(itr, |itr| {
            let _ = itr.close();
        });
        let mut keys = Vec::new();
        for elem in itr.by_ref() {
            let series_key = self.series_file.series_key(elem.series_id);
            if series_key.is_empty() {
                continue;
            }
            let (series_name, tags) = parse_series_key(&series_key).context(SeriesFileSnafu)?;
            keys.push(crate::models::points::make_key(&series_name, &tags));
        }
        keys.sort();
        Ok(keys)
    }

    /// Returns measurement names with at least one series selected by a
    /// storage-native predicate.
    pub fn measurement_names_by_storage_predicate(
        &self,
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        let all = self.measurement_names()?;
        let Some(predicate) = predicate else {
            return Ok(all);
        };
        let mut names = Vec::new();
        for name in all {
            if !self
                .series_keys_by_storage_predicate(&name, Some(predicate))?
                .is_empty()
            {
                names.push(name);
            }
        }
        Ok(names)
    }

    // MatchTagValueSeriesIDIterator
    // matchTagValueSeriesIDIterator
    // matchTagValueEqualEmptySeriesIDIterator
    // matchTagValueEqualNotEmptySeriesIDIterator
    // matchTagValueNotEqualEmptySeriesIDIterator
    // matchTagValueNotEqualNotEmptySeriesIDIterator
    // TagValuesByKeyAndExpr
    // tagValuesByKeyAndExpr
    // MeasurementTagKeyValuesByExpr

    /// Returns a set of tag values fro a measurement keys.
    pub fn measurement_tag_key_values(
        &self,
        name: &[u8],
        keys: &mut [String],
        keys_sorted: bool,
    ) -> Result<Vec<Vec<String>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut res = vec![];
        // If the keys are not sorted, then sort them.
        if !keys_sorted {
            keys.sort();
        }

        for key in keys.iter() {
            let vitr = self.tag_value_iter(name, key.as_bytes())?;
            if let Some(mut vitr) = vitr {
                // defer! {
                //     vitr.close();
                // }

                let mut values = vec![];
                for val in vitr.by_ref() {
                    values.push(to_string!(val));
                }
                res.push(values);
                let _ = vitr.close();
                continue;
            }

            // Preserve one result per requested key. A key absent from this
            // shard must not hide later keys that may exist locally or in
            // another mapped shard.
            res.push(Vec::new());
        }

        Ok(res)
    }

    pub(crate) fn tag_sets_for_storage_scan(
        &self,
        sfile: Arc<SeriesFile>,
        scan: &crate::query::storage_ir::StorageScan,
        max_series_n: i64,
    ) -> Result<Vec<TagSet>> {
        self.tag_sets_for_storage_scan_inner(
            sfile,
            scan,
            max_series_n,
            #[cfg(feature = "slow-trace")]
            None,
        )
    }

    #[cfg(feature = "slow-trace")]
    pub(crate) fn tag_sets_for_storage_scan_profiled(
        &self,
        sfile: Arc<SeriesFile>,
        scan: &crate::query::storage_ir::StorageScan,
        max_series_n: i64,
        diagnostics: &TsiSelectionDiagnostics,
    ) -> Result<Vec<TagSet>> {
        self.tag_sets_for_storage_scan_inner(sfile, scan, max_series_n, Some(diagnostics))
    }

    fn tag_sets_for_storage_scan_inner(
        &self,
        sfile: Arc<SeriesFile>,
        scan: &crate::query::storage_ir::StorageScan,
        max_series_n: i64,
        #[cfg(feature = "slow-trace")] diagnostics: Option<&TsiSelectionDiagnostics>,
    ) -> Result<Vec<TagSet>> {
        self.series_file.retain();
        defer! {
            self.series_file.release();
        }

        let cancellation = scan.cancellation.as_ref();
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &scan.execution.profile {
            let total_lookup_started = std::time::Instant::now();
            let mut total_measurement_series = 0u64;
            if let Some(mut series) =
                self.measurement_series_iter2(scan.measurement.name.as_bytes())?
            {
                loop {
                    if total_measurement_series & 0x3fff == 0x3fff
                        && cancellation.is_some_and(ScanCancellation::is_cancelled)
                    {
                        let _ = series.close();
                        return Err(Error::StorageScanCancelled);
                    }
                    let Some(series_id) = series.next() else {
                        break;
                    };
                    if series_id.series_id == 0 {
                        break;
                    }
                    total_measurement_series = total_measurement_series.saturating_add(1);
                }
                let _ = series.close();
            }
            profile.record_total_measurement_series(
                total_measurement_series,
                total_lookup_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        let (itr, storage_residual_filter) = if let Some(predicate) = &scan.predicate {
            #[cfg(feature = "slow-trace")]
            let filtered_result = {
                let started = std::time::Instant::now();
                let result = self.filter_series_by_storage_predicate_profiled(
                    scan.measurement.name.as_bytes(),
                    predicate,
                    cancellation,
                    scan.execution.profile.as_deref(),
                    diagnostics,
                );
                if let Some(diagnostics) = diagnostics {
                    diagnostics.record_predicate_selection(started);
                }
                result
            };
            #[cfg(not(feature = "slow-trace"))]
            let filtered_result = self.filter_series_by_storage_predicate_with_cancellation(
                scan.measurement.name.as_bytes(),
                predicate,
                cancellation,
            );
            let filtered = match filtered_result {
                Err(Error::StoragePredicateCancelled) => {
                    return Err(Error::StorageScanCancelled);
                }
                result => result?,
            };
            let residual = filtered.residual_predicate.map(|predicate| {
                if scan.execution.residual_column_resolution
                    == crate::query::storage_ir::ResidualColumnResolution::LegacyNameAlias
                {
                    predicate.with_legacy_name_alias()
                } else {
                    predicate
                }
            });
            let itr = FilterUndeletedSeriesIDIter {
                sfile: Arc::clone(&self.series_file),
                itr: Box::new(new_series_id_set_iter(filtered.candidate_series)),
            };
            (Some(itr), residual)
        } else {
            (
                self.measurement_series_iter2(scan.measurement.name.as_bytes())?,
                None,
            )
        };

        #[cfg(feature = "slow-trace")]
        let materialization_started = std::time::Instant::now();
        let tag_sets = self.build_tag_sets(
            sfile,
            itr.map(|itr| Box::new(itr) as Box<dyn SeriesIDIterator>),
            storage_residual_filter,
            &scan.projection.series.identity_tag_keys,
            cancellation,
            max_series_n,
        )?;
        #[cfg(feature = "slow-trace")]
        if let Some(diagnostics) = diagnostics {
            diagnostics.record_series_key_materialization(materialization_started);
        }
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &scan.execution.profile {
            let physical_series = tag_sets
                .iter()
                .map(|tag_set| tag_set.series_keys.len() as u64)
                .sum();
            let per_tagset_min = tag_sets
                .iter()
                .map(|tag_set| tag_set.series_keys.len() as u64)
                .min()
                .unwrap_or(0);
            let per_tagset_max = tag_sets
                .iter()
                .map(|tag_set| tag_set.series_keys.len() as u64)
                .max()
                .unwrap_or(0);
            profile.record_selected_tag_set_layout(
                tag_sets.len() as u64,
                physical_series,
                per_tagset_min,
                per_tagset_max,
            );
        }
        Ok(tag_sets)
    }

    fn build_tag_sets(
        &self,
        sfile: Arc<SeriesFile>,
        itr: Option<Box<dyn SeriesIDIterator>>,
        storage_residual_filter: Option<StoragePredicate>,
        dimensions: &[String],
        cancellation: Option<&ScanCancellation>,
        max_series_n: i64,
    ) -> Result<Vec<TagSet>> {
        let Some(mut itr) = itr else {
            return Ok(vec![]);
        };

        let mut dims = dimensions.to_vec();
        dims.sort();

        let mut tag_sets = HashMap::<String, TagSet>::with_capacity(64);

        let mut series_n = 0;
        let requested_max_series_n = max_series_n;
        let effective_max_series_n = if max_series_n > 0 {
            max_series_n
        } else {
            (!0usize >> 1) as i64
        };

        // The tag sets require a string for each series key in the set, The series
        // file formatted keys need to be parsed into models format. Since they will
        // end up as strings we can re-use an intermediate buffer for this process.

        let mut tags_buf = crate::models::Tags::new(); // Buffer for tags. Tags are not needed outside of each loop iteration.
        for se in itr.by_ref() {
            if se.series_id == 0 {
                break;
            }

            // Skip if the series has been tombstoned.
            let key = sfile.series_key(se.series_id);
            if key.is_empty() {
                continue;
            }

            if series_n & 0x3fff == 0x3fff {
                // Check every 16384 series while planning either native or QL-compatible scans.
                if cancellation.is_some_and(ScanCancellation::is_cancelled) {
                    let _ = itr.close();
                    return Err(Error::StorageScanCancelled);
                }
            }

            if series_n > effective_max_series_n {
                let _ = itr.close();
                return Err(Error::StringErr {
                    reason: format!(
                        "max-select-series limit exceeded: ({}/{})",
                        series_n, requested_max_series_n
                    ),
                });
            }

            // NOTE - must not escape this loop iteration.
            let series_name;
            (series_name, tags_buf) = match parse_series_key(&key) {
                Ok((name, tags)) => (name, tags),
                Err(e) => {
                    let _ = itr.close();
                    return Err(Error::SeriesFile { source: e });
                }
            };

            let mut tags_as_key = vec![];
            if !dims.is_empty() {
                tags_as_key = make_tags_key(&dims, &tags_buf);
            }

            let tag_set = if tag_sets.contains_key(to_str!(&tags_as_key)) {
                tag_sets.get_mut(to_str!(&tags_as_key)).unwrap()
            } else {
                tag_sets.insert(
                    to_string!(&tags_as_key),
                    TagSet::new(HashMap::new(), vec![], tags_as_key.clone()),
                );
                tag_sets.get_mut(to_str!(&tags_as_key)).unwrap()
            };

            // Associate the series and filter with the Tagset.
            let mut key_buf = vec![];
            key_buf = append_make_key(Some(&key_buf), &series_name, &tags_buf);
            tag_set.add_storage_filter(to_string!(&key_buf), storage_residual_filter.clone());

            series_n += 1;
        }

        // Sort the series in each tag set.
        for t in tag_sets.values_mut() {
            t.sort();
        }

        // The tag_sets have been created, as a map of tag_sets. Just send
        // the values back as a slice, sorting for consistency.
        let mut sorted_tags_sets = vec![];
        for (_, v) in tag_sets {
            sorted_tags_sets.push(v);
        }
        sorted_tags_sets.sort_by(|a, b| a.key.cmp(&b.key));

        let _ = itr.close();

        Ok(sorted_tags_sets)
    }
}

/// TagSet is a fundamental concept within the query system. It represents a composite series,
/// composed of multiple individual series that share a set of tag attributes.

#[derive(Default, Clone)]
pub struct TagSet {
    pub tags: HashMap<String, String>,
    pub(crate) storage_filters: Vec<Option<StoragePredicate>>,
    pub series_keys: Vec<String>,
    pub key: Vec<u8>,
}

impl TagSet {
    pub fn new(tags: HashMap<String, String>, series_keys: Vec<String>, key: Vec<u8>) -> Self {
        Self {
            tags,
            storage_filters: vec![None; series_keys.len()],
            series_keys,
            key,
        }
    }

    pub fn len(&self) -> usize {
        self.series_keys.len()
    }

    pub fn add_filter(&mut self, key: String, filter: Option<StoragePredicate>) {
        self.series_keys.push(key);
        self.storage_filters.push(filter);
    }

    fn add_storage_filter(&mut self, key: String, storage_filter: Option<StoragePredicate>) {
        self.series_keys.push(key);
        self.storage_filters.push(storage_filter);
    }

    fn sort(&mut self) {
        let mut entries = self
            .series_keys
            .drain(..)
            .zip(self.storage_filters.drain(..))
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        for (key, storage_filter) in entries {
            self.series_keys.push(key);
            self.storage_filters.push(storage_filter);
        }
    }

    /// Reverses the order of series keys and filters in the TagSet.
    pub fn reverse(&mut self) {
        self.series_keys.reverse();
        self.storage_filters.reverse();
    }
}

pub fn limit_tag_sets(mut a: Vec<TagSet>, slimit: i64, soffset: i64) -> Vec<TagSet> {
    // Ignore if no limit or offset is specified.
    if slimit == 0 && soffset == 0 {
        return a;
    }

    // If offset is beyond the number of tag sets then return nil.
    if soffset > a.len() as i64 {
        return vec![];
    }

    // Clamp limit to the max number of tag sets.
    let slimit = if soffset + slimit > a.len() as i64 {
        a.len() as i64 - soffset
    } else {
        slimit
    };

    a.drain(soffset as usize..(soffset + slimit) as usize)
        .collect()
}

// new_index returns an instance of an index based on its format.
// If the path does not exist then the DefaultFormat is used.
pub fn new_index(
    _id: u64,
    database: String,
    path: &Path,
    _series_id_set: Arc<SeriesIDSet>,
    sfile: Arc<SeriesFile>,
    opt: &EngineOptions,
) -> Result<Tsi1Index> {
    match fs::metadata(path) {
        Ok(_) => {}
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(Error::IOError { source: e });
            }
        }
    }

    Ok(Tsi1Index::new(
        sfile,
        database,
        path,
        opt.config.max_index_log_file_size,
        opt.config.series_id_set_cache_size,
    ))
}

pub fn must_open_index(
    id: u64,
    database: String,
    path: &Path,
    series_id_set: Arc<SeriesIDSet>,
    sfile: Arc<SeriesFile>,
    opt: &EngineOptions,
) -> Tsi1Index {
    let mut idx = new_index(id, database, path, series_id_set, sfile, opt).unwrap();
    idx.open().unwrap();

    idx
}

#[cfg(any(test, feature = "benchmarking"))]
pub(crate) fn must_open_active_index(
    _id: u64,
    database: String,
    path: &Path,
    _series_id_set: Arc<SeriesIDSet>,
    sfile: Arc<SeriesFile>,
    opt: &EngineOptions,
) -> ActiveIndex {
    let index = ActiveIndex::new_with_executor(
        sfile,
        database,
        path,
        opt.config.max_index_log_file_size,
        opt.config.series_id_set_cache_size,
        Arc::clone(&opt.executor),
    );
    index.open().unwrap();
    index
}

/// Returns an iterator that only returns series which occur in both iterators.
/// If both series have associated expressions then they are combined together.
pub fn intersect_series_id_iters(
    itr0: Option<Box<dyn SeriesIDIterator>>,
    itr1: Option<Box<dyn SeriesIDIterator>>,
) -> Option<Box<dyn SeriesIDIterator>> {
    if itr0.is_none() || itr1.is_none() {
        if let Some(itr0) = itr0 {
            itr0.close();
        }
        if let Some(itr1) = itr1 {
            itr1.close();
        }
        return None;
    }

    // Create series id set, if available.
    let itrs = [itr0.unwrap(), itr1.unwrap()];
    if let Some(a) = new_series_id_set_iters2(&itrs) {
        let ss = a[0].series_id_set().and(&a[1].series_id_set());
        // `a` holds references to itr0/itr1 so itr0/itr1 should not be closed when `a` is still in use
        itrs[0].close();
        itrs[1].close();
        return Some(Box::new(new_series_id_set_iter(ss)));
    }

    Some(Box::new(SeriesIDIntersectIter {
        buf: [SeriesIDElem::default(), SeriesIDElem::default()],
        itrs,
    }))
}

// SeriesIDIntersectIter is an iterator that merges two iterators together.
struct SeriesIDIntersectIter {
    buf: [SeriesIDElem; 2],
    itrs: [Box<dyn SeriesIDIterator>; 2],
}

impl SeriesIDIterator for SeriesIDIntersectIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut err = Ok(());
        if let Err(e) = self.itrs[0].close() {
            if err.is_ok() {
                err = Err(e);
            }
        }

        if let Err(e) = self.itrs[1].close() {
            if err.is_ok() {
                err = Err(e);
            }
        }

        err
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for SeriesIDIntersectIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Fill buffers.
            if self.buf[0].series_id == 0 {
                if let Some(n) = self.itrs[0].next() {
                    self.buf[0] = n;
                }
            }
            if self.buf[1].series_id == 0 {
                if let Some(n) = self.itrs[1].next() {
                    self.buf[1] = n;
                }
            }

            // Exit if either buffer is still empty.
            if self.buf[0].series_id == 0 || self.buf[1].series_id == 0 {
                return None;
            }

            // Skip if both series are not equal.
            let (a, b) = (self.buf[0].series_id, self.buf[1].series_id);
            if a < b {
                self.buf[0].series_id = 0;
                continue;
            } else if a > b {
                self.buf[1].series_id = 0;
                continue;
            }

            // Merge series together if equal.
            let mut elem = self.buf[0].clone();

            (self.buf[0].series_id, self.buf[1].series_id) = (0, 0);
            return Some(elem);
        }
    }
}

/// Returns an iterator that only returns series which occur the first iterator
/// but not the second iterator.
pub fn difference_series_id_iters(
    itr0: Option<Box<dyn SeriesIDIterator>>,
    itr1: Option<Box<dyn SeriesIDIterator>>,
) -> Option<Box<dyn SeriesIDIterator>> {
    if itr0.is_none() && itr1.is_none() {
        return None;
    } else if itr1.is_none() {
        return itr0;
    } else if itr0.is_none() {
        itr1.unwrap().close();
        return None;
    }

    // Create series id set, if available.
    let itrs = [itr0.unwrap(), itr1.unwrap()];
    if let Some(a) = new_series_id_set_iters2(&itrs) {
        let ss = a[0].series_id_set().and_not(&a[1].series_id_set());
        // `a` holds references to itr0/itr1 so itr0/itr1 should not be closed when `a` is still in use
        itrs[0].close();
        itrs[1].close();
        return Some(Box::new(new_series_id_set_iter(ss)));
    }

    Some(Box::new(SeriesIDDifferenceIter {
        buf: [SeriesIDElem::default(), SeriesIDElem::default()],
        itrs,
    }))
}

// SeriesIDDifferenceIter is an iterator that merges two iterators together.
struct SeriesIDDifferenceIter {
    buf: [SeriesIDElem; 2],
    itrs: [Box<dyn SeriesIDIterator>; 2],
}

impl SeriesIDIterator for SeriesIDDifferenceIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut err = Ok(());
        if let Err(e) = self.itrs[0].close() {
            if err.is_ok() {
                err = Err(e);
            }
        }

        if let Err(e) = self.itrs[1].close() {
            if err.is_ok() {
                err = Err(e);
            }
        }

        err
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for SeriesIDDifferenceIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Fill buffers.
            if self.buf[0].series_id == 0 {
                if let Some(n) = self.itrs[0].next() {
                    self.buf[0] = n;
                }
                return None;
            }
            if self.buf[1].series_id == 0 {
                if let Some(n) = self.itrs[1].next() {
                    self.buf[1] = n;
                }
                return None;
            }

            // Exit if first buffer is still empty.
            if self.buf[0].series_id == 0 {
                return None;
            } else if self.buf[1].series_id == 0 {
                let elem = self.buf[0].clone();
                self.buf[0].series_id = 0;
                return Some(elem);
            }

            // Return first series if it's less.
            // If second series is less then skip it.
            // If both series are equal then skip both.
            let (a, b) = (self.buf[0].series_id, self.buf[1].series_id);
            if a < b {
                let elem = self.buf[0].clone();
                self.buf[0].series_id = 0;
                return Some(elem);
            } else if a > b {
                self.buf[1].series_id = 0;
                continue;
            } else {
                (self.buf[0].series_id, self.buf[1].series_id) = (0, 0);
                continue;
            }
        }
    }
}

/// Returns an iterator that returns series from both both iterators.
/// If both series have associated expressions then they are combined together.
pub fn union_series_id_iters(
    itr0: Option<Box<dyn SeriesIDIterator>>,
    itr1: Option<Box<dyn SeriesIDIterator>>,
) -> Option<Box<dyn SeriesIDIterator>> {
    // Return other iterator if either one is nil.
    if itr0.is_none() {
        return itr1;
    }

    if itr1.is_none() {
        return itr0;
    }

    // Create series id set, if available.
    // let (itr0, itr1) = (itr0.unwrap(), itr1.unwrap());
    let itrs = [itr0.unwrap(), itr1.unwrap()];
    if let Some(a) = new_series_id_set_iters2(&itrs) {
        let ss = series_set::SeriesIDSet::new(&[]);
        ss.merge(&[&*a[0].series_id_set(), &*a[1].series_id_set()]);
        // `a` holds references to itr0/itr1 so itr0/itr1 should not be closed when `a` is still in use
        itrs[0].close();
        itrs[1].close();
        return Some(Box::new(new_series_id_set_iter(ss)));
    }

    Some(Box::new(SeriesIDUnionIter {
        buf: [SeriesIDElem::default(), SeriesIDElem::default()],
        itrs,
    }))
}

struct SeriesIDUnionIter {
    buf: [SeriesIDElem; 2],
    itrs: [Box<dyn SeriesIDIterator>; 2],
}

impl SeriesIDIterator for SeriesIDUnionIter {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut err = Ok(());
        if let Err(e) = self.itrs[0].close() {
            if err.is_ok() {
                err = Err(e);
            }
        }

        if let Err(e) = self.itrs[1].close() {
            if err.is_ok() {
                err = Err(e);
            }
        }

        err
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Iterator for SeriesIDUnionIter {
    type Item = SeriesIDElem;

    fn next(&mut self) -> Option<Self::Item> {
        // Fill buffers.
        if self.buf[0].series_id == 0 {
            if let Some(n) = self.itrs[0].next() {
                self.buf[0] = n;
            }
        }
        if self.buf[1].series_id == 0 {
            if let Some(n) = self.itrs[1].next() {
                self.buf[1] = n;
            }
        }

        // Return non-zero or lesser series.
        let (a, b) = (self.buf[0].series_id, self.buf[1].series_id);
        if a == 0 && b == 0 {
            return None;
        } else if b == 0 || (a != 0 && a < b) {
            let elem = self.buf[0].clone();
            self.buf[0].series_id = 0;
            return Some(elem);
        } else if a == 0 || (b != 0 && a > b) {
            let elem = self.buf[1].clone();
            self.buf[1].series_id = 0;
            return Some(elem);
        }

        // Attach element.
        let mut elem = self.buf[0].clone();

        (self.buf[0].series_id, self.buf[1].series_id) = (0, 0);
        return Some(elem);
    }
}

pub trait MeasurementIterator: Iterator<Item = Vec<u8>> {
    fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>>;
}
