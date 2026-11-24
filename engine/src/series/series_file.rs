use std::{cmp::Ordering, fs, hash::Hasher, io, path::PathBuf, sync::Arc};

use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, TaskAdmissionGate, accepted_task_guard,
    legacy_global_executor,
};
use bytes::{Buf, BufMut};
use integer_encoding::VarInt;
use parking_lot::{Mutex, RwLock, lock_api::RawRwLock};
use scopeguard::defer;
use snafu::{ResultExt, Snafu, ensure};
use twox_hash::XxHash64;

use crate::{common::slow_trace, index, index::SeriesIDIter, series::series_partition::*};

// SERIES_ID_SIZE is the size in bytes of a series key ID.
pub(crate) const SERIES_ID_SIZE: usize = 8;

// SERIES_FILE_PARTITION_N is the number of partitions a series file is split into.
pub(crate) const SERIES_FILE_PARTITION_N: i8 = 8;

/// Error type for WAL.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOError { reason: io::Error },

    /// Series file closed.
    #[snafu(display("tsdb: series file closed"))]
    SeriesFileClosed,

    /// Invalid series partition id.
    #[snafu(display("tsdb: invalid series partition id"))]
    InvalidSeriesPartitionID,

    /// Read series failed.
    #[snafu(display("tsdb: parse series `{}` failed", data))]
    ParseSeriesFailed { data: String },

    /// Series partition error.
    #[snafu(display("tsdb: {}", source))]
    SeriesPartition {
        source: crate::series::series_partition::Error,
    },

    /// String error.
    #[snafu(display("tsdb: {}", reason))]
    StringErr { reason: String },

    #[snafu(display("tsdb: series file task scheduling failed: {}", source))]
    Execution { source: SpawnError },

    #[snafu(display("tsdb: accepted series file task did not complete"))]
    TaskTerminated,
}

/// Result type for SeriesFile.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

pub struct SeriesFile {
    path: Arc<PathBuf>,
    partitions: Option<Vec<Arc<RwLock<SeriesPartition>>>>,
    executor: ExecutorRef,

    max_snapshot_concurrency: i32,

    refs: Refs, // RwLock to track references to the SeriesFile that are in use.
}

struct Refs {
    inner: parking_lot::RawRwLock, // RwLock to track references to the SeriesFile that are in use.
}

impl Refs {
    pub fn wlock(&self) {
        self.inner.lock_exclusive()
    }

    pub fn rlock(&self) {
        self.inner.lock_shared()
    }

    pub fn w_unlock(&self) {
        unsafe { self.inner.unlock_exclusive() }
    }

    pub fn r_unlock(&self) {
        unsafe { self.inner.unlock_shared() }
    }
}

impl Default for Refs {
    fn default() -> Self {
        Self {
            inner: parking_lot::RawRwLock::INIT,
        }
    }
}

impl SeriesFile {
    /// Returns a new instance of SeriesFile.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::new_with_executor(path, legacy_global_executor())
    }

    /// Returns a SeriesFile using the host-supplied execution capability.
    pub fn new_with_executor(path: impl Into<PathBuf>, executor: ExecutorRef) -> Self {
        let mut max_snapshot_concurrency = num_cpus::get() as i32;
        if max_snapshot_concurrency < 1 {
            max_snapshot_concurrency = 1;
        }

        SeriesFile {
            path: Arc::new(path.into()),
            partitions: None,
            executor,
            max_snapshot_concurrency,
            refs: Refs {
                inner: parking_lot::RawRwLock::INIT,
            },
        }
    }

    /// Open memoru maps the data file at the file's file.
    pub fn open(&mut self) -> Result<()> {
        let started = std::time::Instant::now();
        tracing::debug!(
            target: "reefsdb::tsdb::series",
            path = %self.path.display(),
            "series file opening"
        );
        let result = self.open_inner();
        match &result {
            Ok(()) => tracing::info!(
                target: "reefsdb::tsdb::series",
                path = %self.path.display(),
                partition_count = SERIES_FILE_PARTITION_N,
                duration_ms = started.elapsed().as_millis() as u64,
                "series file opened"
            ),
            Err(error) => tracing::error!(
                target: "reefsdb::tsdb::series",
                path = %self.path.display(),
                error = %error,
                "series file open failed"
            ),
        }
        result
    }

    fn open_inner(&mut self) -> Result<()> {
        // Wait for all references to be released and prevent new ones from being acquired.
        self.refs.wlock();

        // Create path if it doesn't exist.
        if let Err(e) = fs::create_dir_all(&*self.path) {
            self.refs.w_unlock();
            return Err(e.into());
        }

        // Open partitions.
        let mut partitions = Vec::with_capacity(SERIES_FILE_PARTITION_N as usize);
        let mut i = 0i32;
        while i < SERIES_FILE_PARTITION_N as i32 {
            let mut p = new_series_partition(
                i,
                self.series_partition_path(i),
                self.max_snapshot_concurrency,
                Arc::clone(&self.executor),
            );
            if let Err(e) = p.open().context(SeriesPartitionSnafu) {
                let _ = Self::close_partitions(&partitions);
                self.refs.w_unlock();
                return Err(e);
            }

            partitions.push(Arc::new(RwLock::new(p)));
            i += 1;
        }
        self.partitions = Some(partitions);

        self.refs.w_unlock();
        Ok(())
    }

    pub fn with_max_compaction_concurrency(&mut self, max_compaction_concurrency: i32) {
        let mut max_compaction_concurrency = max_compaction_concurrency;
        if max_compaction_concurrency < 1 {
            max_compaction_concurrency = num_cpus::get() as i32;
            if max_compaction_concurrency < 1 {
                max_compaction_concurrency = 1;
            }
        }
        self.max_snapshot_concurrency = max_compaction_concurrency;
    }

    fn close_partitions(partitions: &[Arc<RwLock<SeriesPartition>>]) -> Result<()> {
        for p in partitions.iter() {
            p.write().close().context(SeriesPartitionSnafu)?;
        }

        Ok(())
    }

    /// Close unmaps the data file.
    pub fn close(&self) -> Result<()> {
        tracing::debug!(
            target: "reefsdb::tsdb::series",
            path = %self.path.display(),
            "series file closing"
        );
        let result = self.close_inner();
        if result.is_ok() {
            tracing::info!(
                target: "reefsdb::tsdb::series",
                path = %self.path.display(),
                "series file closed"
            );
        }
        result
    }

    fn close_inner(&self) -> Result<()> {
        self.refs.wlock();
        defer! {
            self.refs.w_unlock();
        }

        Self::close_partitions(self.partitions.as_ref().unwrap())
    }

    /// Returns the path to the file.
    pub fn path(&self) -> PathBuf {
        self.path.to_path_buf()
    }

    /// Makes previously created IDs durable before numeric-ID-only files commit.
    /// Series creation flushes its buffered log before publishing IDs to callers.
    pub(crate) fn sync_ids(&self) -> std::io::Result<()> {
        if let Some(partitions) = &self.partitions {
            for partition in partitions {
                let (path, segments) = {
                    let partition = partition.read();
                    (partition.path(), partition.segment_paths())
                };
                for segment in segments {
                    std::fs::File::open(segment)?.sync_all()?;
                }
                std::fs::File::open(path)?.sync_all()?;
            }
        }
        std::fs::File::open(self.path.as_path())?.sync_all()
    }

    /// Returns the path to a given partition.
    pub fn series_partition_path(&self, i: i32) -> PathBuf {
        self.path.join(format!("{:02}", i))
    }

    #[cfg(any(test, feature = "benchmarking"))]
    /// Returns all partitions.
    pub fn partitions(&self) -> Option<Vec<Arc<RwLock<SeriesPartition>>>> {
        self.partitions.clone()
    }

    /// Adds a reference count to the file.
    pub fn retain(&self) {
        self.refs.rlock();
    }

    /// Removes a reference count to the file.
    pub fn release(&self) {
        self.refs.r_unlock();
    }

    /// Allows compactions to run.
    pub fn enable_compactions(&self) {
        for p in self.partitions.as_ref().unwrap() {
            p.read().enable_compactions();
        }
    }

    /// Prevents new compactions from running.
    pub fn disable_compactions(&self) {
        for p in self.partitions.as_ref().unwrap() {
            p.read().disable_compactions();
        }
    }

    /// Waits for all Retains to be released.
    pub fn wait(&self) {
        self.refs.wlock();
        defer! {
            self.refs.w_unlock();
        }
    }

    /// Returns the size of all partitions, in bytes.
    pub fn file_size(&self) -> Result<i64> {
        let mut n = 0i64;
        for p in self.partitions.as_ref().unwrap() {
            let v = p.read().file_size().context(SeriesPartitionSnafu)?;
            n += v;
        }

        Ok(n)
    }

    /// Creates a list of series in bulk if they don't exist.
    /// The returned ids slice returns IDs for every name+tags, creating new series IDs as needed.
    pub fn create_series_list_if_not_exists(
        &self,
        names: &[&[u8]],
        tags_slice: &[&crate::models::points::Tags],
    ) -> Result<Vec<u64>> {
        let keys = generates_series_keys(names, tags_slice);
        let key_partition_ids = self.series_keys_partition_ids(keys.as_slice());
        let ids = Arc::new(Mutex::new(vec![0u64; keys.len()]));
        let admission = TaskAdmissionGate::pending();

        let (s, r) = crossbeam_channel::bounded(SERIES_FILE_PARTITION_N as usize);
        let mut submitted = 0usize;
        let mut submission_failed = false;
        for p in self
            .partitions
            .as_ref()
            .expect("series partitions should not be None")
            .iter()
        {
            let p = Arc::clone(p);
            let keys = keys.clone();
            let key_partition_ids = key_partition_ids.clone();
            let ids = Arc::clone(&ids);
            let s = s.clone();
            let cancelled = s.clone();
            let rejected = s.clone();
            let task_admission = admission.clone();
            let cancelled_admission = admission.clone();
            let (registration, completion) = accepted_task_guard(move || {
                cancelled_admission.cancel();
                let _ = cancelled.send(Err(Error::TaskTerminated));
            });
            submitted += 1;
            match self.executor.spawn_blocking(move || {
                if !task_admission.wait() {
                    completion.complete_with(|| {
                        let _ = s.send(Ok(()));
                    });
                    return;
                }

                let result = p
                    .read()
                    .create_series_list_if_not_exists(
                        keys.as_slice(),
                        key_partition_ids.as_slice(),
                        ids,
                    )
                    .context(SeriesPartitionSnafu);
                completion.complete_with(|| {
                    let _ = s.send(result);
                });
            }) {
                Ok(_) => registration.accepted(),
                Err(source) => {
                    let _ = rejected.send(Err(Error::Execution { source }));
                    registration.rejected();
                    admission.cancel();
                    submission_failed = true;
                    break;
                }
            }
        }

        if submission_failed {
            admission.cancel();
        } else {
            admission.run();
        }

        let mut errs = vec![];
        for _ in 0..submitted {
            errs.push(r.recv());
        }

        errs.into_iter().try_for_each(|err| {
            match err {
                Ok(res) => res, // create series ok or failed
                Err(e) => Err(Error::StringErr {
                    reason: format!("create series task paniced: {:?}", e),
                }), // create series task paniced
            }
        })?;

        Ok(Arc::try_unwrap(ids).unwrap().into_inner())
    }

    /// Flags a series as permanently deleted.
    /// If the series is reintroduced later then it must create a new id.
    pub fn delete_series_id(&self, id: u64) -> Result<()> {
        let p = self.series_id_partition(id);
        ensure!(p.is_some(), InvalidSeriesPartitionIDSnafu);

        p.unwrap()
            .read()
            .delete_series_id(id)
            .context(SeriesPartitionSnafu)
    }

    /// Returns true if the ID has been deleted before.
    pub fn is_deleted(&self, id: u64) -> bool {
        let p = self.series_id_partition(id);
        if p.is_none() {
            return false;
        }

        p.unwrap().read().is_deleted(id)
    }

    /// Returns the series key for a given id.
    pub fn series_key(&self, id: u64) -> Vec<u8> {
        if id == 0 {
            return vec![];
        }
        if let Some(p) = self.series_id_partition(id) {
            let lock_start = slow_trace::now();
            let partition = p.read();
            slow_trace::log_lock_wait(
                "series_partition.read",
                lock_start,
                "stage=query_series_lookup",
            );
            return partition.series_key(id);
        }

        vec![]
    }

    /// Returns a list of series keys from a list of ids.
    pub fn series_keys(&self, ids: &[u64]) -> Vec<Vec<u8>> {
        let mut keys = vec![vec![]; ids.len()];
        for (i, id) in ids.iter().enumerate() {
            keys[i] = self.series_key(*id);
        }
        keys
    }

    /// Returns the parsed series name and tags for an offset.
    pub fn series(&self, id: u64) -> Result<(Vec<u8>, Option<crate::models::points::Tags>)> {
        let key = self.series_key(id);
        if key.is_empty() {
            return Ok((vec![], None));
        }
        let (name, tags) = parse_series_key(key.as_slice())?;
        Ok((name, Some(tags)))
    }

    /// Return the series id for the series.
    pub fn series_id(&self, name: &[u8], tags: &crate::models::points::Tags) -> u64 {
        let key = encode_series_key(name, tags);
        let key_partition = self.series_key_partition(key.as_slice());
        if key_partition.is_none() {
            return 0;
        }
        key_partition
            .unwrap()
            .read()
            .find_id_by_series_key(key.as_slice())
    }

    /// Return true if the series exists.
    pub fn has_series(&self, name: &[u8], tags: &crate::models::points::Tags) -> bool {
        self.series_id(name, tags) > 0
    }

    /// Returns the number of series.
    pub fn series_count(&self) -> u64 {
        let mut n = 0u64;
        for p in self.partitions.as_ref().unwrap() {
            n += p.read().series_count();
        }
        n
    }

    /// Returns an iterator over all the series.
    pub fn series_id_iterator(&self) -> SeriesIDIter {
        let mut ids: Vec<u64> = vec![];
        for p in self.partitions.as_ref().unwrap() {
            ids = p.read().append_series_ids(ids.as_slice());
        }
        ids.sort();
        index::new_series_id_slice_iterator(ids)
    }

    pub fn series_id_partition_id(&self, id: u64) -> i32 {
        ((id - 1) % SERIES_FILE_PARTITION_N as u64) as i32
    }

    pub fn series_id_partition(&self, id: u64) -> Option<Arc<RwLock<SeriesPartition>>> {
        let partition_id = self.series_id_partition_id(id);

        if self.partitions.is_none()
            || partition_id >= self.partitions.as_ref().expect("partitions is None").len() as i32
        {
            return None;
        }
        let p = Arc::clone(&self.partitions.as_ref().unwrap()[partition_id as usize]);
        Some(p)
    }

    pub fn series_keys_partition_ids<T: AsRef<[u8]>>(&self, keys: &[T]) -> Vec<i32> {
        let mut partition_ids = Vec::<i32>::with_capacity(keys.len());
        for (i, _) in keys.iter().enumerate() {
            partition_ids.push(self.series_key_partition_id(keys[i].as_ref()));
        }
        partition_ids
    }

    pub fn series_key_partition_id(&self, key: &[u8]) -> i32 {
        let mut hasher = XxHash64::default();
        hasher.write(key);
        (hasher.finish() % (SERIES_FILE_PARTITION_N as u64)) as i32
    }

    pub fn series_key_partition(&self, key: &[u8]) -> Option<Arc<RwLock<SeriesPartition>>> {
        let partition_id = self.series_key_partition_id(key);
        if self.partitions.is_none()
            || partition_id >= self.partitions.as_ref().expect("partitions is None").len() as i32
        {
            return None;
        }
        let p = Arc::clone(
            &self.partitions.as_ref().expect("partitions is None")[partition_id as usize],
        );
        Some(p)
    }
}

impl Default for SeriesFile {
    fn default() -> Self {
        Self::new(PathBuf::new())
    }
}

// Serializes name and tags to a byte slice. The total length is prepended as a uvarint.
pub fn encode_series_key(
    /*dst: &mut [u8], */ name: &[u8],
    tags: &crate::models::points::Tags,
) -> Vec<u8> {
    let mut buf = vec![0u8; i64::MAX.required_space()];

    // The tag count is variable encoded, so we need to know ahead of time what
    // the size of the tag count value will be.
    let mut tc_buf = vec![0u8; i64::MAX.required_space()];
    let tc_sz = (tags.len() as u64).encode_var(&mut tc_buf);

    // Size of name/tags. Does not include total length.
    let size = 2  // size of measurement
        + name.len()  // measurement
        + tc_sz  // size of number of tags
        + (4 * tags.len())  // length of each tag key and value
        + tags.size() as usize; // size of tag keys/values

    // Variable encode length.
    let total_sz = (size as u64).encode_var(&mut buf);

    // callcate dst buf
    let mut dst: Vec<u8> = vec![0; size + total_sz];

    let mut n = 0;
    // Append totoal length.
    dst[n..total_sz].copy_from_slice(&buf[..total_sz]);
    n += total_sz;

    // Append name.
    (&mut buf[..2]).put_u16(name.len() as u16);
    dst[n..total_sz + 2].copy_from_slice(&buf[..2]);
    n += 2;
    dst[n..n + name.len()].copy_from_slice(name);
    n += name.len();

    // Append tag count.
    dst[n..n + tc_sz].copy_from_slice(&tc_buf[..tc_sz]);
    n += tc_sz;

    // Append tags.
    for tag in tags.iter() {
        (&mut buf[..2]).put_u16(tag.key.len() as u16);
        dst[n..n + 2].copy_from_slice(&buf[..2]);
        n += 2;
        dst[n..n + tag.key.len()].copy_from_slice(tag.key.as_slice());
        n += tag.key.len();

        (&mut buf[..2]).put_u16(tag.value.len() as u16);
        dst[n..n + 2].copy_from_slice(&buf[..2]);
        n += 2;
        dst[n..n + tag.value.len()].copy_from_slice(tag.value.as_slice());
        n += tag.value.len();
    }

    // Verify total length equals the encoded bytes count.
    assert_eq!(dst.len(), size + total_sz);
    dst
}

/// Returns the series key from the beginning of the buffer.
pub(crate) fn read_series_key(data: &[u8]) -> Result<(&[u8], &[u8])> {
    if let Some((sz, n)) = u64::decode_var(data) {
        let idx = sz as usize + n;
        return Ok((&data[..idx], &data[idx..])); // (key, reminder)
    }

    ParseSeriesFailedSnafu {
        data: format!("{:?}", data),
    }
    .fail()
}

pub(crate) fn read_series_key_len(data: &[u8]) -> Result<(i32, &[u8])> {
    if let Some((sz, i)) = u64::decode_var(data) {
        return Ok((sz as i32, &data[i..])); // (sz, reminder)
    }

    Err(Error::ParseSeriesFailed {
        data: format!("{:?}", data),
    })
}

pub(crate) fn read_series_key_measurement(data: &[u8]) -> (&[u8], &[u8]) {
    let n = (&data[0..2]).get_u16() as usize;
    let data = &data[2..];
    (&data[..n], &data[n..])
}

pub(crate) fn read_series_key_tag_n(data: &[u8]) -> Result<(i32, &[u8])> {
    if let Some((n64, i)) = u64::decode_var(data) {
        return Ok((n64 as i32, &data[i..])); // (sz, reminder)
    }

    ParseSeriesFailedSnafu {
        data: format!("{:?}", data),
    }
    .fail()
}

pub(crate) fn read_series_key_tag(data: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let (n, data) = ((&data[0..2]).get_u16() as usize, &data[2..]);
    let (key, data) = (&data[..n], &data[n..]);

    let (n, data) = ((&data[0..2]).get_u16() as usize, &data[2..]);
    let (value, data) = (&data[..n], &data[n..]);

    (key, value, data)
}

// // Extracts the name & tags from a series key.
// pub(crate) fn parse_series_key(data: &[u8]) -> (Option<String>, Option<crate::models::points::Tags>) {
//     if let Ok((name, tags)) = parse_series_key2(data) {
//         return (Some(name), Some(tags));
//     }
//     (None, None)
// }

// // Extracts the name and tags for data, parsing the tags into
// // dst_tags, which is then returned.
// //
// // The returned dst_tags may have a different length and capacity.
// pub(crate) fn parse_series_key_into(
//     data: &[u8],
//     dst_tags: &mut crate::models::points::Tags,
// ) -> Result<(String, crate::models::points::Tags)> {
//     parse_series_key2(data, dst_tags)
// }

// Extracts the name & tags from a series key.
pub(crate) fn parse_series_key(data: &[u8]) -> Result<(Vec<u8>, crate::models::points::Tags)> {
    let (_sz, data) = read_series_key_len(data)?;
    let (name, data) = read_series_key_measurement(data);
    let (tag_n, data) = read_series_key_tag_n(data)?;
    let tag_n = tag_n as usize;

    let mut dst = crate::models::points::Tags::with_capacity(tag_n);
    let remaining = dst.spare_capacity_mut();
    let mut data = data;
    let mut i = 0;
    while i < tag_n {
        let key: &[u8];
        let value: &[u8];
        (key, value, data) = read_series_key_tag(data);
        remaining[i].write(crate::models::points::Tag {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        i += 1;
    }

    unsafe {
        dst.set_len(tag_n);
    }

    Ok((name.to_vec(), dst))
}

pub(crate) fn compare_series_keys(a: &[u8], b: &[u8]) -> Result<Ordering> {
    // Handle `None` keys
    if a.is_empty() && b.is_empty() {
        return Ok(Ordering::Equal);
    } else if a.is_empty() {
        return Ok(Ordering::Less);
    } else if b.is_empty() {
        return Ok(Ordering::Greater);
    }

    // read total size
    let (_, a) = read_series_key_len(a)?;
    let (_, b) = read_series_key_len(b)?;

    // read names
    let (name0, a) = read_series_key_measurement(a);
    let (name1, b) = read_series_key_measurement(b);

    // comapre names, return if not equal
    match name0.cmp(name1) {
        Ordering::Less => return Ok(Ordering::Less),
        Ordering::Greater => return Ok(Ordering::Greater),
        _ => {}
    }

    // read tag counts
    let (tag_n0, a) = read_series_key_tag_n(a)?;
    let (tag_n1, b) = read_series_key_tag_n(b)?;

    // compare each tag in order
    let mut i = 0;
    loop {
        // check for EOF
        if i == tag_n0 && i == tag_n1 {
            return Ok(Ordering::Equal);
        } else if i == tag_n0 {
            return Ok(Ordering::Less);
        } else if i == tag_n1 {
            return Ok(Ordering::Greater);
        }

        // read keys
        let (key0, value0, _a) = read_series_key_tag(a);
        let (key1, value1, _b) = read_series_key_tag(b);

        // compare keys & values
        match key0.cmp(key1) {
            Ordering::Less => return Ok(Ordering::Less),
            Ordering::Greater => return Ok(Ordering::Greater),
            _ => {}
        }

        match value0.cmp(value1) {
            Ordering::Less => return Ok(Ordering::Less),
            Ordering::Greater => return Ok(Ordering::Greater),
            _ => {}
        }

        i += 1;
    }
}

// Generates series keys for a list of names & tags using a single large memory block.
pub(crate) fn generates_series_keys(
    names: &[&[u8]],
    tags_slice: &[&crate::models::points::Tags],
) -> Vec<Vec<u8>> {
    let mut keys = Vec::<Vec<u8>>::with_capacity(names.len());
    for (i, name) in names.iter().enumerate() {
        let key = encode_series_key(name, tags_slice[i]);
        keys.push(key);
    }
    keys
}

/// Returns the number of bytes required to encode a list of name/tags.
pub fn series_keys_size(names: &[&[u8]], tags_slice: &[crate::models::points::Tags]) -> usize {
    let mut n = 0usize;
    for (i, _name) in names.iter().enumerate() {
        n += series_key_size(names[i], &tags_slice[i]);
    }
    n
}

/// Returns the number of bytes required to encode a series key.
pub fn series_key_size(name: &[u8], tags: &crate::models::points::Tags) -> usize {
    let mut n = 0usize;
    n += 2 + name.len();
    n += (tags.len() as u64).required_space();

    for tag in tags.iter() {
        n += 2 + tag.key.len();
        n += 2 + tag.value.len();
    }
    n += (n as u64).required_space();
    n
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {

    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use crate::common::to_str;
    use parking_lot::RwLock;

    use crate::series::{series_file, series_partition::new_series_partition_compactor};

    #[test]
    fn test_series_file_parse_series_key() {
        let name = "cpu";
        let tags = crate::models::points::Tags::from(HashMap::from([
            ("region".as_bytes().to_vec(), "east".as_bytes().to_vec()),
            ("server".as_bytes().to_vec(), "a".as_bytes().to_vec()),
        ]));
        let key = series_file::encode_series_key(name.as_bytes(), &tags);

        //let dst = crate::models::points::Tags::with_capacity(0usize);
        let (got_name, got_tags) = series_file::parse_series_key(key.as_slice()).unwrap();
        assert_eq!(
            to_str!(&got_name),
            name,
            "got {:?}, expected {}",
            got_name,
            name
        );

        assert_eq!(
            got_tags.len(),
            2,
            "gto tags length {}, expected {}",
            got_tags.len(),
            2
        );
        assert_eq!(
            got_tags, tags,
            "got tags {:?}, expected {:?}",
            got_tags, tags
        );

        //let dst = crate::models::points::Tags::with_capacity(5usize);
        let (_, got_tags) = series_file::parse_series_key(key.as_slice()).unwrap();
        assert_eq!(
            got_tags.len(),
            2,
            "gto tags length {}, expected {}",
            got_tags.len(),
            2
        );
        assert_eq!(
            got_tags.capacity(),
            2,
            "got tags capacity {}, expected {}",
            got_tags.capacity(),
            2
        );
        assert_eq!(
            got_tags, tags,
            "got tags {:?}, expected {:?}",
            got_tags, tags
        );

        //let dst = crate::models::points::Tags::with_capacity(1);
        let (_, got_tags) = series_file::parse_series_key(key.as_slice()).unwrap();
        assert_eq!(
            got_tags.len(),
            2,
            "got tags length {}, expected {}",
            got_tags.len(),
            2
        );
        assert_eq!(
            got_tags, tags,
            "got tags {:?}, expected {:?}",
            got_tags, tags
        );
    }

    // Ensure that broken series files are closed
    #[test]
    fn test_series_file_open_when_file_corrupt_should_return_err() {
        let mut f = new_broken_series_file(&[0u8; 5]);
        let err = f.open();
        assert!(err.is_err(), "should report error");
    }

    // Ensure series file contains the correct set of series.
    #[test]
    fn test_series_file_series() {
        let sfile = must_open_series_file();

        let series = [
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
        ];
        for s in &series {
            let series_ids =
                sfile.create_series_list_if_not_exists(&[s.name.as_slice()], &[&s.tags]);

            let series_ids = series_ids.unwrap();
        }

        // Vertify total number of series is correct.
        assert_eq!(
            sfile.series_count(),
            3,
            "unexpected series count: {}",
            sfile.series_count()
        );

        // Verfify all series exist.
        for (i, s) in series.iter().enumerate() {
            let series_id = sfile.series_id(&s.name, &s.tags);
            assert_ne!(series_id, 0, "series doesn't exist: i={}", i);
        }

        // Vertify non-existent series doesn't exist.
        if sfile.has_series(
            b"foo",
            &crate::models::points::Tags::from(HashMap::from([(
                "region".as_bytes().to_vec(),
                "north".as_bytes().to_vec(),
            )])),
        ) {
            panic!("series should not exist");
        }
    }

    // Ensure series file can be compacted.
    #[test]
    fn test_series_file_compactor() {
        let mut sfile = must_open_series_file();

        // Disable automatic compactions.
        let partitions = sfile.partitions().unwrap();
        for p in partitions {
            p.write().set_compact_threshold(0);
        }

        let mut names = Vec::<String>::with_capacity(10000);
        let mut tags_slice = Vec::<crate::models::points::Tags>::with_capacity(10000);
        for i in 0..10000 {
            names.push(format!("m{}", i));
            tags_slice.push(crate::models::points::Tags::from(HashMap::from([(
                "foo".as_bytes().to_vec(),
                "bar".as_bytes().to_vec(),
            )])));
        }

        let tag_slice_refs: Vec<&crate::models::points::Tags> = tags_slice.iter().collect();
        let tag_slice_refs_refs = tag_slice_refs.as_slice();
        sfile
            .create_series_list_if_not_exists(
                names
                    .iter()
                    .map(|v| v.as_bytes())
                    .collect::<Vec<&[u8]>>()
                    .as_slice(),
                tag_slice_refs_refs,
            )
            .unwrap();

        // Vertify total number of series is correct.
        assert_eq!(
            sfile.series_count(),
            names.len() as u64,
            "unexpected series count: {}",
            sfile.series_count()
        );

        // Compact in-place for each partitin.
        let partitions = sfile.partitions().unwrap();
        for p in partitions {
            let compactor = new_series_partition_compactor();
            let series = Arc::clone(&p.read().series);
            let err = compactor.compact(series);
            assert!(err.is_ok(), "compact failed: {:?}", err);
        }

        // Vertify all series exist.
        for (i, _) in names.iter().enumerate() {
            let series_id = sfile.series_id(names[i].as_bytes(), &tags_slice[i]);
            assert_ne!(
                series_id, 0,
                "series doesn't exist: {:?},{:?}",
                names[i], &tags_slice[i]
            );
        }
    }

    // Ensure series file deletions persist across compactions.
    #[test]
    fn test_series_file_delete_series_id() {
        let mut sfile = must_open_series_file();

        let ids0 = sfile
            .create_series_list_if_not_exists(
                &["m1".as_bytes()],
                &[&crate::models::points::Tags::new()],
            )
            .unwrap();

        let _ = sfile
            .create_series_list_if_not_exists(
                &["m2".as_bytes()],
                &[&crate::models::points::Tags::new()],
            )
            .unwrap();

        sfile.force_compact().unwrap();

        // Delete and ensure deletion.
        sfile.delete_series_id(ids0[0]).unwrap();
        sfile
            .create_series_list_if_not_exists(
                &["m1".as_bytes()],
                &[&crate::models::points::Tags::new()],
            )
            .unwrap();
        assert!(
            sfile.is_deleted(ids0[0]),
            "expected deletion before compaction"
        );

        sfile.force_compact().unwrap();
        assert!(
            sfile.is_deleted(ids0[0]),
            "expected deletion after compaction"
        );

        sfile.reopen().unwrap();
        assert!(sfile.is_deleted(ids0[0]), "expected deletion after reopen");
    }

    #[test]
    fn test_series_file_compaction() {
        let mut sfile = must_open_series_file();

        // Generate a bounch of keys.
        let mut mms = Vec::<String>::with_capacity(1000);
        let mut tag_sets = Vec::<crate::models::points::Tags>::with_capacity(1000);
        for i in 0..1000 {
            mms.push("cpu".to_string());
            tag_sets.push(crate::models::points::Tags::from(HashMap::from([(
                "region".as_bytes().to_vec(),
                format!("r{}", i).into_bytes(),
            )])));
        }

        let tag_slice: Vec<&crate::models::points::Tags> = tag_sets.iter().collect();
        let tag_slice_refs = tag_slice.as_slice();
        // Add all to the series file.
        let ids = sfile
            .create_series_list_if_not_exists(
                mms.iter()
                    .map(|v| v.as_bytes())
                    .collect::<Vec<&[u8]>>()
                    .as_slice(),
                tag_slice_refs,
            )
            .unwrap();

        // Delete a subset of keys.
        for (i, id) in ids.iter().enumerate() {
            if i % 10 == 0 {
                assert!(sfile.delete_series_id(*id).is_ok());
            }
        }

        // Compute total size of all series data.
        let orig_size = sfile.file_size().unwrap();

        // Compact all segments.
        let mut paths: Vec<PathBuf> = Vec::new();
        let partitions = sfile.partitions().unwrap();
        for p in partitions {
            let segments = &p.read().segments();
            // let index = p.read().index();
            for ss in segments.iter() {
                let ss = ss.read();
                let mut tmp_path = ss.path();
                tmp_path.set_extension("tmp"); // "xxx.tmp"
                let _ = ss.compact_to_path(tmp_path, &*p.read().index());
                paths.push(ss.path());
            }
        }

        // Close index.
        sfile.close().unwrap();

        // Overwrite files.
        for path in paths.iter() {
            let mut tmp_path = path.clone();
            tmp_path.set_extension("tmp");
            fs::rename(&tmp_path, &path).unwrap();
        }

        // Reopen index.
        let mut sfile = series_file::SeriesFile::new(sfile.path());
        sfile.open().unwrap();

        // Ensure series status is correct.
        for (i, id) in ids.iter().enumerate() {
            let (got, want) = (sfile.is_deleted(*id), (i % 10) == 0);
            assert_eq!(got, want, "is_deleted({})={}, want {}", id, got, want);
        }

        // Verify new size is smaller.
        let new_size = sfile.file_size().unwrap();
        assert!(
            new_size < orig_size,
            "expected new size ({}) to be smaller than original size ({})",
            new_size,
            orig_size
        );
    }

    #[cfg(test)]
    #[ctor::ctor]
    fn init_runtime() {
        let runtime_opt = crate::runtime::global::RuntimeOptions::default();
        crate::runtime::global::init_global_runtimes(&runtime_opt);
    }

    // //////////// series file test util functions //////////////

    use std::{
        fs,
        ops::{Deref, DerefMut},
    };

    #[derive(Default)]
    pub struct Series {
        pub name: Vec<u8>,
        pub tags: crate::models::points::Tags,
        pub delete: bool,
    }

    pub fn new_series_file() -> SeriesFile {
        let mut dir_path = std::env::temp_dir();
        dir_path.push(format!("tsdb-series-file-{:x}", rand::random::<u64>())); // Add the prefix
        fs::create_dir_all(&dir_path)
            .expect("unable to create series file temporary working directory");

        SeriesFile(series_file::SeriesFile::new(dir_path))
    }

    pub fn new_broken_series_file(content: &[u8]) -> SeriesFile {
        let mut sfile = new_series_file();
        let fpath = sfile.path();
        let _ = sfile.open();
        let _ = sfile.close();

        let seg_path = fpath.join("00").join("0000");
        if !seg_path.exists() {
            panic!("{:?} does not exist", seg_path);
        }
        fs::write(seg_path, content).unwrap();
        sfile
    }

    // returns a new, open instance of SeriesFile. Panic on error.
    pub fn must_open_series_file() -> SeriesFile {
        let mut sfile = new_series_file();
        let _ = sfile.open().unwrap();

        sfile
    }

    // SeriesFile is a test wrapper for series_file::SeriesFile.
    pub struct SeriesFile(pub crate::series::series_file::SeriesFile);
    impl SeriesFile {
        // Close closes the log file and removes it from disk.
        pub fn close(&mut self) -> series_file::Result<()> {
            self.0.close()
        }

        // Reopen close & reopens the series file.
        pub fn reopen(&mut self) -> series_file::Result<()> {
            self.0.close()?;
            self.0 = series_file::SeriesFile::new(self.0.path());
            self.0.open()
        }

        // ForceCompact executes an immediate compaction across all partitions.
        pub fn force_compact(&mut self) -> series_file::Result<()> {
            let partitions = self.partitions().unwrap();
            for p in partitions {
                let series = Arc::clone(&p.read().series);
                series_file::new_series_partition_compactor()
                    .compact(series)
                    .unwrap();
            }

            Ok(())
        }
    }

    impl Deref for SeriesFile {
        type Target = series_file::SeriesFile;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for SeriesFile {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl From<SeriesFile> for series_file::SeriesFile {
        fn from(sfile: SeriesFile) -> Self {
            sfile.0
        }
    }
}
