#[cfg(feature = "slow-trace")]
use std::collections::BTreeSet;
use std::{cmp::Reverse, collections::BTreeMap, sync::Arc};
use std::sync::mpsc;

use crate::common::to_str;
use crate::models::points::{
    BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Value,
};
use crate::runtime::execution::{ExecutorRef, RuntimeExecutorExt, TaskJoinError, legacy_global_executor};
use bytes::Buf;
use parking_lot::RwLock;
use snafu::ensure;

use super::reader::{self, Result, TimeRange, VLogReader};
#[cfg(feature = "slow-trace")]
use crate::query::storage_ir::StorageScanProfile;
use crate::{
    common::slow_trace,
    storage::tsm1::{
        data_encoding::encoding_values::{
            BoolValues, FloatValues, IntegerValues, StringValues, UnsignedValues,
        },
        vlog::{VLogIndex, vlog_index_ring::VLogIndexes, writer::INDEX_ENTRY_SIZE},
    },
};

// FIXME: may be too small or too big
const DEFAULT_READ_INDEXS_PER_THREAD: usize = 100;

/// VLogKeyCursor allows iteration through keys in a set of files within a VLog FileStore.
pub struct VLogKeyCursor {
    indexed_stream: Option<super::index::raw_cursor::RawCursor>,
    key: Vec<u8>,

    // // seeks is all the file locations that we need to return during iteration.
    seeks: Vec<VLogLocation>, // sorted by min_time first, then by file id

    // pos is the index within seeks.
    pub pos: usize,

    // // current is the set of blocks possibly containing the next set of points.
    // // Normally this is just one entry, but there may be multiple if points have
    // // been overwritten.
    current: BTreeMap<Arc<VLogReader>, Vec<VLogIndex>>,

    ascending: bool,
    executor: ExecutorRef,
    #[cfg(feature = "slow-trace")]
    profile: Option<Arc<StorageScanProfile>>,
}

impl VLogKeyCursor {
    pub(crate) fn from_indexed_stream(stream: super::index::raw_cursor::RawCursor,
        ascending: bool, executor: ExecutorRef) -> Self {
        let mut cursor = Self::new_with_executor(&[], VLogIndexes(Vec::new()), &[], 0, ascending, executor);
        cursor.indexed_stream = Some(stream);
        cursor
    }
    /// Returns a new instance of KeyCursor.
    /// This function assumes the read-lock has been taken.
    pub fn new(
        files: &[Arc<VLogReader>],
        mut indexes: VLogIndexes, // sort by gen then by min time.
        key: &[u8],
        t: i64,
        ascending: bool,
    ) -> Self {
        Self::new_with_executor(files, indexes, key, t, ascending, legacy_global_executor())
    }

    pub fn new_with_executor(
        files: &[Arc<VLogReader>],
        mut indexes: VLogIndexes,
        key: &[u8],
        t: i64,
        ascending: bool,
        executor: ExecutorRef,
    ) -> Self {
        // FIXME:
        // FIXME:
        // FIXME:
        // FIXME:
        // FIXME:
        /*
        为了发挥SSD性能，这里应该这样操作：
        1、vlog_locations 根据vlog_indexes进行分组和排序：
            a、每一组Vec<Locations>，都能通过多个tharead一次从多个files中读取数据，并且读取完的数据可以
               “很容易”进行merge、sort
            b、分组里，按照cpu_cores作为数量？比如，换一个分组里存在cpu_cores个VLogReader，可以cpu_cores个thread
              从这些VLogReader里读取数据
        2、分组细节：
            a、遍历这些VLogIndex，找到有overlap time range的VLogIndex
            b、重新整理 所有的VLogIndex，以及overlap vlog index，创建新的 Vec<VLogIndex>，这里的都没有overlap了
            c、对new_vlog_indexes进行分组，比如，根据(gen, seq)，对前面x个vlogindex分组：
                partitined_vlog_index:
                    vec![
                        time_range_1:
                        vec![
                            vec![index, index, index,...],  // 同一个 (gen, seq)
                            vec![index, index, index,...],  // 同一个 (gen, seq)
                            ...
                        ]
                        time_range_2:
                        vec![
                            vec![index, index, index,...],  // 同一个 (gen, seq)
                            vec![index, index, index,...],  // 同一个 (gen, seq)
                            ...
                        ]
                        ...
                    ]
                ]

        3、对于partitined_vlog_index，每次对其中的一个 time_range_1 一次全部取出数据，使用y个thread并行从files中取数



         */

        let mut c = Self {
            indexed_stream: None,
            key: key.to_vec(),
            seeks: vec![],
            pos: 0,
            current: BTreeMap::new(),
            ascending,
            executor,
            #[cfg(feature = "slow-trace")]
            profile: None,
        };

        if ascending {
            indexes.sort_unstable_by(|a, b| {
                if a.overlaps_time_range(b.min, b.max) {
                    match a.generation.cmp(&b.generation) {
                        std::cmp::Ordering::Equal => a.seq.cmp(&b.seq),
                        other => other,
                    }
                } else {
                    a.min.cmp(&b.min)
                }
            });
        } else {
            indexes.sort_unstable_by(|a, b| {
                if a.overlaps_time_range(b.min, b.max) {
                    match a.generation.cmp(&b.generation) {
                        std::cmp::Ordering::Equal => a.seq.cmp(&b.seq),
                        other => other,
                    }
                } else {
                    a.max.cmp(&b.max)
                }
            });
        }

        c.seeks = vlog_locations(files, indexes, key);

        // Determine the distinct set of TSM files in use and mark then as in-use
        for f in c.seeks.iter() {
            f.r.ref_reader();
        }

        c.seek();

        c
    }

    #[cfg(feature = "slow-trace")]
    pub(crate) fn attach_profile(&mut self, profile: Arc<StorageScanProfile>) {
        let files = self
            .seeks
            .iter()
            .map(|location| (location.r.generation, location.r.seq))
            .collect::<BTreeSet<_>>()
            .len() as u64;
        profile.record_vlog_cursor(files, self.seeks.len() as u64);
        self.profile = Some(profile);
    }

    #[cfg(feature = "slow-trace")]
    fn record_read_profile(&self, values_decoded: usize, started: std::time::Instant) {
        let Some(profile) = &self.profile else {
            return;
        };
        let blocks_decoded = self
            .current
            .values()
            .map(|indexes| indexes.len() as u64)
            .sum();
        let block_bytes = self
            .current
            .values()
            .flat_map(|indexes| indexes.iter())
            .filter_map(|index| u64::try_from(index.size).ok())
            .sum();
        profile.record_vlog_read(
            blocks_decoded,
            values_decoded as u64,
            block_bytes,
            self.current.len() as u64,
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        );
    }

    #[cfg(feature = "slow-trace")]
    fn record_sort_profile(&self, started: std::time::Instant) {
        if let Some(profile) = &self.profile {
            profile
                .record_vlog_sort(started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
        }
    }

    fn read_indexed_values(&mut self) -> Result<Vec<Value>> {
        self.indexed_stream.as_mut()
            .ok_or_else(|| reader::Error::StringErr { reason: "indexed cursor is missing".into() })?
            .read_batch().map(<[Value]>::to_vec)
            .map_err(|error| reader::Error::StringErr { reason: error.to_string() })
    }

    /// Removes all references on the cursor.
    pub fn close(&mut self) {
        self.indexed_stream = None;
        // Remove all of our in-use references since we're done
        for f in self.seeks.iter() {
            f.r.unref_reader();
        }

        // self.buf = vec![];
        self.seeks = vec![];
        self.current = BTreeMap::new();
    }

    pub fn seek(&mut self) {
        if self.seeks.is_empty() {
            return;
        }

        self.current.clear();
        if self.pos == self.seeks.len() {
            return;
        }
        // if self.seeks.len() == 1 {
        //     let index = self.seeks.first().unwrap();
        //     self.current.insert(Arc::clone(&index.r), vec![index.vlog_index.clone()]);
        //     return;
        // }

        // 按照file id分组，每组里的index按照file_1,file_2,...放到一起，且一组里最多有cpu_cores*10个index
        // 即，一个file里最大有10个index。
        let mut n = num_cpus::get();
        if self.seeks.len() - self.pos < n {
            n = self.seeks.len() - self.pos;
        }
        let mut total = n * DEFAULT_READ_INDEXS_PER_THREAD;
        if self.seeks.len() - self.pos < total {
            total = self.seeks.len() - self.pos;
        }

        let mut i = 0;
        while i < total && self.current.keys().len() <= n {
            // let (gen, seq) = (self.seeks[i].r.gen, self.seeks[i].r.seq);
            let r = Arc::clone(&self.seeks[self.pos + i].r);
            self.current
                .entry(r)
                .or_default()
                .push(self.seeks[self.pos + i].vlog_index.clone());

            i += 1;
        }

        // Extend the batch to include any trailing blocks that overlap the last
        // selected block. The reader and index must come from the same location.
        while i < total
            && i > 0
            && self.seeks[self.pos + i - 1].vlog_index.overlaps_time_range(
                self.seeks[self.pos + i].vlog_index.min,
                self.seeks[self.pos + i].vlog_index.max,
            )
        {
            let r = Arc::clone(&self.seeks[self.pos + i].r);
            self.current
                .entry(r)
                .or_default()
                .push(self.seeks[self.pos + i].vlog_index.clone());
            i += 1;
        }

        // self.seeks.drain(..i);
        self.pos += i;
    }

    /// Moves the cursor to the next position.
    /// Data should be read by the ReadBlock functions.
    pub fn next(&mut self) {
        if let Some(stream) = &mut self.indexed_stream {
            stream.next();
            return;
        }
        if self.current.is_empty() {
            return;
        }

        self.current.clear();
        self.seek();
    }

    /// Reads the next block as a set of integer values.
    pub fn read_integer_block(&mut self) -> Result<Vec<IntegerValue>> {
        if self.indexed_stream.is_some() {
            return self.read_indexed_values()?.into_iter().map(|value| match value {
                Value::IntegerValue(value) => Ok(value),
                _ => Err(reader::Error::StringErr { reason: "aggregate block type mismatch".into() }),
            }).collect();
        }
        let read_start = slow_trace::now();
        // No matching blocks to decode
        if self.current.is_empty() {
            return Ok(vec![]);
        }

        // A synchronous cursor may run inside an already-entered executor.
        // Keep reader dispatch parallel without entering another executor to wait.
        let mut notifications = vec![];
        for (r, indexes) in self.current.iter() {
            let r = Arc::clone(r);
            let indexes = indexes.clone();
            let key = self.key.clone();
            let (sender, receiver) = mpsc::sync_channel(1);
            let task = self.executor
                .spawn_blocking(move || {
                    let _ = sender.send(
                        r.read_integer_multi_block_and_exclude_tombstones(&r, &key, &indexes),
                    );
                })
                .map_err(|source| reader::Error::Execution { source })?;
            notifications.push((receiver, task));
        }

        let mut values = vec![];
        for (notify, _task) in notifications {
            let mut vals = notify.recv().map_err(|_| reader::Error::StringErr {
                reason: format!("read_integer_block from vlog failed: {:?}", TaskJoinError),
            })??;
            values.append(&mut vals);
        }
        #[cfg(feature = "slow-trace")]
        self.record_read_profile(values.len(), read_start);
        slow_trace::log_query_stage(
            "query_vlog_lookup",
            read_start,
            "component=vlog_read_blocks",
        );

        let sort_start = slow_trace::now();
        if self.ascending {
            values.sort_by_key(|v| v.unix_nano);
        } else {
            values.sort_by_key(|v| Reverse(v.unix_nano));
        }
        #[cfg(feature = "slow-trace")]
        self.record_sort_profile(sort_start);
        slow_trace::log_query_stage("query_merge_sort", sort_start, "component=vlog_sort");

        Ok(values)
    }

    /// Reads the next block as a set of unsigned values.
    pub fn read_unsigned_block(&mut self) -> Result<Vec<UnsignedValue>> {
        if self.indexed_stream.is_some() {
            return self.read_indexed_values()?.into_iter().map(|value| match value {
                Value::UnsignedValue(value) => Ok(value),
                _ => Err(reader::Error::StringErr { reason: "aggregate block type mismatch".into() }),
            }).collect();
        }
        let read_start = slow_trace::now();
        // No matching blocks to decode
        if self.current.is_empty() {
            return Ok(vec![]);
        }

        let mut notifications = vec![];
        for (r, indexes) in self.current.iter() {
            let r = Arc::clone(r);
            let indexes = indexes.clone();
            let key = self.key.clone();
            let (sender, receiver) = mpsc::sync_channel(1);
            let task = self.executor
                .spawn_blocking(move || {
                    let _ = sender.send(
                        r.read_unsigned_multi_block_and_exclude_tombstones(&r, &key, &indexes),
                    );
                })
                .map_err(|source| reader::Error::Execution { source })?;
            notifications.push((receiver, task));
        }

        let mut values = vec![];
        for (notify, _task) in notifications {
            let mut vals = notify.recv().map_err(|_| reader::Error::StringErr {
                reason: format!("read_unsigned_block from vlog failed: {:?}", TaskJoinError),
            })??;
            values.append(&mut vals);
        }
        #[cfg(feature = "slow-trace")]
        self.record_read_profile(values.len(), read_start);
        slow_trace::log_query_stage(
            "query_vlog_lookup",
            read_start,
            "component=vlog_read_blocks",
        );

        let sort_start = slow_trace::now();
        if self.ascending {
            values.sort_by_key(|v| v.unix_nano);
        } else {
            values.sort_by_key(|v| Reverse(v.unix_nano));
        }
        #[cfg(feature = "slow-trace")]
        self.record_sort_profile(sort_start);
        slow_trace::log_query_stage("query_merge_sort", sort_start, "component=vlog_sort");

        Ok(values)
    }

    /// Reads the next block as a set of float values.
    pub fn read_float_block(&mut self) -> Result<Vec<FloatValue>> {
        if self.indexed_stream.is_some() {
            return self.read_indexed_values()?.into_iter().map(|value| match value {
                Value::FloatValue(value) => Ok(value),
                _ => Err(reader::Error::StringErr { reason: "aggregate block type mismatch".into() }),
            }).collect();
        }
        let read_start = slow_trace::now();
        // No matching blocks to decode
        if self.current.is_empty() {
            return Ok(vec![]);
        }

        let mut notifications = vec![];
        for (r, indexes) in self.current.iter() {
            let r = Arc::clone(r);
            let indexes = indexes.clone();
            let key = self.key.clone();
            let (sender, receiver) = mpsc::sync_channel(1);
            let task = self.executor
                .spawn_blocking(move || {
                    let _ = sender.send(
                        r.read_float_multi_block_and_exclude_tombstones(&r, &key, &indexes),
                    );
                })
                .map_err(|source| reader::Error::Execution { source })?;
            notifications.push((receiver, task));
        }

        let mut values = vec![];
        for (notify, _task) in notifications {
            let mut vals = notify.recv().map_err(|_| reader::Error::StringErr {
                reason: format!("read_float_block from vlog failed: {:?}", TaskJoinError),
            })??;
            values.append(&mut vals);
        }
        #[cfg(feature = "slow-trace")]
        self.record_read_profile(values.len(), read_start);
        slow_trace::log_query_stage(
            "query_vlog_lookup",
            read_start,
            "component=vlog_read_blocks",
        );

        let sort_start = slow_trace::now();
        if self.ascending {
            values.sort_by_key(|v| v.unix_nano);
        } else {
            values.sort_by_key(|v| Reverse(v.unix_nano));
        }
        #[cfg(feature = "slow-trace")]
        self.record_sort_profile(sort_start);
        slow_trace::log_query_stage("query_merge_sort", sort_start, "component=vlog_sort");

        Ok(values)
    }

    /// Reads the next block as a set of bool values.
    pub fn read_bool_block(&mut self) -> Result<Vec<BoolValue>> {
        if self.indexed_stream.is_some() {
            return self.read_indexed_values()?.into_iter().map(|value| match value {
                Value::BoolValue(value) => Ok(value),
                _ => Err(reader::Error::StringErr { reason: "aggregate block type mismatch".into() }),
            }).collect();
        }
        let read_start = slow_trace::now();
        // No matching blocks to decode
        if self.current.is_empty() {
            return Ok(vec![]);
        }

        let mut notifications = vec![];
        for (r, indexes) in self.current.iter() {
            let r = Arc::clone(r);
            let indexes = indexes.clone();
            let key = self.key.clone();
            let (sender, receiver) = mpsc::sync_channel(1);
            let task = self.executor
                .spawn_blocking(move || {
                    let _ = sender.send(
                        r.read_bool_multi_block_and_exclude_tombstones(&r, &key, &indexes),
                    );
                })
                .map_err(|source| reader::Error::Execution { source })?;
            notifications.push((receiver, task));
        }

        let mut values = vec![];
        for (notify, _task) in notifications {
            let mut vals = notify.recv().map_err(|_| reader::Error::StringErr {
                reason: format!("read_bool_block from vlog failed: {:?}", TaskJoinError),
            })??;
            values.append(&mut vals);
        }
        #[cfg(feature = "slow-trace")]
        self.record_read_profile(values.len(), read_start);
        slow_trace::log_query_stage(
            "query_vlog_lookup",
            read_start,
            "component=vlog_read_blocks",
        );

        let sort_start = slow_trace::now();
        if self.ascending {
            values.sort_by_key(|v| v.unix_nano);
        } else {
            values.sort_by_key(|v| Reverse(v.unix_nano));
        }
        #[cfg(feature = "slow-trace")]
        self.record_sort_profile(sort_start);
        slow_trace::log_query_stage("query_merge_sort", sort_start, "component=vlog_sort");

        Ok(values)
    }

    /// Reads the next block as a set of string values.
    pub fn read_string_block(&mut self) -> Result<Vec<StringValue>> {
        if self.indexed_stream.is_some() {
            return self.read_indexed_values()?.into_iter().map(|value| match value {
                Value::StringValue(value) => Ok(value),
                _ => Err(reader::Error::StringErr { reason: "aggregate block type mismatch".into() }),
            }).collect();
        }
        let read_start = slow_trace::now();
        // No matching blocks to decode
        if self.current.is_empty() {
            return Ok(vec![]);
        }

        let mut notifications = vec![];
        for (r, indexes) in self.current.iter() {
            let r = Arc::clone(r);
            let indexes = indexes.clone();
            let key = self.key.clone();
            let (sender, receiver) = mpsc::sync_channel(1);
            let task = self.executor
                .spawn_blocking(move || {
                    let _ = sender.send(
                        r.read_string_multi_block_and_exclude_tombstones(&r, &key, &indexes),
                    );
                })
                .map_err(|source| reader::Error::Execution { source })?;
            notifications.push((receiver, task));
        }

        let mut values = vec![];
        for (notify, _task) in notifications {
            let mut vals = notify.recv().map_err(|_| reader::Error::StringErr {
                reason: format!("read_string_block from vlog failed: {:?}", TaskJoinError),
            })??;
            values.append(&mut vals);
        }
        #[cfg(feature = "slow-trace")]
        self.record_read_profile(values.len(), read_start);
        slow_trace::log_query_stage(
            "query_vlog_lookup",
            read_start,
            "component=vlog_read_blocks",
        );

        let sort_start = slow_trace::now();
        if self.ascending {
            values.sort_by_key(|v| v.unix_nano);
        } else {
            values.sort_by_key(|v| Reverse(v.unix_nano));
        }
        #[cfg(feature = "slow-trace")]
        self.record_sort_profile(sort_start);
        slow_trace::log_query_stage("query_merge_sort", sort_start, "component=vlog_sort");

        Ok(values)
    }
}

#[derive(Clone)]
pub struct VLogLocation {
    pub r: Arc<VLogReader>,
    pub vlog_index: VLogIndex,
}

/// Returns the files and index blocks for a key and time.  ascending indicates
/// whether the key will be scan in ascending time order or descenging time order.
/// This function assumes the read-lock has been taken.
pub fn vlog_locations(
    files: &[Arc<VLogReader>],
    vlog_indexes: VLogIndexes,
    key: &[u8],
) -> Vec<VLogLocation> {
    let mut locations = vec![];
    for f in files {
        'outer: for idx in vlog_indexes.iter() {
            if (idx.generation, idx.seq) == (f.generation, f.seq) {
                // TODO: optimize me, iterator a vary large vlog_indexes mutli times.
                let tombstones = f.tombstone_range(key);

                // Skip any blocks only contain values that are tombstoned.
                for t in tombstones.iter() {
                    if t.min <= idx.min && t.max >= idx.max {
                        continue 'outer;
                    }
                }

                let location = VLogLocation {
                    r: Arc::clone(f),
                    vlog_index: idx.clone(),
                };

                locations.push(location);
            }
        }
    }
    locations
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, path::Path, sync::Arc};

    use crate::models::points::{FLOAT, INTEGER};

    use super::*;
    use crate::storage::tsm1::{
        cursor::array_values::{FloatArray, IntegerArray},
        data_encoding::encoding_values::{encode_float_array_block, encode_integer_array_block},
        vlog::{
            compact::VLOG_FILE_EXTENSION, file_store::default_format_file_name,
            vlog_index_cache::Cache, writer::VLogWriter,
        },
    };

    fn encoded_float_block(timestamp: i64, value: f64) -> Vec<u8> {
        let mut values = FloatArray {
            timestamps: vec![timestamp],
            values: vec![value],
        };
        encode_float_array_block(&mut values, &[]).unwrap()
    }

    fn encoded_integer_block(timestamp: i64, value: i64) -> Vec<u8> {
        let mut values = IntegerArray {
            timestamps: vec![timestamp],
            values: vec![value],
        };
        encode_integer_array_block(&mut values, &[]).unwrap()
    }

    fn write_reader_with_shifted_integer_block(
        dir: &Path,
        generation: i64,
        pad_blocks: usize,
        pad_key: &[u8],
        target_key: &[u8],
    ) -> (Arc<VLogReader>, VLogIndex) {
        let seq = 1;
        let path = dir.join(format!(
            "{}.{}",
            default_format_file_name(generation, seq),
            VLOG_FILE_EXTENSION
        ));
        let fd = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let mut writer = VLogWriter::new(fd, generation, seq, &path);
        let mut indexes_by_key = HashMap::<String, Vec<VLogIndex>>::new();

        for i in 0..pad_blocks {
            let timestamp = i as i64;
            let block = encoded_float_block(timestamp, timestamp as f64);
            let (offset, size) = writer
                .write_block(pad_key, timestamp, timestamp, &block)
                .unwrap();
            indexes_by_key
                .entry(String::from_utf8(pad_key.to_vec()).unwrap())
                .or_default()
                .push(VLogIndex {
                    aggregate: None,
                    count: 1,
                    typ: FLOAT,
                    min: timestamp,
                    max: timestamp,
                    generation,
                    seq,
                    offset,
                    size,
                });
        }

        let block = encoded_integer_block(10, 1000 + generation);
        let (offset, size) = writer.write_block(target_key, 10, 10, &block).unwrap();
        let target_index = VLogIndex {
            aggregate: None,
            count: 1,
            typ: INTEGER,
            min: 10,
            max: 10,
            generation,
            seq,
            offset,
            size,
        };
        indexes_by_key
            .entry(String::from_utf8(target_key.to_vec()).unwrap())
            .or_default()
            .push(target_index.clone());

        writer.close().unwrap();

        let cache = Arc::new(Cache::new(0));
        cache.write_multi(indexes_by_key).unwrap();
        let reader = VLogReader::new(
            fs::OpenOptions::new().read(true).open(&path).unwrap(),
            &path,
            generation,
            seq,
            vec![(pad_key.to_vec(), FLOAT), (target_key.to_vec(), INTEGER)],
            cache,
        )
        .unwrap();

        (Arc::new(reader), target_index)
    }

    #[test]
    fn test_vlog_key_cursor_overlap_batch_keeps_reader_index_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let pad_key = b"cpu,host=A#!~#pad";
        let target_key = b"cpu,host=A#!~#value";
        let reader_count = num_cpus::get() + 3;
        let mut readers = Vec::with_capacity(reader_count);
        let mut indexes = Vec::with_capacity(reader_count);

        for generation in 1..=reader_count {
            let (reader, index) = write_reader_with_shifted_integer_block(
                dir.path(),
                generation as i64,
                generation,
                pad_key,
                target_key,
            );
            readers.push(reader);
            indexes.push(index);
        }

        let mut cursor = VLogKeyCursor::new(&readers, VLogIndexes(indexes), target_key, 0, true);
        let mut values = cursor.read_integer_block().unwrap();
        cursor.close();

        values.sort_by_key(|value| value.value);
        let got = values
            .into_iter()
            .map(|value| value.value)
            .collect::<Vec<_>>();
        let expected = (1..=reader_count)
            .map(|generation| 1000 + generation as i64)
            .collect::<Vec<_>>();
        assert_eq!(got, expected);
    }


    mod nested_executor_cursor_reads {
        use super::*;
        use crate::models::points::{BOOL, STRING, UNSIGNED};
        use crate::runtime::execution::{
            BoxBlockingTask, BoxFutureTask, RuntimeExecutor, SpawnError, TaskController,
        };
        use crate::storage::tsm1::{
            cursor::array_values::{BoolArray, StringArray, UnsignedArray},
            data_encoding::encoding_values::{
                encode_bool_array_block, encode_string_array_block, encode_unsigned_array_block,
            },
        };
        use std::{
            panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
            sync::atomic::{AtomicUsize, Ordering},
        };

        const KEY: &[u8] = b"cpu,hostname=host_0#!~#value";
        const TYPES: [u8; 5] = [INTEGER, UNSIGNED, FLOAT, BOOL, STRING];

        fn encoded_values(typ: u8, generation: i64) -> (Vec<u8>, Vec<Value>) {
            let timestamps = vec![10, 20, 30];
            match typ {
                INTEGER => {
                    let values = vec![100 * generation - 2, 100 * generation, 100 * generation + 5];
                    let expected = timestamps.iter().zip(&values).map(|(&t, &v)|
                        Value::IntegerValue(IntegerValue::new(t, v))).collect();
                    (encode_integer_array_block(&mut IntegerArray { timestamps, values }, &[]).unwrap(), expected)
                }
                UNSIGNED => {
                    let values = vec![generation as u64, 100 * generation as u64, u64::MAX - generation as u64];
                    let expected = timestamps.iter().zip(&values).map(|(&t, &v)|
                        Value::UnsignedValue(UnsignedValue::new(t, v))).collect();
                    (encode_unsigned_array_block(&mut UnsignedArray { timestamps, values }, &[]).unwrap(), expected)
                }
                FLOAT => {
                    let values = vec![-1.5 * generation as f64, 0.0, 3.25 * generation as f64];
                    let expected = timestamps.iter().zip(&values).map(|(&t, &v)|
                        Value::FloatValue(FloatValue::new(t, v))).collect();
                    (encode_float_array_block(&mut FloatArray { timestamps, values }, &[]).unwrap(), expected)
                }
                BOOL => {
                    let values = vec![true, false, generation % 2 == 0];
                    let expected = timestamps.iter().zip(&values).map(|(&t, &v)|
                        Value::BoolValue(BoolValue::new(t, v))).collect();
                    (encode_bool_array_block(&mut BoolArray { timestamps, values }, &[]).unwrap(), expected)
                }
                STRING => {
                    let values = vec![String::new(), format!("generation={generation}, quoted=\"value\""),
                                      format!("\u{72b6}\u{6001}_{generation}\\tail")];
                    let expected = timestamps.iter().zip(&values).map(|(&t, v)|
                        Value::StringValue(StringValue::new(t, v.clone()))).collect();
                    (encode_string_array_block(&mut StringArray { timestamps, values }, &[]).unwrap(), expected)
                }
                _ => panic!("Unsupported cursor regression field type"),
            }
        }

        fn write_typed_reader(dir: &Path, typ: u8, generation: i64)
            -> (Arc<VLogReader>, VLogIndex, Vec<Value>)
        {
            let seq = 1;
            let path = dir.join(format!("{}.{}", default_format_file_name(generation, seq), VLOG_FILE_EXTENSION));
            let fd = fs::OpenOptions::new().read(true).write(true).create_new(true).open(&path).unwrap();
            let mut writer = VLogWriter::new(fd, generation, seq, &path);
            let (block, expected) = encoded_values(typ, generation);
            let (offset, size) = writer.write_block(KEY, 10, 30, &block).unwrap();
            let index = VLogIndex {
            aggregate: None, count: 3, typ, min: 10, max: 30, generation, seq, offset, size };
            writer.close().unwrap();
            assert!(fs::metadata(&path).unwrap().len() > 5);
            let cache = Arc::new(Cache::new(0));
            cache.write_multi(HashMap::from([(String::from_utf8(KEY.to_vec()).unwrap(), vec![index.clone()])])).unwrap();
            let reader = Arc::new(VLogReader::new(
                fs::OpenOptions::new().read(true).open(&path).unwrap(), &path,
                generation, seq, vec![(KEY.to_vec(), typ)], cache).unwrap());
            (reader, index, expected)
        }

        fn read_typed(cursor: &mut VLogKeyCursor, typ: u8) -> Result<Vec<Value>> {
            match typ {
                INTEGER => cursor.read_integer_block().map(|v| v.into_iter().map(Value::IntegerValue).collect()),
                UNSIGNED => cursor.read_unsigned_block().map(|v| v.into_iter().map(Value::UnsignedValue).collect()),
                FLOAT => cursor.read_float_block().map(|v| v.into_iter().map(Value::FloatValue).collect()),
                BOOL => cursor.read_bool_block().map(|v| v.into_iter().map(Value::BoolValue).collect()),
                STRING => cursor.read_string_block().map(|v| v.into_iter().map(Value::StringValue).collect()),
                _ => panic!("Unsupported cursor regression field type"),
            }
        }

        fn read_and_close(mut cursor: VLogKeyCursor, readers: Vec<Arc<VLogReader>>, typ: u8)
            -> Result<Vec<Value>>
        {
            // A real synchronous persisted read is polled inside an entered LocalPool.
            // Do not use should_panic: the same test must fail before and pass after repair.
            let outcome = catch_unwind(AssertUnwindSafe(|| futures::executor::block_on(async {
                read_typed(&mut cursor, typ)
            })));
            cursor.close();
            for reader in readers { reader.close().unwrap(); }
            match outcome { Ok(result) => result, Err(panic) => resume_unwind(panic) }
        }

        fn verify_typed_read(typ: u8) {
            for ascending in [true, false] {
                for tombstone in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let mut readers = Vec::new();
                    let mut indexes = Vec::new();
                    let mut expected = Vec::new();
                    for generation in 1..=2 {
                        let (reader, index, values) = write_typed_reader(dir.path(), typ, generation);
                        if tombstone {
                            VLogReader::delete_range(Arc::clone(&reader), &[KEY], 20, 20).unwrap();
                        }
                        readers.push(reader); indexes.push(index); expected.extend(values);
                    }
                    if tombstone { expected.retain(|value| value.unix_nano() != 20); }
                    if ascending { expected.sort_by_key(Value::unix_nano); }
                    else { expected.sort_by_key(|value| Reverse(value.unix_nano())); }
                    let cursor = VLogKeyCursor::new(&readers, VLogIndexes(indexes), KEY,
                        if ascending { 0 } else { i64::MAX }, ascending);
                    assert!(!cursor.current.is_empty(), "the actual legacy reader path must be selected");
                    assert!(cursor.indexed_stream.is_none());
                    let actual = read_and_close(cursor, readers, typ).unwrap();
                    assert_eq!(actual, expected, "type={typ} ascending={ascending} tombstone={tombstone}");
                }
            }
        }

        #[test]
        fn persisted_integer_read_inside_executor_keeps_values_order_and_tombstones() { verify_typed_read(INTEGER); }
        #[test]
        fn persisted_unsigned_read_inside_executor_keeps_values_order_and_tombstones() { verify_typed_read(UNSIGNED); }
        #[test]
        fn persisted_float_read_inside_executor_keeps_values_order_and_tombstones() { verify_typed_read(FLOAT); }
        #[test]
        fn persisted_bool_read_inside_executor_keeps_values_order_and_tombstones() { verify_typed_read(BOOL); }
        #[test]
        fn persisted_string_read_inside_executor_keeps_values_order_and_tombstones() { verify_typed_read(STRING); }

        #[derive(Clone, Copy)]
        enum Disposition { Reject, DropAccepted, PanicAccepted }

        struct FaultExecutor { disposition: Disposition, submissions: Arc<AtomicUsize> }

        impl RuntimeExecutor for FaultExecutor {
            fn schedule(&self, _task: BoxFutureTask) -> std::result::Result<TaskController, SpawnError> {
                Err(SpawnError::rejected("Unexpected async cursor task"))
            }
            fn schedule_blocking(&self, task: BoxBlockingTask) -> std::result::Result<TaskController, SpawnError> {
                self.submissions.fetch_add(1, Ordering::SeqCst);
                match self.disposition {
                    Disposition::Reject => Err(SpawnError::rejected("Injected VLog read submission refusal")),
                    Disposition::DropAccepted => { drop(task); Ok(TaskController::detached()) }
                    Disposition::PanicAccepted => {
                        // The existing global blocking executor owns and catches the worker panic.
                        legacy_global_executor().schedule_blocking(Box::new(move || {
                            let _accepted_task = task;
                            panic!("Injected accepted VLog reader worker panic");
                        }))
                    }
                }
            }
            fn schedule_compact(&self, _task: BoxFutureTask) -> std::result::Result<TaskController, SpawnError> {
                Err(SpawnError::rejected("Unexpected compact cursor task"))
            }
            fn schedule_blocking_compact(&self, _task: BoxBlockingTask) -> std::result::Result<TaskController, SpawnError> {
                Err(SpawnError::rejected("Unexpected blocking compact cursor task"))
            }
        }

        fn verify_executor_failure(disposition: Disposition) {
            for typ in TYPES {
                let dir = tempfile::tempdir().unwrap();
                let (reader, index, _) = write_typed_reader(dir.path(), typ, 1);
                let readers = vec![reader];
                let submissions = Arc::new(AtomicUsize::new(0));
                let executor = Arc::new(FaultExecutor { disposition, submissions: Arc::clone(&submissions) });
                let cursor = VLogKeyCursor::new_with_executor(&readers, VLogIndexes(vec![index]), KEY, 0, true, executor);
                let error = read_and_close(cursor, readers, typ).unwrap_err();
                match disposition {
                    Disposition::Reject => match error {
                        reader::Error::Execution { source } => assert_eq!(source.reason(), "Injected VLog read submission refusal"),
                        other => panic!("Spawn refusal changed error type: {other:?}"),
                    },
                    _ => match error {
                        reader::Error::StringErr { reason } => {
                            assert!(reason.contains("from vlog failed: TaskJoinError"), "{reason}");
                        }
                        other => panic!("Accepted worker failure changed error type: {other:?}"),
                    },
                }
                assert_eq!(submissions.load(Ordering::SeqCst), 1);
            }
        }

        #[test]
        fn actual_cursor_preserves_injected_submit_refusal_for_every_type() { verify_executor_failure(Disposition::Reject); }
        #[test]
        fn actual_cursor_accepted_worker_drop_returns_error_for_every_type() { verify_executor_failure(Disposition::DropAccepted); }
        #[test]
        fn actual_cursor_accepted_worker_panic_returns_error_for_every_type() { verify_executor_failure(Disposition::PanicAccepted); }

        #[test]
        fn actual_cursor_native_io_failure_is_propagated_without_replacing_the_error() {
            for typ in TYPES {
                let dir = tempfile::tempdir().unwrap();
                let (reader, index, _) = write_typed_reader(dir.path(), typ, 1);
                fs::OpenOptions::new().write(true).open(reader.path()).unwrap().set_len(5).unwrap();
                let readers = vec![reader];
                let cursor = VLogKeyCursor::new(&readers, VLogIndexes(vec![index]), KEY, 0, true);
                match read_and_close(cursor, readers, typ).unwrap_err() {
                    reader::Error::IO { reason } => assert_eq!(reason.kind(), std::io::ErrorKind::UnexpectedEof),
                    other => panic!("Native reader IO error changed type: {other:?}"),
                }
            }
        }
    }
}
