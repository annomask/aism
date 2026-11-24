//! Shared index bytes and bounded traversal/worker-local validated metadata.

use parking_lot::RwLock;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}},
};

pub(crate) const DEFAULT_CACHE_BYTES: usize = 32 * 1024 * 1024;
const ENTRY_CHARGE: usize = 192;
const MAX_ADMISSION: usize = 64 * 1024;

/// One worker reuses validated directory metadata from up to eight immutable
/// files. Pinned Arcs prevent address reuse; fresh file views still select
/// every source and tombstone independently for each storage key.
#[derive(Default)]
pub(crate) struct QueryDirectoryCache {
    file: Option<Arc<super::reader::IndexFile>>,
    minima: Option<(usize, Box<[u64]>)>,
    directory: Option<(usize, Box<[super::directory::DirEntry]>)>,
    previous: VecDeque<DirectoryPages>,
}

struct DirectoryPages {
    file: Arc<super::reader::IndexFile>,
    minima: Option<(usize, Box<[u64]>)>,
    directory: Option<(usize, Box<[super::directory::DirEntry]>)>,
}

impl QueryDirectoryCache {
    fn select_file(&mut self, file: &Arc<super::reader::IndexFile>) {
        if self.file.as_ref().is_some_and(|previous| Arc::ptr_eq(previous, file)) {
            return;
        }
        // Reopened/replaced files with the same number must never reuse pages.
        self.previous.retain(|entry| entry.file.file_no != file.file_no || Arc::ptr_eq(&entry.file, file));
        let cached = self.previous.iter().position(|entry| Arc::ptr_eq(&entry.file, file))
            .and_then(|index| self.previous.remove(index));
        if let Some(previous) = self.file.take() {
            if previous.file_no != file.file_no {
                self.previous.push_back(DirectoryPages {
                    file: previous,
                    minima: self.minima.take(),
                    directory: self.directory.take(),
                });
            }
        }
        while self.previous.len() > 7 {
            self.previous.pop_front();
        }
        let pages = cached.unwrap_or_else(|| DirectoryPages {
            file: Arc::clone(file), minima: None, directory: None,
        });
        self.file = Some(pages.file);
        self.minima = pages.minima;
        self.directory = pages.directory;
    }

    pub(super) fn minima(
        &mut self,
        file: &Arc<super::reader::IndexFile>,
        page: usize,
        load: impl FnOnce() -> super::Result<Vec<u64>>,
    ) -> super::Result<&[u64]> {
        self.select_file(file);
        if self.minima.as_ref().is_none_or(|(previous, _)| *previous != page) {
            let values = load()?;
            if values.is_empty() || values.len() > super::directory::INDEX_PER_PAGE {
                return Err(super::invalid("invalid cached directory minima size"));
            }
            self.minima = Some((page, values.into_boxed_slice()));
        } else {
            crate::common::slow_trace::log_query_stage(
                "query_index_metadata_cache_hit", crate::common::slow_trace::now(), "page_index",
            );
        }
        Ok(&self.minima.as_ref().unwrap().1)
    }

    pub(super) fn directory(
        &mut self,
        file: &Arc<super::reader::IndexFile>,
        page: usize,
        load: impl FnOnce() -> super::Result<Vec<super::directory::DirEntry>>,
    ) -> super::Result<&[super::directory::DirEntry]> {
        self.select_file(file);
        if self.directory.as_ref().is_none_or(|(previous, _)| *previous != page) {
            let entries = load()?;
            if entries.is_empty() || entries.len() > super::directory::DIR_PER_PAGE {
                return Err(super::invalid("invalid cached directory page size"));
            }
            self.directory = Some((page, entries.into_boxed_slice()));
        } else {
            crate::common::slow_trace::log_query_stage(
                "query_index_metadata_cache_hit", crate::common::slow_trace::now(), "directory",
            );
        }
        Ok(&self.directory.as_ref().unwrap().1)
    }

    #[cfg(test)]
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.minima.as_ref().map_or(0, |(_, values)| std::mem::size_of_val(&**values))
            + self.directory.as_ref().map_or(0, |(_, values)| std::mem::size_of_val(&**values))
            + self.previous.capacity() * std::mem::size_of::<DirectoryPages>()
            + self.previous.iter().map(|pages| {
                pages.minima.as_ref().map_or(0, |(_, values)| std::mem::size_of_val(&**values))
                    + pages.directory.as_ref().map_or(0, |(_, values)| std::mem::size_of_val(&**values))
            }).sum::<usize>()
    }
}

#[cfg(test)]
#[path = "directory_cache_tests.rs"]
mod directory_cache_tests;

/// Traversal-local decoded groups never compete with the shared byte cache.
/// At most one page group per tree level is cached. DFS batch views additionally
/// pin their pending groups, bounded by the frontier of each captured source.
pub(crate) struct QueryGroupCache {
    groups: [Option<(
        (u32, u64, usize),
        Arc<super::projected_batch::ProjectedGroup>,
    )>; super::MAX_LEVELS as usize],
    tables: [Option<((u32, u64, usize, u64), Arc<[super::page_group::PageGroupEntry]>)>;
        super::MAX_LEVELS as usize],
}
impl Default for QueryGroupCache {
    fn default() -> Self {
        Self {
            groups: std::array::from_fn(|_| None),
            tables: std::array::from_fn(|_| None),
        }
    }
}
impl QueryGroupCache {
    pub(super) fn table(
        &mut self,
        level: super::Level,
        key: (u32, u64, usize, u64),
        load: impl FnOnce() -> super::Result<Vec<super::page_group::PageGroupEntry>>,
    ) -> super::Result<Arc<[super::page_group::PageGroupEntry]>> {
        let slot = self.tables.get_mut(level.checked_sub(1)
            .ok_or_else(|| super::invalid("invalid table cache level"))? as usize)
            .ok_or_else(|| super::invalid("invalid table cache level"))?;
        if let Some((previous, entries)) = slot {
            if *previous == key {
                crate::common::slow_trace::log_query_stage(
                    "query_index_metadata_cache_hit", crate::common::slow_trace::now(), "group_table",
                );
                return Ok(Arc::clone(entries));
            }
        }
        let entries: Arc<[_]> = load()?.into();
        // Large paged tables bypass this cache; small decoded tables have a
        // separate fixed bound of 64 KiB per tree level and traversal.
        if std::mem::size_of_val(&*entries) <= 64 * 1024 {
            *slot = Some((key, Arc::clone(&entries)));
        }
        Ok(entries)
    }

    pub(super) fn group(
        &mut self,
        level: super::Level,
        key: (u32, u64, usize),
        load: impl FnOnce() -> super::Result<super::projected_batch::ProjectedGroup>,
    ) -> super::Result<(bool, &Arc<super::projected_batch::ProjectedGroup>)> {
        let slot = self
            .groups
            .get_mut(
                level
                    .checked_sub(1)
                    .ok_or_else(|| super::invalid("invalid group cache level"))?
                    as usize,
            )
            .ok_or_else(|| super::invalid("invalid group cache level"))?;
        let hit = slot.as_ref().is_some_and(|(previous, _)| *previous == key);
        if !hit {
            let records = load()?;
            if records.is_empty()
                || records.len() > super::PAGE_GROUP
                || records.capacity() > super::PAGE_GROUP
            {
                return Err(super::invalid("invalid decoded group cache size"));
            }
            *slot = Some((key, Arc::new(records)));
        } else {
            crate::common::slow_trace::log_query_stage(
                "query_index_projection_cache_hit",
                crate::common::slow_trace::now(),
                "traversal-local validated group",
            );
        }
        Ok((hit, &slot.as_ref().unwrap().1))
    }
}
type Key = (u32, u64, usize);
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CacheStats {
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) bytes: usize,
    pub(crate) entries: usize,
}
const MAX_CACHE_SHARDS: usize = 16;
const MAX_SECOND_CHANCES: usize = 8;

struct Entry {
    bytes: Arc<[u8]>,
    referenced: AtomicBool,
}
struct Inner {
    budget: usize,
    bytes: usize,
    clock: u64,
    // Tree storage is released on eviction, without retained table capacity.
    values: BTreeMap<Key, Entry>,
    order: BTreeMap<u64, Key>,
}
struct CacheShard {
    inner: RwLock<Inner>,
    hits: AtomicU64,
    misses: AtomicU64,
}
pub(crate) struct ReadCache {
    shards: Box<[CacheShard]>,
}
impl ReadCache {
    pub(crate) fn new(budget: usize) -> Arc<Self> {
        let count = (budget / (MAX_ADMISSION + ENTRY_CHARGE))
            .clamp(1, MAX_CACHE_SHARDS).next_power_of_two();
        Arc::new(Self {
            shards: (0..count).map(|index| CacheShard {
                inner: RwLock::new(Inner {
                    budget: budget / count + usize::from(index < budget % count),
                    bytes: 0,
                    clock: 0,
                    values: BTreeMap::new(),
                    order: BTreeMap::new(),
                }),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
            }).collect(),
        })
    }
    fn shard(&self, key: Key) -> &CacheShard {
        // Mix all identity parts; aligned offsets must not select one shard.
        let mut hash = u64::from(key.0).wrapping_mul(0x9e3779b97f4a7c15)
            ^ key.1.rotate_left(21) ^ (key.2 as u64).rotate_left(41);
        hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d049bb133111eb);
        hash ^= hash >> 31;
        &self.shards[hash as usize & (self.shards.len() - 1)]
    }
    pub(crate) fn set_budget(&self, budget: usize) {
        let count = self.shards.len();
        for (index, shard) in self.shards.iter().enumerate() {
            let mut inner = shard.inner.write();
            inner.budget = budget / count + usize::from(index < budget % count);
            Self::evict(&mut inner);
        }
    }
    fn evict(inner: &mut Inner) {
        if inner.budget == 0 {
            inner.values.clear();
            inner.order.clear();
            inner.bytes = 0;
            return;
        }
        let mut chances = 0;
        while inner.bytes > inner.budget {
            let (_, key) = inner.order.pop_first().unwrap();
            if chances < MAX_SECOND_CHANCES
                && inner.values[&key].referenced.swap(false, Ordering::Relaxed)
            {
                // Bound work under the write lock even if every entry is hot.
                chances += 1;
                inner.clock += 1;
                let clock = inner.clock;
                inner.order.insert(clock, key);
                continue;
            }
            let entry = inner.values.remove(&key).unwrap();
            inner.bytes -= entry.bytes.len() + ENTRY_CHARGE;
        }
    }
    pub(super) fn get(&self, key: Key) -> Option<Arc<[u8]>> {
        let shard = self.shard(key);
        let started = crate::common::slow_trace::now();
        let (bytes, acquired) = {
            let inner = shard.inner.read();
            let acquired = crate::common::slow_trace::now();
            let bytes = inner.values.get(&key).map(|entry| {
                entry.referenced.store(true, Ordering::Relaxed);
                Arc::clone(&entry.bytes)
            });
            (bytes, acquired)
        };
        let released = crate::common::slow_trace::now();
        if bytes.is_some() {
            shard.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            shard.misses.fetch_add(1, Ordering::Relaxed);
        }
        // Record after release so diagnostics do not extend the critical section.
        crate::common::slow_trace::log_lock_wait_until(
            "query_wait_for_index_cache_lock", started, acquired, "shared index cache get",
        );
        crate::common::slow_trace::log_query_stage_until(
            "query_index_cache_lock_work", acquired, released, "shared index cache get",
        );
        bytes
    }
    pub(super) fn insert(&self, key: Key, bytes: &[u8]) {
        if bytes.len() > MAX_ADMISSION {
            return;
        }
        let shard = self.shard(key);
        let charge = bytes.len() + ENTRY_CHARGE;
        let eligible = {
            let inner = shard.inner.read();
            charge <= inner.budget && !inner.values.contains_key(&key)
        };
        if !eligible {
            return;
        }
        // Allocate outside the write lock, then recheck publication and budget.
        let value: Arc<[u8]> = bytes.into();
        let mut inner = shard.inner.write();
        if charge > inner.budget || inner.values.contains_key(&key) {
            return;
        }
        inner.clock += 1;
        let clock = inner.clock;
        inner.bytes += charge;
        inner.order.insert(clock, key);
        inner.values.insert(key, Entry { bytes: value, referenced: AtomicBool::new(false) });
        Self::evict(&mut inner);
    }
    pub(crate) fn stats(&self) -> CacheStats {
        let mut stats = CacheStats::default();
        for shard in &self.shards {
            let inner = shard.inner.read();
            stats.bytes += inner.bytes;
            stats.entries += inner.values.len();
            stats.hits += shard.hits.load(Ordering::Relaxed);
            stats.misses += shard.misses.load(Ordering::Relaxed);
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn referenced_entry_gets_a_bounded_second_chance() {
        let cache = ReadCache::new(2 * (ENTRY_CHARGE + 1));
        let a = (1, 1, 1);
        let b = (1, 2, 1);
        let c = (1, 3, 1);
        cache.insert(a, &[1]);
        cache.insert(b, &[2]);
        let pinned = cache.get(a).unwrap();
        cache.insert(c, &[3]);
        assert!(cache.get(a).is_some());
        assert!(cache.get(b).is_none());
        assert!(cache.get(c).is_some());
        cache.set_budget(0);
        assert_eq!(&*pinned, &[1]);
        assert_eq!(cache.stats().bytes, 0);
    }

    #[test]
    fn sharded_cache_preserves_budget_and_identity_under_concurrent_access() {
        let budget = 2 * 1024 * 1024;
        let cache = ReadCache::new(budget);
        assert_eq!(cache.shards.len(), MAX_CACHE_SHARDS);
        let mut workers = Vec::new();
        let resizing = Arc::clone(&cache);
        let resize_worker = std::thread::spawn(move || {
            for _ in 0..20 {
                resizing.set_budget(budget / 2);
                resizing.set_budget(budget);
            }
        });
        for owner in 1..=8 {
            let cache = Arc::clone(&cache);
            workers.push(std::thread::spawn(move || {
                for page in 0..500 {
                    let key = (owner, page * 4096, 256);
                    cache.insert(key, &[owner as u8; 256]);
                    if let Some(value) = cache.get(key) {
                        assert_eq!(&*value, &[owner as u8; 256]);
                    }
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        resize_worker.join().unwrap();
        assert!(cache.stats().bytes <= budget);
        assert_eq!(cache.stats().hits + cache.stats().misses, 4000);
        cache.set_budget(1024);
        assert!(cache.stats().bytes <= 1024);
        cache.set_budget(0);
        assert_eq!(cache.stats().entries, 0);
    }

    #[test]
    fn traversal_cache_is_bounded_by_levels_and_keys_include_file_and_projection() {
        use super::super::{BlockLoc, Position, SeqNo, page_group::SelectedRecord};
        let record = SelectedRecord {
            seq_no: SeqNo::new(1, 1).unwrap(),
            level: 1,
            pos: Position {
                start: 0,
                end: 1,
                block: BlockLoc::default(),
                block_size: 0,
                flags: 0,
                child_count: 0,
                version: 0,
            },
            values: [None; 9],
        };
        let mut cache = QueryGroupCache::default();
        for file in 1..100 {
            for level in 1..=super::super::MAX_LEVELS {
                let key = (file, level as u64, 1);
                cache
                    .group(level, key, || {
                        super::super::projected_batch::ProjectedGroup::from_records(
                            vec![record.clone(); super::super::PAGE_GROUP],
                        )
                    })
                    .unwrap();
                cache
                    .group(level, key, || panic!("cached group decoded twice"))
                    .unwrap();
                assert!(
                    cache
                        .groups
                        .iter()
                        .flatten()
                        .all(|(_, rows)| rows.capacity() <= super::super::PAGE_GROUP)
                );
            }
        }
        cache
            .group(1, (99, 1, 2), || {
                super::super::projected_batch::ProjectedGroup::from_records(vec![record.clone()])
            })
            .unwrap();
        assert_eq!(cache.groups[0].as_ref().unwrap().1.len(), 1);
        assert!(
            cache
                .group(0, (0, 0, 0), || {
                    super::super::projected_batch::ProjectedGroup::from_records(vec![
                        record.clone(),
                    ])
                })
                .is_err()
        );
        assert!(
            cache
                .group(1, (100, 0, 0), || {
                    super::super::projected_batch::ProjectedGroup::from_records(vec![])
                })
                .is_err()
        );
        assert!(
            cache
                .group(1, (100, 0, 0), || {
                    super::super::projected_batch::ProjectedGroup::from_records(vec![
                        record;
                        super::super::PAGE_GROUP
                            + 1
                    ])
                })
                .is_err()
        );
    }
    #[test]
    fn shared_cache_is_bounded_across_file_and_field_cardinality() {
        let cache = ReadCache::new(4096);
        for file in 1..=100 {
            for page in 0..100 {
                cache.insert((file, page, 256), &[file as u8; 256]);
                assert!(cache.stats().bytes <= 4096);
            }
        }
        assert!(cache.get((1, 0, 256)).is_none());
        assert_eq!(&*cache.get((100, 99, 256)).unwrap(), &[100; 256]);
        assert!(cache.stats().hits > 0 && cache.stats().misses > 0);
        assert!(cache.stats().entries < 10);
        cache.set_budget(0);
        assert_eq!(cache.stats().bytes, 0);
    }
}
