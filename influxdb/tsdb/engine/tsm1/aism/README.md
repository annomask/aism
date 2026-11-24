# Logical cross-file and cross-shard aggregates

Native TSM files and per-file companions retain their existing formats. The
shard and cross-shard catalogs use `AISMLOG4`: each series/field has ordered
aggregate records by level and time range. A parent stores its statistics and
child count, not filenames, offsets, part numbers, or child row addresses.
Descent searches the next level by the parent's time range. Leaves resolve
through the current native reader directory.

The logical content identity hashes ordered leaf statistics and block-content
digests. It excludes physical names, offsets and file partitioning. Native
compaction that copies the same blocks can therefore publish fresh reader
bindings while retaining the exact upper hierarchy and its persisted catalog.
Changed values refold the affected logical ancestors; changed block boundaries
rebuild the necessary hierarchy. The native TSM compaction policy is unchanged.

Recovery validates the logical identity, all child ranges and aggregate states,
and complete leaf coverage against current companions. Older `AISMTSM3`
catalogs are rejected and rebuilt as optional accelerators; native data and
per-file companions are not migrated. Failed catalog persistence does not affect
native durability. Cache, tombstone, membership and file-generation checks still
protect the captured native read view, including during physical relocation.

Cross-shard MEAN and multi-series floating SUM retain the native shard reducer
order. Per-series floating exactness alone does not prove that swapping shard
and series reductions preserves native results.

Focused Linux tests:

```sh
go test ./tsdb/engine/tsm1/aism
go test ./tsdb/engine/tsm1 -run 'TestAISM' -count=1
go test -race ./tsdb/engine/tsm1/aism ./tsdb/engine/tsm1 -run 'Test(AISM|Shard|Logical|Join|Catalog)' -count=1
```

The storage-engine tests require the same Flux library and `pkg-config` setup as
the normal InfluxDB build. Regression cases cover physical relocation without
catalog rewrites, current-address boundary reads, stale/corrupt catalogs,
compaction, reopen, cache/tombstone visibility and floating reduction order.
