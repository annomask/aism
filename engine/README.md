# ReefsDB with AISM-Tree

ReefsDB provides time-series storage, VLog blocks, series indexing, snapshot
and compaction, and aggregate queries. SQL parsing and server routing are
supplied by the caller. Package names, public APIs, configuration keys and
on-disk format identifiers retain their original values.

## Rust API

The `reefsdb` crate is the supported public API. Storage, index, model, series, query, runtime,
and utility implementations are private modules behind its curated root exports.

To use the local crate:

```toml
[dependencies]
reefsdb = { path = "../engine" }
```

```rust
use reefsdb::{ReefsDb, StoragePredicate, StorageScan};
```

## Tests and benchmarks
Running some unit tests requires specifying features.
1. Run tests without features
```bash
cargo test --workspace
```
2. Run tests with "mock_file_store"
```bash
cargo test -p reefsdb --features mock_file_store
```
3. Run tests with "mock_compact_plan"
```bash
cargo test -p reefsdb --features mock_compact_plan
```
Running some bencharks requires specifying "benchmarking" features.
```bash
cargo bench -p reefsdb --bench bench_tsm1_float --features benchmarking
```

## Aggregate index

For a new VLog shard, set `EngineOptions::vlog_enabled = true` and
`EngineOptions::agg_index_enabled = true`. The latter defaults to `false`.
This mode persists independent `(series, field)` aggregate trees in `.idx`
files alongside VLog data blocks. Aggregate values and hierarchy-file references
extend the original VLog locator records. The original locator cache and locator
WAL publish, invalidate and recover those records; native AISM mode has no second
aggregate commit log. Flush builds its own tree, and native VLog compaction keeps
its original block-copy, decode, merge and retirement rules. Copied blocks retain
their summaries; blocks already decoded by that compactor receive summaries of
its output values. Aggregate queries use the original VLog tombstones as well.
An entirely deleted compaction result retires its inputs without installing an
empty VLog; this empty-output fix is shared by native and AISM modes.
Native locator format 5 requires a new database or an explicit migration from
older aggregate-owned stores; it never silently converts an existing database.
Query conflict checks use ordered root intervals. Small overlapping
trees can refine gaps from leaf positions, bounded to 512 leaves per source and
4096 leaves per operation; larger trees retain conservative raw fallback.
Narrow queries skip this optional full-source prepass.
Hierarchy-only compaction updates references through the existing locator
replacement operation. Data overlap and deletion remain the responsibility of
the native VLog compactor. Deleted series leave the visible TSI membership, while
their persistent IDs and field ordinals remain reserved for reinsertion/recovery.

`ReefsDb::aggregate_projected_scan` accepts the same `AggregateRequest` and
returns only requested components as options in `ProjectedAggregateState`.
AVG includes count and sum; unrequested components are `None`. For empty input,
requested count is `Some(0)` and other result components are `None`. The result's
`projection` distinguishes empty from unrequested components. Both aggregate
state types expose `average()`, returning `Result<Option<f64>, &'static str>`.
Integer AVG uses `wide_sum` (`AggregateWideSum::Integer(i128)` or
`Unsigned(u128)`) to avoid overflow; the existing 64-bit SUM retains wrapping
semantics. Use `average()` instead of dividing that SUM by COUNT. Requested
integer sums initialize `wide_sum` to zero even for empty input. Consumers that
construct state literals must supply this field. Ambiguous legacy summaries
fall back to raw values; persisted formats are unchanged.
Result fields remain public for compatibility. `average()` rejects inconsistent
numeric types or wide/wrapping sums in manually constructed states; projected
results also validate their requested components at the completed-query boundary.

Floating SUM/AVG use IEEE-754 arithmetic. For each captured `(series, field)`
view, mixed positive/negative bounds or non-finite root summaries disable summary
reduction and use chronological raw values, avoiding block-subtotal overflow and
cancellation artifacts. Same-sign finite data retain summary acceleration.
Ordinary rounding can still vary with block or cross-series reduction order;
these APIs do not promise bitwise agreement with a global sequential float sum.

`aggregate_projected_scan_filtered` adds an exact tag/measurement predicate via
the existing TSI series selection. It rejects residual predicates that require
row evaluation on selected series; callers use a storage scan for those cases.
Unavailable aggregate indexes or fields return `None`. Time ranges remain
inclusive, and callers clip SQL buckets and convert exclusive upper endpoints.

`aggregate_projected_scan_windows_filtered` accepts multiple field requests and
ordered, nonoverlapping inclusive `TimeRange` windows. Requests must share a
measurement and the windows' envelope. Series selection runs once; each field's
trees are traversed once and each cross-window leaf is decoded once. Results
align with field requests, and their states align with windows; absent fields
return `None`. Windows may have gaps. Callers still define SQL bucket alignment
and convert half-open bounds; the engine does not hardcode time units. Cache
capture precedes each `(series, field)` file view as before; this is not a
transactional snapshot across series or fields. Residual row predicates require
a storage scan.

`AggregateShardGroup` adds cross-shard aggregation for a fixed list of
`AggregateShard { database, time_range }` members. The caller supplies disjoint
inclusive shard ranges and a shared `SeriesFile`; SQL routing remains in the
upper runtime. Its `aggregate_projected_scan_windows_filtered` method takes one
field request, ordered windows, and the same exact series predicate as the
local API. It merges COUNT/SUM states for AVG, and preserves FIRST/LAST timestamps.

The group combines clean native block summaries into a logical hierarchy keyed
by series, field, level, and time range. Parent records contain aggregate values
and child counts, without child addresses or file identities. The next level is
found by time range; boundary reads use each shard's current native locators.
Copying blocks to new files therefore reuses unchanged logical records. Writes,
deletes, and changed block boundaries invalidate the affected cached content.
The original VLog compaction, locator update, tombstone and WAL recovery paths
are unchanged. The group index is derived in memory from those records after
reopen, with an 8 MiB cache and at most 16,384 leaves per series/field build.
Larger or conflicting views use the native local aggregate path. Floating
SUM/AVG also retain the native per-shard reduction order.

Group queries hold operation leases for all members and validate their write/
delete revisions around capture and execution. Two unsuccessful captures return
a retryable query error instead of using stale summaries. A short mutex protects
only cache publication; index IO, block reads and aggregation run outside it.
`CrossShardAggregateResult` reports native scan counters separately from logical
leaf metadata reads, cross-shard summary uses, cache reuse and local fallbacks.
