# AISM-Tree in InfluxDB

This directory contains an InfluxDB OSS 2.7.12 source snapshot with AISM-Tree
integration in the TSM storage engine. The upstream Go module path and MIT
copyright notices are preserved.

This implementation belongs to the separate experiment on integrating AISM into
InfluxDB. The paper's three-system comparison uses native InfluxDB OSS 2.9.1
without AISM; its InfluxDB measurements do not come from this 2.7.12-based
implementation.

## Implementation

- `tsdb/engine/tsm1/aism/`: aggregate states, per-file trees, paged persistence,
  logical cross-file and cross-shard catalogs, and trace counters.
- `tsdb/engine/tsm1/aism_*.go`: query execution, captured native views, sidecar
  creation, visibility checks, and lifecycle integration.
- `tsdb/shard.go`: cross-shard aggregation integration.
- `cmd/aism-index/`: building companions for an offline database snapshot.

Logical parents contain aggregate values, time ranges, and child counts rather
than physical child addresses. Boundary reads resolve through current native
readers. See [the aggregate-format documentation](tsdb/engine/tsm1/aism/README.md).

## Build and test

Requirements: Linux, Go as specified in `go.mod`, a C/C++ toolchain, `pkg-config`,
and the Flux library matching the version pinned in `go.mod`. See [CONTRIBUTING.md](CONTRIBUTING.md) for the upstream build workflow.

```sh
# Standalone aggregate library.
go test ./tsdb/engine/tsm1/aism -count=1

# Storage-engine integration (requires the Flux C library).
./env go test -p 2 ./tsdb/engine/tsm1/... -count=1
./env go test -p 2 ./http/legacy ./tsdb -run AISM -count=1

# Server and offline companion builder; binaries stay outside tracked source.
./env go build -trimpath -o bin/influxd ./cmd/influxd
./env go build -trimpath -o bin/aism-index ./cmd/aism-index
```

`INFLUXDB_AISM_MODE` selects `raw`, `leaf`, `file`, `full`, or `cross`.
An unset mode keeps the native path. `INFLUXDB_AISM_BUILD=1` enables companion
creation independently of query mode. Use `aism-index` only on an offline
snapshot, never concurrently with a writer.

Per-request experiment tracing requires `INFLUXDB_AISM_EXPERIMENT=1` and the
`X-Aism-Trace: 1` header; `X-Aism-Mode` overrides the mode for that traced request.
The trace reports fallback reasons as well as aggregate and decoding counts.
