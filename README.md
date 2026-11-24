# AISM-Tree artifact

This repository contains two implementations of AISM-Tree for historical
aggregation over time-series data.

| Directory | Implementation |
| --- | --- |
| [`engine/`](engine/README.md) | ReefsDB: Rust storage engine with VLog data and a hierarchical aggregate index |
| [`influxdb/`](influxdb/README.md) | InfluxDB TSM integration with per-file and logical cross-shard aggregates |

Project names, public APIs and persisted format identifiers are unchanged.
Both directories contain the current source code and regression tests, including
compaction, locator recovery, cross-shard queries, and reopen behavior. Database
contents, benchmark outputs, machine-specific configuration, and development
logs are not part of this source release.

## InfluxDB versions and experiments

The two InfluxDB versions correspond to different experiments:

| Experiment | InfluxDB version | Role |
| --- | --- | --- |
| Paper's three-system comparison: ReefsDB, IoTDB, and InfluxDB | OSS 2.9.1 | Native TSM baseline, without AISM |
| Separate experiment on integrating AISM into InfluxDB | OSS 2.7.12 | AISM implementation in this repository's `influxdb/` directory |

The 2.7.12-based AISM implementation is not the InfluxDB version used for the
paper's three-system comparison. Results from the two experiments are separate.

## Build and test

Use Linux for the complete storage-engine test suites. See each implementation's
README for requirements and commands. The Rust crate keeps its name `reefsdb`
and exports `ReefsDb`. The Go integration keeps the upstream
`github.com/influxdata/influxdb/v2` module path for dependency compatibility.

## Licenses

The Rust package declares Apache-2.0; see [`engine/LICENSE`](engine/LICENSE).
The InfluxDB-derived implementation uses the MIT license; see
[`influxdb/LICENSE`](influxdb/LICENSE). Third-party copyright notices and
attribution are retained in the relevant source files. These acknowledgements
identify upstream components, not the authors of this artifact.
