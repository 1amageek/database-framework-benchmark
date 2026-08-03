# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

PostgreSQL benchmark comparing **direct storage operations** (StorageKit on PostgreSQL) vs **database record operations** (1amageek/database-framework stack). Measures CRUD operation overhead of the framework abstraction layer.

## Current Status (2026-08-03)

**The DatabaseBenchmark target does not compile** against database-framework
`26.0803.x`: the L3 (DataStore-layer) measurements reach `container.store(for:)`,
which the framework made package-scoped, plus the removed `withAutoCommit` and
the renamed `fetchByIDInTransaction`. `FIXME(INCOMPLETE_IMPLEMENTATION)` markers
in `Entry.swift` and `ProfileBenchmark.swift` record the completion conditions:
restore L3 through a public probe exported by the framework's
`BenchmarkFramework` product, port to the current transaction API, and
re-validate the per-transition parity targets. L1/L2/L4 measurement code and
the mechanical API drift already repaired (DBConfiguration `storageEngine:`,
async `engine.shutdown()`) are otherwise current.

## Build & Run

```bash
# Build
swift build

# Run benchmarks (requires running PostgreSQL)
POSTGRES_HOST=localhost swift run DatabaseBenchmark

# Start PostgreSQL via Docker
docker run --rm -d -p 5432:5432 \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=benchmark_test \
  postgres:16
```

### Environment Variables

| Variable | Required | Default |
|----------|----------|---------|
| `POSTGRES_HOST` | Yes | — |
| `POSTGRES_PORT` | No | 5432 |
| `POSTGRES_USER` | No | postgres |
| `POSTGRES_PASSWORD` | No | test |
| `POSTGRES_DB` | No | benchmark_test |

## Architecture

- **Entry.swift** — `@main` entry point. Orchestrates profile and comparison benchmark scenarios using `BenchmarkRunner` from `BenchmarkFramework`.
- **DirectStorageWorkload.swift** — Baseline (raw storage layer): direct key-value operations through `StorageTransactionExecutor` on the shared `StorageEngine`, without record encoding, identity resolution, or index maintenance.
- **DatabaseRecordWorkload.swift** — Comparison target (framework record layer): canonical record operations through the public record API — `DBContainer → DatabaseContext → StorageKit → PostgreSQLStorage`. Records are stored as canonical storage frames via `PersistableStorageCodec`.
- **ProfileBenchmark.swift** — Layered profile benchmarks (CPU phase breakdown; L1 direct storage mutation through L4 database record mutation).
- **BenchmarkLayerContract.swift** — Shared layer and transition names used by benchmark output.
- **FixedIterationReporter.swift** — Fixed-iteration measurement summaries printed to the console.
- **Models.swift** — `BenchmarkItem` model annotated with `@Persistable` macro from `DatabaseKit` (database-kit).
- **Config.swift** — PostgreSQL connection configuration from environment variables.

### Key Design Decisions

- Both paths use explicit transactions to ensure fair comparison
- Both workloads share the same `StorageEngine` and persistent store, eliminating backend and connection-pool bias
- `BenchmarkRunner` from `BenchmarkFramework` handles warmup iterations, measurement, and reporting via `ConsoleReporter`

## Dependencies

- **database-framework** (local path, PostgreSQL trait) — provides `DatabaseEngine`, `DatabaseRuntime`, `DatabaseServerFoundation`, `ScalarIndex`, `BenchmarkFramework`
- **database-kit** (local path, `DatabaseKit`) — provides `@Persistable` macro and model infrastructure
- **database-types** (local path, `DatabaseTypes`) — shared primitive types
- **storage-kit** (local path, `StorageKit`, `StorageKitSystemClock`, `PostgreSQLStorage`) — storage engine abstraction for PostgreSQL
- **postgres-nio** (remote, from: 1.25.0) — PostgreSQL driver
- **swift-log** (remote, from: 1.7.0) — logging
- Swift tools 6.4 / macOS 26+
