# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

PostgreSQL benchmark comparing **direct storage operations** (StorageKit on PostgreSQL) vs **database record operations** (1amageek/database-framework stack). Measures CRUD operation overhead of the framework abstraction layer.

## Current Status (2026-08-20)

The source tracks the current adjacent database-framework checkout. L3
measurements open the package-scoped DataStore layer through the framework's
opt-in `DataStoreBenchmarkProbe` SPI. `BenchmarkFramework` is owned by this
benchmark package and is absent from database-framework products and tests.
Storage paths use `withTransaction`, database transactions use
`fetch(_:identifiedBy:)`, runtime composition carries a stable
`DatabaseExecutionRuntimeIdentity`, and `DBContainer` owns authoritative engine
shutdown. Entity and field authorization are deliberately excluded through the
framework's explicit testing SPI; never copy that security configuration into
a production application. Do not treat source compatibility or a successful
build as benchmark evidence; run the focused tests and the requested profile
against an isolated PostgreSQL 16 database.

## Build & Run

```bash
# Build
swift build

# Run benchmarks (requires running PostgreSQL)
POSTGRES_HOST=localhost swift run DatabaseBenchmark

# Validate the production PostgreSQL paths without collecting measurements
POSTGRES_HOST=localhost swift run DatabaseBenchmark --smoke

```

Use an isolated PostgreSQL 16 instance and a disposable database. Do not run
validation against a developer's long-running or default database.

### Environment Variables

| Variable | Required | Default |
|----------|----------|---------|
| `POSTGRES_HOST` | Yes | — |
| `POSTGRES_PORT` | No | 5432 |
| `POSTGRES_USER` | No | postgres |
| `POSTGRES_PASSWORD` | No | test |
| `POSTGRES_DB` | No | benchmark_test |

## Architecture

- **Entry.swift** — `@main` entry point. Orchestrates the PostgreSQL smoke validation and profile and comparison benchmark scenarios using `BenchmarkRunner` from `BenchmarkFramework`.
- **BenchmarkWallClock.swift** — Native host adapter that supplies canonical absolute timestamps through `DatabaseTypesFoundation`.
- **DirectStorageWorkload.swift** — Baseline (raw storage layer): direct key-value operations through `StorageTransactionExecutor` on the shared `StorageEngine`, without record encoding, identity resolution, or index maintenance.
- **DatabaseRecordWorkload.swift** — Comparison target (framework record layer): canonical record operations through the public record API — `DBContainer → DatabaseContext → StorageKit → PostgreSQLStorage`. Records are stored as canonical storage frames via `PersistableStorageCodec`.
- **ProfileBenchmark.swift** — Layered profile benchmarks (CPU phase breakdown; L1 direct storage mutation through L4 database record mutation).
- **BenchmarkLayerContract.swift** — Shared layer and transition names used by benchmark output.
- **FixedIterationReporter.swift** — Fixed-iteration measurement summaries printed to the console.
- **Models.swift** — `BenchmarkItem` model annotated with `@Persistable` macro from `DatabaseKit` (database-kit).
- **Config.swift** — PostgreSQL connection configuration from environment variables.

### Key Design Decisions

- Both paths use explicit transactions to ensure fair comparison
- Both workloads use the `StorageEngine` exclusively owned by one `DBContainer`, eliminating backend and connection-pool bias
- L3 access is available only through the benchmark-specific `DataStoreBenchmarkProbe`; production application access remains `DatabaseContext`
- Entity and field authorization are disabled only through the explicit testing SPI so security-policy cost is outside this benchmark's layer contract
- `BenchmarkRunner` from `BenchmarkFramework` handles warmup iterations, measurement, and reporting via `ConsoleReporter`

## Dependencies

- **database-framework** (local path, PostgreSQL trait) — provides `DatabaseEngine`, `DatabaseRuntime`, and the opt-in benchmark probe SPI
- **database-kit** (tagged URL dependency, `DatabaseKit`) — provides `@Persistable` macro and model infrastructure
- **database-types** (tagged URL dependency, `DatabaseTypes`, `DatabaseTypesFoundation`) — primitive values and native Foundation conversion
- **storage-kit** (tagged URL dependency, `StorageKit`, `StorageKitSystemClock`, `PostgreSQLStorage`) — storage engine abstraction for PostgreSQL
- **swift-log** (remote, from: 1.7.0) — logging
- Swift tools 6.4 / macOS 26+
