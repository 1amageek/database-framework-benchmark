# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

PostgreSQL benchmark comparing **Raw PostgreSQL** (direct PostgresNIO queries) vs **DatabaseFramework** (1amageek/database-framework stack). Measures CRUD operation overhead of the framework abstraction layer.

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

- **Entry.swift** — `@main` entry point. Orchestrates all benchmark scenarios (single insert, batch insert, point read, update, delete) using `BenchmarkRunner` from `BenchmarkFramework`.
- **RawPostgreSQL.swift** — Baseline: direct PostgresNIO queries with explicit transactions (`client.withTransaction`). Uses a relational `benchmark_items` table with native SQL types.
- **FrameworkPostgreSQL.swift** — Comparison target: full framework stack `DBContainer → FDBContext → StorageKit → PostgreSQLStorage`. Data is stored as Protobuf-encoded KV pairs in PostgreSQL.
- **Models.swift** — `BenchmarkItem` model annotated with `@Persistable` macro from `Core` (database-kit).
- **Config.swift** — PostgreSQL connection configuration from environment variables.

### Key Design Decisions

- Both paths use explicit transactions to ensure fair comparison
- Raw SQL uses individual INSERTs in a single transaction (not bulk INSERT) to match framework's per-item KV puts
- `BenchmarkRunner` from `BenchmarkFramework` handles warmup iterations, measurement, and reporting via `ConsoleReporter`

## Dependencies

- **database-framework** (PostgreSQL trait only) — provides `DatabaseEngine`, `ScalarIndex`, `BenchmarkFramework`
- **database-kit** (`Core`) — provides `@Persistable` macro and model infrastructure
- **storage-kit** (`StorageKit`, `PostgreSQLStorage`) — storage engine abstraction for PostgreSQL
- **postgres-nio** — direct PostgreSQL driver for raw benchmarks
- Swift 6.2 / macOS 26+
