# database-framework-benchmark

Development benchmarks for measuring database-framework execution overhead
against a real PostgreSQL backend. This repository compares direct storage,
canonical persistence, framework data-store, and application-facing operation
paths so performance regressions can be attributed to a specific layer.

## Status

This is a development tool, not a production dependency. Its layer-three
measurements use the opt-in `DataStoreBenchmarkProbe` SPI from
`DatabaseEngine`; application code continues to use `DatabaseContext` and
cannot open package-scoped stores directly. `BenchmarkFramework` is owned by
this package and does not enter the database-framework product or test graph.

The measured write path is kept explicit:

| Layer | Contract |
|---|---|
| L1 | Direct storage mutation |
| L2 | Canonical record storage |
| L3 | DataStore batch mutation or record read |
| L4 | Application-facing database record operation |

A result is meaningful only after the source revision has passed its focused
tests and the requested profile has run against an isolated PostgreSQL 16
database. Source compatibility alone is not performance evidence.

Entity and field authorization are intentionally disabled through the
framework's explicit testing SPI so these profiles isolate persistence and
execution-layer overhead. This configuration must not be copied into a
production application.

## Repository Integration

The package intentionally uses the adjacent `database-framework` checkout so
it can validate an in-flight benchmark probe before that framework is
released. `database-kit`, `storage-kit`, and `database-types` use their tagged
URL dependencies, which keeps one canonical SwiftPM identity for each package.
This repository is an integration and performance tool, not a library release.

## Runtime Configuration

The executable reads its PostgreSQL endpoint from environment variables.

| Variable | Purpose | Default |
|---|---|---|
| `POSTGRES_HOST` | PostgreSQL host | Required |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_USER` | PostgreSQL role | `postgres` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `test` |
| `POSTGRES_DB` | Disposable benchmark database | `benchmark_test` |

Use `--smoke` for a short PostgreSQL CRUD and lifecycle validation without
collecting performance evidence. Use `--profile` to run phase profiling,
`--compare` to run operation comparisons, or omit all flags to run both
measurement groups.

## License

Licensed under the [MIT License](LICENSE).
