# database-framework-benchmark

Development benchmarks for measuring database-framework execution overhead
against a real PostgreSQL backend. This repository compares direct storage,
canonical persistence, framework data-store, and application-facing operation
paths so performance regressions can be attributed to a specific layer.

## Status

This is a development tool, not a production dependency. The current source
contains explicit `FIXME(INCOMPLETE_IMPLEMENTATION)` markers because its
layer-three probes have not yet been migrated to the latest public
`BenchmarkFramework` contract. A benchmark result is not valid until those
markers are resolved and the measured phase boundaries are revalidated.

## Repository Integration

The package intentionally uses adjacent local checkouts of
`database-framework`, `database-kit`, `storage-kit`, and `database-types`. This
keeps in-flight benchmark work aligned with framework changes. It is not a
release package and must not be used as evidence for URL-only release
verification of the libraries it measures.

## Runtime Configuration

The executable reads its PostgreSQL endpoint from environment variables.

| Variable | Purpose | Default |
|---|---|---|
| `POSTGRES_HOST` | PostgreSQL host | Required |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_USER` | PostgreSQL role | `postgres` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `test` |
| `POSTGRES_DB` | Disposable benchmark database | `benchmark_test` |

Use `--profile` to run phase profiling, `--compare` to run operation
comparisons, or omit both flags to request both groups after the incomplete
implementation markers have been resolved.

## License

Licensed under the [MIT License](LICENSE).
