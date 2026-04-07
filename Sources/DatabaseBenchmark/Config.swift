import Foundation
import PostgresNIO
import PostgreSQLStorage

/// PostgreSQL connection configuration for benchmarks.
///
/// Reads from environment variables:
/// - `POSTGRES_HOST` (required)
/// - `POSTGRES_PORT` (optional, default: 5432)
/// - `POSTGRES_USER` (optional, default: "postgres")
/// - `POSTGRES_PASSWORD` (optional, default: "test")
/// - `POSTGRES_DB` (optional, default: "benchmark_test")
///
/// Quick start with Docker:
/// ```
/// docker run --rm -d -p 5432:5432 \
///   -e POSTGRES_PASSWORD=test \
///   -e POSTGRES_DB=benchmark_test \
///   postgres:16
/// ```
struct BenchmarkConfig: Sendable {
    let host: String
    let port: Int
    let username: String
    let password: String
    let database: String

    static func fromEnvironment() throws -> BenchmarkConfig {
        guard let host = ProcessInfo.processInfo.environment["POSTGRES_HOST"] else {
            throw BenchmarkError.missingEnvironment("POSTGRES_HOST is required. Example: export POSTGRES_HOST=localhost")
        }
        let port = Int(ProcessInfo.processInfo.environment["POSTGRES_PORT"] ?? "5432") ?? 5432
        let username = ProcessInfo.processInfo.environment["POSTGRES_USER"] ?? "postgres"
        let password = ProcessInfo.processInfo.environment["POSTGRES_PASSWORD"] ?? "test"
        let database = ProcessInfo.processInfo.environment["POSTGRES_DB"] ?? "benchmark_test"

        return BenchmarkConfig(
            host: host,
            port: port,
            username: username,
            password: password,
            database: database
        )
    }

    /// PostgresNIO client configuration for raw benchmarks
    var postgresClientConfig: PostgresClient.Configuration {
        PostgresClient.Configuration(
            host: host,
            port: port,
            username: username,
            password: password,
            database: database,
            tls: .disable
        )
    }

    /// PostgreSQLStorage configuration for framework benchmarks
    var storageConfig: PostgreSQLConfiguration {
        PostgreSQLConfiguration(
            host: host,
            port: port,
            username: username,
            password: password,
            database: database
        )
    }
}

enum BenchmarkError: Error, CustomStringConvertible {
    case missingEnvironment(String)

    var description: String {
        switch self {
        case .missingEnvironment(let message):
            return message
        }
    }
}
