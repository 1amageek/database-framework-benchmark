import Foundation
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
/// Validation and measurements must use an isolated PostgreSQL 16 database.
struct BenchmarkConfig: Sendable {
    let host: String
    let port: Int
    let username: String
    let password: String
    let database: String

    static func fromEnvironment() throws -> BenchmarkConfig {
        let environment = ProcessInfo.processInfo.environment
        guard let host = environment["POSTGRES_HOST"], !host.isEmpty else {
            throw BenchmarkError.missingEnvironment("POSTGRES_HOST is required. Example: export POSTGRES_HOST=localhost")
        }
        let port: Int
        if let rawPort = environment["POSTGRES_PORT"] {
            guard let parsedPort = Int(rawPort), (1...65_535).contains(parsedPort) else {
                throw BenchmarkError.invalidEnvironment(
                    name: "POSTGRES_PORT",
                    value: rawPort
                )
            }
            port = parsedPort
        } else {
            port = 5432
        }
        let username = environment["POSTGRES_USER"] ?? "postgres"
        let password = environment["POSTGRES_PASSWORD"] ?? "test"
        let database = environment["POSTGRES_DB"] ?? "benchmark_test"

        return BenchmarkConfig(
            host: host,
            port: port,
            username: username,
            password: password,
            database: database
        )
    }

    /// Configuration for the shared benchmark storage engine.
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
    case invalidEnvironment(name: String, value: String)

    var description: String {
        switch self {
        case .missingEnvironment(let message):
            return message
        case .invalidEnvironment(let name, let value):
            return "\(name) has an invalid value: \(value)"
        }
    }
}
