import Foundation
import PostgresNIO
import Logging

private let logger = Logger(label: "benchmark.raw-postgresql")

/// Direct PostgresNIO operations for baseline benchmarks.
///
/// All operations use explicit transactions (`client.withTransaction`)
/// to match the framework's transaction model for fair comparison.
/// Raw PostgreSQL uses a relational table with native types.
enum RawPostgreSQL {

    // MARK: - Table Management (DDL, no transaction needed)

    static func createTable(client: PostgresClient) async throws {
        try await client.query("""
            CREATE TABLE IF NOT EXISTS benchmark_items (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                age INT NOT NULL,
                score DOUBLE PRECISION NOT NULL
            )
            """,
            logger: logger
        )
    }

    static func dropTable(client: PostgresClient) async throws {
        try await client.query("DROP TABLE IF EXISTS benchmark_items", logger: logger)
    }

    static func truncate(client: PostgresClient) async throws {
        try await client.query("TRUNCATE TABLE benchmark_items", logger: logger)
    }

    // MARK: - CRUD Operations (all use explicit transactions)

    static func insertOne(
        client: PostgresClient,
        id: String,
        name: String,
        age: Int,
        score: Double
    ) async throws {
        try await client.withTransaction(logger: logger) { connection in
            try await connection.query("""
                INSERT INTO benchmark_items (id, name, age, score)
                VALUES (\(id), \(name), \(age), \(score))
                """,
                logger: logger
            )
        }
    }

    static func readOne(
        client: PostgresClient,
        id: String
    ) async throws -> (String, String, Int, Double)? {
        try await client.withTransaction(logger: logger) { connection in
            let rows = try await connection.query(
                "SELECT id, name, age, score FROM benchmark_items WHERE id = \(id)",
                logger: logger
            )
            for try await (id, name, age, score) in rows.decode((String, String, Int, Double).self) {
                return (id, name, age, score)
            }
            return nil
        }
    }

    static func updateOne(
        client: PostgresClient,
        id: String,
        name: String,
        age: Int,
        score: Double
    ) async throws {
        try await client.withTransaction(logger: logger) { connection in
            try await connection.query("""
                UPDATE benchmark_items
                SET name = \(name), age = \(age), score = \(score)
                WHERE id = \(id)
                """,
                logger: logger
            )
        }
    }

    static func deleteOne(
        client: PostgresClient,
        id: String
    ) async throws {
        try await client.withTransaction(logger: logger) { connection in
            try await connection.query(
                "DELETE FROM benchmark_items WHERE id = \(id)",
                logger: logger
            )
        }
    }

    // MARK: - Batch Operations

    /// Insert multiple items using individual INSERTs within a single transaction.
    ///
    /// This matches the framework's behavior of individual KV puts within
    /// one transaction, ensuring a fair comparison of per-item overhead.
    static func batchInsert(
        client: PostgresClient,
        items: [(id: String, name: String, age: Int, score: Double)]
    ) async throws {
        guard !items.isEmpty else { return }
        try await client.withTransaction(logger: logger) { connection in
            for item in items {
                try await connection.query("""
                    INSERT INTO benchmark_items (id, name, age, score)
                    VALUES (\(item.id), \(item.name), \(item.age), \(item.score))
                    """,
                    logger: logger
                )
            }
        }
    }

    // MARK: - Seed Data (setup, not measured)

    /// Populate the table with N records for read/update/delete benchmarks.
    @discardableResult
    static func seedData(
        client: PostgresClient,
        count: Int
    ) async throws -> [String] {
        var ids: [String] = []
        let batchSize = 100
        for batchStart in stride(from: 0, to: count, by: batchSize) {
            let end = min(batchStart + batchSize, count)
            var items: [(id: String, name: String, age: Int, score: Double)] = []
            for i in batchStart..<end {
                let id = "seed-\(String(format: "%06d", i))"
                ids.append(id)
                items.append((id: id, name: "User \(i)", age: 20 + (i % 60), score: Double(50 + (i % 50))))
            }
            try await batchInsert(client: client, items: items)
        }
        return ids
    }
}
