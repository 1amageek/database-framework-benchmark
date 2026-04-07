import Foundation
import PostgresNIO

/// Direct PostgresNIO operations for baseline benchmarks.
///
/// Uses a relational table with native PostgreSQL types and indexes,
/// representing the best-case performance without any framework overhead.
enum RawPostgreSQL {

    // MARK: - Table Management

    static func createTable(client: PostgresClient) async throws {
        try await client.query("""
            CREATE TABLE IF NOT EXISTS benchmark_items (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                age INT NOT NULL,
                score DOUBLE PRECISION NOT NULL
            )
            """)
    }

    static func dropTable(client: PostgresClient) async throws {
        try await client.query("DROP TABLE IF EXISTS benchmark_items")
    }

    static func truncate(client: PostgresClient) async throws {
        try await client.query("TRUNCATE TABLE benchmark_items")
    }

    // MARK: - CRUD Operations

    static func insertOne(
        client: PostgresClient,
        id: String,
        name: String,
        age: Int,
        score: Double
    ) async throws {
        try await client.query("""
            INSERT INTO benchmark_items (id, name, age, score)
            VALUES (\(id), \(name), \(age), \(score))
            """)
    }

    static func readOne(
        client: PostgresClient,
        id: String
    ) async throws -> (String, String, Int, Double)? {
        let rows = try await client.query(
            "SELECT id, name, age, score FROM benchmark_items WHERE id = \(id)"
        )
        for try await (id, name, age, score) in rows.decode((String, String, Int, Double).self) {
            return (id, name, age, score)
        }
        return nil
    }

    static func updateOne(
        client: PostgresClient,
        id: String,
        name: String,
        age: Int,
        score: Double
    ) async throws {
        try await client.query("""
            UPDATE benchmark_items
            SET name = \(name), age = \(age), score = \(score)
            WHERE id = \(id)
            """)
    }

    static func deleteOne(
        client: PostgresClient,
        id: String
    ) async throws {
        try await client.query("DELETE FROM benchmark_items WHERE id = \(id)")
    }

    // MARK: - Batch Operations

    /// Insert multiple items in a single multi-row VALUES statement.
    ///
    /// NOTE: Uses `unsafeSQL` for dynamic query construction.
    /// This is acceptable for benchmarks with controlled data but should
    /// never be used in production code.
    static func batchInsert(
        client: PostgresClient,
        items: [(id: String, name: String, age: Int, score: Double)]
    ) async throws {
        guard !items.isEmpty else { return }
        var values: [String] = []
        for item in items {
            let escaped = item.name.replacingOccurrences(of: "'", with: "''")
            values.append("('\(item.id)', '\(escaped)', \(item.age), \(item.score))")
        }
        let sql = "INSERT INTO benchmark_items (id, name, age, score) VALUES " + values.joined(separator: ", ")
        try await client.query(PostgresQuery(unsafeSQL: sql))
    }

    // MARK: - Seed Data

    /// Populate the table with N records for read/update/delete benchmarks.
    /// Returns the IDs of the inserted records.
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
