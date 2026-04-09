import Foundation
import DatabaseEngine
import StorageKit
import PostgreSQLStorage
import Core

/// DatabaseFramework operations for comparison benchmarks.
///
/// Uses the full framework stack: DBContainer → FDBContext → StorageKit → PostgreSQLStorage.
/// Data is stored as Protobuf-encoded key-value pairs in PostgreSQL's kv_store table.
enum FrameworkPostgreSQL {

    // MARK: - Setup

    static func makeContainer(config: BenchmarkConfig) async throws -> DBContainer {
        let engine = try await PostgreSQLStorageEngine(configuration: config.storageConfig)
        let schema = Schema([BenchmarkItem.self], version: .init(1, 0, 0))
        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
    }

    /// Clear all framework data from the KV store.
    static func cleanup(container: DBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: BenchmarkItem.self)
        let (begin, end) = subspace.range()
        try await container.engine.withTransaction { tx in
            tx.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - CRUD Operations

    static func insertOne(container: DBContainer, item: BenchmarkItem) async throws {
        let context = FDBContext(container: container)
        context.insert(item)
        try await context.save()
    }

    static func readOne(container: DBContainer, id: String) async throws -> BenchmarkItem? {
        let context = FDBContext(container: container)
        return try await context.model(for: id, as: BenchmarkItem.self)
    }

    static func updateOne(container: DBContainer, item: BenchmarkItem) async throws {
        // FDBContext uses upsert semantics: insert() overwrites existing data
        let context = FDBContext(container: container)
        context.insert(item)
        try await context.save()
    }

    static func deleteOne(container: DBContainer, id: String) async throws {
        let context = FDBContext(container: container)
        var item = BenchmarkItem()
        item.id = id
        context.delete(item)
        try await context.save()
    }

    // MARK: - Batch Operations

    static func batchInsert(container: DBContainer, items: [BenchmarkItem]) async throws {
        let context = FDBContext(container: container)
        for item in items {
            context.insert(item)
        }
        try await context.save()
    }

    // MARK: - Seed Data

    /// Populate the KV store with N records for read/update/delete benchmarks.
    /// Returns the IDs of the inserted records.
    @discardableResult
    static func seedData(
        container: DBContainer,
        count: Int,
        idPrefix: String = "seed"
    ) async throws -> [String] {
        var ids: [String] = []
        let batchSize = 100
        for batchStart in stride(from: 0, to: count, by: batchSize) {
            let end = min(batchStart + batchSize, count)
            let context = FDBContext(container: container)
            for i in batchStart..<end {
                let id = "\(idPrefix)-\(String(format: "%06d", i))"
                ids.append(id)
                var item = BenchmarkItem()
                item.id = id
                item.name = "User \(i)"
                item.age = 20 + (i % 60)
                item.score = Double(50 + (i % 50))
                context.insert(item)
            }
            try await context.save()
        }
        return ids
    }
}
