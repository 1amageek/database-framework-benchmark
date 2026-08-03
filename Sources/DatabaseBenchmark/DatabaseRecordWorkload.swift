import Foundation
import DatabaseEngine
import DatabaseRuntime
import DatabaseServerFoundation
import StorageKit
import StorageKitSystemClock
import PostgreSQLStorage
import DatabaseKit

/// Canonical database record operations used by comparison benchmarks.
///
/// The workload exercises the same public record API used by applications,
/// including record encoding, identity resolution, and index maintenance.
enum DatabaseRecordWorkload {

    // MARK: - Setup

    static func makeContainer(config: BenchmarkConfig) async throws -> DBContainer {
        let engine = try await PostgreSQLStorageEngine(configuration: config.storageConfig)
        let schema = try Schema(
            entities: [try BenchmarkItem.schemaEntity],
            version: .init(1, 0, 0)
        )
        return try await DBContainer.open(
            for: schema,
            configuration: .init(
                storageEngine: engine,
                monotonicClock: SystemStorageClock(),
                wallClock: RealtimeDatabaseWallClock()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(BenchmarkItem.self),
                ]
            ),
            security: .disabled
        )
    }

    /// Clear all benchmark records from the resolved item subspace.
    static func cleanup(container: DBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: BenchmarkItem.self)
        let (begin, end) = subspace.range()
        try await StorageTransactionExecutor(engine: container.engine).withTransaction { tx in
            try tx.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - CRUD Operations

    static func insertOne(container: DBContainer, item: BenchmarkItem) async throws {
        let context = container.newContext()
        try context.insert(item)
        try await context.save()
    }

    static func readOne(container: DBContainer, id: String) async throws -> BenchmarkItem? {
        let context = container.newContext()
        return try await context.withTransaction { transaction in
            try await transaction.fetch(BenchmarkItem.self, identifiedBy: id)
        }
    }

    static func updateOne(container: DBContainer, item: BenchmarkItem) async throws {
        // Record insertion has defined upsert semantics for an existing identity.
        let context = container.newContext()
        try context.upsert(item)
        try await context.save()
    }

    static func deleteOne(container: DBContainer, id: String) async throws {
        let context = container.newContext()
        var item = BenchmarkItem()
        item.id = id
        try context.delete(item)
        try await context.save()
    }

    // MARK: - Batch Operations

    static func batchInsert(container: DBContainer, items: [BenchmarkItem]) async throws {
        let context = container.newContext()
        for item in items {
            try context.insert(item)
        }
        try await context.save()
    }

    // MARK: - Seed Data

    /// Populate the record store for read, update, and delete benchmarks.
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
            let context = container.newContext()
            for i in batchStart..<end {
                let id = "\(idPrefix)-\(String(format: "%06d", i))"
                ids.append(id)
                var item = BenchmarkItem()
                item.id = id
                item.name = "User \(i)"
                item.age = Int64(20 + (i % 60))
                item.score = Double(50 + (i % 50))
                try context.insert(item)
            }
            try await context.save()
        }
        return ids
    }
}
