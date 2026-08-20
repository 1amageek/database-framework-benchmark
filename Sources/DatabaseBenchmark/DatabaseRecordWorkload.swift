import Foundation
@_spi(Testing) import DatabaseEngine
import DatabaseRuntime
import StorageKit
import StorageKitSystemClock
import PostgreSQLStorage
import DatabaseKit

/// Canonical database record operations used by comparison benchmarks.
///
/// The workload exercises the same public record API used by applications,
/// including record encoding and identity resolution.
enum DatabaseRecordWorkload {
    private static let executionIdentity = DatabaseExecutionRuntimeIdentity(
        identifier: "database-framework-benchmark",
        revision: 1
    )

    // MARK: - Setup

    static func makeContainer(config: BenchmarkConfig) async throws -> DBContainer {
        let engine = try await PostgreSQLStorageEngine(configuration: config.storageConfig)
        return try await makeContainer(engine: engine)
    }

    /// Transfers storage-engine ownership into a benchmark container.
    /// The engine is shut down before a setup failure is returned.
    static func makeContainer(engine: any StorageEngine) async throws -> DBContainer {
        do {
            let schema = try makeSchema()
            let runtimeConfiguration = try DatabaseFrameworkRuntime.configuration(
                executionIdentity: executionIdentity,
                schema: schema
            )
            let configuration = DBConfiguration(
                storageEngine: engine,
                monotonicClock: SystemStorageClock(),
                wallClock: BenchmarkWallClock()
            )
            return try await DBContainer.open(
                for: schema,
                configuration: configuration,
                runtimeConfiguration: runtimeConfiguration,
                security: .disabledForTesting
            )
        } catch {
            await engine.shutdown()
            throw error
        }
    }

    private static func makeSchema() throws -> Schema {
        try Schema(
            entities: [try BenchmarkItem.schemaEntity],
            version: .init(1, 0, 0)
        )
    }

    static func makeContext(in container: DBContainer) -> DatabaseContext {
        container.newContext(authorization: .anonymous)
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
        let context = makeContext(in: container)
        try context.insert(item)
        try await context.save()
    }

    static func readOne(container: DBContainer, id: String) async throws -> BenchmarkItem? {
        let context = makeContext(in: container)
        return try await context.withTransaction { transaction in
            try await transaction.fetch(BenchmarkItem.self, identifiedBy: id)
        }
    }

    static func updateOne(container: DBContainer, item: BenchmarkItem) async throws {
        // The update benchmark intentionally measures explicit upsert semantics.
        let context = makeContext(in: container)
        try context.upsert(item)
        try await context.save()
    }

    static func deleteOne(container: DBContainer, id: String) async throws {
        let context = makeContext(in: container)
        var item = BenchmarkItem()
        item.id = id
        try context.delete(item)
        try await context.save()
    }

    // MARK: - Batch Operations

    static func batchInsert(container: DBContainer, items: [BenchmarkItem]) async throws {
        let context = makeContext(in: container)
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
            let context = makeContext(in: container)
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
