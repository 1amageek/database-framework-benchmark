import Foundation
import StorageKit
import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Direct storage operations used as the record workload baseline.
///
/// Shares the same `StorageEngine` and persistent store as the database record
/// workload, eliminating backend and connection-pool bias.
///
/// This baseline deliberately measures the storage transaction contract without
/// record encoding, identity resolution, or index maintenance.
enum DirectStorageWorkload {

    /// Representative 70-byte payload matching the canonical record size.
    private static let representativeItemValue = ByteString(repeating: 0x42, count: 70)
    private static let itemKeyPrefix = ByteString(
        utf8: "direct-storage/BenchmarkItem/"
    )

    // MARK: - CRUD Operations (one-shot transactions, shared connection pool)

    static func insertOne(engine: any StorageEngine, id: String) async throws {
        let key = directStorageItemKey(id: id)
        let value = representativeItemValue
        try await StorageTransactionExecutor(engine: engine).withTransaction { tx in
            try tx.setValue(value, for: key)
        }
    }

    static func readOne(engine: any StorageEngine, id: String) async throws -> Bool {
        let key = directStorageItemKey(id: id)
        let result = try await StorageTransactionExecutor(engine: engine).withTransaction { tx in
            try await tx.getValue(for: key, snapshot: false)
        }
        return result != nil
    }

    static func updateOne(engine: any StorageEngine, id: String) async throws {
        // The storage transaction contract defines setValue as an upsert.
        try await insertOne(engine: engine, id: id)
    }

    static func deleteOne(engine: any StorageEngine, id: String) async throws {
        let key = directStorageItemKey(id: id)
        try await StorageTransactionExecutor(engine: engine).withTransaction { tx in
            try tx.clear(key: key)
        }
    }

    // MARK: - Batch Operations

    /// Insert multiple values within a single storage transaction.
    static func batchInsert(engine: any StorageEngine, count: Int) async throws {
        guard count > 0 else { return }
        try await StorageTransactionExecutor(engine: engine).withTransaction { tx in
            for _ in 0..<count {
                let id = UUID().uuidString
                try tx.setValue(representativeItemValue, for: directStorageItemKey(id: id))
            }
        }
    }

    // MARK: - Seed Data

    @discardableResult
    static func seedData(
        engine: any StorageEngine,
        count: Int,
        idPrefix: String = "seed"
    ) async throws -> [String] {
        var ids: [String] = []
        let batchSize = 100
        for batchStart in stride(from: 0, to: count, by: batchSize) {
            let end = min(batchStart + batchSize, count)
            var batchIDs: [String] = []
            batchIDs.reserveCapacity(end - batchStart)
            for i in batchStart..<end {
                batchIDs.append(
                    "\(idPrefix)-\(String(format: "%06d", i))"
                )
            }
            let immutableBatchIDs = batchIDs
            ids.append(contentsOf: immutableBatchIDs)
            try await StorageTransactionExecutor(engine: engine).withTransaction { tx in
                for id in immutableBatchIDs {
                    try tx.setValue(representativeItemValue, for: directStorageItemKey(id: id))
                }
            }
        }
        return ids
    }

    // MARK: - Cleanup

    static func cleanup(engine: any StorageEngine) async throws {
        try await StorageTransactionExecutor(engine: engine).withTransaction { tx in
            try tx.clearRange(
                beginKey: ByteString(utf8: "direct-storage/"),
                endKey: ByteString(utf8: "direct-storage0")
            )
        }
    }

    // MARK: - Key Construction

    /// Construct the baseline key without allocating an intermediate byte array.
    private static func directStorageItemKey(id: String) -> ByteString {
        ByteString.copying(count: itemKeyPrefix.count + id.utf8.count) { destination in
            var offset = 0
            for byte in itemKeyPrefix {
                destination[offset] = byte
                offset += 1
            }
            for byte in id.utf8 {
                destination[offset] = byte
                offset += 1
            }
        }
    }
}
