import Foundation
import StorageKit
import DatabaseEngine
import Core

/// Direct KV operations through the same StorageEngine as the framework.
///
/// Shares the same PostgresClient connection pool and kv_store table
/// as the framework, eliminating connection pool bias.
/// The only difference: Raw skips FDBContext, Protobuf, and ItemEnvelope.
///
/// Raw path:   engine.withAutoCommit → tx.setValue/getValue/clear
/// Framework:  FDBContext → DataStore → Protobuf → ItemEnvelope → engine.withAutoCommit → tx.setValue/getValue/clear
enum RawKV {

    /// Pre-computed value bytes (~70 bytes) matching typical Protobuf-encoded BenchmarkItem.
    private static let precomputedValue: [UInt8] = Array(repeating: 0x42, count: 70)

    // MARK: - CRUD Operations (auto-commit, shared connection pool)

    static func insertOne(engine: any StorageEngine, id: String) async throws {
        let key = makeKeyBytes(id: id)
        let value = precomputedValue
        try await engine.withAutoCommit { tx in
            tx.setValue(value, for: key)
        }
    }

    static func readOne(engine: any StorageEngine, id: String) async throws -> Bool {
        let key = makeKeyBytes(id: id)
        let result = try await engine.withAutoCommit { tx in
            try await tx.getValue(for: key, snapshot: false)
        }
        return result != nil
    }

    static func updateOne(engine: any StorageEngine, id: String) async throws {
        // Same as insert (UPSERT semantics via KV setValue)
        try await insertOne(engine: engine, id: id)
    }

    static func deleteOne(engine: any StorageEngine, id: String) async throws {
        let key = makeKeyBytes(id: id)
        try await engine.withAutoCommit { tx in
            tx.clear(key: key)
        }
    }

    // MARK: - Batch Operations

    /// Insert multiple items using individual KV puts within a single transaction.
    static func batchInsert(engine: any StorageEngine, count: Int) async throws {
        guard count > 0 else { return }
        try await engine.withTransaction { tx in
            for _ in 0..<count {
                let id = UUID().uuidString
                tx.setValue(precomputedValue, for: makeKeyBytes(id: id))
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
            try await engine.withTransaction { tx in
                for i in batchStart..<end {
                    let id = "\(idPrefix)-\(String(format: "%06d", i))"
                    ids.append(id)
                    tx.setValue(precomputedValue, for: makeKeyBytes(id: id))
                }
            }
        }
        return ids
    }

    // MARK: - Cleanup

    static func cleanup(engine: any StorageEngine) async throws {
        try await engine.withTransaction { tx in
            tx.clearRange(
                beginKey: Array("raw/".utf8),
                endKey: Array("raw0".utf8)  // "0" > "/" in ASCII, covers all "raw/..." keys
            )
        }
    }

    // MARK: - Helpers

    /// Key format: "raw/BenchmarkItem/" + id (~54 bytes total, matching framework key size)
    private static func makeKeyBytes(id: String) -> [UInt8] {
        var key: [UInt8] = Array("raw/BenchmarkItem/".utf8)
        key.append(contentsOf: Array(id.utf8))
        return key
    }
}
