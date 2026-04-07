import Foundation
import BenchmarkFramework
import PostgresNIO
import PostgreSQLStorage
import StorageKit
import DatabaseEngine
import Core
import Logging

private let logger = Logger(label: "benchmark.profile")

// MARK: - Phase Timing

/// Measures individual phases of the framework's write path.
///
/// The insert call chain:
/// ```
/// FDBContext.init → insert() → save()
///   → TransactionRunner → StorageEngine.withTransaction
///     → ProtobufEncoder.encode()
///     → ItemEnvelope.serialize()
///     → transaction.setValue()
///     → IndexMaintenanceService.updateIndexes()
///   → commit()
/// ```
///
/// This benchmark isolates each layer to find where time is spent:
///
/// ```
/// Layer 0: Raw PostgreSQL           (native INSERT)
/// Layer 1: StorageKit KV raw        (transaction + setValue, no serialization)
/// Layer 2: StorageKit KV + Protobuf (transaction + serialize + setValue)
/// Layer 3: Full Framework           (FDBContext + save)
/// ```
enum ProfileBenchmark {

    struct PhaseResult: CustomStringConvertible {
        let name: String
        let iterations: Int
        let totalNanos: UInt64
        var avgMicros: Double { Double(totalNanos) / Double(iterations) / 1000.0 }

        var description: String {
            let padded = name.padding(toLength: 40, withPad: " ", startingAt: 0)
            return "  \(padded) \(String(format: "%8.1f", avgMicros)) us"
        }
    }

    // MARK: - Layer Comparison

    static func run(
        runner: BenchmarkRunner,
        client: PostgresClient,
        container: DBContainer
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Layer-by-Layer Overhead Analysis")
        print(String(repeating: "=", count: 70))

        // Clean state
        try await RawPostgreSQL.truncate(client: client)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let strategies: [Strategy] = [
            ("L0: Raw PostgreSQL", {
                let id = UUID().uuidString
                try await RawPostgreSQL.insertOne(
                    client: client, id: id, name: "Alice", age: 30, score: 85.5
                )
            }),
            ("L1: StorageKit KV (raw bytes)", {
                let id = UUID().uuidString
                try await storageKitRawWrite(engine: container.engine, id: id)
            }),
            ("L2: StorageKit KV + Protobuf", {
                let id = UUID().uuidString
                try await storageKitProtobufWrite(engine: container.engine, id: id)
            }),
            ("L3: Full Framework", {
                var item = BenchmarkItem()
                item.name = "Alice"
                item.age = 30
                item.score = 85.5
                try await FrameworkPostgreSQL.insertOne(container: container, item: item)
            }),
        ]
        let result = try await runner.compareStrategies(
            name: "Insert: Layer-by-Layer",
            strategies: strategies
        )
        ConsoleReporter.print(result)

        // Print delta analysis
        printDeltaAnalysis(result)
    }

    // MARK: - Phase Breakdown (CPU-only, no I/O)

    static func runPhaseBreakdown(iterations: Int = 10000) throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: CPU Phase Breakdown (no I/O, \(iterations) iterations)")
        print(String(repeating: "=", count: 70))

        let clock = ContinuousClock()

        // Phase 1: Context creation
        let contextCreation = try measurePhase(iterations: iterations, clock: clock) {
            // Requires a container, but we're measuring allocation cost
            // Use a lightweight substitute
        }

        // Phase 2: Protobuf serialization
        let encoder = ProtobufEncoder()
        var item = BenchmarkItem()
        item.name = "Alice"
        item.age = 30
        item.score = 85.5

        let protobufEncode = try measurePhase(iterations: iterations, clock: clock) {
            _ = try encoder.encode(item)
        }

        // Phase 3: ItemEnvelope wrapping
        let sampleData = try Array(encoder.encode(item))
        let envelopeWrap = try measurePhase(iterations: iterations, clock: clock) {
            let envelope = ItemEnvelope.inline(data: sampleData)
            _ = envelope.serialize()
        }

        // Phase 4: DataAccess.serialize (Protobuf + Array conversion)
        let dataAccessSerialize = try measurePhase(iterations: iterations, clock: clock) {
            _ = try DataAccess.serialize(item)
        }

        // Print results
        let results = [
            PhaseResult(name: "ProtobufEncoder.encode()", iterations: iterations, totalNanos: protobufEncode),
            PhaseResult(name: "ItemEnvelope.inline + serialize()", iterations: iterations, totalNanos: envelopeWrap),
            PhaseResult(name: "DataAccess.serialize() (encode + Array)", iterations: iterations, totalNanos: dataAccessSerialize),
        ]

        print("")
        print("  Phase                                      Avg (us)")
        print("  " + String(repeating: "-", count: 52))
        for r in results {
            print(r)
        }
        print("")
    }

    // MARK: - Read Path Profile

    static func runReadProfile(
        runner: BenchmarkRunner,
        client: PostgresClient,
        container: DBContainer
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Read Path Layer-by-Layer")
        print(String(repeating: "=", count: 70))

        // Seed data
        try await RawPostgreSQL.truncate(client: client)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let rawID = "read-profile-raw"
        let fwID = "read-profile-fw"
        let kvID = "read-profile-kv"

        // Seed raw
        try await RawPostgreSQL.insertOne(
            client: client, id: rawID, name: "Alice", age: 30, score: 85.5
        )

        // Seed framework
        var fwItem = BenchmarkItem()
        fwItem.id = fwID
        fwItem.name = "Alice"
        fwItem.age = 30
        fwItem.score = 85.5
        try await FrameworkPostgreSQL.insertOne(container: container, item: fwItem)

        // Seed KV (raw bytes for L1 read)
        try await storageKitProtobufWrite(engine: container.engine, id: kvID)

        let strategies: [Strategy] = [
            ("L0: Raw PostgreSQL", {
                _ = try await RawPostgreSQL.readOne(client: client, id: rawID)
            }),
            ("L1: StorageKit KV (raw get)", {
                try await storageKitRawRead(engine: container.engine, id: kvID)
            }),
            ("L3: Full Framework", {
                _ = try await FrameworkPostgreSQL.readOne(container: container, id: fwID)
            }),
        ]
        let result = try await runner.compareStrategies(
            name: "Read: Layer-by-Layer",
            strategies: strategies
        )
        ConsoleReporter.print(result)
        printDeltaAnalysis(result)
    }

    // MARK: - StorageKit Direct Operations

    /// Layer 1: Raw KV write through StorageKit (no serialization overhead).
    /// Writes a fixed-size byte array directly to the KV store.
    private static func storageKitRawWrite(engine: any StorageEngine, id: String) async throws {
        // Pre-computed key and value (skip serialization cost)
        let key: [UInt8] = Array("benchmark/items/\(id)".utf8)
        // ~50 bytes to match typical BenchmarkItem protobuf size
        let value: [UInt8] = Array(repeating: 0x42, count: 50)

        try await engine.withTransaction { tx in
            tx.setValue(value, for: key)
        }
    }

    /// Layer 2: KV write with Protobuf serialization + ItemEnvelope.
    private static func storageKitProtobufWrite(engine: any StorageEngine, id: String) async throws {
        var item = BenchmarkItem()
        item.id = id
        item.name = "Alice"
        item.age = 30
        item.score = 85.5

        let data = try DataAccess.serialize(item)
        let envelope = ItemEnvelope.inline(data: data)
        let serialized = envelope.serialize()
        let key: [UInt8] = Array("benchmark/items/\(id)".utf8)

        try await engine.withTransaction { tx in
            tx.setValue(serialized, for: key)
        }
    }

    /// Layer 1 read: Raw KV get through StorageKit.
    private static func storageKitRawRead(engine: any StorageEngine, id: String) async throws {
        let key: [UInt8] = Array("benchmark/items/\(id)".utf8)
        try await engine.withTransaction { tx in
            _ = try await tx.getValue(for: key, snapshot: false)
        }
    }

    // MARK: - Helpers

    private static func measurePhase(
        iterations: Int,
        clock: ContinuousClock,
        operation: () throws -> Void
    ) throws -> UInt64 {
        // Warmup
        for _ in 0..<100 {
            try operation()
        }

        let start = clock.now
        for _ in 0..<iterations {
            try operation()
        }
        let elapsed = clock.now - start
        let nanos = elapsed.components.seconds * 1_000_000_000
            + Int64(elapsed.components.attoseconds / 1_000_000_000)
        return UInt64(nanos)
    }

    private static func printDeltaAnalysis(_ result: StrategyComparisonResult) {
        let strategies = result.strategies
        guard strategies.count >= 2 else { return }

        print("  Delta Analysis:")
        print("  " + String(repeating: "-", count: 52))

        for i in 1..<strategies.count {
            let prev = strategies[i - 1]
            let curr = strategies[i]
            let delta = curr.metrics.latency.p50 - prev.metrics.latency.p50
            let pct: String
            if prev.metrics.latency.p50 > 0 {
                pct = String(format: "(+%.0f%%)", (delta / prev.metrics.latency.p50) * 100)
            } else {
                pct = ""
            }
            let from = prev.name.padding(toLength: 25, withPad: " ", startingAt: 0)
            let to = curr.name.padding(toLength: 25, withPad: " ", startingAt: 0)
            print("  \(from) → \(to)  \(String(format: "%+.2f", delta))ms \(pct)")
        }

        // Total overhead
        let base = strategies[0].metrics.latency.p50
        let full = strategies[strategies.count - 1].metrics.latency.p50
        let totalDelta = full - base
        print("\n  Total framework overhead: \(String(format: "%.2f", totalDelta))ms (\(String(format: "%.1f", full / base))x)")
        print("")
    }
}
