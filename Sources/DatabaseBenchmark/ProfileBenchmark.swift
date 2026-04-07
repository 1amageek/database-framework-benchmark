import Foundation
import BenchmarkFramework
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
///   → TransactionRunner → StorageEngine.withAutoCommit
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
/// Layer 1: Raw KV              (engine.withAutoCommit + setValue, no serialization)
/// Layer 2: Raw KV + Protobuf   (engine.withAutoCommit + serialize + setValue)
/// Layer 3: Full Framework       (FDBContext + save)
/// ```
///
/// All layers share the same StorageEngine and connection pool.
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
        engine: any StorageEngine,
        container: DBContainer
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Layer-by-Layer Overhead Analysis")
        print(String(repeating: "=", count: 70))

        // Clean state
        try await RawKV.cleanup(engine: engine)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let strategies: [Strategy] = [
            ("L1: Raw KV (bytes only)", {
                let id = UUID().uuidString
                try await storageKitRawWrite(engine: engine, id: id)
            }),
            ("L2: Raw KV + Protobuf", {
                let id = UUID().uuidString
                try await storageKitProtobufWrite(engine: engine, id: id)
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

        // Phase 1: Protobuf serialization
        let encoder = ProtobufEncoder()
        var item = BenchmarkItem()
        item.name = "Alice"
        item.age = 30
        item.score = 85.5

        let protobufEncode = try measurePhase(iterations: iterations, clock: clock) {
            _ = try encoder.encode(item)
        }

        // Phase 2: ItemEnvelope wrapping
        let sampleData = try Array(encoder.encode(item))
        let envelopeWrap = try measurePhase(iterations: iterations, clock: clock) {
            let envelope = ItemEnvelope.inline(data: sampleData)
            _ = envelope.serialize()
        }

        // Phase 3: DataAccess.serialize (Protobuf + Array conversion)
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
        engine: any StorageEngine,
        container: DBContainer
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Read Path Layer-by-Layer")
        print(String(repeating: "=", count: 70))

        // Seed data
        try await RawKV.cleanup(engine: engine)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let fwID = "read-profile-fw"
        let kvID = "read-profile-kv"

        // Seed framework
        var fwItem = BenchmarkItem()
        fwItem.id = fwID
        fwItem.name = "Alice"
        fwItem.age = 30
        fwItem.score = 85.5
        try await FrameworkPostgreSQL.insertOne(container: container, item: fwItem)

        // Seed KV (raw bytes for L1 read)
        try await storageKitProtobufWrite(engine: engine, id: kvID)

        let strategies: [Strategy] = [
            ("L1: Raw KV (raw get)", {
                try await storageKitRawRead(engine: engine, id: kvID)
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

    // MARK: - Delete Path Profile

    static func runDeleteProfile(
        runner: BenchmarkRunner,
        engine: any StorageEngine,
        container: DBContainer
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Delete Path Layer-by-Layer")
        print(String(repeating: "=", count: 70))

        let strategies: [Strategy] = [
            ("L1: Raw KV", {
                let id = UUID().uuidString
                try await storageKitRawWrite(engine: engine, id: id)
                try await storageKitRawDelete(engine: engine, id: id)
            }),
            ("L3: Full Framework", {
                let id = UUID().uuidString
                var item = BenchmarkItem()
                item.id = id
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await FrameworkPostgreSQL.insertOne(container: container, item: item)
                try await FrameworkPostgreSQL.deleteOne(container: container, id: id)
            }),
        ]
        let result = try await runner.compareStrategies(
            name: "Insert+Delete: Layer-by-Layer",
            strategies: strategies
        )
        ConsoleReporter.print(result)
        printDeltaAnalysis(result)
    }

    // MARK: - Update Path Profile

    static func runUpdateProfile(
        runner: BenchmarkRunner,
        engine: any StorageEngine,
        container: DBContainer
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Update Path Layer-by-Layer")
        print(String(repeating: "=", count: 70))

        try await RawKV.cleanup(engine: engine)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let kvID = "update-profile-kv"
        let fwID = "update-profile-fw"

        try await storageKitProtobufWrite(engine: engine, id: kvID)

        var fwItem = BenchmarkItem()
        fwItem.id = fwID
        fwItem.name = "Alice"
        fwItem.age = 30
        fwItem.score = 85.5
        try await FrameworkPostgreSQL.insertOne(container: container, item: fwItem)

        let strategies: [Strategy] = [
            ("L1: Raw KV (overwrite)", {
                try await storageKitProtobufWrite(engine: engine, id: kvID)
            }),
            ("L3: Full Framework", {
                let v = Int.random(in: 0..<10000)
                var item = BenchmarkItem()
                item.id = fwID
                item.name = "Updated \(v)"
                item.age = 25 + v
                item.score = 70.0 + Double(v)
                try await FrameworkPostgreSQL.updateOne(container: container, item: item)
            }),
        ]
        let result = try await runner.compareStrategies(
            name: "Update: Layer-by-Layer",
            strategies: strategies
        )
        ConsoleReporter.print(result)
        printDeltaAnalysis(result)
    }

    // MARK: - StorageKit Direct Operations

    /// Layer 1: Raw KV write (no serialization overhead, auto-commit).
    private static func storageKitRawWrite(engine: any StorageEngine, id: String) async throws {
        let key: [UInt8] = Array("benchmark/items/\(id)".utf8)
        let value: [UInt8] = Array(repeating: 0x42, count: 70)
        try await engine.withAutoCommit { tx in
            tx.setValue(value, for: key)
        }
    }

    /// Layer 2: KV write with Protobuf serialization + ItemEnvelope (auto-commit).
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

        try await engine.withAutoCommit { tx in
            tx.setValue(serialized, for: key)
        }
    }

    /// Layer 1 delete: Raw KV delete (auto-commit).
    private static func storageKitRawDelete(engine: any StorageEngine, id: String) async throws {
        let key: [UInt8] = Array("benchmark/items/\(id)".utf8)
        try await engine.withAutoCommit { tx in
            tx.clear(key: key)
        }
    }

    /// Layer 1 read: Raw KV get (auto-commit).
    private static func storageKitRawRead(engine: any StorageEngine, id: String) async throws {
        let key: [UInt8] = Array("benchmark/items/\(id)".utf8)
        try await engine.withAutoCommit { tx in
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
