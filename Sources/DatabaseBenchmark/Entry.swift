import Foundation
import BenchmarkFramework
import PostgresNIO
import PostgreSQLStorage
import StorageKit
import DatabaseEngine
import Core

typealias Strategy = (String, @Sendable () async throws -> Void)

@main
struct BenchmarkApp {
    static func main() async throws {
        let config = try BenchmarkConfig.fromEnvironment()

        print("=== database-framework-benchmark ===")
        print("Host: \(config.host):\(config.port)")
        print("Database: \(config.database)")
        print("")

        let client = PostgresClient(configuration: config.postgresClientConfig)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { await client.run() }

            let container = try await FrameworkPostgreSQL.makeContainer(config: config)
            let engine = container.engine

            let runner = BenchmarkRunner(config: .init(
                warmupIterations: 10,
                measurementIterations: 100,
                throughputDuration: 5.0
            ))

            // Warm up connection pool and PostgreSQL caches before any measurement.
            // This prevents execution-order bias where the second strategy benefits
            // from caches warmed by the first strategy's throughput phase.
            print("Warming up connection pool...")
            for _ in 0..<100 {
                try await RawKV.insertOne(engine: engine, id: UUID().uuidString)
            }
            for _ in 0..<100 {
                var item = BenchmarkItem()
                item.name = "warmup"
                try await FrameworkPostgreSQL.insertOne(container: container, item: item)
            }
            try await RawKV.cleanup(engine: engine)
            try await FrameworkPostgreSQL.cleanup(container: container)
            print("Warmup complete.\n")

            // Parse run mode from arguments
            let args = CommandLine.arguments
            let profileOnly = args.contains("--profile")
            let compareOnly = args.contains("--compare")
            let runAll = !profileOnly && !compareOnly

            // --- Profile Benchmarks ---
            if profileOnly || runAll {
                try ProfileBenchmark.runPhaseBreakdown()
                try await ProfileBenchmark.run(
                    runner: runner, engine: engine, container: container
                )
                try await ProfileBenchmark.runReadProfile(
                    runner: runner, engine: engine, container: container
                )
                try await ProfileBenchmark.runUpdateProfile(
                    runner: runner, engine: engine, container: container
                )
                try await ProfileBenchmark.runDeleteProfile(
                    runner: runner, engine: engine, container: container
                )
            }

            // --- Comparison Benchmarks ---
            if compareOnly || runAll {
                try await runSingleInsertBenchmark(runner: runner, engine: engine, container: container)
                try await runBatchInsertBenchmark(runner: runner, engine: engine, container: container, batchSize: 100)
                try await runPointReadBenchmark(runner: runner, engine: engine, container: container)
                try await runUpdateBenchmark(runner: runner, engine: engine, container: container)
                try await runDeleteBenchmark(runner: runner, engine: engine, container: container)
            }

            // Cleanup
            try await RawKV.cleanup(engine: engine)
            try await FrameworkPostgreSQL.cleanup(container: container)
            group.cancelAll()
        }

        print("Benchmark complete.")
    }
}

// MARK: - Scenarios

private func runSingleInsertBenchmark(
    runner: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await RawKV.cleanup(engine: engine)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let strategies: [Strategy] = [
        ("Raw KV", {
            let id = UUID().uuidString
            try await RawKV.insertOne(engine: engine, id: id)
        }),
        ("DatabaseFramework", {
            var item = BenchmarkItem()
            item.name = "Alice"
            item.age = 30
            item.score = 85.5
            try await FrameworkPostgreSQL.insertOne(container: container, item: item)
        }),
    ]
    let result = try await runner.compareStrategies(name: "Single Insert", strategies: strategies)
    ConsoleReporter.print(result)
}

private func runBatchInsertBenchmark(
    runner: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer,
    batchSize: Int
) async throws {
    try await RawKV.cleanup(engine: engine)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let size = batchSize
    let strategies: [Strategy] = [
        ("Raw KV", {
            try await RawKV.batchInsert(engine: engine, count: size)
        }),
        ("DatabaseFramework", {
            var items: [BenchmarkItem] = []
            for i in 0..<size {
                var item = BenchmarkItem()
                item.name = "User \(i)"
                item.age = 20 + (i % 60)
                item.score = Double(50 + (i % 50))
                items.append(item)
            }
            try await FrameworkPostgreSQL.batchInsert(container: container, items: items)
        }),
    ]
    let result = try await runner.compareStrategies(name: "Batch Insert (\(batchSize) items)", strategies: strategies)
    ConsoleReporter.print(result)
}

private func runPointReadBenchmark(
    runner: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await RawKV.cleanup(engine: engine)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let seedCount = 1000
    let rawIDs = try await RawKV.seedData(engine: engine, count: seedCount)
    let frameworkIDs = try await FrameworkPostgreSQL.seedData(container: container, count: seedCount)

    let rawTargetID = rawIDs[seedCount / 2]
    let fwTargetID = frameworkIDs[seedCount / 2]

    let strategies: [Strategy] = [
        ("Raw KV", {
            _ = try await RawKV.readOne(engine: engine, id: rawTargetID)
        }),
        ("DatabaseFramework", {
            _ = try await FrameworkPostgreSQL.readOne(container: container, id: fwTargetID)
        }),
    ]
    let result = try await runner.compareStrategies(name: "Point Read (from \(seedCount) records)", strategies: strategies)
    ConsoleReporter.print(result)
}

private func runUpdateBenchmark(
    runner: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await RawKV.cleanup(engine: engine)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let rawID = "update-target-raw"
    let fwID = "update-target-fw"
    try await RawKV.insertOne(engine: engine, id: rawID)

    var fwItem = BenchmarkItem()
    fwItem.id = fwID
    fwItem.name = "Original"
    fwItem.age = 25
    fwItem.score = 70.0
    try await FrameworkPostgreSQL.insertOne(container: container, item: fwItem)

    let strategies: [Strategy] = [
        ("Raw KV", {
            try await RawKV.updateOne(engine: engine, id: rawID)
        }),
        ("DatabaseFramework", {
            let variation = Int.random(in: 0..<10000)
            var item = BenchmarkItem()
            item.id = fwID
            item.name = "Updated \(variation)"
            item.age = 25 + variation
            item.score = 70.0 + Double(variation)
            try await FrameworkPostgreSQL.updateOne(container: container, item: item)
        }),
    ]
    let result = try await runner.compareStrategies(name: "Point Update", strategies: strategies)
    ConsoleReporter.print(result)
}

private func runDeleteBenchmark(
    runner: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await RawKV.cleanup(engine: engine)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let strategies: [Strategy] = [
        ("Raw KV", {
            let id = UUID().uuidString
            try await RawKV.insertOne(engine: engine, id: id)
            try await RawKV.deleteOne(engine: engine, id: id)
        }),
        ("DatabaseFramework", {
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
    let result = try await runner.compareStrategies(name: "Insert + Delete", strategies: strategies)
    ConsoleReporter.print(result)
}
