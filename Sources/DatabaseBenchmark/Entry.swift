import Foundation
import BenchmarkFramework
import PostgresNIO
import PostgreSQLStorage
import DatabaseEngine
import Core

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

            let runner = BenchmarkRunner(config: .init(
                warmupIterations: 5,
                measurementIterations: 50,
                throughputDuration: 5.0
            ))

            // Prepare raw table
            try await RawPostgreSQL.createTable(client: client)

            // --- Benchmark 1: Single Insert ---
            try await runSingleInsertBenchmark(runner: runner, client: client, container: container)

            // --- Benchmark 2: Batch Insert (100 items) ---
            try await runBatchInsertBenchmark(runner: runner, client: client, container: container, batchSize: 100)

            // --- Benchmark 3: Point Read ---
            try await runPointReadBenchmark(runner: runner, client: client, container: container)

            // --- Benchmark 4: Update ---
            try await runUpdateBenchmark(runner: runner, client: client, container: container)

            // --- Benchmark 5: Delete ---
            try await runDeleteBenchmark(runner: runner, client: client, container: container)

            // Cleanup
            try await RawPostgreSQL.dropTable(client: client)
            try await FrameworkPostgreSQL.cleanup(container: container)
            group.cancelAll()
        }

        print("Benchmark complete.")
    }
}

// MARK: - Scenarios

private func runSingleInsertBenchmark(
    runner: BenchmarkRunner,
    client: PostgresClient,
    container: DBContainer
) async throws {
    // Clean state
    try await RawPostgreSQL.truncate(client: client)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let result = try await runner.compareStrategies(
        name: "Single Insert",
        strategies: [
            ("Raw PostgreSQL", {
                let id = UUID().uuidString
                try await RawPostgreSQL.insertOne(
                    client: client, id: id, name: "Alice", age: 30, score: 85.5
                )
            }),
            ("DatabaseFramework", {
                var item = BenchmarkItem()
                item.name = "Alice"
                item.age = 30
                item.score = 85.5
                try await FrameworkPostgreSQL.insertOne(container: container, item: item)
            }),
        ]
    )
    ConsoleReporter.print(result)
}

private func runBatchInsertBenchmark(
    runner: BenchmarkRunner,
    client: PostgresClient,
    container: DBContainer,
    batchSize: Int
) async throws {
    try await RawPostgreSQL.truncate(client: client)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let result = try await runner.compareStrategies(
        name: "Batch Insert (\(batchSize) items)",
        strategies: [
            ("Raw PostgreSQL", {
                var items: [(id: String, name: String, age: Int, score: Double)] = []
                for i in 0..<batchSize {
                    items.append((
                        id: UUID().uuidString,
                        name: "User \(i)",
                        age: 20 + (i % 60),
                        score: Double(50 + (i % 50))
                    ))
                }
                try await RawPostgreSQL.batchInsert(client: client, items: items)
            }),
            ("DatabaseFramework", {
                var items: [BenchmarkItem] = []
                for i in 0..<batchSize {
                    var item = BenchmarkItem()
                    item.name = "User \(i)"
                    item.age = 20 + (i % 60)
                    item.score = Double(50 + (i % 50))
                    items.append(item)
                }
                try await FrameworkPostgreSQL.batchInsert(container: container, items: items)
            }),
        ]
    )
    ConsoleReporter.print(result)
}

private func runPointReadBenchmark(
    runner: BenchmarkRunner,
    client: PostgresClient,
    container: DBContainer
) async throws {
    // Seed data for reads
    try await RawPostgreSQL.truncate(client: client)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let seedCount = 1000
    let rawIDs = try await RawPostgreSQL.seedData(client: client, count: seedCount)
    let frameworkIDs = try await FrameworkPostgreSQL.seedData(container: container, count: seedCount)

    // Read a known ID from the middle of the dataset
    let rawTargetID = rawIDs[seedCount / 2]
    let fwTargetID = frameworkIDs[seedCount / 2]

    let result = try await runner.compareStrategies(
        name: "Point Read (from \(seedCount) records)",
        strategies: [
            ("Raw PostgreSQL", {
                _ = try await RawPostgreSQL.readOne(client: client, id: rawTargetID)
            }),
            ("DatabaseFramework", {
                _ = try await FrameworkPostgreSQL.readOne(container: container, id: fwTargetID)
            }),
        ]
    )
    ConsoleReporter.print(result)
}

private func runUpdateBenchmark(
    runner: BenchmarkRunner,
    client: PostgresClient,
    container: DBContainer
) async throws {
    // Seed one record to update repeatedly
    try await RawPostgreSQL.truncate(client: client)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let rawID = "update-target-raw"
    let fwID = "update-target-fw"
    try await RawPostgreSQL.insertOne(client: client, id: rawID, name: "Original", age: 25, score: 70.0)

    var fwItem = BenchmarkItem()
    fwItem.id = fwID
    fwItem.name = "Original"
    fwItem.age = 25
    fwItem.score = 70.0
    try await FrameworkPostgreSQL.insertOne(container: container, item: fwItem)

    var counter = 0
    let result = try await runner.compareStrategies(
        name: "Point Update",
        strategies: [
            ("Raw PostgreSQL", {
                counter += 1
                try await RawPostgreSQL.updateOne(
                    client: client, id: rawID, name: "Updated \(counter)", age: 25 + counter, score: 70.0 + Double(counter)
                )
            }),
            ("DatabaseFramework", {
                counter += 1
                var item = BenchmarkItem()
                item.id = fwID
                item.name = "Updated \(counter)"
                item.age = 25 + counter
                item.score = 70.0 + Double(counter)
                try await FrameworkPostgreSQL.updateOne(container: container, item: item)
            }),
        ]
    )
    ConsoleReporter.print(result)
}

private func runDeleteBenchmark(
    runner: BenchmarkRunner,
    client: PostgresClient,
    container: DBContainer
) async throws {
    // For delete benchmark, we insert before each delete operation.
    // This measures delete latency including the insert overhead,
    // but both sides pay the same insert cost.
    try await RawPostgreSQL.truncate(client: client)
    try await FrameworkPostgreSQL.cleanup(container: container)

    let result = try await runner.compareStrategies(
        name: "Insert + Delete",
        strategies: [
            ("Raw PostgreSQL", {
                let id = UUID().uuidString
                try await RawPostgreSQL.insertOne(client: client, id: id, name: "Temp", age: 30, score: 50.0)
                try await RawPostgreSQL.deleteOne(client: client, id: id)
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
    )
    ConsoleReporter.print(result)
}
