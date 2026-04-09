import Foundation
import Synchronization
import BenchmarkFramework
import PostgresNIO
import PostgreSQLStorage
import StorageKit
import DatabaseEngine
import Core

typealias Strategy = (String, @Sendable () async throws -> Void)

private final class CyclicIDPool: Sendable {
    private let ids: [String]
    private let state: Mutex<Int>

    init(ids: [String]) {
        self.ids = ids
        self.state = Mutex(0)
    }

    func next() -> String {
        state.withLock { index in
            let id = ids[index % ids.count]
            index += 1
            return id
        }
    }
}

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

            // Give PostgresNIO a brief head start so benchmark warmup does not race
            // the background run loop and skew the first measurements.
            try await Task.sleep(nanoseconds: 50_000_000)

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
                try await ProfileBenchmark.runReadLifecycleProfile(
                    runner: runner, engine: engine, container: container
                )
                try await ProfileBenchmark.runReadFixedIterationProfile(
                    engine: engine, container: container
                )
                try await ProfileBenchmark.runUpdateProfile(
                    runner: runner, engine: engine, container: container
                )
                try await ProfileBenchmark.runUpdateFixedIterationProfile(
                    engine: engine, container: container
                )
                try await ProfileBenchmark.runDeleteProfile(
                    runner: runner, engine: engine, container: container
                )
                try await ProfileBenchmark.runDeleteFixedIterationProfile(
                    engine: engine, container: container
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
        (BenchmarkLayerContract.rawKV, {
            let id = UUID().uuidString
            try await RawKV.insertOne(engine: engine, id: id)
        }),
        (BenchmarkLayerContract.fullFramework, {
            var item = BenchmarkItem()
            item.name = "Alice"
            item.age = 30
            item.score = 85.5
            try await FrameworkPostgreSQL.insertOne(container: container, item: item)
        }),
    ]
    let result = try await runner.compareStrategies(name: "Single Insert", strategies: strategies)
    ConsoleReporter.print(result)
    _ = try await FixedIterationReporter.print(title: "Single Insert", strategies: strategies)
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
        (BenchmarkLayerContract.rawKV, {
            try await RawKV.batchInsert(engine: engine, count: size)
        }),
        (BenchmarkLayerContract.fullFramework, {
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
    _ = try await FixedIterationReporter.print(title: "Batch Insert", strategies: strategies, iterations: 30)
}

private func runPointReadBenchmark(
    runner _: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await RawKV.cleanup(engine: engine)
    try await FrameworkPostgreSQL.cleanup(container: container)
    let dataStore = try await container.store(for: BenchmarkItem.self)

    let pointReadRunner = BenchmarkRunner(config: .init(
        warmupIterations: 15,
        measurementIterations: 120,
        throughputDuration: 5.0,
        comparisonRounds: 3,
        measureMemory: false
    ))

    let seedCount = 5000
    let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
    let rawIDs = try await RawKV.seedData(engine: engine, count: seedCount, idPrefix: "read-raw")
    let parityIDs = try await ProfileBenchmark.seedFrameworkLayoutStorageData(
        engine: engine,
        layout: layout,
        count: seedCount,
        idPrefix: "read-parity"
    )
    let dataStoreIDs = try await FrameworkPostgreSQL.seedData(
        container: container,
        count: seedCount,
        idPrefix: "read-ds"
    )
    let frameworkIDs = try await FrameworkPostgreSQL.seedData(
        container: container,
        count: seedCount,
        idPrefix: "read-fw"
    )
    let rawPool = CyclicIDPool(ids: rawIDs)
    let parityPool = CyclicIDPool(ids: parityIDs)
    let dataStorePool = CyclicIDPool(ids: dataStoreIDs)
    let frameworkPool = CyclicIDPool(ids: frameworkIDs)

    let strategies: [Strategy] = [
        (BenchmarkLayerContract.pointReadPresenceBaseline, {
            _ = try await RawKV.readOne(engine: engine, id: rawPool.next())
        }),
        (BenchmarkLayerContract.pointReadDecodeParity, {
            _ = try await ProfileBenchmark.frameworkLayoutStorageDecodedRead(
                engine: engine,
                layout: layout,
                id: parityPool.next()
            )
        }),
        (BenchmarkLayerContract.readDataStoreParity, {
            _ = try await dataStore.fetch(BenchmarkItem.self, id: dataStorePool.next())
        }),
        (BenchmarkLayerContract.fullFramework, {
            _ = try await FrameworkPostgreSQL.readOne(container: container, id: frameworkPool.next())
        }),
    ]
    let result = try await compareStrategiesMedianOfPasses(
        runner: pointReadRunner,
        name: "Point Read (from \(seedCount) records)",
        strategies: strategies
    )
    ConsoleReporter.print(result)
    let fixedMeasurements = try await FixedIterationReporter.print(
        title: "Point Read",
        strategies: strategies,
        iterations: 500,
        rounds: 5
    )
    printPointReadTargetAssessment(result: result, fixedMeasurements: fixedMeasurements)
}

private func runUpdateBenchmark(
    runner _: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await RawKV.cleanup(engine: engine)
    try await FrameworkPostgreSQL.cleanup(container: container)
    let dataStore = try await container.store(for: BenchmarkItem.self)
    let updateRunner = BenchmarkRunner(config: .init(
        warmupIterations: 15,
        measurementIterations: 120,
        throughputDuration: 5.0,
        comparisonRounds: 3,
        measureMemory: false
    ))

    let seedCount = 1024
    let rawIDs = try await RawKV.seedData(engine: engine, count: seedCount, idPrefix: "update-raw")
    let dataStoreIDs = try await FrameworkPostgreSQL.seedData(
        container: container,
        count: seedCount,
        idPrefix: "update-ds"
    )
    let frameworkIDs = try await FrameworkPostgreSQL.seedData(
        container: container,
        count: seedCount,
        idPrefix: "update-fw"
    )
    let rawPool = CyclicIDPool(ids: rawIDs)
    let dataStorePool = CyclicIDPool(ids: dataStoreIDs)
    let frameworkPool = CyclicIDPool(ids: frameworkIDs)

    let strategies: [Strategy] = [
        (BenchmarkLayerContract.rawKV, {
            try await RawKV.updateOne(engine: engine, id: rawPool.next())
        }),
        (BenchmarkLayerContract.dataStoreParity, {
            let id = dataStorePool.next()
            var item = BenchmarkItem()
            item.id = id
            item.name = "Updated Stable"
            item.age = 42
            item.score = 91.25
            try await dataStore.executeBatch(inserts: [item], deletes: [])
        }),
        (BenchmarkLayerContract.fullFramework, {
            let id = frameworkPool.next()
            var item = BenchmarkItem()
            item.id = id
            item.name = "Updated Stable"
            item.age = 42
            item.score = 91.25
            try await FrameworkPostgreSQL.updateOne(container: container, item: item)
        }),
    ]
    let result = try await compareStrategiesMedianOfPasses(
        runner: updateRunner,
        name: "Point Update",
        strategies: strategies
    )
    ConsoleReporter.print(result)
    let fixedMeasurements = try await FixedIterationReporter.print(
        title: "Point Update",
        strategies: strategies,
        rounds: 5
    )
    printParityAssessment(
        title: "Point Update",
        result: result,
        fixedMeasurements: fixedMeasurements,
        storageBaselineName: BenchmarkLayerContract.rawKV,
        dataStoreName: BenchmarkLayerContract.dataStoreParity,
        contextName: BenchmarkLayerContract.fullFramework
    )
}

private func runDeleteBenchmark(
    runner _: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await RawKV.cleanup(engine: engine)
    try await FrameworkPostgreSQL.cleanup(container: container)
    let dataStore = try await container.store(for: BenchmarkItem.self)
    let deleteRunner = BenchmarkRunner(config: .init(
        warmupIterations: 15,
        measurementIterations: 120,
        throughputDuration: 5.0,
        comparisonRounds: 3,
        measureMemory: false
    ))
    let idCount = 1024
    let rawPool = CyclicIDPool(ids: (0..<idCount).map { String(format: "delete-raw-%06d", $0) })
    let dataStorePool = CyclicIDPool(ids: (0..<idCount).map { String(format: "delete-ds-%06d", $0) })
    let frameworkPool = CyclicIDPool(ids: (0..<idCount).map { String(format: "delete-fw-%06d", $0) })

    let strategies: [Strategy] = [
        (BenchmarkLayerContract.rawKV, {
            let id = rawPool.next()
            try await RawKV.insertOne(engine: engine, id: id)
            try await RawKV.deleteOne(engine: engine, id: id)
        }),
        (BenchmarkLayerContract.dataStoreParity, {
            var item = BenchmarkItem()
            item.id = dataStorePool.next()
            item.name = "Temp"
            item.age = 30
            item.score = 50.0
            try await dataStore.executeBatch(inserts: [item], deletes: [])
            try await dataStore.executeBatch(inserts: [], deletes: [item])
        }),
        (BenchmarkLayerContract.fullFramework, {
            let id = frameworkPool.next()
            var item = BenchmarkItem()
            item.id = id
            item.name = "Temp"
            item.age = 30
            item.score = 50.0
            try await FrameworkPostgreSQL.insertOne(container: container, item: item)
            try await FrameworkPostgreSQL.deleteOne(container: container, id: id)
        }),
    ]
    let result = try await compareStrategiesMedianOfPasses(
        runner: deleteRunner,
        name: "Insert + Delete",
        strategies: strategies
    )
    ConsoleReporter.print(result)
    let fixedMeasurements = try await FixedIterationReporter.print(
        title: "Insert + Delete",
        strategies: strategies,
        rounds: 5
    )
    printParityAssessment(
        title: "Insert + Delete",
        result: result,
        fixedMeasurements: fixedMeasurements,
        storageBaselineName: BenchmarkLayerContract.rawKV,
        dataStoreName: BenchmarkLayerContract.dataStoreParity,
        contextName: BenchmarkLayerContract.fullFramework
    )
}

private func printPointReadTargetAssessment(
    result: StrategyComparisonResult,
    fixedMeasurements: [FixedIterationReporter.MeasurementSummary]
) {
    guard
        let presenceMetrics = result.strategies.first(where: {
            $0.name == BenchmarkLayerContract.pointReadPresenceBaseline
        })?.metrics.throughput?.opsPerSecond,
        let decodeParityMetrics = result.strategies.first(where: {
            $0.name == BenchmarkLayerContract.pointReadDecodeParity
        })?.metrics.throughput?.opsPerSecond,
        let dataStoreMetrics = result.strategies.first(where: {
            $0.name == BenchmarkLayerContract.readDataStoreParity
        })?.metrics.throughput?.opsPerSecond,
        let frameworkMetrics = result.strategies.first(where: {
            $0.name == BenchmarkLayerContract.fullFramework
        })?.metrics.throughput?.opsPerSecond,
        let presenceFixed = fixedMeasurements.first(where: {
            $0.name == BenchmarkLayerContract.pointReadPresenceBaseline
        })?.averageMicros,
        let decodeParityFixed = fixedMeasurements.first(where: {
            $0.name == BenchmarkLayerContract.pointReadDecodeParity
        })?.averageMicros,
        let dataStoreFixed = fixedMeasurements.first(where: {
            $0.name == BenchmarkLayerContract.readDataStoreParity
        })?.averageMicros,
        let frameworkFixed = fixedMeasurements.first(where: {
            $0.name == BenchmarkLayerContract.fullFramework
        })?.averageMicros,
        presenceMetrics > 0,
        decodeParityMetrics > 0,
        dataStoreMetrics > 0
    else {
        return
    }

    let strictThroughputOverheadPct = ((presenceMetrics - frameworkMetrics) / presenceMetrics) * 100
    let storageBaselineGapPct = ((presenceMetrics - decodeParityMetrics) / presenceMetrics) * 100
    let storageToDataStoreThroughputPct = ((decodeParityMetrics - dataStoreMetrics) / decodeParityMetrics) * 100
    let dataStoreToFrameworkThroughputPct = ((dataStoreMetrics - frameworkMetrics) / dataStoreMetrics) * 100
    let strictFixedDelta = frameworkFixed - presenceFixed
    let storageBaselineFixedDelta = decodeParityFixed - presenceFixed
    let storageToDataStoreFixedDelta = dataStoreFixed - decodeParityFixed
    let dataStoreToFrameworkFixedDelta = frameworkFixed - dataStoreFixed
    let tolerance = 0.05

    print("  Point Read Target vs Actual")
    print("  " + String(repeating: "-", count: 52))
    print("  Strict Gap")
    print("  " + String(repeating: "-", count: 52))
    print("  Raw presence -> Full Framework throughput gap: \(String(format: "%.2f", strictThroughputOverheadPct))%")
    print("  Raw presence -> Full Framework fixed delta: \(String(format: "%+.2f", strictFixedDelta)) us/op")
    print("")
    print("  Storage Parity Summary")
    print("  " + String(repeating: "-", count: 52))
    print("  Raw presence -> storage-stack parity throughput gap: \(String(format: "%.2f", storageBaselineGapPct))%")
    print("  Raw presence -> storage-stack parity fixed delta: \(String(format: "%+.2f", storageBaselineFixedDelta)) us/op")
    print("  \(formatPointReadTargetLine(label: "Storage-stack parity -> DataStore.fetch() throughput target <= 10%", delta: storageToDataStoreThroughputPct, unit: "%", tolerance: 10 + tolerance))")
    print("  \(formatPointReadTargetLine(label: "Storage-stack parity -> DataStore.fetch() fixed target <= 20 us/op", delta: storageToDataStoreFixedDelta, unit: " us/op", tolerance: 20 + tolerance))")
    print("")
    print("  Context Parity Summary")
    print("  " + String(repeating: "-", count: 52))
    print("  \(formatPointReadTargetLine(label: "DataStore.fetch() -> Full Framework throughput target <= 10%", delta: dataStoreToFrameworkThroughputPct, unit: "%", tolerance: 10 + tolerance))")
    print("  \(formatPointReadTargetLine(label: "DataStore.fetch() -> Full Framework fixed target <= 20 us/op", delta: dataStoreToFrameworkFixedDelta, unit: " us/op", tolerance: 20 + tolerance))")
    print("")
}

private func compareStrategiesMedianOfPasses(
    runner: BenchmarkRunner,
    name: String,
    strategies: [Strategy],
    passes: Int = 3
) async throws -> StrategyComparisonResult {
    var passResults: [StrategyComparisonResult] = []
    passResults.reserveCapacity(max(1, passes))

    for _ in 0..<max(1, passes) {
        let result = try await runner.compareStrategies(name: name, strategies: strategies)
        passResults.append(result)
    }

    return aggregateStrategyComparisonResults(name: name, passes: passResults)
}

private func aggregateStrategyComparisonResults(
    name: String,
    passes: [StrategyComparisonResult]
) -> StrategyComparisonResult {
    guard let first = passes.first else {
        return StrategyComparisonResult(name: name, strategies: [])
    }

    let aggregatedStrategies = first.strategies.enumerated().map { index, scenario in
        let metrics = passes.map { $0.strategies[index].metrics }
        return ScenarioResult(
            name: scenario.name,
            metrics: MetricsSnapshot(
                latency: aggregateLatencyMetrics(metrics.map(\.latency)),
                throughput: aggregateThroughputMetrics(metrics.compactMap(\.throughput)),
                memory: nil,
                storage: nil,
                accuracy: nil
            )
        )
    }

    return StrategyComparisonResult(name: name, strategies: aggregatedStrategies)
}

private func aggregateLatencyMetrics(_ metrics: [LatencyMetrics]) -> LatencyMetrics {
    LatencyMetrics(
        p50: median(metrics.map(\.p50)),
        p95: median(metrics.map(\.p95)),
        p99: median(metrics.map(\.p99)),
        avg: median(metrics.map(\.avg)),
        min: metrics.map(\.min).min() ?? 0,
        max: metrics.map(\.max).max() ?? 0,
        samples: Int(median(metrics.map { Double($0.samples) }).rounded())
    )
}

private func aggregateThroughputMetrics(_ metrics: [ThroughputMetrics]) -> ThroughputMetrics? {
    guard !metrics.isEmpty else {
        return nil
    }

    return ThroughputMetrics(
        opsPerSecond: median(metrics.map(\.opsPerSecond)),
        totalOperations: Int(median(metrics.map { Double($0.totalOperations) }).rounded()),
        durationSeconds: median(metrics.map(\.durationSeconds))
    )
}

private func median(_ values: [Double]) -> Double {
    guard !values.isEmpty else {
        return 0
    }

    let sorted = values.sorted()
    let count = sorted.count
    let middle = count / 2
    if count.isMultiple(of: 2) {
        return (sorted[middle - 1] + sorted[middle]) / 2
    }
    return sorted[middle]
}

private func printParityAssessment(
    title: String,
    result: StrategyComparisonResult,
    fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
    storageBaselineName: String,
    dataStoreName: String,
    contextName: String
) {
    print("  \(title) Parity Summary")
    print("  " + String(repeating: "-", count: 52))
    printTargetAssessmentSection(
        heading: "Storage Parity Summary",
        result: result,
        fixedMeasurements: fixedMeasurements,
        baselineName: storageBaselineName,
        candidateName: dataStoreName
    )
    printTargetAssessmentSection(
        heading: "Context Parity Summary",
        result: result,
        fixedMeasurements: fixedMeasurements,
        baselineName: dataStoreName,
        candidateName: contextName
    )
}

private func printTargetAssessmentSection(
    heading: String,
    result: StrategyComparisonResult,
    fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
    baselineName: String,
    candidateName: String,
    throughputTargetOverheadPct: Double = 10.0,
    fixedTargetDeltaMicros: Double = 20.0,
    tolerance: Double = 0.05
) {
    guard
        let baseline = result.strategies.first(where: { $0.name == baselineName }),
        let candidate = result.strategies.first(where: { $0.name == candidateName }),
        let fixedBaseline = fixedMeasurements.first(where: { $0.name == baselineName }),
        let fixedCandidate = fixedMeasurements.first(where: { $0.name == candidateName }),
        let baselineThroughput = baseline.metrics.throughput?.opsPerSecond,
        let candidateThroughput = candidate.metrics.throughput?.opsPerSecond,
        baselineThroughput > 0
    else {
        return
    }

    let throughputOverheadPct = ((baselineThroughput - candidateThroughput) / baselineThroughput) * 100
    let fixedDelta = fixedCandidate.averageMicros - fixedBaseline.averageMicros

    print("  \(heading)")
    print("  " + String(repeating: "-", count: 52))
    print("  \(formatPointReadTargetLine(label: "\(baselineName) -> \(candidateName) throughput target <= \(Int(throughputTargetOverheadPct))", delta: throughputOverheadPct, unit: "%", tolerance: throughputTargetOverheadPct + tolerance))")
    print("  \(formatPointReadTargetLine(label: "\(baselineName) -> \(candidateName) fixed target <= \(Int(fixedTargetDeltaMicros)) us/op", delta: fixedDelta, unit: " us/op", tolerance: fixedTargetDeltaMicros + tolerance))")
    print("")
}

private func formatPointReadTargetLine(
    label: String,
    delta: Double,
    unit: String,
    tolerance: Double
) -> String {
    if delta <= 0 {
        return "\(label): framework faster by \(String(format: "%.2f", abs(delta)))\(unit) [PASS]"
    }
    return "\(label): actual \(String(format: "%.2f", delta))\(unit) [\(delta <= tolerance ? "PASS" : "MISS")]"
}
