import Foundation
import Network
import Synchronization
import BenchmarkFramework
import PostgreSQLStorage
import StorageKit
import DatabaseEngine
import DatabaseKit

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
    static func main() async {
        do {
            let config = try BenchmarkConfig.fromEnvironment()

            print("=== database-framework-benchmark ===")
            print("Host: \(config.host):\(config.port)")
            print("Database: \(config.database)")
            print("")

            // Parse run mode from arguments
            let args = CommandLine.arguments
            let profileOnly = args.contains("--profile")
            let compareOnly = args.contains("--compare")
            let runAll = !profileOnly && !compareOnly

            var failures: [(String, Error)] = []

            // --- Profile Benchmarks ---
            if profileOnly || runAll {
                do {
                    try ProfileBenchmark.runPhaseBreakdown()
                } catch {
                    failures.append(("CPU Phase Breakdown", error))
                    printBenchmarkFailure("CPU Phase Breakdown", error: error)
                }
                await runStep("Insert Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await ProfileBenchmark.run(
                            runner: runner, engine: engine, container: container
                        )
                    }
                }
                await runStep("Read Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await ProfileBenchmark.runReadProfile(
                            runner: runner, engine: engine, container: container
                        )
                    }
                }
                await runStep("Read Lifecycle Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await ProfileBenchmark.runReadLifecycleProfile(
                            runner: runner, engine: engine, container: container
                        )
                    }
                }
                await runStep("Read Fixed Iteration Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { _, engine, container in
                        try await ProfileBenchmark.runReadFixedIterationProfile(
                            engine: engine, container: container
                        )
                    }
                }
                await runStep("Update Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await ProfileBenchmark.runUpdateProfile(
                            runner: runner, engine: engine, container: container
                        )
                    }
                }
                await runStep("Update Lifecycle Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { _, engine, container in
                        try await ProfileBenchmark.runUpdateLifecycleProfile(
                            engine: engine, container: container
                        )
                    }
                }
                await runStep("Update Fixed Iteration Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { _, engine, container in
                        try await ProfileBenchmark.runUpdateFixedIterationProfile(
                            engine: engine, container: container
                        )
                    }
                }
                await runStep("Delete Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await ProfileBenchmark.runDeleteProfile(
                            runner: runner, engine: engine, container: container
                        )
                    }
                }
                await runStep("Delete Lifecycle Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { _, _, container in
                        try await ProfileBenchmark.runDeleteLifecycleProfile(
                            container: container
                        )
                    }
                }
                await runStep("Delete Fixed Iteration Profile", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { _, engine, container in
                        try await ProfileBenchmark.runDeleteFixedIterationProfile(
                            engine: engine, container: container
                        )
                    }
                }
            }

            // --- Comparison Benchmarks ---
            if compareOnly || runAll {
                await runStep("Single Insert Compare", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await runSingleInsertBenchmark(runner: runner, engine: engine, container: container)
                    }
                }
                await runStep("Batch Insert Compare", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await runBatchInsertBenchmark(runner: runner, engine: engine, container: container, batchSize: 100)
                    }
                }
                await runStep("Point Read Compare", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await runPointReadBenchmark(runner: runner, engine: engine, container: container)
                    }
                }
                await runStep("Point Update Compare", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await runUpdateBenchmark(runner: runner, engine: engine, container: container)
                    }
                }
                await runStep("Point Delete Compare", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await runPointDeleteBenchmark(runner: runner, engine: engine, container: container)
                    }
                }
                await runStep("Insert + Delete Compare", failures: &failures) {
                    try await withBenchmarkEnvironment(config: config) { runner, engine, container in
                        try await runDeleteBenchmark(runner: runner, engine: engine, container: container)
                    }
                }
            }

            print("Benchmark complete.")
            if !failures.isEmpty {
                print("")
                print("Benchmark finished with failures:")
                for (name, error) in failures {
                    print(" - \(name): \(error)")
                }
            }
        } catch {
            print("Benchmark startup failed: \(error)")
        }
    }
}

private func withBenchmarkEnvironment<T>(
    config: BenchmarkConfig,
    _ body: @Sendable (BenchmarkRunner, any StorageEngine, DBContainer) async throws -> T
) async throws -> T {
    let attempts = 3
    var lastError: Error?

    for attempt in 1...attempts {
        do {
            guard await isEndpointReachable(host: config.host, port: config.port) else {
                throw BenchmarkRuntimeError.postgresUnavailable(host: config.host, port: config.port)
            }

            let container = try await DatabaseRecordWorkload.makeContainer(config: config)
            let engine = container.engine
            let runner = BenchmarkRunner(config: .init(
                warmupIterations: 10,
                measurementIterations: 100,
                throughputDuration: 5.0
            ))

            do {
                try await warmupBenchmarkEnvironment(engine: engine, container: container)
                let result = try await body(runner, engine, container)
                do {
                    try await DirectStorageWorkload.cleanup(engine: engine)
                    try await DatabaseRecordWorkload.cleanup(container: container)
                } catch {
                    print("Cleanup warning: \(error)")
                }
                engine.shutdown()
                return result
            } catch {
                do {
                    try await DirectStorageWorkload.cleanup(engine: engine)
                    try await DatabaseRecordWorkload.cleanup(container: container)
                } catch {
                    print("Cleanup warning after failure: \(error)")
                }
                engine.shutdown()
                throw error
            }
        } catch {
            lastError = error
            if attempt < attempts {
                let backoffSeconds = UInt64(attempt)
                print("Benchmark environment attempt \(attempt) failed: \(error)")
                print("Retrying after \(backoffSeconds)s...")
                try await Task.sleep(nanoseconds: backoffSeconds * 1_000_000_000)
                continue
            }
        }
    }

    throw lastError ?? BenchmarkRuntimeError.environmentSetupFailed
}

private func warmupBenchmarkEnvironment(
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    print("Warming up connection pool...")
    for _ in 0..<25 {
        try await DirectStorageWorkload.insertOne(engine: engine, id: UUID().uuidString)
    }
    for _ in 0..<25 {
        var item = BenchmarkItem()
        item.name = "warmup"
        try await DatabaseRecordWorkload.insertOne(container: container, item: item)
    }
    try await DirectStorageWorkload.cleanup(engine: engine)
    try await DatabaseRecordWorkload.cleanup(container: container)
    print("Warmup complete.\n")
}

private func runStep(
    _ name: String,
    failures: inout [(String, Error)],
    operation: @Sendable () async throws -> Void
) async {
    do {
        try await operation()
    } catch {
        failures.append((name, error))
        printBenchmarkFailure(name, error: error)
    }
}

private func printBenchmarkFailure(_ name: String, error: Error) {
    print("")
    print(String(repeating: "!", count: 70))
    print("Benchmark step failed: \(name)")
    print("Reason: \(error)")
    print(String(repeating: "!", count: 70))
    print("")
}

private enum BenchmarkRuntimeError: Error {
    case environmentSetupFailed
    case postgresUnavailable(host: String, port: Int)
}

private func isEndpointReachable(host: String, port: Int) async -> Bool {
    guard let nwPort = NWEndpoint.Port(rawValue: UInt16(port)) else {
        return false
    }

    return await withCheckedContinuation { continuation in
        let connection = NWConnection(host: NWEndpoint.Host(host), port: nwPort, using: .tcp)
        let queue = DispatchQueue(label: "benchmark.postgres.preflight")
        let finished = Mutex(false)

        @Sendable func complete(_ value: Bool) {
            let shouldResume = finished.withLock { state in
                if state {
                    return false
                }
                state = true
                return true
            }
            guard shouldResume else { return }
            connection.cancel()
            continuation.resume(returning: value)
        }

        connection.stateUpdateHandler = { state in
            switch state {
            case .ready:
                complete(true)
            case .failed(_), .cancelled:
                complete(false)
            default:
                break
            }
        }

        connection.start(queue: queue)

        queue.asyncAfter(deadline: .now() + 1.0) {
            complete(false)
        }
    }
}

// MARK: - Scenarios

private func runSingleInsertBenchmark(
    runner _: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await DirectStorageWorkload.cleanup(engine: engine)
    try await DatabaseRecordWorkload.cleanup(container: container)

    let insertRunner = BenchmarkRunner(config: .init(
        warmupIterations: 15,
        measurementIterations: 120,
        throughputDuration: 5.0,
        comparisonRounds: 3,
        measureMemory: false
    ))

    let strategies: [Strategy] = [
        (BenchmarkLayerContract.directStorage, {
            let id = UUID().uuidString
            try await DirectStorageWorkload.insertOne(engine: engine, id: id)
        }),
        (BenchmarkLayerContract.databaseRecordQueryAPI, {
            var item = BenchmarkItem()
            item.name = "Alice"
            item.age = 30
            item.score = 85.5
            try await DatabaseRecordWorkload.insertOne(container: container, item: item)
        }),
    ]
    let result = try await compareStrategiesMedianOfPasses(
        runner: insertRunner,
        name: "Single Insert",
        strategies: strategies
    )
    ConsoleReporter.print(result)
    _ = try await FixedIterationReporter.print(
        title: "Single Insert",
        strategies: strategies,
        rounds: 5
    )
}

private func runBatchInsertBenchmark(
    runner: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer,
    batchSize: Int
) async throws {
    try await DirectStorageWorkload.cleanup(engine: engine)
    try await DatabaseRecordWorkload.cleanup(container: container)

    let size = batchSize
    let strategies: [Strategy] = [
        (BenchmarkLayerContract.directStorage, {
            try await DirectStorageWorkload.batchInsert(engine: engine, count: size)
        }),
        (BenchmarkLayerContract.databaseRecordQueryAPI, {
            var items: [BenchmarkItem] = []
            for i in 0..<size {
                var item = BenchmarkItem()
                item.name = "User \(i)"
                item.age = Int64(20 + (i % 60))
                item.score = Double(50 + (i % 50))
                items.append(item)
            }
            try await DatabaseRecordWorkload.batchInsert(container: container, items: items)
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
    try await DirectStorageWorkload.cleanup(engine: engine)
    try await DatabaseRecordWorkload.cleanup(container: container)
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
    let directStorageIDs = try await DirectStorageWorkload.seedData(engine: engine, count: seedCount, idPrefix: "read-direct-storage")
    let canonicalStorageIDs = try await ProfileBenchmark.seedCanonicalRecordStorageData(
        engine: engine,
        layout: layout,
        count: seedCount,
        idPrefix: "read-parity"
    )
    let dataStoreIDs = try await DatabaseRecordWorkload.seedData(
        container: container,
        count: seedCount,
        idPrefix: "read-ds"
    )
    let databaseRecordIDs = try await DatabaseRecordWorkload.seedData(
        container: container,
        count: seedCount,
        idPrefix: "read-fw"
    )
    let directStoragePool = CyclicIDPool(ids: directStorageIDs)
    let canonicalStoragePool = CyclicIDPool(ids: canonicalStorageIDs)
    let dataStorePool = CyclicIDPool(ids: dataStoreIDs)
    let databaseRecordPool = CyclicIDPool(ids: databaseRecordIDs)

    let strategies: [Strategy] = [
        (BenchmarkLayerContract.directStoragePresenceRead, {
            _ = try await DirectStorageWorkload.readOne(engine: engine, id: directStoragePool.next())
        }),
        (BenchmarkLayerContract.canonicalRecordDecodeRead, {
            _ = try await ProfileBenchmark.canonicalRecordStorageRead(
                engine: engine,
                layout: layout,
                id: canonicalStoragePool.next()
            )
        }),
        (BenchmarkLayerContract.dataStoreRecordRead, {
            _ = try await dataStore.fetch(BenchmarkItem.self, id: dataStorePool.next())
        }),
        (BenchmarkLayerContract.databaseRecordQueryAPI, {
            _ = try await DatabaseRecordWorkload.readOne(container: container, id: databaseRecordPool.next())
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
    try await DirectStorageWorkload.cleanup(engine: engine)
    try await DatabaseRecordWorkload.cleanup(container: container)
    let dataStore = try await container.store(for: BenchmarkItem.self)
    let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
    let updateRunner = BenchmarkRunner(config: .init(
        warmupIterations: 15,
        measurementIterations: 120,
        throughputDuration: 5.0,
        comparisonRounds: 3,
        measureMemory: false
    ))

    let seedCount = 1024
    let directStorageIDs = try await DirectStorageWorkload.seedData(engine: engine, count: seedCount, idPrefix: "update-direct-storage")
    let storageIDs = try await ProfileBenchmark.seedCanonicalRecordStorageData(
        engine: engine,
        layout: layout,
        count: seedCount,
        idPrefix: "update-canonical-storage"
    )
    let dataStoreIDs = try await DatabaseRecordWorkload.seedData(
        container: container,
        count: seedCount,
        idPrefix: "update-ds"
    )
    let databaseRecordIDs = try await DatabaseRecordWorkload.seedData(
        container: container,
        count: seedCount,
        idPrefix: "update-fw"
    )
    let directStoragePool = CyclicIDPool(ids: directStorageIDs)
    let storagePool = CyclicIDPool(ids: storageIDs)
    let dataStorePool = CyclicIDPool(ids: dataStoreIDs)
    let databaseRecordPool = CyclicIDPool(ids: databaseRecordIDs)

    let strategies: [Strategy] = [
        (BenchmarkLayerContract.directStorage, {
            try await DirectStorageWorkload.updateOne(engine: engine, id: directStoragePool.next())
        }),
        (BenchmarkLayerContract.canonicalRecordStorageMutation, {
            try await ProfileBenchmark.canonicalRecordStorageWrite(
                engine: engine,
                layout: layout,
                id: storagePool.next()
            )
        }),
        (BenchmarkLayerContract.dataStoreBatchMutationAPI, {
            let id = dataStorePool.next()
            var item = BenchmarkItem()
            item.id = id
            item.name = "Updated Stable"
            item.age = 42
            item.score = 91.25
            try await dataStore.executeBatch(inserts: [item], deletes: [])
        }),
        (BenchmarkLayerContract.databaseRecordMutationAPI, {
            let id = databaseRecordPool.next()
            var item = BenchmarkItem()
            item.id = id
            item.name = "Updated Stable"
            item.age = 42
            item.score = 91.25
            try await DatabaseRecordWorkload.updateOne(container: container, item: item)
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
    printDatabaseRecordMutationAssessment(
        title: "Point Update",
        result: result,
        fixedMeasurements: fixedMeasurements,
        directStorageName: BenchmarkLayerContract.directStorage,
        canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorageMutation,
        dataStoreMutationName: BenchmarkLayerContract.dataStoreBatchMutationAPI,
        databaseRecordMutationName: BenchmarkLayerContract.databaseRecordMutationAPI
    )
}

private func runDeleteBenchmark(
    runner _: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await DirectStorageWorkload.cleanup(engine: engine)
    try await DatabaseRecordWorkload.cleanup(container: container)
    let dataStore = try await container.store(for: BenchmarkItem.self)
    let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
    let deleteRunner = BenchmarkRunner(config: .init(
        warmupIterations: 15,
        measurementIterations: 120,
        throughputDuration: 5.0,
        comparisonRounds: 3,
        measureMemory: false
    ))
    let idCount = 1024
    let directStoragePool = CyclicIDPool(ids: (0..<idCount).map { String(format: "delete-direct-storage-%06d", $0) })
    let dataStorePool = CyclicIDPool(ids: (0..<idCount).map { String(format: "delete-ds-%06d", $0) })
    let databaseRecordPool = CyclicIDPool(ids: (0..<idCount).map { String(format: "delete-database-record-%06d", $0) })

    let strategies: [Strategy] = [
        (BenchmarkLayerContract.directStorage, {
            let id = directStoragePool.next()
            try await DirectStorageWorkload.insertOne(engine: engine, id: id)
            try await DirectStorageWorkload.deleteOne(engine: engine, id: id)
        }),
        (BenchmarkLayerContract.canonicalRecordStorageMutation, {
            let id = UUID().uuidString
            try await ProfileBenchmark.canonicalRecordStorageWrite(
                engine: engine,
                layout: layout,
                id: id
            )
            try await ProfileBenchmark.canonicalRecordStorageDelete(
                engine: engine,
                layout: layout,
                id: id
            )
        }),
        (BenchmarkLayerContract.dataStoreBatchMutationAPI, {
            var item = BenchmarkItem()
            item.id = dataStorePool.next()
            item.name = "Temp"
            item.age = 30
            item.score = 50.0
            try await dataStore.executeBatch(inserts: [item], deletes: [])
            try await dataStore.executeBatch(inserts: [], deletes: [item])
        }),
        (BenchmarkLayerContract.databaseRecordMutationAPI, {
            let id = databaseRecordPool.next()
            var item = BenchmarkItem()
            item.id = id
            item.name = "Temp"
            item.age = 30
            item.score = 50.0
            try await DatabaseRecordWorkload.insertOne(container: container, item: item)
            try await DatabaseRecordWorkload.deleteOne(container: container, id: id)
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
    printDatabaseRecordMutationAssessment(
        title: "Insert + Delete",
        result: result,
        fixedMeasurements: fixedMeasurements,
        directStorageName: BenchmarkLayerContract.directStorage,
        canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorageMutation,
        dataStoreMutationName: BenchmarkLayerContract.dataStoreBatchMutationAPI,
        databaseRecordMutationName: BenchmarkLayerContract.databaseRecordMutationAPI
    )
}

private func runPointDeleteBenchmark(
    runner _: BenchmarkRunner,
    engine: any StorageEngine,
    container: DBContainer
) async throws {
    try await DirectStorageWorkload.cleanup(engine: engine)
    try await DatabaseRecordWorkload.cleanup(container: container)
    let dataStore = try await container.store(for: BenchmarkItem.self)
    let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
    let deleteRunner = BenchmarkRunner(config: .init(
        warmupIterations: 15,
        measurementIterations: 120,
        throughputDuration: 5.0,
        comparisonRounds: 3,
        measureMemory: false
    ))

    let comparePasses = 3
    let compareSeedCount = 20_000
    var comparePassResults: [StrategyComparisonResult] = []
    comparePassResults.reserveCapacity(comparePasses)

    for pass in 0..<comparePasses {
        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)

        let directStorageIDs = try await DirectStorageWorkload.seedData(
            engine: engine,
            count: compareSeedCount,
            idPrefix: "point-delete-direct-storage-p\(pass)"
        )
        let storageIDs = try await ProfileBenchmark.seedCanonicalRecordStorageData(
            engine: engine,
            layout: layout,
            count: compareSeedCount,
            idPrefix: "point-delete-canonical-storage-p\(pass)"
        )
        let dataStoreIDs = try await DatabaseRecordWorkload.seedData(
            container: container,
            count: compareSeedCount,
            idPrefix: "point-delete-ds-p\(pass)"
        )
        let databaseRecordIDs = try await DatabaseRecordWorkload.seedData(
            container: container,
            count: compareSeedCount,
            idPrefix: "point-delete-database-record-p\(pass)"
        )

        let comparisonDirectStoragePool = CyclicIDPool(ids: directStorageIDs)
        let compareStoragePool = CyclicIDPool(ids: storageIDs)
        let compareDataStorePool = CyclicIDPool(ids: dataStoreIDs)
        let comparisonDatabaseRecordPool = CyclicIDPool(ids: databaseRecordIDs)
        let compareStrategies: [Strategy] = [
            (BenchmarkLayerContract.directStorage, {
                try await DirectStorageWorkload.deleteOne(engine: engine, id: comparisonDirectStoragePool.next())
            }),
            (BenchmarkLayerContract.canonicalRecordStorageMutation, {
                try await ProfileBenchmark.canonicalRecordStorageDelete(
                    engine: engine,
                    layout: layout,
                    id: compareStoragePool.next()
                )
            }),
            (BenchmarkLayerContract.dataStoreBatchMutationAPI, {
                var item = BenchmarkItem()
                item.id = compareDataStorePool.next()
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await dataStore.executeBatch(inserts: [], deletes: [item])
            }),
            (BenchmarkLayerContract.databaseRecordMutationAPI, {
                try await DatabaseRecordWorkload.deleteOne(container: container, id: comparisonDatabaseRecordPool.next())
            }),
        ]
        let rotatedStrategies = rotateStrategies(compareStrategies, by: pass)
        let passResult = try await deleteRunner.compareStrategies(
            name: "Point Delete",
            strategies: rotatedStrategies
        )
        comparePassResults.append(passResult)
    }

    let result = aggregateStrategyComparisonResults(
        name: "Point Delete",
        orderedNames: BenchmarkLayerContract.writeCompareLabels,
        passes: comparePassResults
    )
    ConsoleReporter.print(result)

    try await DirectStorageWorkload.cleanup(engine: engine)
    try await DatabaseRecordWorkload.cleanup(container: container)
    let fixedSeedCount = 5_000
    let fixedDirectStoragePool = CyclicIDPool(ids: try await DirectStorageWorkload.seedData(
        engine: engine,
        count: fixedSeedCount,
        idPrefix: "point-delete-direct-storage-fixed"
    ))
    let fixedStoragePool = CyclicIDPool(ids: try await ProfileBenchmark.seedCanonicalRecordStorageData(
        engine: engine,
        layout: layout,
        count: fixedSeedCount,
        idPrefix: "point-delete-canonical-storage-fixed"
    ))
    let fixedDataStorePool = CyclicIDPool(ids: try await DatabaseRecordWorkload.seedData(
        container: container,
        count: fixedSeedCount,
        idPrefix: "point-delete-ds-fixed"
    ))
    let fixedDatabaseRecordPool = CyclicIDPool(ids: try await DatabaseRecordWorkload.seedData(
        container: container,
        count: fixedSeedCount,
        idPrefix: "point-delete-database-record-fixed"
    ))
    let fixedStrategies: [Strategy] = [
        (BenchmarkLayerContract.directStorage, {
            try await DirectStorageWorkload.deleteOne(engine: engine, id: fixedDirectStoragePool.next())
        }),
        (BenchmarkLayerContract.canonicalRecordStorageMutation, {
            try await ProfileBenchmark.canonicalRecordStorageDelete(
                engine: engine,
                layout: layout,
                id: fixedStoragePool.next()
            )
        }),
        (BenchmarkLayerContract.dataStoreBatchMutationAPI, {
            var item = BenchmarkItem()
            item.id = fixedDataStorePool.next()
            item.name = "Temp"
            item.age = 30
            item.score = 50.0
            try await dataStore.executeBatch(inserts: [], deletes: [item])
        }),
        (BenchmarkLayerContract.databaseRecordMutationAPI, {
            try await DatabaseRecordWorkload.deleteOne(container: container, id: fixedDatabaseRecordPool.next())
        }),
    ]
    let fixedMeasurements = try await FixedIterationReporter.print(
        title: "Point Delete",
        strategies: fixedStrategies,
        rounds: 5
    )
    printDatabaseRecordMutationAssessment(
        title: "Point Delete",
        result: result,
        fixedMeasurements: fixedMeasurements,
        directStorageName: BenchmarkLayerContract.directStorage,
        canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorageMutation,
        dataStoreMutationName: BenchmarkLayerContract.dataStoreBatchMutationAPI,
        databaseRecordMutationName: BenchmarkLayerContract.databaseRecordMutationAPI
    )
}

private func printPointReadTargetAssessment(
    result: StrategyComparisonResult,
    fixedMeasurements: [FixedIterationReporter.MeasurementSummary]
) {
    guard
        let presenceMetrics = result.strategies.first(where: {
            $0.name == BenchmarkLayerContract.directStoragePresenceRead
        })?.metrics.throughput?.opsPerSecond,
        let canonicalRecordDecodeThroughput = result.strategies.first(where: {
            $0.name == BenchmarkLayerContract.canonicalRecordDecodeRead
        })?.metrics.throughput?.opsPerSecond,
        let dataStoreMetrics = result.strategies.first(where: {
            $0.name == BenchmarkLayerContract.dataStoreRecordRead
        })?.metrics.throughput?.opsPerSecond,
        let databaseRecordMetrics = result.strategies.first(where: {
            $0.name == BenchmarkLayerContract.databaseRecordQueryAPI
        })?.metrics.throughput?.opsPerSecond,
        let presenceFixed = fixedMeasurements.first(where: {
            $0.name == BenchmarkLayerContract.directStoragePresenceRead
        })?.averageMicroseconds,
        let canonicalRecordDecodeMicroseconds = fixedMeasurements.first(where: {
            $0.name == BenchmarkLayerContract.canonicalRecordDecodeRead
        })?.averageMicroseconds,
        let dataStoreFixed = fixedMeasurements.first(where: {
            $0.name == BenchmarkLayerContract.dataStoreRecordRead
        })?.averageMicroseconds,
        let databaseRecordFixed = fixedMeasurements.first(where: {
            $0.name == BenchmarkLayerContract.databaseRecordQueryAPI
        })?.averageMicroseconds,
        presenceMetrics > 0,
        canonicalRecordDecodeThroughput > 0,
        dataStoreMetrics > 0
    else {
        return
    }

    let directStorageToDatabaseRecordThroughputGapPercent = ((presenceMetrics - databaseRecordMetrics) / presenceMetrics) * 100
    let canonicalStorageGapPercent = ((presenceMetrics - canonicalRecordDecodeThroughput) / presenceMetrics) * 100
    let storageToDataStoreThroughputPercent = ((canonicalRecordDecodeThroughput - dataStoreMetrics) / canonicalRecordDecodeThroughput) * 100
    let dataStoreToDatabaseRecordThroughputPercent = ((dataStoreMetrics - databaseRecordMetrics) / dataStoreMetrics) * 100
    let directStorageToDatabaseRecordFixedDelta = databaseRecordFixed - presenceFixed
    let canonicalStorageFixedDelta = canonicalRecordDecodeMicroseconds - presenceFixed
    let storageToDataStoreFixedDelta = dataStoreFixed - canonicalRecordDecodeMicroseconds
    let dataStoreToDatabaseRecordFixedDelta = databaseRecordFixed - dataStoreFixed
    let tolerance = 0.05

    print("  Point Read Target vs Actual")
    print("  " + String(repeating: "-", count: 52))
    print("  Strict Gap")
    print("  " + String(repeating: "-", count: 52))
    print("  Direct storage presence -> database record query throughput gap: \(String(format: "%.2f", directStorageToDatabaseRecordThroughputGapPercent))%")
    print("  Direct storage presence -> database record query fixed delta: \(String(format: "%+.2f", directStorageToDatabaseRecordFixedDelta)) us/op")
    print("")
    print("  Storage Parity Summary")
    print("  " + String(repeating: "-", count: 52))
    print("  Direct storage presence -> canonical record decode throughput gap: \(String(format: "%.2f", canonicalStorageGapPercent))%")
    print("  Direct storage presence -> canonical record decode fixed delta: \(String(format: "%+.2f", canonicalStorageFixedDelta)) us/op")
    print("  \(formatPointReadTargetLine(label: "Canonical record decode -> DataStore record read throughput target <= 10%", delta: storageToDataStoreThroughputPercent, unit: "%", tolerance: 10 + tolerance))")
    print("  \(formatPointReadTargetLine(label: "Canonical record decode -> DataStore record read fixed target <= 20 us/op", delta: storageToDataStoreFixedDelta, unit: " us/op", tolerance: 20 + tolerance))")
    print("")
    print("  Context Parity Summary")
    print("  " + String(repeating: "-", count: 52))
    print("  \(formatPointReadTargetLine(label: "DataStore record read -> database record query throughput target <= 10%", delta: dataStoreToDatabaseRecordThroughputPercent, unit: "%", tolerance: 10 + tolerance))")
    print("  \(formatPointReadTargetLine(label: "DataStore record read -> database record query fixed target <= 20 us/op", delta: dataStoreToDatabaseRecordFixedDelta, unit: " us/op", tolerance: 20 + tolerance))")
    print("")
}

private func compareStrategiesMedianOfPasses(
    runner: BenchmarkRunner,
    name: String,
    strategies: [Strategy],
    passes: Int = 3
) async throws -> StrategyComparisonResult {
    let orderedNames = strategies.map(\.0)
    var passResults: [StrategyComparisonResult] = []
    passResults.reserveCapacity(max(1, passes))

    for pass in 0..<max(1, passes) {
        let rotatedStrategies = rotateStrategies(strategies, by: pass)
        let result = try await runner.compareStrategies(name: name, strategies: rotatedStrategies)
        passResults.append(result)
    }

    return aggregateStrategyComparisonResults(
        name: name,
        orderedNames: orderedNames,
        passes: passResults
    )
}

private func aggregateStrategyComparisonResults(
    name: String,
    orderedNames: [String],
    passes: [StrategyComparisonResult]
) -> StrategyComparisonResult {
    guard !passes.isEmpty else {
        return StrategyComparisonResult(name: name, strategies: [])
    }

    var aggregatedStrategies: [ScenarioResult] = []
    aggregatedStrategies.reserveCapacity(orderedNames.count)

    for scenarioName in orderedNames {
        let scenarioMetrics = passes.compactMap { pass in
            pass.strategies.first(where: { $0.name == scenarioName })?.metrics
        }
        guard !scenarioMetrics.isEmpty else {
            continue
        }
        aggregatedStrategies.append(ScenarioResult(
            name: scenarioName,
            metrics: MetricsSnapshot(
                latency: aggregateLatencyMetrics(scenarioMetrics.map(\.latency)),
                throughput: aggregateThroughputMetrics(scenarioMetrics.compactMap(\.throughput)),
                memory: nil,
                storage: nil,
                accuracy: nil
            )
        ))
    }

    return StrategyComparisonResult(name: name, strategies: aggregatedStrategies)
}

private func rotateStrategies(_ strategies: [Strategy], by offset: Int) -> [Strategy] {
    guard !strategies.isEmpty else {
        return strategies
    }

    let normalizedOffset = offset % strategies.count
    guard normalizedOffset != 0 else {
        return strategies
    }

    return Array(strategies[normalizedOffset...] + strategies[..<normalizedOffset])
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
    canonicalStorageName: String,
    dataStoreName: String,
    contextName: String
) {
    print("  \(title) Parity Summary")
    print("  " + String(repeating: "-", count: 52))
    printTargetAssessmentSection(
        heading: "Storage Parity Summary",
        result: result,
        fixedMeasurements: fixedMeasurements,
        baselineName: canonicalStorageName,
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

private func printDatabaseRecordMutationAssessment(
    title: String,
    result: StrategyComparisonResult,
    fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
    directStorageName: String,
    canonicalStorageName: String,
    dataStoreMutationName: String,
    databaseRecordMutationName: String,
    throughputTargetOverheadPercent: Double = 10.0,
    fixedTargetDeltaMicroseconds: Double = 20.0,
    tolerance: Double = 0.05
) {
    print("  \(title) \(BenchmarkLayerContract.databaseRecordParitySummary)")
    print("  " + String(repeating: "-", count: 52))
    printTargetAssessmentSection(
        heading: BenchmarkLayerContract.databaseRecordParitySummary,
        result: result,
        fixedMeasurements: fixedMeasurements,
        baselineName: directStorageName,
        candidateName: databaseRecordMutationName,
        throughputTargetOverheadPercent: throughputTargetOverheadPercent,
        fixedTargetDeltaMicroseconds: fixedTargetDeltaMicroseconds,
        tolerance: tolerance
    )

    print("  \(title) \(BenchmarkLayerContract.diagnosticBreakdown)")
    print("  " + String(repeating: "-", count: 52))
    printWriteDiagnosticSection(
        heading: "Storage diagnostic",
        result: result,
        fixedMeasurements: fixedMeasurements,
        baselineName: canonicalStorageName,
        candidateName: dataStoreMutationName
    )
    printWriteDiagnosticSection(
        heading: "Database record mutation diagnostic",
        result: result,
        fixedMeasurements: fixedMeasurements,
        baselineName: dataStoreMutationName,
        candidateName: databaseRecordMutationName,
        expectsDatabaseRecordAdvantage: true
    )
}

private func printTargetAssessmentSection(
    heading: String,
    result: StrategyComparisonResult,
    fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
    baselineName: String,
    candidateName: String,
    throughputTargetOverheadPercent: Double = 10.0,
    fixedTargetDeltaMicroseconds: Double = 20.0,
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

    let throughputOverheadPercent = ((baselineThroughput - candidateThroughput) / baselineThroughput) * 100
    let fixedDelta = fixedCandidate.averageMicroseconds - fixedBaseline.averageMicroseconds

    print("  \(heading)")
    print("  " + String(repeating: "-", count: 52))
    print("  \(formatPointReadTargetLine(label: "\(baselineName) -> \(candidateName) throughput target <= \(Int(throughputTargetOverheadPercent))%", delta: throughputOverheadPercent, unit: "%", tolerance: throughputTargetOverheadPercent + tolerance))")
    print("  \(formatPointReadTargetLine(label: "\(baselineName) -> \(candidateName) fixed target <= \(Int(fixedTargetDeltaMicroseconds)) us/op", delta: fixedDelta, unit: " us/op", tolerance: fixedTargetDeltaMicroseconds + tolerance))")
    print("")
}

private func printWriteDiagnosticSection(
    heading: String,
    result: StrategyComparisonResult,
    fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
    baselineName: String,
    candidateName: String,
    expectsDatabaseRecordAdvantage: Bool = false
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

    let throughputDeltaPercent = ((baselineThroughput - candidateThroughput) / baselineThroughput) * 100
    let fixedDelta = fixedCandidate.averageMicroseconds - fixedBaseline.averageMicroseconds

    print("  \(heading)")
    print("  " + String(repeating: "-", count: 52))
    print("  \(baselineName) -> \(candidateName) throughput delta: \(formatWriteDiagnosticLine(delta: throughputDeltaPercent, unit: "%", expectsDatabaseRecordAdvantage: expectsDatabaseRecordAdvantage))")
    print("  \(baselineName) -> \(candidateName) fixed delta: \(formatWriteDiagnosticLine(delta: fixedDelta, unit: " us/op", expectsDatabaseRecordAdvantage: expectsDatabaseRecordAdvantage))")
    print("")
}

private func formatPointReadTargetLine(
    label: String,
    delta: Double,
    unit: String,
    tolerance: Double
) -> String {
    if delta <= 0 {
        return "\(label): candidate faster by \(String(format: "%.2f", abs(delta)))\(unit) [PASS]"
    }
    return "\(label): actual \(String(format: "%.2f", delta))\(unit) [\(delta <= tolerance ? "PASS" : "MISS")]"
}

private func formatWriteDiagnosticLine(
    delta: Double,
    unit: String,
    expectsDatabaseRecordAdvantage: Bool
) -> String {
    if delta < 0 {
        let suffix = expectsDatabaseRecordAdvantage ? " (expected database record API advantage)" : ""
        return "faster by \(String(format: "%.2f", abs(delta)))\(unit)\(suffix)"
    }
    return "slower by \(String(format: "%.2f", delta))\(unit)"
}
