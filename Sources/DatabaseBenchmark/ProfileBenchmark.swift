import Foundation
import BenchmarkFramework
import StorageKit
import DatabaseEngine
import Core
import Logging
import Synchronization

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
/// Layer 1: Raw KV (ad hoc key, bytes only)                  - minimum raw baseline
/// Layer 2: Raw KV + framework layout + storage stack        - layout + DataAccess + ItemStorage
/// Layer 3: Generic DataStore batch path                     - internal generic batch path
/// Layer 4: Full Framework product path                      - FDBContext.save() fast path
/// ```
///
/// All layers share the same StorageEngine and connection pool.
enum ProfileBenchmark {
    private final class IterationIDPool: Sendable {
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

    private struct PoolRoundState: Sendable {
        let pool: IterationIDPool
    }

    private struct ContextRoundState: Sendable {
        let pool: IterationIDPool
        let context: FDBContext
    }

    private struct DataStoreRoundState: Sendable {
        let store: any DataStore
    }

    private struct DataStorePoolRoundState: Sendable {
        let store: any DataStore
        let pool: IterationIDPool
    }

    private struct DeleteContextRoundState: Sendable {
        let insertContext: FDBContext
        let deleteContext: FDBContext
    }

    struct BenchmarkStorageLayout: Sendable {
        let itemSubspace: Subspace
        let blobsSubspace: Subspace

        func frameworkItemKey(id: String) -> [UInt8] {
            itemSubspace.pack(Tuple([id]))
        }
    }

    struct PhaseResult: CustomStringConvertible {
        let name: String
        let iterations: Int
        let totalNanos: UInt64
        var avgMicros: Double { Double(totalNanos) / Double(iterations) / 1000.0 }

        var description: String {
            let padded = name.padding(toLength: max(40, name.count), withPad: " ", startingAt: 0)
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
        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)

        let strategies: [Strategy] = [
            (BenchmarkLayerContract.writeL1, {
                let id = UUID().uuidString
                try await rawAdHocWrite(engine: engine, id: id)
            }),
            (BenchmarkLayerContract.l2, {
                let id = UUID().uuidString
                try await frameworkLayoutStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: id,
                    isNewRecord: true
                )
            }),
            (BenchmarkLayerContract.writeL3, {
                var item = BenchmarkItem()
                item.name = "Alice"
                item.age = 30
                item.score = 85.5
                try await reusedStore.executeBatch(inserts: [item], deletes: [])
            }),
            (BenchmarkLayerContract.writeL4, {
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
        let fixedMeasurements = try await FixedIterationReporter.print(
            title: "Insert: Layer-by-Layer",
            strategies: strategies
        )
        printWriteLayerDeltaAnalysis(
            result,
            strictBaselineName: BenchmarkLayerContract.writeL1,
            storageBaselineName: BenchmarkLayerContract.l2,
            genericPathName: BenchmarkLayerContract.writeL3,
            productPathName: BenchmarkLayerContract.writeL4
        )
        printWriteProductTargetAssessment(
            title: "Insert Product parity summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            productBaselineName: BenchmarkLayerContract.writeL1,
            storageBaselineName: BenchmarkLayerContract.l2,
            genericPathName: BenchmarkLayerContract.writeL3,
            productPathName: BenchmarkLayerContract.writeL4
        )
    }

    // MARK: - Phase Breakdown (CPU-only, no I/O)

    static func runPhaseBreakdown(iterations: Int = 10000) throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: CPU Phase Breakdown (no I/O, \(iterations) iterations)")
        print(String(repeating: "=", count: 70))

        let clock = ContinuousClock()
        let decoder = ProtobufDecoder()

        // Phase 1: Protobuf serialization
        var item = BenchmarkItem()
        item.name = "Alice"
        item.age = 30
        item.score = 85.5

        let protobufEncode = try measurePhase(iterations: iterations, clock: clock) {
            _ = try DataAccess.serialize(item)
        }

        // Phase 2: TransformingSerializer byte path (small values stay uncompressed)
        let sampleBytes = try DataAccess.serialize(item)
        let sampleData = Data(sampleBytes)
        let transformer = TransformingSerializer(configuration: .default)
        let transformSerialize = try measurePhase(iterations: iterations, clock: clock) {
            _ = try transformer.serializeSyncBytes(sampleBytes)
        }

        // Phase 3: ItemEnvelope wrapping
        let envelopeWrap = try measurePhase(iterations: iterations, clock: clock) {
            let envelope = ItemEnvelope.inline(data: sampleBytes)
            _ = envelope.serialize()
        }

        // Phase 4: TransformingSerializer reverse path
        let transformedData = try transformer.serializeSyncBytes(sampleBytes)
        let transformDeserialize = try measurePhase(iterations: iterations, clock: clock) {
            _ = try transformer.deserializeSyncBytes(transformedData)
        }

        // Phase 5: Copied Data bridge used by the old deserialize path.
        let copiedDataBridge = try measurePhase(iterations: iterations, clock: clock) {
            _ = Data(sampleBytes)
        }

        // Phase 6: Decoder-only path (Data already materialized).
        let protobufDecode = try measurePhase(iterations: iterations, clock: clock) {
            let decoded: BenchmarkItem = try decoder.decode(BenchmarkItem.self, from: sampleData)
            _ = decoded.id
        }

        // Phase 7: Framework deserialize path ([UInt8] bridge + decode + model materialization).
        let dataAccessDeserialize = try measurePhase(iterations: iterations, clock: clock) {
            let decoded: BenchmarkItem = try DataAccess.deserialize(sampleBytes)
            _ = decoded.id
        }

        // Print results
        let results = [
            PhaseResult(name: "ProtobufEncoder.encode()", iterations: iterations, totalNanos: protobufEncode),
            PhaseResult(name: "TransformingSerializer.serializeSyncBytes()", iterations: iterations, totalNanos: transformSerialize),
            PhaseResult(name: "ItemEnvelope.inline + serialize()", iterations: iterations, totalNanos: envelopeWrap),
            PhaseResult(name: "TransformingSerializer.deserializeSyncBytes()", iterations: iterations, totalNanos: transformDeserialize),
            PhaseResult(name: "Data(bytes) copy bridge", iterations: iterations, totalNanos: copiedDataBridge),
            PhaseResult(name: "ProtobufDecoder.decode() (decode + materialize)", iterations: iterations, totalNanos: protobufDecode),
            PhaseResult(name: "DataAccess.deserialize() ([UInt8] bridge + decode)", iterations: iterations, totalNanos: dataAccessDeserialize),
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
        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)

        // Seed framework
        var fwItem = BenchmarkItem()
        fwItem.id = fwID
        fwItem.name = "Alice"
        fwItem.age = 30
        fwItem.score = 85.5
        try await FrameworkPostgreSQL.insertOne(container: container, item: fwItem)

        // Seed KV with the same serialization stack as the framework.
        try await frameworkLayoutStorageWrite(
            engine: engine,
            layout: layout,
            id: kvID,
            isNewRecord: true
        )

        let strategies: [Strategy] = [
            (BenchmarkLayerContract.readL1, {
                try await rawFrameworkKeyRead(engine: engine, layout: layout, id: kvID)
            }),
            (BenchmarkLayerContract.l2, {
                _ = try await frameworkLayoutStorageDecodedRead(engine: engine, layout: layout, id: kvID)
            }),
            (BenchmarkLayerContract.readDataStoreParity, {
                _ = try await reusedStore.fetch(BenchmarkItem.self, id: fwID)
            }),
            (BenchmarkLayerContract.fullFramework, {
                _ = try await FrameworkPostgreSQL.readOne(container: container, id: fwID)
            }),
        ]
        let result = try await runner.compareStrategies(
            name: "Read: Layer-by-Layer",
            strategies: strategies
        )
        ConsoleReporter.print(result)
        let fixedMeasurements = try await FixedIterationReporter.print(
            title: "Read: Layer-by-Layer",
            strategies: strategies,
            iterations: 300,
            rounds: 3
        )
        printStorageAndContextDeltaAnalysis(
            result,
            strictBaselineName: BenchmarkLayerContract.readL1,
            storageBaselineName: BenchmarkLayerContract.l2,
            dataStoreName: BenchmarkLayerContract.readDataStoreParity,
            contextName: BenchmarkLayerContract.fullFramework,
            storageDescription: BenchmarkLayerContract.readL2ToL3Description,
            contextDescription: BenchmarkLayerContract.readL3ToL4Description
        )
        printParityTargetAssessment(
            title: "Point Read Parity Summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            storageBaselineName: BenchmarkLayerContract.l2,
            dataStoreName: BenchmarkLayerContract.readDataStoreParity,
            contextName: BenchmarkLayerContract.fullFramework
        )
    }

    // MARK: - Read Lifecycle Profile

    static func runReadLifecycleProfile(
        runner: BenchmarkRunner,
        engine: any StorageEngine,
        container: DBContainer
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Read Path Lifecycle Overhead")
        print(String(repeating: "=", count: 70))

        try await RawKV.cleanup(engine: engine)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let readID = "read-lifecycle"
        var item = BenchmarkItem()
        item.id = readID
        item.name = "Alice"
        item.age = 30
        item.score = 85.5
        try await FrameworkPostgreSQL.insertOne(container: container, item: item)

        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)
        let reusedContext = FDBContext(container: container)

        let strategies: [Strategy] = [
            (BenchmarkLayerContract.l2, {
                _ = try await frameworkLayoutStorageDecodedRead(engine: engine, layout: layout, id: readID)
            }),
            ("DataStore.fetchById + autoCommit parity", {
                _ = try await reusedStore.withAutoCommit { transaction in
                    try await reusedStore.fetchByIdInTransaction(
                        BenchmarkItem.self,
                        id: readID,
                        transaction: transaction
                    )
                }
            }),
            (BenchmarkLayerContract.readDataStoreParity, {
                _ = try await reusedStore.fetch(BenchmarkItem.self, id: readID)
            }),
            ("Fresh DataStore.fetchById + autoCommit parity", {
                let store = try await container.store(for: BenchmarkItem.self)
                _ = try await store.withAutoCommit { transaction in
                    try await store.fetchByIdInTransaction(
                        BenchmarkItem.self,
                        id: readID,
                        transaction: transaction
                    )
                }
            }),
            (BenchmarkLayerContract.reusedContextParity, {
                _ = try await reusedContext.model(for: readID, as: BenchmarkItem.self)
            }),
            (BenchmarkLayerContract.freshContextParity, {
                _ = try await FrameworkPostgreSQL.readOne(container: container, id: readID)
            }),
        ]
        let result = try await runner.compareStrategies(
            name: "Read: Lifecycle Overhead",
            strategies: strategies
        )
        ConsoleReporter.print(result)
        _ = try await FixedIterationReporter.print(
            title: "Read: Lifecycle Overhead",
            strategies: strategies,
            iterations: 300,
            rounds: 3
        )
        printStorageAndContextDeltaAnalysis(
            result,
            storageBaselineName: BenchmarkLayerContract.l2,
            dataStoreName: BenchmarkLayerContract.readDataStoreParity,
            contextName: BenchmarkLayerContract.freshContextParity
        )
    }

    // MARK: - Read Fixed-Iteration Profile

    static func runReadFixedIterationProfile(
        engine: any StorageEngine,
        container: DBContainer,
        iterations: Int = 1000
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Read Hot Path Fixed Iteration (\(iterations) iterations)")
        print(String(repeating: "=", count: 70))

        try await RawKV.cleanup(engine: engine)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let readID = "read-fixed"
        var item = BenchmarkItem()
        item.id = readID
        item.name = "Alice"
        item.age = 30
        item.score = 85.5
        try await FrameworkPostgreSQL.insertOne(container: container, item: item)

        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)
        let reusedContext = FDBContext(container: container)
        let clock = ContinuousClock()

        let contextInit = try measurePhase(iterations: iterations, clock: clock) {
            _ = FDBContext(container: container)
        }
        let storeInit = try await measureAsyncPhase(iterations: iterations) {
            _ = try await container.store(for: BenchmarkItem.self)
        }
        let rawDecode = try await measureAsyncPhase(iterations: iterations) {
            _ = try await frameworkLayoutStorageDecodedRead(engine: engine, layout: layout, id: readID)
        }
        let dataStoreAutoCommit = try await measureAsyncPhase(iterations: iterations) {
            _ = try await reusedStore.withAutoCommit { transaction in
                try await reusedStore.fetchByIdInTransaction(
                    BenchmarkItem.self,
                    id: readID,
                    transaction: transaction
                )
            }
        }
        let dataStoreFetch = try await measureAsyncPhase(iterations: iterations) {
            _ = try await reusedStore.fetch(BenchmarkItem.self, id: readID)
        }
        let reusedContextRead = try await measureAsyncPhase(iterations: iterations) {
            _ = try await reusedContext.model(for: readID, as: BenchmarkItem.self)
        }
        let freshContextRead = try await measureAsyncPhase(iterations: iterations) {
            _ = try await FrameworkPostgreSQL.readOne(container: container, id: readID)
        }

        let results = [
            PhaseResult(name: "FDBContext.init()", iterations: iterations, totalNanos: contextInit),
            PhaseResult(name: "DBContainer.store(for:)", iterations: iterations, totalNanos: storeInit),
            PhaseResult(name: "Raw KV + framework layout + storage stack", iterations: iterations, totalNanos: rawDecode),
            PhaseResult(name: "DataStore.fetchById + autoCommit", iterations: iterations, totalNanos: dataStoreAutoCommit),
            PhaseResult(name: "DataStore.fetch()", iterations: iterations, totalNanos: dataStoreFetch),
            PhaseResult(name: "FDBContext.model() reused context", iterations: iterations, totalNanos: reusedContextRead),
            PhaseResult(name: "FDBContext.model() fresh context", iterations: iterations, totalNanos: freshContextRead),
        ]

        print("")
        print("  Phase                                      Avg (us)")
        print("  " + String(repeating: "-", count: 52))
        for r in results {
            print(r)
        }
        print("")

        print("  Inferred overheads")
        print("  " + String(repeating: "-", count: 52))
        printSignedDelta(
            name: "fetch() - fetchById+autoCommit",
            deltaNanos: Int64(dataStoreFetch) - Int64(dataStoreAutoCommit),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh context - reused context",
            deltaNanos: Int64(freshContextRead) - Int64(reusedContextRead),
            iterations: iterations
        )
        print("")
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
        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)

        let strategies: [Strategy] = [
            (BenchmarkLayerContract.writeL1, {
                let id = UUID().uuidString
                try await rawAdHocWrite(engine: engine, id: id)
                try await rawAdHocDelete(engine: engine, id: id)
            }),
            (BenchmarkLayerContract.l2, {
                let id = UUID().uuidString
                try await frameworkLayoutStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: id,
                    isNewRecord: true
                )
                try await frameworkLayoutStorageDelete(
                    engine: engine,
                    layout: layout,
                    id: id,
                    skipBlobCleanup: true
                )
            }),
            (BenchmarkLayerContract.writeL3, {
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await reusedStore.executeBatch(inserts: [item], deletes: [])
                try await reusedStore.executeBatch(inserts: [], deletes: [item])
            }),
            (BenchmarkLayerContract.writeL4, {
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
        let fixedMeasurements = try await measureDeletePathFixedSummaries(
            engine: engine,
            container: container,
            iterations: 200,
            rounds: 3
        )
        FixedIterationReporter.print(
            title: "Insert+Delete: Layer-by-Layer",
            summaries: fixedMeasurements,
            iterations: 200,
            rounds: 3
        )
        printStorageAndContextDeltaAnalysis(
            result,
            strictBaselineName: BenchmarkLayerContract.writeL1,
            storageBaselineName: BenchmarkLayerContract.l2,
            dataStoreName: BenchmarkLayerContract.writeL3,
            contextName: BenchmarkLayerContract.writeL4,
            storageDescription: BenchmarkLayerContract.writeL2ToL3Description,
            contextDescription: BenchmarkLayerContract.writeL3ToL4Description
        )
        printWriteProductTargetAssessment(
            title: "Insert+Delete Product parity summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            productBaselineName: BenchmarkLayerContract.writeL1,
            storageBaselineName: BenchmarkLayerContract.l2,
            genericPathName: BenchmarkLayerContract.writeL3,
            productPathName: BenchmarkLayerContract.writeL4
        )
    }

    private static func measureDeletePathFixedSummaries(
        engine: any StorageEngine,
        container: DBContainer,
        iterations: Int,
        rounds: Int
    ) async throws -> [FixedIterationReporter.MeasurementSummary] {
        try await RawKV.cleanup(engine: engine)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let layout = try await benchmarkStorageLayout(container: container)

        let rawInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in PoolRoundState(pool: IterationIDPool(ids: [])) },
            operation: { _ in
                let id = UUID().uuidString
                try await rawAdHocWrite(engine: engine, id: id)
                try await rawAdHocDelete(engine: engine, id: id)
            }
        )
        let layoutInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in PoolRoundState(pool: IterationIDPool(ids: [])) },
            operation: { _ in
                let id = UUID().uuidString
                try await frameworkLayoutStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: id,
                    isNewRecord: true
                )
                try await frameworkLayoutStorageDelete(
                    engine: engine,
                    layout: layout,
                    id: id,
                    skipBlobCleanup: true
                )
            }
        )
        let dataStoreInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let store = try await container.store(for: BenchmarkItem.self)
                return DataStoreRoundState(store: store)
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await state.store.executeBatch(inserts: [item], deletes: [])
                try await state.store.executeBatch(inserts: [], deletes: [item])
            }
        )
        let productInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await FrameworkPostgreSQL.cleanup(container: container)
                return DeleteContextRoundState(
                    insertContext: FDBContext(container: container),
                    deleteContext: FDBContext(container: container)
                )
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                state.insertContext.insert(item)
                try await state.insertContext.save()
                state.deleteContext.delete(item)
                try await state.deleteContext.save()
            }
        )

        return [
            .init(name: BenchmarkLayerContract.writeL1, totalNanos: rawInsertDelete / UInt64(iterations)),
            .init(name: BenchmarkLayerContract.l2, totalNanos: layoutInsertDelete / UInt64(iterations)),
            .init(name: BenchmarkLayerContract.writeL3, totalNanos: dataStoreInsertDelete / UInt64(iterations)),
            .init(name: BenchmarkLayerContract.writeL4, totalNanos: productInsertDelete / UInt64(iterations)),
        ]
    }

    // MARK: - Delete Fixed-Iteration Profile

    static func runDeleteLifecycleProfile(
        container: DBContainer,
        iterations: Int = 1000,
        rounds: Int = 3
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Delete Path Lifecycle Overhead (\(iterations) iterations, median of \(rounds) rounds)")
        print(String(repeating: "=", count: 70))

        try await FrameworkPostgreSQL.cleanup(container: container)

        let reusedInsertContext = FDBContext(container: container)
        let reusedDeleteContext = FDBContext(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)
        let clock = ContinuousClock()
        let seedCount = max(1024, iterations + 32)
        var insertSetupCounter = 0
        var deleteSetupCounter = 0

        let contextInit = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            _ = FDBContext(container: container)
        }
        let reusedInsertRollback = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            var item = BenchmarkItem()
            item.id = "delete-life-insert-reused-\(insertSetupCounter)"
            item.name = "Temp"
            item.age = 30
            item.score = 50.0
            insertSetupCounter += 1
            reusedInsertContext.insert(item)
            reusedInsertContext.rollback()
        }
        let freshInsertRollback = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            let context = FDBContext(container: container)
            var item = BenchmarkItem()
            item.id = "delete-life-insert-fresh-\(insertSetupCounter)"
            item.name = "Temp"
            item.age = 30
            item.score = 50.0
            insertSetupCounter += 1
            context.insert(item)
            context.rollback()
        }
        let reusedDeleteRollback = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            var item = BenchmarkItem()
            item.id = "delete-life-delete-reused-\(deleteSetupCounter)"
            item.name = "Temp"
            item.age = 30
            item.score = 50.0
            deleteSetupCounter += 1
            reusedDeleteContext.delete(item)
            reusedDeleteContext.rollback()
        }
        let freshDeleteRollback = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            let context = FDBContext(container: container)
            var item = BenchmarkItem()
            item.id = "delete-life-delete-fresh-\(deleteSetupCounter)"
            item.name = "Temp"
            item.age = 30
            item.score = 50.0
            deleteSetupCounter += 1
            context.delete(item)
            context.rollback()
        }
        let reusedInsertSave = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await FrameworkPostgreSQL.cleanup(container: container)
                return ContextRoundState(
                    pool: IterationIDPool(ids: []),
                    context: FDBContext(container: container)
                )
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                state.context.insert(item)
                try await state.context.save()
            }
        )
        let reusedStoreInsert = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await FrameworkPostgreSQL.cleanup(container: container)
                return DataStoreRoundState(store: reusedStore)
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await state.store.executeBatch(inserts: [item], deletes: [])
            }
        )
        let lookupStoreInsert = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await FrameworkPostgreSQL.cleanup(container: container)
                return PoolRoundState(pool: IterationIDPool(ids: []))
            },
            operation: { _ in
                let store = try await container.store(for: BenchmarkItem.self)
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await store.executeBatch(inserts: [item], deletes: [])
            }
        )
        let freshInsertSave = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await FrameworkPostgreSQL.cleanup(container: container)
                return PoolRoundState(pool: IterationIDPool(ids: []))
            },
            operation: { _ in
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await FrameworkPostgreSQL.insertOne(container: container, item: item)
            }
        )
        let reusedDeleteSave = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "delete-life-reused-r\(round)"
                )
                return ContextRoundState(
                    pool: IterationIDPool(ids: ids),
                    context: FDBContext(container: container)
                )
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                state.context.delete(item)
                try await state.context.save()
            }
        )
        let reusedStoreDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "delete-life-store-reused-r\(round)"
                )
                return DataStorePoolRoundState(
                    store: reusedStore,
                    pool: IterationIDPool(ids: ids)
                )
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await state.store.executeBatch(inserts: [], deletes: [item])
            }
        )
        let lookupStoreDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "delete-life-store-lookup-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                let store = try await container.store(for: BenchmarkItem.self)
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await store.executeBatch(inserts: [], deletes: [item])
            }
        )
        let freshDeleteSave = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "delete-life-fresh-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await FrameworkPostgreSQL.deleteOne(container: container, id: state.pool.next())
            }
        )

        let results = [
            PhaseResult(name: "FDBContext.init()", iterations: iterations, totalNanos: contextInit),
            PhaseResult(name: "FDBContext.insert()+rollback() reused", iterations: iterations, totalNanos: reusedInsertRollback),
            PhaseResult(name: "FDBContext.init()+insert()+rollback() fresh", iterations: iterations, totalNanos: freshInsertRollback),
            PhaseResult(name: "FDBContext.delete()+rollback() reused", iterations: iterations, totalNanos: reusedDeleteRollback),
            PhaseResult(name: "FDBContext.init()+delete()+rollback() fresh", iterations: iterations, totalNanos: freshDeleteRollback),
            PhaseResult(name: "DataStore.executeBatch() insert reused store", iterations: iterations, totalNanos: reusedStoreInsert),
            PhaseResult(name: "DataStore.executeBatch() insert with store lookup", iterations: iterations, totalNanos: lookupStoreInsert),
            PhaseResult(name: "FDBContext.save() insert reused", iterations: iterations, totalNanos: reusedInsertSave),
            PhaseResult(name: "FDBContext.save() insert fresh", iterations: iterations, totalNanos: freshInsertSave),
            PhaseResult(name: "DataStore.executeBatch() delete reused store", iterations: iterations, totalNanos: reusedStoreDelete),
            PhaseResult(name: "DataStore.executeBatch() delete with store lookup", iterations: iterations, totalNanos: lookupStoreDelete),
            PhaseResult(name: "FDBContext.save() delete reused", iterations: iterations, totalNanos: reusedDeleteSave),
            PhaseResult(name: "FDBContext.save() delete fresh", iterations: iterations, totalNanos: freshDeleteSave),
        ]

        print("")
        print("  Phase                                      Avg (us)")
        print("  " + String(repeating: "-", count: 52))
        for r in results {
            print(r)
        }
        print("")

        print("  Inferred overheads")
        print("  " + String(repeating: "-", count: 52))
        printSignedDelta(
            name: "fresh insert setup - reused",
            deltaNanos: Int64(freshInsertRollback) - Int64(reusedInsertRollback),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh delete setup - reused",
            deltaNanos: Int64(freshDeleteRollback) - Int64(reusedDeleteRollback),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh insert save - reused",
            deltaNanos: Int64(freshInsertSave) - Int64(reusedInsertSave),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh delete save - reused",
            deltaNanos: Int64(freshDeleteSave) - Int64(reusedDeleteSave),
            iterations: iterations
        )
        printSignedDelta(
            name: "store lookup insert - reused store",
            deltaNanos: Int64(lookupStoreInsert) - Int64(reusedStoreInsert),
            iterations: iterations
        )
        printSignedDelta(
            name: "store lookup delete - reused store",
            deltaNanos: Int64(lookupStoreDelete) - Int64(reusedStoreDelete),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh insert save - store lookup insert",
            deltaNanos: Int64(freshInsertSave) - Int64(lookupStoreInsert),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh delete save - store lookup delete",
            deltaNanos: Int64(freshDeleteSave) - Int64(lookupStoreDelete),
            iterations: iterations
        )
        print("")
    }

    static func runDeleteFixedIterationProfile(
        engine: any StorageEngine,
        container: DBContainer,
        iterations: Int = 500,
        rounds: Int = 3
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Insert+Delete Hot Path Fixed Iteration (\(iterations) iterations, median of \(rounds) rounds)")
        print(String(repeating: "=", count: 70))

        try await RawKV.cleanup(engine: engine)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let layout = try await benchmarkStorageLayout(container: container)
        let rawInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in PoolRoundState(pool: IterationIDPool(ids: [])) },
            operation: { _ in
                let id = UUID().uuidString
                try await rawAdHocWrite(engine: engine, id: id)
                try await rawAdHocDelete(engine: engine, id: id)
            }
        )

        let layoutInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in PoolRoundState(pool: IterationIDPool(ids: [])) },
            operation: { _ in
                let id = UUID().uuidString
                try await frameworkLayoutStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: id,
                    isNewRecord: true
                )
                try await frameworkLayoutStorageDelete(
                    engine: engine,
                    layout: layout,
                    id: id,
                    skipBlobCleanup: true
                )
            }
        )

        let dataStoreInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let store = try await container.store(for: BenchmarkItem.self)
                return DataStoreRoundState(store: store)
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await state.store.executeBatch(inserts: [item], deletes: [])
                try await state.store.executeBatch(inserts: [], deletes: [item])
            }
        )

        let reusedContextInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await FrameworkPostgreSQL.cleanup(container: container)
                return DeleteContextRoundState(
                    insertContext: FDBContext(container: container),
                    deleteContext: FDBContext(container: container)
                )
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                state.insertContext.insert(item)
                try await state.insertContext.save()
                state.deleteContext.delete(item)
                try await state.deleteContext.save()
            }
        )

        let freshContextInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await FrameworkPostgreSQL.cleanup(container: container)
                return PoolRoundState(pool: IterationIDPool(ids: []))
            },
            operation: { _ in
                let id = UUID().uuidString
                var item = BenchmarkItem()
                item.id = id
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await FrameworkPostgreSQL.insertOne(container: container, item: item)
                try await FrameworkPostgreSQL.deleteOne(container: container, id: id)
            }
        )

        let results = [
            PhaseResult(name: "Raw KV insert+delete", iterations: iterations, totalNanos: rawInsertDelete),
            PhaseResult(name: "Framework layout + storage insert+delete", iterations: iterations, totalNanos: layoutInsertDelete),
            PhaseResult(name: "Generic DataStore batch path insert+delete", iterations: iterations, totalNanos: dataStoreInsertDelete),
            PhaseResult(name: "FDBContext.save() reused contexts", iterations: iterations, totalNanos: reusedContextInsertDelete),
            PhaseResult(name: "FDBContext.save() fresh contexts", iterations: iterations, totalNanos: freshContextInsertDelete),
        ]

        print("")
        print("  Phase                                      Avg (us)")
        print("  " + String(repeating: "-", count: 52))
        for r in results {
            print(r)
        }
        print("")

        print("  Inferred overheads")
        print("  " + String(repeating: "-", count: 52))
        printSignedDelta(
            name: "generic-vs-layout insert+delete",
            deltaNanos: Int64(dataStoreInsertDelete) - Int64(layoutInsertDelete),
            iterations: iterations
        )
        printSignedDelta(
            name: "product-vs-generic insert+delete",
            deltaNanos: Int64(reusedContextInsertDelete) - Int64(dataStoreInsertDelete),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh-vs-reused insert+delete",
            deltaNanos: Int64(freshContextInsertDelete) - Int64(reusedContextInsertDelete),
            iterations: iterations
        )
        print("")
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
        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)

        try await frameworkLayoutStorageWrite(
            engine: engine,
            layout: layout,
            id: kvID,
            isNewRecord: true
        )

        var fwItem = BenchmarkItem()
        fwItem.id = fwID
        fwItem.name = "Alice"
        fwItem.age = 30
        fwItem.score = 85.5
        try await FrameworkPostgreSQL.insertOne(container: container, item: fwItem)

        var updated = BenchmarkItem()
        updated.id = fwID
        updated.name = "Updated Stable"
        updated.age = 42
        updated.score = 91.25
        let updatedItem = updated

        let strategies: [Strategy] = [
            (BenchmarkLayerContract.writeL1, {
                try await rawAdHocWrite(engine: engine, id: kvID)
            }),
            (BenchmarkLayerContract.l2, {
                try await frameworkLayoutStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: kvID,
                    isNewRecord: false
                )
            }),
            (BenchmarkLayerContract.writeL3, {
                try await reusedStore.executeBatch(inserts: [updatedItem], deletes: [])
            }),
            (BenchmarkLayerContract.writeL4, {
                try await FrameworkPostgreSQL.updateOne(container: container, item: updatedItem)
            }),
        ]
        let result = try await runner.compareStrategies(
            name: "Update: Layer-by-Layer",
            strategies: strategies
        )
        ConsoleReporter.print(result)
        let fixedMeasurements = try await measureUpdatePathFixedSummaries(
            engine: engine,
            container: container,
            iterations: 200,
            rounds: 3
        )
        FixedIterationReporter.print(
            title: "Update: Layer-by-Layer",
            summaries: fixedMeasurements,
            iterations: 200,
            rounds: 3
        )
        printStorageAndContextDeltaAnalysis(
            result,
            strictBaselineName: BenchmarkLayerContract.writeL1,
            storageBaselineName: BenchmarkLayerContract.l2,
            dataStoreName: BenchmarkLayerContract.writeL3,
            contextName: BenchmarkLayerContract.writeL4,
            storageDescription: BenchmarkLayerContract.writeL2ToL3Description,
            contextDescription: BenchmarkLayerContract.writeL3ToL4Description
        )
        printWriteProductTargetAssessment(
            title: "Point Update Product parity summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            productBaselineName: BenchmarkLayerContract.writeL1,
            storageBaselineName: BenchmarkLayerContract.l2,
            genericPathName: BenchmarkLayerContract.writeL3,
            productPathName: BenchmarkLayerContract.writeL4
        )
    }

    private static func measureUpdatePathFixedSummaries(
        engine: any StorageEngine,
        container: DBContainer,
        iterations: Int,
        rounds: Int
    ) async throws -> [FixedIterationReporter.MeasurementSummary] {
        try await RawKV.cleanup(engine: engine)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)
        let seedCount = 1024

        let rawUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await RawKV.cleanup(engine: engine)
                let ids = try await RawKV.seedData(
                    engine: engine,
                    count: seedCount,
                    idPrefix: "update-profile-raw-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await RawKV.updateOne(engine: engine, id: state.pool.next())
            }
        )
        let layoutUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await seedFrameworkLayoutStorageData(
                    engine: engine,
                    layout: layout,
                    count: seedCount,
                    idPrefix: "update-profile-layout-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await frameworkLayoutStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: state.pool.next(),
                    isNewRecord: false
                )
            }
        )
        let dataStoreUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-profile-ds-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Updated Stable"
                item.age = 42
                item.score = 91.25
                try await reusedStore.executeBatch(inserts: [item], deletes: [])
            }
        )
        let productUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-profile-fw-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Updated Stable"
                item.age = 42
                item.score = 91.25
                try await FrameworkPostgreSQL.updateOne(container: container, item: item)
            }
        )

        let divisor = UInt64(iterations)
        return [
            .init(name: BenchmarkLayerContract.writeL1, totalNanos: rawUpdate / divisor),
            .init(name: BenchmarkLayerContract.l2, totalNanos: layoutUpdate / divisor),
            .init(name: BenchmarkLayerContract.writeL3, totalNanos: dataStoreUpdate / divisor),
            .init(name: BenchmarkLayerContract.writeL4, totalNanos: productUpdate / divisor),
        ]
    }

    // MARK: - Update Lifecycle Profile

    static func runUpdateLifecycleProfile(
        engine _: any StorageEngine,
        container: DBContainer,
        iterations: Int = 1000,
        rounds: Int = 3
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Update Path Lifecycle Overhead (\(iterations) iterations, median of \(rounds) rounds)")
        print(String(repeating: "=", count: 70))

        try await FrameworkPostgreSQL.cleanup(container: container)

        let reusedContext = FDBContext(container: container)
        let clock = ContinuousClock()
        let seedCount = 1024
        var reusedSetupCounter = 0
        var freshSetupCounter = 0

        let contextInit = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            _ = FDBContext(container: container)
        }
        let reusedInsertRollback = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            var item = BenchmarkItem()
            item.id = "update-life-local-reused-\(reusedSetupCounter)"
            item.name = "Updated Stable"
            item.age = 42
            item.score = 91.25
            reusedSetupCounter += 1
            reusedContext.insert(item)
            reusedContext.rollback()
        }
        let freshInsertRollback = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            let context = FDBContext(container: container)
            var item = BenchmarkItem()
            item.id = "update-life-local-fresh-\(freshSetupCounter)"
            item.name = "Updated Stable"
            item.age = 42
            item.score = 91.25
            freshSetupCounter += 1
            context.insert(item)
            context.rollback()
        }
        let reusedContextUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-life-reused-r\(round)"
                )
                return ContextRoundState(
                    pool: IterationIDPool(ids: ids),
                    context: FDBContext(container: container)
                )
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Updated Stable"
                item.age = 42
                item.score = 91.25
                state.context.insert(item)
                try await state.context.save()
            }
        )
        let freshContextUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-life-fresh-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Updated Stable"
                item.age = 42
                item.score = 91.25
                try await FrameworkPostgreSQL.updateOne(container: container, item: item)
            }
        )

        let results = [
            PhaseResult(name: "FDBContext.init()", iterations: iterations, totalNanos: contextInit),
            PhaseResult(name: "FDBContext.insert()+rollback() reused", iterations: iterations, totalNanos: reusedInsertRollback),
            PhaseResult(name: "FDBContext.init()+insert()+rollback() fresh", iterations: iterations, totalNanos: freshInsertRollback),
            PhaseResult(name: "FDBContext.save() reused context", iterations: iterations, totalNanos: reusedContextUpdate),
            PhaseResult(name: "FDBContext.save() fresh context", iterations: iterations, totalNanos: freshContextUpdate),
        ]

        print("")
        print("  Phase                                      Avg (us)")
        print("  " + String(repeating: "-", count: 52))
        for r in results {
            print(r)
        }
        print("")

        print("  Inferred overheads")
        print("  " + String(repeating: "-", count: 52))
        printSignedDelta(
            name: "fresh setup - reused setup",
            deltaNanos: Int64(freshInsertRollback) - Int64(reusedInsertRollback),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh save - reused save",
            deltaNanos: Int64(freshContextUpdate) - Int64(reusedContextUpdate),
            iterations: iterations
        )
        print("")
    }

    // MARK: - Update Fixed-Iteration Profile

    static func runUpdateFixedIterationProfile(
        engine: any StorageEngine,
        container: DBContainer,
        iterations: Int = 500,
        rounds: Int = 3
    ) async throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: Update Hot Path Fixed Iteration (\(iterations) iterations, median of \(rounds) rounds)")
        print(String(repeating: "=", count: 70))

        try await RawKV.cleanup(engine: engine)
        try await FrameworkPostgreSQL.cleanup(container: container)

        let layout = try await benchmarkStorageLayout(container: container)
        let seedCount = 1024
        let reusedStore = try await container.store(for: BenchmarkItem.self)

        let rawUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await RawKV.cleanup(engine: engine)
                let ids = try await RawKV.seedData(
                    engine: engine,
                    count: seedCount,
                    idPrefix: "update-fixed-raw-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await RawKV.updateOne(engine: engine, id: state.pool.next())
            }
        )
        let layoutUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await seedFrameworkLayoutStorageData(
                    engine: engine,
                    layout: layout,
                    count: seedCount,
                    idPrefix: "update-fixed-layout-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await frameworkLayoutStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: state.pool.next(),
                    isNewRecord: false
                )
            }
        )
        let dataStoreUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-fixed-ds-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Updated Stable"
                item.age = 42
                item.score = 91.25
                try await reusedStore.executeBatch(inserts: [item], deletes: [])
            }
        )
        let reusedContextUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-fixed-reused-r\(round)"
                )
                return ContextRoundState(
                    pool: IterationIDPool(ids: ids),
                    context: FDBContext(container: container)
                )
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Updated Stable"
                item.age = 42
                item.score = 91.25
                state.context.insert(item)
                try await state.context.save()
            }
        )
        let freshContextUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await FrameworkPostgreSQL.cleanup(container: container)
                let ids = try await FrameworkPostgreSQL.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-fixed-fresh-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Updated Stable"
                item.age = 42
                item.score = 91.25
                try await FrameworkPostgreSQL.updateOne(container: container, item: item)
            }
        )

        let results = [
            PhaseResult(name: "Raw KV update", iterations: iterations, totalNanos: rawUpdate),
            PhaseResult(name: "Framework layout + storage update", iterations: iterations, totalNanos: layoutUpdate),
            PhaseResult(name: "Generic DataStore batch path", iterations: iterations, totalNanos: dataStoreUpdate),
            PhaseResult(name: "FDBContext.save() reused context", iterations: iterations, totalNanos: reusedContextUpdate),
            PhaseResult(name: "FDBContext.save() fresh context", iterations: iterations, totalNanos: freshContextUpdate),
        ]

        print("")
        print("  Phase                                      Avg (us)")
        print("  " + String(repeating: "-", count: 52))
        for r in results {
            print(r)
        }
        print("")

        print("  Inferred overheads")
        print("  " + String(repeating: "-", count: 52))
        printSignedDelta(
            name: "generic-vs-layout storage update",
            deltaNanos: Int64(dataStoreUpdate) - Int64(layoutUpdate),
            iterations: iterations
        )
        printSignedDelta(
            name: "product-vs-generic update",
            deltaNanos: Int64(reusedContextUpdate) - Int64(dataStoreUpdate),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh-vs-reused update",
            deltaNanos: Int64(freshContextUpdate) - Int64(reusedContextUpdate),
            iterations: iterations
        )
        print("")
    }

    // MARK: - StorageKit Direct Operations

    /// Layer 1 write helper: ad hoc key and opaque bytes only.
    static func rawAdHocWrite(engine: any StorageEngine, id: String) async throws {
        let key = rawAdHocKeyBytes(id: id)
        let value: [UInt8] = Array(repeating: 0x42, count: 70)
        try await engine.withAutoCommit { tx in
            tx.setValue(value, for: key)
        }
    }

    /// Layer 2 write helper: framework layout parity + DataAccess + ItemStorage.
    static func frameworkLayoutStorageWrite(
        engine: any StorageEngine,
        layout: BenchmarkStorageLayout,
        id: String,
        isNewRecord: Bool
    ) async throws {
        var item = BenchmarkItem()
        item.id = id
        item.name = "Alice"
        item.age = 30
        item.score = 85.5

        let data = try DataAccess.serialize(item)
        let key = benchmarkKeyBytes(layout: layout, id: id)

        try await engine.withAutoCommit { tx in
            let storage = ItemStorage(
                transaction: tx,
                blobsSubspace: layout.blobsSubspace
            )
            try await storage.write(data, for: key, isNewRecord: isNewRecord)
        }
    }

    /// Layer 1 delete helper: ad hoc key delete.
    static func rawAdHocDelete(engine: any StorageEngine, id: String) async throws {
        let key = rawAdHocKeyBytes(id: id)
        try await engine.withAutoCommit { tx in
            tx.clear(key: key)
        }
    }

    /// Layer 2 delete helper: framework layout parity + ItemStorage delete.
    static func frameworkLayoutStorageDelete(
        engine: any StorageEngine,
        layout: BenchmarkStorageLayout,
        id: String,
        skipBlobCleanup: Bool
    ) async throws {
        let key = benchmarkKeyBytes(layout: layout, id: id)
        try await engine.withAutoCommit { tx in
            let storage = ItemStorage(
                transaction: tx,
                blobsSubspace: layout.blobsSubspace
            )
            try await storage.delete(for: key, skipBlobCleanup: skipBlobCleanup)
        }
    }

    /// Layer 1 read helper: framework key only, no envelope decode or ItemStorage path.
    static func rawFrameworkKeyRead(
        engine: any StorageEngine,
        layout: BenchmarkStorageLayout,
        id: String
    ) async throws {
        let key = benchmarkKeyBytes(layout: layout, id: id)
        try await engine.withAutoCommit { tx in
            _ = try await tx.getValue(for: key, snapshot: false)
        }
    }

    // MARK: - Helpers

    @discardableResult
    static func frameworkLayoutStorageDecodedRead(
        engine: any StorageEngine,
        layout: BenchmarkStorageLayout,
        id: String
    ) async throws -> BenchmarkItem? {
        let key = benchmarkKeyBytes(layout: layout, id: id)
        return try await engine.withAutoCommit { tx in
            let storage = ItemStorage(
                transaction: tx,
                blobsSubspace: layout.blobsSubspace
            )
            guard let data = try await storage.read(for: key, snapshot: false) else {
                return nil
            }
            return try DataAccess.deserialize(data)
        }
    }

    @discardableResult
    static func seedFrameworkLayoutStorageData(
        engine: any StorageEngine,
        layout: BenchmarkStorageLayout,
        count: Int,
        idPrefix: String = "parity"
    ) async throws -> [String] {
        var ids: [String] = []
        ids.reserveCapacity(count)
        let batchSize = 100

        for batchStart in stride(from: 0, to: count, by: batchSize) {
            let end = min(batchStart + batchSize, count)
            try await engine.withTransaction { tx in
                let storage = ItemStorage(
                    transaction: tx,
                    blobsSubspace: layout.blobsSubspace
                )
                for i in batchStart..<end {
                    let id = "\(idPrefix)-\(String(format: "%06d", i))"
                    ids.append(id)

                    var item = BenchmarkItem()
                    item.id = id
                    item.name = "User \(i)"
                    item.age = 20 + (i % 60)
                    item.score = Double(50 + (i % 50))

                    let data = try DataAccess.serialize(item)
                    try await storage.write(
                        data,
                        for: benchmarkKeyBytes(layout: layout, id: id),
                        isNewRecord: true
                    )
                }
            }
        }

        return ids
    }

    /// Canonical framework layout helper for benchmark parity checks.
    static func benchmarkStorageLayout(container: DBContainer) async throws -> BenchmarkStorageLayout {
        let subspace = try await container.resolveDirectory(for: BenchmarkItem.self)
        return BenchmarkStorageLayout(
            itemSubspace: subspace.subspace(SubspaceKey.items).subspace(BenchmarkItem.persistableType),
            blobsSubspace: subspace.subspace(SubspaceKey.blobs)
        )
    }

    /// Canonical framework item key helper for benchmark parity checks.
    static func benchmarkKeyBytes(layout: BenchmarkStorageLayout, id: String) -> [UInt8] {
        layout.frameworkItemKey(id: id)
    }

    static func rawAdHocKeyBytes(id: String) -> [UInt8] {
        Array("benchmark/items/\(id)".utf8)
    }

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

    private static func measurePhaseMedian(
        iterations: Int,
        rounds: Int,
        clock: ContinuousClock,
        operation: () throws -> Void
    ) throws -> UInt64 {
        var samples: [UInt64] = []
        samples.reserveCapacity(max(1, rounds))
        for _ in 0..<max(1, rounds) {
            samples.append(try measurePhase(iterations: iterations, clock: clock, operation: operation))
        }
        return median(samples)
    }

    private static func measureAsyncPhase(
        iterations: Int,
        operation: @Sendable () async throws -> Void
    ) async throws -> UInt64 {
        for _ in 0..<20 {
            try await operation()
        }

        let start = DispatchTime.now().uptimeNanoseconds
        for _ in 0..<iterations {
            try await operation()
        }
        let end = DispatchTime.now().uptimeNanoseconds
        return end - start
    }

    private static func measureAsyncPhaseMedian(
        iterations: Int,
        rounds: Int,
        operation: @Sendable () async throws -> Void
    ) async throws -> UInt64 {
        var samples: [UInt64] = []
        samples.reserveCapacity(max(1, rounds))
        for _ in 0..<max(1, rounds) {
            samples.append(try await measureAsyncPhase(iterations: iterations, operation: operation))
        }
        return median(samples)
    }

    private static func measureAsyncPhaseMedianWithSetup<State: Sendable>(
        iterations: Int,
        rounds: Int,
        setup: @Sendable (Int) async throws -> State,
        operation: @Sendable (State) async throws -> Void
    ) async throws -> UInt64 {
        var samples: [UInt64] = []
        samples.reserveCapacity(max(1, rounds))
        for round in 0..<max(1, rounds) {
            let state = try await setup(round)
            samples.append(try await measureAsyncPhase(iterations: iterations) {
                try await operation(state)
            })
        }
        return median(samples)
    }

    private static func median(_ values: [UInt64]) -> UInt64 {
        guard !values.isEmpty else {
            return 0
        }

        let sorted = values.sorted()
        let middle = sorted.count / 2
        if sorted.count.isMultiple(of: 2) {
            return (sorted[middle - 1] + sorted[middle]) / 2
        }
        return sorted[middle]
    }

    private static func printGenericDeltaAnalysis(_ result: StrategyComparisonResult) {
        let strategies = result.strategies
        guard strategies.count >= 2 else { return }
        let nameWidth = max(25, strategies.map(\.name.count).max() ?? 0)

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
            let from = prev.name.padding(toLength: nameWidth, withPad: " ", startingAt: 0)
            let to = curr.name.padding(toLength: nameWidth, withPad: " ", startingAt: 0)
            print("  \(from) → \(to)  \(String(format: "%+.2f", delta))ms \(pct)")
        }

        // Total overhead
        let base = strategies[0].metrics.latency.p50
        let full = strategies[strategies.count - 1].metrics.latency.p50
        let totalDelta = full - base
        print("\n  L1 → last total: \(String(format: "%.2f", totalDelta))ms (\(String(format: "%.1f", full / base))x)")
        print("")
    }

    private static func printThreeLayerDeltaAnalysis(_ result: StrategyComparisonResult) {
        let strategies = result.strategies
        guard strategies.count >= 3 else {
            printGenericDeltaAnalysis(result)
            return
        }

        let l1 = strategies[0]
        let l2 = strategies[1]
        let l3 = strategies[2]

        print("  Delta Analysis:")
        print("  " + String(repeating: "-", count: 52))
        printLayerDelta(
            label: "L1 → L2",
            description: BenchmarkLayerContract.l1ToL2Description,
            from: l1,
            to: l2
        )
        printLayerDelta(
            label: "L2 → L3",
            description: BenchmarkLayerContract.readL2ToL3Description,
            from: l2,
            to: l3
        )

        let abstractionDelta = l3.metrics.latency.p50 - l2.metrics.latency.p50
        let totalDelta = l3.metrics.latency.p50 - l1.metrics.latency.p50
        print("")
        print("  L2 → L3 abstraction overhead: \(String(format: "%+.2f", abstractionDelta))ms")
        print("  L1 → L3 total: \(String(format: "%+.2f", totalDelta))ms (\(String(format: "%.1f", l3.metrics.latency.p50 / l1.metrics.latency.p50))x)")
        print("")
    }

    private static func printStorageAndContextDeltaAnalysis(
        _ result: StrategyComparisonResult,
        strictBaselineName: String? = nil,
        storageBaselineName: String,
        dataStoreName: String,
        contextName: String,
        storageDescription: String = BenchmarkLayerContract.readL2ToL3Description,
        contextDescription: String = BenchmarkLayerContract.readL3ToL4Description
    ) {
        guard
            let storageBaseline = result.strategies.first(where: { $0.name == storageBaselineName }),
            let dataStore = result.strategies.first(where: { $0.name == dataStoreName }),
            let context = result.strategies.first(where: { $0.name == contextName })
        else {
            printGenericDeltaAnalysis(result)
            return
        }

        if let strictBaselineName,
           let strictBaseline = result.strategies.first(where: { $0.name == strictBaselineName }) {
            print("  Strict Gap")
            print("  " + String(repeating: "-", count: 52))
            printLayerDelta(
                label: "\(strictBaselineName) → \(contextName)",
                description: "product-level delta",
                from: strictBaseline,
                to: context
            )
            print("")
        }

        print("  Storage Overhead")
        print("  " + String(repeating: "-", count: 52))
        printLayerDelta(
            label: "\(storageBaselineName) → \(dataStoreName)",
            description: storageDescription,
            from: storageBaseline,
            to: dataStore
        )
        print("")

        print("  Context Overhead")
        print("  " + String(repeating: "-", count: 52))
        printLayerDelta(
            label: "\(dataStoreName) → \(contextName)",
            description: contextDescription,
            from: dataStore,
            to: context
        )
        print("")
    }

    private static func printWriteLayerDeltaAnalysis(
        _ result: StrategyComparisonResult,
        strictBaselineName: String,
        storageBaselineName: String,
        genericPathName: String,
        productPathName: String
    ) {
        guard
            let strictBaseline = result.strategies.first(where: { $0.name == strictBaselineName }),
            let storageBaseline = result.strategies.first(where: { $0.name == storageBaselineName }),
            let genericPath = result.strategies.first(where: { $0.name == genericPathName }),
            let productPath = result.strategies.first(where: { $0.name == productPathName })
        else {
            printGenericDeltaAnalysis(result)
            return
        }

        print("  Delta Analysis")
        print("  " + String(repeating: "-", count: 52))
        printLayerDelta(
            label: "\(strictBaselineName) → \(storageBaselineName)",
            description: BenchmarkLayerContract.l1ToL2Description,
            from: strictBaseline,
            to: storageBaseline
        )
        printLayerDelta(
            label: "\(storageBaselineName) → \(genericPathName)",
            description: BenchmarkLayerContract.writeL2ToL3Description,
            from: storageBaseline,
            to: genericPath
        )
        printLayerDelta(
            label: "\(genericPathName) → \(productPathName)",
            description: BenchmarkLayerContract.writeL3ToL4Description,
            from: genericPath,
            to: productPath
        )
        print("")
        print("  \(strictBaselineName) → \(productPathName) product-level strict gap: \(String(format: "%+.2f", productPath.metrics.latency.p50 - strictBaseline.metrics.latency.p50))ms")
        print("")
    }

    private static func printParityTargetAssessment(
        title: String,
        result: StrategyComparisonResult,
        fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
        storageBaselineName: String,
        dataStoreName: String,
        contextName: String,
        targetThroughputOverheadPct: Double = 10.0,
        targetFixedDeltaMicros: Double = 20.0,
        tolerance: Double = 0.05
    ) {
        print("  \(title)")
        print("  " + String(repeating: "-", count: 52))
        printTargetAssessmentSection(
            heading: "Storage Parity Summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: storageBaselineName,
            candidateName: dataStoreName,
            targetThroughputOverheadPct: targetThroughputOverheadPct,
            targetFixedDeltaMicros: targetFixedDeltaMicros,
            tolerance: tolerance
        )
        printTargetAssessmentSection(
            heading: "Context Parity Summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: dataStoreName,
            candidateName: contextName,
            targetThroughputOverheadPct: targetThroughputOverheadPct,
            targetFixedDeltaMicros: targetFixedDeltaMicros,
            tolerance: tolerance
        )
    }

    private static func printWriteProductTargetAssessment(
        title: String,
        result: StrategyComparisonResult,
        fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
        productBaselineName: String,
        storageBaselineName: String,
        genericPathName: String,
        productPathName: String,
        targetThroughputOverheadPct: Double = 10.0,
        targetFixedDeltaMicros: Double = 20.0,
        tolerance: Double = 0.05
    ) {
        print("  \(title)")
        print("  " + String(repeating: "-", count: 52))
        printTargetAssessmentSection(
            heading: BenchmarkLayerContract.productParitySummary,
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: productBaselineName,
            candidateName: productPathName,
            targetThroughputOverheadPct: targetThroughputOverheadPct,
            targetFixedDeltaMicros: targetFixedDeltaMicros,
            tolerance: tolerance
        )
        printWriteDiagnosticAssessmentSection(
            heading: BenchmarkLayerContract.diagnosticBreakdown,
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: storageBaselineName,
            candidateName: genericPathName
        )
        printWriteDiagnosticAssessmentSection(
            heading: "Product fast-path diagnostic",
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: genericPathName,
            candidateName: productPathName,
            expectedFastPathWin: true
        )
    }

    private static func printWriteDiagnosticAssessmentSection(
        heading: String,
        result: StrategyComparisonResult,
        fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
        baselineName: String,
        candidateName: String,
        expectedFastPathWin: Bool = false
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

        let throughputDelta = ((baselineThroughput - candidateThroughput) / baselineThroughput) * 100
        let fixedDelta = fixedCandidate.averageMicros - fixedBaseline.averageMicros

        print("  \(heading)")
        print("  " + String(repeating: "-", count: 52))
        print("  \(baselineName) -> \(candidateName) throughput delta: \(formatWriteDiagnosticLine(delta: throughputDelta, unit: "%", expectedFastPathWin: expectedFastPathWin))")
        print("  \(baselineName) -> \(candidateName) fixed delta: \(formatWriteDiagnosticLine(delta: fixedDelta, unit: " us/op", expectedFastPathWin: expectedFastPathWin))")
        print("")
    }

    private static func printTargetAssessmentSection(
        heading: String,
        result: StrategyComparisonResult,
        fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
        baselineName: String,
        candidateName: String,
        targetThroughputOverheadPct: Double = 10.0,
        targetFixedDeltaMicros: Double = 20.0,
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
        print(
            "  \(baselineName) -> \(candidateName) throughput target <= \(Int(targetThroughputOverheadPct))%: \(formatTargetLine(delta: throughputOverheadPct, unit: "%", tolerance: targetThroughputOverheadPct + tolerance))"
        )
        print(
            "  \(baselineName) -> \(candidateName) fixed target <= \(Int(targetFixedDeltaMicros)) us/op: \(formatTargetLine(delta: fixedDelta, unit: " us/op", tolerance: targetFixedDeltaMicros + tolerance))"
        )
        print("")
    }

    private static func formatTargetLine(
        delta: Double,
        unit: String,
        tolerance: Double
    ) -> String {
        if delta <= 0 {
            return "faster by \(String(format: "%.2f", abs(delta)))\(unit) [PASS]"
        }
        return "actual \(String(format: "%.2f", delta))\(unit) [\(delta <= tolerance ? "PASS" : "MISS")]"
    }

    private static func formatWriteDiagnosticLine(
        delta: Double,
        unit: String,
        expectedFastPathWin: Bool
    ) -> String {
        if delta < 0 {
            let suffix = expectedFastPathWin ? " (expected product fast-path win)" : ""
            return "faster by \(String(format: "%.2f", abs(delta)))\(unit)\(suffix)"
        }
        return "slower by \(String(format: "%.2f", delta))\(unit)"
    }

    private static func printLayerDelta(
        label: String,
        description: String,
        from: ScenarioResult,
        to: ScenarioResult
    ) {
        let delta = to.metrics.latency.p50 - from.metrics.latency.p50
        let pct: String
        if from.metrics.latency.p50 > 0 {
            pct = String(format: "(%+.0f%%)", (delta / from.metrics.latency.p50) * 100)
        } else {
            pct = ""
        }
        print("  \(label) (\(description))  \(String(format: "%+.2f", delta))ms \(pct)")
    }

    private static func printSignedDelta(
        name: String,
        deltaNanos: Int64,
        iterations: Int
    ) {
        let avgMicros = Double(deltaNanos) / Double(iterations) / 1000.0
        let padded = name.padding(toLength: max(40, name.count), withPad: " ", startingAt: 0)
        print("  \(padded) \(String(format: "%+8.1f", avgMicros)) us")
    }
}
