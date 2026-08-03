import Foundation
import BenchmarkFramework
import StorageKit
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import Logging
import Synchronization

// FIXME(INCOMPLETE_IMPLEMENTATION): This target does not compile against
// database-framework >= 26.0803.x. Layer profiling reaches
// `container.store(for:)` (now package-scoped), the removed
// `withAutoCommit`, and the renamed `fetchByIDInTransaction`. There is no
// production call path; this is a development-only benchmark. Success
// requires porting the L3 phases onto a public BenchmarkFramework probe and
// the current transaction API, then re-validating the profile phase labels
// against BenchmarkLayerContract.

private let logger = Logger(label: "benchmark.profile")

// MARK: - Phase Timing

/// Measures the observable phases of canonical record operations.
///
/// The insert call chain:
/// ```
/// DatabaseContext.init → insert() → save()
///   → TransactionRunner → StorageEngine.withAutoCommit
///     → DatabaseRecordStorageCodec.encode()
///     → ItemEnvelope.serialize()
///     → transaction.setValue()
///     → IndexMaintenanceService.updateIndexes()
///   → commit()
/// ```
///
/// This benchmark isolates each responsibility to identify its measured cost:
///
/// ```
/// Layer 1: Direct storage mutation                          - storage baseline
/// Layer 2: Canonical record storage                         - encoding and persisted layout
/// Layer 3: DataStore batch mutation                         - typed record storage API
/// Layer 4: Database record mutation                         - application-facing record API
/// ```
///
/// All layers share the same StorageEngine and connection pool.
enum ProfileBenchmark {
    private enum PhaseValidationError: Error {
        case expectedInlineEnvelope
        case checksumMismatch
    }

    // ItemChecksum is intentionally internal to DatabaseEngine. Keep this
    // benchmark-only table aligned with the canonical CRC32C implementation so
    // the measured CPU path includes the same checksum work as ItemStorage.
    private static let crc32cTable: [UInt32] = (0..<256).map { value in
        var crc = UInt32(value)
        for _ in 0..<8 {
            crc = (crc & 1) == 0
                ? crc >> 1
                : (crc >> 1) ^ 0x82F6_3B78
        }
        return crc
    }

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
        let context: DatabaseContext
    }

    private struct DataStoreRoundState: Sendable {
        let store: any DataStore
    }

    private struct DataStorePoolRoundState: Sendable {
        let store: any DataStore
        let pool: IterationIDPool
    }

    private struct DeleteContextRoundState: Sendable {
        let insertContext: DatabaseContext
        let deleteContext: DatabaseContext
    }

    struct BenchmarkStorageLayout: Sendable {
        let itemSubspace: Subspace
        let blobsSubspace: Subspace
        let itemStorageFactory: ItemStorageFactory

        func canonicalItemKey(id: String) -> ByteString {
            itemSubspace.pack(Tuple([id]))
        }
    }

    struct PhaseResult: CustomStringConvertible {
        let name: String
        let iterations: Int
        let totalNanoseconds: UInt64
        var averageMicroseconds: Double { Double(totalNanoseconds) / Double(iterations) / 1000.0 }

        var description: String {
            let padded = name.padding(toLength: max(40, name.count), withPad: " ", startingAt: 0)
            return "  \(padded) \(String(format: "%8.1f", averageMicroseconds)) us"
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
        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)
        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)

        let strategies: [Strategy] = [
            (BenchmarkLayerContract.directStorageMutation, {
                let id = UUID().uuidString
                try await adHocStorageWrite(engine: engine, id: id)
            }),
            (BenchmarkLayerContract.canonicalRecordStorage, {
                let id = UUID().uuidString
                try await canonicalRecordStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: id
                )
            }),
            (BenchmarkLayerContract.dataStoreBatchMutation, {
                var item = BenchmarkItem()
                item.name = "Alice"
                item.age = 30
                item.score = 85.5
                try await reusedStore.executeBatch(inserts: [item], deletes: [])
            }),
            (BenchmarkLayerContract.databaseRecordMutation, {
                var item = BenchmarkItem()
                item.name = "Alice"
                item.age = 30
                item.score = 85.5
                try await DatabaseRecordWorkload.insertOne(container: container, item: item)
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
        printWriteWorkloadDeltaAnalysis(
            result,
            directStorageName: BenchmarkLayerContract.directStorageMutation,
            canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorage,
            dataStoreMutationName: BenchmarkLayerContract.dataStoreBatchMutation,
            databaseRecordMutationName: BenchmarkLayerContract.databaseRecordMutation
        )
        printDatabaseRecordMutationTargetAssessment(
            title: "Insert Database Record Parity Summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            directStorageName: BenchmarkLayerContract.directStorageMutation,
            canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorage,
            dataStoreMutationName: BenchmarkLayerContract.dataStoreBatchMutation,
            databaseRecordMutationName: BenchmarkLayerContract.databaseRecordMutation
        )
    }

    // MARK: - Phase Breakdown (CPU-only, no I/O)

    static func runPhaseBreakdown(iterations: Int = 10000) throws {
        print("")
        print(String(repeating: "=", count: 70))
        print("PROFILE: CPU Phase Breakdown (no I/O, \(iterations) iterations)")
        print(String(repeating: "=", count: 70))

        let clock = ContinuousClock()

        // Phase 1: canonical DBRC serialization.
        var item = BenchmarkItem()
        item.name = "Alice"
        item.age = 30
        item.score = 85.5

        let recordEncode = try measurePhase(iterations: iterations, clock: clock) {
            _ = try DatabaseRecordStorageCodec.encode(item)
        }

        let sampleBytes = try DatabaseRecordStorageCodec.encode(item)

        // Phase 2: checksum and canonical inline envelope serialization.
        let envelopeEncode = try measurePhase(iterations: iterations, clock: clock) {
            let checksum = crc32c(sampleBytes)
            let envelope = try ItemEnvelope.inline(
                payload: sampleBytes,
                encoding: .identity,
                plainByteCount: UInt64(sampleBytes.count),
                checksum: checksum
            )
            _ = envelope.serialize()
        }

        let sampleChecksum = crc32c(sampleBytes)
        let sampleEnvelope = try ItemEnvelope.inline(
            payload: sampleBytes,
            encoding: .identity,
            plainByteCount: UInt64(sampleBytes.count),
            checksum: sampleChecksum
        ).serialize()

        // Phase 3: strict envelope parsing, owner-retaining payload view, and
        // checksum verification. No Data or Array bridge is materialized.
        let envelopeDecode = try measurePhase(iterations: iterations, clock: clock) {
            let envelope = try ItemEnvelope.deserialize(sampleEnvelope)
            guard case .inline(let payload) = envelope.content else {
                throw PhaseValidationError.expectedInlineEnvelope
            }
            guard crc32c(payload) == envelope.checksum else {
                throw PhaseValidationError.checksumMismatch
            }
            _ = payload.count
        }

        // Phase 4: canonical DBRC decode and model materialization.
        let recordDecode = try measurePhase(iterations: iterations, clock: clock) {
            let decoded = try DatabaseRecordStorageCodec.decode(
                BenchmarkItem.self,
                from: sampleBytes
            )
            _ = decoded.id
        }

        // Print results
        let results = [
            PhaseResult(name: "DBRC encode", iterations: iterations, totalNanoseconds: recordEncode),
            PhaseResult(name: "CRC32C + inline envelope encode", iterations: iterations, totalNanoseconds: envelopeEncode),
            PhaseResult(name: "Envelope decode/view + CRC32C", iterations: iterations, totalNanoseconds: envelopeDecode),
            PhaseResult(name: "DBRC decode + materialize", iterations: iterations, totalNanoseconds: recordDecode),
        ]

        print("")
        print("  Phase                                      Avg (us)")
        print("  " + String(repeating: "-", count: 52))
        for r in results {
            print(r)
        }
        print("")
    }

    private static func crc32c(_ bytes: ByteString) -> UInt32 {
        bytes.withUnsafeBytes { source in
            var crc = UInt32.max
            for byte in source {
                let index = Int(UInt8(truncatingIfNeeded: crc) ^ byte)
                crc = crc32cTable[index] ^ (crc >> 8)
            }
            return ~crc
        }
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
        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)

        let databaseRecordID = "read-profile-database-record"
        let canonicalStorageID = "read-profile-canonical-storage"
        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)

        // Seed the application-facing record API.
        var databaseRecordItem = BenchmarkItem()
        databaseRecordItem.id = databaseRecordID
        databaseRecordItem.name = "Alice"
        databaseRecordItem.age = 30
        databaseRecordItem.score = 85.5
        try await DatabaseRecordWorkload.insertOne(container: container, item: databaseRecordItem)

        // Seed the canonical record storage representation.
        try await canonicalRecordStorageWrite(
            engine: engine,
            layout: layout,
            id: canonicalStorageID
        )

        let strategies: [Strategy] = [
            (BenchmarkLayerContract.canonicalKeyPresenceRead, {
                try await canonicalKeyStorageRead(engine: engine, layout: layout, id: canonicalStorageID)
            }),
            (BenchmarkLayerContract.canonicalRecordStorage, {
                _ = try await canonicalRecordStorageRead(engine: engine, layout: layout, id: canonicalStorageID)
            }),
            (BenchmarkLayerContract.dataStoreRecordRead, {
                _ = try await reusedStore.fetch(BenchmarkItem.self, id: databaseRecordID)
            }),
            (BenchmarkLayerContract.databaseRecordQueryAPI, {
                _ = try await DatabaseRecordWorkload.readOne(container: container, id: databaseRecordID)
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
            directStorageName: BenchmarkLayerContract.canonicalKeyPresenceRead,
            canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorage,
            dataStoreName: BenchmarkLayerContract.dataStoreRecordRead,
            contextName: BenchmarkLayerContract.databaseRecordQueryAPI,
            storageDescription: BenchmarkLayerContract.dataStoreReadTransitionDescription,
            contextDescription: BenchmarkLayerContract.contextReadTransitionDescription
        )
        printParityTargetAssessment(
            title: "Point Read Parity Summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorage,
            dataStoreName: BenchmarkLayerContract.dataStoreRecordRead,
            contextName: BenchmarkLayerContract.databaseRecordQueryAPI
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

        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)

        let readID = "read-lifecycle"
        var item = BenchmarkItem()
        item.id = readID
        item.name = "Alice"
        item.age = 30
        item.score = 85.5
        try await DatabaseRecordWorkload.insertOne(container: container, item: item)

        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)
        let reusedContext = DatabaseContext(container: container)

        let strategies: [Strategy] = [
            (BenchmarkLayerContract.canonicalRecordStorage, {
                _ = try await canonicalRecordStorageRead(engine: engine, layout: layout, id: readID)
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
            (BenchmarkLayerContract.dataStoreRecordRead, {
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
            (BenchmarkLayerContract.reusedContextRecordRead, {
                _ = try await reusedContext.withTransaction { transaction in
                    try await transaction.fetch(
                        BenchmarkItem.self,
                        identifiedBy: readID
                    )
                }
            }),
            (BenchmarkLayerContract.freshContextRecordRead, {
                _ = try await DatabaseRecordWorkload.readOne(container: container, id: readID)
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
            canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorage,
            dataStoreName: BenchmarkLayerContract.dataStoreRecordRead,
            contextName: BenchmarkLayerContract.freshContextRecordRead
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

        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)

        let readID = "read-fixed"
        var item = BenchmarkItem()
        item.id = readID
        item.name = "Alice"
        item.age = 30
        item.score = 85.5
        try await DatabaseRecordWorkload.insertOne(container: container, item: item)

        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)
        let reusedContext = DatabaseContext(container: container)
        let clock = ContinuousClock()

        let contextInit = try measurePhase(iterations: iterations, clock: clock) {
            _ = DatabaseContext(container: container)
        }
        let storeInit = try await measureAsyncPhase(iterations: iterations) {
            _ = try await container.store(for: BenchmarkItem.self)
        }
        let canonicalStorageDecode = try await measureAsyncPhase(iterations: iterations) {
            _ = try await canonicalRecordStorageRead(engine: engine, layout: layout, id: readID)
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
            _ = try await reusedContext.withTransaction { transaction in
                try await transaction.fetch(
                    BenchmarkItem.self,
                    identifiedBy: readID
                )
            }
        }
        let freshContextRead = try await measureAsyncPhase(iterations: iterations) {
            _ = try await DatabaseRecordWorkload.readOne(container: container, id: readID)
        }

        let results = [
            PhaseResult(name: "DatabaseContext.init()", iterations: iterations, totalNanoseconds: contextInit),
            PhaseResult(name: "DBContainer.store(for:)", iterations: iterations, totalNanoseconds: storeInit),
            PhaseResult(name: "Canonical record storage", iterations: iterations, totalNanoseconds: canonicalStorageDecode),
            PhaseResult(name: "DataStore.fetchById + autoCommit", iterations: iterations, totalNanoseconds: dataStoreAutoCommit),
            PhaseResult(name: "DataStore.fetch()", iterations: iterations, totalNanoseconds: dataStoreFetch),
            PhaseResult(name: "DatabaseContext.fetch() reused context", iterations: iterations, totalNanoseconds: reusedContextRead),
            PhaseResult(name: "DatabaseContext.fetch() fresh context", iterations: iterations, totalNanoseconds: freshContextRead),
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
            deltaNanoseconds: Int64(dataStoreFetch) - Int64(dataStoreAutoCommit),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh context - reused context",
            deltaNanoseconds: Int64(freshContextRead) - Int64(reusedContextRead),
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
            (BenchmarkLayerContract.directStorageMutation, {
                let id = UUID().uuidString
                try await adHocStorageWrite(engine: engine, id: id)
                try await adHocStorageDelete(engine: engine, id: id)
            }),
            (BenchmarkLayerContract.canonicalRecordStorage, {
                let id = UUID().uuidString
                try await canonicalRecordStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: id
                )
                try await canonicalRecordStorageDelete(
                    engine: engine,
                    layout: layout,
                    id: id
                )
            }),
            (BenchmarkLayerContract.dataStoreBatchMutation, {
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await reusedStore.executeBatch(inserts: [item], deletes: [])
                try await reusedStore.executeBatch(inserts: [], deletes: [item])
            }),
            (BenchmarkLayerContract.databaseRecordMutation, {
                let id = UUID().uuidString
                var item = BenchmarkItem()
                item.id = id
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await DatabaseRecordWorkload.insertOne(container: container, item: item)
                try await DatabaseRecordWorkload.deleteOne(container: container, id: id)
            }),
        ]
        let result = try await runner.compareStrategies(
            name: "Insert+Delete: Layer-by-Layer",
            strategies: strategies
        )
        ConsoleReporter.print(result)
        let fixedMeasurements = try await measureDeleteWorkloadFixedSummaries(
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
            directStorageName: BenchmarkLayerContract.directStorageMutation,
            canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorage,
            dataStoreName: BenchmarkLayerContract.dataStoreBatchMutation,
            contextName: BenchmarkLayerContract.databaseRecordMutation,
            storageDescription: BenchmarkLayerContract.dataStoreBatchTransitionDescription,
            contextDescription: BenchmarkLayerContract.databaseRecordMutationTransitionDescription
        )
        printDatabaseRecordMutationTargetAssessment(
            title: "Insert+Delete Database Record Parity Summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            directStorageName: BenchmarkLayerContract.directStorageMutation,
            canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorage,
            dataStoreMutationName: BenchmarkLayerContract.dataStoreBatchMutation,
            databaseRecordMutationName: BenchmarkLayerContract.databaseRecordMutation
        )
    }

    private static func measureDeleteWorkloadFixedSummaries(
        engine: any StorageEngine,
        container: DBContainer,
        iterations: Int,
        rounds: Int
    ) async throws -> [FixedIterationReporter.MeasurementSummary] {
        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)

        let layout = try await benchmarkStorageLayout(container: container)

        let adHocStorageInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in PoolRoundState(pool: IterationIDPool(ids: [])) },
            operation: { _ in
                let id = UUID().uuidString
                try await adHocStorageWrite(engine: engine, id: id)
                try await adHocStorageDelete(engine: engine, id: id)
            }
        )
        let canonicalRecordInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in PoolRoundState(pool: IterationIDPool(ids: [])) },
            operation: { _ in
                let id = UUID().uuidString
                try await canonicalRecordStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: id
                )
                try await canonicalRecordStorageDelete(
                    engine: engine,
                    layout: layout,
                    id: id
                )
            }
        )
        let dataStoreInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await DatabaseRecordWorkload.cleanup(container: container)
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
        let databaseRecordInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await DatabaseRecordWorkload.cleanup(container: container)
                return DeleteContextRoundState(
                    insertContext: DatabaseContext(container: container),
                    deleteContext: DatabaseContext(container: container)
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
            .init(name: BenchmarkLayerContract.directStorageMutation, totalNanoseconds: adHocStorageInsertDelete / UInt64(iterations)),
            .init(name: BenchmarkLayerContract.canonicalRecordStorage, totalNanoseconds: canonicalRecordInsertDelete / UInt64(iterations)),
            .init(name: BenchmarkLayerContract.dataStoreBatchMutation, totalNanoseconds: dataStoreInsertDelete / UInt64(iterations)),
            .init(name: BenchmarkLayerContract.databaseRecordMutation, totalNanoseconds: databaseRecordInsertDelete / UInt64(iterations)),
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

        try await DatabaseRecordWorkload.cleanup(container: container)

        let reusedInsertContext = DatabaseContext(container: container)
        let reusedDeleteContext = DatabaseContext(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)
        let clock = ContinuousClock()
        let seedCount = max(1024, iterations + 32)
        var insertSetupCounter = 0
        var deleteSetupCounter = 0

        let contextInit = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            _ = DatabaseContext(container: container)
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
            let context = DatabaseContext(container: container)
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
            let context = DatabaseContext(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                return ContextRoundState(
                    pool: IterationIDPool(ids: []),
                    context: DatabaseContext(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                return PoolRoundState(pool: IterationIDPool(ids: []))
            },
            operation: { _ in
                var item = BenchmarkItem()
                item.id = UUID().uuidString
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await DatabaseRecordWorkload.insertOne(container: container, item: item)
            }
        )
        let reusedDeleteSave = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "delete-life-reused-r\(round)"
                )
                return ContextRoundState(
                    pool: IterationIDPool(ids: ids),
                    context: DatabaseContext(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "delete-life-fresh-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await DatabaseRecordWorkload.deleteOne(container: container, id: state.pool.next())
            }
        )

        let results = [
            PhaseResult(name: "DatabaseContext.init()", iterations: iterations, totalNanoseconds: contextInit),
            PhaseResult(name: "DatabaseContext.insert()+rollback() reused", iterations: iterations, totalNanoseconds: reusedInsertRollback),
            PhaseResult(name: "DatabaseContext.init()+insert()+rollback() fresh", iterations: iterations, totalNanoseconds: freshInsertRollback),
            PhaseResult(name: "DatabaseContext.delete()+rollback() reused", iterations: iterations, totalNanoseconds: reusedDeleteRollback),
            PhaseResult(name: "DatabaseContext.init()+delete()+rollback() fresh", iterations: iterations, totalNanoseconds: freshDeleteRollback),
            PhaseResult(name: "DataStore.executeBatch() insert reused store", iterations: iterations, totalNanoseconds: reusedStoreInsert),
            PhaseResult(name: "DataStore.executeBatch() insert with store lookup", iterations: iterations, totalNanoseconds: lookupStoreInsert),
            PhaseResult(name: "DatabaseContext.save() insert reused", iterations: iterations, totalNanoseconds: reusedInsertSave),
            PhaseResult(name: "DatabaseContext.save() insert fresh", iterations: iterations, totalNanoseconds: freshInsertSave),
            PhaseResult(name: "DataStore.executeBatch() delete reused store", iterations: iterations, totalNanoseconds: reusedStoreDelete),
            PhaseResult(name: "DataStore.executeBatch() delete with store lookup", iterations: iterations, totalNanoseconds: lookupStoreDelete),
            PhaseResult(name: "DatabaseContext.save() delete reused", iterations: iterations, totalNanoseconds: reusedDeleteSave),
            PhaseResult(name: "DatabaseContext.save() delete fresh", iterations: iterations, totalNanoseconds: freshDeleteSave),
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
            deltaNanoseconds: Int64(freshInsertRollback) - Int64(reusedInsertRollback),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh delete setup - reused",
            deltaNanoseconds: Int64(freshDeleteRollback) - Int64(reusedDeleteRollback),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh insert save - reused",
            deltaNanoseconds: Int64(freshInsertSave) - Int64(reusedInsertSave),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh delete save - reused",
            deltaNanoseconds: Int64(freshDeleteSave) - Int64(reusedDeleteSave),
            iterations: iterations
        )
        printSignedDelta(
            name: "store lookup insert - reused store",
            deltaNanoseconds: Int64(lookupStoreInsert) - Int64(reusedStoreInsert),
            iterations: iterations
        )
        printSignedDelta(
            name: "store lookup delete - reused store",
            deltaNanoseconds: Int64(lookupStoreDelete) - Int64(reusedStoreDelete),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh insert save - store lookup insert",
            deltaNanoseconds: Int64(freshInsertSave) - Int64(lookupStoreInsert),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh delete save - store lookup delete",
            deltaNanoseconds: Int64(freshDeleteSave) - Int64(lookupStoreDelete),
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

        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)

        let layout = try await benchmarkStorageLayout(container: container)
        let adHocStorageInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in PoolRoundState(pool: IterationIDPool(ids: [])) },
            operation: { _ in
                let id = UUID().uuidString
                try await adHocStorageWrite(engine: engine, id: id)
                try await adHocStorageDelete(engine: engine, id: id)
            }
        )

        let canonicalRecordInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in PoolRoundState(pool: IterationIDPool(ids: [])) },
            operation: { _ in
                let id = UUID().uuidString
                try await canonicalRecordStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: id
                )
                try await canonicalRecordStorageDelete(
                    engine: engine,
                    layout: layout,
                    id: id
                )
            }
        )

        let dataStoreInsertDelete = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { _ in
                try await DatabaseRecordWorkload.cleanup(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                return DeleteContextRoundState(
                    insertContext: DatabaseContext(container: container),
                    deleteContext: DatabaseContext(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                return PoolRoundState(pool: IterationIDPool(ids: []))
            },
            operation: { _ in
                let id = UUID().uuidString
                var item = BenchmarkItem()
                item.id = id
                item.name = "Temp"
                item.age = 30
                item.score = 50.0
                try await DatabaseRecordWorkload.insertOne(container: container, item: item)
                try await DatabaseRecordWorkload.deleteOne(container: container, id: id)
            }
        )

        let results = [
            PhaseResult(name: "Direct storage insert+delete", iterations: iterations, totalNanoseconds: adHocStorageInsertDelete),
            PhaseResult(name: "Canonical record storage insert+delete", iterations: iterations, totalNanoseconds: canonicalRecordInsertDelete),
            PhaseResult(name: "DataStore batch mutation insert+delete", iterations: iterations, totalNanoseconds: dataStoreInsertDelete),
            PhaseResult(name: "DatabaseContext.save() reused contexts", iterations: iterations, totalNanoseconds: reusedContextInsertDelete),
            PhaseResult(name: "DatabaseContext.save() fresh contexts", iterations: iterations, totalNanoseconds: freshContextInsertDelete),
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
            name: "data-store-vs-canonical-storage insert+delete",
            deltaNanoseconds: Int64(dataStoreInsertDelete) - Int64(canonicalRecordInsertDelete),
            iterations: iterations
        )
        printSignedDelta(
            name: "database-record-vs-data-store insert+delete",
            deltaNanoseconds: Int64(reusedContextInsertDelete) - Int64(dataStoreInsertDelete),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh-vs-reused insert+delete",
            deltaNanoseconds: Int64(freshContextInsertDelete) - Int64(reusedContextInsertDelete),
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

        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)

        let canonicalStorageID = "update-profile-canonical-storage"
        let databaseRecordID = "update-profile-database-record"
        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)

        try await canonicalRecordStorageWrite(
            engine: engine,
            layout: layout,
            id: canonicalStorageID
        )

        var databaseRecordItem = BenchmarkItem()
        databaseRecordItem.id = databaseRecordID
        databaseRecordItem.name = "Alice"
        databaseRecordItem.age = 30
        databaseRecordItem.score = 85.5
        try await DatabaseRecordWorkload.insertOne(container: container, item: databaseRecordItem)

        var updated = BenchmarkItem()
        updated.id = databaseRecordID
        updated.name = "Updated Stable"
        updated.age = 42
        updated.score = 91.25
        let updatedItem = updated

        let strategies: [Strategy] = [
            (BenchmarkLayerContract.directStorageMutation, {
                try await adHocStorageWrite(engine: engine, id: canonicalStorageID)
            }),
            (BenchmarkLayerContract.canonicalRecordStorage, {
                try await canonicalRecordStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: canonicalStorageID
                )
            }),
            (BenchmarkLayerContract.dataStoreBatchMutation, {
                try await reusedStore.executeBatch(inserts: [updatedItem], deletes: [])
            }),
            (BenchmarkLayerContract.databaseRecordMutation, {
                try await DatabaseRecordWorkload.updateOne(container: container, item: updatedItem)
            }),
        ]
        let result = try await runner.compareStrategies(
            name: "Update: Layer-by-Layer",
            strategies: strategies
        )
        ConsoleReporter.print(result)
        let fixedMeasurements = try await measureUpdateWorkloadFixedSummaries(
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
            directStorageName: BenchmarkLayerContract.directStorageMutation,
            canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorage,
            dataStoreName: BenchmarkLayerContract.dataStoreBatchMutation,
            contextName: BenchmarkLayerContract.databaseRecordMutation,
            storageDescription: BenchmarkLayerContract.dataStoreBatchTransitionDescription,
            contextDescription: BenchmarkLayerContract.databaseRecordMutationTransitionDescription
        )
        printDatabaseRecordMutationTargetAssessment(
            title: "Point Update Database Record Parity Summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            directStorageName: BenchmarkLayerContract.directStorageMutation,
            canonicalStorageName: BenchmarkLayerContract.canonicalRecordStorage,
            dataStoreMutationName: BenchmarkLayerContract.dataStoreBatchMutation,
            databaseRecordMutationName: BenchmarkLayerContract.databaseRecordMutation
        )
    }

    private static func measureUpdateWorkloadFixedSummaries(
        engine: any StorageEngine,
        container: DBContainer,
        iterations: Int,
        rounds: Int
    ) async throws -> [FixedIterationReporter.MeasurementSummary] {
        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)

        let layout = try await benchmarkStorageLayout(container: container)
        let reusedStore = try await container.store(for: BenchmarkItem.self)
        let seedCount = 1024

        let directStorageUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await DirectStorageWorkload.cleanup(engine: engine)
                let ids = try await DirectStorageWorkload.seedData(
                    engine: engine,
                    count: seedCount,
                    idPrefix: "update-profile-direct-storage-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await DirectStorageWorkload.updateOne(engine: engine, id: state.pool.next())
            }
        )
        let canonicalRecordUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await seedCanonicalRecordStorageData(
                    engine: engine,
                    layout: layout,
                    count: seedCount,
                    idPrefix: "update-profile-canonical-storage-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await canonicalRecordStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: state.pool.next()
                )
            }
        )
        let dataStoreUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
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
        let databaseRecordUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-profile-database-record-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                var item = BenchmarkItem()
                item.id = state.pool.next()
                item.name = "Updated Stable"
                item.age = 42
                item.score = 91.25
                try await DatabaseRecordWorkload.updateOne(container: container, item: item)
            }
        )

        let divisor = UInt64(iterations)
        return [
            .init(name: BenchmarkLayerContract.directStorageMutation, totalNanoseconds: directStorageUpdate / divisor),
            .init(name: BenchmarkLayerContract.canonicalRecordStorage, totalNanoseconds: canonicalRecordUpdate / divisor),
            .init(name: BenchmarkLayerContract.dataStoreBatchMutation, totalNanoseconds: dataStoreUpdate / divisor),
            .init(name: BenchmarkLayerContract.databaseRecordMutation, totalNanoseconds: databaseRecordUpdate / divisor),
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

        try await DatabaseRecordWorkload.cleanup(container: container)

        let reusedContext = DatabaseContext(container: container)
        let clock = ContinuousClock()
        let seedCount = 1024
        var reusedSetupCounter = 0
        var freshSetupCounter = 0

        let contextInit = try measurePhaseMedian(iterations: iterations, rounds: rounds, clock: clock) {
            _ = DatabaseContext(container: container)
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
            let context = DatabaseContext(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-life-reused-r\(round)"
                )
                return ContextRoundState(
                    pool: IterationIDPool(ids: ids),
                    context: DatabaseContext(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
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
                try await DatabaseRecordWorkload.updateOne(container: container, item: item)
            }
        )

        let results = [
            PhaseResult(name: "DatabaseContext.init()", iterations: iterations, totalNanoseconds: contextInit),
            PhaseResult(name: "DatabaseContext.insert()+rollback() reused", iterations: iterations, totalNanoseconds: reusedInsertRollback),
            PhaseResult(name: "DatabaseContext.init()+insert()+rollback() fresh", iterations: iterations, totalNanoseconds: freshInsertRollback),
            PhaseResult(name: "DatabaseContext.save() reused context", iterations: iterations, totalNanoseconds: reusedContextUpdate),
            PhaseResult(name: "DatabaseContext.save() fresh context", iterations: iterations, totalNanoseconds: freshContextUpdate),
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
            deltaNanoseconds: Int64(freshInsertRollback) - Int64(reusedInsertRollback),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh save - reused save",
            deltaNanoseconds: Int64(freshContextUpdate) - Int64(reusedContextUpdate),
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

        try await DirectStorageWorkload.cleanup(engine: engine)
        try await DatabaseRecordWorkload.cleanup(container: container)

        let layout = try await benchmarkStorageLayout(container: container)
        let seedCount = 1024
        let reusedStore = try await container.store(for: BenchmarkItem.self)

        let directStorageUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await DirectStorageWorkload.cleanup(engine: engine)
                let ids = try await DirectStorageWorkload.seedData(
                    engine: engine,
                    count: seedCount,
                    idPrefix: "update-fixed-direct-storage-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await DirectStorageWorkload.updateOne(engine: engine, id: state.pool.next())
            }
        )
        let canonicalRecordUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await seedCanonicalRecordStorageData(
                    engine: engine,
                    layout: layout,
                    count: seedCount,
                    idPrefix: "update-fixed-canonical-storage-r\(round)"
                )
                return PoolRoundState(pool: IterationIDPool(ids: ids))
            },
            operation: { state in
                try await canonicalRecordStorageWrite(
                    engine: engine,
                    layout: layout,
                    id: state.pool.next()
                )
            }
        )
        let dataStoreUpdate = try await measureAsyncPhaseMedianWithSetup(
            iterations: iterations,
            rounds: rounds,
            setup: { round in
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
                    container: container,
                    count: seedCount,
                    idPrefix: "update-fixed-reused-r\(round)"
                )
                return ContextRoundState(
                    pool: IterationIDPool(ids: ids),
                    context: DatabaseContext(container: container)
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
                try await DatabaseRecordWorkload.cleanup(container: container)
                let ids = try await DatabaseRecordWorkload.seedData(
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
                try await DatabaseRecordWorkload.updateOne(container: container, item: item)
            }
        )

        let results = [
            PhaseResult(name: "Direct storage update", iterations: iterations, totalNanoseconds: directStorageUpdate),
            PhaseResult(name: "Canonical record storage update", iterations: iterations, totalNanoseconds: canonicalRecordUpdate),
            PhaseResult(name: "DataStore batch mutation", iterations: iterations, totalNanoseconds: dataStoreUpdate),
            PhaseResult(name: "DatabaseContext.save() reused context", iterations: iterations, totalNanoseconds: reusedContextUpdate),
            PhaseResult(name: "DatabaseContext.save() fresh context", iterations: iterations, totalNanoseconds: freshContextUpdate),
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
            name: "data-store-vs-canonical-storage update",
            deltaNanoseconds: Int64(dataStoreUpdate) - Int64(canonicalRecordUpdate),
            iterations: iterations
        )
        printSignedDelta(
            name: "database-record-vs-data-store update",
            deltaNanoseconds: Int64(reusedContextUpdate) - Int64(dataStoreUpdate),
            iterations: iterations
        )
        printSignedDelta(
            name: "fresh-vs-reused update",
            deltaNanoseconds: Int64(freshContextUpdate) - Int64(reusedContextUpdate),
            iterations: iterations
        )
        print("")
    }

    // MARK: - StorageKit Direct Operations

    /// Layer 1 write path: ad hoc key and opaque bytes only.
    static func adHocStorageWrite(engine: any StorageEngine, id: String) async throws {
        let key = adHocItemKey(id: id)
        let value = ByteString(repeating: 0x42, count: 70)
        try await engine.withAutoCommit { tx in
            try tx.setValue(value, for: key)
        }
    }

    /// Write through the canonical record storage contract.
    static func canonicalRecordStorageWrite(
        engine: any StorageEngine,
        layout: BenchmarkStorageLayout,
        id: String
    ) async throws {
        var item = BenchmarkItem()
        item.id = id
        item.name = "Alice"
        item.age = 30
        item.score = 85.5

        let data = try DataAccess.serialize(item)
        let key = canonicalItemKey(layout: layout, id: id)

        try await engine.withAutoCommit { tx in
            let storage = layout.itemStorageFactory.make(
                transaction: tx,
                blobsSubspace: layout.blobsSubspace
            )
            try await storage.write(data, for: key)
        }
    }

    /// Layer 1 delete path: ad hoc key delete.
    static func adHocStorageDelete(engine: any StorageEngine, id: String) async throws {
        let key = adHocItemKey(id: id)
        try await engine.withAutoCommit { tx in
            try tx.clear(key: key)
        }
    }

    /// Delete through the canonical record storage contract.
    static func canonicalRecordStorageDelete(
        engine: any StorageEngine,
        layout: BenchmarkStorageLayout,
        id: String
    ) async throws {
        let key = canonicalItemKey(layout: layout, id: id)
        try await engine.withAutoCommit { tx in
            let storage = layout.itemStorageFactory.make(
                transaction: tx,
                blobsSubspace: layout.blobsSubspace
            )
            try await storage.delete(for: key)
        }
    }

    /// Read canonical-key presence without decoding the stored record.
    static func canonicalKeyStorageRead(
        engine: any StorageEngine,
        layout: BenchmarkStorageLayout,
        id: String
    ) async throws {
        let key = canonicalItemKey(layout: layout, id: id)
        try await engine.withAutoCommit { tx in
            _ = try await tx.getValue(for: key, snapshot: false)
        }
    }

    // MARK: - Canonical Record Storage

    @discardableResult
    static func canonicalRecordStorageRead(
        engine: any StorageEngine,
        layout: BenchmarkStorageLayout,
        id: String
    ) async throws -> BenchmarkItem? {
        let key = canonicalItemKey(layout: layout, id: id)
        return try await engine.withAutoCommit { tx in
            let storage = layout.itemStorageFactory.make(
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
    static func seedCanonicalRecordStorageData(
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
                let storage = layout.itemStorageFactory.make(
                    transaction: tx,
                    blobsSubspace: layout.blobsSubspace
                )
                for i in batchStart..<end {
                    let id = "\(idPrefix)-\(String(format: "%06d", i))"
                    ids.append(id)

                    var item = BenchmarkItem()
                    item.id = id
                    item.name = "User \(i)"
                    item.age = Int64(20 + (i % 60))
                    item.score = Double(50 + (i % 50))

                    let data = try DataAccess.serialize(item)
                    try await storage.write(
                        data,
                        for: canonicalItemKey(layout: layout, id: id)
                    )
                }
            }
        }

        return ids
    }

    /// Resolve the canonical record layout used by benchmark parity checks.
    static func benchmarkStorageLayout(container: DBContainer) async throws -> BenchmarkStorageLayout {
        let subspace = try await container.resolveDirectory(for: BenchmarkItem.self)
        return BenchmarkStorageLayout(
            itemSubspace: subspace.subspace(SubspaceKey.items).subspace(BenchmarkItem.persistableType),
            blobsSubspace: subspace.subspace(SubspaceKey.blobs),
            itemStorageFactory: container.itemStorageFactory
        )
    }

    /// Construct a canonical item key used by benchmark parity checks.
    static func canonicalItemKey(layout: BenchmarkStorageLayout, id: String) -> ByteString {
        layout.canonicalItemKey(id: id)
    }

    static func adHocItemKey(id: String) -> ByteString {
        let prefix = "benchmark/items/".utf8
        return ByteString.copying(count: prefix.count + id.utf8.count) { destination in
            var offset = 0
            for byte in prefix {
                destination[offset] = byte
                offset += 1
            }
            for byte in id.utf8 {
                destination[offset] = byte
                offset += 1
            }
        }
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

    private static func printAdjacentStrategyDeltas(_ result: StrategyComparisonResult) {
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
            printAdjacentStrategyDeltas(result)
            return
        }

        let l1 = strategies[0]
        let l2 = strategies[1]
        let l3 = strategies[2]

        print("  Delta Analysis:")
        print("  " + String(repeating: "-", count: 52))
        printLayerDelta(
            label: "L1 → L2",
            description: BenchmarkLayerContract.storageEncodingTransitionDescription,
            from: l1,
            to: l2
        )
        printLayerDelta(
            label: "L2 → L3",
            description: BenchmarkLayerContract.dataStoreReadTransitionDescription,
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
        directStorageName: String? = nil,
        canonicalStorageName: String,
        dataStoreName: String,
        contextName: String,
        storageDescription: String = BenchmarkLayerContract.dataStoreReadTransitionDescription,
        contextDescription: String = BenchmarkLayerContract.contextReadTransitionDescription
    ) {
        guard
            let canonicalStorage = result.strategies.first(where: { $0.name == canonicalStorageName }),
            let dataStore = result.strategies.first(where: { $0.name == dataStoreName }),
            let context = result.strategies.first(where: { $0.name == contextName })
        else {
            printAdjacentStrategyDeltas(result)
            return
        }

        if let directStorageName,
           let directStorage = result.strategies.first(where: { $0.name == directStorageName }) {
            print("  Strict Gap")
            print("  " + String(repeating: "-", count: 52))
            printLayerDelta(
                label: "\(directStorageName) → \(contextName)",
                description: "database record API delta",
                from: directStorage,
                to: context
            )
            print("")
        }

        print("  Storage Overhead")
        print("  " + String(repeating: "-", count: 52))
        printLayerDelta(
            label: "\(canonicalStorageName) → \(dataStoreName)",
            description: storageDescription,
            from: canonicalStorage,
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

    private static func printWriteWorkloadDeltaAnalysis(
        _ result: StrategyComparisonResult,
        directStorageName: String,
        canonicalStorageName: String,
        dataStoreMutationName: String,
        databaseRecordMutationName: String
    ) {
        guard
            let directStorage = result.strategies.first(where: { $0.name == directStorageName }),
            let canonicalStorage = result.strategies.first(where: { $0.name == canonicalStorageName }),
            let dataStoreMutation = result.strategies.first(where: { $0.name == dataStoreMutationName }),
            let databaseRecordMutation = result.strategies.first(where: { $0.name == databaseRecordMutationName })
        else {
            printAdjacentStrategyDeltas(result)
            return
        }

        print("  Delta Analysis")
        print("  " + String(repeating: "-", count: 52))
        printLayerDelta(
            label: "\(directStorageName) → \(canonicalStorageName)",
            description: BenchmarkLayerContract.storageEncodingTransitionDescription,
            from: directStorage,
            to: canonicalStorage
        )
        printLayerDelta(
            label: "\(canonicalStorageName) → \(dataStoreMutationName)",
            description: BenchmarkLayerContract.dataStoreBatchTransitionDescription,
            from: canonicalStorage,
            to: dataStoreMutation
        )
        printLayerDelta(
            label: "\(dataStoreMutationName) → \(databaseRecordMutationName)",
            description: BenchmarkLayerContract.databaseRecordMutationTransitionDescription,
            from: dataStoreMutation,
            to: databaseRecordMutation
        )
        print("")
        print("  \(directStorageName) → \(databaseRecordMutationName) database record API gap: \(String(format: "%+.2f", databaseRecordMutation.metrics.latency.p50 - directStorage.metrics.latency.p50))ms")
        print("")
    }

    private static func printParityTargetAssessment(
        title: String,
        result: StrategyComparisonResult,
        fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
        canonicalStorageName: String,
        dataStoreName: String,
        contextName: String,
        targetThroughputOverheadPercent: Double = 10.0,
        targetFixedDeltaMicroseconds: Double = 20.0,
        tolerance: Double = 0.05
    ) {
        print("  \(title)")
        print("  " + String(repeating: "-", count: 52))
        printTargetAssessmentSection(
            heading: "Storage Parity Summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: canonicalStorageName,
            candidateName: dataStoreName,
            targetThroughputOverheadPercent: targetThroughputOverheadPercent,
            targetFixedDeltaMicroseconds: targetFixedDeltaMicroseconds,
            tolerance: tolerance
        )
        printTargetAssessmentSection(
            heading: "Context Parity Summary",
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: dataStoreName,
            candidateName: contextName,
            targetThroughputOverheadPercent: targetThroughputOverheadPercent,
            targetFixedDeltaMicroseconds: targetFixedDeltaMicroseconds,
            tolerance: tolerance
        )
    }

    private static func printDatabaseRecordMutationTargetAssessment(
        title: String,
        result: StrategyComparisonResult,
        fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
        directStorageName: String,
        canonicalStorageName: String,
        dataStoreMutationName: String,
        databaseRecordMutationName: String,
        targetThroughputOverheadPercent: Double = 10.0,
        targetFixedDeltaMicroseconds: Double = 20.0,
        tolerance: Double = 0.05
    ) {
        print("  \(title)")
        print("  " + String(repeating: "-", count: 52))
        printTargetAssessmentSection(
            heading: BenchmarkLayerContract.databaseRecordParitySummary,
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: directStorageName,
            candidateName: databaseRecordMutationName,
            targetThroughputOverheadPercent: targetThroughputOverheadPercent,
            targetFixedDeltaMicroseconds: targetFixedDeltaMicroseconds,
            tolerance: tolerance
        )
        printWriteDiagnosticAssessmentSection(
            heading: BenchmarkLayerContract.diagnosticBreakdown,
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: canonicalStorageName,
            candidateName: dataStoreMutationName
        )
        printWriteDiagnosticAssessmentSection(
            heading: "Database record mutation diagnostic",
            result: result,
            fixedMeasurements: fixedMeasurements,
            baselineName: dataStoreMutationName,
            candidateName: databaseRecordMutationName,
            expectsDatabaseRecordAdvantage: true
        )
    }

    private static func printWriteDiagnosticAssessmentSection(
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

        let throughputDelta = ((baselineThroughput - candidateThroughput) / baselineThroughput) * 100
        let fixedDelta = fixedCandidate.averageMicroseconds - fixedBaseline.averageMicroseconds

        print("  \(heading)")
        print("  " + String(repeating: "-", count: 52))
        print("  \(baselineName) -> \(candidateName) throughput delta: \(formatWriteDiagnosticLine(delta: throughputDelta, unit: "%", expectsDatabaseRecordAdvantage: expectsDatabaseRecordAdvantage))")
        print("  \(baselineName) -> \(candidateName) fixed delta: \(formatWriteDiagnosticLine(delta: fixedDelta, unit: " us/op", expectsDatabaseRecordAdvantage: expectsDatabaseRecordAdvantage))")
        print("")
    }

    private static func printTargetAssessmentSection(
        heading: String,
        result: StrategyComparisonResult,
        fixedMeasurements: [FixedIterationReporter.MeasurementSummary],
        baselineName: String,
        candidateName: String,
        targetThroughputOverheadPercent: Double = 10.0,
        targetFixedDeltaMicroseconds: Double = 20.0,
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
        print(
            "  \(baselineName) -> \(candidateName) throughput target <= \(Int(targetThroughputOverheadPercent))%: \(formatTargetLine(delta: throughputOverheadPercent, unit: "%", tolerance: targetThroughputOverheadPercent + tolerance))"
        )
        print(
            "  \(baselineName) -> \(candidateName) fixed target <= \(Int(targetFixedDeltaMicroseconds)) us/op: \(formatTargetLine(delta: fixedDelta, unit: " us/op", tolerance: targetFixedDeltaMicroseconds + tolerance))"
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
        expectsDatabaseRecordAdvantage: Bool
    ) -> String {
        if delta < 0 {
            let suffix = expectsDatabaseRecordAdvantage ? " (expected database record API advantage)" : ""
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
        deltaNanoseconds: Int64,
        iterations: Int
    ) {
        let averageMicroseconds = Double(deltaNanoseconds) / Double(iterations) / 1000.0
        let padded = name.padding(toLength: max(40, name.count), withPad: " ", startingAt: 0)
        print("  \(padded) \(String(format: "%+8.1f", averageMicroseconds)) us")
    }
}
