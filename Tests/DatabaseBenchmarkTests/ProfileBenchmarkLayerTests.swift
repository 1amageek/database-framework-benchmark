import Testing
import StorageKit
import DatabaseEngine
import DatabaseKit
@testable import DatabaseBenchmark

@Suite("ProfileBenchmark Layer Tests")
struct ProfileBenchmarkLayerTests {
    private func makeContainer() async throws -> DBContainer {
        let engine = InMemoryEngine()
        let schema = Schema([BenchmarkItem.self], version: .init(1, 0, 0))
        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(),
            security: .disabled
        )
    }

    @Test("canonical item key matches resolved items subspace packing")
    func canonicalItemKeyMatchesResolvedLayout() async throws {
        let container = try await makeContainer()
        let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
        let id = "layout-test"

        let subspace = try await container.resolveDirectory(for: BenchmarkItem.self)
        let expected = subspace
            .subspace(SubspaceKey.items)
            .subspace(BenchmarkItem.persistableType)
            .pack(Tuple([id]))

        #expect(ProfileBenchmark.canonicalItemKey(layout: layout, id: id) == expected)
    }

    @Test("canonical item key differs from the ad hoc storage key")
    func canonicalKeyDiffersFromAdHocStorageKey() async throws {
        let container = try await makeContainer()
        let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
        let id = "read-key"

        let canonicalKey = ProfileBenchmark.canonicalItemKey(layout: layout, id: id)
        let adHocStorageKey = ProfileBenchmark.adHocItemKey(id: id)

        #expect(canonicalKey != adHocStorageKey)
    }

    @Test("canonical record storage round-trips and deletes")
    func canonicalRecordStorageRoundTripsAndDeletes() async throws {
        let engine = InMemoryEngine()
        let schema = Schema([BenchmarkItem.self], version: .init(1, 0, 0))
        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(),
            security: .disabled
        )
        let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
        let id = "roundtrip"

        try await ProfileBenchmark.canonicalRecordStorageWrite(
            engine: engine,
            layout: layout,
            id: id
        )

        let item = try await ProfileBenchmark.canonicalRecordStorageRead(
            engine: engine,
            layout: layout,
            id: id
        )
        #expect(item?.id == id)

        try await ProfileBenchmark.canonicalRecordStorageDelete(
            engine: engine,
            layout: layout,
            id: id
        )

        let deleted = try await ProfileBenchmark.canonicalRecordStorageRead(
            engine: engine,
            layout: layout,
            id: id
        )
        #expect(deleted == nil)
    }

    @Test("seeded canonical records decode through the storage stack")
    func seededCanonicalRecordsRoundTrip() async throws {
        let engine = InMemoryEngine()
        let schema = Schema([BenchmarkItem.self], version: .init(1, 0, 0))
        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(),
            security: .disabled
        )
        let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)

        let ids = try await ProfileBenchmark.seedCanonicalRecordStorageData(
            engine: engine,
            layout: layout,
            count: 3,
            idPrefix: "seed-parity"
        )

        #expect(ids.count == 3)

        let item = try await ProfileBenchmark.canonicalRecordStorageRead(
            engine: engine,
            layout: layout,
            id: ids[1]
        )
        #expect(item?.id == ids[1])
    }

    @Test("layer labels are stable and explicit about their contracts")
    func layerLabelsAreStable() {
        #expect(BenchmarkLayerContract.directStorage == "Direct storage")
        #expect(BenchmarkLayerContract.databaseRecordQueryAPI == "Database record query API")
        #expect(BenchmarkLayerContract.canonicalRecordStorageMutation == "Canonical record storage mutation")
        #expect(BenchmarkLayerContract.dataStoreBatchMutationAPI == "DataStore batch mutation API")
        #expect(BenchmarkLayerContract.databaseRecordMutationAPI == "Database record mutation API")
        #expect(BenchmarkLayerContract.directStorageMutation == "L1: Direct storage mutation")
        #expect(BenchmarkLayerContract.canonicalKeyPresenceRead == "L1: Canonical-key presence read")
        #expect(BenchmarkLayerContract.canonicalRecordStorage == "L2: Canonical record storage")
        #expect(BenchmarkLayerContract.dataStoreBatchMutation == "L3: DataStore batch mutation")
        #expect(BenchmarkLayerContract.databaseRecordMutation == "L4: Database record mutation")
        #expect(BenchmarkLayerContract.directStoragePresenceRead == "Direct storage presence read")
        #expect(BenchmarkLayerContract.canonicalRecordDecodeRead == "Canonical record decode read")
        #expect(BenchmarkLayerContract.dataStoreRecordRead == "DataStore record read")
        #expect(BenchmarkLayerContract.dataStoreBatchMutationProfile == "DataStore batch mutation")
        #expect(BenchmarkLayerContract.databaseRecordMutationProfile == "Database record mutation")
        #expect(BenchmarkLayerContract.reusedContextRecordRead == "Reused-context record read")
        #expect(BenchmarkLayerContract.freshContextRecordRead == "Fresh-context record read")
        #expect(BenchmarkLayerContract.storageEncodingTransitionDescription == "record encoding and storage")
        #expect(BenchmarkLayerContract.dataStoreBatchTransitionDescription == "DataStore batch mutation")
        #expect(BenchmarkLayerContract.databaseRecordMutationTransitionDescription == "database record mutation")
        #expect(BenchmarkLayerContract.dataStoreReadTransitionDescription == "DataStore record read")
        #expect(BenchmarkLayerContract.contextReadTransitionDescription == "context identity resolution")
        #expect(BenchmarkLayerContract.databaseRecordParitySummary == "Database Record Parity Summary")
        #expect(BenchmarkLayerContract.diagnosticBreakdown == "Diagnostic breakdown")
        #expect(BenchmarkLayerContract.storageParitySummary == "Storage Parity Summary")
        #expect(BenchmarkLayerContract.contextParitySummary == "Context Parity Summary")
        #expect(BenchmarkLayerContract.readProfileLabels == [
            BenchmarkLayerContract.canonicalKeyPresenceRead,
            BenchmarkLayerContract.canonicalRecordStorage,
            BenchmarkLayerContract.dataStoreRecordRead,
            BenchmarkLayerContract.databaseRecordQueryAPI,
        ])
        #expect(BenchmarkLayerContract.pointReadCompareLabels == [
            BenchmarkLayerContract.directStoragePresenceRead,
            BenchmarkLayerContract.canonicalRecordDecodeRead,
            BenchmarkLayerContract.dataStoreRecordRead,
            BenchmarkLayerContract.databaseRecordQueryAPI,
        ])
        #expect(BenchmarkLayerContract.writeCompareLabels == [
            BenchmarkLayerContract.directStorage,
            BenchmarkLayerContract.canonicalRecordStorageMutation,
            BenchmarkLayerContract.dataStoreBatchMutationAPI,
            BenchmarkLayerContract.databaseRecordMutationAPI,
        ])
        #expect(BenchmarkLayerContract.writeProfileLabels == [
            BenchmarkLayerContract.directStorageMutation,
            BenchmarkLayerContract.canonicalRecordStorage,
            BenchmarkLayerContract.dataStoreBatchMutation,
            BenchmarkLayerContract.databaseRecordMutation,
        ])
        #expect(BenchmarkLayerContract.deleteProfileLabels == [
            BenchmarkLayerContract.directStorageMutation,
            BenchmarkLayerContract.canonicalRecordStorage,
            BenchmarkLayerContract.dataStoreBatchMutation,
            BenchmarkLayerContract.databaseRecordMutation,
        ])
    }
}
