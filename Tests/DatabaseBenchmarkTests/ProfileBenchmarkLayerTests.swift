import Testing
import BenchmarkFramework
import StorageKit
@_spi(Benchmarking) import DatabaseEngine
import DatabaseKit
@testable import DatabaseBenchmark

@Suite("ProfileBenchmark Layer Tests")
struct ProfileBenchmarkLayerTests {
    private func makeContainer() async throws -> DBContainer {
        try await DatabaseRecordWorkload.makeContainer(engine: InMemoryEngine())
    }

    private func withContainer(
        _ operation: @Sendable (DBContainer) async throws -> Void
    ) async throws {
        let container = try await makeContainer()
        do {
            try await operation(container)
            await container.shutdown()
        } catch {
            await container.shutdown()
            throw error
        }
    }

    @Test("canonical item key matches resolved items subspace packing")
    func canonicalItemKeyMatchesResolvedLayout() async throws {
        try await withContainer { container in
            let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
            let id = "layout-test"

            let subspace = try await container.resolveDirectory(for: BenchmarkItem.self)
            let expected = subspace
                .subspace(SubspaceKey.items)
                .subspace(BenchmarkItem.persistableType)
                .pack(Tuple([id]))

            #expect(ProfileBenchmark.canonicalItemKey(layout: layout, id: id) == expected)
        }
    }

    @Test("canonical item key differs from the ad hoc storage key")
    func canonicalKeyDiffersFromAdHocStorageKey() async throws {
        try await withContainer { container in
            let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
            let id = "read-key"

            let canonicalKey = ProfileBenchmark.canonicalItemKey(layout: layout, id: id)
            let adHocStorageKey = ProfileBenchmark.adHocItemKey(id: id)

            #expect(canonicalKey != adHocStorageKey)
        }
    }

    @Test("canonical record storage round-trips and deletes")
    func canonicalRecordStorageRoundTripsAndDeletes() async throws {
        try await withContainer { container in
            let engine = container.engine
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
    }

    @Test("seeded canonical records decode through the storage stack")
    func seededCanonicalRecordsRoundTrip() async throws {
        try await withContainer { container in
            let engine = container.engine
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
    }

    @Test("benchmark probe opens the canonical DataStore layer")
    func benchmarkProbeOpensCanonicalDataStore() async throws {
        try await withContainer { container in
            let store = try await DataStoreBenchmarkProbe.openDataStore(
                for: BenchmarkItem.self,
                in: container
            )
            var item = BenchmarkItem()
            item.id = "probe-roundtrip"
            item.name = "Probe"

            try await store.executeBatch(inserts: [item], deletes: [])
            let fetched = try await store.fetch(
                BenchmarkItem.self,
                id: item.id
            )

            #expect(fetched?.id == item.id)
            #expect(fetched?.name == item.name)
        }
    }

    @Test("benchmark probe rejects models outside the container schema")
    func benchmarkProbeRejectsUndeclaredModel() async throws {
        try await withContainer { container in
            do {
                _ = try await DataStoreBenchmarkProbe.openDataStore(
                    for: UndeclaredBenchmarkItem.self,
                    in: container
                )
                Issue.record("Expected the benchmark probe to reject an undeclared model")
            } catch let error as ContainerSchemaError {
                #expect(
                    error == .entityNotFound(
                        UndeclaredBenchmarkItem.persistableType
                    )
                )
            } catch {
                Issue.record("Unexpected error: \(error)")
            }
        }
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
