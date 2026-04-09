import Testing
import StorageKit
import DatabaseEngine
import Core
@testable import DatabaseBenchmark

@Suite("ProfileBenchmark Layer Tests")
struct ProfileBenchmarkLayerTests {
    private func makeContainer() async throws -> DBContainer {
        let engine = InMemoryEngine()
        let schema = Schema([BenchmarkItem.self], version: .init(1, 0, 0))
        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
    }

    @Test("canonical framework key matches resolved items subspace packing")
    func canonicalFrameworkKeyMatchesResolvedLayout() async throws {
        let container = try await makeContainer()
        let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
        let id = "layout-test"

        let subspace = try await container.resolveDirectory(for: BenchmarkItem.self)
        let expected = subspace
            .subspace(SubspaceKey.items)
            .subspace(BenchmarkItem.persistableType)
            .pack(Tuple([id]))

        #expect(ProfileBenchmark.benchmarkKeyBytes(layout: layout, id: id) == expected)
    }

    @Test("read L1 framework key differs from ad hoc raw key baseline")
    func frameworkReadKeyDiffersFromAdHocBaseline() async throws {
        let container = try await makeContainer()
        let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
        let id = "read-key"

        let frameworkKey = ProfileBenchmark.benchmarkKeyBytes(layout: layout, id: id)
        let rawKey = ProfileBenchmark.rawAdHocKeyBytes(id: id)

        #expect(frameworkKey != rawKey)
    }

    @Test("framework layout storage helpers round-trip and delete via canonical layout")
    func frameworkLayoutStorageHelpersRoundTrip() async throws {
        let engine = InMemoryEngine()
        let schema = Schema([BenchmarkItem.self], version: .init(1, 0, 0))
        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)
        let id = "roundtrip"

        try await ProfileBenchmark.frameworkLayoutStorageWrite(
            engine: engine,
            layout: layout,
            id: id,
            isNewRecord: true
        )

        let item = try await ProfileBenchmark.frameworkLayoutStorageDecodedRead(
            engine: engine,
            layout: layout,
            id: id
        )
        #expect(item?.id == id)

        try await ProfileBenchmark.frameworkLayoutStorageDelete(
            engine: engine,
            layout: layout,
            id: id,
            skipBlobCleanup: false
        )

        let deleted = try await ProfileBenchmark.frameworkLayoutStorageDecodedRead(
            engine: engine,
            layout: layout,
            id: id
        )
        #expect(deleted == nil)
    }

    @Test("decode parity seed helper writes canonical layout records that decode correctly")
    func decodeParitySeedHelperRoundTrips() async throws {
        let engine = InMemoryEngine()
        let schema = Schema([BenchmarkItem.self], version: .init(1, 0, 0))
        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let layout = try await ProfileBenchmark.benchmarkStorageLayout(container: container)

        let ids = try await ProfileBenchmark.seedFrameworkLayoutStorageData(
            engine: engine,
            layout: layout,
            count: 3,
            idPrefix: "seed-parity"
        )

        #expect(ids.count == 3)

        let item = try await ProfileBenchmark.frameworkLayoutStorageDecodedRead(
            engine: engine,
            layout: layout,
            id: ids[1]
        )
        #expect(item?.id == ids[1])
    }

    @Test("layer labels are stable and explicit about their contracts")
    func layerLabelsAreStable() {
        #expect(BenchmarkLayerContract.rawKV == "Raw KV")
        #expect(BenchmarkLayerContract.dataStoreParity == "DataStore parity")
        #expect(BenchmarkLayerContract.fullFramework == "Full Framework")
        #expect(BenchmarkLayerContract.writeL1 == "L1: Raw KV (ad hoc key, bytes only)")
        #expect(BenchmarkLayerContract.readL1 == "L1: Raw KV (framework key only)")
        #expect(BenchmarkLayerContract.l2 == "L2: Raw KV + framework layout + storage stack")
        #expect(BenchmarkLayerContract.l3 == "L3: Full Framework")
        #expect(BenchmarkLayerContract.pointReadPresenceBaseline == "Raw KV (presence only)")
        #expect(BenchmarkLayerContract.pointReadDecodeParity == "Raw KV + storage-stack decode parity")
        #expect(BenchmarkLayerContract.readDataStoreParity == "DataStore.fetch() parity")
        #expect(BenchmarkLayerContract.updateDataStoreParity == "DataStore.executeBatch() parity")
        #expect(BenchmarkLayerContract.deleteDataStoreParity == "DataStore.executeBatch() parity")
        #expect(BenchmarkLayerContract.reusedContextParity == "FDBContext parity (reused)")
        #expect(BenchmarkLayerContract.freshContextParity == "FDBContext parity (fresh)")
        #expect(BenchmarkLayerContract.l1ToL2Description == "framework layout + storage stack")
        #expect(BenchmarkLayerContract.l2ToL3Description == "DataStore/FDBContext abstraction")
        #expect(BenchmarkLayerContract.readProfileLabels == [
            BenchmarkLayerContract.readL1,
            BenchmarkLayerContract.l2,
            BenchmarkLayerContract.readDataStoreParity,
            BenchmarkLayerContract.fullFramework,
        ])
        #expect(BenchmarkLayerContract.pointReadCompareLabels == [
            BenchmarkLayerContract.pointReadPresenceBaseline,
            BenchmarkLayerContract.pointReadDecodeParity,
            BenchmarkLayerContract.readDataStoreParity,
            BenchmarkLayerContract.fullFramework,
        ])
        #expect(BenchmarkLayerContract.writeProfileLabels == [
            BenchmarkLayerContract.writeL1,
            BenchmarkLayerContract.l2,
            BenchmarkLayerContract.updateDataStoreParity,
            BenchmarkLayerContract.fullFramework,
        ])
        #expect(BenchmarkLayerContract.deleteProfileLabels == [
            BenchmarkLayerContract.writeL1,
            BenchmarkLayerContract.l2,
            BenchmarkLayerContract.deleteDataStoreParity,
            BenchmarkLayerContract.fullFramework,
        ])
    }
}
