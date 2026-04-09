import Foundation

enum BenchmarkLayerContract {
    static let rawKV = "Raw KV"
    static let dataStoreParity = "DataStore parity"
    static let fullFramework = "Full Framework"

    static let writeL1 = "L1: Raw KV (ad hoc key, bytes only)"
    static let readL1 = "L1: Raw KV (framework key only)"
    static let l2 = "L2: Raw KV + framework layout + storage stack"
    static let l3 = "L3: Full Framework"
    static let pointReadPresenceBaseline = "Raw KV (presence only)"
    static let pointReadDecodeParity = "Raw KV + storage-stack decode parity"
    static let readDataStoreParity = "DataStore.fetch() parity"
    static let updateDataStoreParity = "DataStore.executeBatch() parity"
    static let deleteDataStoreParity = "DataStore.executeBatch() parity"
    static let reusedContextParity = "FDBContext parity (reused)"
    static let freshContextParity = "FDBContext parity (fresh)"

    static let l1ToL2Description = "framework layout + storage stack"
    static let l2ToL3Description = "DataStore/FDBContext abstraction"

    static let readProfileLabels = [readL1, l2, readDataStoreParity, fullFramework]
    static let pointReadCompareLabels = [
        pointReadPresenceBaseline,
        pointReadDecodeParity,
        readDataStoreParity,
        fullFramework,
    ]
    static let writeProfileLabels = [writeL1, l2, updateDataStoreParity, fullFramework]
    static let deleteProfileLabels = [writeL1, l2, deleteDataStoreParity, fullFramework]
}
