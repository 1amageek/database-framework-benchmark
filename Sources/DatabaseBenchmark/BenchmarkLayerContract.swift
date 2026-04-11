import Foundation

enum BenchmarkLayerContract {
    static let rawKV = "Raw KV"
    static let fullFramework = "Full Framework"
    static let writeStorageParityBaseline = "Storage parity baseline"
    static let genericDataStoreBatchPath = "Generic DataStore batch path"
    static let fullFrameworkProductPath = "Full Framework (product path)"

    static let writeL1 = "L1: Raw KV (ad hoc key, bytes only)"
    static let readL1 = "L1: Raw KV (framework key only)"
    static let l2 = "L2: Raw KV + framework layout + storage stack"
    static let writeL3 = "L3: Generic DataStore batch path"
    static let writeL4 = "L4: Full Framework product path"
    static let pointReadPresenceBaseline = "Raw KV (presence only)"
    static let pointReadDecodeParity = "Raw KV + storage-stack decode parity"
    static let readDataStoreParity = "DataStore.fetch() parity"
    static let genericDataStoreBatchPathProfile = "Generic DataStore batch path"
    static let fullFrameworkProductPathProfile = "Full Framework product path"
    static let reusedContextParity = "FDBContext parity (reused)"
    static let freshContextParity = "FDBContext parity (fresh)"

    static let l1ToL2Description = "framework layout + storage stack"
    static let writeL2ToL3Description = "generic batch abstraction delta"
    static let writeL3ToL4Description = "product fast-path delta"
    static let readL2ToL3Description = "storage parity"
    static let readL3ToL4Description = "context parity"

    static let productParitySummary = "Product parity summary"
    static let diagnosticBreakdown = "Diagnostic breakdown"
    static let storageParitySummary = "Storage Parity Summary"
    static let contextParitySummary = "Context Parity Summary"

    static let readProfileLabels = [readL1, l2, readDataStoreParity, fullFramework]
    static let pointReadCompareLabels = [
        pointReadPresenceBaseline,
        pointReadDecodeParity,
        readDataStoreParity,
        fullFramework,
    ]
    static let writeCompareLabels = [
        rawKV,
        writeStorageParityBaseline,
        genericDataStoreBatchPath,
        fullFrameworkProductPath,
    ]
    static let writeProfileLabels = [writeL1, l2, writeL3, writeL4]
    static let deleteProfileLabels = [writeL1, l2, writeL3, writeL4]
}
