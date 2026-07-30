import Foundation

enum BenchmarkLayerContract {
    static let directStorage = "Direct storage"
    static let databaseRecordQueryAPI = "Database record query API"
    static let canonicalRecordStorageMutation = "Canonical record storage mutation"
    static let dataStoreBatchMutationAPI = "DataStore batch mutation API"
    static let databaseRecordMutationAPI = "Database record mutation API"

    static let directStorageMutation = "L1: Direct storage mutation"
    static let canonicalKeyPresenceRead = "L1: Canonical-key presence read"
    static let canonicalRecordStorage = "L2: Canonical record storage"
    static let dataStoreBatchMutation = "L3: DataStore batch mutation"
    static let databaseRecordMutation = "L4: Database record mutation"
    static let directStoragePresenceRead = "Direct storage presence read"
    static let canonicalRecordDecodeRead = "Canonical record decode read"
    static let dataStoreRecordRead = "DataStore record read"
    static let dataStoreBatchMutationProfile = "DataStore batch mutation"
    static let databaseRecordMutationProfile = "Database record mutation"
    static let reusedContextRecordRead = "Reused-context record read"
    static let freshContextRecordRead = "Fresh-context record read"

    static let storageEncodingTransitionDescription = "record encoding and storage"
    static let dataStoreBatchTransitionDescription = "DataStore batch mutation"
    static let databaseRecordMutationTransitionDescription = "database record mutation"
    static let dataStoreReadTransitionDescription = "DataStore record read"
    static let contextReadTransitionDescription = "context identity resolution"

    static let databaseRecordParitySummary = "Database Record Parity Summary"
    static let diagnosticBreakdown = "Diagnostic breakdown"
    static let storageParitySummary = "Storage Parity Summary"
    static let contextParitySummary = "Context Parity Summary"

    static let readProfileLabels = [
        canonicalKeyPresenceRead,
        canonicalRecordStorage,
        dataStoreRecordRead,
        databaseRecordQueryAPI,
    ]
    static let pointReadCompareLabels = [
        directStoragePresenceRead,
        canonicalRecordDecodeRead,
        dataStoreRecordRead,
        databaseRecordQueryAPI,
    ]
    static let writeCompareLabels = [
        directStorage,
        canonicalRecordStorageMutation,
        dataStoreBatchMutationAPI,
        databaseRecordMutationAPI,
    ]
    static let writeProfileLabels = [
        directStorageMutation,
        canonicalRecordStorage,
        dataStoreBatchMutation,
        databaseRecordMutation,
    ]
    static let deleteProfileLabels = [
        directStorageMutation,
        canonicalRecordStorage,
        dataStoreBatchMutation,
        databaseRecordMutation,
    ]
}
