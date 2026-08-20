import DatabaseEngine
import DatabaseTypes
import DatabaseTypesFoundation
import Foundation

/// Supplies absolute time for the native benchmark host.
struct BenchmarkWallClock: WallClock {
    var now: Timestamp {
        do {
            return try Timestamp(Date())
        } catch {
            preconditionFailure(
                "The benchmark host clock produced an invalid timestamp: \(error)"
            )
        }
    }
}
