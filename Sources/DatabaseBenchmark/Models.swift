import Foundation
import DatabaseKit

@Persistable
struct BenchmarkItem {
    #Directory<BenchmarkItem>("benchmark", "items")

    var id: String = UUID().uuidString
    var name: String = ""
    var age: Int64 = 0
    var score: Double = 0.0
}
