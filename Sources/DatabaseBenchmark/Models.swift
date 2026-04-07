import Foundation
import Core

@Persistable
struct BenchmarkItem {
    #Directory<BenchmarkItem>("benchmark", "items")

    var id: String = UUID().uuidString
    var name: String = ""
    var age: Int = 0
    var score: Double = 0.0
}
