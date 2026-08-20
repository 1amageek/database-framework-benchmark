import DatabaseKit

@Persistable
struct UndeclaredBenchmarkItem {
    #Directory<UndeclaredBenchmarkItem>("benchmark", "undeclared-items")

    var id: String = ""
}
