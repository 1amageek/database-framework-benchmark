// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "database-framework-benchmark",
    platforms: [.macOS(.v26)],
    dependencies: [
        // Use the adjacent local checkout so benchmark runs against in-flight framework changes.
        .package(path: "../database-framework", traits: ["PostgreSQL"]),
        .package(url: "https://github.com/1amageek/database-kit.git", from: "26.0411.0"),
        .package(url: "https://github.com/1amageek/storage-kit.git", from: "26.0324.0", traits: ["PostgreSQL"]),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.25.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.7.0"),
    ],
    targets: [
        .executableTarget(
            name: "DatabaseBenchmark",
            dependencies: [
                .product(name: "DatabaseEngine", package: "database-framework"),
                .product(name: "ScalarIndex", package: "database-framework"),
                .product(name: "BenchmarkFramework", package: "database-framework"),
                .product(name: "Core", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "PostgreSQLStorage", package: "storage-kit"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
                .product(name: "Logging", package: "swift-log"),
            ]
        ),
        .testTarget(
            name: "DatabaseBenchmarkTests",
            dependencies: [
                "DatabaseBenchmark",
                .product(name: "DatabaseEngine", package: "database-framework"),
                .product(name: "Core", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ]
        ),
    ]
)
