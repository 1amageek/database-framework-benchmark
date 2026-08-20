// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "database-framework-benchmark",
    platforms: [.macOS(.v26)],
    products: [
        .library(
            name: "BenchmarkFramework",
            targets: ["BenchmarkFramework"]
        ),
        .executable(
            name: "DatabaseBenchmark",
            targets: ["DatabaseBenchmark"]
        ),
    ],
    dependencies: [
        // The benchmark-only probe is developed in the adjacent framework checkout.
        .package(path: "../database-framework", traits: ["PostgreSQL"]),
        .package(
            url: "https://github.com/1amageek/database-kit.git",
            from: "26.0819.0"
        ),
        .package(
            url: "https://github.com/1amageek/storage-kit.git",
            from: "26.0820.0"
        ),
        .package(
            url: "https://github.com/1amageek/database-types.git",
            from: "26.0730.0"
        ),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.7.0"),
        .package(
            url: "https://github.com/1amageek/swift-testing-heartbeat.git",
            from: "0.1.0"
        ),
    ],
    targets: [
        .target(
            name: "BenchmarkFramework",
            dependencies: [
                .product(name: "StorageKit", package: "storage-kit"),
            ]
        ),
        .executableTarget(
            name: "DatabaseBenchmark",
            dependencies: [
                .product(name: "DatabaseEngine", package: "database-framework"),
                .product(name: "DatabaseRuntime", package: "database-framework"),
                "BenchmarkFramework",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "StorageKitSystemClock", package: "storage-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(
                    name: "DatabaseTypesFoundation",
                    package: "database-types"
                ),
                .product(name: "PostgreSQLStorage", package: "storage-kit"),
                .product(name: "Logging", package: "swift-log"),
            ]
        ),
        .testTarget(
            name: "DatabaseBenchmarkTests",
            dependencies: [
                "DatabaseBenchmark",
                "BenchmarkFramework",
                .product(name: "DatabaseEngine", package: "database-framework"),
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ]
        ),
        .testTarget(
            name: "BenchmarkFrameworkTests",
            dependencies: [
                "BenchmarkFramework",
                .product(
                    name: "TestHeartbeat",
                    package: "swift-testing-heartbeat"
                ),
            ]
        ),
    ]
)
