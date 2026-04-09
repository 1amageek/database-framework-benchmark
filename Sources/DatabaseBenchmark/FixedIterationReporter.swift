import Foundation

enum FixedIterationReporter {
    struct MeasurementSummary {
        let name: String
        let totalNanos: UInt64

        var averageMicros: Double {
            Double(totalNanos) / 1000.0
        }
    }

    static func print(
        title: String,
        strategies: [Strategy],
        iterations: Int = 200,
        rounds: Int = 1
    ) async throws -> [MeasurementSummary] {
        Swift.print("  \(title)")
        var measurements: [MeasurementSummary] = []
        measurements.reserveCapacity(strategies.count)
        for strategy in strategies {
            let measurement = try await measure(
                name: strategy.0,
                iterations: iterations,
                rounds: rounds,
                operation: strategy.1
            )
            measurements.append(measurement)
        }

        let header = rounds > 1
            ? "  Fixed Iteration Avg (\(iterations) iterations, median of \(rounds) rounds)"
            : "  Fixed Iteration Avg (\(iterations) iterations)"
        Swift.print(header)
        Swift.print("  " + String(repeating: "-", count: 52))
        let nameWidth = max(28, measurements.map(\.name.count).max() ?? 0)
        for measurement in measurements {
            let padded = measurement.name.padding(toLength: nameWidth, withPad: " ", startingAt: 0)
            Swift.print("  \(padded) \(String(format: "%8.1f", measurement.averageMicros)) us/op")
        }
        Swift.print("")

        if let baseline = measurements.first {
            for measurement in measurements.dropFirst() {
                let deltaMicros = measurement.averageMicros - baseline.averageMicros
                Swift.print(
                    "  Delta vs \(baseline.name): \(measurement.name) \(String(format: "%+.1f", deltaMicros)) us/op"
                )
            }
            Swift.print("")
        }

        return measurements
    }

    private static func measure(
        name: String,
        iterations: Int,
        rounds: Int,
        operation: @Sendable () async throws -> Void
    ) async throws -> MeasurementSummary {
        var roundAverages: [UInt64] = []
        roundAverages.reserveCapacity(max(1, rounds))

        for _ in 0..<max(1, rounds) {
            for _ in 0..<20 {
                try await operation()
            }

            let start = DispatchTime.now().uptimeNanoseconds
            for _ in 0..<iterations {
                try await operation()
            }
            let end = DispatchTime.now().uptimeNanoseconds
            roundAverages.append((end - start) / UInt64(iterations))
        }

        let sorted = roundAverages.sorted()
        let median = sorted[sorted.count / 2]

        return MeasurementSummary(
            name: name,
            totalNanos: median
        )
    }
}
