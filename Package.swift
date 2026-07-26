// swift-tools-version: 6.2
//
//  pulsar — epoll event loop bridge for Swift Concurrency.
//
//  Built on top of the `mio` package (epoll primitives). Provides
//  SerialExecutor + TaskExecutor conformance (SE-0392/SE-0431) so
//  Swift Concurrency Tasks can be pinned to a single epoll-driven
//  thread — the thread-per-core model.
//
//  Direct port of the reactor layer from tokio, adapted for Swift's
//  cooperative thread pool.
//
import PackageDescription

let package = Package(
    name: "pulsar",
    products: [
        .library(name: "Pulsar", targets: ["Pulsar"]),
    ],
    dependencies: [
        .package(url: "https://github.com/akvilary/mio.git", from: "0.2.0"),
    ],
    targets: [
        .target(
            name: "Pulsar",
            dependencies: [
                .product(name: "MIO", package: "mio"),
            ],
            path: "Sources/Pulsar",
            swiftSettings: baseSwiftSettings
        ),
        .testTarget(
            name: "PulsarTests",
            dependencies: ["Pulsar"],
            path: "Tests/PulsarTests",
            swiftSettings: baseSwiftSettings
        ),
    ]
)

var baseSwiftSettings: [SwiftSetting] {
    [
        .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
        .enableExperimentalFeature("Lifetimes"),
        .enableExperimentalFeature("StrictMemorySafety"),
    ]
}
