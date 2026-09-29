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
        // Published dependency — the exact form every consumer
        // (starlight included) resolves: mio is fetched from GitHub
        // within the version bound below. A `path: "../mio"`
        // dependency works only inside the local workspace layout and
        // is uninstallable for consumers of a published tag — the
        // release process must never carry one. To develop pulsar
        // against local mio changes, tag/push mio (or temporarily
        // switch this line to a path dep) and bump the bound.
        .package(url: "https://github.com/akvilary/mio.git", from: "0.3.0"),
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
        // A/B benchmark harness for the channel-table hot path
        // (see Sources/PulsarBench/main.swift header).
        .executableTarget(
            name: "PulsarBench",
            dependencies: ["Pulsar"],
            path: "Sources/PulsarBench",
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
