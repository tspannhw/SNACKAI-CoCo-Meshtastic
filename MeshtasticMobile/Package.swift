// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "MeshtasticMobile",
    platforms: [
        .iOS(.v16),
        .macOS(.v13)
    ],
    products: [
        .library(
            name: "MeshtasticMobile",
            targets: ["MeshtasticMobile"]
        )
    ],
    dependencies: [
        // JWT for Snowflake authentication
        // .package(url: "https://github.com/vapor/jwt-kit.git", from: "4.0.0"),
    ],
    targets: [
        .target(
            name: "MeshtasticMobile",
            dependencies: [],
            path: "MeshtasticMobile"
        ),
        .testTarget(
            name: "MeshtasticMobileTests",
            dependencies: ["MeshtasticMobile"],
            path: "MeshtasticMobileTests"
        )
    ]
)
