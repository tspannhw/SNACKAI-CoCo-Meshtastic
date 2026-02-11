# Meshtastic Mobile - iOS App

A native SwiftUI iOS application for monitoring Meshtastic mesh network data stored in Snowflake.

## Features

- 📊 **Real-time Node Monitoring** - View all mesh nodes with status indicators
- 🔋 **Battery & Signal Tracking** - Monitor battery levels and SNR/RSSI
- 💬 **Text Messages** - View mesh network text communications
- 🗺️ **Map View** - Visualize node locations on Apple Maps
- 🌙 **Dark Theme** - Native iOS dark mode optimized design
- ⚡ **Pull-to-Refresh** - Instant data updates
- 📱 **iOS 16+** - Modern SwiftUI with async/await

## Architecture

```
MeshtasticMobile/
├── MeshtasticMobile/
│   ├── App/
│   │   └── MeshtasticMobileApp.swift     # App entry point
│   ├── Models/
│   │   ├── MeshtasticNode.swift          # Node data model
│   │   └── MeshtasticMessage.swift       # Message data model
│   ├── Services/
│   │   ├── SnowflakeService.swift        # Snowflake REST API client
│   │   └── SnowflakeAuth.swift           # JWT authentication
│   ├── Views/
│   │   ├── ContentView.swift             # Main TabView
│   │   ├── NodesView.swift               # Node list view
│   │   ├── MessagesView.swift            # Messages view
│   │   ├── MapView.swift                 # Map view
│   │   └── Components/
│   │       ├── NodeCard.swift            # Node card component
│   │       ├── MetricCard.swift          # Metric display card
│   │       └── MessageBubble.swift       # Message bubble
│   └── Resources/
│       ├── Assets.xcassets/              # App icons & colors
│       └── Info.plist                    # App configuration
├── MeshtasticMobileTests/                # Unit tests
├── MeshtasticMobileUITests/              # UI tests
├── Scripts/
│   ├── build.sh                          # Build script
│   ├── test.sh                           # Test script
│   ├── archive.sh                        # Archive for distribution
│   └── deploy.sh                         # Deploy to TestFlight
└── README.md
```

## Requirements

- macOS 13.0+ (Ventura or later)
- Xcode 15.0+
- iOS 16.0+ deployment target
- Apple Developer Account (for device deployment)
- Snowflake account with Meshtastic data

## Deploy to Personal iPhone

**Quick deploy to your iPhone for testing:**

```bash
# 1. Enable Developer Mode on iPhone (Settings → Privacy & Security → Developer Mode)
# 2. Connect iPhone via USB and trust the Mac
# 3. Run:
./Scripts/deploy-device.sh
```

📱 **[Full iPhone Setup Guide](./DEPLOY_TO_IPHONE.md)**

---

## Quick Start

### 1. Clone and Setup

```bash
cd /Users/tspann/Downloads/code/coco/meshtastic/MeshtasticMobile

# Generate Xcode project (if using Swift Package Manager)
swift package generate-xcodeproj

# Or open directly in Xcode
open MeshtasticMobile.xcodeproj
```

### 2. Configure Snowflake Connection

Edit `SnowflakeService.swift` with your credentials:

```swift
private let config = SnowflakeConfig(
    account: "YOUR_ACCOUNT",
    user: "YOUR_USER", 
    warehouse: "INGEST",
    database: "DEMO",
    schema: "DEMO",
    role: "ACCOUNTADMIN"
)
```

### 3. Build and Run

```bash
# Build for simulator
./Scripts/build.sh simulator

# Build for device
./Scripts/build.sh device

# Run tests
./Scripts/test.sh
```

## Snowflake Authentication

The app supports two authentication methods:

### Option A: JWT Key-Pair (Recommended for Production)

```swift
// In SnowflakeAuth.swift
let jwt = try SnowflakeAuth.generateJWT(
    account: "myaccount",
    user: "myuser",
    privateKeyPath: Bundle.main.path(forResource: "snowflake_key", ofType: "p8")!
)
```

### Option B: Backend Proxy (Recommended for App Store)

Create a backend service that handles Snowflake authentication:

```
iOS App → Your Backend API → Snowflake
```

This keeps credentials secure and allows for:
- Rate limiting
- Caching
- User authentication
- Analytics

## API Endpoints

The app queries these Snowflake views/tables:

| Query | Description |
|-------|-------------|
| Node Summary | Aggregated node stats (battery, SNR, location) |
| Recent Packets | Last 24 hours of mesh traffic |
| Text Messages | Decoded text messages |
| Interactive Table | Sub-second queries (if available) |

## Building for Distribution

### TestFlight (Beta Testing)

```bash
# Archive and upload to App Store Connect
./Scripts/archive.sh
./Scripts/deploy.sh testflight
```

### App Store

```bash
# Full release build
./Scripts/deploy.sh appstore
```

## Configuration

### Info.plist Settings

```xml
<!-- Network access -->
<key>NSAppTransportSecurity</key>
<dict>
    <key>NSAllowsArbitraryLoads</key>
    <false/>
    <key>NSExceptionDomains</key>
    <dict>
        <key>snowflakecomputing.com</key>
        <dict>
            <key>NSIncludesSubdomains</key>
            <true/>
            <key>NSExceptionAllowsInsecureHTTPLoads</key>
            <false/>
            <key>NSExceptionRequiresForwardSecrecy</key>
            <true/>
        </dict>
    </dict>
</dict>

<!-- Location for map -->
<key>NSLocationWhenInUseUsageDescription</key>
<string>Show your location relative to mesh nodes</string>

<!-- Background refresh -->
<key>UIBackgroundModes</key>
<array>
    <string>fetch</string>
</array>
```

### Environment Variables

For CI/CD builds:

```bash
export SNOWFLAKE_ACCOUNT="myaccount"
export SNOWFLAKE_USER="myuser"
export SNOWFLAKE_WAREHOUSE="INGEST"
export APPLE_TEAM_ID="XXXXXXXXXX"
export APP_STORE_CONNECT_API_KEY="path/to/key.p8"
```

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| "No such module" | Run `swift package resolve` |
| Signing errors | Check Team ID in Xcode |
| Network timeout | Verify Snowflake account URL |
| JWT errors | Check key format (PKCS8) |

### Debug Logging

Enable verbose logging in development:

```swift
#if DEBUG
SnowflakeService.shared.enableDebugLogging = true
#endif
```

## License

MIT License - See LICENSE file
