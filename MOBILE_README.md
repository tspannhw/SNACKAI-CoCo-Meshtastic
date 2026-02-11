# Meshtastic Mobile Dashboard

## Overview

This directory contains two mobile dashboard solutions for viewing Meshtastic mesh network data from Snowflake:

1. **Streamlit Mobile Dashboard** (`mobile_dashboard.py`) - Web-based, works on any device
2. **Native iOS App** (`MeshtasticMobile/`) - Native Swift/SwiftUI application

---

## 1. Streamlit Mobile Dashboard

### Features
- 📱 Mobile-optimized responsive design
- 🔄 Pull-to-refresh support
- 📊 Real-time node status cards
- 💬 Text message viewer
- 🗺️ Simple map view
- ⚡ Fast loading with caching

### Running Locally
```bash
cd /Users/tspann/Downloads/code/coco/meshtastic
streamlit run mobile_dashboard.py --server.port 8503
```

### Accessing on Mobile
1. **Same Network**: Use your computer's local IP (e.g., `http://192.168.1.100:8503`)
2. **ngrok Tunnel** (for external access):
   ```bash
   ngrok http 8503
   ```
   Then use the provided ngrok URL on your mobile device.

### Deploy to Snowflake (Streamlit in Snowflake)
```sql
-- Create stage for the app
CREATE STAGE IF NOT EXISTS DEMO.DEMO.STREAMLIT_APPS;

-- Upload via snow CLI
-- snow streamlit deploy mobile_dashboard --database DEMO --schema DEMO
```

---

## 2. Native iOS App (SwiftUI)

### Project Structure
```
MeshtasticMobile/
├── MeshtasticMobile/
│   ├── MeshtasticMobileApp.swift    # App entry point
│   ├── ContentView.swift             # Main UI views
│   └── SnowflakeService.swift        # Snowflake data service
```

### Features
- Native iOS performance
- SwiftUI dark theme matching dashboard
- TabView navigation (Nodes, Messages, Map)
- Pull-to-refresh
- Async/await data loading
- Node status indicators
- Battery warnings

### Building the iOS App

1. **Open in Xcode**:
   - Open Xcode
   - Create new iOS App project
   - Copy the Swift files into the project

2. **Configure Info.plist** for network access:
   ```xml
   <key>NSAppTransportSecurity</key>
   <dict>
       <key>NSAllowsArbitraryLoads</key>
       <true/>
   </dict>
   ```

3. **Set Environment Variables** (for real Snowflake connection):
   - SNOWFLAKE_ACCOUNT
   - SNOWFLAKE_WAREHOUSE

### Connecting to Snowflake from iOS

For production use, you'll need to:

1. **Use Snowflake SQL REST API**:
   ```swift
   // POST to https://<account>.snowflakecomputing.com/api/v2/statements
   // With JWT authentication
   ```

2. **Or use a backend proxy**:
   - Create a simple REST API that queries Snowflake
   - iOS app calls your API endpoint

---

## 3. Snowflake Interactive Tables & Warehouses

### Overview

Interactive Tables provide **sub-second query latency** for high-concurrency dashboard and mobile applications. They're designed for:
- Real-time dashboards with 100+ concurrent users
- Mobile apps needing instant data refresh
- Low-latency API responses (< 100ms typical)

### Region Availability

| Region | Location |
|--------|----------|
| us-east-1 | N. Virginia |
| us-west-2 | Oregon |
| us-east-2 | Ohio |
| ca-central-1 | Canada Central |
| ap-northeast-1 | Tokyo |
| ap-southeast-2 | Sydney |
| eu-central-1 | Frankfurt |
| eu-west-1 | Ireland |
| eu-west-2 | London |

### Quick Start

```sql
-- Step 1: Create Interactive Table (static snapshot)
CREATE INTERACTIVE TABLE DEMO.DEMO.MESHTASTIC_LIVE
    CLUSTER BY (from_id, packet_type)
AS
SELECT * FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- Step 2: Create Interactive Table (auto-refresh)
CREATE INTERACTIVE TABLE DEMO.DEMO.MESHTASTIC_LIVE_AUTO
    CLUSTER BY (from_id)
    TARGET_LAG = '60 seconds'
    WAREHOUSE = INGEST
AS
SELECT * FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- Step 3: Create Pre-Aggregated Node Summary
CREATE INTERACTIVE TABLE DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
    CLUSTER BY (from_id)
    TARGET_LAG = '2 minutes'
    WAREHOUSE = INGEST
AS
SELECT 
    from_id as node_id,
    COUNT(*) as packet_count,
    MAX(battery_level) as battery_level,
    ROUND(AVG(rx_snr), 2) as avg_snr,
    MAX(latitude) as latitude,
    MAX(longitude) as longitude,
    MAX(temperature) as temperature,
    MAX(short_name) as short_name,
    MAX(ingested_at) as last_seen,
    DATEDIFF(minute, MAX(ingested_at), CURRENT_TIMESTAMP()) as mins_ago
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
GROUP BY from_id;

-- Step 4: Create Interactive Warehouse (REQUIRED for querying)
CREATE INTERACTIVE WAREHOUSE MESH_INTERACTIVE
    WAREHOUSE_SIZE = 'XSMALL'
    TABLES = (
        DEMO.DEMO.MESHTASTIC_LIVE,
        DEMO.DEMO.MESHTASTIC_LIVE_AUTO,
        DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
    );

-- Step 5: Resume and use
ALTER WAREHOUSE MESH_INTERACTIVE RESUME;
USE WAREHOUSE MESH_INTERACTIVE;

-- Step 6: Query with sub-second latency
SELECT * FROM MESHTASTIC_NODE_SUMMARY WHERE mins_ago <= 60;
```

### Key Limitations

| Limitation | Details |
|------------|---------|
| **No UPDATE/DELETE** | Only INSERT OVERWRITE supported |
| **5-second query timeout** | Interactive warehouses enforce strict timeout |
| **Must use interactive warehouse** | Standard warehouses cannot query interactive tables |
| **No downstream streams** | Cannot create streams on interactive tables |
| **No dynamic table dependency** | Dynamic tables cannot use interactive tables as source |
| **Minimum billing** | 1-hour minimum, no auto-suspend by default |

### Example Queries

```sql
USE WAREHOUSE MESH_INTERACTIVE;

-- Active nodes (last 10 minutes)
SELECT node_id, battery_level, avg_snr, mins_ago,
    CASE WHEN mins_ago <= 5 THEN 'Active'
         WHEN mins_ago <= 30 THEN 'Recent'
         ELSE 'Stale' END as status
FROM MESHTASTIC_NODE_SUMMARY
WHERE mins_ago <= 60
ORDER BY last_seen DESC;

-- Recent text messages
SELECT from_id, text_message, ingested_at
FROM MESHTASTIC_LIVE
WHERE packet_type = 'text' AND text_message IS NOT NULL
ORDER BY ingested_at DESC LIMIT 20;

-- Map data
SELECT node_id, latitude, longitude, battery_level
FROM MESHTASTIC_NODE_SUMMARY
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
```

### Full Setup Script

📄 **[interactive_tables_setup.sql](./interactive_tables_setup.sql)** - Complete SQL with all options

---

## Quick Start

### Option A: Web Dashboard (Easiest)
```bash
# Start the mobile-optimized Streamlit dashboard
streamlit run mobile_dashboard.py --server.port 8503

# Access from phone browser at http://<your-ip>:8503
```

### Option B: Full Geospatial Dashboard
```bash
# Start the full 7-tab geospatial dashboard
streamlit run geospatial_dashboard.py --server.port 8502
```

---

## Screenshots

The mobile dashboard includes:
- **Summary Cards**: Active nodes, total nodes, battery, SNR
- **Nodes Tab**: List of all nodes with status indicators
- **Messages Tab**: Recent text messages with timestamps
- **Map Tab**: Node coordinates display

---

## Environment Configuration

Both dashboards read from `~/.snowflake/connections.toml`:

```toml
[tspann1]
account = "SFSENORTHAMERICA-TSPANN-AWS1"
user = "kafkaguy"
private_key_path = "~/.snowflake/keys/snowflake_private_key.p8"
role = "ACCOUNTADMIN"
warehouse = "INGEST"
```

Set connection name via environment variable:
```bash
export SNOWFLAKE_CONNECTION_NAME=tspann1
```
