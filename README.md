# Meshtastic Mesh Network Streamer for Snowflake

A Python-based data pipeline that streams real-time data from Meshtastic LoRa mesh network devices to Snowflake using Snowpipe Streaming v2 High-Speed REST API.

## Overview

This project provides a complete solution for capturing IoT sensor data from Meshtastic-compatible devices (like the SenseCAP Card Tracker T1000-E) and streaming it to Snowflake for real-time analytics and visualization.

### Key Features

- **Real-time Streaming**: Uses Snowpipe Streaming v2 REST API for high-performance data ingestion
- **Multi-Connection Support**: BLE (Bluetooth), Serial, and TCP connections
- **Auto-Discovery**: Automatically scans for available Meshtastic devices
- **Comprehensive Data Capture**: GPS, telemetry, environmental sensors, and mesh messages
- **Interactive Dashboard**: Streamlit-based visualization with live maps
- **Clean Shutdown**: Graceful Ctrl+C handling with data flush

## Architecture

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│   Meshtastic    │      │     Python       │      │    Snowflake    │
│   T1000-E       │─────▶│    Streamer      │─────▶│   Snowpipe v2   │
│   (BLE/Serial)  │      │                  │      │   REST API      │
└─────────────────┘      └──────────────────┘      └─────────────────┘
         │                        │                        │
         │                        │                        ▼
    GPS/Sensors           Message Queue            MESHTASTIC_DATA
    Position              Batch Processing              Table
    Telemetry             JSON Serialization            │
    Environment                                         ▼
                                                  ┌─────────────────┐
                                                  │   Streamlit     │
                                                  │   Dashboard     │
                                                  └─────────────────┘
```

## Supported Hardware

### Primary Device: SenseCAP Card Tracker T1000-E
- **GPS**: Latitude, longitude, altitude, speed, heading, satellites, DOP values
- **Device Telemetry**: Battery level, voltage, uptime
- **Environmental Sensors**: Temperature, humidity, barometric pressure
- **Connectivity**: BLE (Bluetooth Low Energy), Serial USB

### Other Compatible Devices
- Heltec LoRa 32
- LILYGO T-Beam
- RAK WisBlock
- Any Meshtastic-compatible device

## Installation

### Prerequisites

- Python 3.9+
- Snowflake account with Snowpipe Streaming enabled
- Meshtastic device with firmware 2.0+
- macOS/Linux (for BLE support)

### Setup

```bash
# Clone or navigate to project directory
cd meshtastic

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install meshtastic bleak pyserial requests snowflake-connector-python streamlit plotly pandas
```

## Configuration

### snowflake_config.json

```json
{
    "account": "YOUR_ACCOUNT",
    "user": "YOUR_USER",
    "role": "YOUR_ROLE",
    "warehouse": "YOUR_WAREHOUSE",
    "pat": "YOUR_PAT_TOKEN",
    "database": "DEMO",
    "schema": "DEMO",
    "table": "MESHTASTIC_DATA",
    "pipe": "MESHTASTIC_STREAM_PIPE",
    "channel_name": "MESH_CHNL",
    "batch_size": 10,
    "flush_interval_seconds": 5,
    "meshtastic": {
        "connection_type": "auto",
        "device_path": null,
        "ble_address": "YOUR_BLE_ADDRESS",
        "hostname": null
    }
}
```

### Connection Types

| Type | Description | Use Case |
|------|-------------|----------|
| `auto` | Auto-detect best available | Recommended default |
| `serial` | USB serial connection | Most reliable |
| `ble` | Bluetooth Low Energy | Wireless, no cable |
| `tcp` | Network connection | Remote devices |

## Snowflake Setup

### 1. Create the Target Table

```sql
CREATE OR REPLACE TABLE DEMO.DEMO.MESHTASTIC_DATA (
    -- Metadata
    ingested_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    packet_type STRING,
    
    -- Source/Destination
    from_id STRING,
    from_num NUMBER,
    to_id STRING,
    to_num NUMBER,
    channel NUMBER,
    
    -- Signal Quality
    rx_snr FLOAT,
    rx_rssi FLOAT,
    hop_limit NUMBER,
    hop_start NUMBER,
    
    -- GPS Position
    latitude FLOAT,
    longitude FLOAT,
    altitude NUMBER,
    altitude_hae NUMBER,
    altitude_geoidal_separation NUMBER,
    ground_speed NUMBER,
    ground_track NUMBER,
    precision_bits NUMBER,
    sats_in_view NUMBER,
    pdop NUMBER,
    hdop NUMBER,
    vdop NUMBER,
    gps_timestamp NUMBER,
    fix_quality NUMBER,
    fix_type NUMBER,
    position_source STRING,
    seq_number NUMBER,
    
    -- Text Messages
    text_message STRING,
    
    -- Device Telemetry
    battery_level NUMBER,
    voltage FLOAT,
    channel_utilization FLOAT,
    air_util_tx FLOAT,
    uptime_seconds NUMBER,
    
    -- Environmental Sensors
    temperature FLOAT,
    temperature_f FLOAT,
    relative_humidity FLOAT,
    barometric_pressure FLOAT,
    gas_resistance FLOAT,
    iaq NUMBER,
    
    -- Light Sensors
    lux FLOAT,
    white_lux FLOAT,
    ir_lux FLOAT,
    uv_lux FLOAT,
    
    -- Weather
    wind_direction NUMBER,
    wind_speed FLOAT,
    wind_gust FLOAT,
    
    -- Other Sensors
    weight FLOAT,
    distance FLOAT,
    
    -- Air Quality
    pm10_standard NUMBER,
    pm25_standard NUMBER,
    pm100_standard NUMBER,
    pm10_environmental NUMBER,
    pm25_environmental NUMBER,
    pm100_environmental NUMBER,
    co2 NUMBER,
    
    -- Power Monitoring
    ch1_voltage FLOAT,
    ch1_current FLOAT,
    ch2_voltage FLOAT,
    ch2_current FLOAT,
    ch3_voltage FLOAT,
    ch3_current FLOAT,
    
    -- Raw Data
    raw_packet VARIANT
);
```

### 2. Create the Streaming Pipe

```sql
CREATE OR REPLACE PIPE DEMO.DEMO.MESHTASTIC_STREAM_PIPE
    AS COPY INTO DEMO.DEMO.MESHTASTIC_DATA
    FROM (SELECT * FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')));
```

### 3. Create PAT Token

```sql
-- In Snowsight: Admin > Security > Programmatic Access Tokens
-- Or via SQL:
ALTER USER your_user SET PROGRAMMATIC_ACCESS_TOKEN = TRUE;
```

## Usage

### Start the Streamer

```bash
# Activate virtual environment
source .venv/bin/activate

# Run with auto-detection
python meshtastic_snowflake_streamer.py

# Or specify connection type
# Edit snowflake_config.json: "connection_type": "serial"
```

### Expected Output

```
======================================================================
MESHTASTIC-SNOWFLAKE STREAMER - SNOWPIPE STREAMING V2 MODE
Using ONLY Snowpipe Streaming v2 REST API - NO SQL INSERTs
======================================================================
==================================================
SCANNING FOR MESHTASTIC DEVICES
==================================================
Scanning for serial devices...
Found serial device: /dev/cu.usbmodem13301 - T1000-E
Scanning for Bluetooth devices (10.0s)...
Found BLE device: Meshtastic_4b14 (93C61E0F-855D-AECB-05B1-3C5193B22964)
Scan complete: 1 serial, 1 BLE devices found
==================================================
Connected via BLE: 93C61E0F-855D-AECB-05B1-3C5193B22964
Position from !b9d44b14: lat=40.291533, lon=-74.527539, alt=44
Telemetry from !b9d44b14: temp=29.5, battery=101%, voltage=3.92V
Successfully appended 2 rows
```

### Stop the Streamer

Press `Ctrl+C` for graceful shutdown:
```
Received signal 2, shutting down gracefully...
Streaming worker stopped
```

## Dashboard

### Run Locally

```bash
source .venv/bin/activate
streamlit run streamlit_app.py
```

### Deploy to Snowflake (Streamlit in Snowflake)

```sql
-- Upload streamlit_app.py to a stage
PUT file:///path/to/streamlit_app.py @DEMO.DEMO.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Create Streamlit app
CREATE OR REPLACE STREAMLIT DEMO.DEMO.MESHTASTIC_DASHBOARD
    ROOT_LOCATION = '@DEMO.DEMO.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH';
```

### Dashboard Features

| Tab | Description |
|-----|-------------|
| **Live Map** | Interactive map with device locations and movement tracks |
| **Device Status** | Battery levels, voltage, uptime, signal quality |
| **Environmental** | Temperature, humidity, pressure charts |
| **GPS Details** | Position accuracy, satellites, altitude profiles |
| **Analytics** | Packet distribution, traffic patterns, node activity |
| **Raw Data** | Browse and export raw packet data |
| **Slack** | Send manual alerts and device status to Slack |

## Slack Integration

The streamer and dashboard support Slack notifications for real-time alerts.

### Setup Slack Webhook

1. Go to [Slack API Apps](https://api.slack.com/apps)
2. Create a new app or select existing
3. Enable **Incoming Webhooks**
4. Create a webhook URL for your channel
5. Add the webhook URL to config

### Configure in `snowflake_config.json`

```json
{
    "slack": {
        "enabled": true,
        "webhook_url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
        "channel": "#meshtastic-alerts",
        "low_battery_threshold": 20,
        "notify_position": false
    }
}
```

### Alert Types

| Alert | Trigger | Description |
|-------|---------|-------------|
| **Low Battery** | Battery ≤ threshold | Automatic alert when device battery is low |
| **Position Update** | New GPS fix | Optional notification for location changes |
| **Manual Alert** | Dashboard button | Send custom alerts from Slack tab |

### Dashboard Slack Features

- Configure webhook URL in sidebar
- Test Slack connection
- Send manual alerts with custom messages
- Share device status summaries

## Data Captured

### GPS Position (every 15 min default)
| Field | Description |
|-------|-------------|
| latitude | Decimal degrees |
| longitude | Decimal degrees |
| altitude | Meters above sea level |
| ground_speed | Meters per second |
| ground_track | Heading in degrees |
| sats_in_view | Number of GPS satellites |
| pdop/hdop/vdop | Dilution of precision values |
| gps_timestamp | Unix timestamp from GPS |

### Device Telemetry (every 30 sec)
| Field | Description |
|-------|-------------|
| battery_level | Percentage (0-100) |
| voltage | Battery voltage |
| uptime_seconds | Device uptime |
| channel_utilization | LoRa channel usage % |
| air_util_tx | Transmit air utilization |

### Environmental (every 30 min default)
| Field | Description |
|-------|-------------|
| temperature | Celsius |
| relative_humidity | Percentage |
| barometric_pressure | Pascals |

## Project Structure

```
meshtastic/
├── meshtastic_snowflake_streamer.py  # Main orchestrator
├── meshtastic_interface.py           # Device connection & parsing
├── snowpipe_streaming_client.py      # Snowpipe v2 REST client
├── streamlit_app.py                  # Dashboard
├── snowflake_config.json             # Configuration
├── test_packet_parsing.py            # Unit tests
└── README.md                         # This file
```

## Troubleshooting

### BLE Connection Issues
```bash
# Scan for devices manually
python -c "from meshtastic_interface import MeshtasticReceiver; r=MeshtasticReceiver(); print(r.scan_ble_devices())"
```

### No GPS Data
- Ensure device is outdoors with clear sky view
- Wait 1-2 minutes for cold start GPS fix
- Check position broadcasting is enabled in device settings

### Snowpipe Errors
- Verify PAT token is valid and not expired
- Check table schema matches expected columns
- Ensure pipe is in RUNNING state: `SHOW PIPES`

### Device in Boot Mode
- T1000-E shows as "T1000-E-BOOT" when in bootloader
- Press reset button or power cycle the device

## API Reference

### MeshtasticReceiver

```python
from meshtastic_interface import MeshtasticReceiver

# Auto-detect and connect
receiver = MeshtasticReceiver(connection_type='auto')
receiver.connect()

# Or specify connection
receiver = MeshtasticReceiver(
    connection_type='ble',
    device_path='93C61E0F-855D-AECB-05B1-3C5193B22964'
)

# Scan for devices
serial_devices = receiver.scan_serial_devices()
ble_devices = receiver.scan_ble_devices()
all_devices = receiver.scan_all_devices()
```

### SnowpipeStreamingClient

```python
from snowpipe_streaming_client import SnowpipeStreamingClient

client = SnowpipeStreamingClient('snowflake_config.json')
client.discover_ingest_host()
client.open_channel()

rows = [{'packet_type': 'test', 'from_id': '!test'}]
client.insert_rows(rows)

client.close_channel()
```

## License

MIT License - See LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## Support

- Meshtastic Documentation: https://meshtastic.org/docs/
- Snowpipe Streaming: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview
- SenseCAP T1000-E: https://wiki.seeedstudio.com/sensecap_t1000_e/
