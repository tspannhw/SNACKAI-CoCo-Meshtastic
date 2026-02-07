# Meshtastic to Snowflake Streaming Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![UV](https://img.shields.io/badge/uv-package%20manager-blueviolet)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

Stream data from Meshtastic mesh network devices (like the SenseCAP Card Tracker T1000-E) to Snowflake using Snowpipe Streaming v2 High-Speed REST API.

## Features

- 📡 Real-time streaming of Meshtastic packets to Snowflake
- 🔌 Support for Serial, TCP, and BLE connections to Meshtastic devices
- 🔐 JWT or PAT authentication with Snowflake
- 📦 Batched inserts for efficiency
- 🗺️ Position, telemetry, and text message parsing
- 📊 Pre-built Snowflake views, Streamlit dashboard, and Semantic View
- 🧪 Comprehensive test suite

## Quick Start with UV

[UV](https://github.com/astral-sh/uv) is a fast Python package manager. Install it first:

```bash
# Install UV (macOS/Linux)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with Homebrew
brew install uv

# Or with pip
pip install uv
```

### 1. Setup Project

```bash
cd meshtastic

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install core dependencies
uv pip install -e .

# Install with all extras (dev, streamlit, notebook)
uv pip install -e ".[all]"
```

### 2. Generate RSA Keys for Snowflake Auth

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Get the formatted public key for Snowflake
PUBK=$(cat ./rsa_key.pub | grep -v KEY- | tr -d '\012')
echo "ALTER USER YOUR_USER SET RSA_PUBLIC_KEY='$PUBK';"
```

### 3. Configure Snowflake

```bash
cp snowflake_config.json.template snowflake_config.json
# Edit snowflake_config.json with your credentials
```

Run the DDL in Snowflake:
```sql
-- In Snowsight or via snow CLI
-- Creates table, pipe, and views
SOURCE snowflake_ddl.sql;

-- Create semantic view for Cortex Analyst
SOURCE semantic_view.sql;
```

### 4. Test Device Connection

```bash
# Auto-detect serial device
python test_sensecap.py

# Specific port
python test_sensecap.py -p /dev/ttyUSB0

# TCP/WiFi connection
python test_sensecap.py -t tcp -H 192.168.1.100

# Bluetooth (BLE) connections
python test_sensecap.py --scan-ble                    # Scan for BLE devices
python test_sensecap.py -t ble                        # BLE with auto-detect
python test_sensecap.py -t ble -p AA:BB:CC:DD:EE:FF   # BLE with MAC address

# Wait for environmental data (temperature, humidity)
python test_sensecap.py -w 60 -o readings.json        # Wait 60s for telemetry
python test_sensecap.py -t ble -w 120 -o data.json    # BLE + wait + export
```

### 5. Run the Streamer

```bash
python meshtastic_snowflake_streamer.py

# Or with custom config
MESHTASTIC_CONFIG=/path/to/config.json python meshtastic_snowflake_streamer.py
```

## Project Structure

```
meshtastic/
├── pyproject.toml                    # UV/pip project config
├── snowflake_config.json.template    # Config template
├── .gitignore                        # Git ignore patterns
│
├── # Core Modules
├── meshtastic_snowflake_streamer.py  # Main streaming application
├── meshtastic_interface.py           # Meshtastic device connection
├── snowpipe_streaming_client.py      # Snowpipe Streaming v2 REST API
├── snowflake_jwt_auth.py             # JWT/PAT authentication
│
├── # Testing & Tools
├── test_sensecap.py                  # SenseCAP device tester
├── tests/                            # pytest test suite
│   ├── __init__.py
│   └── test_meshtastic_streaming.py
│
├── # Snowflake Assets
├── snowflake_ddl.sql                 # Table, pipe, views DDL
├── semantic_view.sql                 # Cortex Analyst semantic view
├── EXAMPLE_PROMPTS.md                # 50 example prompts for semantic view
│
├── # Analytics & Visualization
├── streamlit_app.py                  # Streamlit dashboard for Snowflake
└── meshtastic_analysis.ipynb         # Jupyter notebook for analysis
```

## Configuration

### snowflake_config.json

```json
{
    "account": "YOUR_ACCOUNT",
    "user": "YOUR_USER",
    "role": "ACCOUNTADMIN",
    "private_key_file": "rsa_key.p8",
    "database": "DEMO",
    "schema": "DEMO",
    "table": "MESHTASTIC_DATA",
    "batch_size": 10,
    "flush_interval_seconds": 5,
    "meshtastic": {
        "connection_type": "serial",
        "device_path": null,
        "hostname": null
    }
}
```

| Field | Description |
|-------|-------------|
| `account` | Snowflake account identifier (e.g., `xy12345`) |
| `user` | Snowflake username |
| `role` | Role for authentication |
| `private_key_file` | Path to RSA private key (JWT auth) |
| `pat` | Programmatic Access Token (alternative to JWT) |
| `database` | Target database |
| `schema` | Target schema |
| `table` | Target table name |
| `batch_size` | Messages per batch (default: 10) |
| `flush_interval_seconds` | Max seconds between flushes |
| `meshtastic.connection_type` | `serial`, `tcp`, or `ble` |
| `meshtastic.device_path` | Serial port or BLE MAC address |
| `meshtastic.hostname` | TCP hostname for WiFi connection |

## Data Model

### MESHTASTIC_DATA Table

| Column | Type | Description |
|--------|------|-------------|
| `ingested_at` | TIMESTAMP_TZ | When data was ingested |
| `packet_type` | VARCHAR | position, telemetry, text, raw |
| `from_id` | VARCHAR | Source node ID (!xxxxxxxx) |
| `to_id` | VARCHAR | Destination (^all = broadcast) |
| `rx_snr` | FLOAT | Signal-to-noise ratio (dB) |
| `rx_rssi` | FLOAT | Signal strength (dBm) |
| `latitude` | FLOAT | GPS latitude |
| `longitude` | FLOAT | GPS longitude |
| `altitude` | FLOAT | GPS altitude (meters) |
| `battery_level` | NUMBER | Battery % (0-100) |
| `voltage` | FLOAT | Battery voltage |
| `temperature` | FLOAT | Temperature (°C) |
| `relative_humidity` | FLOAT | Humidity (%) |
| `barometric_pressure` | FLOAT | Pressure (hPa) |
| `text_message` | VARCHAR | Chat message content |
| `raw_packet` | VARIANT | Full packet JSON |

### Pre-built Views

- `MESHTASTIC_POSITIONS` - GPS position tracks
- `MESHTASTIC_TELEMETRY` - Device health metrics
- `MESHTASTIC_MESSAGES` - Text chat messages
- `MESHTASTIC_ACTIVE_NODES` - Node summary with last position
- `MESHTASTIC_HOURLY_STATS` - Hourly traffic aggregates

### Semantic View for Cortex Analyst

```sql
-- Query with natural language
SELECT * FROM TABLE(
    SNOWFLAKE.CORTEX.COMPLETE(
        'What is the average battery level across all nodes?',
        DEMO.DEMO.MESHTASTIC_SEMANTIC_VIEW
    )
);
```

See `EXAMPLE_PROMPTS.md` for 50+ example queries.

## Running Tests

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest

# With coverage
pytest --cov=. --cov-report=html

# Run specific test
pytest tests/test_meshtastic_streaming.py -v
```

## Streamlit Dashboard

Deploy to Snowflake:

```bash
# Upload to stage
snow stage copy streamlit_app.py @DEMO.DEMO.STREAMLIT_STAGE

# Create Streamlit app in Snowflake
snow streamlit create meshtastic_dashboard \
    --file @DEMO.DEMO.STREAMLIT_STAGE/streamlit_app.py
```

Or run locally:
```bash
uv pip install -e ".[streamlit]"
streamlit run streamlit_app.py
```

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  Meshtastic Device  │     │   Python Streamer   │     │     Snowflake       │
│  (SenseCAP T1000-E) │────▶│                     │────▶│                     │
│                     │     │ • Serial/TCP/BLE    │     │ • Snowpipe Stream   │
│  • GPS (AG3335)     │     │ • Packet parsing    │     │ • MESHTASTIC_DATA   │
│  • LoRa (LR1110)    │     │ • Batch buffering   │     │ • Semantic View     │
│  • Sensors          │     │ • JWT Auth          │     │ • Streamlit App     │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
```

## Supported Devices

| Device | Radio | MCU | GPS | Notes |
|--------|-------|-----|-----|-------|
| SenseCAP T1000-E | LR1110 | nRF52840 | AG3335 | Credit card size tracker |
| Heltec LoRa 32 | SX1262 | ESP32 | Optional | WiFi + BLE |
| LILYGO T-Beam | SX1262 | ESP32 | NEO-6M | Built-in GPS |
| RAK WisBlock | SX1262 | nRF52840 | Optional | Modular design |

## Troubleshooting

### BLE Connection Issues
```bash
# First scan for devices
python test_sensecap.py --scan-ble

# Default BLE pairing PIN: 123456

# macOS: Grant Bluetooth permissions in System Settings > Privacy & Security
# Linux: May need bluez package: sudo apt install bluez
# T1000-E: Press button to wake device before connecting
```

### UV Issues
```bash
# Reinstall from scratch
rm -rf .venv uv.lock
uv venv
uv pip install -e ".[all]"
```

### JWT Authentication Failed
1. Verify public key: `DESCRIBE USER your_user;`
2. Check key format matches Snowflake requirements
3. Try PAT authentication instead (simpler)

### No Messages Received
1. Ensure device is in range of other mesh nodes
2. Check serial port permissions: `sudo chmod 666 /dev/ttyUSB0`
3. Verify Meshtastic firmware is installed
4. Double-press button on T1000-E to force position update

### Connection Timeout
1. Check network allows outbound to Snowflake
2. Verify account identifier format
3. Test: `curl -I https://YOUR_ACCOUNT.snowflakecomputing.com`

## License

Apache 2.0

## Resources

- [Meshtastic Python API](https://python.meshtastic.org/)
- [SenseCAP T1000-E Documentation](https://wiki.seeedstudio.com/sensecap_t1000_e/)
- [Snowpipe Streaming v2 REST API](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api)
- [Snowflake Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Seeed Sense Cap T1000E Meshtastic Device](https://wiki.seeedstudio.com/sensecap_t1000_e/)
