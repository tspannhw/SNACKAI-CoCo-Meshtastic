# Meshtastic Dashboard

A Pac-Man themed React dashboard for visualizing Meshtastic mesh network data stored in Snowflake.

## Features

- 🎮 **Pac-Man Arcade Styling** - Retro arcade theme with animated elements
- ⏱️ **Auto-Refresh** - Updates every 2 minutes with countdown timer
- 📊 **Real-time Stats** - Messages, devices, battery, temperature, positions, SNR
- 📋 **Packet Browser** - View telemetry, position, nodeinfo packets
- 🔌 **MQTT Integration** - Consume data from public Meshtastic MQTT server
- ❄️ **Snowflake Backend** - Queries semantic views for analytics
- 📝 **Comprehensive Logging** - Structured logging with rotation
- ✅ **Validation** - Data validation and health checks

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MESHTASTIC DASHBOARD ARCHITECTURE                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Meshtastic  │     │   Public     │     │   Private    │
│   Devices    │────▶│ MQTT Broker  │     │ MQTT Broker  │
│  (LoRa Mesh) │     │mqtt.meshtastic│    │  (optional)  │
└──────────────┘     └──────┬───────┘     └──────┬───────┘
                           │                     │
                           ▼                     ▼
              ┌────────────────────────────────────────────┐
              │          MQTT Consumer (Python)            │
              │  ┌─────────────────────────────────────┐   │
              │  │ • Subscribe to msh/+/2/json/#       │   │
              │  │ • Parse JSON payloads               │   │
              │  │ • Validate & transform data         │   │
              │  │ • Buffer messages                   │   │
              │  └─────────────────────────────────────┘   │
              └────────────────────┬───────────────────────┘
                                   │
                                   ▼
              ┌────────────────────────────────────────────┐
              │         Snowpipe Streaming v2              │
              │  ┌─────────────────────────────────────┐   │
              │  │ • Batch inserts                     │   │
              │  │ • High-throughput ingestion         │   │
              │  │ • At-least-once delivery            │   │
              │  └─────────────────────────────────────┘   │
              └────────────────────┬───────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SNOWFLAKE                                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        DEMO.DEMO Schema                                │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐    │  │
│  │  │ MESHTASTIC_DATA │  │ MESHTASTIC_     │  │ MESHTASTIC_         │    │  │
│  │  │ (Base Table)    │  │ SEMANTIC_VIEW   │  │ STREAM_PIPE         │    │  │
│  │  └────────┬────────┘  └────────┬────────┘  └─────────────────────┘    │  │
│  │           │                    │                                       │  │
│  │           ▼                    ▼                                       │  │
│  │  ┌─────────────────────────────────────────────────────────────┐      │  │
│  │  │                    Materialized Views                        │      │  │
│  │  │  • MESHTASTIC_POSITIONS  - GPS coordinates                   │      │  │
│  │  │  • MESHTASTIC_TELEMETRY  - Battery, temp, sensors            │      │  │
│  │  │  • MESHTASTIC_MESSAGES   - Text messages                     │      │  │
│  │  │  • MESHTASTIC_ACTIVE_NODES - Node summary                    │      │  │
│  │  │  • MESHTASTIC_HOURLY_STATS - Aggregated metrics              │      │  │
│  │  │  • MESHTASTIC_WEATHER    - Environmental data                │      │  │
│  │  └─────────────────────────────────────────────────────────────┘      │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
              ┌────────────────────────────────────────────┐
              │           Flask API Server                  │
              │  ┌─────────────────────────────────────┐   │
              │  │ Endpoints:                          │   │
              │  │ • GET /api/meshtastic - Raw data    │   │
              │  │ • GET /api/stats - Aggregated stats │   │
              │  │ • GET /api/positions - GPS data     │   │
              │  │ • GET /api/telemetry - Sensor data  │   │
              │  │ • GET /api/health - Health check    │   │
              │  │ • GET /api/semantic/* - SV queries  │   │
              │  └─────────────────────────────────────┘   │
              └────────────────────┬───────────────────────┘
                                   │
                                   ▼
              ┌────────────────────────────────────────────┐
              │         React Dashboard (Pac-Man UI)        │
              │  ┌─────────────────────────────────────┐   │
              │  │ • Real-time stats display           │   │
              │  │ • Packet browser table              │   │
              │  │ • 2-minute auto-refresh             │   │
              │  │ • Animated Pac-Man theme            │   │
              │  │ • Responsive design                 │   │
              │  └─────────────────────────────────────┘   │
              └────────────────────────────────────────────┘
                                   │
                                   ▼
              ┌────────────────────────────────────────────┐
              │              Web Browser                    │
              │         http://localhost:3000               │
              └────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                            SUPPORTING SERVICES                               │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │   Logging       │  │   Validation    │  │   Monitoring    │             │
│  │  (Rotating)     │  │  (Pydantic)     │  │  (Health API)   │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │   manage.sh     │  │   Slack Alerts  │  │   Unit Tests    │             │
│  │  start/stop     │  │  (optional)     │  │  (pytest)       │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. Meshtastic devices broadcast packets over LoRa mesh                     │
│  2. Gateway nodes with internet publish to MQTT broker                      │
│  3. MQTT Consumer subscribes to JSON topics (msh/+/2/json/#)                │
│  4. Messages validated, transformed, buffered                               │
│  5. Snowpipe Streaming v2 ingests batches to Snowflake                      │
│  6. Views provide structured access to telemetry, positions, etc.           │
│  7. Semantic View enables natural language queries via Cortex Analyst       │
│  8. Flask API serves data to React dashboard                                │
│  9. Dashboard auto-refreshes every 2 minutes                                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Start all services
./manage.sh start

# Check status
./manage.sh status

# View logs
./manage.sh logs

# Stop all services
./manage.sh stop
```

## Installation

### Prerequisites

- Python 3.9+
- Node.js 18+
- Snowflake account with connection configured
- (Optional) Access to Meshtastic MQTT broker

### Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Node.js dependencies
cd meshtastic-dashboard
npm install
npm run build
cd ..

# Configure Snowflake connection
export SNOWFLAKE_CONNECTION_NAME=your_connection

# Start services
./manage.sh start
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SNOWFLAKE_CONNECTION_NAME` | Snowflake connection name | `tspann1` |
| `MQTT_BROKER` | MQTT broker hostname | `mqtt.meshtastic.org` |
| `MQTT_PORT` | MQTT broker port | `1883` |
| `MQTT_TOPIC` | MQTT topic pattern | `msh/+/2/json/#` |
| `API_PORT` | Flask API port | `5000` |
| `DASHBOARD_PORT` | React dashboard port | `3000` |
| `LOG_LEVEL` | Logging level | `INFO` |

### snowflake_config.json

```json
{
    "account": "YOUR_ACCOUNT",
    "database": "DEMO",
    "schema": "DEMO",
    "table": "MESHTASTIC_DATA",
    "batch_size": 10,
    "flush_interval_seconds": 5
}
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/meshtastic` | GET | Recent packets (limit 50) |
| `/api/stats` | GET | Aggregated statistics |
| `/api/positions` | GET | GPS position data |
| `/api/telemetry` | GET | Device telemetry |
| `/api/messages` | GET | Text messages |
| `/api/nodes` | GET | Active nodes summary |
| `/api/weather` | GET | Environmental data |
| `/api/health` | GET | Health check |
| `/api/semantic/query` | POST | Semantic view query |

## Semantic Views

The dashboard leverages Snowflake semantic views for analytics:

### MESHTASTIC_SEMANTIC_VIEW

Provides natural language query capabilities via Cortex Analyst:

**Dimensions:** CHANNEL, FROM_ID, FROM_NUM, INGESTED_AT, PACKET_TYPE, TEXT_MESSAGE, TO_ID, TO_NUM

**Facts:** AIR_UTIL_TX, ALTITUDE, BAROMETRIC_PRESSURE, BATTERY_LEVEL, CHANNEL_UTILIZATION, GROUND_SPEED, HOP_LIMIT, LATITUDE, LONGITUDE, RELATIVE_HUMIDITY, RX_RSSI, RX_SNR, SATS_IN_VIEW, TEMPERATURE, UPTIME_SECONDS, VOLTAGE

**Metrics:** AVG_BATTERY, AVG_SNR, AVG_TEMPERATURE, LOW_BATTERY_DEVICES, MESSAGE_COUNT, POSITION_COUNT, TELEMETRY_COUNT, TOTAL_PACKETS, UNIQUE_NODES

### Example Queries

```sql
-- Active nodes in last hour
SELECT * FROM DEMO.DEMO.MESHTASTIC_ACTIVE_NODES 
WHERE last_seen > DATEADD(hour, -1, CURRENT_TIMESTAMP());

-- Hourly packet statistics
SELECT * FROM DEMO.DEMO.MESHTASTIC_HOURLY_STATS 
ORDER BY hour DESC LIMIT 24;

-- Recent positions with good signal
SELECT * FROM DEMO.DEMO.MESHTASTIC_POSITIONS 
WHERE rx_snr > -15 
ORDER BY ingested_at DESC LIMIT 100;

-- Low battery devices
SELECT from_id, last_battery, last_seen 
FROM DEMO.DEMO.MESHTASTIC_ACTIVE_NODES 
WHERE last_battery < 20;
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html

# Run specific test file
pytest tests/test_validation.py -v
```

## Project Structure

```
meshtastic-dashboard/
├── README.md                    # This documentation
├── ARCHITECTURE.md              # Detailed architecture docs
├── manage.sh                    # Service management script
├── requirements.txt             # Python dependencies
├── api_server.py               # Flask API server
├── mqtt_consumer.py            # MQTT consumer for public broker
├── validation.py               # Data validation schemas
├── tests/
│   ├── __init__.py
│   ├── test_validation.py      # Validation tests
│   ├── test_api.py             # API endpoint tests
│   └── test_mqtt.py            # MQTT consumer tests
├── logs/                        # Log files directory
├── src/
│   ├── App.tsx                 # Main React component
│   └── App.css                 # Pac-Man themed styles
├── public/
│   └── index.html              # HTML template
└── build/                       # Production build output
```

## Logging

Logs are written to `logs/` directory with rotation:

- `api_server.log` - API server logs
- `mqtt_consumer.log` - MQTT consumer logs
- `dashboard.log` - Combined dashboard logs

Log format:
```
2026-03-12 10:15:30 INFO [api_server] Request: GET /api/meshtastic - 200 - 45ms
```

## Troubleshooting

### Common Issues

1. **Snowflake connection fails**
   - Verify `SNOWFLAKE_CONNECTION_NAME` is set
   - Check connection with `cortex connections list`

2. **MQTT consumer not receiving data**
   - Verify broker is reachable: `mosquitto_sub -h mqtt.meshtastic.org -t 'msh/#'`
   - Check firewall allows port 1883

3. **Dashboard shows no data**
   - Check API health: `curl http://localhost:5000/api/health`
   - Verify Snowflake has data: `SELECT COUNT(*) FROM DEMO.DEMO.MESHTASTIC_DATA`

## License

MIT License

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `pytest tests/ -v`
4. Submit a pull request
