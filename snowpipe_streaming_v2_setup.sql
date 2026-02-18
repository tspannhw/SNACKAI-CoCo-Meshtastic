-- ============================================================================
-- SNOWPIPE STREAMING V2 HIGH-PERFORMANCE SETUP
-- Optimized for maximum throughput and minimum latency
-- ============================================================================

-- ===========================================
-- 1. TABLE OPTIMIZATIONS
-- ===========================================

-- Enable clustering for faster queries on streaming data
ALTER TABLE DEMO.DEMO.MESHTASTIC_DATA 
    CLUSTER BY (from_id, ingested_at);

-- Enable schema evolution for dynamic fields
ALTER TABLE DEMO.DEMO.MESHTASTIC_DATA 
    SET ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Enable change tracking for downstream processing
ALTER TABLE DEMO.DEMO.MESHTASTIC_DATA 
    SET CHANGE_TRACKING = TRUE;

-- Resume automatic clustering
ALTER TABLE DEMO.DEMO.MESHTASTIC_DATA RESUME RECLUSTER;

-- ===========================================
-- 2. HIGH-PERFORMANCE STREAMING PIPE
-- ===========================================

-- Create optimized HP pipe with pre-clustering
CREATE OR REPLACE PIPE DEMO.DEMO.MESHTASTIC_HP_STREAM_PIPE
    COMMENT = 'Snowpipe Streaming V2 High-Performance with pre-clustering'
AS
COPY INTO DEMO.DEMO.MESHTASTIC_DATA (
    INGESTED_AT, PACKET_TYPE, FROM_ID, FROM_NUM, TO_ID, TO_NUM, 
    CHANNEL, RX_SNR, RX_RSSI, HOP_LIMIT, HOP_START,
    LATITUDE, LONGITUDE, ALTITUDE, GROUND_SPEED, GROUND_TRACK,
    SATS_IN_VIEW, PDOP, HDOP, VDOP, GPS_TIMESTAMP, PRECISION_BITS,
    TEXT_MESSAGE, BATTERY_LEVEL, VOLTAGE, CHANNEL_UTILIZATION,
    AIR_UTIL_TX, UPTIME_SECONDS, TEMPERATURE, TEMPERATURE_F,
    RELATIVE_HUMIDITY, BAROMETRIC_PRESSURE, GAS_RESISTANCE, IAQ,
    LUX, WHITE_LUX, IR_LUX, UV_LUX, WIND_DIRECTION, WIND_SPEED,
    WIND_GUST, WEIGHT, DISTANCE, PM10_STANDARD, PM25_STANDARD,
    PM100_STANDARD, PM10_ENVIRONMENTAL, PM25_ENVIRONMENTAL,
    PM100_ENVIRONMENTAL, CO2, CH1_VOLTAGE, CH1_CURRENT,
    CH2_VOLTAGE, CH2_CURRENT, CH3_VOLTAGE, CH3_CURRENT, RAW_PACKET
)
FROM (
    SELECT 
        $1:ingested_at::TIMESTAMP_TZ,
        $1:packet_type::VARCHAR,
        $1:from_id::VARCHAR,
        $1:from_num::NUMBER,
        $1:to_id::VARCHAR,
        $1:to_num::NUMBER,
        $1:channel::NUMBER,
        $1:rx_snr::FLOAT,
        $1:rx_rssi::FLOAT,
        $1:hop_limit::NUMBER,
        $1:hop_start::NUMBER,
        $1:latitude::FLOAT,
        $1:longitude::FLOAT,
        $1:altitude::FLOAT,
        $1:ground_speed::FLOAT,
        $1:ground_track::FLOAT,
        $1:sats_in_view::NUMBER,
        $1:pdop::FLOAT,
        $1:hdop::FLOAT,
        $1:vdop::FLOAT,
        $1:gps_timestamp::NUMBER,
        $1:precision_bits::NUMBER,
        $1:text_message::VARCHAR,
        $1:battery_level::NUMBER,
        $1:voltage::FLOAT,
        $1:channel_utilization::FLOAT,
        $1:air_util_tx::FLOAT,
        $1:uptime_seconds::NUMBER,
        $1:temperature::FLOAT,
        $1:temperature_f::FLOAT,
        $1:relative_humidity::FLOAT,
        $1:barometric_pressure::FLOAT,
        $1:gas_resistance::FLOAT,
        $1:iaq::NUMBER,
        $1:lux::FLOAT,
        $1:white_lux::FLOAT,
        $1:ir_lux::FLOAT,
        $1:uv_lux::FLOAT,
        $1:wind_direction::FLOAT,
        $1:wind_speed::FLOAT,
        $1:wind_gust::FLOAT,
        $1:weight::FLOAT,
        $1:distance::FLOAT,
        $1:pm10_standard::FLOAT,
        $1:pm25_standard::FLOAT,
        $1:pm100_standard::FLOAT,
        $1:pm10_environmental::FLOAT,
        $1:pm25_environmental::FLOAT,
        $1:pm100_environmental::FLOAT,
        $1:co2::FLOAT,
        $1:ch1_voltage::FLOAT,
        $1:ch1_current::FLOAT,
        $1:ch2_voltage::FLOAT,
        $1:ch2_current::FLOAT,
        $1:ch3_voltage::FLOAT,
        $1:ch3_current::FLOAT,
        $1::VARIANT
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
)
CLUSTER_AT_INGEST_TIME = TRUE;

-- ===========================================
-- 3. MONITORING VIEWS
-- ===========================================

-- Create view for streaming performance monitoring
CREATE OR REPLACE VIEW DEMO.DEMO.V_STREAMING_PERFORMANCE AS
SELECT 
    'MESHTASTIC_DATA' as table_name,
    COUNT(*) as total_rows,
    MIN(ingested_at) as earliest_record,
    MAX(ingested_at) as latest_record,
    DATEDIFF(second, MAX(ingested_at), CURRENT_TIMESTAMP()) as data_lag_seconds,
    COUNT(DISTINCT from_id) as unique_nodes,
    COUNT(DISTINCT DATE_TRUNC('minute', ingested_at)) as active_minutes
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP());

-- ===========================================
-- 4. INTERACTIVE TABLE FOR REAL-TIME QUERIES
-- ===========================================

-- Create Interactive Table with 60-second refresh (minimum supported)
CREATE OR REPLACE INTERACTIVE TABLE DEMO.DEMO.MESHTASTIC_LIVE_AUTO
    CLUSTER BY (from_id)
    TARGET_LAG = '60 seconds'
    WAREHOUSE = INGEST
AS
SELECT 
    ingested_at,
    packet_type,
    from_id,
    to_id,
    channel,
    latitude,
    longitude,
    altitude,
    ground_speed,
    sats_in_view,
    battery_level,
    voltage,
    temperature,
    relative_humidity,
    rx_snr,
    rx_rssi,
    hop_limit,
    text_message
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- Node summary interactive table
CREATE OR REPLACE INTERACTIVE TABLE DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
    CLUSTER BY (node_id)
    TARGET_LAG = '60 seconds'
    WAREHOUSE = INGEST
AS
SELECT 
    from_id as node_id,
    COUNT(*) as packet_count,
    MAX(battery_level) as battery_level,
    ROUND(AVG(rx_snr), 2) as avg_snr,
    MAX(rx_rssi) as last_rssi,
    MAX(latitude) as latitude,
    MAX(longitude) as longitude,
    MAX(altitude) as altitude,
    MAX(temperature) as temperature,
    MAX(relative_humidity) as humidity,
    MAX(ingested_at) as last_seen,
    MIN(ingested_at) as first_seen,
    DATEDIFF(minute, MAX(ingested_at), CURRENT_TIMESTAMP()) as mins_ago
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
GROUP BY from_id;

-- ===========================================
-- 5. VERIFY SETUP
-- ===========================================

-- Check pipe status
SHOW PIPES LIKE '%MESHTASTIC%' IN SCHEMA DEMO.DEMO;

-- Check Interactive Tables
SHOW INTERACTIVE TABLES IN SCHEMA DEMO.DEMO;

-- Check table clustering
SHOW TABLES LIKE 'MESHTASTIC_DATA' IN SCHEMA DEMO.DEMO;

-- ===========================================
-- 6. CLIENT CONFIGURATION REFERENCE
-- ===========================================

/*
PYTHON SDK INSTALLATION:
------------------------
pip install snowpipe-streaming

ENVIRONMENT VARIABLES (set before running):
-------------------------------------------
export SS_ENABLE_METRICS=TRUE
export SS_METRICS_PORT=50000
export SS_METRICS_IP=0.0.0.0
export SS_LOG_LEVEL=warn

PROFILE.JSON:
-------------
{
  "authorization_type": "JWT",
  "url": "https://SFSENORTHAMERICA-TSPANN-AWS1.snowflakecomputing.com",
  "user": "kafkaguy",
  "account": "SFSENORTHAMERICA-TSPANN-AWS1",
  "private_key_file": "/path/to/rsa_key.p8",
  "role": "ACCOUNTADMIN"
}

PYTHON CLIENT CODE:
-------------------
from snowpipe_streaming import SnowpipeStreamingClient

client = SnowpipeStreamingClient(profile_path="profile.json")

# Open channel against HP pipe
channel = client.open_channel(
    pipe_name="DEMO.DEMO.MESHTASTIC_HP_STREAM_PIPE",
    channel_name="meshtastic_hp_channel"
)

# Batch insert for maximum throughput (5000+ rows per batch)
channel.insert_rows(rows)

PERFORMANCE TIPS:
-----------------
1. Use batch sizes of 1,000-10,000 rows
2. Open multiple channels (4-8) for parallel ingestion
3. Pre-clustering sorts data during ingestion
4. Target latency: 5-10 seconds end-to-end
5. Max throughput: 10 GB/s per table
*/
