-- Snowflake DDL for Meshtastic Data Table
-- Run this in your Snowflake account before starting the streamer

USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO;
USE SCHEMA DEMO;

CREATE TABLE IF NOT EXISTS MESHTASTIC_DATA (
    ingested_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    packet_type VARCHAR(50),
    from_id VARCHAR(100),
    from_num NUMBER,
    to_id VARCHAR(100),
    to_num NUMBER,
    channel NUMBER,
    rx_snr FLOAT,
    rx_rssi FLOAT,
    hop_limit NUMBER,
    hop_start NUMBER,
    -- GPS
    latitude FLOAT,
    longitude FLOAT,
    altitude FLOAT,
    ground_speed FLOAT,
    ground_track FLOAT,
    sats_in_view NUMBER,
    pdop FLOAT,
    hdop FLOAT,
    vdop FLOAT,
    gps_timestamp NUMBER,
    precision_bits NUMBER,
    -- Messages
    text_message VARCHAR(500),
    -- Device Metrics
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
    lux FLOAT,
    white_lux FLOAT,
    ir_lux FLOAT,
    uv_lux FLOAT,
    wind_direction FLOAT,
    wind_speed FLOAT,
    wind_gust FLOAT,
    weight FLOAT,
    distance FLOAT,
    -- Air Quality
    pm10_standard FLOAT,
    pm25_standard FLOAT,
    pm100_standard FLOAT,
    pm10_environmental FLOAT,
    pm25_environmental FLOAT,
    pm100_environmental FLOAT,
    co2 FLOAT,
    -- Power Metrics
    ch1_voltage FLOAT,
    ch1_current FLOAT,
    ch2_voltage FLOAT,
    ch2_current FLOAT,
    ch3_voltage FLOAT,
    ch3_current FLOAT,
    -- Raw
    raw_packet VARIANT
);

CREATE OR REPLACE PIPE SNOWPIPE_STREAMING_MESHTASTIC
AS SELECT * FROM MESHTASTIC_DATA;

ALTER TABLE MESHTASTIC_DATA SET ENABLE_SCHEMA_EVOLUTION = TRUE;

CREATE OR REPLACE VIEW MESHTASTIC_POSITIONS AS
SELECT 
    ingested_at,
    from_id,
    from_num,
    latitude,
    longitude,
    altitude,
    ground_speed,
    sats_in_view,
    rx_snr,
    rx_rssi
FROM MESHTASTIC_DATA
WHERE packet_type = 'position'
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;

CREATE OR REPLACE VIEW MESHTASTIC_TELEMETRY AS
SELECT 
    ingested_at,
    from_id,
    from_num,
    battery_level,
    voltage,
    temperature,
    temperature_f,
    relative_humidity,
    barometric_pressure,
    gas_resistance,
    iaq,
    lux,
    uv_lux,
    wind_speed,
    wind_direction,
    pm25_standard,
    co2,
    channel_utilization,
    air_util_tx,
    uptime_seconds
FROM MESHTASTIC_DATA
WHERE packet_type = 'telemetry';

CREATE OR REPLACE VIEW MESHTASTIC_MESSAGES AS
SELECT 
    ingested_at,
    from_id,
    to_id,
    channel,
    text_message,
    rx_snr,
    rx_rssi
FROM MESHTASTIC_DATA
WHERE packet_type = 'text'
  AND text_message IS NOT NULL;

CREATE OR REPLACE VIEW MESHTASTIC_ACTIVE_NODES AS
SELECT 
    from_id,
    from_num,
    MAX(ingested_at) as last_seen,
    COUNT(*) as packet_count,
    AVG(rx_snr) as avg_snr,
    AVG(rx_rssi) as avg_rssi,
    MAX(latitude) as last_latitude,
    MAX(longitude) as last_longitude,
    MAX(battery_level) as last_battery,
    MAX(temperature) as last_temp_c,
    MAX(relative_humidity) as last_humidity
FROM MESHTASTIC_DATA
WHERE from_id IS NOT NULL
GROUP BY from_id, from_num;

-- Weather/Environmental View
CREATE OR REPLACE VIEW MESHTASTIC_WEATHER AS
SELECT 
    ingested_at,
    from_id,
    temperature,
    (temperature * 9/5) + 32 as temperature_f,
    relative_humidity,
    barometric_pressure,
    gas_resistance,
    iaq,
    lux,
    uv_lux,
    wind_speed,
    wind_direction,
    wind_gust,
    pm10_standard,
    pm25_standard,
    pm100_standard,
    co2
FROM MESHTASTIC_DATA
WHERE temperature IS NOT NULL 
   OR relative_humidity IS NOT NULL 
   OR barometric_pressure IS NOT NULL
   OR wind_speed IS NOT NULL
   OR pm25_standard IS NOT NULL;

CREATE OR REPLACE VIEW MESHTASTIC_HOURLY_STATS AS
SELECT 
    DATE_TRUNC('hour', ingested_at) as hour,
    packet_type,
    COUNT(*) as packet_count,
    COUNT(DISTINCT from_id) as unique_nodes,
    AVG(rx_snr) as avg_snr,
    AVG(rx_rssi) as avg_rssi
FROM MESHTASTIC_DATA
GROUP BY DATE_TRUNC('hour', ingested_at), packet_type
ORDER BY hour DESC;

COMMENT ON TABLE MESHTASTIC_DATA IS 'Raw Meshtastic mesh network data ingested via Snowpipe Streaming v2';
COMMENT ON VIEW MESHTASTIC_POSITIONS IS 'GPS position packets from Meshtastic nodes';
COMMENT ON VIEW MESHTASTIC_TELEMETRY IS 'Device telemetry (battery, temperature, etc.) from Meshtastic nodes';
COMMENT ON VIEW MESHTASTIC_MESSAGES IS 'Text messages sent over the Meshtastic mesh';
COMMENT ON VIEW MESHTASTIC_ACTIVE_NODES IS 'Summary of active nodes with last known position and battery';
COMMENT ON VIEW MESHTASTIC_HOURLY_STATS IS 'Hourly aggregated statistics by packet type';
