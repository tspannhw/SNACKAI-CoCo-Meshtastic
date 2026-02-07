-- Semantic View for Meshtastic Data
-- Enables natural language queries via Cortex Analyst

USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO;
USE SCHEMA DEMO;

CREATE OR REPLACE SEMANTIC VIEW MESHTASTIC_SEMANTIC_VIEW
  TABLES (
    mesh AS DEMO.DEMO.MESHTASTIC_DATA PRIMARY KEY (ingested_at, from_id)
  )
  FACTS (
    mesh.rx_snr AS rx_snr COMMENT = 'Signal-to-noise ratio in dB',
    mesh.rx_rssi AS rx_rssi COMMENT = 'Received signal strength indicator in dBm',
    mesh.latitude AS latitude COMMENT = 'GPS latitude coordinate',
    mesh.longitude AS longitude COMMENT = 'GPS longitude coordinate',
    mesh.altitude AS altitude COMMENT = 'GPS altitude in meters',
    mesh.ground_speed AS ground_speed COMMENT = 'Ground speed in meters per second',
    mesh.battery_level AS battery_level COMMENT = 'Battery level percentage 0-100',
    mesh.voltage AS voltage COMMENT = 'Battery voltage in volts',
    mesh.temperature AS temperature COMMENT = 'Temperature in Celsius',
    mesh.relative_humidity AS relative_humidity COMMENT = 'Relative humidity percentage',
    mesh.barometric_pressure AS barometric_pressure COMMENT = 'Barometric pressure in hPa',
    mesh.channel_utilization AS channel_utilization COMMENT = 'LoRa channel utilization percentage',
    mesh.air_util_tx AS air_util_tx COMMENT = 'Airtime utilization TX percentage',
    mesh.uptime_seconds AS uptime_seconds COMMENT = 'Device uptime in seconds',
    mesh.hop_limit AS hop_limit COMMENT = 'LoRa packet hop limit',
    mesh.sats_in_view AS sats_in_view COMMENT = 'Number of GPS satellites in view'
  )
  DIMENSIONS (
    mesh.ingested_at AS ingested_at COMMENT = 'Timestamp when data was ingested into Snowflake',
    mesh.packet_type AS packet_type COMMENT = 'Type of packet: position, telemetry, text, or raw',
    mesh.from_id AS from_id COMMENT = 'Source node ID in format !xxxxxxxx',
    mesh.from_num AS from_num COMMENT = 'Source node number',
    mesh.to_id AS to_id COMMENT = 'Destination node ID or ^all for broadcast',
    mesh.to_num AS to_num COMMENT = 'Destination node number',
    mesh.channel AS channel COMMENT = 'LoRa channel number',
    mesh.text_message AS text_message COMMENT = 'Text message content if packet_type is text'
  )
  METRICS (
    mesh.total_packets AS COUNT(*) COMMENT = 'Total number of packets',
    mesh.unique_nodes AS COUNT(DISTINCT mesh.from_id) COMMENT = 'Count of unique mesh nodes',
    mesh.avg_snr AS AVG(mesh.rx_snr) COMMENT = 'Average signal-to-noise ratio',
    mesh.avg_rssi AS AVG(mesh.rx_rssi) COMMENT = 'Average received signal strength',
    mesh.avg_battery AS AVG(mesh.battery_level) COMMENT = 'Average battery level',
    mesh.min_battery AS MIN(mesh.battery_level) COMMENT = 'Minimum battery level',
    mesh.max_battery AS MAX(mesh.battery_level) COMMENT = 'Maximum battery level',
    mesh.avg_temperature AS AVG(mesh.temperature) COMMENT = 'Average temperature',
    mesh.position_count AS COUNT(CASE WHEN mesh.packet_type = 'position' THEN 1 END) COMMENT = 'Number of position packets',
    mesh.telemetry_count AS COUNT(CASE WHEN mesh.packet_type = 'telemetry' THEN 1 END) COMMENT = 'Number of telemetry packets',
    mesh.message_count AS COUNT(CASE WHEN mesh.packet_type = 'text' THEN 1 END) COMMENT = 'Number of text messages',
    mesh.total_uptime AS SUM(mesh.uptime_seconds) COMMENT = 'Total uptime across all readings'
  )
  COMMENT = 'Meshtastic mesh network data including GPS positions, device telemetry, text messages, and LoRa signal quality metrics';

-- Grant access
GRANT SELECT ON SEMANTIC VIEW DEMO.DEMO.MESHTASTIC_SEMANTIC_VIEW TO ROLE PUBLIC;

-- Verify creation
DESCRIBE SEMANTIC VIEW DEMO.DEMO.MESHTASTIC_SEMANTIC_VIEW;
