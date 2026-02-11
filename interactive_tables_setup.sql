-- ============================================================================
-- Snowflake Interactive Tables & Warehouses for Meshtastic Mesh Network
-- ============================================================================
-- Interactive Tables provide sub-second query latency for high-concurrency
-- workloads like real-time dashboards and mobile applications.
--
-- REGION AVAILABILITY: Interactive Tables are only available in select AWS regions:
--   - us-east-1 (N. Virginia), us-west-2 (Oregon), us-east-2 (Ohio)
--   - ca-central-1 (Canada Central)
--   - ap-northeast-1 (Tokyo), ap-southeast-2 (Sydney)
--   - eu-central-1 (Frankfurt), eu-west-1 (Ireland), eu-west-2 (London)
--
-- LIMITATIONS:
--   - No UPDATE/DELETE (only INSERT OVERWRITE)
--   - 5-second query timeout on interactive warehouses
--   - Must use interactive warehouse to query interactive tables
--   - No streams, no dynamic tables as downstream
-- ============================================================================

-- Step 1: Set context
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE INGEST;  -- Use standard warehouse for DDL operations

-- ============================================================================
-- INTERACTIVE TABLE: Recent Meshtastic Data (Last 24 Hours)
-- ============================================================================
-- This table is optimized for low-latency queries on recent mesh network data.
-- CLUSTER BY improves performance for WHERE clauses on from_id and packet_type.

CREATE OR REPLACE INTERACTIVE TABLE MESHTASTIC_LIVE
    CLUSTER BY (from_id, packet_type)
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
    channel_utilization,
    air_util_tx,
    temperature,
    relative_humidity,
    barometric_pressure,
    rx_snr,
    rx_rssi,
    rx_time,
    hop_limit,
    hop_start,
    text_message,
    hardware_model,
    firmware_version,
    short_name,
    long_name
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- Verify table creation
DESCRIBE TABLE MESHTASTIC_LIVE;
SELECT COUNT(*) as row_count FROM MESHTASTIC_LIVE;

-- ============================================================================
-- INTERACTIVE TABLE: Auto-Refresh Version (Dynamic)
-- ============================================================================
-- This version automatically refreshes from the source table every 60 seconds.
-- Requires specifying a standard warehouse for refresh operations.

CREATE OR REPLACE INTERACTIVE TABLE MESHTASTIC_LIVE_AUTO
    CLUSTER BY (from_id, packet_type)
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
    text_message,
    short_name,
    long_name
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- ============================================================================
-- INTERACTIVE TABLE: Node Summary (Aggregated)
-- ============================================================================
-- Pre-aggregated summary for faster node status queries

CREATE OR REPLACE INTERACTIVE TABLE MESHTASTIC_NODE_SUMMARY
    CLUSTER BY (from_id)
    TARGET_LAG = '2 minutes'
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
    MAX(short_name) as short_name,
    MAX(long_name) as long_name,
    MAX(ingested_at) as last_seen,
    MIN(ingested_at) as first_seen,
    DATEDIFF(minute, MAX(ingested_at), CURRENT_TIMESTAMP()) as mins_ago
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
GROUP BY from_id;

-- ============================================================================
-- INTERACTIVE WAREHOUSE: Create and Configure
-- ============================================================================
-- Interactive warehouses are optimized for low-latency, high-concurrency queries.
-- They remain active by default (no auto-suspend).

CREATE OR REPLACE INTERACTIVE WAREHOUSE MESH_INTERACTIVE
    WAREHOUSE_SIZE = 'XSMALL'
    TABLES = (
        DEMO.DEMO.MESHTASTIC_LIVE,
        DEMO.DEMO.MESHTASTIC_LIVE_AUTO,
        DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
    );

-- Resume the warehouse (created in suspended state)
ALTER WAREHOUSE MESH_INTERACTIVE RESUME;

-- Verify warehouse status
SHOW WAREHOUSES LIKE 'MESH_INTERACTIVE';

ALTER WAREHOUSE MESH_INTERACTIVE SUSPEND;

-- https://docs.snowflake.com/en/user-guide/interactive

-- ============================================================================
-- ALTERNATIVE: Create Warehouse First, Then Add Tables
-- ============================================================================

-- Create empty interactive warehouse
-- CREATE OR REPLACE INTERACTIVE WAREHOUSE MESH_INTERACTIVE_V2
--     WAREHOUSE_SIZE = 'XSMALL';

-- Add tables later
-- ALTER WAREHOUSE MESH_INTERACTIVE_V2 ADD TABLES (
--     DEMO.DEMO.MESHTASTIC_LIVE,
--     DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
-- );

-- Remove tables if needed
-- ALTER WAREHOUSE MESH_INTERACTIVE_V2 DROP TABLES (DEMO.DEMO.MESHTASTIC_LIVE);

-- ============================================================================
-- QUERYING INTERACTIVE TABLES
-- ============================================================================
-- IMPORTANT: You must use the interactive warehouse to query interactive tables!

-- Switch to interactive warehouse
USE WAREHOUSE MESH_INTERACTIVE;

-- Query 1: Get all active nodes (last 10 minutes)
SELECT 
    node_id,
    battery_level,
    avg_snr,
    latitude,
    longitude,
    mins_ago,
    CASE 
        WHEN mins_ago <= 5 THEN 'Active'
        WHEN mins_ago <= 30 THEN 'Recent'
        WHEN mins_ago <= 60 THEN 'Stale'
        ELSE 'Offline'
    END as status
FROM MESHTASTIC_NODE_SUMMARY
WHERE mins_ago <= 60
ORDER BY last_seen DESC;

-- Query 2: Get recent packets by type
SELECT 
    from_id,
    packet_type,
    COUNT(*) as count,
    MAX(ingested_at) as latest
FROM MESHTASTIC_LIVE
WHERE ingested_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY from_id, packet_type
ORDER BY latest DESC;

-- Query 3: Get recent text messages
SELECT 
    ingested_at,
    from_id,
    text_message,
    rx_snr
FROM MESHTASTIC_LIVE
WHERE packet_type = 'text'
    AND text_message IS NOT NULL
ORDER BY ingested_at DESC
LIMIT 20;

-- Query 4: Node positions for map
SELECT 
    node_id,
    latitude,
    longitude,
    battery_level,
    mins_ago
FROM MESHTASTIC_NODE_SUMMARY
WHERE latitude IS NOT NULL
    AND longitude IS NOT NULL;

-- ============================================================================
-- PERFORMANCE TUNING
-- ============================================================================

-- Disable query result cache for benchmarking (optional)
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Increase concurrency for simple queries (default is warehouse-dependent)
ALTER WAREHOUSE MESH_INTERACTIVE SET MAX_CONCURRENCY_LEVEL = 16;

-- Check warehouse size recommendations:
-- < 500 GB working set: XSMALL
-- 500 GB - 1 TB: SMALL  
-- 1 TB - 2 TB: MEDIUM
-- 2 TB - 4 TB: LARGE
-- 4 TB - 8 TB: XLARGE

-- ============================================================================
-- MONITORING & MANAGEMENT
-- ============================================================================

-- Check interactive table refresh status
SELECT *
FROM TABLE(INFORMATION_SCHEMA.INTERACTIVE_TABLE_REFRESH_HISTORY(
    TABLE_NAME => 'MESHTASTIC_LIVE_AUTO',
    RESULT_LIMIT => 10
));

-- List all interactive tables
SHOW INTERACTIVE TABLES IN SCHEMA DEMO.DEMO;

-- Check table size and row count
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'DEMO'
    AND TABLE_NAME LIKE 'MESHTASTIC%';

-- ============================================================================
-- CLEANUP (if needed)
-- ============================================================================

-- Suspend interactive warehouse (saves costs, but causes latency on resume)
-- ALTER WAREHOUSE MESH_INTERACTIVE SUSPEND;

-- Drop interactive tables
-- DROP TABLE IF EXISTS MESHTASTIC_LIVE;
-- DROP TABLE IF EXISTS MESHTASTIC_LIVE_AUTO;
-- DROP TABLE IF EXISTS MESHTASTIC_NODE_SUMMARY;

-- Drop interactive warehouse
-- DROP WAREHOUSE IF EXISTS MESH_INTERACTIVE;

-- ============================================================================
-- BILLING NOTES
-- ============================================================================
-- Interactive warehouses:
--   - Minimum billing: 1 hour, then per-second
--   - No auto-suspend by default (runs continuously)
--   - Suspend/resume triggers new 1-hour minimum
--
-- Interactive tables:
--   - Standard storage costs
--   - May be larger than standard tables due to indexes
--   - Auto-refresh incurs standard warehouse compute costs
-- ============================================================================
