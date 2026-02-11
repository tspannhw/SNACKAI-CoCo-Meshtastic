-- Snowflake-Hosted MCP Server Setup for Meshtastic Agent
-- Deploy the MCP server as a Snowpark Container Service

-- 1. Create compute pool for the MCP server
CREATE COMPUTE POOL IF NOT EXISTS MESHTASTIC_MCP_POOL
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300;

-- 2. Create image repository
CREATE IMAGE REPOSITORY IF NOT EXISTS DEMO.DEMO.MESHTASTIC_IMAGES;

-- 3. Create network rule for external access (if needed)
CREATE OR REPLACE NETWORK RULE MESHTASTIC_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('0.0.0.0:443', '0.0.0.0:80');

-- 4. Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION MESHTASTIC_ACCESS
    ALLOWED_NETWORK_RULES = (MESHTASTIC_EGRESS_RULE)
    ENABLED = TRUE;

-- 5. Create the service specification
-- Save this as service_spec.yaml and upload to a stage

-- 6. Show the agent we created
DESCRIBE AGENT DEMO.DEMO.MESHTASTIC_AGENT;

-- 7. Test the agent
SELECT SNOWFLAKE.CORTEX.INVOKE_AGENT(
    'DEMO.DEMO.MESHTASTIC_AGENT',
    'What mesh network nodes are currently active and what are their battery levels?'
) as response;

-- 8. Create a stored procedure to wrap agent calls for MCP
CREATE OR REPLACE PROCEDURE DEMO.DEMO.MESHTASTIC_AGENT_QUERY(question VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    RETURN (
        SELECT SNOWFLAKE.CORTEX.INVOKE_AGENT(
            'DEMO.DEMO.MESHTASTIC_AGENT',
            :question
        )
    );
END;
$$;

-- 9. Create UDF for direct SQL queries via MCP
CREATE OR REPLACE FUNCTION DEMO.DEMO.MESH_GET_ACTIVE_NODES(hours INTEGER)
RETURNS TABLE (
    node_id VARCHAR,
    battery_level FLOAT,
    snr FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    packets INTEGER,
    last_seen TIMESTAMP_NTZ
)
AS
$$
    SELECT 
        from_id as node_id,
        MAX(battery_level) as battery_level,
        MAX(rx_snr) as snr,
        MAX(latitude) as latitude,
        MAX(longitude) as longitude,
        COUNT(*)::INTEGER as packets,
        MAX(ingested_at) as last_seen
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= DATEADD(hour, -hours, CURRENT_TIMESTAMP())
    GROUP BY from_id
    ORDER BY last_seen DESC
$$;

-- 10. Create UDF for network statistics
CREATE OR REPLACE FUNCTION DEMO.DEMO.MESH_NETWORK_STATS(hours INTEGER)
RETURNS TABLE (
    unique_nodes INTEGER,
    total_packets INTEGER,
    avg_battery FLOAT,
    avg_snr FLOAT,
    avg_temp FLOAT,
    position_packets INTEGER,
    telemetry_packets INTEGER,
    text_packets INTEGER
)
AS
$$
    SELECT 
        COUNT(DISTINCT from_id)::INTEGER as unique_nodes,
        COUNT(*)::INTEGER as total_packets,
        AVG(battery_level) as avg_battery,
        AVG(rx_snr) as avg_snr,
        AVG(temperature) as avg_temp,
        COUNT(CASE WHEN packet_type = 'position' THEN 1 END)::INTEGER as position_packets,
        COUNT(CASE WHEN packet_type = 'telemetry' THEN 1 END)::INTEGER as telemetry_packets,
        COUNT(CASE WHEN packet_type = 'text' THEN 1 END)::INTEGER as text_packets
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= DATEADD(hour, -hours, CURRENT_TIMESTAMP())
$$;

-- 11. Create UDF for low battery alerts
CREATE OR REPLACE FUNCTION DEMO.DEMO.MESH_LOW_BATTERY_ALERTS(threshold INTEGER)
RETURNS TABLE (
    node_id VARCHAR,
    battery_level FLOAT,
    voltage FLOAT,
    last_seen TIMESTAMP_NTZ
)
AS
$$
    SELECT 
        from_id as node_id,
        battery_level,
        voltage,
        ingested_at as last_seen
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE battery_level IS NOT NULL
        AND battery_level <= threshold
    QUALIFY ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY ingested_at DESC) = 1
    ORDER BY battery_level ASC
$$;

-- 12. Create UDF for GPS positions
CREATE OR REPLACE FUNCTION DEMO.DEMO.MESH_GPS_POSITIONS(hours INTEGER)
RETURNS TABLE (
    node_id VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    altitude FLOAT,
    speed FLOAT,
    satellites INTEGER,
    timestamp TIMESTAMP_NTZ
)
AS
$$
    SELECT 
        from_id as node_id,
        latitude,
        longitude,
        altitude,
        ground_speed as speed,
        sats_in_view::INTEGER as satellites,
        ingested_at as timestamp
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= DATEADD(hour, -hours, CURRENT_TIMESTAMP())
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY ingested_at DESC) = 1
    ORDER BY ingested_at DESC
$$;

-- 13. Create UDF for signal quality analysis
CREATE OR REPLACE FUNCTION DEMO.DEMO.MESH_SIGNAL_QUALITY(hours INTEGER)
RETURNS TABLE (
    node_id VARCHAR,
    avg_snr FLOAT,
    min_snr FLOAT,
    max_snr FLOAT,
    avg_rssi FLOAT,
    sample_count INTEGER,
    signal_quality VARCHAR
)
AS
$$
    SELECT 
        from_id as node_id,
        AVG(rx_snr) as avg_snr,
        MIN(rx_snr) as min_snr,
        MAX(rx_snr) as max_snr,
        AVG(rx_rssi) as avg_rssi,
        COUNT(*)::INTEGER as sample_count,
        CASE 
            WHEN AVG(rx_snr) >= 10 THEN 'Excellent'
            WHEN AVG(rx_snr) >= 5 THEN 'Good'
            WHEN AVG(rx_snr) >= 0 THEN 'Fair'
            ELSE 'Poor'
        END as signal_quality
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= DATEADD(hour, -hours, CURRENT_TIMESTAMP())
        AND rx_snr IS NOT NULL
    GROUP BY from_id
    ORDER BY avg_snr DESC
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE DEMO.DEMO.MESHTASTIC_AGENT_QUERY(VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION DEMO.DEMO.MESH_GET_ACTIVE_NODES(INTEGER) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION DEMO.DEMO.MESH_NETWORK_STATS(INTEGER) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION DEMO.DEMO.MESH_LOW_BATTERY_ALERTS(INTEGER) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION DEMO.DEMO.MESH_GPS_POSITIONS(INTEGER) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION DEMO.DEMO.MESH_SIGNAL_QUALITY(INTEGER) TO ROLE PUBLIC;
