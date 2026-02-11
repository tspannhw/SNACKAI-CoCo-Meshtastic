-- ============================================================================
-- MESHTASTIC MCP SERVER SETUP
-- Snowflake-managed MCP Server for Mesh Network Analytics
-- ============================================================================

-- 1. CREATE THE CORTEX AGENT
CREATE OR REPLACE AGENT DEMO.DEMO.MESHTASTIC_AGENT
PROFILE = '{"display_name": "Mesh Network Analyst", "avatar": "mesh-icon.png", "color": "green"}'
FROM SPECIFICATION
$$
models:
  orchestration: claude-4-sonnet

orchestration:
  budget:
    seconds: 300
    tokens: 100000

instructions:
  response: "Format responses with clear sections using markdown. Use bullet points for metrics. Include specific values with units (dB, %, meters, etc). Highlight concerning values in bold (low battery <20%, poor SNR <5dB). Always provide actionable recommendations."
  orchestration: "For any questions about device locations, battery, signal, temperature, or network status, use the mesh_network_analyst tool to query the data. Aggregate data when asked about trends or summaries."
  system: "You are an expert IoT mesh network analyst specializing in Meshtastic LoRa devices. You help users understand GPS positions, battery health, signal quality (SNR/RSSI), environmental sensors, and network topology. Provide actionable insights about device health, coverage gaps, and network optimization."
  sample_questions:
    - question: "What is the current status of all mesh nodes?"
      answer: "I'll query the latest telemetry data for all active nodes including their battery levels, signal quality, and last known positions."
    - question: "Which devices have low battery?"
      answer: "I'll check battery levels across all devices and identify any below 20% that need attention."
    - question: "Show me the GPS positions of all trackers"
      answer: "I'll retrieve the latest GPS coordinates for all devices with position data."

tools:
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "mesh_network_analyst"
      description: "Query Meshtastic mesh network IoT data including GPS positions (latitude, longitude, altitude), device telemetry (battery level, voltage, temperature, humidity), LoRa signal metrics (SNR, RSSI), text messages, packet types, and network statistics."

tool_resources:
  mesh_network_analyst:
    semantic_view: "DEMO.DEMO.MESHTASTIC_SEMANTIC_VIEW"
$$;

-- 2. CREATE UTILITY FUNCTIONS FOR MCP TOOLS

-- Get active nodes with status
CREATE OR REPLACE FUNCTION DEMO.DEMO.MESH_GET_ACTIVE_NODES(hours INTEGER)
RETURNS TABLE (
    node_id VARCHAR,
    battery_level NUMBER,
    snr FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    packets NUMBER,
    last_seen TIMESTAMP_TZ
)
AS
$$
    SELECT 
        from_id as node_id,
        MAX(battery_level) as battery_level,
        MAX(rx_snr) as snr,
        MAX(latitude) as latitude,
        MAX(longitude) as longitude,
        COUNT(*) as packets,
        MAX(ingested_at) as last_seen
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= DATEADD(hour, -hours, CURRENT_TIMESTAMP())
    GROUP BY from_id
    ORDER BY last_seen DESC
$$;

-- Get network statistics
CREATE OR REPLACE FUNCTION DEMO.DEMO.MESH_NETWORK_STATS(hours INTEGER)
RETURNS TABLE (
    unique_nodes NUMBER,
    total_packets NUMBER,
    avg_battery NUMBER(10,2),
    avg_snr FLOAT,
    avg_temperature FLOAT,
    position_packets NUMBER,
    telemetry_packets NUMBER,
    text_packets NUMBER
)
AS
$$
    SELECT 
        COUNT(DISTINCT from_id) as unique_nodes,
        COUNT(*) as total_packets,
        AVG(battery_level)::NUMBER(10,2) as avg_battery,
        AVG(rx_snr) as avg_snr,
        AVG(temperature) as avg_temperature,
        COUNT(CASE WHEN packet_type = 'position' THEN 1 END) as position_packets,
        COUNT(CASE WHEN packet_type = 'telemetry' THEN 1 END) as telemetry_packets,
        COUNT(CASE WHEN packet_type = 'text' THEN 1 END) as text_packets
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= DATEADD(hour, -hours, CURRENT_TIMESTAMP())
$$;

-- Signal quality analysis
CREATE OR REPLACE FUNCTION DEMO.DEMO.MESH_SIGNAL_ANALYSIS(hours INTEGER)
RETURNS TABLE (
    node_id VARCHAR,
    avg_snr FLOAT,
    min_snr FLOAT,
    max_snr FLOAT,
    avg_rssi FLOAT,
    sample_count NUMBER,
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
        COUNT(*) as sample_count,
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

-- GPS positions
CREATE OR REPLACE FUNCTION DEMO.DEMO.MESH_GPS_POSITIONS(hours INTEGER)
RETURNS TABLE (
    node_id VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    altitude FLOAT,
    speed FLOAT,
    satellites NUMBER,
    last_update TIMESTAMP_TZ
)
AS
$$
    SELECT 
        from_id as node_id,
        latitude,
        longitude,
        altitude,
        ground_speed as speed,
        sats_in_view as satellites,
        ingested_at as last_update
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= DATEADD(hour, -hours, CURRENT_TIMESTAMP())
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY ingested_at DESC) = 1
    ORDER BY ingested_at DESC
$$;

-- 3. CREATE THE MCP SERVER
CREATE OR REPLACE MCP SERVER DEMO.DEMO.MESHTASTIC_MCP_SERVER
FROM SPECIFICATION $$
tools:
  - name: "mesh-network-analyst"
    type: "CORTEX_ANALYST_MESSAGE"
    identifier: "DEMO.DEMO.MESHTASTIC_SEMANTIC_VIEW"
    description: "Query Meshtastic mesh network IoT data using natural language. Ask questions about GPS positions, device telemetry (battery, voltage, temperature, humidity), LoRa signal metrics (SNR, RSSI), text messages, and network statistics."
    title: "Mesh Network Analyst"

  - name: "mesh-agent"
    type: "CORTEX_AGENT_RUN"
    identifier: "DEMO.DEMO.MESHTASTIC_AGENT"
    description: "AI-powered mesh network analyst agent. Provides comprehensive insights about device health, coverage analysis, signal optimization, network topology, and actionable recommendations."
    title: "Mesh Network AI Agent"

  - name: "get-active-nodes"
    type: "GENERIC"
    identifier: "DEMO.DEMO.MESH_GET_ACTIVE_NODES"
    description: "Get all active mesh network nodes from the last N hours. Returns node ID, battery level, SNR, GPS coordinates, packet count, and last seen timestamp."
    title: "Get Active Nodes"

  - name: "get-network-stats"
    type: "GENERIC"
    identifier: "DEMO.DEMO.MESH_NETWORK_STATS"
    description: "Get overall network statistics including unique nodes, total packets, average battery, average SNR, average temperature, and packet type counts."
    title: "Network Statistics"

  - name: "get-signal-analysis"
    type: "GENERIC"
    identifier: "DEMO.DEMO.MESH_SIGNAL_ANALYSIS"
    description: "Analyze signal quality across all nodes. Returns average/min/max SNR, RSSI, sample count, and quality rating (Excellent/Good/Fair/Poor)."
    title: "Signal Analysis"

  - name: "get-gps-positions"
    type: "GENERIC"
    identifier: "DEMO.DEMO.MESH_GPS_POSITIONS"
    description: "Get latest GPS positions for all nodes. Returns latitude, longitude, altitude, speed, satellite count, and timestamp."
    title: "GPS Positions"

  - name: "execute-mesh-sql"
    type: "SYSTEM_EXECUTE_SQL"
    description: "Execute custom SQL queries against the Meshtastic mesh network data. Table: DEMO.DEMO.MESHTASTIC_DATA with columns: from_id, packet_type, latitude, longitude, altitude, battery_level, voltage, temperature, relative_humidity, rx_snr, rx_rssi, text, ingested_at."
    title: "Execute SQL Query"
$$;

-- 4. GRANT PERMISSIONS (adjust roles as needed)
GRANT USAGE ON MCP SERVER DEMO.DEMO.MESHTASTIC_MCP_SERVER TO ROLE PUBLIC;
GRANT SELECT ON SEMANTIC VIEW DEMO.DEMO.MESHTASTIC_SEMANTIC_VIEW TO ROLE PUBLIC;
GRANT USAGE ON AGENT DEMO.DEMO.MESHTASTIC_AGENT TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION DEMO.DEMO.MESH_GET_ACTIVE_NODES(INTEGER) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION DEMO.DEMO.MESH_NETWORK_STATS(INTEGER) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION DEMO.DEMO.MESH_SIGNAL_ANALYSIS(INTEGER) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION DEMO.DEMO.MESH_GPS_POSITIONS(INTEGER) TO ROLE PUBLIC;

-- 5. VERIFY SETUP
SHOW MCP SERVERS LIKE 'MESHTASTIC%' IN SCHEMA DEMO.DEMO;
DESCRIBE MCP SERVER DEMO.DEMO.MESHTASTIC_MCP_SERVER;
SHOW AGENTS LIKE 'MESHTASTIC%' IN SCHEMA DEMO.DEMO;

-- ============================================================================
-- MCP ENDPOINT URL FORMAT:
-- https://<account_url>/api/v2/databases/DEMO/schemas/DEMO/mcp-servers/MESHTASTIC_MCP_SERVER
--
-- Example for this account:
-- https://SFSENORTHAMERICA-TSPANN-AWS1.snowflakecomputing.com/api/v2/databases/DEMO/schemas/DEMO/mcp-servers/MESHTASTIC_MCP_SERVER
-- ============================================================================

-- TEST QUERIES
-- Get active nodes
SELECT * FROM TABLE(DEMO.DEMO.MESH_GET_ACTIVE_NODES(24)) LIMIT 10;

-- Get network stats
SELECT * FROM TABLE(DEMO.DEMO.MESH_NETWORK_STATS(24));

-- Get signal analysis
SELECT * FROM TABLE(DEMO.DEMO.MESH_SIGNAL_ANALYSIS(24));

-- Get GPS positions
SELECT * FROM TABLE(DEMO.DEMO.MESH_GPS_POSITIONS(24)) LIMIT 10;
