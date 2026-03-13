-- Semantic View for Meshtastic Data
-- Enables natural language queries via Cortex Analyst

USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO;
USE SCHEMA DEMO;

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

-- ============================================================================
-- SMART ANOMALY DETECTION QUERIES WITH CORTEX AI
-- ============================================================================

-- 1. SENSOR ANOMALIES: Detect unusual temperature readings using statistical outliers
SELECT 
    from_id,
    ingested_at,
    temperature,
    AVG(temperature) OVER (PARTITION BY from_id ORDER BY ingested_at ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS rolling_avg,
    STDDEV(temperature) OVER (PARTITION BY from_id ORDER BY ingested_at ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS rolling_stddev,
    CASE 
        WHEN ABS(temperature - AVG(temperature) OVER (PARTITION BY from_id ORDER BY ingested_at ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)) 
             > 2 * NULLIF(STDDEV(temperature) OVER (PARTITION BY from_id ORDER BY ingested_at ROWS BETWEEN 10 PRECEDING AND CURRENT ROW), 0)
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END AS temperature_status,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', 
        'Analyze this sensor reading: Temperature=' || temperature || 'C, Rolling Avg=' || 
        ROUND(AVG(temperature) OVER (PARTITION BY from_id ORDER BY ingested_at ROWS BETWEEN 10 PRECEDING AND CURRENT ROW), 1) || 
        'C. Is this anomalous? Reply in 20 words or less.') AS ai_analysis
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE temperature IS NOT NULL
ORDER BY ingested_at DESC
LIMIT 20;

-- 2. LOCATION ANOMALIES: Detect impossible GPS jumps (speed violations)
WITH location_changes AS (
    SELECT 
        from_id,
        ingested_at,
        latitude,
        longitude,
        LAG(latitude) OVER (PARTITION BY from_id ORDER BY ingested_at) AS prev_lat,
        LAG(longitude) OVER (PARTITION BY from_id ORDER BY ingested_at) AS prev_lon,
        LAG(ingested_at) OVER (PARTITION BY from_id ORDER BY ingested_at) AS prev_time,
        HAVERSINE(
            LAG(latitude) OVER (PARTITION BY from_id ORDER BY ingested_at),
            LAG(longitude) OVER (PARTITION BY from_id ORDER BY ingested_at),
            latitude, longitude
        ) AS distance_km,
        TIMESTAMPDIFF('second', 
            LAG(ingested_at) OVER (PARTITION BY from_id ORDER BY ingested_at), 
            ingested_at) AS time_diff_sec
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
)
SELECT 
    from_id,
    ingested_at,
    latitude, longitude,
    ROUND(distance_km, 3) AS distance_km,
    time_diff_sec,
    ROUND(distance_km / NULLIF(time_diff_sec / 3600.0, 0), 1) AS implied_speed_kmh,
    CASE 
        WHEN distance_km / NULLIF(time_diff_sec / 3600.0, 0) > 200 THEN 'GPS_JUMP_ANOMALY'
        WHEN distance_km / NULLIF(time_diff_sec / 3600.0, 0) > 120 THEN 'HIGH_SPEED'
        ELSE 'NORMAL'
    END AS location_status
FROM location_changes
WHERE distance_km IS NOT NULL AND time_diff_sec > 0
ORDER BY implied_speed_kmh DESC NULLS LAST
LIMIT 20;

-- 3. BATTERY ANOMALIES: Sudden drops or impossible increases
WITH battery_changes AS (
    SELECT 
        from_id,
        ingested_at,
        battery_level,
        LAG(battery_level) OVER (PARTITION BY from_id ORDER BY ingested_at) AS prev_battery,
        battery_level - LAG(battery_level) OVER (PARTITION BY from_id ORDER BY ingested_at) AS battery_change
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE battery_level IS NOT NULL
)
SELECT 
    from_id,
    ingested_at,
    battery_level,
    prev_battery,
    battery_change,
    CASE 
        WHEN battery_change > 10 THEN 'CHARGING_OR_ANOMALY'
        WHEN battery_change < -20 THEN 'RAPID_DRAIN_ANOMALY'
        ELSE 'NORMAL'
    END AS battery_status,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'Device battery went from ' || prev_battery || '% to ' || battery_level || 
        '%. Is this normal device behavior? Answer in 15 words.') AS ai_diagnosis
FROM battery_changes
WHERE ABS(battery_change) > 10
ORDER BY ABS(battery_change) DESC
LIMIT 15;

-- 4. SIGNAL QUALITY ANOMALIES: Poor LoRa reception patterns
SELECT 
    from_id,
    DATE_TRUNC('hour', ingested_at) AS hour,
    COUNT(*) AS packet_count,
    ROUND(AVG(rx_rssi), 1) AS avg_rssi,
    ROUND(AVG(rx_snr), 1) AS avg_snr,
    ROUND(STDDEV(rx_rssi), 2) AS rssi_variance,
    CASE 
        WHEN AVG(rx_rssi) < -120 THEN 'CRITICAL_SIGNAL'
        WHEN AVG(rx_rssi) < -100 THEN 'WEAK_SIGNAL'
        WHEN STDDEV(rx_rssi) > 15 THEN 'UNSTABLE_SIGNAL'
        ELSE 'GOOD'
    END AS signal_status
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE rx_rssi IS NOT NULL
GROUP BY from_id, DATE_TRUNC('hour', ingested_at)
HAVING AVG(rx_rssi) < -90 OR STDDEV(rx_rssi) > 10
ORDER BY avg_rssi ASC
LIMIT 20;

-- 5. MULTI-SENSOR CORRELATION ANOMALIES: AI analysis of combined readings
SELECT 
    from_id,
    ingested_at,
    temperature,
    relative_humidity,
    barometric_pressure,
    battery_level,
    rx_rssi,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'Analyze IoT sensor data: Temp=' || COALESCE(temperature::VARCHAR, 'N/A') || 'C, ' ||
        'Humidity=' || COALESCE(relative_humidity::VARCHAR, 'N/A') || '%, ' ||
        'Pressure=' || COALESCE(barometric_pressure::VARCHAR, 'N/A') || 'hPa, ' ||
        'Battery=' || COALESCE(battery_level::VARCHAR, 'N/A') || '%, ' ||
        'Signal=' || COALESCE(rx_rssi::VARCHAR, 'N/A') || 'dBm. ' ||
        'Identify anomalies or concerns in 30 words.') AS ai_health_check
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE packet_type = 'telemetry'
  AND (temperature IS NOT NULL OR battery_level IS NOT NULL)
ORDER BY ingested_at DESC
LIMIT 10;

-- 6. GEOGRAPHIC CLUSTERING: Find nodes outside normal operating area
WITH node_bounds AS (
    SELECT 
        from_id,
        AVG(latitude) AS center_lat,
        AVG(longitude) AS center_lon,
        STDDEV(latitude) AS lat_spread,
        STDDEV(longitude) AS lon_spread
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    GROUP BY from_id
    HAVING COUNT(*) > 5
)
SELECT 
    m.from_id,
    m.ingested_at,
    m.latitude,
    m.longitude,
    ROUND(nb.center_lat, 6) AS typical_lat,
    ROUND(nb.center_lon, 6) AS typical_lon,
    ROUND(HAVERSINE(nb.center_lat, nb.center_lon, m.latitude, m.longitude), 2) AS distance_from_center_km,
    CASE 
        WHEN HAVERSINE(nb.center_lat, nb.center_lon, m.latitude, m.longitude) > 10 THEN 'FAR_FROM_BASE'
        WHEN HAVERSINE(nb.center_lat, nb.center_lon, m.latitude, m.longitude) > 5 THEN 'EXTENDED_RANGE'
        ELSE 'NORMAL_AREA'
    END AS geo_status
FROM DEMO.DEMO.MESHTASTIC_DATA m
JOIN node_bounds nb ON m.from_id = nb.from_id
WHERE m.latitude IS NOT NULL
ORDER BY distance_from_center_km DESC
LIMIT 20;

-- 7. MESSAGE SENTIMENT ANALYSIS: Analyze text messages for urgency
SELECT 
    from_id,
    to_id,
    ingested_at,
    text_message,
    SNOWFLAKE.CORTEX.SENTIMENT(text_message) AS sentiment_score,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        text_message, 
        ['Emergency', 'Normal', 'Question', 'Status Update', 'Social']
    ):label::VARCHAR AS message_category,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(text_message) < -0.3 THEN 'NEGATIVE_ALERT'
        WHEN LOWER(text_message) LIKE '%help%' OR LOWER(text_message) LIKE '%emergency%' THEN 'URGENT'
        ELSE 'NORMAL'
    END AS priority
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE packet_type = 'text' AND text_message IS NOT NULL
ORDER BY sentiment_score ASC
LIMIT 20;

-- 8. NODE HEALTH SUMMARY: Device health with AI recommendations
WITH node_stats AS (
    SELECT 
        from_id,
        COUNT(*) AS total_packets,
        MAX(ingested_at) AS last_seen,
        AVG(battery_level) AS avg_battery,
        MIN(battery_level) AS min_battery,
        AVG(rx_rssi) AS avg_signal,
        AVG(temperature) AS avg_temp,
        COUNT(DISTINCT DATE(ingested_at)) AS active_days
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY from_id
)
SELECT 
    from_id,
    total_packets,
    last_seen,
    ROUND(avg_battery, 0) AS avg_battery_pct,
    ROUND(avg_signal, 1) AS avg_rssi,
    ROUND(avg_temp, 1) AS avg_temp_c,
    active_days,
    CASE 
        WHEN TIMESTAMPDIFF('hour', last_seen, CURRENT_TIMESTAMP()) > 24 THEN 'OFFLINE'
        WHEN avg_battery < 20 THEN 'LOW_BATTERY'
        WHEN avg_signal < -110 THEN 'POOR_SIGNAL'
        ELSE 'HEALTHY'
    END AS health_status,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'IoT node stats: ' || total_packets || ' packets, ' ||
        'battery=' || ROUND(avg_battery, 0) || '%, signal=' || ROUND(avg_signal, 0) || 'dBm, ' ||
        'last seen ' || TIMESTAMPDIFF('hour', last_seen, CURRENT_TIMESTAMP()) || ' hours ago. ' ||
        'Recommend maintenance action in 20 words.') AS ai_recommendation
FROM node_stats
ORDER BY health_status DESC, avg_battery ASC
LIMIT 15;

-- ============================================================================
-- COMPREHENSIVE NODE ANALYSIS - DEEP DIVE INTO MESHTASTIC NODES
-- ============================================================================

-- 9. NODE ACTIVITY PROFILING: Complete behavioral fingerprint per node
SELECT 
    from_id AS node_id,
    COUNT(*) AS total_transmissions,
    COUNT(DISTINCT packet_type) AS packet_types_used,
    MIN(ingested_at) AS first_seen,
    MAX(ingested_at) AS last_seen,
    DATEDIFF('day', MIN(ingested_at), MAX(ingested_at)) AS days_active,
    ROUND(COUNT(*) / NULLIF(DATEDIFF('hour', MIN(ingested_at), MAX(ingested_at)), 0), 2) AS packets_per_hour,
    COUNT(CASE WHEN packet_type = 'position' THEN 1 END) AS position_packets,
    COUNT(CASE WHEN packet_type = 'telemetry' THEN 1 END) AS telemetry_packets,
    COUNT(CASE WHEN packet_type = 'text' THEN 1 END) AS text_messages,
    COUNT(DISTINCT to_id) AS unique_destinations,
    ROUND(AVG(hop_limit), 1) AS avg_hop_limit,
    LISTAGG(DISTINCT channel, ', ') WITHIN GROUP (ORDER BY channel) AS channels_used,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'Mesh node transmitted ' || COUNT(*) || ' packets over ' || 
        DATEDIFF('day', MIN(ingested_at), MAX(ingested_at)) || ' days. ' ||
        'Position:' || COUNT(CASE WHEN packet_type = 'position' THEN 1 END) || 
        ', Telemetry:' || COUNT(CASE WHEN packet_type = 'telemetry' THEN 1 END) ||
        ', Messages:' || COUNT(CASE WHEN packet_type = 'text' THEN 1 END) ||
        '. Characterize this node role in 25 words.') AS ai_node_profile
FROM DEMO.DEMO.MESHTASTIC_DATA
GROUP BY from_id
ORDER BY total_transmissions DESC;

-- 10. NETWORK TOPOLOGY: Who communicates with whom (mesh relationships)
WITH communications AS (
    SELECT 
        from_id,
        to_id,
        COUNT(*) AS message_count,
        AVG(rx_rssi) AS avg_signal_strength,
        AVG(rx_snr) AS avg_snr,
        MIN(ingested_at) AS first_contact,
        MAX(ingested_at) AS last_contact
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE to_id IS NOT NULL AND to_id != '^all'
    GROUP BY from_id, to_id
)
SELECT 
    from_id AS source_node,
    to_id AS destination_node,
    message_count,
    ROUND(avg_signal_strength, 1) AS avg_rssi,
    ROUND(avg_snr, 1) AS avg_snr,
    first_contact,
    last_contact,
    DATEDIFF('day', first_contact, last_contact) AS relationship_days,
    CASE 
        WHEN message_count > 100 THEN 'STRONG_LINK'
        WHEN message_count > 20 THEN 'REGULAR_LINK'
        ELSE 'OCCASIONAL_LINK'
    END AS link_strength,
    CASE 
        WHEN avg_signal_strength > -80 THEN 'EXCELLENT'
        WHEN avg_signal_strength > -100 THEN 'GOOD'
        WHEN avg_signal_strength > -115 THEN 'FAIR'
        ELSE 'POOR'
    END AS connection_quality
FROM communications
ORDER BY message_count DESC
LIMIT 50;

-- 11. NODE RELIABILITY METRICS: Uptime, consistency, and gaps
WITH node_activity AS (
    SELECT 
        from_id,
        DATE(ingested_at) AS activity_date,
        COUNT(*) AS daily_packets,
        MIN(ingested_at) AS first_packet,
        MAX(ingested_at) AS last_packet,
        AVG(battery_level) AS avg_battery
    FROM DEMO.DEMO.MESHTASTIC_DATA
    GROUP BY from_id, DATE(ingested_at)
),
node_gaps AS (
    SELECT 
        from_id,
        activity_date,
        daily_packets,
        LAG(activity_date) OVER (PARTITION BY from_id ORDER BY activity_date) AS prev_date,
        DATEDIFF('day', LAG(activity_date) OVER (PARTITION BY from_id ORDER BY activity_date), activity_date) AS gap_days
    FROM node_activity
)
SELECT 
    from_id AS node_id,
    COUNT(DISTINCT activity_date) AS active_days,
    MIN(activity_date) AS monitoring_start,
    MAX(activity_date) AS monitoring_end,
    DATEDIFF('day', MIN(activity_date), MAX(activity_date)) + 1 AS total_span_days,
    ROUND(COUNT(DISTINCT activity_date) * 100.0 / NULLIF(DATEDIFF('day', MIN(activity_date), MAX(activity_date)) + 1, 0), 1) AS uptime_percentage,
    MAX(gap_days) AS max_gap_days,
    ROUND(AVG(daily_packets), 1) AS avg_daily_packets,
    ROUND(STDDEV(daily_packets), 1) AS packet_variability,
    CASE 
        WHEN COUNT(DISTINCT activity_date) * 100.0 / NULLIF(DATEDIFF('day', MIN(activity_date), MAX(activity_date)) + 1, 0) > 90 THEN 'HIGHLY_RELIABLE'
        WHEN COUNT(DISTINCT activity_date) * 100.0 / NULLIF(DATEDIFF('day', MIN(activity_date), MAX(activity_date)) + 1, 0) > 70 THEN 'RELIABLE'
        WHEN COUNT(DISTINCT activity_date) * 100.0 / NULLIF(DATEDIFF('day', MIN(activity_date), MAX(activity_date)) + 1, 0) > 50 THEN 'INTERMITTENT'
        ELSE 'UNRELIABLE'
    END AS reliability_rating
FROM node_gaps
GROUP BY from_id
ORDER BY uptime_percentage DESC;

-- 12. GEOSPATIAL NODE ANALYSIS: Coverage area and movement patterns
WITH node_locations AS (
    SELECT 
        from_id,
        latitude,
        longitude,
        altitude,
        ground_speed,
        ingested_at,
        ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY ingested_at) AS seq
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
)
SELECT 
    from_id AS node_id,
    COUNT(*) AS position_reports,
    ROUND(MIN(latitude), 6) AS min_lat,
    ROUND(MAX(latitude), 6) AS max_lat,
    ROUND(MIN(longitude), 6) AS min_lon,
    ROUND(MAX(longitude), 6) AS max_lon,
    ROUND(AVG(latitude), 6) AS centroid_lat,
    ROUND(AVG(longitude), 6) AS centroid_lon,
    ROUND(AVG(altitude), 1) AS avg_altitude_m,
    ROUND(MAX(altitude) - MIN(altitude), 1) AS altitude_range_m,
    ROUND(HAVERSINE(MIN(latitude), MIN(longitude), MAX(latitude), MAX(longitude)), 2) AS coverage_diagonal_km,
    ROUND(AVG(ground_speed) * 3.6, 1) AS avg_speed_kmh,
    ROUND(MAX(ground_speed) * 3.6, 1) AS max_speed_kmh,
    CASE 
        WHEN AVG(ground_speed) < 0.5 THEN 'STATIONARY'
        WHEN AVG(ground_speed) < 5 THEN 'SLOW_MOVING'
        WHEN AVG(ground_speed) < 15 THEN 'MOBILE'
        ELSE 'HIGH_MOBILITY'
    END AS mobility_class,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'GPS node covers area from (' || ROUND(MIN(latitude), 4) || ',' || ROUND(MIN(longitude), 4) || ') to (' ||
        ROUND(MAX(latitude), 4) || ',' || ROUND(MAX(longitude), 4) || '). Avg altitude: ' || ROUND(AVG(altitude), 0) || 
        'm. Avg speed: ' || ROUND(AVG(ground_speed) * 3.6, 1) || 'km/h. Describe likely deployment scenario in 25 words.') AS ai_deployment_analysis
FROM node_locations
GROUP BY from_id
HAVING COUNT(*) > 5
ORDER BY coverage_diagonal_km DESC;

-- 13. ENVIRONMENTAL CONDITIONS BY NODE: Temperature, humidity, pressure analysis
SELECT 
    from_id AS node_id,
    COUNT(*) AS sensor_readings,
    ROUND(AVG(temperature), 1) AS avg_temp_c,
    ROUND(MIN(temperature), 1) AS min_temp_c,
    ROUND(MAX(temperature), 1) AS max_temp_c,
    ROUND(STDDEV(temperature), 2) AS temp_variability,
    ROUND(AVG(relative_humidity), 1) AS avg_humidity_pct,
    ROUND(MIN(relative_humidity), 1) AS min_humidity_pct,
    ROUND(MAX(relative_humidity), 1) AS max_humidity_pct,
    ROUND(AVG(barometric_pressure), 1) AS avg_pressure_hpa,
    ROUND(MIN(barometric_pressure), 1) AS min_pressure_hpa,
    ROUND(MAX(barometric_pressure), 1) AS max_pressure_hpa,
    CASE 
        WHEN AVG(temperature) < 0 THEN 'FREEZING'
        WHEN AVG(temperature) < 15 THEN 'COLD'
        WHEN AVG(temperature) < 25 THEN 'MODERATE'
        WHEN AVG(temperature) < 35 THEN 'WARM'
        ELSE 'HOT'
    END AS temperature_zone,
    CASE 
        WHEN AVG(relative_humidity) > 80 THEN 'HUMID'
        WHEN AVG(relative_humidity) > 50 THEN 'COMFORTABLE'
        ELSE 'DRY'
    END AS humidity_zone,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'Environmental sensor: Temp range ' || ROUND(MIN(temperature), 0) || '-' || ROUND(MAX(temperature), 0) || 'C, ' ||
        'Humidity ' || ROUND(AVG(relative_humidity), 0) || '%, Pressure ' || ROUND(AVG(barometric_pressure), 0) || 'hPa. ' ||
        'Recommend optimal hardware protection in 20 words.') AS ai_environment_advice
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE temperature IS NOT NULL OR relative_humidity IS NOT NULL
GROUP BY from_id
ORDER BY sensor_readings DESC;

-- 14. POWER MANAGEMENT ANALYSIS: Battery trends and consumption patterns
WITH battery_timeline AS (
    SELECT 
        from_id,
        DATE(ingested_at) AS report_date,
        AVG(battery_level) AS avg_battery,
        MIN(battery_level) AS min_battery,
        MAX(battery_level) AS max_battery,
        AVG(voltage) AS avg_voltage,
        COUNT(*) AS readings
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE battery_level IS NOT NULL
    GROUP BY from_id, DATE(ingested_at)
),
battery_trend AS (
    SELECT 
        *,
        LAG(avg_battery) OVER (PARTITION BY from_id ORDER BY report_date) AS prev_day_battery,
        avg_battery - LAG(avg_battery) OVER (PARTITION BY from_id ORDER BY report_date) AS daily_change
    FROM battery_timeline
)
SELECT 
    from_id AS node_id,
    COUNT(*) AS days_monitored,
    ROUND(AVG(avg_battery), 1) AS overall_avg_battery,
    ROUND(MIN(min_battery), 0) AS lowest_battery_seen,
    ROUND(AVG(avg_voltage), 2) AS avg_voltage,
    ROUND(AVG(daily_change), 2) AS avg_daily_drain_pct,
    ROUND(MIN(daily_change), 2) AS worst_daily_drain,
    COUNT(CASE WHEN daily_change > 5 THEN 1 END) AS charging_events,
    COUNT(CASE WHEN min_battery < 20 THEN 1 END) AS low_battery_days,
    CASE 
        WHEN AVG(daily_change) > -1 THEN 'SOLAR_POWERED'
        WHEN AVG(daily_change) > -5 THEN 'EFFICIENT'
        WHEN AVG(daily_change) > -10 THEN 'MODERATE_DRAIN'
        ELSE 'HIGH_CONSUMPTION'
    END AS power_profile,
    ROUND(-100.0 / NULLIF(AVG(daily_change), 0), 0) AS estimated_days_to_empty,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'Battery analysis: Avg ' || ROUND(AVG(avg_battery), 0) || '%, daily drain ' || 
        ROUND(AVG(daily_change), 1) || '%, voltage ' || ROUND(AVG(avg_voltage), 2) || 'V, ' ||
        ROUND(-100.0 / NULLIF(AVG(daily_change), 0), 0) || ' est. days remaining. ' ||
        'Power optimization recommendations in 25 words.') AS ai_power_advice
FROM battery_trend
GROUP BY from_id
HAVING COUNT(*) > 3
ORDER BY avg_daily_drain_pct ASC;

-- 15. SIGNAL QUALITY DEEP DIVE: LoRa performance per node
WITH signal_stats AS (
    SELECT 
        from_id,
        HOUR(ingested_at) AS hour_of_day,
        rx_rssi,
        rx_snr,
        channel_utilization,
        air_util_tx,
        hop_limit
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE rx_rssi IS NOT NULL
)
SELECT 
    from_id AS node_id,
    COUNT(*) AS signal_samples,
    ROUND(AVG(rx_rssi), 1) AS avg_rssi_dbm,
    ROUND(MIN(rx_rssi), 1) AS worst_rssi,
    ROUND(MAX(rx_rssi), 1) AS best_rssi,
    ROUND(STDDEV(rx_rssi), 2) AS rssi_stability,
    ROUND(AVG(rx_snr), 1) AS avg_snr_db,
    ROUND(MIN(rx_snr), 1) AS worst_snr,
    ROUND(AVG(channel_utilization), 1) AS avg_channel_util_pct,
    ROUND(MAX(channel_utilization), 1) AS peak_channel_util,
    ROUND(AVG(air_util_tx), 1) AS avg_airtime_tx_pct,
    ROUND(AVG(hop_limit), 1) AS avg_hop_limit,
    CASE 
        WHEN AVG(rx_rssi) > -90 THEN 'EXCELLENT'
        WHEN AVG(rx_rssi) > -105 THEN 'GOOD'
        WHEN AVG(rx_rssi) > -115 THEN 'MARGINAL'
        ELSE 'POOR'
    END AS signal_rating,
    CASE 
        WHEN STDDEV(rx_rssi) < 5 THEN 'VERY_STABLE'
        WHEN STDDEV(rx_rssi) < 10 THEN 'STABLE'
        WHEN STDDEV(rx_rssi) < 15 THEN 'VARIABLE'
        ELSE 'UNSTABLE'
    END AS stability_rating,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'LoRa signal: RSSI=' || ROUND(AVG(rx_rssi), 0) || 'dBm (range ' || ROUND(MIN(rx_rssi), 0) || ' to ' || ROUND(MAX(rx_rssi), 0) || '), ' ||
        'SNR=' || ROUND(AVG(rx_snr), 0) || 'dB, channel util=' || ROUND(AVG(channel_utilization), 0) || '%. ' ||
        'Suggest antenna/placement improvements in 25 words.') AS ai_signal_advice
FROM signal_stats
GROUP BY from_id
ORDER BY avg_rssi_dbm DESC;

-- 16. NODE COMPARISON DASHBOARD: Side-by-side benchmarking
WITH node_metrics AS (
    SELECT 
        from_id,
        COUNT(*) AS total_packets,
        COUNT(DISTINCT DATE(ingested_at)) AS active_days,
        AVG(battery_level) AS avg_battery,
        AVG(rx_rssi) AS avg_rssi,
        AVG(temperature) AS avg_temp,
        COUNT(CASE WHEN packet_type = 'text' THEN 1 END) AS messages_sent,
        AVG(channel_utilization) AS avg_channel_util,
        MAX(ingested_at) AS last_active
    FROM DEMO.DEMO.MESHTASTIC_DATA
    GROUP BY from_id
),
fleet_avg AS (
    SELECT 
        AVG(total_packets) AS fleet_avg_packets,
        AVG(avg_battery) AS fleet_avg_battery,
        AVG(avg_rssi) AS fleet_avg_rssi,
        AVG(active_days) AS fleet_avg_days
    FROM node_metrics
)
SELECT 
    nm.from_id AS node_id,
    nm.total_packets,
    ROUND((nm.total_packets - fa.fleet_avg_packets) / NULLIF(fa.fleet_avg_packets, 0) * 100, 1) AS packets_vs_fleet_pct,
    nm.active_days,
    ROUND(nm.avg_battery, 0) AS avg_battery_pct,
    ROUND((nm.avg_battery - fa.fleet_avg_battery), 1) AS battery_vs_fleet,
    ROUND(nm.avg_rssi, 1) AS avg_rssi,
    ROUND((nm.avg_rssi - fa.fleet_avg_rssi), 1) AS rssi_vs_fleet,
    nm.messages_sent,
    ROUND(nm.avg_channel_util, 1) AS channel_util_pct,
    nm.last_active,
    TIMESTAMPDIFF('hour', nm.last_active, CURRENT_TIMESTAMP()) AS hours_since_active,
    CASE 
        WHEN nm.total_packets > fa.fleet_avg_packets * 1.5 AND nm.avg_rssi > fa.fleet_avg_rssi THEN 'TOP_PERFORMER'
        WHEN nm.total_packets > fa.fleet_avg_packets AND nm.avg_battery > fa.fleet_avg_battery THEN 'ABOVE_AVERAGE'
        WHEN nm.total_packets < fa.fleet_avg_packets * 0.5 OR nm.avg_rssi < fa.fleet_avg_rssi - 10 THEN 'NEEDS_ATTENTION'
        ELSE 'AVERAGE'
    END AS fleet_ranking,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'Node performance: ' || nm.total_packets || ' packets (' || 
        ROUND((nm.total_packets - fa.fleet_avg_packets) / NULLIF(fa.fleet_avg_packets, 0) * 100, 0) || '% vs fleet), ' ||
        'battery ' || ROUND(nm.avg_battery, 0) || '%, signal ' || ROUND(nm.avg_rssi, 0) || 'dBm. ' ||
        'Rate this node and suggest improvements in 30 words.') AS ai_node_assessment
FROM node_metrics nm
CROSS JOIN fleet_avg fa
ORDER BY total_packets DESC;

-- ============================================================================
-- 17. PREDICTIVE DEGRADATION ANALYSIS: Identify failing devices before failure
-- ============================================================================

-- Comprehensive multi-metric trend analysis to predict device failures
WITH weekly_metrics AS (
    SELECT 
        from_id,
        DATE_TRUNC('week', ingested_at) AS week_start,
        COUNT(*) AS packet_count,
        AVG(battery_level) AS avg_battery,
        AVG(rx_rssi) AS avg_rssi,
        AVG(rx_snr) AS avg_snr,
        AVG(temperature) AS avg_temp,
        AVG(voltage) AS avg_voltage,
        STDDEV(rx_rssi) AS rssi_variability,
        COUNT(DISTINCT DATE(ingested_at)) AS active_days,
        MIN(battery_level) AS min_battery,
        MAX(channel_utilization) AS peak_channel_util
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= DATEADD('week', -8, CURRENT_TIMESTAMP())
    GROUP BY from_id, DATE_TRUNC('week', ingested_at)
),
trend_analysis AS (
    SELECT 
        from_id,
        week_start,
        packet_count,
        avg_battery,
        avg_rssi,
        avg_snr,
        avg_voltage,
        rssi_variability,
        active_days,
        ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY week_start) AS week_num,
        COUNT(*) OVER (PARTITION BY from_id) AS total_weeks,
        -- Week-over-week changes
        packet_count - LAG(packet_count) OVER (PARTITION BY from_id ORDER BY week_start) AS packet_change,
        avg_battery - LAG(avg_battery) OVER (PARTITION BY from_id ORDER BY week_start) AS battery_change,
        avg_rssi - LAG(avg_rssi) OVER (PARTITION BY from_id ORDER BY week_start) AS rssi_change,
        avg_snr - LAG(avg_snr) OVER (PARTITION BY from_id ORDER BY week_start) AS snr_change,
        avg_voltage - LAG(avg_voltage) OVER (PARTITION BY from_id ORDER BY week_start) AS voltage_change,
        rssi_variability - LAG(rssi_variability) OVER (PARTITION BY from_id ORDER BY week_start) AS stability_change
    FROM weekly_metrics
),
degradation_scores AS (
    SELECT 
        from_id,
        -- Calculate trend slopes (negative = degrading)
        REGR_SLOPE(avg_rssi, week_num) AS rssi_trend_slope,
        REGR_SLOPE(avg_battery, week_num) AS battery_trend_slope,
        REGR_SLOPE(avg_snr, week_num) AS snr_trend_slope,
        REGR_SLOPE(packet_count, week_num) AS activity_trend_slope,
        REGR_SLOPE(avg_voltage, week_num) AS voltage_trend_slope,
        REGR_SLOPE(rssi_variability, week_num) AS instability_trend,
        -- Recent values
        MAX(CASE WHEN week_num = total_weeks THEN avg_rssi END) AS current_rssi,
        MAX(CASE WHEN week_num = total_weeks THEN avg_battery END) AS current_battery,
        MAX(CASE WHEN week_num = total_weeks THEN avg_voltage END) AS current_voltage,
        MAX(CASE WHEN week_num = total_weeks THEN packet_count END) AS current_packets,
        MAX(CASE WHEN week_num = 1 THEN avg_rssi END) AS initial_rssi,
        MAX(CASE WHEN week_num = 1 THEN avg_battery END) AS initial_battery,
        -- Consistency
        AVG(active_days) AS avg_active_days_per_week,
        STDDEV(packet_count) AS packet_variability,
        COUNT(*) AS weeks_monitored
    FROM trend_analysis
    GROUP BY from_id
    HAVING COUNT(*) >= 3
)
SELECT 
    from_id AS node_id,
    weeks_monitored,
    
    -- Current State
    ROUND(current_rssi, 1) AS current_rssi_dbm,
    ROUND(current_battery, 0) AS current_battery_pct,
    ROUND(current_voltage, 2) AS current_voltage_v,
    current_packets AS last_week_packets,
    
    -- Degradation Trends (negative = getting worse)
    ROUND(rssi_trend_slope, 2) AS rssi_weekly_change,
    ROUND(battery_trend_slope, 2) AS battery_weekly_change,
    ROUND(snr_trend_slope, 2) AS snr_weekly_change,
    ROUND(voltage_trend_slope, 3) AS voltage_weekly_change,
    ROUND(activity_trend_slope, 1) AS activity_weekly_change,
    ROUND(instability_trend, 2) AS instability_trend,
    
    -- Total Degradation Over Period
    ROUND(current_rssi - initial_rssi, 1) AS total_rssi_change,
    ROUND(current_battery - initial_battery, 0) AS total_battery_change,
    
    -- Degradation Score (higher = more concerning)
    ROUND(
        (CASE WHEN rssi_trend_slope < -1 THEN 25 WHEN rssi_trend_slope < 0 THEN 10 ELSE 0 END) +
        (CASE WHEN battery_trend_slope < -5 THEN 25 WHEN battery_trend_slope < -2 THEN 15 ELSE 0 END) +
        (CASE WHEN snr_trend_slope < -0.5 THEN 15 WHEN snr_trend_slope < 0 THEN 5 ELSE 0 END) +
        (CASE WHEN voltage_trend_slope < -0.02 THEN 20 WHEN voltage_trend_slope < 0 THEN 10 ELSE 0 END) +
        (CASE WHEN activity_trend_slope < -10 THEN 15 WHEN activity_trend_slope < 0 THEN 5 ELSE 0 END) +
        (CASE WHEN instability_trend > 2 THEN 10 ELSE 0 END) +
        (CASE WHEN current_battery < 30 THEN 15 WHEN current_battery < 50 THEN 5 ELSE 0 END) +
        (CASE WHEN current_rssi < -115 THEN 10 WHEN current_rssi < -105 THEN 5 ELSE 0 END)
    , 0) AS degradation_score,
    
    -- Risk Classification
    CASE 
        WHEN (CASE WHEN rssi_trend_slope < -1 THEN 25 ELSE 0 END) +
             (CASE WHEN battery_trend_slope < -5 THEN 25 ELSE 0 END) +
             (CASE WHEN voltage_trend_slope < -0.02 THEN 20 ELSE 0 END) >= 50 THEN 'CRITICAL - IMMINENT FAILURE'
        WHEN rssi_trend_slope < -2 AND battery_trend_slope < -5 THEN 'HIGH RISK - MULTIPLE DEGRADING'
        WHEN rssi_trend_slope < -1.5 OR voltage_trend_slope < -0.03 THEN 'ELEVATED - SIGNAL/POWER ISSUE'
        WHEN battery_trend_slope < -8 THEN 'ELEVATED - BATTERY FAILING'
        WHEN activity_trend_slope < -20 THEN 'MODERATE - ACTIVITY DECLINING'
        WHEN instability_trend > 3 THEN 'MODERATE - BECOMING UNSTABLE'
        WHEN rssi_trend_slope < 0 OR battery_trend_slope < 0 THEN 'LOW - MINOR DEGRADATION'
        ELSE 'HEALTHY - STABLE'
    END AS risk_level,
    
    -- Estimated Time to Failure
    CASE 
        WHEN battery_trend_slope >= 0 OR current_battery IS NULL THEN 'N/A - Stable/Solar'
        WHEN current_battery / NULLIF(ABS(battery_trend_slope), 0) < 4 THEN 'CRITICAL: <1 month'
        WHEN current_battery / NULLIF(ABS(battery_trend_slope), 0) < 8 THEN 'WARNING: 1-2 months'
        WHEN current_battery / NULLIF(ABS(battery_trend_slope), 0) < 16 THEN 'MONITOR: 2-4 months'
        ELSE 'OK: 4+ months'
    END AS battery_runway,
    
    -- AI Predictive Analysis
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
        'IoT node degradation analysis over ' || weeks_monitored || ' weeks: ' ||
        'RSSI trend: ' || ROUND(rssi_trend_slope, 1) || ' dBm/week (now ' || ROUND(current_rssi, 0) || 'dBm), ' ||
        'Battery trend: ' || ROUND(battery_trend_slope, 1) || '%/week (now ' || ROUND(current_battery, 0) || '%), ' ||
        'Voltage trend: ' || ROUND(voltage_trend_slope, 3) || 'V/week, ' ||
        'Activity trend: ' || ROUND(activity_trend_slope, 0) || ' packets/week. ' ||
        'Predict failure timeline and recommend preventive action in 40 words.') AS ai_failure_prediction

FROM degradation_scores
ORDER BY degradation_score DESC;

-- Grant access
GRANT SELECT ON SEMANTIC VIEW DEMO.DEMO.MESHTASTIC_SEMANTIC_VIEW TO ROLE PUBLIC;

-- Verify creation
DESCRIBE SEMANTIC VIEW DEMO.DEMO.MESHTASTIC_SEMANTIC_VIEW;



