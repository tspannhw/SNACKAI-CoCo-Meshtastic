#!/usr/bin/env python3
"""
Advanced Geospatial Dashboard with AI Functions
Meshtastic Mesh Network Node Analysis
** Streamlit in Snowflake (SiS) Version **
"""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2

# Snowflake-native imports for Streamlit in Snowflake
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Mesh Network Geospatial Intelligence",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_session():
    """Get the active Snowflake session (SiS native connection)"""
    return get_active_session()

def run_query(sql: str) -> pd.DataFrame:
    """Execute SQL query and return pandas DataFrame"""
    try:
        session = get_session()
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km"""
    R = 6371
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def get_ai_insight(prompt: str, model: str = "mistral-large2") -> str:
    """Get AI insight using Cortex COMPLETE"""
    safe_prompt = prompt.replace("'", "''")[:4000]
    sql = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        '{model}',
        '{safe_prompt}'
    ) as insight
    """
    result = run_query(sql)
    if not result.empty and result['INSIGHT'].iloc[0]:
        return result['INSIGHT'].iloc[0]
    return None

st.markdown("""
<style>
.metric-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    padding: 20px;
    border-radius: 15px;
    border: 1px solid #0f3460;
    margin: 10px 0;
}
.node-badge {
    display: inline-block;
    padding: 5px 15px;
    border-radius: 20px;
    background: #0f3460;
    color: #e94560;
    font-weight: bold;
}
.ai-insight {
    background: linear-gradient(135deg, #0f3460 0%, #1a1a2e 100%);
    padding: 15px;
    border-radius: 10px;
    border-left: 4px solid #e94560;
    margin: 10px 0;
}
</style>
""", unsafe_allow_html=True)

st.title("🛰️ Mesh Network Geospatial Intelligence")
st.markdown("*AI-Enhanced Node Connectivity & Spatial Analysis - Powered by Snowflake*")

with st.sidebar:
    st.header("⚙️ Configuration")
    
    time_range = st.selectbox(
        "Time Range",
        ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days", "All Time"],
        index=2
    )
    
    time_map = {
        "Last Hour": "DATEADD(hour, -1, CURRENT_TIMESTAMP())",
        "Last 6 Hours": "DATEADD(hour, -6, CURRENT_TIMESTAMP())",
        "Last 24 Hours": "DATEADD(hour, -24, CURRENT_TIMESTAMP())",
        "Last 7 Days": "DATEADD(day, -7, CURRENT_TIMESTAMP())",
        "All Time": "'2020-01-01'"
    }
    time_filter = time_map[time_range]
    
    st.divider()
    ai_model = st.selectbox(
        "AI Model",
        ["mistral-large2", "llama3.1-70b", "llama3.1-8b"],
        index=0
    )
    
    st.divider()
    show_connections = st.checkbox("Show Node Connections", value=True)
    show_coverage = st.checkbox("Show Coverage Radius", value=True)

nodes_query = f"""
SELECT 
    from_id,
    MAX(latitude) as latitude,
    MAX(longitude) as longitude,
    MAX(altitude) as altitude,
    MAX(battery_level) as battery_level,
    MAX(rx_snr) as snr,
    MAX(rx_rssi) as rssi,
    MAX(temperature) as temperature,
    MAX(relative_humidity) as humidity,
    MAX(ground_speed) as speed,
    COUNT(*) as packet_count,
    MAX(ingested_at) as last_seen,
    DATEDIFF(minute, MAX(ingested_at), CURRENT_TIMESTAMP()) as mins_ago
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= {time_filter}
    AND latitude IS NOT NULL 
    AND longitude IS NOT NULL
    AND latitude BETWEEN -90 AND 90
    AND longitude BETWEEN -180 AND 180
GROUP BY from_id
ORDER BY last_seen DESC
"""

nodes_df = run_query(nodes_query)

tab_map, tab_network, tab_coverage, tab_ai, tab_analytics, tab_live = st.tabs([
    "🗺️ Node Map",
    "🔗 Network Topology",
    "📡 Coverage Analysis",
    "🤖 AI Insights",
    "📊 Spatial Analytics",
    "📋 Live Data"
])

with tab_map:
    if nodes_df.empty:
        st.warning("No node data available. Connect a Meshtastic device to start collecting data.")
        st.info("Sample visualization will be shown with demo coordinates.")
        nodes_df = pd.DataFrame({
            'FROM_ID': ['!4b14demo1', '!4b14demo2', '!4b14demo3'],
            'LATITUDE': [40.7589, 40.7614, 40.7549],
            'LONGITUDE': [-73.9851, -73.9776, -73.9840],
            'ALTITUDE': [15, 22, 18],
            'BATTERY_LEVEL': [85, 72, 45],
            'SNR': [9.5, 7.2, 11.0],
            'RSSI': [-85, -92, -78],
            'TEMPERATURE': [22.5, 23.1, 21.8],
            'HUMIDITY': [45, 52, 48],
            'SPEED': [0, 1.2, 0],
            'PACKET_COUNT': [150, 89, 210],
            'LAST_SEEN': [datetime.now()] * 3,
            'MINS_AGO': [5, 12, 2]
        })
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("🛰️ Active Nodes", len(nodes_df))
    with col2:
        avg_snr = nodes_df['SNR'].mean() if 'SNR' in nodes_df.columns else 0
        st.metric("📶 Avg SNR", f"{avg_snr:.1f} dB" if pd.notna(avg_snr) else "N/A")
    with col3:
        avg_bat = nodes_df['BATTERY_LEVEL'].mean() if 'BATTERY_LEVEL' in nodes_df.columns else 0
        st.metric("🔋 Avg Battery", f"{avg_bat:.0f}%" if pd.notna(avg_bat) else "N/A")
    with col4:
        total_packets = nodes_df['PACKET_COUNT'].sum() if 'PACKET_COUNT' in nodes_df.columns else 0
        st.metric("📦 Total Packets", f"{total_packets:,}")
    
    if not nodes_df.empty and 'LATITUDE' in nodes_df.columns:
        st.markdown("### 🗺️ Node Locations")
        
        map_df = nodes_df[['LATITUDE', 'LONGITUDE']].copy()
        map_df.columns = ['lat', 'lon']
        
        st.map(map_df)
        
        st.markdown("### 📋 Node Details")
        
        display_df = nodes_df[['FROM_ID', 'LATITUDE', 'LONGITUDE', 'BATTERY_LEVEL', 'SNR', 'PACKET_COUNT', 'MINS_AGO']].copy()
        display_df['Status'] = display_df['MINS_AGO'].apply(
            lambda x: '🟢 Active' if x <= 5 else ('🟡 Recent' if x <= 30 else ('🟠 Stale' if x <= 60 else '🔴 Offline'))
        )
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "FROM_ID": st.column_config.TextColumn("Node ID", width="small"),
                "LATITUDE": st.column_config.NumberColumn("Latitude", format="%.6f"),
                "LONGITUDE": st.column_config.NumberColumn("Longitude", format="%.6f"),
                "BATTERY_LEVEL": st.column_config.ProgressColumn("Battery %", min_value=0, max_value=100),
                "SNR": st.column_config.NumberColumn("SNR (dB)", format="%.1f"),
                "PACKET_COUNT": st.column_config.NumberColumn("Packets"),
                "MINS_AGO": st.column_config.NumberColumn("Mins Ago"),
                "Status": st.column_config.TextColumn("Status", width="small")
            }
        )

with tab_network:
    st.subheader("🔗 Network Topology Analysis")
    
    if len(nodes_df) > 1:
        st.markdown("### Node Distance Matrix")
        
        node_ids = nodes_df['FROM_ID'].tolist()
        n = len(node_ids)
        dist_matrix = np.zeros((n, n))
        
        for i in range(n):
            for j in range(n):
                if i != j:
                    dist_matrix[i][j] = haversine_distance(
                        nodes_df.iloc[i]['LATITUDE'], nodes_df.iloc[i]['LONGITUDE'],
                        nodes_df.iloc[j]['LATITUDE'], nodes_df.iloc[j]['LONGITUDE']
                    )
        
        dist_df = pd.DataFrame(dist_matrix, index=node_ids, columns=node_ids)
        st.dataframe(dist_df.style.format("{:.2f} km").background_gradient(cmap='RdYlGn_r'), use_container_width=True)
        
        st.markdown("### Connection Quality")
        
        connections_data = []
        for i in range(n):
            for j in range(i+1, n):
                dist = dist_matrix[i][j]
                snr1 = nodes_df.iloc[i].get('SNR', 0) or 0
                snr2 = nodes_df.iloc[j].get('SNR', 0) or 0
                avg_snr = (snr1 + snr2) / 2
                
                if dist < 1:
                    quality = "Excellent"
                    color = "🟢"
                elif dist < 3:
                    quality = "Good"
                    color = "🟡"
                elif dist < 5:
                    quality = "Fair"
                    color = "🟠"
                else:
                    quality = "Poor"
                    color = "🔴"
                
                connections_data.append({
                    "Node A": node_ids[i],
                    "Node B": node_ids[j],
                    "Distance (km)": f"{dist:.2f}",
                    "Avg SNR": f"{avg_snr:.1f} dB",
                    "Quality": f"{color} {quality}"
                })
        
        if connections_data:
            conn_table = pd.DataFrame(connections_data)
            st.dataframe(conn_table, use_container_width=True, hide_index=True)
        
        if st.button("🤖 Analyze Network Topology", key="topo_ai"):
            with st.spinner("AI analyzing network..."):
                topo_summary = f"Mesh network with {n} nodes. "
                for i, row in nodes_df.iterrows():
                    topo_summary += f"{row['FROM_ID']}: SNR={row.get('SNR', 'N/A')}dB, Battery={row.get('BATTERY_LEVEL', 'N/A')}%. "
                
                prompt = f"""Analyze this mesh network topology and provide recommendations:
                {topo_summary}
                
                Provide:
                1. Network health assessment
                2. Weak links that need attention
                3. Optimal relay node suggestions
                4. Coverage gap analysis
                Keep response concise (under 200 words)."""
                
                insight = get_ai_insight(prompt, ai_model)
                if insight:
                    st.markdown(f'<div class="ai-insight">{insight}</div>', unsafe_allow_html=True)
    else:
        st.info("Need at least 2 nodes for topology analysis")

with tab_coverage:
    st.subheader("📡 Coverage Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Estimated Coverage Parameters")
        tx_power = st.slider("TX Power (dBm)", 0, 30, 20)
        antenna_gain = st.slider("Antenna Gain (dBi)", 0, 10, 3)
        freq_mhz = st.selectbox("Frequency", [868, 915, 433], index=1)
        terrain = st.selectbox("Terrain", ["Urban", "Suburban", "Rural", "Open"], index=1)
        
        terrain_factor = {"Urban": 0.3, "Suburban": 0.5, "Rural": 0.7, "Open": 1.0}
        base_range = 2.0
        est_range = base_range * terrain_factor[terrain] * (tx_power / 20) * (1 + antenna_gain / 10)
        
        st.metric("Estimated Range", f"{est_range:.1f} km")
    
    with col2:
        st.markdown("### Coverage Statistics")
        if not nodes_df.empty and len(nodes_df) > 1:
            total_area = 0
            for _, node in nodes_df.iterrows():
                total_area += 3.14159 * (est_range ** 2)
            
            min_lat = nodes_df['LATITUDE'].min()
            max_lat = nodes_df['LATITUDE'].max()
            min_lon = nodes_df['LONGITUDE'].min()
            max_lon = nodes_df['LONGITUDE'].max()
            
            bounding_width = haversine_distance(min_lat, min_lon, min_lat, max_lon)
            bounding_height = haversine_distance(min_lat, min_lon, max_lat, min_lon)
            bounding_area = bounding_width * bounding_height
            
            st.metric("Total Coverage Area", f"{total_area:.1f} km²")
            st.metric("Network Span", f"{max(bounding_width, bounding_height):.2f} km")
            st.metric("Bounding Box", f"{bounding_area:.2f} km²")
            
            overlap_est = min(100, (total_area / max(bounding_area, 0.1)) * 30)
            st.metric("Coverage Overlap", f"{overlap_est:.0f}%")
    
    if st.button("🤖 AI Coverage Optimization", key="coverage_ai"):
        with st.spinner("Analyzing coverage..."):
            coverage_info = f"""
            Network: {len(nodes_df)} nodes
            Frequency: {freq_mhz} MHz
            Terrain: {terrain}
            Estimated range per node: {est_range:.1f} km
            TX Power: {tx_power} dBm
            """
            
            prompt = f"""As a RF engineer, analyze this mesh network coverage and suggest optimizations:
            {coverage_info}
            
            Provide:
            1. Coverage efficiency assessment
            2. Optimal node placement suggestions
            3. Power/antenna recommendations
            4. Dead zone mitigation strategies
            Keep response under 150 words."""
            
            insight = get_ai_insight(prompt, ai_model)
            if insight:
                st.markdown(f'<div class="ai-insight">{insight}</div>', unsafe_allow_html=True)

with tab_ai:
    st.subheader("🤖 AI-Powered Network Intelligence")
    
    analysis_type = st.selectbox(
        "Analysis Type",
        [
            "Network Health Summary",
            "Anomaly Detection",
            "Predictive Maintenance",
            "Signal Optimization",
            "Custom Query"
        ]
    )
    
    if analysis_type == "Custom Query":
        custom_query = st.text_area("Enter your question about the mesh network:", height=100)
        if st.button("Ask AI", key="custom_ai"):
            if custom_query:
                with st.spinner("Processing..."):
                    node_summary = ""
                    for _, row in nodes_df.head(5).iterrows():
                        node_summary += f"Node {row['FROM_ID']}: Battery={row.get('BATTERY_LEVEL', 'N/A')}%, SNR={row.get('SNR', 'N/A')}dB, Packets={row.get('PACKET_COUNT', 0)}. "
                    
                    prompt = f"""Mesh network context: {node_summary}
                    
                    User question: {custom_query}
                    
                    Provide a helpful, technical response."""
                    
                    insight = get_ai_insight(prompt, ai_model)
                    if insight:
                        st.markdown(f'<div class="ai-insight">{insight}</div>', unsafe_allow_html=True)
    else:
        if st.button(f"Run {analysis_type}", key="analysis_ai"):
            with st.spinner(f"Running {analysis_type}..."):
                node_data = ""
                for _, row in nodes_df.iterrows():
                    node_data += f"{row['FROM_ID']}: bat={row.get('BATTERY_LEVEL', 'N/A')}%, snr={row.get('SNR', 'N/A')}dB, temp={row.get('TEMPERATURE', 'N/A')}C, pkts={row.get('PACKET_COUNT', 0)}, last={row.get('MINS_AGO', 'N/A')}min. "
                
                prompts = {
                    "Network Health Summary": f"Provide a health summary for this mesh network: {node_data}. Include overall status, concerns, and recommendations in under 150 words.",
                    "Anomaly Detection": f"Analyze this mesh network data for anomalies: {node_data}. Identify any unusual patterns in battery, signal, or activity. Be specific about which nodes show anomalies.",
                    "Predictive Maintenance": f"Based on this mesh network telemetry: {node_data}. Predict which nodes may need maintenance soon and why. Prioritize by urgency.",
                    "Signal Optimization": f"Analyze signal quality for this mesh network: {node_data}. Suggest specific optimizations to improve SNR and reduce packet loss."
                }
                
                prompt = prompts.get(analysis_type, "Analyze this network data.")
                insight = get_ai_insight(prompt, ai_model)
                if insight:
                    st.markdown(f'<div class="ai-insight">{insight}</div>', unsafe_allow_html=True)
    
    st.divider()
    st.markdown("### 📊 Sentiment Analysis on Text Messages")
    
    text_query = f"""
    SELECT from_id, text_message, ingested_at, rx_snr
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE packet_type = 'text' 
        AND text_message IS NOT NULL
        AND ingested_at >= {time_filter}
    ORDER BY ingested_at DESC
    LIMIT 10
    """
    
    text_df = run_query(text_query)
    
    if not text_df.empty:
        for _, row in text_df.iterrows():
            text = row.get('TEXT_MESSAGE') or row.get('text_message')
            if text:
                safe_text = str(text).replace("'", "''")[:500]
                sentiment_query = f"""
                SELECT SNOWFLAKE.CORTEX.SENTIMENT('{safe_text}') as sentiment
                """
                sent_result = run_query(sentiment_query)
                sentiment = sent_result['SENTIMENT'].iloc[0] if not sent_result.empty else 0
                
                if sentiment > 0.3:
                    emoji = "😊"
                    label = "Positive"
                elif sentiment < -0.3:
                    emoji = "😟"
                    label = "Negative"
                else:
                    emoji = "😐"
                    label = "Neutral"
                
                from_id = row.get('FROM_ID') or row.get('from_id')
                st.markdown(f"""
                **{from_id}** {emoji} *{label}* (score: {sentiment:.2f})
                > {text}
                """)
    else:
        st.info("No text messages found in the selected time range")

with tab_analytics:
    st.subheader("📊 Spatial Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Node Distribution")
        if not nodes_df.empty:
            st.bar_chart(nodes_df.set_index('FROM_ID')['PACKET_COUNT'])
    
    with col2:
        st.markdown("### Signal Quality Distribution")
        if not nodes_df.empty and 'SNR' in nodes_df.columns:
            snr_data = nodes_df[['FROM_ID', 'SNR']].dropna()
            if not snr_data.empty:
                st.bar_chart(snr_data.set_index('FROM_ID')['SNR'])
    
    st.markdown("### Geospatial Statistics")
    
    if not nodes_df.empty:
        geo_stats = {
            "Total Nodes": len(nodes_df),
            "Latitude Range": f"{nodes_df['LATITUDE'].min():.4f} to {nodes_df['LATITUDE'].max():.4f}",
            "Longitude Range": f"{nodes_df['LONGITUDE'].min():.4f} to {nodes_df['LONGITUDE'].max():.4f}",
            "Avg Altitude": f"{nodes_df['ALTITUDE'].mean():.1f} m" if 'ALTITUDE' in nodes_df.columns and nodes_df['ALTITUDE'].notna().any() else "N/A",
            "Network Centroid": f"({nodes_df['LATITUDE'].mean():.4f}, {nodes_df['LONGITUDE'].mean():.4f})"
        }
        
        stats_df = pd.DataFrame(list(geo_stats.items()), columns=['Metric', 'Value'])
        st.dataframe(stats_df, use_container_width=True, hide_index=True)
    
    st.markdown("### 📈 Packet Activity Over Time")
    
    activity_query = f"""
    SELECT 
        DATE_TRUNC('hour', ingested_at) as hour,
        COUNT(*) as packets,
        COUNT(DISTINCT from_id) as active_nodes
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= {time_filter}
    GROUP BY DATE_TRUNC('hour', ingested_at)
    ORDER BY hour
    """
    
    activity_df = run_query(activity_query)
    
    if not activity_df.empty:
        st.line_chart(activity_df.set_index('HOUR')[['PACKETS', 'ACTIVE_NODES']])

with tab_live:
    st.subheader("📋 Live Mesh Network Data")
    st.markdown("*Interactive table with real-time mesh network telemetry*")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        live_limit = st.selectbox("Records to show", [50, 100, 250, 500], index=1)
    with col2:
        packet_filter = st.multiselect(
            "Packet Types",
            ["position", "telemetry", "text", "nodeinfo", "routing"],
            default=["position", "telemetry", "text"]
        )
    with col3:
        if st.button("🔄 Refresh"):
            st.rerun()
    
    packet_filter_sql = "'" + "','".join(packet_filter) + "'" if packet_filter else "'position','telemetry','text'"
    
    live_query = f"""
    SELECT 
        ingested_at as "Timestamp",
        from_id as "Node ID",
        packet_type as "Type",
        ROUND(latitude, 6) as "Latitude",
        ROUND(longitude, 6) as "Longitude",
        altitude as "Altitude (m)",
        battery_level as "Battery %",
        ROUND(voltage, 2) as "Voltage (V)",
        ROUND(temperature, 1) as "Temp (°C)",
        ROUND(relative_humidity, 1) as "Humidity %",
        ROUND(rx_snr, 2) as "SNR (dB)",
        rx_rssi as "RSSI (dBm)",
        ground_speed as "Speed (m/s)",
        sats_in_view as "Satellites",
        text_message as "Message",
        hop_limit as "Hop Limit",
        channel as "Channel"
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= {time_filter}
        AND packet_type IN ({packet_filter_sql})
    ORDER BY ingested_at DESC
    LIMIT {live_limit}
    """
    
    live_df = run_query(live_query)
    
    if not live_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📦 Records", len(live_df))
        with col2:
            unique_nodes = live_df['Node ID'].nunique()
            st.metric("🛰️ Unique Nodes", unique_nodes)
        with col3:
            if 'Battery %' in live_df.columns:
                avg_bat = live_df['Battery %'].dropna().mean()
                st.metric("🔋 Avg Battery", f"{avg_bat:.0f}%" if pd.notna(avg_bat) else "N/A")
            else:
                st.metric("🔋 Avg Battery", "N/A")
        with col4:
            if 'SNR (dB)' in live_df.columns:
                avg_snr = live_df['SNR (dB)'].dropna().mean()
                st.metric("📶 Avg SNR", f"{avg_snr:.1f} dB" if pd.notna(avg_snr) else "N/A")
            else:
                st.metric("📶 Avg SNR", "N/A")
        
        st.markdown("### 📊 Interactive Data Table")
        
        st.dataframe(
            live_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Timestamp": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm:ss"),
                "Node ID": st.column_config.TextColumn("Node ID", width="small"),
                "Type": st.column_config.TextColumn("Type", width="small"),
                "Battery %": st.column_config.ProgressColumn("Battery %", min_value=0, max_value=100, format="%d%%"),
                "SNR (dB)": st.column_config.NumberColumn("SNR (dB)", format="%.2f"),
                "Temp (°C)": st.column_config.NumberColumn("Temp (°C)", format="%.1f"),
                "Message": st.column_config.TextColumn("Message", width="large")
            },
            height=500
        )
        
        st.divider()
        
        st.markdown("### 🔍 Node Summary")
        
        node_summary_query = f"""
        SELECT 
            from_id as "Node ID",
            COUNT(*) as "Packets",
            MAX(battery_level) as "Battery %",
            ROUND(AVG(rx_snr), 2) as "Avg SNR",
            MAX(latitude) as "Last Lat",
            MAX(longitude) as "Last Lon",
            MAX(ingested_at) as "Last Seen",
            DATEDIFF(minute, MAX(ingested_at), CURRENT_TIMESTAMP()) as "Mins Ago"
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE ingested_at >= {time_filter}
        GROUP BY from_id
        ORDER BY MAX(ingested_at) DESC
        """
        
        node_summary_df = run_query(node_summary_query)
        
        if not node_summary_df.empty:
            def status_color(mins):
                if pd.isna(mins):
                    return "⚫ Unknown"
                elif mins <= 5:
                    return "🟢 Active"
                elif mins <= 30:
                    return "🟡 Recent"
                elif mins <= 60:
                    return "🟠 Stale"
                else:
                    return "🔴 Offline"
            
            node_summary_df["Status"] = node_summary_df["Mins Ago"].apply(status_color)
            
            st.dataframe(
                node_summary_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Last Seen": st.column_config.DatetimeColumn("Last Seen", format="YYYY-MM-DD HH:mm:ss"),
                    "Battery %": st.column_config.ProgressColumn("Battery %", min_value=0, max_value=100, format="%d%%"),
                    "Status": st.column_config.TextColumn("Status", width="small")
                },
                height=300
            )
        
        st.divider()
        
        st.markdown("### 💬 Recent Text Messages")
        
        text_msg_query = f"""
        SELECT 
            ingested_at as "Time",
            from_id as "From",
            text_message as "Message",
            rx_snr as "SNR"
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE ingested_at >= {time_filter}
            AND packet_type = 'text'
            AND text_message IS NOT NULL
        ORDER BY ingested_at DESC
        LIMIT 20
        """
        
        text_msg_df = run_query(text_msg_query)
        
        if not text_msg_df.empty:
            for _, row in text_msg_df.iterrows():
                time_str = row['Time'].strftime('%H:%M:%S') if pd.notna(row['Time']) else 'N/A'
                st.markdown(f"""
                **{row['From']}** • {time_str} • SNR: {row['SNR']} dB
                > {row['Message']}
                """)
        else:
            st.info("No text messages in selected time range")
        
    else:
        st.warning("No data available for the selected filters")

st.sidebar.divider()
st.sidebar.markdown("### 📡 Quick Actions")

if st.sidebar.button("🔄 Refresh Data"):
    st.rerun()

if st.sidebar.button("📤 Export Node Data"):
    if not nodes_df.empty:
        csv = nodes_df.to_csv(index=False)
        st.sidebar.download_button(
            "Download CSV",
            csv,
            "mesh_nodes.csv",
            "text/csv"
        )

st.sidebar.divider()
st.sidebar.markdown(f"*Last updated: {datetime.now().strftime('%H:%M:%S')}*")
st.sidebar.markdown("*Running in Snowflake ❄️*")
