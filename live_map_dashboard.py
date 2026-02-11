#!/usr/bin/env python3
"""
Live Meshtastic Map Dashboard
Uses Dynamic Tables for near real-time data refresh
** Streamlit in Snowflake (SiS) Version **
"""
import streamlit as st
import pandas as pd
from datetime import datetime

from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Meshtastic Live Map",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

@st.cache_resource
def get_session():
    return get_active_session()

def run_query(sql: str) -> pd.DataFrame:
    try:
        session = get_session()
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

* { font-family: 'Inter', sans-serif; }

.main-header {
    background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 100%);
    padding: 20px;
    border-radius: 15px;
    margin-bottom: 20px;
    border: 1px solid #00ff88;
}

.metric-row {
    display: flex;
    gap: 15px;
    margin-bottom: 20px;
}

.metric-box {
    background: linear-gradient(135deg, #1a1a2e 0%, #2d2d5a 100%);
    padding: 15px 20px;
    border-radius: 12px;
    border-left: 4px solid #00ff88;
    flex: 1;
    text-align: center;
}

.metric-value {
    font-size: 2rem;
    font-weight: 700;
    color: #00ff88;
}

.metric-label {
    font-size: 0.9rem;
    color: #888;
    margin-top: 5px;
}

.node-card {
    background: #1a1a2e;
    border-radius: 12px;
    padding: 15px;
    margin: 10px 0;
    border-left: 4px solid #00ff88;
}

.node-active { border-left-color: #00ff88; }
.node-recent { border-left-color: #ffaa00; }
.node-stale { border-left-color: #ff6600; }
.node-offline { border-left-color: #ff3333; }

.status-dot {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    margin-right: 8px;
}

.status-active { background: #00ff88; box-shadow: 0 0 10px #00ff88; }
.status-recent { background: #ffaa00; }
.status-stale { background: #ff6600; }
.status-offline { background: #ff3333; }

.live-indicator {
    display: inline-flex;
    align-items: center;
    background: rgba(0, 255, 136, 0.1);
    padding: 5px 12px;
    border-radius: 20px;
    font-size: 0.85rem;
    color: #00ff88;
}

.live-dot {
    width: 8px;
    height: 8px;
    background: #00ff88;
    border-radius: 50%;
    margin-right: 8px;
    animation: pulse 2s infinite;
}

@keyframes pulse {
    0% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.5; transform: scale(1.2); }
    100% { opacity: 1; transform: scale(1); }
}

.data-table {
    background: #1a1a2e;
    border-radius: 10px;
    overflow: hidden;
}
</style>
""", unsafe_allow_html=True)

col_title, col_live = st.columns([4, 1])
with col_title:
    st.markdown("# 🗺️ Meshtastic Live Map")
    st.markdown("*Real-time mesh network visualization powered by Dynamic Tables*")
with col_live:
    st.markdown("""
    <div class="live-indicator">
        <div class="live-dot"></div>
        LIVE
    </div>
    """, unsafe_allow_html=True)

summary_query = """
SELECT 
    COUNT(DISTINCT node_id) as total_nodes,
    SUM(packet_count) as total_packets,
    ROUND(AVG(battery_level), 0) as avg_battery,
    ROUND(AVG(avg_snr), 1) as avg_snr,
    COUNT(DISTINCT CASE WHEN mins_ago <= 10 THEN node_id END) as active_nodes,
    COUNT(DISTINCT CASE WHEN latitude IS NOT NULL THEN node_id END) as nodes_with_gps
FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
"""

summary = run_query(summary_query)

if not summary.empty:
    row = summary.iloc[0]
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric("🛰️ Total Nodes", int(row['TOTAL_NODES']) if pd.notna(row['TOTAL_NODES']) else 0)
    with col2:
        st.metric("🟢 Active (10m)", int(row['ACTIVE_NODES']) if pd.notna(row['ACTIVE_NODES']) else 0)
    with col3:
        st.metric("📍 With GPS", int(row['NODES_WITH_GPS']) if pd.notna(row['NODES_WITH_GPS']) else 0)
    with col4:
        st.metric("📦 Packets", int(row['TOTAL_PACKETS']) if pd.notna(row['TOTAL_PACKETS']) else 0)
    with col5:
        st.metric("🔋 Avg Battery", f"{int(row['AVG_BATTERY'])}%" if pd.notna(row['AVG_BATTERY']) else "N/A")
    with col6:
        st.metric("📶 Avg SNR", f"{row['AVG_SNR']} dB" if pd.notna(row['AVG_SNR']) else "N/A")

st.divider()

tab_map, tab_nodes, tab_packets, tab_messages = st.tabs(["🗺️ Live Map", "📋 Node Details", "📊 Packets", "💬 Messages"])

with tab_map:
    nodes_query = """
    SELECT 
        node_id,
        latitude,
        longitude,
        altitude,
        battery_level,
        avg_snr,
        packet_count,
        temperature,
        mins_ago,
        last_seen,
        CASE 
            WHEN mins_ago <= 5 THEN 'Active'
            WHEN mins_ago <= 30 THEN 'Recent'
            WHEN mins_ago <= 60 THEN 'Stale'
            ELSE 'Offline'
        END as status
    FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
    WHERE latitude IS NOT NULL 
        AND longitude IS NOT NULL
        AND latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
    ORDER BY last_seen DESC
    """
    
    nodes_df = run_query(nodes_query)
    
    if not nodes_df.empty:
        col_map, col_list = st.columns([2, 1])
        
        with col_map:
            st.markdown("### 📍 Node Locations")
            
            map_df = nodes_df[['LATITUDE', 'LONGITUDE']].copy()
            map_df.columns = ['lat', 'lon']
            
            st.map(map_df, zoom=10)
            
            st.caption(f"Showing {len(nodes_df)} nodes with GPS coordinates")
        
        with col_list:
            st.markdown("### 📡 Nodes on Map")
            
            for _, node in nodes_df.iterrows():
                status = node['STATUS']
                status_class = status.lower()
                status_color = {
                    'Active': '#00ff88',
                    'Recent': '#ffaa00', 
                    'Stale': '#ff6600',
                    'Offline': '#ff3333'
                }.get(status, '#888')
                
                battery = f"{int(node['BATTERY_LEVEL'])}%" if pd.notna(node['BATTERY_LEVEL']) else "N/A"
                snr = f"{node['AVG_SNR']} dB" if pd.notna(node['AVG_SNR']) else "N/A"
                mins = int(node['MINS_AGO']) if pd.notna(node['MINS_AGO']) else "?"
                
                st.markdown(f"""
                <div class="node-card node-{status_class}">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span class="status-dot status-{status_class}"></span>
                            <strong>{node['NODE_ID']}</strong>
                        </div>
                        <span style="color: {status_color}; font-size: 0.8rem;">{status}</span>
                    </div>
                    <div style="margin-top: 10px; display: grid; grid-template-columns: 1fr 1fr; gap: 5px; font-size: 0.85rem; color: #aaa;">
                        <div>🔋 {battery}</div>
                        <div>📶 {snr}</div>
                        <div>📍 {node['LATITUDE']:.4f}, {node['LONGITUDE']:.4f}</div>
                        <div>⏱️ {mins}m ago</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.warning("No nodes with GPS coordinates found")
        st.info("Nodes will appear here when they report location data")

with tab_nodes:
    st.markdown("### 📋 All Nodes Summary")
    
    all_nodes_query = """
    SELECT 
        node_id as "Node ID",
        packet_count as "Packets",
        battery_level as "Battery %",
        avg_snr as "Avg SNR",
        ROUND(latitude, 4) as "Latitude",
        ROUND(longitude, 4) as "Longitude",
        ROUND(altitude, 0) as "Altitude (m)",
        ROUND(temperature, 1) as "Temp °C",
        last_seen as "Last Seen",
        mins_ago as "Mins Ago",
        CASE 
            WHEN mins_ago <= 5 THEN '🟢 Active'
            WHEN mins_ago <= 30 THEN '🟡 Recent'
            WHEN mins_ago <= 60 THEN '🟠 Stale'
            ELSE '🔴 Offline'
        END as "Status"
    FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
    ORDER BY last_seen DESC
    """
    
    all_nodes_df = run_query(all_nodes_query)
    
    if not all_nodes_df.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Nodes", len(all_nodes_df))
        with col2:
            active = len(all_nodes_df[all_nodes_df['Mins Ago'] <= 10])
            st.metric("Active (≤10m)", active)
        with col3:
            with_gps = len(all_nodes_df[all_nodes_df['Latitude'].notna()])
            st.metric("With GPS", with_gps)
        
        st.dataframe(all_nodes_df, use_container_width=True, height=400)

with tab_packets:
    st.markdown("### 📊 Recent Packets")
    
    packets_query = """
    SELECT 
        ingested_at as "Time",
        from_id as "From",
        packet_type as "Type",
        ROUND(latitude, 4) as "Lat",
        ROUND(longitude, 4) as "Lon",
        battery_level as "Bat%",
        ROUND(rx_snr, 1) as "SNR",
        rx_rssi as "RSSI",
        ROUND(temperature, 1) as "Temp",
        text_message as "Message"
    FROM DEMO.DEMO.MESHTASTIC_LIVE_AUTO
    ORDER BY ingested_at DESC
    LIMIT 100
    """
    
    packets_df = run_query(packets_query)
    
    if not packets_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            packet_types = packets_df['Type'].value_counts()
            st.markdown("#### Packet Types")
            st.bar_chart(packet_types)
        
        with col2:
            st.markdown("#### By Node")
            node_counts = packets_df['From'].value_counts().head(10)
            st.bar_chart(node_counts)
        
        st.markdown("#### Recent Packet Stream")
        st.dataframe(packets_df, use_container_width=True, height=300)

with tab_messages:
    st.markdown("### 💬 Text Messages")
    
    messages_query = """
    SELECT 
        ingested_at as "Time",
        from_id as "From",
        text_message as "Message",
        rx_snr as "SNR"
    FROM DEMO.DEMO.MESHTASTIC_LIVE_AUTO
    WHERE packet_type = 'text'
        AND text_message IS NOT NULL
        AND text_message != ''
    ORDER BY ingested_at DESC
    LIMIT 50
    """
    
    messages_df = run_query(messages_query)
    
    if not messages_df.empty:
        for _, msg in messages_df.iterrows():
            time_str = msg['Time'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(msg['Time']) else 'N/A'
            snr = f"{msg['SNR']:.1f} dB" if pd.notna(msg['SNR']) else "N/A"
            
            st.markdown(f"""
            <div style="background: #1a1a2e; padding: 15px; border-radius: 12px; margin: 10px 0; border-left: 3px solid #00ff88;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                    <strong style="color: #00ff88;">{msg['From']}</strong>
                    <span style="color: #666; font-size: 0.85rem;">{time_str} • SNR: {snr}</span>
                </div>
                <div style="color: #ddd; font-size: 1.1rem;">{msg['Message']}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No text messages found in the last 24 hours")

st.divider()

col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

st.markdown(f"""
<div style="text-align: center; color: #666; font-size: 0.85rem; margin-top: 20px;">
    Data refreshes automatically via Dynamic Tables (1 minute lag) • 
    Last UI refresh: {datetime.now().strftime('%H:%M:%S')} •
    Powered by Snowflake ❄️
</div>
""", unsafe_allow_html=True)
