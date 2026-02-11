#!/usr/bin/env python3
"""
Meshtastic Live Dashboard - Interactive Tables
Sub-second queries with auto-refresh every 5 seconds
** Streamlit in Snowflake (SiS) Version **
"""
import streamlit as st
import pandas as pd
from datetime import datetime

from snowflake.snowpark.context import get_active_session
import time

REFRESH_INTERVAL = 5

st.set_page_config(
    page_title="Meshtastic Live",
    page_icon="⚡",
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
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600;700&display=swap');

* { font-family: 'Inter', sans-serif; }
code, .mono { font-family: 'JetBrains Mono', monospace; }

.hero {
    background: linear-gradient(135deg, #0a0a1a 0%, #1a1a3e 50%, #0f2027 100%);
    padding: 25px 30px;
    border-radius: 20px;
    margin-bottom: 25px;
    border: 1px solid rgba(0, 255, 136, 0.3);
    box-shadow: 0 0 40px rgba(0, 255, 136, 0.1);
}

.hero-title {
    font-size: 2.2rem;
    font-weight: 700;
    background: linear-gradient(90deg, #00ff88, #00d4ff);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin: 0;
}

.hero-subtitle {
    color: #888;
    margin-top: 5px;
}

.live-badge {
    display: inline-flex;
    align-items: center;
    background: rgba(0, 255, 136, 0.15);
    border: 1px solid rgba(0, 255, 136, 0.5);
    padding: 8px 16px;
    border-radius: 30px;
    font-size: 0.9rem;
    color: #00ff88;
    font-weight: 600;
}

.live-pulse {
    width: 10px;
    height: 10px;
    background: #00ff88;
    border-radius: 50%;
    margin-right: 10px;
    animation: pulse 1.5s infinite;
    box-shadow: 0 0 10px #00ff88;
}

@keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.6; transform: scale(1.3); }
}

.stat-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 15px;
    margin: 20px 0;
}

.stat-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #252550 100%);
    padding: 20px;
    border-radius: 15px;
    text-align: center;
    border: 1px solid rgba(255, 255, 255, 0.1);
    transition: transform 0.2s, box-shadow 0.2s;
}

.stat-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
}

.stat-value {
    font-size: 2rem;
    font-weight: 700;
    color: #00ff88;
    font-family: 'JetBrains Mono', monospace;
}

.stat-label {
    color: #888;
    font-size: 0.85rem;
    margin-top: 5px;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.node-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 15px;
}

.node-card {
    background: linear-gradient(145deg, #1a1a2e 0%, #12122a 100%);
    border-radius: 15px;
    padding: 18px;
    border-left: 4px solid #00ff88;
    transition: all 0.2s;
}

.node-card:hover {
    transform: translateX(5px);
    box-shadow: -5px 0 20px rgba(0, 255, 136, 0.2);
}

.node-card.active { border-left-color: #00ff88; }
.node-card.recent { border-left-color: #ffaa00; }
.node-card.stale { border-left-color: #ff6600; }
.node-card.offline { border-left-color: #ff3333; }

.node-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
}

.node-id {
    font-family: 'JetBrains Mono', monospace;
    font-weight: 700;
    color: #fff;
    font-size: 1.1rem;
}

.node-status {
    padding: 4px 10px;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
}

.status-active { background: rgba(0, 255, 136, 0.2); color: #00ff88; }
.status-recent { background: rgba(255, 170, 0, 0.2); color: #ffaa00; }
.status-stale { background: rgba(255, 102, 0, 0.2); color: #ff6600; }
.status-offline { background: rgba(255, 51, 51, 0.2); color: #ff3333; }

.node-stats {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 8px;
}

.node-stat {
    display: flex;
    align-items: center;
    gap: 6px;
    color: #aaa;
    font-size: 0.9rem;
}

.node-stat-icon {
    font-size: 1rem;
}

.node-stat-value {
    color: #ddd;
    font-family: 'JetBrains Mono', monospace;
}

.message-card {
    background: #1a1a2e;
    border-radius: 15px;
    padding: 15px 20px;
    margin: 10px 0;
    border-left: 3px solid #00d4ff;
}

.message-header {
    display: flex;
    justify-content: space-between;
    margin-bottom: 8px;
}

.message-sender {
    color: #00d4ff;
    font-weight: 600;
    font-family: 'JetBrains Mono', monospace;
}

.message-time {
    color: #666;
    font-size: 0.85rem;
}

.message-text {
    color: #eee;
    font-size: 1.05rem;
    line-height: 1.4;
}

.packet-row {
    background: rgba(255, 255, 255, 0.03);
    padding: 10px 15px;
    border-radius: 8px;
    margin: 5px 0;
    display: flex;
    gap: 15px;
    align-items: center;
    font-size: 0.9rem;
}

.packet-type {
    background: rgba(0, 212, 255, 0.2);
    color: #00d4ff;
    padding: 3px 10px;
    border-radius: 10px;
    font-size: 0.8rem;
    font-weight: 600;
    min-width: 80px;
    text-align: center;
}

.refresh-bar {
    background: linear-gradient(90deg, #0a0a1a, #1a1a3e);
    padding: 15px 25px;
    border-radius: 15px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 20px;
    border: 1px solid rgba(255, 255, 255, 0.1);
}

.tech-badge {
    background: rgba(0, 212, 255, 0.1);
    border: 1px solid rgba(0, 212, 255, 0.3);
    padding: 5px 12px;
    border-radius: 8px;
    font-size: 0.8rem;
    color: #00d4ff;
}
</style>
""", unsafe_allow_html=True)

col1, col2 = st.columns([3, 1])
with col1:
    st.markdown("""
    <div class="hero">
        <h1 class="hero-title">⚡ Meshtastic Live Dashboard</h1>
        <p class="hero-subtitle">Real-time mesh network monitoring • Interactive Tables • 60-second refresh</p>
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown("""
    <div style="text-align: right; padding-top: 20px;">
        <div class="live-badge">
            <div class="live-pulse"></div>
            LIVE DATA
        </div>
    </div>
    """, unsafe_allow_html=True)

summary = run_query("""
SELECT 
    COUNT(DISTINCT node_id) as total_nodes,
    SUM(packet_count) as total_packets,
    ROUND(AVG(battery_level), 0) as avg_battery,
    ROUND(AVG(avg_snr), 1) as avg_snr,
    COUNT(DISTINCT CASE WHEN mins_ago <= 10 THEN node_id END) as active_nodes,
    COUNT(DISTINCT CASE WHEN latitude IS NOT NULL THEN node_id END) as gps_nodes
FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
""")

if not summary.empty:
    r = summary.iloc[0]
    
    st.markdown(f"""
    <div class="stat-grid">
        <div class="stat-card">
            <div class="stat-value">{int(r['TOTAL_NODES']) if pd.notna(r['TOTAL_NODES']) else 0}</div>
            <div class="stat-label">🛰️ Total Nodes</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" style="color: #00ff88;">{int(r['ACTIVE_NODES']) if pd.notna(r['ACTIVE_NODES']) else 0}</div>
            <div class="stat-label">🟢 Active (10m)</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{int(r['GPS_NODES']) if pd.notna(r['GPS_NODES']) else 0}</div>
            <div class="stat-label">📍 With GPS</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" style="color: #00d4ff;">{int(r['TOTAL_PACKETS']) if pd.notna(r['TOTAL_PACKETS']) else 0}</div>
            <div class="stat-label">📦 Packets (24h)</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{int(r['AVG_BATTERY']) if pd.notna(r['AVG_BATTERY']) else 'N/A'}%</div>
            <div class="stat-label">🔋 Avg Battery</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{r['AVG_SNR'] if pd.notna(r['AVG_SNR']) else 'N/A'}</div>
            <div class="stat-label">📶 Avg SNR (dB)</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Map & Nodes", "📊 Live Packets", "💬 Messages", "📈 Analytics"])

with tab1:
    col_map, col_nodes = st.columns([3, 2])
    
    with col_map:
        st.markdown("### 📍 Node Locations")
        
        nodes_gps = run_query("""
        SELECT node_id, latitude, longitude, battery_level, avg_snr, mins_ago
        FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
        WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL
            AND latitude BETWEEN -90 AND 90
            AND longitude BETWEEN -180 AND 180
        ORDER BY last_seen DESC
        """)
        
        if not nodes_gps.empty:
            map_df = nodes_gps[['LATITUDE', 'LONGITUDE']].copy()
            map_df.columns = ['lat', 'lon']
            st.map(map_df, zoom=8)
            st.caption(f"📍 {len(nodes_gps)} nodes with GPS • Updated every 60 seconds")
        else:
            st.info("No nodes with GPS coordinates yet")
    
    with col_nodes:
        st.markdown("### 📡 All Nodes")
        
        nodes = run_query("""
        SELECT 
            node_id, packet_count, battery_level, avg_snr, 
            latitude, longitude, mins_ago, last_seen,
            CASE 
                WHEN mins_ago <= 5 THEN 'active'
                WHEN mins_ago <= 30 THEN 'recent'
                WHEN mins_ago <= 60 THEN 'stale'
                ELSE 'offline'
            END as status
        FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
        ORDER BY last_seen DESC
        """)
        
        if not nodes.empty:
            for _, node in nodes.iterrows():
                status = node['STATUS']
                bat = f"{int(node['BATTERY_LEVEL'])}%" if pd.notna(node['BATTERY_LEVEL']) else "N/A"
                snr = f"{node['AVG_SNR']} dB" if pd.notna(node['AVG_SNR']) else "N/A"
                pkts = int(node['PACKET_COUNT'])
                mins = int(node['MINS_AGO']) if pd.notna(node['MINS_AGO']) else "?"
                has_gps = "📍" if pd.notna(node['LATITUDE']) else "❌"
                
                status_label = {'active': 'Active', 'recent': 'Recent', 'stale': 'Stale', 'offline': 'Offline'}[status]
                
                st.markdown(f"""
                <div class="node-card {status}">
                    <div class="node-header">
                        <span class="node-id">{node['NODE_ID']}</span>
                        <span class="node-status status-{status}">{status_label}</span>
                    </div>
                    <div class="node-stats">
                        <div class="node-stat">
                            <span class="node-stat-icon">🔋</span>
                            <span class="node-stat-value">{bat}</span>
                        </div>
                        <div class="node-stat">
                            <span class="node-stat-icon">📶</span>
                            <span class="node-stat-value">{snr}</span>
                        </div>
                        <div class="node-stat">
                            <span class="node-stat-icon">📦</span>
                            <span class="node-stat-value">{pkts}</span>
                        </div>
                        <div class="node-stat">
                            <span class="node-stat-icon">⏱️</span>
                            <span class="node-stat-value">{mins}m ago</span>
                        </div>
                        <div class="node-stat">
                            <span class="node-stat-icon">{has_gps}</span>
                            <span class="node-stat-value">GPS</span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

with tab2:
    st.markdown("### 📊 Live Packet Stream")
    st.caption("Data from MESHTASTIC_LIVE_AUTO • Refreshes every 60 seconds")
    
    packets = run_query("""
    SELECT 
        ingested_at,
        from_id,
        packet_type,
        ROUND(latitude, 4) as lat,
        ROUND(longitude, 4) as lon,
        battery_level,
        ROUND(rx_snr, 1) as snr,
        rx_rssi,
        ROUND(temperature, 1) as temp,
        text_message
    FROM DEMO.DEMO.MESHTASTIC_LIVE_AUTO
    ORDER BY ingested_at DESC
    LIMIT 50
    """)
    
    if not packets.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Packet Types")
            type_counts = packets['PACKET_TYPE'].value_counts()
            st.bar_chart(type_counts)
        
        with col2:
            st.markdown("#### By Node")
            node_counts = packets['FROM_ID'].value_counts().head(8)
            st.bar_chart(node_counts)
        
        st.markdown("#### Recent Packets")
        
        for _, pkt in packets.head(20).iterrows():
            time_str = pkt['INGESTED_AT'].strftime('%H:%M:%S') if pd.notna(pkt['INGESTED_AT']) else ''
            ptype = pkt['PACKET_TYPE'] or 'unknown'
            bat = f"🔋{int(pkt['BATTERY_LEVEL'])}%" if pd.notna(pkt['BATTERY_LEVEL']) else ""
            snr = f"📶{pkt['SNR']}dB" if pd.notna(pkt['SNR']) else ""
            temp = f"🌡️{pkt['TEMP']}°C" if pd.notna(pkt['TEMP']) else ""
            msg = f"💬 {pkt['TEXT_MESSAGE'][:50]}..." if pd.notna(pkt['TEXT_MESSAGE']) else ""
            
            details = " • ".join(filter(None, [bat, snr, temp, msg]))
            
            st.markdown(f"""
            <div class="packet-row">
                <span style="color: #666; font-family: monospace;">{time_str}</span>
                <span class="packet-type">{ptype}</span>
                <span style="color: #00ff88; font-family: monospace;">{pkt['FROM_ID']}</span>
                <span style="color: #aaa;">{details}</span>
            </div>
            """, unsafe_allow_html=True)

with tab3:
    st.markdown("### 💬 Text Messages")
    
    messages = run_query("""
    SELECT 
        ingested_at,
        from_id,
        text_message,
        rx_snr
    FROM DEMO.DEMO.MESHTASTIC_LIVE_AUTO
    WHERE packet_type = 'text'
        AND text_message IS NOT NULL
        AND text_message != ''
    ORDER BY ingested_at DESC
    LIMIT 30
    """)
    
    if not messages.empty:
        for _, msg in messages.iterrows():
            time_str = msg['INGESTED_AT'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(msg['INGESTED_AT']) else ''
            snr = f"SNR: {msg['RX_SNR']:.1f} dB" if pd.notna(msg['RX_SNR']) else ""
            
            st.markdown(f"""
            <div class="message-card">
                <div class="message-header">
                    <span class="message-sender">{msg['FROM_ID']}</span>
                    <span class="message-time">{time_str} • {snr}</span>
                </div>
                <div class="message-text">{msg['TEXT_MESSAGE']}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No text messages in the last 24 hours")

with tab4:
    st.markdown("### 📈 Network Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Packets by Node")
        node_packets = run_query("""
        SELECT node_id, packet_count
        FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
        ORDER BY packet_count DESC
        LIMIT 10
        """)
        if not node_packets.empty:
            st.bar_chart(node_packets.set_index('NODE_ID')['PACKET_COUNT'])
    
    with col2:
        st.markdown("#### Signal Quality (SNR)")
        node_snr = run_query("""
        SELECT node_id, avg_snr
        FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
        WHERE avg_snr IS NOT NULL
        ORDER BY avg_snr DESC
        """)
        if not node_snr.empty:
            st.bar_chart(node_snr.set_index('NODE_ID')['AVG_SNR'])
    
    st.markdown("#### Hourly Activity")
    hourly = run_query("""
    SELECT 
        DATE_TRUNC('hour', ingested_at) as hour,
        COUNT(*) as packets,
        COUNT(DISTINCT from_id) as nodes
    FROM DEMO.DEMO.MESHTASTIC_LIVE_AUTO
    GROUP BY DATE_TRUNC('hour', ingested_at)
    ORDER BY hour
    """)
    if not hourly.empty:
        st.line_chart(hourly.set_index('HOUR')[['PACKETS', 'NODES']])

st.markdown(f"""
<div class="refresh-bar">
    <div>
        <span class="tech-badge">Interactive Tables</span>
        <span class="tech-badge" style="margin-left: 10px;">60s Refresh</span>
        <span class="tech-badge" style="margin-left: 10px;">Snowflake ❄️</span>
    </div>
    <div style="color: #666;">
        Last refresh: {datetime.now().strftime('%H:%M:%S')}
    </div>
</div>
""", unsafe_allow_html=True)

col1, col2, col3 = st.columns([1, 1, 1])
with col2:
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()

time.sleep(REFRESH_INTERVAL)
st.rerun()
