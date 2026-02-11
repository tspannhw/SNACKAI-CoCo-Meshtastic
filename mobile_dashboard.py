#!/usr/bin/env python3
"""
Mobile-Optimized Instant Dashboard for Meshtastic Mesh Network
Designed for iOS/Android browser access with responsive layout
"""
import streamlit as st
import pandas as pd
import os
import toml
from datetime import datetime
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

st.set_page_config(
    page_title="Meshtastic Mobile",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.stApp {
    background: linear-gradient(180deg, #0a0a1a 0%, #1a1a2e 100%);
}

.mobile-card {
    background: linear-gradient(135deg, #1e1e3f 0%, #2d2d5a 100%);
    border-radius: 16px;
    padding: 16px;
    margin: 8px 0;
    border: 1px solid #3d3d7a;
    box-shadow: 0 4px 15px rgba(0,0,0,0.3);
}

.metric-big {
    font-size: 2.5rem;
    font-weight: 700;
    color: #00ff88;
    text-align: center;
    line-height: 1.2;
}

.metric-label {
    font-size: 0.85rem;
    color: #8888aa;
    text-align: center;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.status-active { color: #00ff88; }
.status-recent { color: #ffcc00; }
.status-stale { color: #ff8800; }
.status-offline { color: #ff4444; }

.node-card {
    background: #1a1a2e;
    border-radius: 12px;
    padding: 12px;
    margin: 8px 0;
    border-left: 4px solid #00ff88;
}

.node-card.warning {
    border-left-color: #ffcc00;
}

.node-card.critical {
    border-left-color: #ff4444;
}

.message-bubble {
    background: #2a2a4a;
    border-radius: 16px 16px 16px 4px;
    padding: 12px 16px;
    margin: 8px 0;
    max-width: 90%;
}

.pull-to-refresh {
    text-align: center;
    padding: 8px;
    color: #666;
    font-size: 0.8rem;
}

div[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1e1e3f 0%, #2d2d5a 100%);
    border-radius: 12px;
    padding: 12px;
    border: 1px solid #3d3d7a;
}

div[data-testid="stMetric"] label {
    color: #8888aa !important;
}

div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
    color: #00ff88 !important;
    font-size: 1.8rem !important;
}

.stButton>button {
    width: 100%;
    background: linear-gradient(135deg, #4a4a8a 0%, #6a6aba 100%);
    color: white;
    border: none;
    border-radius: 12px;
    padding: 16px;
    font-size: 1.1rem;
    font-weight: 600;
    margin: 4px 0;
}

.stButton>button:hover {
    background: linear-gradient(135deg, #5a5a9a 0%, #7a7aca 100%);
}

div[data-testid="stDataFrame"] {
    border-radius: 12px;
    overflow: hidden;
}

.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: #1a1a2e;
    border-radius: 12px;
    padding: 4px;
}

.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    padding: 8px 16px;
    background: transparent;
    color: #8888aa;
}

.stTabs [aria-selected="true"] {
    background: #3d3d7a;
    color: #00ff88 !important;
}

@media (max-width: 768px) {
    .metric-big { font-size: 2rem; }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-size: 1.5rem !important;
    }
}
</style>
""", unsafe_allow_html=True)

SNOWFLAKE_CONN = os.getenv("SNOWFLAKE_CONNECTION_NAME", "tspann1")

@st.cache_resource(ttl=300)
def get_connection():
    config_path = os.path.expanduser("~/.snowflake/connections.toml")
    with open(config_path, "r") as f:
        config = toml.load(f)
    
    conn_config = config.get(SNOWFLAKE_CONN, {})
    
    private_key_path = conn_config.get("private_key_path")
    if private_key_path:
        private_key_path = os.path.expanduser(private_key_path)
        with open(private_key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return snowflake.connector.connect(
            account=conn_config.get("account"),
            user=conn_config.get("user"),
            private_key=pkb,
            role=conn_config.get("role", "ACCOUNTADMIN"),
            warehouse=conn_config.get("warehouse", "INGEST")
        )
    else:
        return snowflake.connector.connect(connection_name=SNOWFLAKE_CONN)

def run_query(sql):
    try:
        conn = get_connection()
        if conn.is_closed():
            st.cache_resource.clear()
            conn = get_connection()
        return pd.read_sql(sql, conn)
    except Exception as e:
        st.cache_resource.clear()
        try:
            conn = get_connection()
            return pd.read_sql(sql, conn)
        except Exception as e2:
            st.error(f"Connection error: {e2}")
            return pd.DataFrame()

st.markdown("# 📱 Mesh Network")
st.markdown(f"*Last updated: {datetime.now().strftime('%H:%M:%S')}*")

col_refresh, col_time = st.columns([1, 2])
with col_refresh:
    if st.button("🔄 Refresh"):
        st.cache_resource.clear()
        st.rerun()
with col_time:
    time_range = st.selectbox(
        "⏱️",
        ["1h", "6h", "24h", "7d"],
        index=2,
        label_visibility="collapsed"
    )

time_map = {
    "1h": "DATEADD(hour, -1, CURRENT_TIMESTAMP())",
    "6h": "DATEADD(hour, -6, CURRENT_TIMESTAMP())",
    "24h": "DATEADD(hour, -24, CURRENT_TIMESTAMP())",
    "7d": "DATEADD(day, -7, CURRENT_TIMESTAMP())"
}
time_filter = time_map[time_range]

summary_query = f"""
SELECT 
    COUNT(DISTINCT from_id) as total_nodes,
    COUNT(*) as total_packets,
    ROUND(AVG(battery_level), 0) as avg_battery,
    ROUND(AVG(rx_snr), 1) as avg_snr,
    COUNT(DISTINCT CASE WHEN DATEDIFF(minute, ingested_at, CURRENT_TIMESTAMP()) <= 10 THEN from_id END) as active_nodes,
    SUM(CASE WHEN packet_type = 'text' AND text_message IS NOT NULL THEN 1 ELSE 0 END) as text_messages
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= {time_filter}
"""

summary_df = run_query(summary_query)

if not summary_df.empty:
    row = summary_df.iloc[0]
    
    col1, col2 = st.columns(2)
    with col1:
        active = row['ACTIVE_NODES'] if pd.notna(row['ACTIVE_NODES']) else 0
        st.metric("🟢 Active Nodes", f"{int(active)}")
    with col2:
        total = row['TOTAL_NODES'] if pd.notna(row['TOTAL_NODES']) else 0
        st.metric("🛰️ Total Nodes", f"{int(total)}")
    
    col3, col4 = st.columns(2)
    with col3:
        bat = row['AVG_BATTERY'] if pd.notna(row['AVG_BATTERY']) else 0
        st.metric("🔋 Avg Battery", f"{int(bat)}%")
    with col4:
        snr = row['AVG_SNR'] if pd.notna(row['AVG_SNR']) else 0
        st.metric("📶 Avg SNR", f"{snr:.1f} dB")
    
    col5, col6 = st.columns(2)
    with col5:
        pkts = row['TOTAL_PACKETS'] if pd.notna(row['TOTAL_PACKETS']) else 0
        st.metric("📦 Packets", f"{int(pkts):,}")
    with col6:
        msgs = row['TEXT_MESSAGES'] if pd.notna(row['TEXT_MESSAGES']) else 0
        st.metric("💬 Messages", f"{int(msgs)}")

st.markdown("---")

tab_nodes, tab_data, tab_messages, tab_map = st.tabs(["📊 Nodes", "📋 Data", "💬 Messages", "🗺️ Map"])

with tab_nodes:
    st.markdown("### Node Status")
    
    nodes_query = f"""
    SELECT 
        from_id as node_id,
        COUNT(*) as packets,
        MAX(battery_level) as battery,
        ROUND(AVG(rx_snr), 1) as snr,
        MAX(ingested_at) as last_seen,
        DATEDIFF(minute, MAX(ingested_at), CURRENT_TIMESTAMP()) as mins_ago,
        MAX(latitude) as lat,
        MAX(longitude) as lon,
        MAX(temperature) as temp
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= {time_filter}
    GROUP BY from_id
    ORDER BY last_seen DESC
    """
    
    nodes_df = run_query(nodes_query)
    
    if not nodes_df.empty:
        for _, node in nodes_df.iterrows():
            mins = node['MINS_AGO'] if pd.notna(node['MINS_AGO']) else 999
            bat = node['BATTERY'] if pd.notna(node['BATTERY']) else 0
            
            if mins <= 5:
                status_class = "status-active"
                status_icon = "🟢"
                card_class = ""
            elif mins <= 30:
                status_class = "status-recent"
                status_icon = "🟡"
                card_class = "warning"
            elif mins <= 60:
                status_class = "status-stale"
                status_icon = "🟠"
                card_class = "warning"
            else:
                status_class = "status-offline"
                status_icon = "🔴"
                card_class = "critical"
            
            if bat < 20:
                card_class = "critical"
                bat_icon = "🪫"
            elif bat < 50:
                bat_icon = "🔋"
            else:
                bat_icon = "🔋"
            
            snr = node['SNR'] if pd.notna(node['SNR']) else 0
            packets = node['PACKETS'] if pd.notna(node['PACKETS']) else 0
            temp = node['TEMP'] if pd.notna(node['TEMP']) else None
            
            temp_str = f" • 🌡️ {temp:.1f}°C" if temp else ""
            
            st.markdown(f"""
            <div class="node-card {card_class}">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <strong style="font-size: 1.1rem;">{status_icon} {node['NODE_ID']}</strong>
                    </div>
                    <div class="{status_class}" style="font-size: 0.9rem;">
                        {mins}m ago
                    </div>
                </div>
                <div style="margin-top: 8px; color: #aaa; font-size: 0.9rem;">
                    {bat_icon} {int(bat)}% • 📶 {snr:.1f} dB • 📦 {packets}{temp_str}
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No nodes found in selected time range")

with tab_data:
    st.markdown("### Recent Data")
    
    limit = st.selectbox("Show", [25, 50, 100], index=0, key="data_limit")
    
    data_query = f"""
    SELECT 
        ingested_at as "Time",
        from_id as "Node",
        packet_type as "Type",
        battery_level as "Bat%",
        ROUND(rx_snr, 1) as "SNR",
        ROUND(temperature, 1) as "Temp",
        text_message as "Msg"
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= {time_filter}
    ORDER BY ingested_at DESC
    LIMIT {limit}
    """
    
    data_df = run_query(data_query)
    
    if not data_df.empty:
        st.dataframe(
            data_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Time": st.column_config.DatetimeColumn(
                    "Time",
                    format="HH:mm:ss",
                    width="small"
                ),
                "Node": st.column_config.TextColumn(
                    "Node",
                    width="small"
                ),
                "Type": st.column_config.TextColumn(
                    "Type",
                    width="small"
                ),
                "Bat%": st.column_config.ProgressColumn(
                    "Bat%",
                    min_value=0,
                    max_value=100,
                    format="%d"
                ),
                "SNR": st.column_config.NumberColumn(
                    "SNR",
                    format="%.1f"
                ),
                "Temp": st.column_config.NumberColumn(
                    "Temp",
                    format="%.1f°"
                ),
                "Msg": st.column_config.TextColumn(
                    "Msg",
                    width="medium"
                )
            },
            height=400
        )
    else:
        st.info("No data found")

with tab_messages:
    st.markdown("### Text Messages")
    
    msg_query = f"""
    SELECT 
        ingested_at as time,
        from_id as sender,
        text_message as message,
        rx_snr as snr
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= {time_filter}
        AND packet_type = 'text'
        AND text_message IS NOT NULL
    ORDER BY ingested_at DESC
    LIMIT 30
    """
    
    msg_df = run_query(msg_query)
    
    if not msg_df.empty:
        for _, msg in msg_df.iterrows():
            time_str = msg['TIME'].strftime('%H:%M') if pd.notna(msg['TIME']) else ""
            snr = msg['SNR'] if pd.notna(msg['SNR']) else 0
            
            st.markdown(f"""
            <div class="message-bubble">
                <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                    <strong style="color: #00ff88;">{msg['SENDER']}</strong>
                    <span style="color: #666; font-size: 0.8rem;">{time_str} • {snr:.1f}dB</span>
                </div>
                <div style="color: #fff;">{msg['MESSAGE']}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No messages in selected time range")

with tab_map:
    st.markdown("### Node Locations")
    
    map_query = f"""
    SELECT DISTINCT
        from_id,
        MAX(latitude) as lat,
        MAX(longitude) as lon,
        MAX(battery_level) as battery,
        DATEDIFF(minute, MAX(ingested_at), CURRENT_TIMESTAMP()) as mins_ago
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE ingested_at >= {time_filter}
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
    GROUP BY from_id
    """
    
    map_df = run_query(map_query)
    
    if not map_df.empty and len(map_df) > 0:
        map_data = pd.DataFrame({
            'lat': map_df['LAT'],
            'lon': map_df['LON']
        })
        st.map(map_data, zoom=12)
        
        st.markdown("#### Coordinates")
        for _, row in map_df.iterrows():
            mins = row['MINS_AGO'] if pd.notna(row['MINS_AGO']) else 999
            status = "🟢" if mins <= 10 else "🟡" if mins <= 30 else "🟠" if mins <= 60 else "🔴"
            st.markdown(f"{status} **{row['FROM_ID']}**: `{row['LAT']:.5f}, {row['LON']:.5f}`")
    else:
        st.info("No GPS data available")

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; font-size: 0.8rem; padding: 16px;">
    📱 Meshtastic Mobile Dashboard<br/>
    Powered by Snowflake • <a href="https://meshtastic.org" style="color: #4a9eff;">meshtastic.org</a>
</div>
""", unsafe_allow_html=True)
