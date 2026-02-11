#!/usr/bin/env python3
"""
Meshtastic + Environmental Live Dashboard
Sub-second queries with auto-refresh every 5 seconds
** Streamlit in Snowflake (SiS) Version **
"""
import streamlit as st
import pandas as pd
from datetime import datetime
import time

from snowflake.snowpark.context import get_active_session

REFRESH_INTERVAL = 5

st.set_page_config(
    page_title="Environmental Live",
    page_icon="🌍",
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

.hero-subtitle { color: #888; margin-top: 5px; }

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
    width: 10px; height: 10px;
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
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 12px;
    margin: 15px 0;
}

.stat-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #252550 100%);
    padding: 15px;
    border-radius: 12px;
    text-align: center;
    border: 1px solid rgba(255, 255, 255, 0.1);
}

.stat-value {
    font-size: 1.6rem;
    font-weight: 700;
    color: #00ff88;
    font-family: 'JetBrains Mono', monospace;
}

.stat-label {
    color: #888;
    font-size: 0.75rem;
    margin-top: 3px;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.node-card {
    background: linear-gradient(145deg, #1a1a2e 0%, #12122a 100%);
    border-radius: 12px;
    padding: 15px;
    margin: 8px 0;
    border-left: 4px solid #00ff88;
}

.node-card.active { border-left-color: #00ff88; }
.node-card.recent { border-left-color: #ffaa00; }
.node-card.stale { border-left-color: #ff6600; }
.node-card.offline { border-left-color: #ff3333; }

.aqi-good { border-left-color: #00e400; }
.aqi-moderate { border-left-color: #ffff00; }
.aqi-sensitive { border-left-color: #ff7e00; }
.aqi-unhealthy { border-left-color: #ff0000; }
.aqi-very-unhealthy { border-left-color: #8f3f97; }
.aqi-hazardous { border-left-color: #7e0023; }

.weather-card {
    background: linear-gradient(145deg, #1a2a3e 0%, #0f1f2a 100%);
    border-radius: 12px;
    padding: 15px;
    margin: 8px 0;
    border-left: 4px solid #00d4ff;
}

.node-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
}

.node-id {
    font-family: 'JetBrains Mono', monospace;
    font-weight: 700;
    color: #fff;
    font-size: 1rem;
}

.node-status {
    padding: 3px 8px;
    border-radius: 10px;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
}

.status-active { background: rgba(0, 255, 136, 0.2); color: #00ff88; }
.status-recent { background: rgba(255, 170, 0, 0.2); color: #ffaa00; }
.status-good { background: rgba(0, 228, 0, 0.2); color: #00e400; }
.status-moderate { background: rgba(255, 255, 0, 0.2); color: #cccc00; }

.node-stats {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 6px;
    font-size: 0.85rem;
}

.node-stat { display: flex; align-items: center; gap: 5px; color: #aaa; }
.node-stat-value { color: #ddd; font-family: 'JetBrains Mono', monospace; }

.message-card {
    background: #1a1a2e;
    border-radius: 12px;
    padding: 12px 15px;
    margin: 8px 0;
    border-left: 3px solid #00d4ff;
}

.refresh-bar {
    background: linear-gradient(90deg, #0a0a1a, #1a1a3e);
    padding: 12px 20px;
    border-radius: 12px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 15px;
    border: 1px solid rgba(255, 255, 255, 0.1);
}

.tech-badge {
    background: rgba(0, 212, 255, 0.1);
    border: 1px solid rgba(0, 212, 255, 0.3);
    padding: 4px 10px;
    border-radius: 6px;
    font-size: 0.75rem;
    color: #00d4ff;
}
</style>
""", unsafe_allow_html=True)

col1, col2 = st.columns([3, 1])
with col1:
    st.markdown("""
    <div class="hero">
        <h1 class="hero-title">🌍 Environmental & Mesh Network Dashboard</h1>
        <p class="hero-subtitle">Meshtastic • Air Quality • Weather • Real-time monitoring</p>
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

mesh_summary = run_query("""
SELECT 
    COUNT(DISTINCT node_id) as total_nodes,
    SUM(packet_count) as total_packets,
    ROUND(AVG(battery_level), 0) as avg_battery,
    ROUND(AVG(avg_snr), 1) as avg_snr,
    COUNT(DISTINCT CASE WHEN mins_ago <= 10 THEN node_id END) as active_nodes
FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
""")

aq_summary = run_query("""
SELECT 
    COUNT(*) as readings,
    COUNT(DISTINCT REPORTINGAREA) as areas,
    ROUND(AVG(AQI), 0) as avg_aqi,
    MAX(AQI) as max_aqi,
    MODE(CATEGORYNAME) as common_category
FROM DEMO.DEMO.AQ
WHERE DATEOBSERVED >= DATEADD(day, -7, CURRENT_DATE())
""")

weather_summary = run_query("""
SELECT 
    COUNT(*) as stations,
    ROUND(AVG(TEMP_F), 1) as avg_temp,
    ROUND(AVG(RELATIVE_HUMIDITY), 0) as avg_humidity,
    ROUND(AVG(WIND_MPH), 1) as avg_wind
FROM DEMO.DEMO.WEATHER_OBSERVATIONS
WHERE CREATED_AT >= DATEADD(day, -1, CURRENT_TIMESTAMP())
""")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("#### 📡 Mesh Network")
    if not mesh_summary.empty:
        r = mesh_summary.iloc[0]
        st.markdown(f"""
        <div class="stat-grid">
            <div class="stat-card">
                <div class="stat-value">{int(r['TOTAL_NODES']) if pd.notna(r['TOTAL_NODES']) else 0}</div>
                <div class="stat-label">Nodes</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: #00ff88;">{int(r['ACTIVE_NODES']) if pd.notna(r['ACTIVE_NODES']) else 0}</div>
                <div class="stat-label">Active</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{int(r['TOTAL_PACKETS']) if pd.notna(r['TOTAL_PACKETS']) else 0}</div>
                <div class="stat-label">Packets</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{int(r['AVG_BATTERY']) if pd.notna(r['AVG_BATTERY']) else 'N/A'}%</div>
                <div class="stat-label">Avg Battery</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

with col2:
    st.markdown("#### 🌬️ Air Quality")
    if not aq_summary.empty:
        r = aq_summary.iloc[0]
        avg_aqi = int(r['AVG_AQI']) if pd.notna(r['AVG_AQI']) else 0
        aqi_color = "#00e400" if avg_aqi <= 50 else "#ffff00" if avg_aqi <= 100 else "#ff7e00" if avg_aqi <= 150 else "#ff0000"
        st.markdown(f"""
        <div class="stat-grid">
            <div class="stat-card">
                <div class="stat-value" style="color: {aqi_color};">{avg_aqi}</div>
                <div class="stat-label">Avg AQI</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{int(r['MAX_AQI']) if pd.notna(r['MAX_AQI']) else 0}</div>
                <div class="stat-label">Max AQI</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{int(r['AREAS']) if pd.notna(r['AREAS']) else 0}</div>
                <div class="stat-label">Areas</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="font-size: 0.9rem;">{r['COMMON_CATEGORY'] if pd.notna(r['COMMON_CATEGORY']) else 'N/A'}</div>
                <div class="stat-label">Status</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

with col3:
    st.markdown("#### 🌡️ Weather")
    if not weather_summary.empty:
        r = weather_summary.iloc[0]
        st.markdown(f"""
        <div class="stat-grid">
            <div class="stat-card">
                <div class="stat-value" style="color: #00d4ff;">{r['AVG_TEMP'] if pd.notna(r['AVG_TEMP']) else 'N/A'}°F</div>
                <div class="stat-label">Avg Temp</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{int(r['AVG_HUMIDITY']) if pd.notna(r['AVG_HUMIDITY']) else 'N/A'}%</div>
                <div class="stat-label">Humidity</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{r['AVG_WIND'] if pd.notna(r['AVG_WIND']) else 'N/A'}</div>
                <div class="stat-label">Wind MPH</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{int(r['STATIONS']) if pd.notna(r['STATIONS']) else 0}</div>
                <div class="stat-label">Stations</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5 = st.tabs(["🗺️ All Locations", "📡 Mesh Network", "🌬️ Air Quality", "🌡️ Weather", "📈 Analytics"])

with tab1:
    st.markdown("### 🗺️ Combined Environmental Map")
    
    col_map, col_legend = st.columns([4, 1])
    
    with col_map:
        all_locations = run_query("""
        SELECT latitude as lat, longitude as lon, 'mesh' as type FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY 
        WHERE latitude IS NOT NULL AND latitude BETWEEN -90 AND 90
        UNION ALL
        SELECT LATITUDE as lat, LONGITUDE as lon, 'aq' as type FROM DEMO.DEMO.AQ 
        WHERE LATITUDE IS NOT NULL AND DATEOBSERVED >= DATEADD(day, -7, CURRENT_DATE())
        UNION ALL
        SELECT LATITUDE as lat, LONGITUDE as lon, 'weather' as type FROM DEMO.DEMO.WEATHER_OBSERVATIONS 
        WHERE LATITUDE IS NOT NULL
        """)
        
        if not all_locations.empty:
            map_df = all_locations[['LAT', 'LON']].copy()
            map_df.columns = ['lat', 'lon']
            st.map(map_df, zoom=3)
            st.caption(f"📍 {len(all_locations)} total locations • Mesh nodes, AQ sensors, Weather stations")
    
    with col_legend:
        st.markdown("""
        **Legend:**
        - 📡 Mesh Nodes
        - 🌬️ Air Quality
        - 🌡️ Weather
        """)

with tab2:
    st.markdown("### 📡 Meshtastic Mesh Network")
    
    col_map, col_nodes = st.columns([3, 2])
    
    with col_map:
        nodes_gps = run_query("""
        SELECT node_id, latitude, longitude, battery_level, avg_snr, mins_ago
        FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
        WHERE latitude IS NOT NULL AND latitude BETWEEN -90 AND 90
        ORDER BY last_seen DESC
        """)
        
        if not nodes_gps.empty:
            map_df = nodes_gps[['LATITUDE', 'LONGITUDE']].copy()
            map_df.columns = ['lat', 'lon']
            st.map(map_df, zoom=8)
    
    with col_nodes:
        nodes = run_query("""
        SELECT node_id, packet_count, battery_level, avg_snr, mins_ago,
            CASE WHEN mins_ago <= 5 THEN 'active' WHEN mins_ago <= 30 THEN 'recent' ELSE 'stale' END as status
        FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
        ORDER BY last_seen DESC LIMIT 10
        """)
        
        if not nodes.empty:
            for _, node in nodes.iterrows():
                status = node['STATUS']
                bat = f"{int(node['BATTERY_LEVEL'])}%" if pd.notna(node['BATTERY_LEVEL']) else "N/A"
                snr = f"{node['AVG_SNR']} dB" if pd.notna(node['AVG_SNR']) else "N/A"
                
                st.markdown(f"""
                <div class="node-card {status}">
                    <div class="node-header">
                        <span class="node-id">{node['NODE_ID']}</span>
                        <span class="node-status status-{status}">{status}</span>
                    </div>
                    <div class="node-stats">
                        <div class="node-stat">🔋 <span class="node-stat-value">{bat}</span></div>
                        <div class="node-stat">📶 <span class="node-stat-value">{snr}</span></div>
                        <div class="node-stat">📦 <span class="node-stat-value">{int(node['PACKET_COUNT'])}</span></div>
                        <div class="node-stat">⏱️ <span class="node-stat-value">{int(node['MINS_AGO']) if pd.notna(node['MINS_AGO']) else '?'}m</span></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

with tab3:
    st.markdown("### 🌬️ Air Quality Index")
    
    col_map, col_readings = st.columns([3, 2])
    
    with col_map:
        aq_locations = run_query("""
        SELECT LATITUDE, LONGITUDE, AQI, REPORTINGAREA, CATEGORYNAME
        FROM DEMO.DEMO.AQ
        WHERE LATITUDE IS NOT NULL AND DATEOBSERVED >= DATEADD(day, -7, CURRENT_DATE())
        """)
        
        if not aq_locations.empty:
            map_df = aq_locations[['LATITUDE', 'LONGITUDE']].copy()
            map_df.columns = ['lat', 'lon']
            st.map(map_df, zoom=3)
            
            st.markdown("#### AQI by Area")
            area_aqi = run_query("""
            SELECT REPORTINGAREA, ROUND(AVG(AQI), 0) as AVG_AQI, MAX(AQI) as MAX_AQI
            FROM DEMO.DEMO.AQ
            WHERE DATEOBSERVED >= DATEADD(day, -7, CURRENT_DATE())
            GROUP BY REPORTINGAREA
            ORDER BY AVG_AQI DESC
            LIMIT 10
            """)
            if not area_aqi.empty:
                st.bar_chart(area_aqi.set_index('REPORTINGAREA')['AVG_AQI'])
    
    with col_readings:
        st.markdown("#### Recent Readings")
        aq_readings = run_query("""
        SELECT REPORTINGAREA, PARAMETERNAME, AQI, CATEGORYNAME, DATEOBSERVED
        FROM DEMO.DEMO.AQ
        WHERE DATEOBSERVED >= DATEADD(day, -3, CURRENT_DATE())
        ORDER BY DATEOBSERVED DESC, AQI DESC
        LIMIT 15
        """)
        
        if not aq_readings.empty:
            for _, r in aq_readings.iterrows():
                aqi = int(r['AQI']) if pd.notna(r['AQI']) else 0
                aqi_class = "aqi-good" if aqi <= 50 else "aqi-moderate" if aqi <= 100 else "aqi-sensitive" if aqi <= 150 else "aqi-unhealthy"
                status_class = "status-good" if aqi <= 50 else "status-moderate"
                
                st.markdown(f"""
                <div class="node-card {aqi_class}">
                    <div class="node-header">
                        <span class="node-id">{r['REPORTINGAREA']}</span>
                        <span class="node-status {status_class}">{r['CATEGORYNAME']}</span>
                    </div>
                    <div class="node-stats">
                        <div class="node-stat">📊 <span class="node-stat-value">AQI {aqi}</span></div>
                        <div class="node-stat">🧪 <span class="node-stat-value">{r['PARAMETERNAME']}</span></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

with tab4:
    st.markdown("### 🌡️ Weather Observations")
    
    col_map, col_stations = st.columns([3, 2])
    
    with col_map:
        weather_locations = run_query("""
        SELECT LATITUDE, LONGITUDE, LOCATION, TEMP_F, WEATHER
        FROM DEMO.DEMO.WEATHER_OBSERVATIONS
        WHERE LATITUDE IS NOT NULL
        """)
        
        if not weather_locations.empty:
            map_df = weather_locations[['LATITUDE', 'LONGITUDE']].copy()
            map_df.columns = ['lat', 'lon']
            st.map(map_df, zoom=3)
            
            st.markdown("#### Temperature Distribution")
            temp_dist = run_query("""
            SELECT 
                CASE 
                    WHEN TEMP_F < 32 THEN 'Freezing (<32°F)'
                    WHEN TEMP_F < 50 THEN 'Cold (32-50°F)'
                    WHEN TEMP_F < 70 THEN 'Mild (50-70°F)'
                    WHEN TEMP_F < 85 THEN 'Warm (70-85°F)'
                    ELSE 'Hot (>85°F)'
                END as temp_range,
                COUNT(*) as stations
            FROM DEMO.DEMO.WEATHER_OBSERVATIONS
            WHERE TEMP_F IS NOT NULL
            GROUP BY temp_range
            ORDER BY MIN(TEMP_F)
            """)
            if not temp_dist.empty:
                st.bar_chart(temp_dist.set_index('TEMP_RANGE')['STATIONS'])
    
    with col_stations:
        st.markdown("#### Weather Stations")
        stations = run_query("""
        SELECT LOCATION, TEMP_F, RELATIVE_HUMIDITY, WIND_MPH, WIND_DIR, WEATHER
        FROM DEMO.DEMO.WEATHER_OBSERVATIONS
        WHERE TEMP_F IS NOT NULL
        ORDER BY CREATED_AT DESC
        LIMIT 12
        """)
        
        if not stations.empty:
            for _, s in stations.iterrows():
                temp = int(s['TEMP_F']) if pd.notna(s['TEMP_F']) else 'N/A'
                humidity = int(s['RELATIVE_HUMIDITY']) if pd.notna(s['RELATIVE_HUMIDITY']) else 'N/A'
                wind = f"{s['WIND_MPH']} mph {s['WIND_DIR']}" if pd.notna(s['WIND_MPH']) else 'Calm'
                weather = s['WEATHER'] if pd.notna(s['WEATHER']) else ''
                
                st.markdown(f"""
                <div class="weather-card">
                    <div class="node-header">
                        <span class="node-id">{s['LOCATION'][:25]}</span>
                        <span class="node-status status-active">{weather}</span>
                    </div>
                    <div class="node-stats">
                        <div class="node-stat">🌡️ <span class="node-stat-value">{temp}°F</span></div>
                        <div class="node-stat">💧 <span class="node-stat-value">{humidity}%</span></div>
                        <div class="node-stat">💨 <span class="node-stat-value">{wind}</span></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

with tab5:
    st.markdown("### 📈 Cross-Domain Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Mesh Network Activity")
        node_packets = run_query("""
        SELECT node_id, packet_count
        FROM DEMO.DEMO.MESHTASTIC_NODE_SUMMARY
        ORDER BY packet_count DESC LIMIT 10
        """)
        if not node_packets.empty:
            st.bar_chart(node_packets.set_index('NODE_ID')['PACKET_COUNT'])
        
        st.markdown("#### AQI by Parameter Type")
        param_aqi = run_query("""
        SELECT PARAMETERNAME, ROUND(AVG(AQI), 0) as AVG_AQI
        FROM DEMO.DEMO.AQ
        WHERE DATEOBSERVED >= DATEADD(day, -7, CURRENT_DATE())
        GROUP BY PARAMETERNAME
        ORDER BY AVG_AQI DESC
        """)
        if not param_aqi.empty:
            st.bar_chart(param_aqi.set_index('PARAMETERNAME')['AVG_AQI'])
    
    with col2:
        st.markdown("#### Weather by State")
        state_weather = run_query("""
        SELECT SUBSTR(LOCATION, -2) as STATE, ROUND(AVG(TEMP_F), 1) as AVG_TEMP, COUNT(*) as STATIONS
        FROM DEMO.DEMO.WEATHER_OBSERVATIONS
        WHERE TEMP_F IS NOT NULL
        GROUP BY SUBSTR(LOCATION, -2)
        HAVING COUNT(*) >= 5
        ORDER BY AVG_TEMP DESC
        LIMIT 15
        """)
        if not state_weather.empty:
            st.bar_chart(state_weather.set_index('STATE')['AVG_TEMP'])
        
        st.markdown("#### Data Sources Summary")
        summary_data = pd.DataFrame({
            'Source': ['Mesh Nodes', 'AQ Readings', 'Weather Stations'],
            'Records': [
                int(mesh_summary.iloc[0]['TOTAL_NODES']) if not mesh_summary.empty else 0,
                int(aq_summary.iloc[0]['READINGS']) if not aq_summary.empty else 0,
                int(weather_summary.iloc[0]['STATIONS']) if not weather_summary.empty else 0
            ]
        })
        st.dataframe(summary_data)

st.markdown(f"""
<div class="refresh-bar">
    <div>
        <span class="tech-badge">Interactive Tables</span>
        <span class="tech-badge" style="margin-left: 8px;">Air Quality</span>
        <span class="tech-badge" style="margin-left: 8px;">Weather</span>
        <span class="tech-badge" style="margin-left: 8px;">5s Refresh</span>
    </div>
    <div style="color: #666;">
        Last refresh: {datetime.now().strftime('%H:%M:%S')}
    </div>
</div>
""", unsafe_allow_html=True)

col1, col2, col3 = st.columns([1, 1, 1])
with col2:
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.experimental_rerun()

time.sleep(REFRESH_INTERVAL)
st.experimental_rerun()
