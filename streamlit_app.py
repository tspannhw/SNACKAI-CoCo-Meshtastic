#!/usr/bin/env python3
"""
Meshtastic Mesh Network Dashboard - Enhanced Edition
=====================================================
Real-time visualization and monitoring of Meshtastic LoRa mesh network data
with interactive maps, search, and Cortex Agent integration.

Features:
- Interactive Folium map with detailed clickable popups
- Location search (address or lat/long)
- Cortex Agent chat interface for natural language queries
- Real-time telemetry monitoring (battery, temperature, humidity)
- GPS data visualization (position, altitude, speed, satellites)
- Signal quality metrics (SNR, RSSI)
- Network analytics and traffic patterns

Data Source: DEMO.DEMO.MESHTASTIC_DATA (Snowpipe Streaming v2)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
import requests
import folium
from folium.plugins import MarkerCluster, Search, Fullscreen, LocateControl
from streamlit_folium import st_folium
import re

st.set_page_config(
    page_title="Meshtastic Mesh Network Dashboard",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

CUSTOM_CSS = """
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        border-radius: 10px;
        padding: 15px;
        color: white;
        margin: 5px 0;
    }
    .status-online { color: #00ff00; }
    .status-offline { color: #ff6b6b; }
    .status-warning { color: #ffa500; }
    .device-card {
        border: 1px solid #333;
        border-radius: 8px;
        padding: 10px;
        margin: 5px 0;
        background: #1a1a2e;
    }
    div[data-testid="stMetricValue"] {
        font-size: 28px;
    }
    .chat-message {
        padding: 10px 15px;
        border-radius: 10px;
        margin: 5px 0;
        max-width: 85%;
    }
    .user-message {
        background: #1e3a5f;
        margin-left: auto;
        text-align: right;
    }
    .agent-message {
        background: #2d5a87;
        margin-right: auto;
    }
    .search-container {
        background: #1a1a2e;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 15px;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


@st.cache_resource
def get_snowflake_connection():
    """Get Snowflake connection - works in Snowsight or locally."""
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        import snowflake.connector
        conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "tspann1")
        conn = snowflake.connector.connect(connection_name=conn_name)
        return conn


def run_query(query: str) -> pd.DataFrame:
    """Execute SQL query and return DataFrame."""
    session = get_snowflake_connection()
    try:
        return session.sql(query).to_pandas()
    except AttributeError:
        cursor = session.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)


def format_timestamp(ts):
    """Format timestamp for display."""
    if pd.isna(ts):
        return "N/A"
    if isinstance(ts, str):
        return ts[:19]
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def celsius_to_fahrenheit(c):
    """Convert Celsius to Fahrenheit."""
    if c is None or pd.isna(c):
        return None
    return (c * 9/5) + 32


def get_battery_status(level):
    """Get battery status color and icon."""
    if level is None or pd.isna(level):
        return "gray", "?"
    if level >= 80:
        return "#00ff00", "full"
    elif level >= 50:
        return "#90EE90", "high"
    elif level >= 20:
        return "#ffa500", "medium"
    else:
        return "#ff6b6b", "low"


def clamp_battery(level):
    """Clamp battery level to valid range [0, 100]."""
    if level is None or pd.isna(level):
        return None
    return max(0, min(100, int(level)))


def send_slack_message(webhook_url: str, message: str, channel: str = None) -> bool:
    """Send a message to Slack via webhook."""
    try:
        payload = {"text": message}
        if channel:
            payload["channel"] = channel
        response = requests.post(webhook_url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        st.error(f"Slack error: {e}")
        return False


def format_slack_alert(device_id: str, alert_type: str, data: dict) -> str:
    """Format alert message for Slack."""
    emoji = {
        "low_battery": "🔋",
        "position_update": "📍",
        "offline": "⚠️",
        "telemetry": "📊"
    }.get(alert_type, "📡")
    
    msg = f"{emoji} *Meshtastic Alert: {alert_type.replace('_', ' ').title()}*\n"
    msg += f"Device: `{device_id}`\n"
    for key, value in data.items():
        msg += f"• {key}: {value}\n"
    return msg


def geocode_address(address: str) -> tuple:
    """Geocode an address to lat/long using Nominatim."""
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": address,
            "format": "json",
            "limit": 1
        }
        headers = {"User-Agent": "MeshtasticDashboard/1.0"}
        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            results = response.json()
            if results:
                return float(results[0]["lat"]), float(results[0]["lon"]), results[0].get("display_name", address)
    except Exception as e:
        st.warning(f"Geocoding error: {e}")
    return None, None, None


def parse_coordinates(text: str) -> tuple:
    """Parse coordinates from text (lat, long or lat long)."""
    patterns = [
        r'(-?\d+\.?\d*)[,\s]+(-?\d+\.?\d*)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text.strip())
        if match:
            lat, lon = float(match.group(1)), float(match.group(2))
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon, f"{lat:.6f}, {lon:.6f}"
    return None, None, None


def query_cortex_agent(question: str) -> str:
    """Query the Meshtastic Cortex Agent."""
    try:
        safe_question = question.replace("'", "''").replace("\\", "\\\\")
        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'DEMO.DEMO.MESHTASTIC_AGENT',
            '{safe_question}'
        ) as response
        """
        result = run_query(query)
        if not result.empty and result['RESPONSE'].iloc[0]:
            return result['RESPONSE'].iloc[0]
        return "I couldn't get a response. Please try rephrasing your question."
    except Exception as e:
        return f"Error querying agent: {str(e)}"


def get_nodes_near_location(lat: float, lon: float, radius_km: float = 10) -> pd.DataFrame:
    """Find nodes near a specific location."""
    query = f"""
    SELECT 
        from_id,
        latitude,
        longitude,
        altitude,
        battery_level,
        temperature,
        rx_snr,
        rx_rssi,
        ingested_at,
        HAVERSINE({lat}, {lon}, latitude, longitude) as distance_km
    FROM DEMO.DEMO.MESHTASTIC_DATA
    WHERE packet_type = 'position'
      AND latitude IS NOT NULL 
      AND longitude IS NOT NULL
      AND latitude != 0
      AND longitude != 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY ingested_at DESC) = 1
    HAVING distance_km <= {radius_km}
    ORDER BY distance_km
    """
    return run_query(query)


def create_folium_map(positions_df: pd.DataFrame, center_lat: float = None, center_lon: float = None, 
                      search_lat: float = None, search_lon: float = None, search_label: str = None) -> folium.Map:
    """Create an interactive Folium map with detailed popups."""
    if center_lat is None and not positions_df.empty:
        center_lat = positions_df['LATITUDE'].mean()
        center_lon = positions_df['LONGITUDE'].mean()
    elif center_lat is None:
        center_lat, center_lon = 40.7128, -74.0060
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=12,
        tiles='OpenStreetMap',
        control_scale=True
    )
    
    Fullscreen(position='topleft').add_to(m)
    LocateControl(auto_start=False).add_to(m)
    
    folium.TileLayer('cartodbdark_matter', name='Dark Mode').add_to(m)
    folium.TileLayer('cartodbpositron', name='Light Mode').add_to(m)
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri',
        name='Satellite'
    ).add_to(m)
    
    if search_lat and search_lon:
        folium.Marker(
            location=[search_lat, search_lon],
            popup=folium.Popup(f"<b>Search Location</b><br>{search_label or 'Selected Location'}", max_width=300),
            icon=folium.Icon(color='red', icon='search', prefix='fa'),
            tooltip="Search Location"
        ).add_to(m)
        
        folium.Circle(
            location=[search_lat, search_lon],
            radius=10000,
            color='red',
            fill=True,
            fill_opacity=0.1,
            popup="10km Search Radius"
        ).add_to(m)
    
    if not positions_df.empty:
        marker_cluster = MarkerCluster(name="Clustered Nodes").add_to(m)
        node_layer = folium.FeatureGroup(name="Individual Nodes")
        track_layer = folium.FeatureGroup(name="Movement Tracks")
        
        for _, row in positions_df.iterrows():
            node_id = row.get('FROM_ID', 'Unknown')
            lat = row.get('LATITUDE')
            lon = row.get('LONGITUDE')
            
            if pd.isna(lat) or pd.isna(lon):
                continue
            
            battery = row.get('BATTERY_LEVEL')
            temp = row.get('TEMPERATURE')
            altitude = row.get('ALTITUDE')
            speed = row.get('GROUND_SPEED')
            sats = row.get('SATS_IN_VIEW')
            snr = row.get('RX_SNR')
            rssi = row.get('RX_RSSI')
            humidity = row.get('RELATIVE_HUMIDITY')
            voltage = row.get('VOLTAGE')
            uptime = row.get('UPTIME_SECONDS')
            last_seen = row.get('INGESTED_AT')
            distance = row.get('DISTANCE_KM')
            
            bat_color, _ = get_battery_status(battery)
            
            popup_html = f"""
            <div style="font-family: Arial, sans-serif; min-width: 280px; max-width: 350px;">
                <h3 style="margin: 0 0 10px 0; color: #1e3a5f; border-bottom: 2px solid #2d5a87; padding-bottom: 5px;">
                    📡 {node_id}
                </h3>
                
                <div style="background: #f5f5f5; padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                    <h4 style="margin: 0 0 5px 0; color: #333;">📍 Location</h4>
                    <table style="width: 100%; font-size: 13px;">
                        <tr><td><b>Latitude:</b></td><td>{lat:.6f}°</td></tr>
                        <tr><td><b>Longitude:</b></td><td>{lon:.6f}°</td></tr>
                        <tr><td><b>Altitude:</b></td><td>{f'{altitude:.1f} m' if pd.notna(altitude) else 'N/A'}</td></tr>
                        <tr><td><b>Speed:</b></td><td>{f'{speed:.1f} m/s' if pd.notna(speed) else 'N/A'}</td></tr>
                        <tr><td><b>Satellites:</b></td><td>{int(sats) if pd.notna(sats) else 'N/A'}</td></tr>
                        {f'<tr><td><b>Distance:</b></td><td>{distance:.2f} km</td></tr>' if pd.notna(distance) else ''}
                    </table>
                </div>
                
                <div style="background: #e8f4e8; padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                    <h4 style="margin: 0 0 5px 0; color: #333;">🔋 Device Status</h4>
                    <table style="width: 100%; font-size: 13px;">
                        <tr>
                            <td><b>Battery:</b></td>
                            <td style="color: {bat_color}; font-weight: bold;">
                                {f'{int(battery)}%' if pd.notna(battery) else 'N/A'}
                            </td>
                        </tr>
                        <tr><td><b>Voltage:</b></td><td>{f'{voltage:.2f} V' if pd.notna(voltage) else 'N/A'}</td></tr>
                        <tr><td><b>Uptime:</b></td><td>{f'{int(uptime)//3600}h {(int(uptime)%3600)//60}m' if pd.notna(uptime) else 'N/A'}</td></tr>
                    </table>
                </div>
                
                <div style="background: #e8f0f8; padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                    <h4 style="margin: 0 0 5px 0; color: #333;">🌡️ Environmental</h4>
                    <table style="width: 100%; font-size: 13px;">
                        <tr><td><b>Temperature:</b></td><td>{f'{temp:.1f}°C ({celsius_to_fahrenheit(temp):.1f}°F)' if pd.notna(temp) else 'N/A'}</td></tr>
                        <tr><td><b>Humidity:</b></td><td>{f'{humidity:.1f}%' if pd.notna(humidity) else 'N/A'}</td></tr>
                    </table>
                </div>
                
                <div style="background: #f8f0e8; padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                    <h4 style="margin: 0 0 5px 0; color: #333;">📶 Signal Quality</h4>
                    <table style="width: 100%; font-size: 13px;">
                        <tr><td><b>SNR:</b></td><td>{f'{snr:.1f} dB' if pd.notna(snr) else 'N/A'}</td></tr>
                        <tr><td><b>RSSI:</b></td><td>{f'{rssi:.0f} dBm' if pd.notna(rssi) else 'N/A'}</td></tr>
                    </table>
                </div>
                
                <div style="text-align: center; color: #666; font-size: 11px; margin-top: 5px;">
                    Last seen: {format_timestamp(last_seen)}
                </div>
            </div>
            """
            
            popup = folium.Popup(popup_html, max_width=400)
            
            if pd.notna(battery):
                if battery >= 80:
                    icon_color = 'green'
                elif battery >= 50:
                    icon_color = 'lightgreen'
                elif battery >= 20:
                    icon_color = 'orange'
                else:
                    icon_color = 'red'
            else:
                icon_color = 'blue'
            
            marker = folium.Marker(
                location=[lat, lon],
                popup=popup,
                icon=folium.Icon(color=icon_color, icon='broadcast-tower', prefix='fa'),
                tooltip=f"{node_id} | Battery: {int(battery) if pd.notna(battery) else 'N/A'}% | SNR: {f'{snr:.1f}' if pd.notna(snr) else 'N/A'} dB"
            )
            
            marker.add_to(marker_cluster)
            marker.add_to(node_layer)
        
        node_layer.add_to(m)
        track_layer.add_to(m)
    
    folium.LayerControl(collapsed=False).add_to(m)
    
    return m


def main():
    st.title("📡 Meshtastic Mesh Network Dashboard")
    st.markdown("""
    **Real-time monitoring** of LoRa mesh network nodes via Snowpipe Streaming v2  
    *SenseCAP T1000-E Tracker | GPS + Environmental Sensors | BLE/Serial Connection*
    """)
    
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'search_location' not in st.session_state:
        st.session_state.search_location = None
    
    with st.sidebar:
        st.image("https://meshtastic.org/img/logo.svg", width=150)
        st.header("Dashboard Settings")
        
        time_range = st.selectbox(
            "Time Range",
            ["Last 15 minutes", "Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days", "All time"],
            index=3
        )
        
        time_filters = {
            "Last 15 minutes": "DATEADD(minute, -15, CURRENT_TIMESTAMP())",
            "Last 1 hour": "DATEADD(hour, -1, CURRENT_TIMESTAMP())",
            "Last 6 hours": "DATEADD(hour, -6, CURRENT_TIMESTAMP())",
            "Last 24 hours": "DATEADD(hour, -24, CURRENT_TIMESTAMP())",
            "Last 7 days": "DATEADD(day, -7, CURRENT_TIMESTAMP())",
            "All time": "'1970-01-01'::TIMESTAMP_TZ"
        }
        time_filter = time_filters[time_range]
        
        st.divider()
        
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
        show_fahrenheit = st.checkbox("Show temperature in °F", value=True)
        
        st.divider()
        
        with st.expander("About This Dashboard"):
            st.markdown("""
            **Data Pipeline:**
            - Device: SenseCAP T1000-E
            - Connection: BLE / Serial
            - Streaming: Snowpipe v2 REST API
            - Storage: Snowflake
            
            **Captured Data:**
            - GPS: lat, lon, altitude, speed, sats
            - Device: battery, voltage, uptime
            - Environmental: temp, humidity, pressure
            - Network: SNR, RSSI, hop count
            """)
        
        st.divider()
        with st.expander("Slack Notifications"):
            slack_webhook = st.text_input(
                "Slack Webhook URL",
                type="password",
                key="slack_webhook",
                help="Enter your Slack incoming webhook URL"
            )
            slack_channel = st.text_input(
                "Channel (optional)",
                placeholder="#meshtastic-alerts",
                key="slack_channel"
            )
            enable_slack = st.checkbox("Enable Slack alerts", value=False, key="enable_slack")
            
            if st.button("Test Slack Connection"):
                if slack_webhook:
                    test_msg = format_slack_alert(
                        "test-device",
                        "telemetry",
                        {"Status": "Test message from Meshtastic Dashboard", "Time": datetime.now().strftime('%H:%M:%S')}
                    )
                    if send_slack_message(slack_webhook, test_msg, slack_channel):
                        st.success("Test message sent!")
                    else:
                        st.error("Failed to send test message")
                else:
                    st.warning("Enter webhook URL first")
        
        st.divider()
        st.caption("Data Source:")
        st.code("DEMO.DEMO.MESHTASTIC_DATA", language=None)
        st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")
        
        if auto_refresh:
            import time
            time.sleep(30)
            st.rerun()
    
    try:
        stats_query = f"""
        SELECT 
            COUNT(*) as total_packets,
            COUNT(DISTINCT from_id) as unique_nodes,
            COUNT(CASE WHEN packet_type = 'position' THEN 1 END) as position_packets,
            COUNT(CASE WHEN packet_type = 'telemetry' THEN 1 END) as telemetry_packets,
            COUNT(CASE WHEN packet_type = 'text' THEN 1 END) as text_packets,
            AVG(rx_snr) as avg_snr,
            AVG(NULLIF(battery_level, 0)) as avg_battery,
            MAX(ingested_at) as last_packet,
            MIN(ingested_at) as first_packet
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE ingested_at >= {time_filter}
        """
        stats = run_query(stats_query)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total Packets",
                f"{stats['TOTAL_PACKETS'].iloc[0]:,}",
                help="Total packets received in selected time range"
            )
        with col2:
            st.metric(
                "Active Nodes",
                int(stats['UNIQUE_NODES'].iloc[0]),
                help="Number of unique device IDs"
            )
        with col3:
            st.metric(
                "Position Updates",
                int(stats['POSITION_PACKETS'].iloc[0]),
                help="GPS position packets"
            )
        with col4:
            st.metric(
                "Telemetry Packets",
                int(stats['TELEMETRY_PACKETS'].iloc[0]),
                help="Device/environmental telemetry"
            )
        with col5:
            avg_snr = stats['AVG_SNR'].iloc[0]
            snr_text = f"{avg_snr:.1f} dB" if avg_snr and not pd.isna(avg_snr) else "N/A"
            st.metric(
                "Avg Signal (SNR)",
                snr_text,
                help="Average Signal-to-Noise Ratio"
            )
            
    except Exception as e:
        st.warning(f"Could not load statistics: {e}")
    
    tab_map, tab_agent, tab_device, tab_env, tab_gps, tab_analytics, tab_raw = st.tabs([
        "🗺️ Live Map & Search",
        "🤖 AI Agent",
        "🔋 Device Status", 
        "🌡️ Environmental",
        "📍 GPS Details",
        "📊 Analytics",
        "🔍 Raw Data"
    ])
    
    with tab_map:
        st.subheader("Interactive Device Map")
        
        search_col1, search_col2, search_col3 = st.columns([3, 1, 1])
        
        with search_col1:
            search_input = st.text_input(
                "🔍 Search Location",
                placeholder="Enter address (e.g., 'Times Square, NYC') or coordinates (e.g., '40.7580, -73.9855')",
                help="Search by address or paste lat/long coordinates to find nearby nodes"
            )
        
        with search_col2:
            search_radius = st.selectbox("Search Radius", [5, 10, 25, 50, 100], index=1, format_func=lambda x: f"{x} km")
        
        with search_col3:
            search_btn = st.button("🔍 Search", type="primary", use_container_width=True)
        
        search_lat, search_lon, search_label = None, None, None
        nearby_nodes = pd.DataFrame()
        
        if search_btn and search_input:
            lat, lon, label = parse_coordinates(search_input)
            if lat is None:
                lat, lon, label = geocode_address(search_input)
            
            if lat and lon:
                search_lat, search_lon, search_label = lat, lon, label
                st.session_state.search_location = (lat, lon, label)
                st.success(f"📍 Found: {label}")
                
                with st.spinner("Finding nearby nodes..."):
                    nearby_nodes = get_nodes_near_location(lat, lon, search_radius)
                    
                    if not nearby_nodes.empty:
                        st.info(f"Found {len(nearby_nodes)} node(s) within {search_radius}km")
                    else:
                        st.warning(f"No nodes found within {search_radius}km of this location")
            else:
                st.error("Could not find location. Try a different address or coordinates.")
        
        if st.session_state.search_location:
            search_lat, search_lon, search_label = st.session_state.search_location
            if st.button("Clear Search", type="secondary"):
                st.session_state.search_location = None
                st.rerun()
        
        map_col1, map_col2 = st.columns([3, 1])
        
        with map_col2:
            st.markdown("#### Map Options")
            show_track = st.checkbox("Show movement tracks", value=True, key="show_track_main")
            track_limit = st.slider("Max data points", 50, 500, 200) if show_track else 200
        
        try:
            positions_query = f"""
            SELECT 
                from_id,
                latitude,
                longitude,
                altitude,
                ground_speed,
                sats_in_view,
                gps_timestamp,
                ingested_at,
                battery_level,
                voltage,
                temperature,
                relative_humidity,
                uptime_seconds,
                rx_snr,
                rx_rssi
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE packet_type = 'position'
              AND latitude IS NOT NULL 
              AND longitude IS NOT NULL
              AND latitude != 0
              AND longitude != 0
              AND ingested_at >= {time_filter}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY ingested_at DESC) = 1
            ORDER BY ingested_at DESC
            LIMIT {track_limit}
            """
            positions = run_query(positions_query)
            
            with map_col1:
                if not positions.empty or (search_lat and search_lon):
                    display_df = nearby_nodes if not nearby_nodes.empty else positions
                    
                    folium_map = create_folium_map(
                        display_df,
                        center_lat=search_lat,
                        center_lon=search_lon,
                        search_lat=search_lat,
                        search_lon=search_lon,
                        search_label=search_label
                    )
                    
                    st_data = st_folium(
                        folium_map,
                        width=None,
                        height=600,
                        returned_objects=["last_clicked"],
                        key="main_map"
                    )
                    
                    if st_data and st_data.get("last_clicked"):
                        clicked = st_data["last_clicked"]
                        st.info(f"Clicked: {clicked['lat']:.6f}, {clicked['lng']:.6f}")
                else:
                    st.info("No position data available. Make sure the device has GPS lock.")
                    st.markdown("""
                    **Troubleshooting:**
                    - Ensure device is outdoors with clear sky view
                    - Wait for GPS fix (may take 1-2 minutes cold start)
                    - Check that position broadcasting is enabled
                    """)
        except Exception as e:
            st.error(f"Error loading map data: {e}")
        
        if not nearby_nodes.empty:
            st.markdown("#### Nodes Near Search Location")
            display_cols = ['FROM_ID', 'DISTANCE_KM', 'LATITUDE', 'LONGITUDE', 'BATTERY_LEVEL', 'RX_SNR', 'INGESTED_AT']
            display_df = nearby_nodes[display_cols].copy()
            display_df.columns = ['Node ID', 'Distance (km)', 'Latitude', 'Longitude', 'Battery %', 'SNR (dB)', 'Last Seen']
            display_df['Distance (km)'] = display_df['Distance (km)'].round(2)
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        elif not positions.empty:
            st.markdown("#### All Node Positions")
            pos_display = positions[['FROM_ID', 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'BATTERY_LEVEL', 'RX_SNR', 'INGESTED_AT']].copy()
            pos_display.columns = ['Node', 'Latitude', 'Longitude', 'Altitude (m)', 'Battery %', 'SNR (dB)', 'Last Update']
            st.dataframe(pos_display, use_container_width=True, hide_index=True)
    
    with tab_agent:
        st.subheader("🤖 Meshtastic AI Agent")
        st.markdown("""
        Ask questions about your mesh network in natural language. The agent can query device locations, 
        battery status, signal quality, network health, and more.
        """)
        
        example_questions = [
            "What Meshtastic nodes are active right now?",
            "Show me devices with low battery",
            "What is the network health summary?",
            "Which devices have poor signal quality?",
            "What are the recent GPS positions?",
            "Find nodes near coordinates 40.7580, -73.9855",
            "How many packets were received in the last hour?",
            "What is the average battery level across all devices?"
        ]
        
        with st.expander("💡 Example Questions", expanded=False):
            cols = st.columns(2)
            for i, q in enumerate(example_questions):
                if cols[i % 2].button(q, key=f"example_{i}", use_container_width=True):
                    st.session_state.agent_input = q
        
        for msg in st.session_state.chat_history:
            if msg["role"] == "user":
                st.markdown(f"""
                <div class="chat-message user-message">
                    <strong>You:</strong> {msg["content"]}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="chat-message agent-message">
                    <strong>🤖 Agent:</strong><br>{msg["content"]}
                </div>
                """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([5, 1])
        
        with col1:
            user_input = st.text_input(
                "Ask the Meshtastic Agent",
                value=st.session_state.get("agent_input", ""),
                placeholder="e.g., 'What nodes are near Times Square?' or 'Show me devices with low battery'",
                key="agent_question",
                label_visibility="collapsed"
            )
        
        with col2:
            ask_btn = st.button("Ask", type="primary", use_container_width=True)
        
        if ask_btn and user_input:
            st.session_state.chat_history.append({"role": "user", "content": user_input})
            
            with st.spinner("Agent is thinking..."):
                response = query_cortex_agent(user_input)
            
            st.session_state.chat_history.append({"role": "assistant", "content": response})
            st.session_state.agent_input = ""
            st.rerun()
        
        col1, col2 = st.columns(2)
        if col1.button("🗑️ Clear Chat History"):
            st.session_state.chat_history = []
            st.rerun()
        
        st.divider()
        
        st.markdown("#### Quick Location Query")
        loc_col1, loc_col2, loc_col3 = st.columns([3, 1, 1])
        
        with loc_col1:
            location_query = st.text_input(
                "Location",
                placeholder="Enter address or coordinates",
                key="agent_location"
            )
        
        with loc_col2:
            query_radius = st.selectbox("Radius", [5, 10, 25, 50], index=1, format_func=lambda x: f"{x} km", key="agent_radius")
        
        with loc_col3:
            if st.button("Find Nodes", type="secondary", use_container_width=True):
                if location_query:
                    lat, lon, label = parse_coordinates(location_query)
                    if lat is None:
                        lat, lon, label = geocode_address(location_query)
                    
                    if lat and lon:
                        question = f"What Meshtastic nodes are near coordinates {lat}, {lon} (within {query_radius}km)?"
                        st.session_state.chat_history.append({"role": "user", "content": question})
                        
                        with st.spinner("Finding nodes..."):
                            response = query_cortex_agent(question)
                        
                        st.session_state.chat_history.append({"role": "assistant", "content": response})
                        st.rerun()
                    else:
                        st.error("Could not geocode location")
    
    with tab_device:
        st.subheader("Device Health & Status")
        
        try:
            device_query = f"""
            SELECT 
                from_id,
                MAX(ingested_at) as last_seen,
                MAX(battery_level) as battery_level,
                MAX(voltage) as voltage,
                MAX(uptime_seconds) as uptime_seconds,
                MAX(channel_utilization) as channel_util,
                MAX(air_util_tx) as air_util_tx,
                COUNT(*) as packet_count,
                AVG(rx_snr) as avg_snr,
                AVG(rx_rssi) as avg_rssi
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE ingested_at >= {time_filter}
              AND from_id IS NOT NULL
            GROUP BY from_id
            ORDER BY last_seen DESC
            """
            devices = run_query(device_query)
            
            if not devices.empty:
                for _, device in devices.iterrows():
                    node_id = device['FROM_ID']
                    battery = device['BATTERY_LEVEL']
                    voltage = device['VOLTAGE']
                    last_seen = device['LAST_SEEN']
                    uptime = device['UPTIME_SECONDS']
                    
                    bat_color, bat_icon = get_battery_status(battery)
                    
                    minutes_ago = 0
                    if last_seen and not pd.isna(last_seen):
                        try:
                            if isinstance(last_seen, str):
                                last_dt = pd.to_datetime(last_seen)
                            else:
                                last_dt = last_seen
                            minutes_ago = (datetime.now() - last_dt.replace(tzinfo=None)).total_seconds() / 60
                        except:
                            pass
                    
                    status_color = "status-online" if minutes_ago < 5 else ("status-warning" if minutes_ago < 30 else "status-offline")
                    status_text = "ONLINE" if minutes_ago < 5 else ("IDLE" if minutes_ago < 30 else "OFFLINE")
                    
                    with st.container():
                        col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])
                        
                        with col1:
                            st.markdown(f"### {node_id}")
                            st.markdown(f"<span class='{status_color}'>{status_text}</span> - Last seen: {format_timestamp(last_seen)}", unsafe_allow_html=True)
                        
                        with col2:
                            if battery and not pd.isna(battery):
                                clamped_battery = clamp_battery(battery)
                                st.metric("Battery", f"{int(battery)}%")
                                st.progress(clamped_battery / 100)
                                if enable_slack and slack_webhook and clamped_battery < 20:
                                    alert_msg = format_slack_alert(
                                        node_id, "low_battery",
                                        {"Battery": f"{clamped_battery}%", "Voltage": f"{voltage:.2f}V" if voltage else "N/A"}
                                    )
                                    send_slack_message(slack_webhook, alert_msg, slack_channel)
                            else:
                                st.metric("Battery", "N/A")
                        
                        with col3:
                            if voltage and not pd.isna(voltage):
                                st.metric("Voltage", f"{voltage:.2f}V")
                            else:
                                st.metric("Voltage", "N/A")
                        
                        with col4:
                            if uptime and not pd.isna(uptime):
                                hours = int(uptime) // 3600
                                mins = (int(uptime) % 3600) // 60
                                st.metric("Uptime", f"{hours}h {mins}m")
                            else:
                                st.metric("Uptime", "N/A")
                        
                        with col5:
                            avg_snr = device['AVG_SNR']
                            if avg_snr and not pd.isna(avg_snr):
                                st.metric("Avg SNR", f"{avg_snr:.1f} dB")
                            else:
                                st.metric("Avg SNR", "N/A")
                        
                        st.divider()
                
                st.markdown("#### Battery History")
                battery_hist_query = f"""
                SELECT 
                    from_id,
                    ingested_at,
                    battery_level,
                    voltage
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE packet_type = 'telemetry'
                  AND battery_level IS NOT NULL
                  AND ingested_at >= {time_filter}
                ORDER BY ingested_at
                """
                battery_hist = run_query(battery_hist_query)
                
                if not battery_hist.empty:
                    fig = px.line(
                        battery_hist,
                        x='INGESTED_AT',
                        y='BATTERY_LEVEL',
                        color='FROM_ID',
                        title='Battery Level Over Time',
                        labels={'INGESTED_AT': 'Time', 'BATTERY_LEVEL': 'Battery %', 'FROM_ID': 'Node'}
                    )
                    fig.update_layout(yaxis_range=[0, 105])
                    fig.add_hline(y=20, line_dash="dash", line_color="red", annotation_text="Low Battery")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No device data available for the selected time range")
                
        except Exception as e:
            st.error(f"Error loading device data: {e}")
    
    with tab_env:
        st.subheader("Environmental Sensors")
        st.markdown("Temperature, humidity, and barometric pressure from T1000-E sensors")
        
        try:
            env_query = f"""
            SELECT 
                from_id,
                ingested_at,
                temperature,
                relative_humidity,
                barometric_pressure
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE packet_type = 'telemetry'
              AND temperature IS NOT NULL
              AND ingested_at >= {time_filter}
            ORDER BY ingested_at DESC
            LIMIT 500
            """
            env_data = run_query(env_query)
            
            if not env_data.empty:
                latest_env = env_data.iloc[0]
                temp_c = latest_env['TEMPERATURE']
                temp_f = celsius_to_fahrenheit(temp_c) if temp_c else None
                humidity = latest_env['RELATIVE_HUMIDITY']
                pressure = latest_env['BAROMETRIC_PRESSURE']
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if temp_c and not pd.isna(temp_c):
                        if show_fahrenheit and temp_f:
                            st.metric("Temperature", f"{temp_f:.1f}°F", f"{temp_c:.1f}°C")
                        else:
                            st.metric("Temperature", f"{temp_c:.1f}°C", f"{temp_f:.1f}°F" if temp_f else "")
                    else:
                        st.metric("Temperature", "N/A")
                
                with col2:
                    if humidity and not pd.isna(humidity):
                        st.metric("Humidity", f"{humidity:.1f}%")
                    else:
                        st.metric("Humidity", "N/A")
                
                with col3:
                    if pressure and not pd.isna(pressure):
                        hpa = pressure / 100 if pressure > 10000 else pressure
                        st.metric("Pressure", f"{hpa:.1f} hPa")
                    else:
                        st.metric("Pressure", "N/A")
                
                st.markdown("#### Temperature History")
                
                if show_fahrenheit:
                    env_data['TEMP_DISPLAY'] = env_data['TEMPERATURE'].apply(celsius_to_fahrenheit)
                    temp_label = 'Temperature (°F)'
                else:
                    env_data['TEMP_DISPLAY'] = env_data['TEMPERATURE']
                    temp_label = 'Temperature (°C)'
                
                fig = px.line(
                    env_data,
                    x='INGESTED_AT',
                    y='TEMP_DISPLAY',
                    color='FROM_ID',
                    title=f'Temperature Over Time ({temp_label})',
                    labels={'INGESTED_AT': 'Time', 'TEMP_DISPLAY': temp_label, 'FROM_ID': 'Node'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                if 'RELATIVE_HUMIDITY' in env_data.columns and env_data['RELATIVE_HUMIDITY'].notna().any():
                    st.markdown("#### Humidity History")
                    fig_hum = px.line(
                        env_data[env_data['RELATIVE_HUMIDITY'].notna()],
                        x='INGESTED_AT',
                        y='RELATIVE_HUMIDITY',
                        color='FROM_ID',
                        title='Relative Humidity Over Time',
                        labels={'INGESTED_AT': 'Time', 'RELATIVE_HUMIDITY': 'Humidity %', 'FROM_ID': 'Node'}
                    )
                    fig_hum.update_layout(yaxis_range=[0, 100])
                    st.plotly_chart(fig_hum, use_container_width=True)
            else:
                st.info("No environmental sensor data available.")
                
        except Exception as e:
            st.error(f"Error loading environmental data: {e}")
    
    with tab_gps:
        st.subheader("GPS & Position Details")
        st.markdown("Detailed GPS information including accuracy metrics and movement data")
        
        try:
            gps_query = f"""
            SELECT 
                from_id,
                ingested_at,
                latitude,
                longitude,
                altitude,
                ground_speed,
                ground_track,
                sats_in_view,
                pdop,
                hdop,
                vdop,
                gps_timestamp,
                precision_bits
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE packet_type = 'position'
              AND latitude IS NOT NULL
              AND ingested_at >= {time_filter}
            ORDER BY ingested_at DESC
            LIMIT 200
            """
            gps_data = run_query(gps_query)
            
            if not gps_data.empty:
                latest_gps = gps_data.iloc[0]
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Latitude", f"{latest_gps['LATITUDE']:.6f}°")
                    st.metric("Longitude", f"{latest_gps['LONGITUDE']:.6f}°")
                
                with col2:
                    alt = latest_gps['ALTITUDE']
                    st.metric("Altitude", f"{alt} m" if alt and not pd.isna(alt) else "N/A")
                    speed = latest_gps['GROUND_SPEED']
                    st.metric("Ground Speed", f"{speed} m/s" if speed and not pd.isna(speed) else "N/A")
                
                with col3:
                    sats = latest_gps['SATS_IN_VIEW']
                    st.metric("Satellites", int(sats) if sats and not pd.isna(sats) else "N/A")
                    hdop = latest_gps['HDOP']
                    st.metric("HDOP", f"{hdop/100:.1f}" if hdop and not pd.isna(hdop) else "N/A")
                
                with col4:
                    gps_ts = latest_gps['GPS_TIMESTAMP']
                    if gps_ts and not pd.isna(gps_ts):
                        try:
                            gps_time = datetime.fromtimestamp(int(gps_ts))
                            st.metric("GPS Time", gps_time.strftime("%H:%M:%S"))
                        except:
                            st.metric("GPS Time", str(gps_ts))
                    else:
                        st.metric("GPS Time", "N/A")
                    track = latest_gps['GROUND_TRACK']
                    st.metric("Heading", f"{track}°" if track and not pd.isna(track) else "N/A")
                
                st.markdown("#### Altitude Profile")
                if gps_data['ALTITUDE'].notna().any():
                    fig_alt = px.line(
                        gps_data[gps_data['ALTITUDE'].notna()].sort_values('INGESTED_AT'),
                        x='INGESTED_AT',
                        y='ALTITUDE',
                        color='FROM_ID',
                        title='Altitude Over Time',
                        labels={'INGESTED_AT': 'Time', 'ALTITUDE': 'Altitude (m)', 'FROM_ID': 'Node'}
                    )
                    st.plotly_chart(fig_alt, use_container_width=True)
                
                st.markdown("#### GPS Quality Metrics")
                col1, col2 = st.columns(2)
                
                with col1:
                    if gps_data['SATS_IN_VIEW'].notna().any():
                        fig_sats = px.line(
                            gps_data[gps_data['SATS_IN_VIEW'].notna()].sort_values('INGESTED_AT'),
                            x='INGESTED_AT',
                            y='SATS_IN_VIEW',
                            color='FROM_ID',
                            title='Satellites in View',
                            labels={'INGESTED_AT': 'Time', 'SATS_IN_VIEW': 'Satellites', 'FROM_ID': 'Node'}
                        )
                        st.plotly_chart(fig_sats, use_container_width=True)
                
                with col2:
                    if gps_data['HDOP'].notna().any():
                        gps_data['HDOP_SCALED'] = gps_data['HDOP'] / 100
                        fig_dop = px.line(
                            gps_data[gps_data['HDOP'].notna()].sort_values('INGESTED_AT'),
                            x='INGESTED_AT',
                            y='HDOP_SCALED',
                            color='FROM_ID',
                            title='Horizontal DOP (lower is better)',
                            labels={'INGESTED_AT': 'Time', 'HDOP_SCALED': 'HDOP', 'FROM_ID': 'Node'}
                        )
                        fig_dop.add_hline(y=1, line_dash="dash", line_color="green", annotation_text="Excellent")
                        fig_dop.add_hline(y=2, line_dash="dash", line_color="orange", annotation_text="Good")
                        st.plotly_chart(fig_dop, use_container_width=True)
                
                st.markdown("#### Recent Position History")
                display_cols = ['FROM_ID', 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'GROUND_SPEED', 'SATS_IN_VIEW', 'INGESTED_AT']
                display_data = gps_data[display_cols].head(20).copy()
                display_data.columns = ['Node', 'Latitude', 'Longitude', 'Altitude (m)', 'Speed (m/s)', 'Satellites', 'Timestamp']
                st.dataframe(display_data, use_container_width=True, hide_index=True)
            else:
                st.info("No GPS position data available.")
                
        except Exception as e:
            st.error(f"Error loading GPS data: {e}")
    
    with tab_analytics:
        st.subheader("Network Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Packet Distribution")
            try:
                type_query = f"""
                SELECT 
                    packet_type,
                    COUNT(*) as count
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE ingested_at >= {time_filter}
                  AND packet_type IS NOT NULL
                GROUP BY packet_type
                ORDER BY count DESC
                """
                type_df = run_query(type_query)
                
                if not type_df.empty:
                    fig = px.pie(
                        type_df, 
                        values='COUNT', 
                        names='PACKET_TYPE',
                        title='Packets by Type',
                        hole=0.4,
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")
        
        with col2:
            st.markdown("#### Signal Quality Distribution")
            try:
                snr_query = f"""
                SELECT 
                    ROUND(rx_snr, 0) as snr_bucket,
                    COUNT(*) as count
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE rx_snr IS NOT NULL
                  AND ingested_at >= {time_filter}
                GROUP BY ROUND(rx_snr, 0)
                ORDER BY snr_bucket
                """
                snr_df = run_query(snr_query)
                
                if not snr_df.empty:
                    fig = px.bar(
                        snr_df,
                        x='SNR_BUCKET',
                        y='COUNT',
                        title='SNR Distribution',
                        labels={'SNR_BUCKET': 'SNR (dB)', 'COUNT': 'Packet Count'},
                        color='SNR_BUCKET',
                        color_continuous_scale='RdYlGn'
                    )
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")
        
        st.markdown("#### Hourly Traffic Pattern")
        try:
            hourly_query = f"""
            SELECT 
                DATE_TRUNC('hour', ingested_at) as hour,
                COUNT(*) as packets,
                COUNT(DISTINCT from_id) as nodes,
                COUNT(CASE WHEN packet_type = 'position' THEN 1 END) as positions,
                COUNT(CASE WHEN packet_type = 'telemetry' THEN 1 END) as telemetry
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE ingested_at >= {time_filter}
            GROUP BY DATE_TRUNC('hour', ingested_at)
            ORDER BY hour
            """
            hourly_df = run_query(hourly_query)
            
            if not hourly_df.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(x=hourly_df['HOUR'], y=hourly_df['POSITIONS'], name='Position', marker_color='#2ecc71'))
                fig.add_trace(go.Bar(x=hourly_df['HOUR'], y=hourly_df['TELEMETRY'], name='Telemetry', marker_color='#3498db'))
                fig.update_layout(
                    barmode='stack',
                    title='Hourly Packet Volume',
                    xaxis_title='Hour',
                    yaxis_title='Packets'
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
        
        st.markdown("#### Node Activity Summary")
        try:
            nodes_query = f"""
            SELECT 
                from_id as "Node ID",
                COUNT(*) as "Total Packets",
                COUNT(CASE WHEN packet_type = 'position' THEN 1 END) as "Positions",
                COUNT(CASE WHEN packet_type = 'telemetry' THEN 1 END) as "Telemetry",
                MAX(ingested_at) as "Last Seen",
                ROUND(AVG(rx_snr), 1) as "Avg SNR (dB)",
                MAX(battery_level) as "Battery %"
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE from_id IS NOT NULL
              AND ingested_at >= {time_filter}
            GROUP BY from_id
            ORDER BY "Total Packets" DESC
            LIMIT 20
            """
            nodes_df = run_query(nodes_query)
            st.dataframe(nodes_df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab_raw:
        st.subheader("Raw Packet Data")
        
        packet_type_filter = st.multiselect(
            "Filter by Packet Type",
            ["position", "telemetry", "text", "nodeinfo", "routing"],
            default=[]
        )
        
        node_filter = st.text_input("Filter by Node ID (partial match)", "")
        
        try:
            where_clauses = [f"ingested_at >= {time_filter}"]
            
            if packet_type_filter:
                types_str = ", ".join([f"'{t}'" for t in packet_type_filter])
                where_clauses.append(f"packet_type IN ({types_str})")
            
            if node_filter:
                where_clauses.append(f"from_id ILIKE '%{node_filter}%'")
            
            where_clause = " AND ".join(where_clauses)
            
            raw_query = f"""
            SELECT 
                ingested_at,
                from_id,
                to_id,
                packet_type,
                latitude,
                longitude,
                altitude,
                battery_level,
                temperature,
                rx_snr,
                rx_rssi,
                text_message,
                channel,
                hop_limit
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE {where_clause}
            ORDER BY ingested_at DESC
            LIMIT 100
            """
            raw_df = run_query(raw_query)
            
            if not raw_df.empty:
                st.dataframe(raw_df, use_container_width=True, hide_index=True, height=500)
                
                csv = raw_df.to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv,
                    "meshtastic_data.csv",
                    "text/csv",
                    key='download-csv'
                )
            else:
                st.info("No data matching the selected filters")
                
        except Exception as e:
            st.error(f"Error loading raw data: {e}")


if __name__ == "__main__":
    main()
