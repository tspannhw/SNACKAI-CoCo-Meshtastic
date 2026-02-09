#!/usr/bin/env python3
"""
Meshtastic Mesh Network Dashboard
=================================
Real-time visualization and monitoring of Meshtastic LoRa mesh network data
streamed to Snowflake via Snowpipe Streaming v2.

Features:
- Interactive map with device locations and tracking
- Real-time telemetry monitoring (battery, temperature, humidity)
- GPS data visualization (position, altitude, speed, satellites)
- Signal quality metrics (SNR, RSSI)
- Network analytics and traffic patterns
- Device health monitoring

Data Source: DEMO.DEMO.MESHTASTIC_DATA (Snowpipe Streaming v2)
Device: SenseCAP Card Tracker T1000-E via BLE/Serial
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
import requests

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


def main():
    st.title("📡 Meshtastic Mesh Network Dashboard")
    st.markdown("""
    **Real-time monitoring** of LoRa mesh network nodes via Snowpipe Streaming v2  
    *SenseCAP T1000-E Tracker | GPS + Environmental Sensors | BLE/Serial Connection*
    """)
    
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
    
    tab_map, tab_device, tab_env, tab_gps, tab_analytics, tab_ai, tab_raw, tab_slack = st.tabs([
        "🗺️ Live Map",
        "🔋 Device Status", 
        "🌡️ Environmental",
        "📍 GPS Details",
        "📊 Analytics",
        "🤖 AI Analysis",
        "🔍 Raw Data",
        "📢 Slack"
    ])
    
    with tab_map:
        st.subheader("Live Device Locations")
        st.markdown("Interactive map showing mesh network node positions with tracking history")
        
        map_col1, map_col2 = st.columns([3, 1])
        
        with map_col2:
            map_style = st.selectbox(
                "Map Style",
                ["open-street-map", "carto-positron", "carto-darkmatter", "stamen-terrain"],
                index=0
            )
            show_track = st.checkbox("Show movement track", value=True)
            track_limit = st.slider("Track points", 10, 200, 50) if show_track else 50
        
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
                rx_snr
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE packet_type = 'position'
              AND latitude IS NOT NULL 
              AND longitude IS NOT NULL
              AND latitude != 0
              AND longitude != 0
              AND ingested_at >= {time_filter}
            ORDER BY ingested_at DESC
            LIMIT {track_limit}
            """
            positions = run_query(positions_query)
            
            with map_col1:
                if not positions.empty:
                    latest = positions.groupby('FROM_ID').first().reset_index()
                    
                    fig = go.Figure()
                    
                    if show_track and len(positions) > 1:
                        for node_id in positions['FROM_ID'].unique():
                            node_track = positions[positions['FROM_ID'] == node_id].sort_values('INGESTED_AT')
                            if len(node_track) > 1:
                                fig.add_trace(go.Scattermapbox(
                                    lat=node_track['LATITUDE'],
                                    lon=node_track['LONGITUDE'],
                                    mode='lines',
                                    line=dict(width=2, color='cyan'),
                                    name=f"{node_id} track",
                                    opacity=0.6
                                ))
                    
                    battery_colors = []
                    for _, row in latest.iterrows():
                        color, _ = get_battery_status(row.get('BATTERY_LEVEL'))
                        battery_colors.append(color)
                    
                    hover_text = []
                    for _, row in latest.iterrows():
                        text = f"<b>{row['FROM_ID']}</b><br>"
                        text += f"Altitude: {row.get('ALTITUDE', 'N/A')} m<br>"
                        text += f"Speed: {row.get('GROUND_SPEED', 'N/A')} m/s<br>"
                        text += f"Satellites: {row.get('SATS_IN_VIEW', 'N/A')}<br>"
                        text += f"Battery: {row.get('BATTERY_LEVEL', 'N/A')}%<br>"
                        text += f"SNR: {row.get('RX_SNR', 'N/A')} dB<br>"
                        text += f"Last seen: {format_timestamp(row.get('INGESTED_AT'))}"
                        hover_text.append(text)
                    
                    fig.add_trace(go.Scattermapbox(
                        lat=latest['LATITUDE'],
                        lon=latest['LONGITUDE'],
                        mode='markers+text',
                        marker=dict(
                            size=20,
                            color=battery_colors,
                            opacity=0.9
                        ),
                        text=latest['FROM_ID'].str[-4:],
                        textposition='top center',
                        textfont=dict(size=10, color='white'),
                        hovertext=hover_text,
                        hoverinfo='text',
                        name='Current Position'
                    ))
                    
                    center_lat = positions['LATITUDE'].mean()
                    center_lon = positions['LONGITUDE'].mean()
                    
                    fig.update_layout(
                        mapbox=dict(
                            style=map_style,
                            center=dict(lat=center_lat, lon=center_lon),
                            zoom=13
                        ),
                        showlegend=True,
                        height=550,
                        margin={"r": 0, "t": 0, "l": 0, "b": 0}
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.markdown("#### Position Details")
                    pos_display = latest[['FROM_ID', 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'GROUND_SPEED', 'SATS_IN_VIEW', 'INGESTED_AT']].copy()
                    pos_display.columns = ['Node', 'Latitude', 'Longitude', 'Altitude (m)', 'Speed (m/s)', 'Satellites', 'Last Update']
                    st.dataframe(pos_display, use_container_width=True, hide_index=True)
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
                st.markdown("""
                **Note:** The T1000-E sends environmental telemetry every 30 minutes by default.
                You can adjust this in device settings.
                """)
                
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
    
    with tab_ai:
        st.subheader("🤖 Cortex AI Analysis")
        st.markdown("Use Snowflake Cortex AI SQL functions to analyze mesh network data")
        
        ai_col1, ai_col2 = st.columns(2)
        
        with ai_col1:
            st.markdown("#### 📝 Text Message Analysis")
            try:
                text_msg_query = f"""
                SELECT 
                    from_id,
                    text_message,
                    ingested_at
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE packet_type = 'text'
                  AND text_message IS NOT NULL
                  AND text_message != ''
                  AND ingested_at >= {time_filter}
                ORDER BY ingested_at DESC
                LIMIT 20
                """
                text_msgs = run_query(text_msg_query)
                
                if not text_msgs.empty:
                    st.markdown("**Sentiment Analysis on Messages:**")
                    sentiment_query = f"""
                    SELECT 
                        from_id,
                        text_message,
                        SNOWFLAKE.CORTEX.SENTIMENT(text_message) as sentiment_score,
                        ingested_at
                    FROM DEMO.DEMO.MESHTASTIC_DATA
                    WHERE packet_type = 'text'
                      AND text_message IS NOT NULL
                      AND text_message != ''
                      AND ingested_at >= {time_filter}
                    ORDER BY ingested_at DESC
                    LIMIT 10
                    """
                    try:
                        sentiment_df = run_query(sentiment_query)
                        if not sentiment_df.empty:
                            for _, row in sentiment_df.iterrows():
                                score = row['SENTIMENT_SCORE']
                                emoji = "😊" if score > 0.3 else ("😐" if score > -0.3 else "😞")
                                st.markdown(f"{emoji} **{row['FROM_ID']}**: {row['TEXT_MESSAGE'][:50]}...")
                                st.caption(f"Sentiment: {score:.2f}")
                    except Exception as e:
                        st.info(f"Sentiment analysis requires Cortex AI access: {e}")
                else:
                    st.info("No text messages found. Send a message from your Meshtastic device!")
            except Exception as e:
                st.error(f"Error loading messages: {e}")
        
        with ai_col2:
            st.markdown("#### 🔮 AI-Powered Insights")
            
            insight_type = st.selectbox(
                "Select Analysis Type",
                ["Device Health Summary", "Network Status Report", "Location Analysis", "Custom Query"]
            )
            
            if st.button("Generate AI Insight", type="primary"):
                try:
                    if insight_type == "Device Health Summary":
                        ai_query = f"""
                        SELECT SNOWFLAKE.CORTEX.COMPLETE(
                            'mistral-large2',
                            CONCAT(
                                'Analyze this IoT device telemetry data and provide a brief health assessment. ',
                                'Data: ',
                                (SELECT LISTAGG(
                                    CONCAT('Device:', from_id, ' Battery:', battery_level, '% Temp:', temperature, 'C Uptime:', uptime_seconds, 's'),
                                    '; '
                                ) FROM (
                                    SELECT from_id, MAX(battery_level) as battery_level, 
                                           MAX(temperature) as temperature, MAX(uptime_seconds) as uptime_seconds
                                    FROM DEMO.DEMO.MESHTASTIC_DATA
                                    WHERE ingested_at >= {time_filter}
                                    GROUP BY from_id
                                    LIMIT 5
                                ))
                            )
                        ) as insight
                        """
                    elif insight_type == "Network Status Report":
                        ai_query = f"""
                        SELECT SNOWFLAKE.CORTEX.COMPLETE(
                            'mistral-large2',
                            CONCAT(
                                'Analyze this LoRa mesh network data and provide a brief network health report. ',
                                'Include signal quality assessment. Data: ',
                                (SELECT CONCAT(
                                    'Total packets: ', COUNT(*), 
                                    ', Unique nodes: ', COUNT(DISTINCT from_id),
                                    ', Avg SNR: ', ROUND(AVG(rx_snr), 1), 'dB',
                                    ', Avg RSSI: ', ROUND(AVG(rx_rssi), 1), 'dBm',
                                    ', Position updates: ', SUM(CASE WHEN packet_type='position' THEN 1 ELSE 0 END),
                                    ', Telemetry packets: ', SUM(CASE WHEN packet_type='telemetry' THEN 1 ELSE 0 END)
                                ) FROM DEMO.DEMO.MESHTASTIC_DATA WHERE ingested_at >= {time_filter})
                            )
                        ) as insight
                        """
                    elif insight_type == "Location Analysis":
                        ai_query = f"""
                        SELECT SNOWFLAKE.CORTEX.COMPLETE(
                            'mistral-large2',
                            CONCAT(
                                'Analyze GPS tracking data for IoT devices and summarize movement patterns. Data: ',
                                (SELECT LISTAGG(
                                    CONCAT('Lat:', ROUND(latitude, 4), ' Lon:', ROUND(longitude, 4), ' Alt:', altitude, 'm Speed:', ground_speed, 'm/s'),
                                    '; '
                                ) FROM (
                                    SELECT latitude, longitude, altitude, ground_speed
                                    FROM DEMO.DEMO.MESHTASTIC_DATA
                                    WHERE packet_type = 'position' 
                                      AND latitude IS NOT NULL
                                      AND ingested_at >= {time_filter}
                                    ORDER BY ingested_at DESC
                                    LIMIT 10
                                ))
                            )
                        ) as insight
                        """
                    else:
                        ai_query = None
                    
                    if ai_query:
                        with st.spinner("Generating AI insight..."):
                            result = run_query(ai_query)
                            if not result.empty:
                                st.success("**AI Analysis:**")
                                st.markdown(result['INSIGHT'].iloc[0])
                except Exception as e:
                    st.error(f"AI analysis error: {e}")
                    st.info("Ensure Cortex AI functions are enabled in your region")
        
        st.divider()
        
        st.markdown("#### 🏷️ AI Classification")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Classify Device Status:**")
            try:
                classify_query = f"""
                SELECT 
                    from_id,
                    battery_level,
                    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
                        CONCAT('Battery level is ', battery_level, ' percent'),
                        ['Critical - needs immediate attention', 'Low - charge soon', 'Good - normal operation', 'Excellent - fully charged']
                    ):label::STRING as status_class
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE battery_level IS NOT NULL
                  AND ingested_at >= {time_filter}
                GROUP BY from_id, battery_level
                ORDER BY battery_level
                LIMIT 5
                """
                classify_df = run_query(classify_query)
                if not classify_df.empty:
                    for _, row in classify_df.iterrows():
                        status = row['STATUS_CLASS']
                        icon = "🔴" if "Critical" in status else ("🟡" if "Low" in status else ("🟢" if "Good" in status else "🔵"))
                        st.markdown(f"{icon} **{row['FROM_ID']}**: {row['BATTERY_LEVEL']}% - {status}")
            except Exception as e:
                st.info(f"Classification requires Cortex AI: {e}")
        
        with col2:
            st.markdown("**Signal Quality Classification:**")
            try:
                signal_classify_query = f"""
                SELECT 
                    from_id,
                    ROUND(AVG(rx_snr), 1) as avg_snr,
                    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
                        CONCAT('Signal SNR is ', ROUND(AVG(rx_snr), 1), ' dB'),
                        ['Poor signal - may lose connection', 'Weak signal - intermittent', 'Good signal - stable', 'Excellent signal - strong']
                    ):label::STRING as signal_class
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE rx_snr IS NOT NULL
                  AND ingested_at >= {time_filter}
                GROUP BY from_id
                LIMIT 5
                """
                signal_df = run_query(signal_classify_query)
                if not signal_df.empty:
                    for _, row in signal_df.iterrows():
                        signal = row['SIGNAL_CLASS']
                        icon = "📶" if "Excellent" in signal else ("📶" if "Good" in signal else ("📉" if "Weak" in signal else "❌"))
                        st.markdown(f"{icon} **{row['FROM_ID']}**: {row['AVG_SNR']} dB - {signal}")
            except Exception as e:
                st.info(f"Classification requires Cortex AI: {e}")
        
        st.divider()
        
        st.markdown("#### 📊 AI Data Summarization")
        if st.button("Generate Network Summary Report"):
            try:
                summary_query = f"""
                SELECT SNOWFLAKE.CORTEX.SUMMARIZE(
                    (SELECT LISTAGG(
                        CONCAT(
                            'Packet from device ', from_id, 
                            ' type:', packet_type,
                            CASE WHEN battery_level IS NOT NULL THEN CONCAT(' battery:', battery_level, '%') ELSE '' END,
                            CASE WHEN temperature IS NOT NULL THEN CONCAT(' temp:', ROUND(temperature,1), 'C') ELSE '' END,
                            CASE WHEN latitude IS NOT NULL THEN CONCAT(' location:', ROUND(latitude,4), ',', ROUND(longitude,4)) ELSE '' END,
                            CASE WHEN rx_snr IS NOT NULL THEN CONCAT(' SNR:', ROUND(rx_snr,1), 'dB') ELSE '' END
                        ),
                        '. '
                    ) FROM (
                        SELECT * FROM DEMO.DEMO.MESHTASTIC_DATA
                        WHERE ingested_at >= {time_filter}
                        ORDER BY ingested_at DESC
                        LIMIT 50
                    ))
                ) as summary
                """
                with st.spinner("Generating summary..."):
                    summary_result = run_query(summary_query)
                    if not summary_result.empty:
                        st.success("**AI-Generated Summary:**")
                        st.markdown(summary_result['SUMMARY'].iloc[0])
            except Exception as e:
                st.error(f"Summary generation error: {e}")
        
        st.divider()
        
        st.markdown("#### 💬 Custom AI Query")
        custom_prompt = st.text_area(
            "Enter your question about the mesh network data:",
            placeholder="e.g., What patterns do you see in the device battery levels over time?",
            height=80
        )
        
        if st.button("Ask AI") and custom_prompt:
            try:
                custom_ai_query = f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'mistral-large2',
                    CONCAT(
                        'You are analyzing IoT mesh network data from Meshtastic devices. ',
                        'The data includes GPS positions, battery levels, temperature, humidity, and signal quality. ',
                        'Based on this context, answer: {custom_prompt.replace("'", "''")} ',
                        'Recent data summary: ',
                        (SELECT CONCAT(
                            'Devices: ', COUNT(DISTINCT from_id),
                            ', Total packets: ', COUNT(*),
                            ', Avg battery: ', ROUND(AVG(battery_level)),
                            '%, Avg temp: ', ROUND(AVG(temperature), 1),
                            'C, Avg SNR: ', ROUND(AVG(rx_snr), 1), 'dB'
                        ) FROM DEMO.DEMO.MESHTASTIC_DATA WHERE ingested_at >= {time_filter})
                    )
                ) as response
                """
                with st.spinner("AI is thinking..."):
                    response = run_query(custom_ai_query)
                    if not response.empty:
                        st.success("**AI Response:**")
                        st.markdown(response['RESPONSE'].iloc[0])
            except Exception as e:
                st.error(f"AI query error: {e}")
        
        with st.expander("Available Cortex AI Functions"):
            st.markdown("""
            | Function | Description |
            |----------|-------------|
            | `SNOWFLAKE.CORTEX.COMPLETE` | Generate text using LLMs (mistral-large2, llama3.1-70b, etc.) |
            | `SNOWFLAKE.CORTEX.SENTIMENT` | Analyze sentiment of text (-1 to 1 scale) |
            | `SNOWFLAKE.CORTEX.SUMMARIZE` | Generate summaries of text content |
            | `SNOWFLAKE.CORTEX.TRANSLATE` | Translate text between languages |
            | `SNOWFLAKE.CORTEX.CLASSIFY_TEXT` | Classify text into custom categories |
            | `SNOWFLAKE.CORTEX.EXTRACT_ANSWER` | Extract answers from text based on questions |
            
            **Note:** Cortex AI functions require appropriate region and account access.
            """)
    
    with tab_raw:
        st.subheader("Raw Packet Data")
        st.markdown("Browse and export raw packet data from the mesh network")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            packet_filter = st.selectbox(
                "Packet Type",
                ["All", "position", "telemetry", "text", "nodeinfo", "raw"]
            )
        with col2:
            node_filter = st.text_input("Node ID Filter", placeholder="e.g., !b9d44b14")
        with col3:
            limit = st.slider("Records", 10, 500, 100)
        
        try:
            where_clauses = [f"ingested_at >= {time_filter}"]
            if packet_filter != "All":
                where_clauses.append(f"packet_type = '{packet_filter}'")
            if node_filter:
                where_clauses.append(f"from_id ILIKE '%{node_filter}%'")
            
            where_str = " AND ".join(where_clauses)
            
            raw_query = f"""
            SELECT 
                ingested_at,
                packet_type,
                from_id,
                to_id,
                latitude,
                longitude,
                altitude,
                battery_level,
                voltage,
                temperature,
                relative_humidity,
                rx_snr,
                rx_rssi,
                text_message
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE {where_str}
            ORDER BY ingested_at DESC
            LIMIT {limit}
            """
            raw_df = run_query(raw_query)
            
            st.dataframe(raw_df, use_container_width=True, height=400)
            
            col1, col2 = st.columns(2)
            with col1:
                csv = raw_df.to_csv(index=False)
                st.download_button(
                    "Download CSV",
                    csv,
                    f"meshtastic_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv"
                )
            with col2:
                json_data = raw_df.to_json(orient='records', date_format='iso')
                st.download_button(
                    "Download JSON",
                    json_data,
                    f"meshtastic_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    "application/json"
                )
                
        except Exception as e:
            st.error(f"Error loading data: {e}")
    
    with tab_slack:
        st.subheader("Slack Integration")
        st.markdown("Send alerts and device updates to Slack channels")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Manual Alert")
            alert_node = st.text_input("Device ID", placeholder="!b9d44b14")
            alert_type = st.selectbox(
                "Alert Type",
                ["position_update", "telemetry", "low_battery", "offline", "custom"]
            )
            custom_message = st.text_area("Custom Message (optional)", height=100)
            
            if st.button("Send Alert to Slack", type="primary"):
                webhook = st.session_state.get("slack_webhook", "")
                channel = st.session_state.get("slack_channel", "")
                if webhook:
                    if custom_message:
                        msg = f"📡 *Meshtastic Manual Alert*\nDevice: `{alert_node}`\n{custom_message}"
                    else:
                        msg = format_slack_alert(
                            alert_node or "unknown",
                            alert_type,
                            {"Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Source": "Manual Dashboard Alert"}
                        )
                    if send_slack_message(webhook, msg, channel):
                        st.success("Alert sent to Slack!")
                    else:
                        st.error("Failed to send alert")
                else:
                    st.warning("Configure Slack webhook in sidebar first")
        
        with col2:
            st.markdown("#### Latest Device Summary")
            try:
                summary_query = f"""
                SELECT 
                    from_id,
                    MAX(battery_level) as battery,
                    MAX(latitude) as lat,
                    MAX(longitude) as lon,
                    MAX(temperature) as temp,
                    MAX(ingested_at) as last_seen
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
                GROUP BY from_id
                """
                summary = run_query(summary_query)
                
                if not summary.empty:
                    selected_device = st.selectbox(
                        "Select device to share",
                        summary['FROM_ID'].tolist()
                    )
                    
                    if st.button("Share Device Status to Slack"):
                        webhook = st.session_state.get("slack_webhook", "")
                        channel = st.session_state.get("slack_channel", "")
                        if webhook:
                            device_row = summary[summary['FROM_ID'] == selected_device].iloc[0]
                            data = {
                                "Battery": f"{device_row['BATTERY']}%" if device_row['BATTERY'] else "N/A",
                                "Location": f"{device_row['LAT']:.4f}, {device_row['LON']:.4f}" if device_row['LAT'] else "N/A",
                                "Temperature": f"{device_row['TEMP']:.1f}°C" if device_row['TEMP'] else "N/A",
                                "Last Seen": str(device_row['LAST_SEEN'])[:19]
                            }
                            msg = format_slack_alert(selected_device, "telemetry", data)
                            if send_slack_message(webhook, msg, channel):
                                st.success("Device status shared!")
                        else:
                            st.warning("Configure Slack webhook in sidebar first")
                    
                    st.dataframe(summary, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"Error loading summary: {e}")
        
        st.divider()
        st.markdown("#### Slack Setup Instructions")
        st.markdown("""
        1. Go to [Slack API Apps](https://api.slack.com/apps)
        2. Create a new app or select existing
        3. Enable **Incoming Webhooks**
        4. Create a webhook URL for your channel
        5. Paste the webhook URL in the sidebar
        """)
    
    st.divider()
    
    with st.expander("System Information"):
        st.markdown("""
        **Meshtastic Mesh Network Monitoring System**
        
        | Component | Details |
        |-----------|---------|
        | Device | SenseCAP Card Tracker T1000-E |
        | Connection | BLE (Bluetooth Low Energy) / Serial |
        | Data Pipeline | Snowpipe Streaming v2 REST API |
        | Storage | Snowflake (DEMO.DEMO.MESHTASTIC_DATA) |
        | Authentication | PAT (Programmatic Access Token) |
        
        **Captured Telemetry:**
        - GPS: latitude, longitude, altitude, speed, heading, satellites, DOP values
        - Device: battery level, voltage, uptime, channel utilization
        - Environmental: temperature, humidity, barometric pressure
        - Network: SNR, RSSI, hop count, channel
        
        **T1000-E Broadcast Intervals:**
        - Position: Every 15 minutes (configurable)
        - Environmental: Every 30 minutes (configurable)
        - Device telemetry: Every 30 seconds
        """)
    
    st.caption(f"Dashboard last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data via Snowpipe Streaming v2")


if __name__ == "__main__":
    main()
