#!/usr/bin/env python3
"""
Meshtastic Dashboard - Streamlit App for Snowflake
Real-time visualization of Meshtastic mesh network data
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

st.set_page_config(
    page_title="Meshtastic Dashboard",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_snowflake_connection():
    from snowflake.snowpark.context import get_active_session
    return get_active_session()

def run_query(query: str) -> pd.DataFrame:
    session = get_snowflake_connection()
    return session.sql(query).to_pandas()

def main():
    st.title("📡 Meshtastic Mesh Network Dashboard")
    st.markdown("Real-time monitoring of LoRa mesh network nodes and telemetry")
    
    with st.sidebar:
        st.header("⚙️ Settings")
        
        time_range = st.selectbox(
            "Time Range",
            ["Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days", "All time"],
            index=2
        )
        
        time_filters = {
            "Last 1 hour": "DATEADD(hour, -1, CURRENT_TIMESTAMP())",
            "Last 6 hours": "DATEADD(hour, -6, CURRENT_TIMESTAMP())",
            "Last 24 hours": "DATEADD(hour, -24, CURRENT_TIMESTAMP())",
            "Last 7 days": "DATEADD(day, -7, CURRENT_TIMESTAMP())",
            "All time": "'1970-01-01'::TIMESTAMP_TZ"
        }
        time_filter = time_filters[time_range]
        
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
        if auto_refresh:
            st.rerun()
        
        st.divider()
        st.markdown("### 📊 Data Source")
        st.code("DEMO.DEMO.MESHTASTIC_DATA")
    
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        stats_query = f"""
        SELECT 
            COUNT(*) as total_packets,
            COUNT(DISTINCT from_id) as unique_nodes,
            COUNT(CASE WHEN packet_type = 'position' THEN 1 END) as position_packets,
            COUNT(CASE WHEN packet_type = 'telemetry' THEN 1 END) as telemetry_packets,
            AVG(rx_snr) as avg_snr,
            AVG(battery_level) as avg_battery
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE ingested_at >= {time_filter}
        """
        stats = run_query(stats_query)
        
        with col1:
            st.metric("Total Packets", f"{stats['TOTAL_PACKETS'].iloc[0]:,}")
        with col2:
            st.metric("Active Nodes", int(stats['UNIQUE_NODES'].iloc[0]))
        with col3:
            avg_snr = stats['AVG_SNR'].iloc[0]
            st.metric("Avg SNR", f"{avg_snr:.1f} dB" if avg_snr else "N/A")
        with col4:
            avg_bat = stats['AVG_BATTERY'].iloc[0]
            st.metric("Avg Battery", f"{avg_bat:.0f}%" if avg_bat else "N/A")
    except Exception as e:
        st.warning(f"Could not load stats: {e}")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🗺️ Node Map", "📈 Telemetry", "💬 Messages", "📊 Analytics", "🔍 Raw Data"
    ])
    
    with tab1:
        st.subheader("Node Locations")
        
        try:
            positions_query = f"""
            SELECT 
                from_id,
                latitude,
                longitude,
                altitude,
                MAX(ingested_at) as last_seen,
                AVG(rx_snr) as avg_snr,
                MAX(battery_level) as battery
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE packet_type = 'position'
              AND latitude IS NOT NULL 
              AND longitude IS NOT NULL
              AND ingested_at >= {time_filter}
            GROUP BY from_id, latitude, longitude, altitude
            ORDER BY last_seen DESC
            LIMIT 100
            """
            positions = run_query(positions_query)
            
            if not positions.empty:
                fig = px.scatter_mapbox(
                    positions,
                    lat="LATITUDE",
                    lon="LONGITUDE",
                    hover_name="FROM_ID",
                    hover_data=["ALTITUDE", "BATTERY", "AVG_SNR"],
                    color="BATTERY",
                    size_max=15,
                    zoom=10,
                    color_continuous_scale="RdYlGn"
                )
                fig.update_layout(
                    mapbox_style="open-street-map",
                    height=500,
                    margin={"r": 0, "t": 0, "l": 0, "b": 0}
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No position data available for the selected time range")
        except Exception as e:
            st.error(f"Error loading map data: {e}")
    
    with tab2:
        st.subheader("Device Telemetry")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🔋 Battery Levels")
            try:
                battery_query = f"""
                SELECT 
                    from_id,
                    ingested_at,
                    battery_level,
                    voltage
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE packet_type = 'telemetry'
                  AND battery_level IS NOT NULL
                  AND ingested_at >= {time_filter}
                ORDER BY ingested_at DESC
                LIMIT 500
                """
                battery_df = run_query(battery_query)
                
                if not battery_df.empty:
                    fig = px.line(
                        battery_df,
                        x="INGESTED_AT",
                        y="BATTERY_LEVEL",
                        color="FROM_ID",
                        title="Battery Level Over Time"
                    )
                    fig.update_layout(yaxis_range=[0, 100])
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No battery data available")
            except Exception as e:
                st.error(f"Error: {e}")
        
        with col2:
            st.markdown("#### 🌡️ Environmental Sensors")
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
                  AND (temperature IS NOT NULL OR relative_humidity IS NOT NULL)
                  AND ingested_at >= {time_filter}
                ORDER BY ingested_at DESC
                LIMIT 500
                """
                env_df = run_query(env_query)
                
                if not env_df.empty and 'TEMPERATURE' in env_df.columns:
                    fig = px.line(
                        env_df,
                        x="INGESTED_AT",
                        y="TEMPERATURE",
                        color="FROM_ID",
                        title="Temperature Over Time (°C)"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No environmental data available")
            except Exception as e:
                st.error(f"Error: {e}")
        
        st.markdown("#### 📶 Signal Quality")
        try:
            signal_query = f"""
            SELECT 
                from_id,
                ingested_at,
                rx_snr,
                rx_rssi
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE rx_snr IS NOT NULL
              AND ingested_at >= {time_filter}
            ORDER BY ingested_at DESC
            LIMIT 500
            """
            signal_df = run_query(signal_query)
            
            if not signal_df.empty:
                fig = go.Figure()
                for node in signal_df['FROM_ID'].unique()[:5]:
                    node_data = signal_df[signal_df['FROM_ID'] == node]
                    fig.add_trace(go.Scatter(
                        x=node_data['INGESTED_AT'],
                        y=node_data['RX_SNR'],
                        mode='lines+markers',
                        name=str(node)[:12]
                    ))
                fig.update_layout(
                    title="Signal-to-Noise Ratio (SNR)",
                    xaxis_title="Time",
                    yaxis_title="SNR (dB)"
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab3:
        st.subheader("💬 Mesh Messages")
        
        try:
            messages_query = f"""
            SELECT 
                ingested_at,
                from_id,
                to_id,
                text_message,
                channel,
                rx_snr
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE packet_type = 'text'
              AND text_message IS NOT NULL
              AND ingested_at >= {time_filter}
            ORDER BY ingested_at DESC
            LIMIT 100
            """
            messages = run_query(messages_query)
            
            if not messages.empty:
                for _, msg in messages.iterrows():
                    with st.container():
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"**{msg['FROM_ID']}** → {msg['TO_ID']}")
                            st.markdown(f"> {msg['TEXT_MESSAGE']}")
                        with col2:
                            st.caption(f"Ch: {msg['CHANNEL']}")
                            st.caption(f"SNR: {msg['RX_SNR']}")
                            st.caption(str(msg['INGESTED_AT'])[:19])
                        st.divider()
            else:
                st.info("No messages in the selected time range")
        except Exception as e:
            st.error(f"Error loading messages: {e}")
    
    with tab4:
        st.subheader("📊 Network Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Packets by Type")
            try:
                type_query = f"""
                SELECT 
                    packet_type,
                    COUNT(*) as count
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE ingested_at >= {time_filter}
                GROUP BY packet_type
                """
                type_df = run_query(type_query)
                
                if not type_df.empty:
                    fig = px.pie(type_df, values='COUNT', names='PACKET_TYPE', hole=0.4)
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")
        
        with col2:
            st.markdown("#### Hourly Traffic")
            try:
                hourly_query = f"""
                SELECT 
                    DATE_TRUNC('hour', ingested_at) as hour,
                    COUNT(*) as packets,
                    COUNT(DISTINCT from_id) as nodes
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE ingested_at >= {time_filter}
                GROUP BY DATE_TRUNC('hour', ingested_at)
                ORDER BY hour
                """
                hourly_df = run_query(hourly_query)
                
                if not hourly_df.empty:
                    fig = px.bar(hourly_df, x='HOUR', y='PACKETS', 
                                 title="Packets per Hour")
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")
        
        st.markdown("#### Active Nodes Summary")
        try:
            nodes_query = f"""
            SELECT 
                from_id as "Node ID",
                COUNT(*) as "Packets",
                MAX(ingested_at) as "Last Seen",
                ROUND(AVG(rx_snr), 1) as "Avg SNR",
                MAX(battery_level) as "Battery %",
                MAX(latitude) as "Latitude",
                MAX(longitude) as "Longitude"
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE from_id IS NOT NULL
              AND ingested_at >= {time_filter}
            GROUP BY from_id
            ORDER BY "Packets" DESC
            LIMIT 20
            """
            nodes_df = run_query(nodes_query)
            st.dataframe(nodes_df, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab5:
        st.subheader("🔍 Raw Packet Data")
        
        packet_filter = st.selectbox(
            "Filter by packet type",
            ["All", "position", "telemetry", "text", "raw"]
        )
        
        limit = st.slider("Number of records", 10, 500, 100)
        
        try:
            where_clause = f"AND packet_type = '{packet_filter}'" if packet_filter != "All" else ""
            
            raw_query = f"""
            SELECT *
            FROM DEMO.DEMO.MESHTASTIC_DATA
            WHERE ingested_at >= {time_filter}
            {where_clause}
            ORDER BY ingested_at DESC
            LIMIT {limit}
            """
            raw_df = run_query(raw_query)
            
            st.dataframe(raw_df, use_container_width=True)
            
            csv = raw_df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                csv,
                "meshtastic_data.csv",
                "text/csv"
            )
        except Exception as e:
            st.error(f"Error loading data: {e}")
    
    st.divider()
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
