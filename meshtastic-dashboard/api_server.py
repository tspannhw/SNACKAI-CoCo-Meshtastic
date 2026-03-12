#!/usr/bin/env python3
"""
Flask API Server for Meshtastic Dashboard

Provides REST API endpoints for querying Meshtastic data from Snowflake,
including semantic view queries, health checks, and comprehensive logging.
"""
import os
import sys
import json
import subprocess
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from functools import wraps
import time

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from validation import (
    validate_snowflake_row,
    create_health_check,
    HealthCheck
)

LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(LOG_DIR, 'api_server.log'),
            maxBytes=10*1024*1024,
            backupCount=5
        ),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('api_server')

app = Flask(__name__, static_folder='build', static_url_path='')
CORS(app)

CONNECTION_NAME = os.getenv('SNOWFLAKE_CONNECTION_NAME', 'tspann1')
DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'DEMO')
SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'DEMO')

request_stats = {
    'total_requests': 0,
    'successful_requests': 0,
    'failed_requests': 0,
    'start_time': datetime.now(timezone.utc)
}


def log_request(f):
    """Decorator to log API requests"""
    @wraps(f)
    def decorated(*args, **kwargs):
        start = time.time()
        request_stats['total_requests'] += 1
        
        try:
            result = f(*args, **kwargs)
            duration = (time.time() - start) * 1000
            request_stats['successful_requests'] += 1
            logger.info(f"Request: {request.method} {request.path} - 200 - {duration:.1f}ms")
            return result
        except Exception as e:
            duration = (time.time() - start) * 1000
            request_stats['failed_requests'] += 1
            logger.error(f"Request: {request.method} {request.path} - 500 - {duration:.1f}ms - {e}")
            raise
    return decorated


def run_snowflake_query(sql: str):
    """Execute SQL via Snowflake connection"""
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]
        cursor.close()
        conn.close()
        
        for row in result:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
        
        return result
    except Exception as e:
        logger.error(f"Snowflake query error: {e}")
        return []


@app.route('/api/health')
@log_request
def health():
    """Health check endpoint"""
    snowflake_ok = True
    mqtt_ok = True
    
    try:
        result = run_snowflake_query("SELECT 1 AS check")
        snowflake_ok = len(result) > 0
    except:
        snowflake_ok = False
    
    uptime = (datetime.now(timezone.utc) - request_stats['start_time']).total_seconds()
    
    health = create_health_check(
        snowflake_ok=snowflake_ok,
        mqtt_ok=mqtt_ok,
        api_ok=True,
        details={
            'uptime_seconds': uptime,
            'total_requests': request_stats['total_requests'],
            'success_rate': request_stats['successful_requests'] / max(1, request_stats['total_requests']),
            'connection': CONNECTION_NAME
        }
    )
    
    return jsonify({
        'status': health.status,
        'timestamp': health.timestamp.isoformat(),
        'checks': health.checks,
        'details': health.details
    })


@app.route('/api/meshtastic')
@log_request
def get_meshtastic():
    """Get recent Meshtastic packets"""
    limit = request.args.get('limit', 50, type=int)
    limit = min(limit, 500)
    
    sql = f"""
        SELECT 
            INGESTED_AT, PACKET_TYPE, FROM_ID, BATTERY_LEVEL, VOLTAGE,
            TEMPERATURE, TEMPERATURE_F, LATITUDE, LONGITUDE, ALTITUDE,
            RX_SNR, RX_RSSI, CHANNEL_UTILIZATION, UPTIME_SECONDS,
            TEXT_MESSAGE, RELATIVE_HUMIDITY, BAROMETRIC_PRESSURE
        FROM {DATABASE}.{SCHEMA}.MESHTASTIC_DATA 
        ORDER BY INGESTED_AT DESC 
        LIMIT {limit}
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'data': data,
        'count': len(data),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/stats')
@log_request
def get_stats():
    """Get aggregated statistics"""
    hours = request.args.get('hours', 24, type=int)
    
    sql = f"""
        SELECT 
            COUNT(*) as TOTAL_MESSAGES,
            COUNT(DISTINCT FROM_ID) as UNIQUE_DEVICES,
            MAX(BATTERY_LEVEL) as MAX_BATTERY,
            AVG(TEMPERATURE) as AVG_TEMP,
            AVG(RX_SNR) as AVG_SNR,
            MAX(INGESTED_AT) as LAST_MESSAGE
        FROM {DATABASE}.{SCHEMA}.MESHTASTIC_DATA
        WHERE INGESTED_AT > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'stats': data[0] if data else {},
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/positions')
@log_request
def get_positions():
    """Get GPS position data using MESHTASTIC_POSITIONS view"""
    limit = request.args.get('limit', 100, type=int)
    
    sql = f"""
        SELECT * FROM {DATABASE}.{SCHEMA}.MESHTASTIC_POSITIONS
        ORDER BY ingested_at DESC
        LIMIT {limit}
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'data': data,
        'count': len(data),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/telemetry')
@log_request
def get_telemetry():
    """Get telemetry data using MESHTASTIC_TELEMETRY view"""
    limit = request.args.get('limit', 100, type=int)
    
    sql = f"""
        SELECT * FROM {DATABASE}.{SCHEMA}.MESHTASTIC_TELEMETRY
        ORDER BY ingested_at DESC
        LIMIT {limit}
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'data': data,
        'count': len(data),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/messages')
@log_request
def get_messages():
    """Get text messages using MESHTASTIC_MESSAGES view"""
    limit = request.args.get('limit', 50, type=int)
    
    sql = f"""
        SELECT * FROM {DATABASE}.{SCHEMA}.MESHTASTIC_MESSAGES
        ORDER BY ingested_at DESC
        LIMIT {limit}
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'data': data,
        'count': len(data),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/nodes')
@log_request
def get_nodes():
    """Get active nodes using MESHTASTIC_ACTIVE_NODES view"""
    sql = f"""
        SELECT * FROM {DATABASE}.{SCHEMA}.MESHTASTIC_ACTIVE_NODES
        ORDER BY last_seen DESC
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'data': data,
        'count': len(data),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/hourly')
@log_request
def get_hourly_stats():
    """Get hourly statistics using MESHTASTIC_HOURLY_STATS view"""
    hours = request.args.get('hours', 24, type=int)
    
    sql = f"""
        SELECT * FROM {DATABASE}.{SCHEMA}.MESHTASTIC_HOURLY_STATS
        WHERE hour > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        ORDER BY hour DESC
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'data': data,
        'count': len(data),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/weather')
@log_request
def get_weather():
    """Get weather/environmental data using MESHTASTIC_WEATHER view"""
    limit = request.args.get('limit', 100, type=int)
    
    sql = f"""
        SELECT * FROM {DATABASE}.{SCHEMA}.MESHTASTIC_WEATHER
        ORDER BY ingested_at DESC
        LIMIT {limit}
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'data': data,
        'count': len(data),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/semantic/metrics')
@log_request
def get_semantic_metrics():
    """Query semantic view for key metrics"""
    sql = f"""
        SELECT 
            TOTAL_PACKETS,
            UNIQUE_NODES,
            MESSAGE_COUNT,
            POSITION_COUNT,
            TELEMETRY_COUNT,
            AVG_BATTERY,
            AVG_TEMPERATURE,
            AVG_SNR,
            LOW_BATTERY_DEVICES,
            POOR_SIGNAL_NODES,
            LAST_PACKET_TIME
        FROM {DATABASE}.{SCHEMA}.MESHTASTIC_SEMANTIC_VIEW
        ORDER BY LAST_PACKET_TIME DESC
        LIMIT 1
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'metrics': data[0] if data else {},
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/semantic/devices')
@log_request
def get_semantic_devices():
    """Query semantic view for device health"""
    sql = f"""
        SELECT 
            FROM_ID,
            PACKET_TYPE,
            BATTERY_LEVEL,
            VOLTAGE,
            TEMPERATURE,
            RX_SNR,
            UPTIME_SECONDS,
            INGESTED_AT
        FROM {DATABASE}.{SCHEMA}.MESHTASTIC_SEMANTIC_VIEW
        WHERE BATTERY_LEVEL IS NOT NULL
        ORDER BY INGESTED_AT DESC
        LIMIT 50
    """
    
    data = run_snowflake_query(sql)
    return jsonify({
        'data': data,
        'count': len(data),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/semantic/alerts')
@log_request  
def get_semantic_alerts():
    """Get alerts from semantic view metrics"""
    sql = f"""
        SELECT 
            FROM_ID as device_id,
            BATTERY_LEVEL as battery,
            RX_SNR as snr,
            INGESTED_AT as last_seen,
            CASE 
                WHEN BATTERY_LEVEL <= 20 THEN 'LOW_BATTERY'
                WHEN RX_SNR < -18 THEN 'POOR_SIGNAL'
                ELSE NULL
            END as alert_type
        FROM {DATABASE}.{SCHEMA}.MESHTASTIC_SEMANTIC_VIEW
        WHERE (BATTERY_LEVEL <= 20 OR RX_SNR < -18)
          AND INGESTED_AT > DATEADD(hour, -1, CURRENT_TIMESTAMP())
        ORDER BY INGESTED_AT DESC
        LIMIT 20
    """
    
    data = run_snowflake_query(sql)
    alerts = [d for d in data if d.get('alert_type')]
    return jsonify({
        'alerts': alerts,
        'count': len(alerts),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/')
def serve():
    """Serve React dashboard"""
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/<path:path>')
def serve_static(path):
    """Serve static files"""
    return send_from_directory(app.static_folder, path)


@app.errorhandler(404)
def not_found(e):
    return send_from_directory(app.static_folder, 'index.html')


@app.errorhandler(500)
def server_error(e):
    logger.error(f"Server error: {e}")
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 5000))
    debug = os.getenv('DEBUG', 'true').lower() == 'true'
    
    logger.info("=" * 60)
    logger.info("🎮 Pac-Man Meshtastic Dashboard API")
    logger.info(f"🕹️  http://localhost:{port}")
    logger.info(f"📡 Snowflake connection: {CONNECTION_NAME}")
    logger.info(f"📊 Database: {DATABASE}.{SCHEMA}")
    logger.info("=" * 60)
    
    app.run(host='0.0.0.0', port=port, debug=debug)
