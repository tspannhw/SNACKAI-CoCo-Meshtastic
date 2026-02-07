#!/usr/bin/env python3
"""Test packet parsing and Snowpipe streaming with simulated Meshtastic packets."""

import json
from datetime import datetime, timezone
from meshtastic_interface import MeshtasticReceiver
from snowpipe_streaming_client import SnowpipeStreamingClient

def test_packet_parsing():
    """Test that packets are parsed correctly."""
    
    position_packet = {
        'from': 3117697812,
        'to': 4294967295,
        'fromId': '!b9d44b14',
        'toId': '^all',
        'channel': 0,
        'rxSnr': 10.5,
        'rxRssi': -65,
        'hopLimit': 3,
        'decoded': {
            'portnum': 3,  # POSITION_APP
            'position': {
                'latitudeI': 402915328,
                'longitudeI': -745275392,
                'altitude': 35,
                'time': 1738930000,
                'satsInView': 8,
                'PDOP': 150,
                'HDOP': 120,
                'groundSpeed': 5,
                'groundTrack': 180
            }
        }
    }
    
    telemetry_packet = {
        'from': 3117697812,
        'to': 4294967295,
        'fromId': '!b9d44b14',
        'toId': '^all',
        'channel': 0,
        'rxSnr': 9.0,
        'rxRssi': -70,
        'decoded': {
            'portnum': 67,  # TELEMETRY_APP
            'telemetry': {
                'time': 1738930100,
                'deviceMetrics': {
                    'batteryLevel': 85,
                    'voltage': 3.8,
                    'channelUtilization': 5.2,
                    'airUtilTx': 1.5,
                    'uptimeSeconds': 3600
                },
                'environmentMetrics': {
                    'temperature': 22.5,
                    'relativeHumidity': 45.0,
                    'barometricPressure': 101325.0
                }
            }
        }
    }
    
    receiver = MeshtasticReceiver(connection_type='test')
    
    print("=== Testing Position Packet ===")
    pos_msg = receiver._parse_position_packet(position_packet)
    print(f"packet_type: {pos_msg.get('packet_type')}")
    print(f"from_id: {pos_msg.get('from_id')}")
    print(f"latitude: {pos_msg.get('latitude')}")
    print(f"longitude: {pos_msg.get('longitude')}")
    print(f"altitude: {pos_msg.get('altitude')}")
    print(f"sats_in_view: {pos_msg.get('sats_in_view')}")
    print(f"ground_speed: {pos_msg.get('ground_speed')}")
    print(f"gps_timestamp: {pos_msg.get('gps_timestamp')}")
    
    assert pos_msg['packet_type'] == 'position', f"Expected 'position', got {pos_msg['packet_type']}"
    assert pos_msg['latitude'] == 40.2915328, f"Latitude mismatch: {pos_msg['latitude']}"
    assert pos_msg['longitude'] == -74.5275392, f"Longitude mismatch: {pos_msg['longitude']}"
    assert pos_msg['altitude'] == 35, f"Altitude mismatch: {pos_msg['altitude']}"
    print("Position packet: PASS\n")
    
    print("=== Testing Telemetry Packet ===")
    tel_msg = receiver._parse_telemetry_packet(telemetry_packet)
    print(f"packet_type: {tel_msg.get('packet_type')}")
    print(f"from_id: {tel_msg.get('from_id')}")
    print(f"battery_level: {tel_msg.get('battery_level')}")
    print(f"voltage: {tel_msg.get('voltage')}")
    print(f"temperature: {tel_msg.get('temperature')}")
    print(f"relative_humidity: {tel_msg.get('relative_humidity')}")
    print(f"barometric_pressure: {tel_msg.get('barometric_pressure')}")
    print(f"uptime_seconds: {tel_msg.get('uptime_seconds')}")
    
    assert tel_msg['packet_type'] == 'telemetry', f"Expected 'telemetry', got {tel_msg['packet_type']}"
    assert tel_msg['battery_level'] == 85, f"Battery mismatch: {tel_msg['battery_level']}"
    assert tel_msg['voltage'] == 3.8, f"Voltage mismatch: {tel_msg['voltage']}"
    assert tel_msg['temperature'] == 22.5, f"Temp mismatch: {tel_msg['temperature']}"
    print("Telemetry packet: PASS\n")
    
    print("=== Testing Portnum Detection ===")
    receiver.on_message_callback = lambda msg: print(f"  -> Callback: {msg.get('packet_type')} from {msg.get('from_id')}")
    
    print("Processing position packet (portnum=3):")
    receiver._on_receive(position_packet, None)
    
    print("Processing telemetry packet (portnum=67):")
    receiver._on_receive(telemetry_packet, None)
    
    print("\n=== ALL TESTS PASSED ===")


def test_streaming():
    """Test that data can be streamed to Snowflake."""
    print("\n=== Testing Snowpipe Streaming ===")
    
    client = SnowpipeStreamingClient('snowflake_config.json')
    
    test_rows = [
        {
            'ingested_at': datetime.now(timezone.utc).isoformat(),
            'packet_type': 'position',
            'from_id': '!test1234',
            'latitude': 40.2915328,
            'longitude': -74.5275392,
            'altitude': 35,
            'ground_speed': 5,
            'sats_in_view': 8,
            'gps_timestamp': 1738930000
        },
        {
            'ingested_at': datetime.now(timezone.utc).isoformat(),
            'packet_type': 'telemetry',
            'from_id': '!test1234',
            'battery_level': 85,
            'voltage': 3.8,
            'temperature': 22.5,
            'relative_humidity': 45.0,
            'barometric_pressure': 101325.0,
            'uptime_seconds': 3600
        }
    ]
    
    try:
        client.discover_ingest_host()
        client.open_channel()
        
        count = client.insert_rows(test_rows)
        print(f"Inserted {count} test rows")
        
        client.close_channel()
        print("Streaming test: PASS")
    except Exception as e:
        print(f"Streaming test: FAIL - {e}")


if __name__ == '__main__':
    test_packet_parsing()
    test_streaming()
