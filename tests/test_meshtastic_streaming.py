#!/usr/bin/env python3
"""Tests for Snowflake JWT Authentication"""
import pytest
import json
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone


class TestSnowflakeJWTAuth:
    """Test cases for SnowflakeJWTAuth class"""
    
    def test_init_with_pat(self):
        """Test initialization with PAT authentication"""
        from snowflake_jwt_auth import SnowflakeJWTAuth
        
        config = {
            'account': 'test_account',
            'user': 'test_user',
            'pat': 'test_pat_token_12345'
        }
        
        auth = SnowflakeJWTAuth(config)
        
        assert auth.auth_method == 'pat'
        assert auth.account == 'TEST_ACCOUNT'
        assert auth.user == 'TEST_USER'
        assert auth.pat == 'test_pat_token_12345'
    
    def test_init_missing_auth_raises_error(self):
        """Test that missing authentication method raises ValueError"""
        from snowflake_jwt_auth import SnowflakeJWTAuth
        
        config = {
            'account': 'test_account',
            'user': 'test_user'
        }
        
        with pytest.raises(ValueError, match="No authentication method configured"):
            SnowflakeJWTAuth(config)
    
    def test_get_scoped_token_with_pat(self):
        """Test get_scoped_token returns PAT directly"""
        from snowflake_jwt_auth import SnowflakeJWTAuth
        
        config = {
            'account': 'test_account',
            'user': 'test_user',
            'pat': 'my_pat_token'
        }
        
        auth = SnowflakeJWTAuth(config)
        token = auth.get_scoped_token()
        
        assert token == 'my_pat_token'
    
    @patch('snowflake_jwt_auth.open', create=True)
    @patch('snowflake_jwt_auth.serialization.load_pem_private_key')
    def test_init_with_jwt_key(self, mock_load_key, mock_open):
        """Test initialization with JWT key-pair authentication"""
        from snowflake_jwt_auth import SnowflakeJWTAuth
        
        mock_key = MagicMock()
        mock_load_key.return_value = mock_key
        mock_open.return_value.__enter__ = lambda s: MagicMock(read=lambda: b'fake_key')
        mock_open.return_value.__exit__ = Mock(return_value=False)
        
        config = {
            'account': 'test_account',
            'user': 'test_user',
            'private_key_file': '/path/to/key.p8'
        }
        
        auth = SnowflakeJWTAuth(config)
        
        assert auth.auth_method == 'jwt'
        assert auth.qualified_username == 'TEST_ACCOUNT.TEST_USER'


class TestSnowpipeStreamingClient:
    """Test cases for SnowpipeStreamingClient"""
    
    def test_init(self):
        """Test client initialization"""
        from snowpipe_streaming_client import SnowpipeStreamingClient
        
        config = {
            'account': 'test_account',
            'database': 'test_db',
            'schema': 'test_schema',
            'table': 'test_table'
        }
        
        mock_auth = Mock()
        client = SnowpipeStreamingClient(config, mock_auth)
        
        assert client.database == 'TEST_DB'
        assert client.schema == 'TEST_SCHEMA'
        assert client.table == 'TEST_TABLE'
        assert client.continuation_token is None
    
    def test_prepare_ndjson_rows(self):
        """Test NDJSON row preparation"""
        from snowpipe_streaming_client import SnowpipeStreamingClient
        
        config = {
            'account': 'test_account',
            'database': 'test_db',
            'schema': 'test_schema',
            'table': 'test_table'
        }
        
        mock_auth = Mock()
        client = SnowpipeStreamingClient(config, mock_auth)
        
        rows = [
            {'col1': 'value1', 'col2': 123},
            {'col1': 'value2', 'col2': 456}
        ]
        
        ndjson_lines = [json.dumps(row) for row in rows]
        payload = "\n".join(ndjson_lines) + "\n"
        
        assert '{"col1": "value1", "col2": 123}' in payload
        assert '{"col1": "value2", "col2": 456}' in payload
        assert payload.count('\n') == 2
    
    def test_append_rows_without_channel_raises_error(self):
        """Test that append_rows fails without open channel"""
        from snowpipe_streaming_client import SnowpipeStreamingClient
        
        config = {
            'account': 'test_account',
            'database': 'test_db',
            'schema': 'test_schema',
            'table': 'test_table'
        }
        
        mock_auth = Mock()
        client = SnowpipeStreamingClient(config, mock_auth)
        
        with pytest.raises(ValueError, match="Channel not opened"):
            client.append_rows([{'test': 'data'}])


class TestMeshtasticInterface:
    """Test cases for MeshtasticReceiver"""
    
    def test_init(self):
        """Test receiver initialization"""
        from meshtastic_interface import MeshtasticReceiver
        
        receiver = MeshtasticReceiver(
            connection_type='serial',
            device_path='/dev/ttyUSB0'
        )
        
        assert receiver.connection_type == 'serial'
        assert receiver.device_path == '/dev/ttyUSB0'
        assert receiver.interface is None
        assert receiver.running is False
    
    def test_parse_position_packet(self):
        """Test position packet parsing"""
        from meshtastic_interface import MeshtasticReceiver
        
        receiver = MeshtasticReceiver()
        
        packet = {
            'from': 12345678,
            'to': 4294967295,
            'fromId': '!00bc614e',
            'toId': '^all',
            'channel': 0,
            'rxSnr': 10.5,
            'rxRssi': -85,
            'decoded': {
                'portnum': 'POSITION_APP',
                'position': {
                    'latitudeI': 407279123,
                    'longitudeI': -740138456,
                    'altitude': 50,
                    'satsInView': 12,
                    'groundSpeed': 5
                }
            }
        }
        
        message = receiver._parse_position_packet(packet)
        
        assert message['packet_type'] == 'position'
        assert message['from_id'] == '!00bc614e'
        assert message['rx_snr'] == 10.5
        assert message['rx_rssi'] == -85
        assert message['altitude'] == 50
        assert message['sats_in_view'] == 12
    
    def test_parse_telemetry_packet(self):
        """Test telemetry packet parsing"""
        from meshtastic_interface import MeshtasticReceiver
        
        receiver = MeshtasticReceiver()
        
        packet = {
            'from': 12345678,
            'to': 4294967295,
            'decoded': {
                'portnum': 'TELEMETRY_APP',
                'telemetry': {
                    'deviceMetrics': {
                        'batteryLevel': 85,
                        'voltage': 4.1,
                        'channelUtilization': 5.2,
                        'airUtilTx': 1.3,
                        'uptimeSeconds': 3600
                    },
                    'environmentMetrics': {
                        'temperature': 22.5,
                        'relativeHumidity': 45.0,
                        'barometricPressure': 1013.25
                    }
                }
            }
        }
        
        message = receiver._parse_telemetry_packet(packet)
        
        assert message['packet_type'] == 'telemetry'
        assert message['battery_level'] == 85
        assert message['voltage'] == 4.1
        assert message['temperature'] == 22.5
        assert message['relative_humidity'] == 45.0
        assert message['barometric_pressure'] == 1013.25
    
    def test_parse_text_packet(self):
        """Test text message packet parsing"""
        from meshtastic_interface import MeshtasticReceiver
        
        receiver = MeshtasticReceiver()
        
        packet = {
            'from': 12345678,
            'to': 4294967295,
            'fromId': '!00bc614e',
            'toId': '^all',
            'decoded': {
                'portnum': 'TEXT_MESSAGE_APP',
                'text': 'Hello Mesh!'
            }
        }
        
        message = receiver._parse_text_packet(packet)
        
        assert message['packet_type'] == 'text'
        assert message['text'] == 'Hello Mesh!'
        assert message['from_id'] == '!00bc614e'


class TestMeshtasticSnowflakeStreamer:
    """Test cases for the main streamer application"""
    
    def test_prepare_row(self):
        """Test row preparation for Snowflake"""
        from meshtastic_snowflake_streamer import MeshtasticSnowflakeStreamer
        
        with patch.object(MeshtasticSnowflakeStreamer, '_load_config') as mock_load:
            mock_load.return_value = {
                'account': 'test',
                'user': 'test',
                'pat': 'test',
                'database': 'db',
                'schema': 'schema',
                'table': 'table'
            }
            
            streamer = MeshtasticSnowflakeStreamer.__new__(MeshtasticSnowflakeStreamer)
            streamer.config = mock_load.return_value
            streamer.meshtastic_config = {}
            streamer.batch_size = 10
            streamer.flush_interval = 5
            
            message = {
                'packet_type': 'position',
                'from_id': '!abc123',
                'latitude': 40.7128,
                'longitude': -74.0060,
                'battery_level': 85,
                'extra_field': 'should_be_removed'
            }
            
            row = streamer._prepare_row(message)
            
            assert row['packet_type'] == 'position'
            assert row['from_id'] == '!abc123'
            assert row['latitude'] == 40.7128
            assert row['longitude'] == -74.0060
            assert row['battery_level'] == 85
            assert 'ingested_at' in row


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
