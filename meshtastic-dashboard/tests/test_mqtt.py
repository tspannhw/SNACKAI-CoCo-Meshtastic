"""
Tests for MQTT consumer
"""
import pytest
import json
from unittest.mock import MagicMock, patch
from mqtt_consumer import MQTTConsumer


class TestMQTTConsumer:
    def test_init_defaults(self):
        consumer = MQTTConsumer()
        assert consumer.broker == "mqtt.meshtastic.org"
        assert consumer.port == 1883
        assert consumer.topic == "msh/+/2/json/#"
        assert consumer.running is False
    
    def test_init_custom_config(self):
        consumer = MQTTConsumer(
            broker="custom.broker.com",
            port=8883,
            topic="custom/#"
        )
        assert consumer.broker == "custom.broker.com"
        assert consumer.port == 8883
        assert consumer.topic == "custom/#"
    
    def test_stats_initialization(self):
        consumer = MQTTConsumer()
        assert consumer.stats['messages_received'] == 0
        assert consumer.stats['messages_valid'] == 0
        assert consumer.stats['errors'] == 0
    
    def test_get_stats(self):
        consumer = MQTTConsumer()
        stats = consumer.get_stats()
        assert 'messages_received' in stats
        assert 'connected' in stats
        assert 'queue_size' in stats


class TestMessageProcessing:
    @pytest.fixture
    def consumer(self):
        return MQTTConsumer()
    
    def test_valid_json_processing(self, consumer):
        mock_msg = MagicMock()
        mock_msg.topic = "msh/US/2/json/LongFast/!test"
        mock_msg.payload = json.dumps({
            "id": 12345,
            "from": 12345678,
            "type": "telemetry",
            "sender": "!test",
            "timestamp": 1234567890,
            "payload": {"battery_level": 91}
        }).encode()
        
        consumer._on_message(None, None, mock_msg)
        
        assert consumer.stats['messages_received'] == 1
        assert consumer.stats['messages_valid'] == 1
        assert 'US' in consumer.region_stats
    
    def test_invalid_json_handling(self, consumer):
        mock_msg = MagicMock()
        mock_msg.topic = "msh/US/2/json/test"
        mock_msg.payload = b"not valid json"
        
        consumer._on_message(None, None, mock_msg)
        
        assert consumer.stats['messages_received'] == 0
    
    def test_region_stats_tracking(self, consumer):
        for region in ['US', 'US', 'EU', 'EU', 'EU']:
            mock_msg = MagicMock()
            mock_msg.topic = f"msh/{region}/2/json/test/!test"
            mock_msg.payload = json.dumps({
                "id": 1,
                "from": 123,
                "type": "telemetry",
                "sender": "!test",
                "timestamp": 123
            }).encode()
            consumer._on_message(None, None, mock_msg)
        
        assert consumer.region_stats.get('US', 0) == 2
        assert consumer.region_stats.get('EU', 0) == 3
    
    def test_type_stats_tracking(self, consumer):
        for ptype in ['telemetry', 'position', 'telemetry']:
            mock_msg = MagicMock()
            mock_msg.topic = "msh/US/2/json/test/!test"
            mock_msg.payload = json.dumps({
                "id": 1,
                "from": 123,
                "type": ptype,
                "sender": "!test",
                "timestamp": 123
            }).encode()
            consumer._on_message(None, None, mock_msg)
        
        assert consumer.type_stats.get('telemetry', 0) == 2
        assert consumer.type_stats.get('position', 0) == 1


class TestConnectionHandling:
    def test_on_connect_success(self):
        consumer = MQTTConsumer()
        mock_client = MagicMock()
        
        consumer._on_connect(mock_client, None, None, 0)
        
        assert consumer.connected is True
        mock_client.subscribe.assert_called_once()
    
    def test_on_connect_failure(self):
        consumer = MQTTConsumer()
        mock_client = MagicMock()
        
        consumer._on_connect(mock_client, None, None, 5)
        
        assert consumer.connected is False
    
    def test_on_disconnect(self):
        consumer = MQTTConsumer()
        consumer.connected = True
        consumer.running = False
        
        consumer._on_disconnect(MagicMock(), None, 0)
        
        assert consumer.connected is False


class TestBatchProcessing:
    def test_process_empty_batch(self):
        consumer = MQTTConsumer()
        result = consumer._process_batch([])
        assert result == 0
    
    def test_process_valid_batch(self):
        consumer = MQTTConsumer()
        batch = [
            {'packet_type': 'telemetry', 'from_id': '!test1', 'latitude': 40.7, 'longitude': -74.0},
            {'packet_type': 'position', 'from_id': '!test2', 'latitude': 41.0, 'longitude': -73.0}
        ]
        result = consumer._process_batch(batch)
        assert result == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
