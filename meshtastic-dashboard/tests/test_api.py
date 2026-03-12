"""
Tests for API endpoints
"""
import pytest
import json
from unittest.mock import patch, MagicMock

try:
    from api_server import app
except ImportError:
    app = None


@pytest.fixture
def client():
    """Create test client"""
    if app is None:
        pytest.skip("api_server not available")
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get('/api/health')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'status' in data
        assert 'timestamp' in data


class TestMeshtasticEndpoint:
    @patch('api_server.run_snowflake_query')
    def test_meshtastic_returns_data(self, mock_query, client):
        mock_query.return_value = [
            {'PACKET_TYPE': 'telemetry', 'FROM_ID': '!test'}
        ]
        response = client.get('/api/meshtastic')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'data' in data
        assert 'count' in data
    
    @patch('api_server.run_snowflake_query')
    def test_meshtastic_empty_result(self, mock_query, client):
        mock_query.return_value = []
        response = client.get('/api/meshtastic')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['count'] == 0


class TestStatsEndpoint:
    @patch('api_server.run_snowflake_query')
    def test_stats_returns_aggregates(self, mock_query, client):
        mock_query.return_value = [{
            'TOTAL_MESSAGES': 100,
            'UNIQUE_DEVICES': 5
        }]
        response = client.get('/api/stats')
        assert response.status_code == 200


class TestPositionsEndpoint:
    @patch('api_server.run_snowflake_query')  
    def test_positions_returns_gps_data(self, mock_query, client):
        mock_query.return_value = [{
            'LATITUDE': 40.7,
            'LONGITUDE': -74.0,
            'FROM_ID': '!test'
        }]
        response = client.get('/api/positions')
        assert response.status_code == 200


class TestTelemetryEndpoint:
    @patch('api_server.run_snowflake_query')
    def test_telemetry_returns_sensor_data(self, mock_query, client):
        mock_query.return_value = [{
            'BATTERY_LEVEL': 91,
            'TEMPERATURE': 25.5
        }]
        response = client.get('/api/telemetry')
        assert response.status_code == 200


class TestNodesEndpoint:
    @patch('api_server.run_snowflake_query')
    def test_nodes_returns_active_nodes(self, mock_query, client):
        mock_query.return_value = [{
            'FROM_ID': '!test',
            'PACKET_COUNT': 50
        }]
        response = client.get('/api/nodes')
        assert response.status_code == 200


class TestErrorHandling:
    @patch('api_server.run_snowflake_query')
    def test_handles_query_error(self, mock_query, client):
        mock_query.side_effect = Exception("Database error")
        response = client.get('/api/meshtastic')
        assert response.status_code == 500


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
