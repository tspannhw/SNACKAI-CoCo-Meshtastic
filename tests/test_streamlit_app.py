#!/usr/bin/env python3
"""
Unit Tests for Meshtastic Dashboard
====================================
Comprehensive test suite for the Streamlit dashboard components.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streamlit_app import (
    format_timestamp,
    celsius_to_fahrenheit,
    get_battery_status,
    clamp_battery,
    format_slack_alert,
    parse_coordinates,
    geocode_address,
    get_nodes_near_location,
    create_folium_map,
)


class TestTemperatureConversion:
    """Tests for temperature conversion functions."""
    
    def test_celsius_to_fahrenheit_freezing(self):
        """Test freezing point conversion."""
        assert celsius_to_fahrenheit(0) == 32
    
    def test_celsius_to_fahrenheit_boiling(self):
        """Test boiling point conversion."""
        assert celsius_to_fahrenheit(100) == 212
    
    def test_celsius_to_fahrenheit_body_temp(self):
        """Test body temperature conversion."""
        result = celsius_to_fahrenheit(37)
        assert abs(result - 98.6) < 0.1
    
    def test_celsius_to_fahrenheit_negative(self):
        """Test negative temperature conversion."""
        assert celsius_to_fahrenheit(-40) == -40
    
    def test_celsius_to_fahrenheit_none(self):
        """Test None input returns None."""
        assert celsius_to_fahrenheit(None) is None
    
    def test_celsius_to_fahrenheit_nan(self):
        """Test NaN input returns None."""
        assert celsius_to_fahrenheit(float('nan')) is None


class TestBatteryStatus:
    """Tests for battery status functions."""
    
    def test_battery_status_full(self):
        """Test full battery (>=80%)."""
        color, status = get_battery_status(100)
        assert color == "#00ff00"
        assert status == "full"
    
    def test_battery_status_high(self):
        """Test high battery (50-79%)."""
        color, status = get_battery_status(60)
        assert color == "#90EE90"
        assert status == "high"
    
    def test_battery_status_medium(self):
        """Test medium battery (20-49%)."""
        color, status = get_battery_status(30)
        assert color == "#ffa500"
        assert status == "medium"
    
    def test_battery_status_low(self):
        """Test low battery (<20%)."""
        color, status = get_battery_status(10)
        assert color == "#ff6b6b"
        assert status == "low"
    
    def test_battery_status_none(self):
        """Test None battery level."""
        color, status = get_battery_status(None)
        assert color == "gray"
        assert status == "?"
    
    def test_battery_status_boundary_80(self):
        """Test boundary at 80%."""
        color, status = get_battery_status(80)
        assert status == "full"
    
    def test_battery_status_boundary_50(self):
        """Test boundary at 50%."""
        color, status = get_battery_status(50)
        assert status == "high"
    
    def test_battery_status_boundary_20(self):
        """Test boundary at 20%."""
        color, status = get_battery_status(20)
        assert status == "medium"


class TestClampBattery:
    """Tests for battery clamping function."""
    
    def test_clamp_battery_normal(self):
        """Test normal battery value."""
        assert clamp_battery(50) == 50
    
    def test_clamp_battery_over_100(self):
        """Test clamping values over 100."""
        assert clamp_battery(150) == 100
    
    def test_clamp_battery_negative(self):
        """Test clamping negative values."""
        assert clamp_battery(-10) == 0
    
    def test_clamp_battery_none(self):
        """Test None input."""
        assert clamp_battery(None) is None
    
    def test_clamp_battery_float(self):
        """Test float conversion to int."""
        assert clamp_battery(55.7) == 55
    
    def test_clamp_battery_zero(self):
        """Test zero value."""
        assert clamp_battery(0) == 0
    
    def test_clamp_battery_100(self):
        """Test exactly 100."""
        assert clamp_battery(100) == 100


class TestTimestampFormatting:
    """Tests for timestamp formatting functions."""
    
    def test_format_timestamp_datetime(self):
        """Test formatting datetime object."""
        dt = datetime(2024, 6, 15, 14, 30, 45)
        result = format_timestamp(dt)
        assert result == "2024-06-15 14:30:45"
    
    def test_format_timestamp_string(self):
        """Test formatting string timestamp."""
        ts = "2024-06-15 14:30:45.123456"
        result = format_timestamp(ts)
        assert result == "2024-06-15 14:30:45"
    
    def test_format_timestamp_none(self):
        """Test None timestamp."""
        assert format_timestamp(None) == "N/A"
    
    def test_format_timestamp_nan(self):
        """Test NaN timestamp."""
        assert format_timestamp(float('nan')) == "N/A"


class TestSlackAlertFormatting:
    """Tests for Slack alert message formatting."""
    
    def test_format_slack_alert_low_battery(self):
        """Test low battery alert format."""
        result = format_slack_alert(
            "!abc123",
            "low_battery",
            {"Battery": "15%", "Voltage": "3.2V"}
        )
        assert "🔋" in result
        assert "Low Battery" in result
        assert "!abc123" in result
        assert "15%" in result
    
    def test_format_slack_alert_position(self):
        """Test position update alert format."""
        result = format_slack_alert(
            "!xyz789",
            "position_update",
            {"Lat": "40.7128", "Lon": "-74.0060"}
        )
        assert "📍" in result
        assert "Position Update" in result
    
    def test_format_slack_alert_offline(self):
        """Test offline alert format."""
        result = format_slack_alert(
            "!node1",
            "offline",
            {"Last Seen": "2 hours ago"}
        )
        assert "⚠️" in result
        assert "Offline" in result
    
    def test_format_slack_alert_unknown_type(self):
        """Test unknown alert type uses default emoji."""
        result = format_slack_alert(
            "!node1",
            "unknown_type",
            {"data": "test"}
        )
        assert "📡" in result


class TestCoordinateParsing:
    """Tests for coordinate parsing functions."""
    
    def test_parse_coordinates_comma_separated(self):
        """Test parsing comma-separated coordinates."""
        lat, lon, label = parse_coordinates("40.7128, -74.0060")
        assert abs(lat - 40.7128) < 0.0001
        assert abs(lon - (-74.0060)) < 0.0001
    
    def test_parse_coordinates_space_separated(self):
        """Test parsing space-separated coordinates."""
        lat, lon, label = parse_coordinates("40.7128 -74.0060")
        assert abs(lat - 40.7128) < 0.0001
        assert abs(lon - (-74.0060)) < 0.0001
    
    def test_parse_coordinates_invalid_lat(self):
        """Test invalid latitude (>90) returns None."""
        lat, lon, label = parse_coordinates("100.0, -74.0060")
        assert lat is None
        assert lon is None
    
    def test_parse_coordinates_invalid_lon(self):
        """Test invalid longitude (>180) returns None."""
        lat, lon, label = parse_coordinates("40.7128, -200.0")
        assert lat is None
        assert lon is None
    
    def test_parse_coordinates_negative_values(self):
        """Test parsing negative coordinates."""
        lat, lon, label = parse_coordinates("-33.8688, 151.2093")
        assert abs(lat - (-33.8688)) < 0.0001
        assert abs(lon - 151.2093) < 0.0001
    
    def test_parse_coordinates_no_decimals(self):
        """Test parsing whole number coordinates."""
        lat, lon, label = parse_coordinates("40, -74")
        assert lat == 40
        assert lon == -74
    
    def test_parse_coordinates_invalid_string(self):
        """Test invalid string returns None."""
        lat, lon, label = parse_coordinates("not coordinates")
        assert lat is None
        assert lon is None
    
    def test_parse_coordinates_empty_string(self):
        """Test empty string returns None."""
        lat, lon, label = parse_coordinates("")
        assert lat is None


class TestFoliumMapCreation:
    """Tests for Folium map creation."""
    
    def test_create_folium_map_empty_dataframe(self):
        """Test map creation with empty DataFrame."""
        df = pd.DataFrame()
        m = create_folium_map(df)
        assert m is not None
        assert m.location == [40.7128, -74.0060]
    
    def test_create_folium_map_with_data(self):
        """Test map creation with position data."""
        df = pd.DataFrame({
            'FROM_ID': ['!node1', '!node2'],
            'LATITUDE': [40.7128, 40.7580],
            'LONGITUDE': [-74.0060, -73.9855],
            'ALTITUDE': [10.0, 15.0],
            'BATTERY_LEVEL': [80, 25],
            'TEMPERATURE': [22.5, 18.0],
            'RX_SNR': [5.5, -2.0],
            'RX_RSSI': [-95, -105],
            'INGESTED_AT': [datetime.now(), datetime.now()]
        })
        m = create_folium_map(df)
        assert m is not None
    
    def test_create_folium_map_with_search_location(self):
        """Test map creation with search marker."""
        df = pd.DataFrame()
        m = create_folium_map(
            df,
            center_lat=40.7580,
            center_lon=-73.9855,
            search_lat=40.7580,
            search_lon=-73.9855,
            search_label="Times Square"
        )
        assert m is not None
        assert m.location == [40.7580, -73.9855]
    
    def test_create_folium_map_battery_colors(self):
        """Test that battery levels affect marker colors."""
        df = pd.DataFrame({
            'FROM_ID': ['!high', '!low'],
            'LATITUDE': [40.7128, 40.7580],
            'LONGITUDE': [-74.0060, -73.9855],
            'BATTERY_LEVEL': [90, 10],
            'INGESTED_AT': [datetime.now(), datetime.now()]
        })
        m = create_folium_map(df)
        assert m is not None


class TestDataValidation:
    """Tests for data validation functions."""
    
    def test_validate_latitude_range(self):
        """Test latitude validation."""
        valid_lats = [-90, -45, 0, 45, 90]
        invalid_lats = [-91, 91, 100, -100]
        
        for lat in valid_lats:
            lat_parsed, _, _ = parse_coordinates(f"{lat}, 0")
            assert lat_parsed == lat
        
        for lat in invalid_lats:
            lat_parsed, _, _ = parse_coordinates(f"{lat}, 0")
            assert lat_parsed is None
    
    def test_validate_longitude_range(self):
        """Test longitude validation."""
        valid_lons = [-180, -90, 0, 90, 180]
        invalid_lons = [-181, 181, 200, -200]
        
        for lon in valid_lons:
            _, lon_parsed, _ = parse_coordinates(f"0, {lon}")
            assert lon_parsed == lon
        
        for lon in invalid_lons:
            _, lon_parsed, _ = parse_coordinates(f"0, {lon}")
            assert lon_parsed is None


class TestGeocoding:
    """Tests for geocoding functions."""
    
    @patch('streamlit_app.requests.get')
    def test_geocode_address_success(self, mock_get):
        """Test successful geocoding."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{
            'lat': '40.7580',
            'lon': '-73.9855',
            'display_name': 'Times Square, New York, NY'
        }]
        mock_get.return_value = mock_response
        
        lat, lon, label = geocode_address("Times Square, NYC")
        assert lat == 40.7580
        assert lon == -73.9855
        assert "Times Square" in label
    
    @patch('streamlit_app.requests.get')
    def test_geocode_address_no_results(self, mock_get):
        """Test geocoding with no results."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        lat, lon, label = geocode_address("nonexistent place xyz123")
        assert lat is None
        assert lon is None
    
    @patch('streamlit_app.requests.get')
    def test_geocode_address_api_error(self, mock_get):
        """Test geocoding with API error."""
        mock_get.side_effect = Exception("Network error")
        
        lat, lon, label = geocode_address("Times Square")
        assert lat is None
        assert lon is None


class TestIntegration:
    """Integration tests for dashboard components."""
    
    def test_full_data_flow(self):
        """Test data flow from raw data to display."""
        raw_data = {
            'FROM_ID': '!abc123',
            'LATITUDE': 40.7128,
            'LONGITUDE': -74.0060,
            'BATTERY_LEVEL': 45,
            'TEMPERATURE': 22.5,
            'RX_SNR': 8.5,
            'INGESTED_AT': datetime.now()
        }
        
        df = pd.DataFrame([raw_data])
        
        bat_color, bat_status = get_battery_status(raw_data['BATTERY_LEVEL'])
        assert bat_status == "medium"
        
        temp_f = celsius_to_fahrenheit(raw_data['TEMPERATURE'])
        assert abs(temp_f - 72.5) < 0.1
        
        m = create_folium_map(df)
        assert m is not None
    
    def test_search_and_display_flow(self):
        """Test search to display workflow."""
        lat, lon, label = parse_coordinates("40.7580, -73.9855")
        assert lat is not None
        assert lon is not None
        
        df = pd.DataFrame({
            'FROM_ID': ['!node1'],
            'LATITUDE': [40.76],
            'LONGITUDE': [-73.98],
            'DISTANCE_KM': [0.5],
            'BATTERY_LEVEL': [75],
            'INGESTED_AT': [datetime.now()]
        })
        
        m = create_folium_map(
            df,
            search_lat=lat,
            search_lon=lon,
            search_label=label
        )
        assert m is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
