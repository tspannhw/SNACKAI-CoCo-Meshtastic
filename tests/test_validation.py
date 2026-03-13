#!/usr/bin/env python3
"""
Tests for Validation Module
===========================
Unit tests for all validation functions.
"""

import pytest
import pandas as pd
import numpy as np
from validation import (
    ValidationResult,
    CoordinateValidator,
    DeviceDataValidator,
    NodeIdValidator,
    SearchInputValidator,
    PacketValidator,
    PacketType,
    validate_dataframe,
)


class TestCoordinateValidator:
    """Tests for CoordinateValidator."""
    
    def test_valid_latitude(self):
        result = CoordinateValidator.validate_latitude(40.7128)
        assert result.is_valid
        assert result.value == 40.7128
    
    def test_latitude_out_of_range_high(self):
        result = CoordinateValidator.validate_latitude(91.0)
        assert not result.is_valid
        assert "out of range" in result.error_message
    
    def test_latitude_out_of_range_low(self):
        result = CoordinateValidator.validate_latitude(-91.0)
        assert not result.is_valid
    
    def test_latitude_none(self):
        result = CoordinateValidator.validate_latitude(None)
        assert not result.is_valid
        assert "required" in result.error_message
    
    def test_latitude_zero_warning(self):
        result = CoordinateValidator.validate_latitude(0)
        assert result.is_valid
        assert result.warnings is not None
        assert any("exactly 0" in w for w in result.warnings)
    
    def test_valid_longitude(self):
        result = CoordinateValidator.validate_longitude(-74.0060)
        assert result.is_valid
        assert result.value == -74.0060
    
    def test_longitude_out_of_range(self):
        result = CoordinateValidator.validate_longitude(181.0)
        assert not result.is_valid
    
    def test_coordinate_pair_valid(self):
        result = CoordinateValidator.validate_coordinates(40.7128, -74.0060)
        assert result.is_valid
        assert result.value == (40.7128, -74.0060)
    
    def test_coordinate_pair_null_island(self):
        result = CoordinateValidator.validate_coordinates(0, 0)
        assert result.is_valid
        assert result.warnings is not None
        assert any("Null Island" in w for w in result.warnings)
    
    def test_parse_comma_separated(self):
        result = CoordinateValidator.parse_coordinate_string("40.7128, -74.0060")
        assert result.is_valid
        assert result.value == (40.7128, -74.0060)
    
    def test_parse_space_separated(self):
        result = CoordinateValidator.parse_coordinate_string("40.7128 -74.0060")
        assert result.is_valid
    
    def test_parse_invalid_string(self):
        result = CoordinateValidator.parse_coordinate_string("not coordinates")
        assert not result.is_valid
    
    def test_parse_empty_string(self):
        result = CoordinateValidator.parse_coordinate_string("")
        assert not result.is_valid
    
    def test_boundary_latitudes(self):
        for lat in [-90, 90]:
            result = CoordinateValidator.validate_latitude(lat)
            assert result.is_valid
    
    def test_boundary_longitudes(self):
        for lon in [-180, 180]:
            result = CoordinateValidator.validate_longitude(lon)
            assert result.is_valid


class TestDeviceDataValidator:
    """Tests for DeviceDataValidator."""
    
    def test_valid_battery(self):
        result = DeviceDataValidator.validate_battery_level(75)
        assert result.is_valid
        assert result.value == 75
    
    def test_low_battery_warning(self):
        result = DeviceDataValidator.validate_battery_level(15)
        assert result.is_valid
        assert result.warnings is not None
        assert any("Low battery" in w for w in result.warnings)
    
    def test_battery_external_power(self):
        result = DeviceDataValidator.validate_battery_level(120)
        assert result.is_valid
        assert result.warnings is not None
        assert any("external power" in w for w in result.warnings)
    
    def test_battery_too_high(self):
        result = DeviceDataValidator.validate_battery_level(200)
        assert not result.is_valid
    
    def test_battery_negative(self):
        result = DeviceDataValidator.validate_battery_level(-10)
        assert not result.is_valid
    
    def test_battery_none(self):
        result = DeviceDataValidator.validate_battery_level(None)
        assert result.is_valid
        assert result.warnings is not None
    
    def test_valid_voltage(self):
        result = DeviceDataValidator.validate_voltage(3.7)
        assert result.is_valid
    
    def test_low_voltage_warning(self):
        result = DeviceDataValidator.validate_voltage(3.0)
        assert result.is_valid
        assert result.warnings is not None
    
    def test_voltage_out_of_range(self):
        result = DeviceDataValidator.validate_voltage(10.0)
        assert not result.is_valid
    
    def test_valid_temperature(self):
        result = DeviceDataValidator.validate_temperature(25.0)
        assert result.is_valid
    
    def test_temperature_too_high(self):
        result = DeviceDataValidator.validate_temperature(100.0)
        assert not result.is_valid
    
    def test_temperature_too_low(self):
        result = DeviceDataValidator.validate_temperature(-50.0)
        assert not result.is_valid
    
    def test_valid_humidity(self):
        result = DeviceDataValidator.validate_humidity(65.0)
        assert result.is_valid
    
    def test_humidity_out_of_range(self):
        result = DeviceDataValidator.validate_humidity(110.0)
        assert not result.is_valid
    
    def test_valid_snr(self):
        result = DeviceDataValidator.validate_snr(10.5)
        assert result.is_valid
    
    def test_poor_snr_warning(self):
        result = DeviceDataValidator.validate_snr(-15.0)
        assert result.is_valid
        assert result.warnings is not None
        assert any("Poor signal" in w for w in result.warnings)
    
    def test_snr_out_of_range(self):
        result = DeviceDataValidator.validate_snr(-50.0)
        assert not result.is_valid
    
    def test_valid_rssi(self):
        result = DeviceDataValidator.validate_rssi(-95.0)
        assert result.is_valid
    
    def test_weak_rssi_warning(self):
        result = DeviceDataValidator.validate_rssi(-115.0)
        assert result.is_valid
        assert result.warnings is not None
    
    def test_rssi_out_of_range(self):
        result = DeviceDataValidator.validate_rssi(-150.0)
        assert not result.is_valid


class TestNodeIdValidator:
    """Tests for NodeIdValidator."""
    
    def test_valid_node_id_with_prefix(self):
        result = NodeIdValidator.validate_node_id("!abc12345")
        assert result.is_valid
        assert result.value == "!abc12345"
    
    def test_valid_node_id_without_prefix(self):
        result = NodeIdValidator.validate_node_id("abc12345")
        assert result.is_valid
        assert result.value == "!abc12345"
    
    def test_invalid_node_id_too_short(self):
        result = NodeIdValidator.validate_node_id("!abc")
        assert not result.is_valid
    
    def test_invalid_node_id_too_long(self):
        result = NodeIdValidator.validate_node_id("!abc123456789")
        assert not result.is_valid
    
    def test_invalid_node_id_non_hex(self):
        result = NodeIdValidator.validate_node_id("!ghijklmn")
        assert not result.is_valid
    
    def test_empty_node_id(self):
        result = NodeIdValidator.validate_node_id("")
        assert not result.is_valid
    
    def test_none_node_id(self):
        result = NodeIdValidator.validate_node_id(None)
        assert not result.is_valid
    
    def test_uppercase_hex(self):
        result = NodeIdValidator.validate_node_id("!ABCD1234")
        assert result.is_valid


class TestSearchInputValidator:
    """Tests for SearchInputValidator."""
    
    def test_valid_address(self):
        result = SearchInputValidator.validate_search_input("Times Square, NYC")
        assert result.is_valid
        assert result.value == "Times Square, NYC"
    
    def test_address_too_long(self):
        long_address = "a" * 250
        result = SearchInputValidator.validate_search_input(long_address)
        assert not result.is_valid
        assert "too long" in result.error_message
    
    def test_address_with_script_tag(self):
        result = SearchInputValidator.validate_search_input("<script>alert(1)</script>")
        assert not result.is_valid
    
    def test_address_sanitization(self):
        result = SearchInputValidator.validate_search_input("Test <b>address</b>")
        assert result.is_valid
        assert "<" not in result.value
        assert ">" not in result.value
    
    def test_empty_input(self):
        result = SearchInputValidator.validate_search_input("")
        assert not result.is_valid
    
    def test_valid_radius(self):
        result = SearchInputValidator.validate_search_radius(25.0)
        assert result.is_valid
        assert result.value == 25.0
    
    def test_radius_too_large(self):
        result = SearchInputValidator.validate_search_radius(1000.0)
        assert not result.is_valid
    
    def test_radius_too_small(self):
        result = SearchInputValidator.validate_search_radius(0.01)
        assert not result.is_valid
    
    def test_radius_default(self):
        result = SearchInputValidator.validate_search_radius(None)
        assert result.is_valid
        assert result.value == 10.0


class TestPacketValidator:
    """Tests for PacketValidator."""
    
    def test_valid_packet_type_position(self):
        result = PacketValidator.validate_packet_type("position")
        assert result.is_valid
        assert result.value == "position"
    
    def test_valid_packet_type_case_insensitive(self):
        result = PacketValidator.validate_packet_type("TELEMETRY")
        assert result.is_valid
        assert result.value == "telemetry"
    
    def test_invalid_packet_type(self):
        result = PacketValidator.validate_packet_type("invalid_type")
        assert not result.is_valid
    
    def test_validate_complete_packet(self):
        packet = {
            'from_id': '!abc12345',
            'packet_type': 'position',
            'latitude': 40.7128,
            'longitude': -74.0060,
            'battery_level': 75,
            'rx_snr': 8.5
        }
        result = PacketValidator.validate_packet(packet)
        assert result.is_valid
    
    def test_validate_packet_with_invalid_coords(self):
        packet = {
            'from_id': '!abc12345',
            'packet_type': 'position',
            'latitude': 100.0,
            'longitude': -74.0060,
        }
        result = PacketValidator.validate_packet(packet)
        assert not result.is_valid
        assert "coordinates" in result.error_message
    
    def test_validate_packet_with_warnings(self):
        packet = {
            'from_id': '!abc12345',
            'packet_type': 'telemetry',
            'battery_level': 15,
            'rx_snr': -12.0
        }
        result = PacketValidator.validate_packet(packet)
        assert result.is_valid
        assert result.warnings is not None
        assert len(result.warnings) >= 1


class TestDataFrameValidation:
    """Tests for DataFrame validation."""
    
    def test_validate_empty_dataframe(self):
        df = pd.DataFrame()
        result_df, warnings = validate_dataframe(df)
        assert result_df.empty
        assert any("empty" in w for w in warnings)
    
    def test_filter_invalid_coordinates(self):
        df = pd.DataFrame({
            'LATITUDE': [40.7, 100.0, 0, 35.0],
            'LONGITUDE': [-74.0, -74.0, 0, -120.0],
            'OTHER_COL': [1, 2, 3, 4]
        })
        result_df, warnings = validate_dataframe(df)
        assert len(result_df) == 2
        assert any("Filtered" in w for w in warnings)
    
    def test_clip_battery_values(self):
        df = pd.DataFrame({
            'BATTERY_LEVEL': [-10, 50, 200]
        })
        result_df, _ = validate_dataframe(df)
        assert result_df['BATTERY_LEVEL'].min() >= 0
        assert result_df['BATTERY_LEVEL'].max() <= 150
    
    def test_clip_snr_values(self):
        df = pd.DataFrame({
            'RX_SNR': [-50, 10, 50]
        })
        result_df, _ = validate_dataframe(df)
        assert result_df['RX_SNR'].min() >= -30
        assert result_df['RX_SNR'].max() <= 30
    
    def test_clip_rssi_values(self):
        df = pd.DataFrame({
            'RX_RSSI': [-200, -95, 10]
        })
        result_df, _ = validate_dataframe(df)
        assert result_df['RX_RSSI'].min() >= -140
        assert result_df['RX_RSSI'].max() <= 0


class TestPacketTypeEnum:
    """Tests for PacketType enum."""
    
    def test_all_packet_types(self):
        expected = ['position', 'telemetry', 'text', 'nodeinfo', 'routing', 'admin', 'waypoint']
        actual = [pt.value for pt in PacketType]
        assert set(expected) == set(actual)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
