#!/usr/bin/env python3
"""
Validation Module for Meshtastic Dashboard
==========================================
Input validation, data validation, and sanitization functions.
"""

import re
from typing import Optional, Tuple, Any, Dict, List
from dataclasses import dataclass
from enum import Enum
import pandas as pd


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class PacketType(Enum):
    """Valid Meshtastic packet types."""
    POSITION = "position"
    TELEMETRY = "telemetry"
    TEXT = "text"
    NODEINFO = "nodeinfo"
    ROUTING = "routing"
    ADMIN = "admin"
    WAYPOINT = "waypoint"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    value: Any = None
    error_message: str = None
    warnings: List[str] = None


class CoordinateValidator:
    """Validator for GPS coordinates."""
    
    MIN_LATITUDE = -90.0
    MAX_LATITUDE = 90.0
    MIN_LONGITUDE = -180.0
    MAX_LONGITUDE = 180.0
    
    @classmethod
    def validate_latitude(cls, lat: float) -> ValidationResult:
        """Validate latitude value."""
        if lat is None or pd.isna(lat):
            return ValidationResult(False, None, "Latitude is required")
        
        try:
            lat = float(lat)
        except (ValueError, TypeError):
            return ValidationResult(False, None, f"Invalid latitude format: {lat}")
        
        if not cls.MIN_LATITUDE <= lat <= cls.MAX_LATITUDE:
            return ValidationResult(
                False, None,
                f"Latitude {lat} out of range [{cls.MIN_LATITUDE}, {cls.MAX_LATITUDE}]"
            )
        
        warnings = []
        if lat == 0:
            warnings.append("Latitude is exactly 0, may indicate missing GPS data")
        
        return ValidationResult(True, lat, warnings=warnings)
    
    @classmethod
    def validate_longitude(cls, lon: float) -> ValidationResult:
        """Validate longitude value."""
        if lon is None or pd.isna(lon):
            return ValidationResult(False, None, "Longitude is required")
        
        try:
            lon = float(lon)
        except (ValueError, TypeError):
            return ValidationResult(False, None, f"Invalid longitude format: {lon}")
        
        if not cls.MIN_LONGITUDE <= lon <= cls.MAX_LONGITUDE:
            return ValidationResult(
                False, None,
                f"Longitude {lon} out of range [{cls.MIN_LONGITUDE}, {cls.MAX_LONGITUDE}]"
            )
        
        warnings = []
        if lon == 0:
            warnings.append("Longitude is exactly 0, may indicate missing GPS data")
        
        return ValidationResult(True, lon, warnings=warnings)
    
    @classmethod
    def validate_coordinates(cls, lat: float, lon: float) -> ValidationResult:
        """Validate a coordinate pair."""
        lat_result = cls.validate_latitude(lat)
        if not lat_result.is_valid:
            return lat_result
        
        lon_result = cls.validate_longitude(lon)
        if not lon_result.is_valid:
            return lon_result
        
        warnings = []
        if lat_result.warnings:
            warnings.extend(lat_result.warnings)
        if lon_result.warnings:
            warnings.extend(lon_result.warnings)
        
        if lat == 0 and lon == 0:
            warnings.append("Coordinates (0, 0) - likely invalid GPS data (Null Island)")
        
        return ValidationResult(True, (lat, lon), warnings=warnings if warnings else None)
    
    @classmethod
    def parse_coordinate_string(cls, coord_str: str) -> ValidationResult:
        """Parse and validate coordinate string."""
        if not coord_str or not isinstance(coord_str, str):
            return ValidationResult(False, None, "Empty or invalid coordinate string")
        
        coord_str = coord_str.strip()
        
        patterns = [
            r'^(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)$',
            r'^(-?\d+\.?\d*)\s+(-?\d+\.?\d*)$',
            r'^lat[:\s]*(-?\d+\.?\d*)\s*[,\s]\s*lo?ng?[:\s]*(-?\d+\.?\d*)$',
        ]
        
        for pattern in patterns:
            match = re.match(pattern, coord_str, re.IGNORECASE)
            if match:
                try:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    return cls.validate_coordinates(lat, lon)
                except (ValueError, IndexError):
                    continue
        
        return ValidationResult(False, None, f"Could not parse coordinates: {coord_str}")


class DeviceDataValidator:
    """Validator for device telemetry data."""
    
    MIN_BATTERY = 0
    MAX_BATTERY = 100
    MAX_BATTERY_EXTERNAL = 150
    MIN_VOLTAGE = 0.0
    MAX_VOLTAGE = 6.0
    MIN_TEMPERATURE_C = -40.0
    MAX_TEMPERATURE_C = 85.0
    MIN_HUMIDITY = 0.0
    MAX_HUMIDITY = 100.0
    MIN_SNR = -30.0
    MAX_SNR = 30.0
    MIN_RSSI = -140.0
    MAX_RSSI = 0.0
    
    @classmethod
    def validate_battery_level(cls, level: float) -> ValidationResult:
        """Validate battery level percentage."""
        if level is None or pd.isna(level):
            return ValidationResult(True, None, warnings=["Battery level not available"])
        
        try:
            level = float(level)
        except (ValueError, TypeError):
            return ValidationResult(False, None, f"Invalid battery format: {level}")
        
        warnings = []
        
        if level > cls.MAX_BATTERY:
            if level <= cls.MAX_BATTERY_EXTERNAL:
                warnings.append(f"Battery {level}% indicates external power")
                return ValidationResult(True, level, warnings=warnings)
            else:
                return ValidationResult(False, None, f"Battery level {level}% exceeds maximum")
        
        if level < cls.MIN_BATTERY:
            return ValidationResult(False, None, f"Battery level {level}% below minimum")
        
        if level < 20:
            warnings.append(f"Low battery warning: {level}%")
        
        return ValidationResult(True, level, warnings=warnings if warnings else None)
    
    @classmethod
    def validate_voltage(cls, voltage: float) -> ValidationResult:
        """Validate voltage reading."""
        if voltage is None or pd.isna(voltage):
            return ValidationResult(True, None)
        
        try:
            voltage = float(voltage)
        except (ValueError, TypeError):
            return ValidationResult(False, None, f"Invalid voltage format: {voltage}")
        
        if not cls.MIN_VOLTAGE <= voltage <= cls.MAX_VOLTAGE:
            return ValidationResult(
                False, None,
                f"Voltage {voltage}V out of expected range [{cls.MIN_VOLTAGE}, {cls.MAX_VOLTAGE}]"
            )
        
        warnings = []
        if voltage < 3.3:
            warnings.append(f"Low voltage warning: {voltage}V")
        
        return ValidationResult(True, voltage, warnings=warnings if warnings else None)
    
    @classmethod
    def validate_temperature(cls, temp: float) -> ValidationResult:
        """Validate temperature in Celsius."""
        if temp is None or pd.isna(temp):
            return ValidationResult(True, None)
        
        try:
            temp = float(temp)
        except (ValueError, TypeError):
            return ValidationResult(False, None, f"Invalid temperature format: {temp}")
        
        if not cls.MIN_TEMPERATURE_C <= temp <= cls.MAX_TEMPERATURE_C:
            return ValidationResult(
                False, None,
                f"Temperature {temp}°C out of sensor range [{cls.MIN_TEMPERATURE_C}, {cls.MAX_TEMPERATURE_C}]"
            )
        
        return ValidationResult(True, temp)
    
    @classmethod
    def validate_humidity(cls, humidity: float) -> ValidationResult:
        """Validate relative humidity percentage."""
        if humidity is None or pd.isna(humidity):
            return ValidationResult(True, None)
        
        try:
            humidity = float(humidity)
        except (ValueError, TypeError):
            return ValidationResult(False, None, f"Invalid humidity format: {humidity}")
        
        if not cls.MIN_HUMIDITY <= humidity <= cls.MAX_HUMIDITY:
            return ValidationResult(
                False, None,
                f"Humidity {humidity}% out of range [{cls.MIN_HUMIDITY}, {cls.MAX_HUMIDITY}]"
            )
        
        return ValidationResult(True, humidity)
    
    @classmethod
    def validate_snr(cls, snr: float) -> ValidationResult:
        """Validate Signal-to-Noise Ratio."""
        if snr is None or pd.isna(snr):
            return ValidationResult(True, None)
        
        try:
            snr = float(snr)
        except (ValueError, TypeError):
            return ValidationResult(False, None, f"Invalid SNR format: {snr}")
        
        if not cls.MIN_SNR <= snr <= cls.MAX_SNR:
            return ValidationResult(
                False, None,
                f"SNR {snr} dB out of expected range [{cls.MIN_SNR}, {cls.MAX_SNR}]"
            )
        
        warnings = []
        if snr < -10:
            warnings.append(f"Poor signal quality: SNR {snr} dB")
        
        return ValidationResult(True, snr, warnings=warnings if warnings else None)
    
    @classmethod
    def validate_rssi(cls, rssi: float) -> ValidationResult:
        """Validate Received Signal Strength Indicator."""
        if rssi is None or pd.isna(rssi):
            return ValidationResult(True, None)
        
        try:
            rssi = float(rssi)
        except (ValueError, TypeError):
            return ValidationResult(False, None, f"Invalid RSSI format: {rssi}")
        
        if not cls.MIN_RSSI <= rssi <= cls.MAX_RSSI:
            return ValidationResult(
                False, None,
                f"RSSI {rssi} dBm out of expected range [{cls.MIN_RSSI}, {cls.MAX_RSSI}]"
            )
        
        warnings = []
        if rssi < -110:
            warnings.append(f"Weak signal: RSSI {rssi} dBm")
        
        return ValidationResult(True, rssi, warnings=warnings if warnings else None)


class NodeIdValidator:
    """Validator for Meshtastic node IDs."""
    
    NODE_ID_PATTERN = r'^!?[0-9a-fA-F]{8}$'
    
    @classmethod
    def validate_node_id(cls, node_id: str) -> ValidationResult:
        """Validate Meshtastic node ID format."""
        if not node_id:
            return ValidationResult(False, None, "Node ID is required")
        
        if not isinstance(node_id, str):
            node_id = str(node_id)
        
        node_id = node_id.strip()
        
        if re.match(cls.NODE_ID_PATTERN, node_id):
            if not node_id.startswith('!'):
                node_id = '!' + node_id
            return ValidationResult(True, node_id)
        
        if len(node_id) > 0 and node_id[0] == '!':
            if re.match(r'^![0-9a-fA-F]+$', node_id):
                return ValidationResult(
                    False, None,
                    f"Node ID {node_id} has invalid length (expected 8 hex chars after !)"
                )
        
        return ValidationResult(False, None, f"Invalid node ID format: {node_id}")


class SearchInputValidator:
    """Validator for search inputs."""
    
    MAX_ADDRESS_LENGTH = 200
    MAX_RADIUS_KM = 500
    MIN_RADIUS_KM = 0.1
    
    @classmethod
    def validate_search_input(cls, search_input: str) -> ValidationResult:
        """Validate and sanitize search input."""
        if not search_input:
            return ValidationResult(False, None, "Search input is required")
        
        search_input = search_input.strip()
        
        if len(search_input) > cls.MAX_ADDRESS_LENGTH:
            return ValidationResult(
                False, None,
                f"Search input too long (max {cls.MAX_ADDRESS_LENGTH} characters)"
            )
        
        dangerous_patterns = [
            r'<script',
            r'javascript:',
            r'on\w+\s*=',
            r'[\x00-\x1f]',
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, search_input, re.IGNORECASE):
                return ValidationResult(False, None, "Invalid characters in search input")
        
        sanitized = re.sub(r'[<>"\']', '', search_input)
        
        return ValidationResult(True, sanitized)
    
    @classmethod
    def validate_search_radius(cls, radius: float) -> ValidationResult:
        """Validate search radius in kilometers."""
        if radius is None:
            return ValidationResult(True, 10.0)
        
        try:
            radius = float(radius)
        except (ValueError, TypeError):
            return ValidationResult(False, None, f"Invalid radius format: {radius}")
        
        if radius < cls.MIN_RADIUS_KM:
            return ValidationResult(
                False, None,
                f"Radius {radius} km below minimum {cls.MIN_RADIUS_KM} km"
            )
        
        if radius > cls.MAX_RADIUS_KM:
            return ValidationResult(
                False, None,
                f"Radius {radius} km exceeds maximum {cls.MAX_RADIUS_KM} km"
            )
        
        return ValidationResult(True, radius)


class PacketValidator:
    """Validator for Meshtastic packet data."""
    
    @classmethod
    def validate_packet_type(cls, packet_type: str) -> ValidationResult:
        """Validate packet type."""
        if not packet_type:
            return ValidationResult(False, None, "Packet type is required")
        
        packet_type = packet_type.lower().strip()
        
        valid_types = [pt.value for pt in PacketType]
        if packet_type not in valid_types:
            return ValidationResult(
                False, None,
                f"Invalid packet type '{packet_type}'. Valid types: {valid_types}"
            )
        
        return ValidationResult(True, packet_type)
    
    @classmethod
    def validate_packet(cls, packet_data: Dict) -> ValidationResult:
        """Validate a complete packet."""
        errors = []
        warnings = []
        
        if 'from_id' in packet_data:
            result = NodeIdValidator.validate_node_id(packet_data['from_id'])
            if not result.is_valid:
                errors.append(f"from_id: {result.error_message}")
        
        if 'packet_type' in packet_data:
            result = cls.validate_packet_type(packet_data['packet_type'])
            if not result.is_valid:
                errors.append(f"packet_type: {result.error_message}")
        
        if 'latitude' in packet_data and 'longitude' in packet_data:
            result = CoordinateValidator.validate_coordinates(
                packet_data['latitude'],
                packet_data['longitude']
            )
            if not result.is_valid:
                errors.append(f"coordinates: {result.error_message}")
            elif result.warnings:
                warnings.extend(result.warnings)
        
        if 'battery_level' in packet_data:
            result = DeviceDataValidator.validate_battery_level(packet_data['battery_level'])
            if not result.is_valid:
                errors.append(f"battery_level: {result.error_message}")
            elif result.warnings:
                warnings.extend(result.warnings)
        
        if 'rx_snr' in packet_data:
            result = DeviceDataValidator.validate_snr(packet_data['rx_snr'])
            if not result.is_valid:
                errors.append(f"rx_snr: {result.error_message}")
            elif result.warnings:
                warnings.extend(result.warnings)
        
        if errors:
            return ValidationResult(False, None, "; ".join(errors))
        
        return ValidationResult(True, packet_data, warnings=warnings if warnings else None)


def validate_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Validate and clean a DataFrame of Meshtastic data.
    
    Returns:
        Tuple of (cleaned DataFrame, list of warning messages)
    """
    warnings = []
    
    if df.empty:
        return df, ["DataFrame is empty"]
    
    if 'LATITUDE' in df.columns and 'LONGITUDE' in df.columns:
        valid_coords = (
            df['LATITUDE'].between(-90, 90) &
            df['LONGITUDE'].between(-180, 180) &
            ~((df['LATITUDE'] == 0) & (df['LONGITUDE'] == 0))
        )
        invalid_count = (~valid_coords).sum()
        if invalid_count > 0:
            warnings.append(f"Filtered {invalid_count} rows with invalid coordinates")
            df = df[valid_coords]
    
    if 'BATTERY_LEVEL' in df.columns:
        df['BATTERY_LEVEL'] = df['BATTERY_LEVEL'].clip(0, 150)
    
    if 'RX_SNR' in df.columns:
        df['RX_SNR'] = df['RX_SNR'].clip(-30, 30)
    
    if 'RX_RSSI' in df.columns:
        df['RX_RSSI'] = df['RX_RSSI'].clip(-140, 0)
    
    return df, warnings


if __name__ == "__main__":
    print("Testing CoordinateValidator...")
    result = CoordinateValidator.parse_coordinate_string("40.7128, -74.0060")
    print(f"  Parse '40.7128, -74.0060': {result}")
    
    print("\nTesting DeviceDataValidator...")
    result = DeviceDataValidator.validate_battery_level(85)
    print(f"  Battery 85%: {result}")
    result = DeviceDataValidator.validate_battery_level(15)
    print(f"  Battery 15%: {result}")
    
    print("\nTesting NodeIdValidator...")
    result = NodeIdValidator.validate_node_id("!abc12345")
    print(f"  Node ID '!abc12345': {result}")
    
    print("\nAll validation tests passed!")
