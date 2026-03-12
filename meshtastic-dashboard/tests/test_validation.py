"""
Tests for Meshtastic data validation module
"""
import pytest
from datetime import datetime
from validation import (
    PacketType,
    MeshtasticPacket,
    PositionPayload,
    TelemetryPayload,
    NodeInfoPayload,
    TextMessagePayload,
    MQTTMessage,
    ValidationResult,
    validate_mqtt_message,
    validate_snowflake_row,
    HealthCheck,
    create_health_check
)


class TestPacketType:
    def test_packet_types(self):
        assert PacketType.TELEMETRY.value == "telemetry"
        assert PacketType.POSITION.value == "position"
        assert PacketType.NODEINFO.value == "nodeinfo"
        assert PacketType.TEXT.value == "text"


class TestPositionPayload:
    def test_valid_position(self):
        pos = PositionPayload(
            latitude_i=407000000,
            longitude_i=-740000000,
            altitude=100,
            ground_speed=5,
            ground_track=180
        )
        assert pos.latitude == pytest.approx(40.7, rel=1e-2)
        assert pos.longitude == pytest.approx(-74.0, rel=1e-2)
        assert pos.altitude == 100
    
    def test_invalid_latitude(self):
        with pytest.raises(ValueError):
            PositionPayload(latitude_i=1000000000)
    
    def test_invalid_longitude(self):
        with pytest.raises(ValueError):
            PositionPayload(longitude_i=2000000000)
    
    def test_null_coordinates(self):
        pos = PositionPayload()
        assert pos.latitude is None
        assert pos.longitude is None


class TestTelemetryPayload:
    def test_valid_telemetry(self):
        tel = TelemetryPayload(
            battery_level=85,
            voltage=4.1,
            temperature=25.5,
            relative_humidity=60.0
        )
        assert tel.battery_level == 85
        assert tel.temperature_f == pytest.approx(77.9, rel=1e-2)
    
    def test_battery_bounds(self):
        valid = TelemetryPayload(battery_level=0)
        assert valid.battery_level == 0
        
        valid = TelemetryPayload(battery_level=101)
        assert valid.battery_level == 101
        
        with pytest.raises(ValueError):
            TelemetryPayload(battery_level=-1)
        
        with pytest.raises(ValueError):
            TelemetryPayload(battery_level=102)
    
    def test_temperature_conversion(self):
        tel = TelemetryPayload(temperature=0)
        assert tel.temperature_f == 32.0
        
        tel = TelemetryPayload(temperature=100)
        assert tel.temperature_f == 212.0
    
    def test_null_temperature_f(self):
        tel = TelemetryPayload()
        assert tel.temperature_f is None


class TestNodeInfoPayload:
    def test_valid_nodeinfo(self):
        node = NodeInfoPayload(
            id="!abcd1234",
            longname="Test Node",
            shortname="TST"
        )
        assert node.id == "!abcd1234"
        assert node.long_name == "Test Node"
        assert node.short_name == "TST"
    
    def test_alias_fields(self):
        node = NodeInfoPayload(**{
            "id": "!12345678",
            "longname": "My Node",
            "shortname": "MN"
        })
        assert node.long_name == "My Node"


class TestTextMessagePayload:
    def test_valid_text(self):
        msg = TextMessagePayload(text="Hello Mesh!")
        assert msg.text == "Hello Mesh!"
    
    def test_whitespace_trimmed(self):
        msg = TextMessagePayload(text="  Hello  ")
        assert msg.text == "Hello"
    
    def test_empty_text_fails(self):
        with pytest.raises(ValueError):
            TextMessagePayload(text="   ")


class TestMQTTMessage:
    def test_valid_message(self):
        msg = MQTTMessage(**{
            "id": 12345,
            "from": 2130636288,
            "to": -1,
            "channel": 0,
            "type": "nodeinfo",
            "sender": "!7efeee00",
            "timestamp": 1646832724,
            "payload": {"id": "!7efeee00", "longname": "base0"}
        })
        assert msg.id == 12345
        assert msg.from_id == 2130636288
        assert msg.type == "nodeinfo"
    
    def test_type_normalization(self):
        msg = MQTTMessage(**{
            "id": 1,
            "from": 1234,
            "type": "TELEMETRY",
            "sender": "!test",
            "timestamp": 12345
        })
        assert msg.type == "telemetry"
    
    def test_to_snowflake_row_position(self):
        msg = MQTTMessage(**{
            "id": 1,
            "from": 0x7efeee00,
            "to": -1,
            "channel": 0,
            "type": "position",
            "sender": "!7efeee00",
            "timestamp": 1646832724,
            "payload": {
                "latitude_i": 406728704,
                "longitude_i": -739643392,
                "altitude": 99
            }
        })
        row = msg.to_snowflake_row()
        assert row['packet_type'] == 'position'
        assert row['latitude'] == pytest.approx(40.6728704, rel=1e-6)
        assert row['longitude'] == pytest.approx(-73.9643392, rel=1e-6)
        assert row['altitude'] == 99
    
    def test_to_snowflake_row_telemetry(self):
        msg = MQTTMessage(**{
            "id": 1,
            "from": 12345,
            "type": "telemetry",
            "sender": "!test",
            "timestamp": 12345,
            "payload": {
                "battery_level": 91,
                "voltage": 4.094,
                "temperature": 29.5
            }
        })
        row = msg.to_snowflake_row()
        assert row['packet_type'] == 'telemetry'
        assert row['battery_level'] == 91
        assert row['voltage'] == 4.094
        assert row['temperature'] == 29.5


class TestValidateMQTTMessage:
    def test_valid_message(self):
        result = validate_mqtt_message({
            "id": 1,
            "from": 12345,
            "type": "telemetry",
            "sender": "!test",
            "timestamp": 12345,
            "payload": {}
        })
        assert result.valid is True
        assert result.data is not None
    
    def test_missing_from(self):
        result = validate_mqtt_message({
            "id": 1,
            "type": "telemetry",
            "timestamp": 12345
        })
        assert result.valid is False
        assert any("from" in e for e in result.errors)
    
    def test_missing_type(self):
        result = validate_mqtt_message({
            "id": 1,
            "from": 12345,
            "timestamp": 12345
        })
        assert result.valid is False
        assert any("type" in e for e in result.errors)
    
    def test_missing_timestamp_warning(self):
        result = validate_mqtt_message({
            "id": 1,
            "from": 12345,
            "type": "telemetry",
            "sender": "!test"
        })
        assert result.valid is True
        assert any("timestamp" in w for w in result.warnings)


class TestValidateSnowflakeRow:
    def test_valid_row(self):
        result = validate_snowflake_row({
            'packet_type': 'position',
            'latitude': 40.7,
            'longitude': -74.0,
            'from_id': '!test'
        })
        assert result.valid is True
    
    def test_invalid_latitude(self):
        result = validate_snowflake_row({
            'latitude': 100.0
        })
        assert result.valid is False
        assert any("latitude" in e for e in result.errors)
    
    def test_invalid_longitude(self):
        result = validate_snowflake_row({
            'longitude': -200.0
        })
        assert result.valid is False
        assert any("longitude" in e for e in result.errors)
    
    def test_unusual_battery_warning(self):
        result = validate_snowflake_row({
            'battery_level': 150
        })
        assert result.valid is True
        assert any("battery" in w for w in result.warnings)
    
    def test_unusual_temperature_warning(self):
        result = validate_snowflake_row({
            'temperature': -100
        })
        assert result.valid is True
        assert any("temperature" in w for w in result.warnings)


class TestHealthCheck:
    def test_healthy(self):
        health = create_health_check(
            snowflake_ok=True,
            mqtt_ok=True,
            api_ok=True
        )
        assert health.status == "healthy"
        assert health.is_healthy is True
    
    def test_degraded(self):
        health = create_health_check(
            snowflake_ok=True,
            mqtt_ok=False,
            api_ok=True
        )
        assert health.status == "degraded"
        assert health.is_healthy is False
    
    def test_unhealthy(self):
        health = create_health_check(
            snowflake_ok=False,
            mqtt_ok=False,
            api_ok=False
        )
        assert health.status == "unhealthy"
        assert health.is_healthy is False


class TestValidationResult:
    def test_valid_result(self):
        result = ValidationResult(valid=True, data={'test': 'data'})
        assert result.valid is True
        assert result.errors == []
        assert result.data == {'test': 'data'}
    
    def test_invalid_result(self):
        result = ValidationResult(
            valid=False,
            errors=['Error 1', 'Error 2']
        )
        assert result.valid is False
        assert len(result.errors) == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
