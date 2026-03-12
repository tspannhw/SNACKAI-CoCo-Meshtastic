"""
Meshtastic Data Validation Module

Provides Pydantic models for validating Meshtastic packets and ensuring data quality.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
import logging
from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)


class PacketType(str, Enum):
    TELEMETRY = "telemetry"
    POSITION = "position"
    NODEINFO = "nodeinfo"
    TEXT = "text"
    RAW = "raw"
    ROUTING = "routing"
    WAYPOINT = "waypoint"
    TRACEROUTE = "traceroute"
    NEIGHBORINFO = "neighborinfo"
    DETECTION = "detection"
    PAXCOUNTER = "paxcounter"


class MeshtasticPacket(BaseModel):
    """Base model for all Meshtastic packets"""
    id: Optional[int] = Field(None, description="Unique packet ID")
    from_id: Optional[str] = Field(None, alias="from", description="Source node ID")
    to_id: Optional[str] = Field(None, alias="to", description="Destination node ID")
    channel: Optional[int] = Field(None, ge=0, le=7, description="Channel index")
    type: Optional[str] = Field(None, description="Packet type")
    timestamp: Optional[int] = Field(None, description="Unix timestamp")
    sender: Optional[str] = Field(None, description="Gateway sender ID")
    
    class Config:
        populate_by_name = True
        extra = "allow"


class PositionPayload(BaseModel):
    """Position packet payload"""
    latitude_i: Optional[int] = Field(None, description="Latitude * 1e7")
    longitude_i: Optional[int] = Field(None, description="Longitude * 1e7")
    altitude: Optional[int] = Field(None, ge=-1000, le=50000, description="Altitude in meters")
    ground_speed: Optional[int] = Field(None, ge=0, description="Ground speed in m/s")
    ground_track: Optional[int] = Field(None, ge=0, le=360, description="Heading in degrees")
    sats_in_view: Optional[int] = Field(None, ge=0, le=50, description="Number of satellites")
    precision_bits: Optional[int] = Field(None, ge=0, le=32, description="Location precision")
    
    @property
    def latitude(self) -> Optional[float]:
        if self.latitude_i is not None:
            return self.latitude_i / 1e7
        return None
    
    @property
    def longitude(self) -> Optional[float]:
        if self.longitude_i is not None:
            return self.longitude_i / 1e7
        return None
    
    @field_validator('latitude_i')
    @classmethod
    def validate_latitude(cls, v):
        if v is not None and (v < -900000000 or v > 900000000):
            raise ValueError(f"Invalid latitude_i: {v}")
        return v
    
    @field_validator('longitude_i')
    @classmethod
    def validate_longitude(cls, v):
        if v is not None and (v < -1800000000 or v > 1800000000):
            raise ValueError(f"Invalid longitude_i: {v}")
        return v


class TelemetryPayload(BaseModel):
    """Telemetry packet payload for device and environment metrics"""
    battery_level: Optional[int] = Field(None, ge=0, le=101, description="Battery %")
    voltage: Optional[float] = Field(None, ge=0, le=20, description="Battery voltage")
    channel_utilization: Optional[float] = Field(None, ge=0, le=100, description="Channel utilization %")
    air_util_tx: Optional[float] = Field(None, ge=0, le=100, description="TX air utilization %")
    uptime_seconds: Optional[int] = Field(None, ge=0, description="Uptime in seconds")
    temperature: Optional[float] = Field(None, ge=-50, le=100, description="Temperature °C")
    relative_humidity: Optional[float] = Field(None, ge=0, le=100, description="Humidity %")
    barometric_pressure: Optional[float] = Field(None, ge=800, le=1200, description="Pressure hPa")
    gas_resistance: Optional[float] = Field(None, ge=0, description="Gas resistance Ω")
    iaq: Optional[int] = Field(None, ge=0, le=500, description="Indoor Air Quality index")
    lux: Optional[float] = Field(None, ge=0, description="Light level")
    wind_speed: Optional[float] = Field(None, ge=0, le=200, description="Wind speed m/s")
    wind_direction: Optional[int] = Field(None, ge=0, le=360, description="Wind direction degrees")
    pm25_standard: Optional[float] = Field(None, ge=0, description="PM2.5 µg/m³")
    co2: Optional[int] = Field(None, ge=0, le=10000, description="CO2 ppm")
    
    @property
    def temperature_f(self) -> Optional[float]:
        if self.temperature is not None:
            return self.temperature * 9/5 + 32
        return None


class NodeInfoPayload(BaseModel):
    """Node info packet payload"""
    id: Optional[str] = Field(None, description="Node ID")
    long_name: Optional[str] = Field(None, max_length=40, alias="longname", description="Long name")
    short_name: Optional[str] = Field(None, max_length=4, alias="shortname", description="Short name")
    hardware: Optional[int] = Field(None, description="Hardware model ID")
    
    class Config:
        populate_by_name = True


class TextMessagePayload(BaseModel):
    """Text message payload"""
    text: str = Field(..., max_length=500, description="Message text")
    
    @field_validator('text')
    @classmethod
    def validate_text(cls, v):
        if v and len(v.strip()) == 0:
            raise ValueError("Text message cannot be empty")
        return v.strip()


class MQTTMessage(BaseModel):
    """Complete MQTT message from Meshtastic broker"""
    id: int = Field(..., description="Unique packet ID")
    channel: int = Field(0, ge=0, le=7, description="Channel index")
    from_id: int = Field(..., alias="from", description="Source node number")
    to_id: int = Field(-1, alias="to", description="Destination node number")
    type: str = Field(..., description="Packet type")
    sender: str = Field(..., description="Gateway sender ID")
    timestamp: int = Field(..., description="Unix timestamp")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Packet payload")
    
    class Config:
        populate_by_name = True
    
    @field_validator('type')
    @classmethod
    def normalize_type(cls, v):
        type_map = {
            'nodeinfo': 'nodeinfo',
            'position': 'position', 
            'telemetry': 'telemetry',
            'text': 'text',
            'traceroute': 'traceroute',
            'neighborinfo': 'neighborinfo',
            'routing': 'routing'
        }
        return type_map.get(v.lower(), v.lower())
    
    def to_snowflake_row(self) -> Dict[str, Any]:
        """Convert to Snowflake table row format"""
        row = {
            'ingested_at': datetime.utcnow().isoformat(),
            'packet_type': self.type,
            'from_id': f"!{self.from_id:08x}" if isinstance(self.from_id, int) else self.from_id,
            'from_num': self.from_id if isinstance(self.from_id, int) else None,
            'to_id': f"!{self.to_id:08x}" if self.to_id != -1 else "^all",
            'to_num': self.to_id,
            'channel': self.channel,
        }
        
        if self.type == 'position' and self.payload:
            pos = PositionPayload(**self.payload)
            row.update({
                'latitude': pos.latitude,
                'longitude': pos.longitude,
                'altitude': pos.altitude,
                'ground_speed': pos.ground_speed,
                'ground_track': pos.ground_track,
                'sats_in_view': pos.sats_in_view,
                'precision_bits': pos.precision_bits,
            })
        
        elif self.type == 'telemetry' and self.payload:
            tel = TelemetryPayload(**self.payload)
            row.update({
                'battery_level': tel.battery_level,
                'voltage': tel.voltage,
                'temperature': tel.temperature,
                'temperature_f': tel.temperature_f,
                'relative_humidity': tel.relative_humidity,
                'barometric_pressure': tel.barometric_pressure,
                'gas_resistance': tel.gas_resistance,
                'iaq': tel.iaq,
                'lux': tel.lux,
                'wind_speed': tel.wind_speed,
                'wind_direction': tel.wind_direction,
                'pm25_standard': tel.pm25_standard,
                'co2': tel.co2,
                'channel_utilization': tel.channel_utilization,
                'air_util_tx': tel.air_util_tx,
                'uptime_seconds': tel.uptime_seconds,
            })
        
        elif self.type == 'nodeinfo' and self.payload:
            node = NodeInfoPayload(**self.payload)
            row.update({
                'text_message': f"{node.long_name} ({node.short_name})",
            })
        
        elif self.type == 'text' and self.payload:
            if isinstance(self.payload, str):
                row['text_message'] = self.payload
            elif 'text' in self.payload:
                row['text_message'] = self.payload['text']
        
        return {k: v for k, v in row.items() if v is not None}


class ValidationResult(BaseModel):
    """Result of validation operation"""
    valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    data: Optional[Dict[str, Any]] = None


def validate_mqtt_message(raw_message: Dict[str, Any]) -> ValidationResult:
    """Validate an MQTT message from Meshtastic broker"""
    errors = []
    warnings = []
    
    try:
        if 'from' not in raw_message:
            errors.append("Missing required field: 'from'")
        
        if 'type' not in raw_message:
            errors.append("Missing required field: 'type'")
        
        if 'timestamp' not in raw_message:
            warnings.append("Missing timestamp, will use current time")
            raw_message['timestamp'] = int(datetime.utcnow().timestamp())
        
        if 'id' not in raw_message:
            warnings.append("Missing packet ID")
            raw_message['id'] = 0
        
        if errors:
            return ValidationResult(valid=False, errors=errors, warnings=warnings)
        
        message = MQTTMessage(**raw_message)
        
        row = message.to_snowflake_row()
        
        return ValidationResult(
            valid=True,
            errors=[],
            warnings=warnings,
            data=row
        )
        
    except Exception as e:
        errors.append(f"Validation error: {str(e)}")
        return ValidationResult(valid=False, errors=errors, warnings=warnings)


def validate_snowflake_row(row: Dict[str, Any]) -> ValidationResult:
    """Validate a row before inserting to Snowflake"""
    errors = []
    warnings = []
    
    if row.get('latitude') is not None:
        lat = row['latitude']
        if lat < -90 or lat > 90:
            errors.append(f"Invalid latitude: {lat}")
    
    if row.get('longitude') is not None:
        lon = row['longitude']
        if lon < -180 or lon > 180:
            errors.append(f"Invalid longitude: {lon}")
    
    if row.get('battery_level') is not None:
        bat = row['battery_level']
        if bat < 0 or bat > 101:
            warnings.append(f"Unusual battery level: {bat}")
    
    if row.get('temperature') is not None:
        temp = row['temperature']
        if temp < -50 or temp > 100:
            warnings.append(f"Unusual temperature: {temp}°C")
    
    return ValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        data=row if len(errors) == 0 else None
    )


class HealthCheck(BaseModel):
    """Health check response"""
    status: str = Field("healthy", description="Overall status")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    checks: Dict[str, bool] = Field(default_factory=dict)
    details: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def is_healthy(self) -> bool:
        return all(self.checks.values()) if self.checks else True


def create_health_check(
    snowflake_ok: bool = True,
    mqtt_ok: bool = True,
    api_ok: bool = True,
    details: Optional[Dict[str, Any]] = None
) -> HealthCheck:
    """Create a health check response"""
    checks = {
        'snowflake': snowflake_ok,
        'mqtt': mqtt_ok,
        'api': api_ok
    }
    
    status = "healthy" if all(checks.values()) else "degraded"
    if not any(checks.values()):
        status = "unhealthy"
    
    return HealthCheck(
        status=status,
        checks=checks,
        details=details or {}
    )
