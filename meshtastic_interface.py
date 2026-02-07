#!/usr/bin/env python3
import logging
import time
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable, Any
from pubsub import pub

logger = logging.getLogger(__name__)


class MeshtasticReceiver:
    def __init__(
        self,
        connection_type: str = 'serial',
        device_path: str = None,
        hostname: str = None,
        on_message_callback: Callable = None,
        ble_scan_timeout: float = 10.0
    ):
        self.connection_type = connection_type
        self.device_path = device_path
        self.hostname = hostname
        self.on_message_callback = on_message_callback
        self.ble_scan_timeout = ble_scan_timeout
        self.interface = None
        self.running = False
        self.message_queue = []
    
    def scan_ble_devices(self) -> List[Dict]:
        """Scan for available Meshtastic BLE devices"""
        try:
            import asyncio
            from bleak import BleakScanner
            
            logger.info("Scanning for Bluetooth devices...")
            
            async def scan():
                devices = await BleakScanner.discover(timeout=self.ble_scan_timeout)
                meshtastic_devices = []
                
                for d in devices:
                    name = d.name or ""
                    if any(kw in name.lower() for kw in ["meshtastic", "t1000", "sensecap", "mesh"]):
                        meshtastic_devices.append({
                            'address': d.address,
                            'name': d.name,
                            'rssi': getattr(d, 'rssi', None)
                        })
                        logger.info(f"Found Meshtastic device: {d.name} ({d.address})")
                
                return meshtastic_devices
            
            return asyncio.run(scan())
        except ImportError:
            logger.error("BLE scanning requires 'bleak' package: pip install bleak")
            return []
        except Exception as e:
            logger.error(f"BLE scan error: {e}")
            return []
        
    def connect(self):
        import meshtastic
        
        if self.connection_type == 'serial':
            import meshtastic.serial_interface
            if self.device_path:
                self.interface = meshtastic.serial_interface.SerialInterface(devPath=self.device_path)
            else:
                self.interface = meshtastic.serial_interface.SerialInterface()
            logger.info(f"Connected via serial: {self.device_path or 'auto-detected'}")
            
        elif self.connection_type == 'tcp':
            import meshtastic.tcp_interface
            if not self.hostname:
                raise ValueError("hostname required for TCP connection")
            self.interface = meshtastic.tcp_interface.TCPInterface(hostname=self.hostname)
            logger.info(f"Connected via TCP to {self.hostname}")
            
        elif self.connection_type == 'ble':
            self._connect_ble()
            
        else:
            raise ValueError(f"Unknown connection type: {self.connection_type}")
        
        pub.subscribe(self._on_receive, "meshtastic.receive")
        pub.subscribe(self._on_connection, "meshtastic.connection.established")
        pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")
        pub.subscribe(self._on_position, "meshtastic.receive.position")
        pub.subscribe(self._on_text, "meshtastic.receive.text")
        pub.subscribe(self._on_telemetry, "meshtastic.receive.data.67")
        
        time.sleep(2)
        
        return self
    
    def _connect_ble(self):
        """Connect via Bluetooth Low Energy"""
        import meshtastic.ble_interface
        
        if not self.device_path:
            logger.info("No BLE address specified, scanning for devices...")
            devices = self.scan_ble_devices()
            if devices:
                self.device_path = devices[0]['address']
                logger.info(f"Auto-selected: {devices[0].get('name')} ({self.device_path})")
            else:
                raise ValueError(
                    "No BLE device address provided and none found via scan. "
                    "Tips: 1) Ensure device is on, 2) Specify address with device_path parameter"
                )
        
        logger.info(f"Connecting to BLE device: {self.device_path}")
        self.interface = meshtastic.ble_interface.BLEInterface(address=self.device_path)
        logger.info(f"Connected via BLE: {self.device_path}")
    
    def _on_connection(self, interface, topic=pub.AUTO_TOPIC):
        logger.info("Connected to Meshtastic device")
        self._log_device_info()
    
    def _on_connection_lost(self, interface, topic=pub.AUTO_TOPIC):
        logger.warning("Lost connection to Meshtastic device")
    
    def _log_device_info(self):
        if self.interface:
            my_info = self.interface.myInfo
            if my_info:
                logger.info(f"Device ID: {my_info.my_node_num}")
            
            if hasattr(self.interface, 'nodes') and self.interface.nodes:
                logger.info(f"Known nodes in mesh: {len(self.interface.nodes)}")
    
    def _on_receive(self, packet, interface):
        try:
            message = self._parse_packet(packet)
            if message:
                logger.debug(f"Received packet type: {message.get('packet_type')}")
                self.message_queue.append(message)
                
                if self.on_message_callback:
                    self.on_message_callback(message)
        except Exception as e:
            logger.error(f"Error processing packet: {e}", exc_info=True)
    
    def _on_position(self, packet, interface):
        try:
            message = self._parse_position_packet(packet)
            if message:
                logger.info(f"Position from {message.get('from_id')}: lat={message.get('latitude')}, lon={message.get('longitude')}")
                self.message_queue.append(message)
                
                if self.on_message_callback:
                    self.on_message_callback(message)
        except Exception as e:
            logger.error(f"Error processing position: {e}", exc_info=True)
    
    def _on_text(self, packet, interface):
        try:
            message = self._parse_text_packet(packet)
            if message:
                logger.info(f"Text from {message.get('from_id')}: {message.get('text')[:50]}...")
                self.message_queue.append(message)
                
                if self.on_message_callback:
                    self.on_message_callback(message)
        except Exception as e:
            logger.error(f"Error processing text message: {e}", exc_info=True)
    
    def _on_telemetry(self, packet, interface):
        try:
            message = self._parse_telemetry_packet(packet)
            if message:
                temp = message.get('temperature')
                if temp is not None:
                    temp_f = (temp * 9/5) + 32
                    logger.info(f"Telemetry from {message.get('from_id')}: temp={temp:.1f}°C/{temp_f:.1f}°F")
                else:
                    logger.debug(f"Telemetry from {message.get('from_id')}")
                self.message_queue.append(message)
                
                if self.on_message_callback:
                    self.on_message_callback(message)
        except Exception as e:
            logger.error(f"Error processing telemetry: {e}", exc_info=True)
    
    def _parse_packet(self, packet: Dict) -> Optional[Dict]:
        from_id = packet.get('fromId', packet.get('from'))
        to_id = packet.get('toId', packet.get('to'))
        
        decoded = packet.get('decoded', {})
        portnum = decoded.get('portnum')
        
        message = {
            'packet_type': 'raw',
            'received_at': datetime.now(timezone.utc).isoformat(),
            'from_id': str(from_id) if from_id else None,
            'from_num': packet.get('from'),
            'to_id': str(to_id) if to_id else None,
            'to_num': packet.get('to'),
            'channel': packet.get('channel', 0),
            'portnum': portnum,
            'hop_limit': packet.get('hopLimit'),
            'hop_start': packet.get('hopStart'),
            'want_ack': packet.get('wantAck', False),
            'rx_snr': packet.get('rxSnr'),
            'rx_rssi': packet.get('rxRssi'),
            'rx_time': packet.get('rxTime'),
            'raw_packet': packet
        }
        
        return message
    
    def _parse_position_packet(self, packet: Dict) -> Optional[Dict]:
        message = self._parse_packet(packet)
        if not message:
            return None
        
        message['packet_type'] = 'position'
        
        decoded = packet.get('decoded', {})
        position = decoded.get('position', {})
        
        message.update({
            'latitude': position.get('latitude') or position.get('latitudeI', 0) / 1e7 if position.get('latitudeI') else None,
            'longitude': position.get('longitude') or position.get('longitudeI', 0) / 1e7 if position.get('longitudeI') else None,
            'altitude': position.get('altitude'),
            'ground_speed': position.get('groundSpeed'),
            'ground_track': position.get('groundTrack'),
            'precision_bits': position.get('precisionBits'),
            'sats_in_view': position.get('satsInView'),
            'pdop': position.get('PDOP'),
            'hdop': position.get('HDOP'),
            'vdop': position.get('VDOP'),
            'gps_timestamp': position.get('time'),
            'position_source': position.get('locSource', 'unknown')
        })
        
        del message['raw_packet']
        message['raw_packet'] = packet
        
        return message
    
    def _parse_text_packet(self, packet: Dict) -> Optional[Dict]:
        message = self._parse_packet(packet)
        if not message:
            return None
        
        message['packet_type'] = 'text'
        
        decoded = packet.get('decoded', {})
        
        message.update({
            'text': decoded.get('text', ''),
            'payload': decoded.get('payload')
        })
        
        return message
    
    def _parse_telemetry_packet(self, packet: Dict) -> Optional[Dict]:
        message = self._parse_packet(packet)
        if not message:
            return None
        
        message['packet_type'] = 'telemetry'
        
        decoded = packet.get('decoded', {})
        telemetry = decoded.get('telemetry', {})
        
        device_metrics = telemetry.get('deviceMetrics', {})
        environment_metrics = telemetry.get('environmentMetrics', {})
        power_metrics = telemetry.get('powerMetrics', {})
        air_quality_metrics = telemetry.get('airQualityMetrics', {})
        
        message.update({
            'telemetry_time': telemetry.get('time'),
            'battery_level': device_metrics.get('batteryLevel'),
            'voltage': device_metrics.get('voltage'),
            'channel_utilization': device_metrics.get('channelUtilization'),
            'air_util_tx': device_metrics.get('airUtilTx'),
            'uptime_seconds': device_metrics.get('uptimeSeconds'),
            'temperature': environment_metrics.get('temperature'),
            'temperature_f': ((environment_metrics.get('temperature') * 9/5) + 32) if environment_metrics.get('temperature') is not None else None,
            'relative_humidity': environment_metrics.get('relativeHumidity'),
            'barometric_pressure': environment_metrics.get('barometricPressure'),
            'gas_resistance': environment_metrics.get('gasResistance'),
            'iaq': environment_metrics.get('iaq'),
            'lux': environment_metrics.get('lux'),
            'white_lux': environment_metrics.get('whiteLux'),
            'ir_lux': environment_metrics.get('irLux'),
            'uv_lux': environment_metrics.get('uvLux'),
            'wind_direction': environment_metrics.get('windDirection'),
            'wind_speed': environment_metrics.get('windSpeed'),
            'wind_gust': environment_metrics.get('windGust'),
            'weight': environment_metrics.get('weight'),
            'distance': environment_metrics.get('distance'),
            'ch1_voltage': power_metrics.get('ch1Voltage'),
            'ch1_current': power_metrics.get('ch1Current'),
            'ch2_voltage': power_metrics.get('ch2Voltage'),
            'ch2_current': power_metrics.get('ch2Current'),
            'ch3_voltage': power_metrics.get('ch3Voltage'),
            'ch3_current': power_metrics.get('ch3Current'),
            'pm10_standard': air_quality_metrics.get('pm10Standard'),
            'pm25_standard': air_quality_metrics.get('pm25Standard'),
            'pm100_standard': air_quality_metrics.get('pm100Standard'),
            'pm10_environmental': air_quality_metrics.get('pm10Environmental'),
            'pm25_environmental': air_quality_metrics.get('pm25Environmental'),
            'pm100_environmental': air_quality_metrics.get('pm100Environmental'),
            'co2': air_quality_metrics.get('co2')
        })
        
        return message
    
    def get_local_node_info(self) -> Dict:
        if not self.interface:
            return {}
        
        node = self.interface.getNode('^local')
        if not node:
            return {}
        
        return {
            'node_id': getattr(node, 'nodeId', None),
            'node_num': getattr(node, 'nodeNum', None),
            'local_config': str(getattr(node, 'localConfig', {})),
            'module_config': str(getattr(node, 'moduleConfig', {}))
        }
    
    def get_all_nodes(self) -> List[Dict]:
        if not self.interface or not hasattr(self.interface, 'nodes'):
            return []
        
        nodes = []
        for node_id, node_info in self.interface.nodes.items():
            user = node_info.get('user', {})
            position = node_info.get('position', {})
            device_metrics = node_info.get('deviceMetrics', {})
            
            nodes.append({
                'node_id': node_id,
                'node_num': node_info.get('num'),
                'user_id': user.get('id'),
                'long_name': user.get('longName'),
                'short_name': user.get('shortName'),
                'hw_model': user.get('hwModel'),
                'is_licensed': user.get('isLicensed', False),
                'latitude': position.get('latitude'),
                'longitude': position.get('longitude'),
                'altitude': position.get('altitude'),
                'last_heard': node_info.get('lastHeard'),
                'snr': node_info.get('snr'),
                'hops_away': node_info.get('hopsAway'),
                'battery_level': device_metrics.get('batteryLevel'),
                'voltage': device_metrics.get('voltage')
            })
        
        return nodes
    
    def send_text(self, text: str, destination: str = None) -> bool:
        if not self.interface:
            logger.error("Not connected to Meshtastic device")
            return False
        
        try:
            if destination:
                self.interface.sendText(text, destinationId=destination)
            else:
                self.interface.sendText(text)
            logger.info(f"Sent text message: {text[:50]}...")
            return True
        except Exception as e:
            logger.error(f"Failed to send text: {e}")
            return False
    
    def request_position(self, destination: str = None) -> bool:
        if not self.interface:
            return False
        
        try:
            self.interface.sendPosition()
            return True
        except Exception as e:
            logger.error(f"Failed to request position: {e}")
            return False
    
    def get_queued_messages(self) -> List[Dict]:
        messages = self.message_queue.copy()
        self.message_queue.clear()
        return messages
    
    def run_forever(self):
        self.running = True
        logger.info("Starting Meshtastic receiver loop")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.close()
    
    def close(self):
        self.running = False
        if self.interface:
            try:
                self.interface.close()
                logger.info("Meshtastic interface closed")
            except Exception as e:
                logger.error(f"Error closing interface: {e}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    def on_message(msg):
        print(f"\n=== Message Received ===")
        print(json.dumps(msg, indent=2, default=str))
    
    receiver = MeshtasticReceiver(
        connection_type='serial',
        on_message_callback=on_message
    )
    
    try:
        receiver.connect()
        print("\nLocal node info:")
        print(json.dumps(receiver.get_local_node_info(), indent=2))
        
        print("\nKnown nodes in mesh:")
        for node in receiver.get_all_nodes():
            print(f"  - {node.get('long_name', 'Unknown')} ({node.get('node_id')})")
        
        print("\nListening for messages (Ctrl+C to stop)...")
        receiver.run_forever()
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        receiver.close()


if __name__ == '__main__':
    main()
