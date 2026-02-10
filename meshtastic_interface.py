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
        connection_type: str = 'auto',
        device_path: str = None,
        hostname: str = None,
        ble_address: str = None,
        on_message_callback: Callable = None,
        ble_scan_timeout: float = 10.0
    ):
        self.connection_type = connection_type
        self.device_path = device_path
        self.hostname = hostname
        self.ble_address = ble_address
        self.on_message_callback = on_message_callback
        self.ble_scan_timeout = ble_scan_timeout
        self.interface = None
        self.running = False
        self.message_queue = []
        self.connected_via = None
    
    @staticmethod
    def scan_serial_devices() -> List[Dict]:
        """Scan for available Meshtastic serial devices"""
        try:
            import serial.tools.list_ports
            
            logger.info("Scanning for serial devices...")
            ports = list(serial.tools.list_ports.comports())
            meshtastic_devices = []
            
            for port in ports:
                desc = (port.description or "").lower()
                mfr = (port.manufacturer or "").lower()
                device = port.device.lower()
                
                if "boot" in desc:
                    logger.debug(f"Skipping bootloader device: {port.device}")
                    continue
                
                is_meshtastic = any(kw in desc or kw in mfr for kw in [
                    "meshtastic", "t1000", "sensecap", "cp210", "ch340", 
                    "ch9102", "ftdi", "silabs", "silicon labs", "esp32",
                    "seeed", "heltec", "lilygo", "rak", "wisblock"
                ])
                
                is_usb_serial = "usbmodem" in device or "usbserial" in device or "wchusbserial" in device
                
                if is_meshtastic or is_usb_serial:
                    meshtastic_devices.append({
                        'port': port.device,
                        'description': port.description,
                        'manufacturer': port.manufacturer,
                        'vid': port.vid,
                        'pid': port.pid
                    })
                    logger.info(f"Found serial device: {port.device} - {port.description}")
            
            return meshtastic_devices
        except ImportError:
            logger.error("Serial scanning requires 'pyserial' package")
            return []
        except Exception as e:
            logger.error(f"Serial scan error: {e}")
            return []
    
    def scan_ble_devices(self) -> List[Dict]:
        """Scan for available Meshtastic BLE devices"""
        try:
            import asyncio
            from bleak import BleakScanner
            
            logger.info(f"Scanning for Bluetooth devices ({self.ble_scan_timeout}s)...")
            
            async def scan():
                devices = await BleakScanner.discover(timeout=self.ble_scan_timeout)
                meshtastic_devices = []
                
                for d in devices:
                    name = d.name or ""
                    if any(kw in name.lower() for kw in ["meshtastic", "t1000", "sensecap", "mesh", "tracker"]):
                        meshtastic_devices.append({
                            'address': d.address,
                            'name': d.name,
                            'rssi': getattr(d, 'rssi', None),
                            'type': 'ble'
                        })
                        logger.info(f"Found BLE device: {d.name} ({d.address}) RSSI: {getattr(d, 'rssi', 'N/A')}")
                
                return meshtastic_devices
            
            return asyncio.run(scan())
        except ImportError:
            logger.warning("BLE scanning requires 'bleak' package: pip install bleak")
            return []
        except Exception as e:
            logger.error(f"BLE scan error: {e}")
            return []
    
    def scan_all_devices(self) -> Dict[str, List[Dict]]:
        """Scan for all available Meshtastic devices (BLE and serial)"""
        logger.info("=" * 50)
        logger.info("SCANNING FOR MESHTASTIC DEVICES")
        logger.info("=" * 50)
        
        results = {
            'ble': [],
            'serial': []
        }
        
        results['serial'] = self.scan_serial_devices()
        results['ble'] = self.scan_ble_devices()
        
        total = len(results['serial']) + len(results['ble'])
        logger.info(f"Scan complete: {len(results['serial'])} serial, {len(results['ble'])} BLE devices found")
        logger.info("=" * 50)
        
        return results
        
    def connect(self):
        import meshtastic
        
        if self.connection_type == 'auto':
            return self._connect_auto()
        elif self.connection_type == 'serial':
            return self._connect_serial()
        elif self.connection_type == 'tcp':
            return self._connect_tcp()
        elif self.connection_type == 'ble':
            return self._connect_ble_with_fallback()
        elif self.connection_type == 'test':
            logger.info("Test mode - no device connection")
            return self
        else:
            raise ValueError(f"Unknown connection type: {self.connection_type}")
    
    def _connect_auto(self):
        """Auto-detect and connect to best available device - BLE first, then serial"""
        import meshtastic.serial_interface
        
        devices = self.scan_all_devices()
        
        if devices['ble']:
            logger.info("Trying BLE connections first (preferred)...")
            ble_4b14 = [d for d in devices['ble'] if '4b14' in (d.get('name') or '').lower() or '4b14' in d.get('address', '').lower()]
            ble_others = [d for d in devices['ble'] if d not in ble_4b14]
            ble_ordered = ble_4b14 + ble_others
            
            for dev in ble_ordered:
                try:
                    self.device_path = dev['address']
                    logger.info(f"Trying BLE: {dev.get('name', 'Unknown')} ({self.device_path})")
                    self._connect_ble()
                    return self
                except Exception as e:
                    logger.warning(f"BLE connection to {dev['address']} failed: {e}")
        
        if self.ble_address:
            logger.info(f"Trying known BLE address: {self.ble_address}")
            try:
                self.device_path = self.ble_address
                self._connect_ble()
                return self
            except Exception as e:
                logger.warning(f"Known BLE address failed: {e}")
        
        if devices['serial']:
            logger.info("Falling back to serial connections...")
            serial_4b14 = [d for d in devices['serial'] if '4b14' in (d.get('description') or '').lower()]
            serial_others = [d for d in devices['serial'] if d not in serial_4b14]
            serial_ordered = serial_4b14 + serial_others
            
            for dev in serial_ordered:
                try:
                    self.device_path = dev['port']
                    logger.info(f"Trying serial port: {self.device_path}")
                    self.interface = meshtastic.serial_interface.SerialInterface(devPath=self.device_path)
                    self.connected_via = 'serial'
                    logger.info(f"Connected via serial: {self.device_path}")
                    self._setup_subscriptions()
                    return self
                except Exception as e:
                    logger.warning(f"Serial connection to {dev['port']} failed: {e}")
        
        logger.info("Attempting meshtastic native auto-detection as last resort...")
        try:
            self.interface = meshtastic.serial_interface.SerialInterface()
            self.connected_via = 'serial'
            logger.info("Connected via serial: auto-detected by meshtastic")
            self._setup_subscriptions()
            return self
        except Exception as e:
            logger.warning(f"Native auto-detection failed: {e}")
        
        raise ConnectionError("No Meshtastic devices found or all connections failed. Make sure device is on and connected.")
    
    def _connect_serial(self):
        """Connect via serial port"""
        import meshtastic.serial_interface
        
        if not self.device_path:
            devices = self.scan_serial_devices()
            if devices:
                self.device_path = devices[0]['port']
                logger.info(f"Auto-selected serial: {self.device_path}")
            else:
                logger.info("No device path specified, letting meshtastic auto-detect...")
        
        if self.device_path:
            self.interface = meshtastic.serial_interface.SerialInterface(devPath=self.device_path)
        else:
            self.interface = meshtastic.serial_interface.SerialInterface()
        
        self.connected_via = 'serial'
        logger.info(f"Connected via serial: {self.device_path or 'auto-detected'}")
        
        self._setup_subscriptions()
        return self
    
    def _connect_tcp(self):
        """Connect via TCP/IP"""
        import meshtastic.tcp_interface
        
        if not self.hostname:
            raise ValueError("hostname required for TCP connection")
        
        self.interface = meshtastic.tcp_interface.TCPInterface(hostname=self.hostname)
        self.connected_via = 'tcp'
        logger.info(f"Connected via TCP to {self.hostname}")
        
        self._setup_subscriptions()
        return self
    
    def _connect_ble_with_fallback(self):
        """Connect via BLE with fallback to serial if it fails"""
        try:
            self._connect_ble()
            return self
        except Exception as e:
            logger.warning(f"BLE connection failed: {e}")
            logger.info("Falling back to serial connection...")
            
            serial_devices = self.scan_serial_devices()
            if serial_devices:
                self.device_path = serial_devices[0]['port']
                self._connect_serial()
                return self
            else:
                raise ConnectionError(f"BLE failed ({e}) and no serial devices found")
    
    def _connect_ble(self):
        """Connect via Bluetooth Low Energy"""
        import meshtastic.ble_interface
        
        ble_addr = self.device_path or self.ble_address
        
        if not ble_addr:
            logger.info("No BLE address specified, scanning for devices...")
            devices = self.scan_ble_devices()
            if devices:
                ble_addr = devices[0]['address']
                logger.info(f"Auto-selected: {devices[0].get('name')} ({ble_addr})")
            else:
                raise ValueError(
                    "No BLE device address provided and none found via scan. "
                    "Tips: 1) Ensure device is on, 2) Specify address with device_path parameter"
                )
        
        logger.info(f"Connecting to BLE device: {ble_addr}")
        self.interface = meshtastic.ble_interface.BLEInterface(address=ble_addr)
        self.device_path = ble_addr
        self.connected_via = 'ble'
        logger.info(f"Connected via BLE: {ble_addr}")
        
        self._setup_subscriptions()
        return self
    
    def _setup_subscriptions(self):
        """Setup pubsub message subscriptions"""
        pub.subscribe(self._on_receive, "meshtastic.receive")
        pub.subscribe(self._on_connection, "meshtastic.connection.established")
        pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")
        time.sleep(2)
    
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
            decoded = packet.get('decoded', {})
            portnum = decoded.get('portnum', '')
            portnum_str = str(portnum)
            
            is_position = (
                'POSITION' in portnum_str or 
                portnum == 3 or 
                'position' in decoded
            )
            is_text = (
                'TEXT' in portnum_str or 
                portnum == 1 or 
                'text' in decoded
            )
            is_telemetry = (
                'TELEMETRY' in portnum_str or 
                portnum == 67 or 
                'telemetry' in decoded
            )
            is_nodeinfo = (
                'NODEINFO' in portnum_str or 
                portnum == 4 or 
                'user' in decoded
            )
            
            logger.debug(f"Packet portnum={portnum} ({portnum_str}), decoded keys: {list(decoded.keys())}")
            
            if is_position:
                message = self._parse_position_packet(packet)
                if message:
                    logger.info(f"Position from {message.get('from_id')}: lat={message.get('latitude')}, lon={message.get('longitude')}, alt={message.get('altitude')}")
            elif is_text:
                message = self._parse_text_packet(packet)
                if message:
                    text = message.get('text', '')[:50]
                    logger.info(f"Text from {message.get('from_id')}: {text}")
            elif is_telemetry:
                message = self._parse_telemetry_packet(packet)
                if message:
                    temp = message.get('temperature')
                    bat = message.get('battery_level')
                    volt = message.get('voltage')
                    logger.info(f"Telemetry from {message.get('from_id')}: temp={temp}, battery={bat}%, voltage={volt}V")
            elif is_nodeinfo:
                message = self._parse_nodeinfo_packet(packet)
                if message:
                    logger.info(f"NodeInfo from {message.get('from_id')}: {message.get('long_name')}")
            else:
                message = self._parse_packet(packet)
                logger.info(f"Other packet type '{portnum}' from {message.get('from_id')}, decoded: {list(decoded.keys())}")
            
            if message:
                self.message_queue.append(message)
                if self.on_message_callback:
                    self.on_message_callback(message)
                    
        except Exception as e:
            logger.error(f"Error processing packet: {e}", exc_info=True)
    
    def _parse_nodeinfo_packet(self, packet: Dict) -> Optional[Dict]:
        message = self._parse_packet(packet)
        if not message:
            return None
        
        message['packet_type'] = 'nodeinfo'
        
        decoded = packet.get('decoded', {})
        user = decoded.get('user', {})
        
        message.update({
            'user_id': user.get('id'),
            'long_name': user.get('longName'),
            'short_name': user.get('shortName'),
            'hw_model': str(user.get('hwModel')) if user.get('hwModel') else None,
            'is_licensed': user.get('isLicensed'),
            'role': str(user.get('role')) if user.get('role') else None,
        })
        
        return message
    
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
        
        lat = position.get('latitude')
        if lat is None and position.get('latitudeI'):
            lat = position.get('latitudeI') / 1e7
        lon = position.get('longitude')
        if lon is None and position.get('longitudeI'):
            lon = position.get('longitudeI') / 1e7
        
        message.update({
            'latitude': lat,
            'longitude': lon,
            'altitude': position.get('altitude'),
            'altitude_hae': position.get('altitudeHae'),
            'altitude_geoidal_separation': position.get('altitudeGeoidalSeparation'),
            'ground_speed': position.get('groundSpeed'),
            'ground_track': position.get('groundTrack'),
            'precision_bits': position.get('precisionBits'),
            'sats_in_view': position.get('satsInView'),
            'pdop': position.get('PDOP'),
            'hdop': position.get('HDOP'),
            'vdop': position.get('VDOP'),
            'gps_timestamp': position.get('time'),
            'fix_quality': position.get('fixQuality'),
            'fix_type': position.get('fixType'),
            'position_source': position.get('locSource', 'unknown'),
            'seq_number': position.get('seqNumber'),
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
        
        try:
            if hasattr(self.interface, 'localNode') and self.interface.localNode:
                node = self.interface.localNode
            elif hasattr(self.interface, 'getNode'):
                node = self.interface.getNode('^local')
            else:
                return {}
            
            if not node:
                return {}
            
            return {
                'node_id': getattr(node, 'nodeId', None),
                'node_num': getattr(node, 'nodeNum', None),
                'local_config': str(getattr(node, 'localConfig', {})),
                'module_config': str(getattr(node, 'moduleConfig', {}))
            }
        except Exception as e:
            logger.warning(f"Could not get local node info: {e}")
            return {}
    
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
