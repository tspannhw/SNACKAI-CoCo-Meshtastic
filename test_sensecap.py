#!/usr/bin/env python3
"""
Test script for SenseCAP Card Tracker T1000-E
Retrieves device info, GPS position, and all sensor readings
Supports Serial, TCP, and BLE (Bluetooth) connections
"""
import argparse
import asyncio
import json
import logging
import time
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def scan_ble_devices() -> List[Dict]:
    """Scan for Meshtastic BLE devices"""
    try:
        import asyncio
        from bleak import BleakScanner
        
        print("\nScanning for Bluetooth devices...")
        
        async def scan():
            devices = await BleakScanner.discover(timeout=10.0)
            meshtastic_devices = []
            
            for d in devices:
                name = d.name or ""
                if "meshtastic" in name.lower() or "t1000" in name.lower() or "sensecap" in name.lower():
                    meshtastic_devices.append({
                        'address': d.address,
                        'name': d.name,
                        'rssi': d.rssi if hasattr(d, 'rssi') else None
                    })
                    print(f"  Found: {d.name} ({d.address})")
            
            if not meshtastic_devices:
                print("  No Meshtastic devices found. Showing all BLE devices:")
                for d in devices[:10]:
                    print(f"    {d.name or 'Unknown'} ({d.address})")
            
            return meshtastic_devices
        
        return asyncio.run(scan())
    except ImportError:
        print("  BLE scanning requires 'bleak' package: pip install bleak")
        return []
    except Exception as e:
        print(f"  BLE scan error: {e}")
        return []


class SenseCapTester:
    def __init__(self, connection_type: str = 'serial', device_path: str = None, hostname: str = None):
        self.connection_type = connection_type
        self.device_path = device_path
        self.hostname = hostname
        self.interface = None
        self.collected_data = {
            'device_info': {},
            'position': {},
            'telemetry': {},
            'environmental': {},
            'node_db': []
        }
    
    def connect(self):
        import meshtastic
        
        print(f"\n{'='*60}")
        print("SENSECAP CARD TRACKER T1000-E TEST")
        print(f"{'='*60}")
        print(f"Connection type: {self.connection_type}")
        
        if self.connection_type == 'serial':
            import meshtastic.serial_interface
            print("Scanning for serial devices...")
            
            from meshtastic.util import findPorts
            ports = findPorts()
            if ports:
                print(f"Found ports: {ports}")
            else:
                print("No serial ports found. Try BLE with: -t ble")
            
            if self.device_path:
                self.interface = meshtastic.serial_interface.SerialInterface(devPath=self.device_path)
            else:
                self.interface = meshtastic.serial_interface.SerialInterface()
            print(f"Connected via serial: {self.device_path or 'auto-detected'}")
            
        elif self.connection_type == 'tcp':
            import meshtastic.tcp_interface
            if not self.hostname:
                raise ValueError("hostname required for TCP connection")
            self.interface = meshtastic.tcp_interface.TCPInterface(hostname=self.hostname)
            print(f"Connected via TCP to {self.hostname}")
            
        elif self.connection_type == 'ble':
            self._connect_ble()
        
        time.sleep(3)
        return self
    
    def _connect_ble(self):
        """Connect via Bluetooth Low Energy"""
        import meshtastic.ble_interface
        
        print("Connecting via Bluetooth LE...")
        
        if not self.device_path:
            devices = scan_ble_devices()
            if devices:
                self.device_path = devices[0]['address']
                print(f"Auto-selected device: {devices[0].get('name')} ({self.device_path})")
            else:
                print("\nNo device address provided and none found via scan.")
                print("Tips for T1000-E BLE connection:")
                print("  1. Ensure device is powered on (press button)")
                print("  2. Device should show in Bluetooth settings")
                print("  3. Try specifying address: -t ble -p XX:XX:XX:XX:XX:XX")
                print("  4. On macOS, address format is UUID, e.g., 12345678-1234-1234-1234-123456789ABC")
                raise ValueError("No BLE device address provided or found")
        
        print(f"Connecting to BLE device: {self.device_path}")
        
        try:
            self.interface = meshtastic.ble_interface.BLEInterface(address=self.device_path)
            print(f"Connected via BLE to {self.device_path}")
        except Exception as e:
            print(f"\nBLE connection failed: {e}")
            print("\nTroubleshooting:")
            print("  - Ensure Bluetooth is enabled on your computer")
            print("  - T1000-E must be awake (press button)")
            print("  - Try: python test_sensecap.py --scan-ble")
            print("  - On macOS: System Settings > Privacy & Security > Bluetooth")
            raise
    
    def get_device_info(self) -> Dict:
        print(f"\n{'='*60}")
        print("DEVICE INFORMATION")
        print(f"{'='*60}")
        
        info = {}
        
        if self.interface.myInfo:
            my_info = self.interface.myInfo
            info['my_node_num'] = my_info.my_node_num
            info['max_channels'] = getattr(my_info, 'max_channels', None)
            info['has_wifi'] = getattr(my_info, 'has_wifi', None)
            info['has_bluetooth'] = getattr(my_info, 'has_bluetooth', None)
            info['has_ethernet'] = getattr(my_info, 'has_ethernet', None)
            info['pio_env'] = getattr(my_info, 'pio_env', None)
        
        if hasattr(self.interface, 'metadata') and self.interface.metadata:
            metadata = self.interface.metadata
            info['firmware_version'] = getattr(metadata, 'firmware_version', None)
            info['device_state_version'] = getattr(metadata, 'device_state_version', None)
            info['hw_model'] = str(getattr(metadata, 'hw_model', None))
            info['has_remote_hardware'] = getattr(metadata, 'hasRemoteHardware', None)
        
        local_node = self.interface.getNode('^local')
        if local_node:
            info['node_id'] = getattr(local_node, 'nodeId', None)
            info['node_num'] = getattr(local_node, 'nodeNum', None)
            
            if hasattr(local_node, 'localConfig'):
                lc = local_node.localConfig
                info['config'] = {
                    'lora': {
                        'region': str(getattr(lc.lora, 'region', None)) if hasattr(lc, 'lora') else None,
                        'modem_preset': str(getattr(lc.lora, 'modem_preset', None)) if hasattr(lc, 'lora') else None,
                        'hop_limit': getattr(lc.lora, 'hop_limit', None) if hasattr(lc, 'lora') else None,
                        'tx_power': getattr(lc.lora, 'tx_power', None) if hasattr(lc, 'lora') else None,
                    },
                    'position': {
                        'gps_enabled': getattr(lc.position, 'gps_enabled', None) if hasattr(lc, 'position') else None,
                        'gps_update_interval': getattr(lc.position, 'gps_update_interval', None) if hasattr(lc, 'position') else None,
                        'position_broadcast_secs': getattr(lc.position, 'position_broadcast_secs', None) if hasattr(lc, 'position') else None,
                    },
                    'power': {
                        'is_power_saving': getattr(lc.power, 'is_power_saving', None) if hasattr(lc, 'power') else None,
                        'on_battery_shutdown_after_secs': getattr(lc.power, 'on_battery_shutdown_after_secs', None) if hasattr(lc, 'power') else None,
                    },
                    'telemetry': {
                        'device_update_interval': getattr(lc.telemetry, 'device_update_interval', None) if hasattr(lc, 'telemetry') else None,
                        'environment_update_interval': getattr(lc.telemetry, 'environment_update_interval', None) if hasattr(lc, 'telemetry') else None,
                        'environment_measurement_enabled': getattr(lc.telemetry, 'environment_measurement_enabled', None) if hasattr(lc, 'telemetry') else None,
                        'environment_screen_enabled': getattr(lc.telemetry, 'environment_screen_enabled', None) if hasattr(lc, 'telemetry') else None,
                    }
                }
            
            if hasattr(local_node, 'moduleConfig'):
                mc = local_node.moduleConfig
                if hasattr(mc, 'telemetry'):
                    info['module_telemetry'] = {
                        'device_update_interval': getattr(mc.telemetry, 'device_update_interval', None),
                        'environment_update_interval': getattr(mc.telemetry, 'environment_update_interval', None),
                        'environment_measurement_enabled': getattr(mc.telemetry, 'environment_measurement_enabled', None),
                        'environment_screen_enabled': getattr(mc.telemetry, 'environment_screen_enabled', None),
                        'environment_display_fahrenheit': getattr(mc.telemetry, 'environment_display_fahrenheit', None),
                    }
        
        self.collected_data['device_info'] = info
        
        for key, value in info.items():
            if key not in ['config', 'module_telemetry']:
                print(f"  {key}: {value}")
        
        if 'config' in info:
            print("\n  Local Config:")
            for section, values in info['config'].items():
                non_null = {k: v for k, v in values.items() if v is not None}
                if non_null:
                    print(f"    {section}: {non_null}")
        
        if 'module_telemetry' in info:
            print("\n  Telemetry Module Config:")
            for k, v in info['module_telemetry'].items():
                if v is not None:
                    print(f"    {k}: {v}")
        
        return info
    
    def get_position(self) -> Dict:
        print(f"\n{'='*60}")
        print("GPS POSITION")
        print(f"{'='*60}")
        
        position = {}
        
        if self.interface.nodes:
            local_node_id = None
            if self.interface.myInfo:
                local_node_id = f"!{self.interface.myInfo.my_node_num:08x}"
            
            for node_id, node_info in self.interface.nodes.items():
                is_local = (node_id == local_node_id) or ('position' in node_info and node_info.get('num') == getattr(self.interface.myInfo, 'my_node_num', None))
                
                if is_local or len(self.interface.nodes) == 1:
                    pos = node_info.get('position', {})
                    if pos:
                        position = {
                            'latitude': pos.get('latitude'),
                            'longitude': pos.get('longitude'),
                            'altitude': pos.get('altitude'),
                            'latitude_i': pos.get('latitudeI'),
                            'longitude_i': pos.get('longitudeI'),
                            'altitude_hae': pos.get('altitudeHae'),
                            'altitude_geoidal_separation': pos.get('altitudeGeoidalSeparation'),
                            'ground_speed': pos.get('groundSpeed'),
                            'ground_track': pos.get('groundTrack'),
                            'sats_in_view': pos.get('satsInView'),
                            'precision_bits': pos.get('precisionBits'),
                            'pdop': pos.get('PDOP'),
                            'hdop': pos.get('HDOP'),
                            'vdop': pos.get('VDOP'),
                            'gps_time': pos.get('time'),
                            'fix_quality': pos.get('fixQuality'),
                            'fix_type': pos.get('fixType'),
                            'timestamp': pos.get('timestamp'),
                        }
                        position = {k: v for k, v in position.items() if v is not None}
                    break
        
        self.collected_data['position'] = position
        
        if position:
            for key, value in position.items():
                print(f"  {key}: {value}")
            
            if position.get('latitude') and position.get('longitude'):
                print(f"\n  Google Maps: https://www.google.com/maps?q={position['latitude']},{position['longitude']}")
        else:
            print("  No GPS position available (device may need GPS fix)")
            print("  Tip: Double-press button on T1000-E to force position update")
        
        return position
    
    def get_telemetry(self) -> Dict:
        print(f"\n{'='*60}")
        print("DEVICE TELEMETRY")
        print(f"{'='*60}")
        
        telemetry = {}
        
        if self.interface.nodes:
            local_node_num = getattr(self.interface.myInfo, 'my_node_num', None) if self.interface.myInfo else None
            
            for node_id, node_info in self.interface.nodes.items():
                if node_info.get('num') == local_node_num or len(self.interface.nodes) == 1:
                    device_metrics = node_info.get('deviceMetrics', {})
                    if device_metrics:
                        telemetry['device'] = {
                            'battery_level': device_metrics.get('batteryLevel'),
                            'voltage': device_metrics.get('voltage'),
                            'channel_utilization': device_metrics.get('channelUtilization'),
                            'air_util_tx': device_metrics.get('airUtilTx'),
                            'uptime_seconds': device_metrics.get('uptimeSeconds'),
                        }
                    
                    power_metrics = node_info.get('powerMetrics', {})
                    if power_metrics:
                        telemetry['power'] = {
                            'ch1_voltage': power_metrics.get('ch1Voltage'),
                            'ch1_current': power_metrics.get('ch1Current'),
                            'ch2_voltage': power_metrics.get('ch2Voltage'),
                            'ch2_current': power_metrics.get('ch2Current'),
                            'ch3_voltage': power_metrics.get('ch3Voltage'),
                            'ch3_current': power_metrics.get('ch3Current'),
                        }
                    break
        
        for section, metrics in telemetry.items():
            telemetry[section] = {k: v for k, v in metrics.items() if v is not None}
        telemetry = {k: v for k, v in telemetry.items() if v}
        
        self.collected_data['telemetry'] = telemetry
        
        if telemetry:
            for section, metrics in telemetry.items():
                print(f"\n  {section.upper()}:")
                for key, value in metrics.items():
                    unit = self._get_unit(key)
                    print(f"    {key}: {value}{unit}")
        else:
            print("  No device telemetry available yet")
        
        return telemetry
    
    def get_environmental_metrics(self) -> Dict:
        """Get environmental sensor data (temperature, humidity, pressure) from T1000-E"""
        print(f"\n{'='*60}")
        print("ENVIRONMENTAL SENSORS (T1000-E)")
        print(f"{'='*60}")
        
        environmental = {}
        
        if self.interface.nodes:
            local_node_num = getattr(self.interface.myInfo, 'my_node_num', None) if self.interface.myInfo else None
            
            for node_id, node_info in self.interface.nodes.items():
                if node_info.get('num') == local_node_num or len(self.interface.nodes) == 1:
                    env_metrics = node_info.get('environmentMetrics', {})
                    if env_metrics:
                        environmental = {
                            'temperature': env_metrics.get('temperature'),
                            'relative_humidity': env_metrics.get('relativeHumidity'),
                            'barometric_pressure': env_metrics.get('barometricPressure'),
                            'gas_resistance': env_metrics.get('gasResistance'),
                            'iaq': env_metrics.get('iaq'),
                            'voltage': env_metrics.get('voltage'),
                            'current': env_metrics.get('current'),
                            'lux': env_metrics.get('lux'),
                            'white_lux': env_metrics.get('whiteLux'),
                            'ir_lux': env_metrics.get('irLux'),
                            'uv_lux': env_metrics.get('uvLux'),
                            'wind_direction': env_metrics.get('windDirection'),
                            'wind_speed': env_metrics.get('windSpeed'),
                            'wind_gust': env_metrics.get('windGust'),
                            'weight': env_metrics.get('weight'),
                            'distance': env_metrics.get('distance'),
                        }
                        environmental = {k: v for k, v in environmental.items() if v is not None}
                    
                    air_quality = node_info.get('airQualityMetrics', {})
                    if air_quality:
                        environmental.update({
                            'pm10_standard': air_quality.get('pm10Standard'),
                            'pm25_standard': air_quality.get('pm25Standard'),
                            'pm100_standard': air_quality.get('pm100Standard'),
                            'pm10_environmental': air_quality.get('pm10Environmental'),
                            'pm25_environmental': air_quality.get('pm25Environmental'),
                            'pm100_environmental': air_quality.get('pm100Environmental'),
                            'particles_03um': air_quality.get('particles03um'),
                            'particles_05um': air_quality.get('particles05um'),
                            'particles_10um': air_quality.get('particles10um'),
                            'particles_25um': air_quality.get('particles25um'),
                            'particles_50um': air_quality.get('particles50um'),
                            'particles_100um': air_quality.get('particles100um'),
                            'co2': air_quality.get('co2'),
                        })
                        environmental = {k: v for k, v in environmental.items() if v is not None}
                    break
        
        self.collected_data['environmental'] = environmental
        
        if environmental:
            print("\n  READINGS:")
            for key, value in environmental.items():
                unit = self._get_unit(key)
                print(f"    {key}: {value}{unit}")
            
            if 'temperature' in environmental:
                temp_c = environmental['temperature']
                temp_f = (temp_c * 9/5) + 32
                print(f"\n  Temperature: {temp_c:.1f}°C / {temp_f:.1f}°F")
        else:
            print("  No environmental data available yet")
            print("\n  Notes for T1000-E:")
            print("    - Environmental telemetry is sent periodically (default: 15 min)")
            print("    - Use -w 60 to wait for telemetry updates")
            print("    - Triple-press button to trigger GPS update")
            print("    - Check firmware supports environmental reporting")
        
        return environmental
    
    def _get_unit(self, key: str) -> str:
        units = {
            'battery_level': '%',
            'voltage': 'V',
            'current': 'mA',
            'temperature': '°C',
            'relative_humidity': '%',
            'barometric_pressure': 'hPa',
            'channel_utilization': '%',
            'air_util_tx': '%',
            'uptime_seconds': 's',
            'ground_speed': 'm/s',
            'altitude': 'm',
            'wind_speed': 'm/s',
            'wind_gust': 'm/s',
            'wind_direction': '°',
            'lux': 'lx',
            'co2': 'ppm',
            'distance': 'mm',
            'gas_resistance': 'Ω',
        }
        return ' ' + units.get(key, '') if key in units else ''
    
    def get_mesh_nodes(self) -> list:
        print(f"\n{'='*60}")
        print("MESH NETWORK NODES")
        print(f"{'='*60}")
        
        nodes = []
        
        if self.interface.nodes:
            for node_id, node_info in self.interface.nodes.items():
                user = node_info.get('user', {})
                position = node_info.get('position', {})
                device_metrics = node_info.get('deviceMetrics', {})
                env_metrics = node_info.get('environmentMetrics', {})
                
                last_heard = node_info.get('lastHeard')
                if last_heard:
                    last_heard_dt = datetime.fromtimestamp(last_heard, tz=timezone.utc)
                    last_heard_str = last_heard_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                else:
                    last_heard_str = None
                
                node = {
                    'node_id': node_id,
                    'node_num': node_info.get('num'),
                    'long_name': user.get('longName'),
                    'short_name': user.get('shortName'),
                    'hw_model': str(user.get('hwModel')) if user.get('hwModel') else None,
                    'mac_address': user.get('macaddr'),
                    'is_licensed': user.get('isLicensed'),
                    'role': str(user.get('role')) if user.get('role') else None,
                    'latitude': position.get('latitude'),
                    'longitude': position.get('longitude'),
                    'altitude': position.get('altitude'),
                    'last_heard': last_heard_str,
                    'snr': node_info.get('snr'),
                    'hops_away': node_info.get('hopsAway'),
                    'battery_level': device_metrics.get('batteryLevel'),
                    'voltage': device_metrics.get('voltage'),
                    'temperature': env_metrics.get('temperature'),
                }
                node = {k: v for k, v in node.items() if v is not None}
                nodes.append(node)
        
        self.collected_data['node_db'] = nodes
        
        if nodes:
            for i, node in enumerate(nodes, 1):
                print(f"\n  Node {i}: {node.get('long_name', 'Unknown')} ({node.get('short_name', '???')})")
                print(f"    ID: {node.get('node_id')}")
                if node.get('hw_model'):
                    print(f"    Hardware: {node.get('hw_model')}")
                if node.get('latitude') and node.get('longitude'):
                    print(f"    Position: {node.get('latitude')}, {node.get('longitude')}")
                if node.get('battery_level'):
                    print(f"    Battery: {node.get('battery_level')}%")
                if node.get('temperature'):
                    print(f"    Temperature: {node.get('temperature')}°C")
                if node.get('last_heard'):
                    print(f"    Last heard: {node.get('last_heard')}")
                if node.get('snr'):
                    print(f"    SNR: {node.get('snr')} dB")
                if node.get('hops_away') is not None:
                    print(f"    Hops away: {node.get('hops_away')}")
        else:
            print("  No nodes discovered yet")
        
        return nodes
    
    def request_fresh_data(self):
        print(f"\n{'='*60}")
        print("REQUESTING FRESH DATA FROM DEVICE")
        print(f"{'='*60}")
        
        try:
            local_node = self.interface.getNode('^local')
            if local_node:
                print("  Requesting position update...")
                try:
                    local_node.requestPosition()
                except Exception as e:
                    print(f"    Position request: {e}")
                time.sleep(2)
                
                print("  Requesting telemetry...")
                try:
                    if hasattr(local_node, 'requestTelemetry'):
                        local_node.requestTelemetry()
                except Exception as e:
                    print(f"    Telemetry request: {e}")
                time.sleep(2)
        except Exception as e:
            print(f"  Note: Could not request fresh data: {e}")
    
    def run_full_test(self, wait_for_data: int = 0) -> Dict:
        try:
            self.connect()
            
            self.get_device_info()
            self.get_position()
            self.get_telemetry()
            self.get_environmental_metrics()
            self.get_mesh_nodes()
            
            if wait_for_data > 0:
                print(f"\n{'='*60}")
                print(f"LISTENING FOR LIVE DATA ({wait_for_data} seconds)")
                print(f"{'='*60}")
                
                self.request_fresh_data()
                
                from pubsub import pub
                
                def on_position(packet, interface):
                    print(f"\n  >> POSITION UPDATE RECEIVED")
                    pos = packet.get('decoded', {}).get('position', {})
                    if pos.get('latitude'):
                        print(f"     Lat: {pos.get('latitude')}, Lon: {pos.get('longitude')}")
                        if pos.get('altitude'):
                            print(f"     Alt: {pos.get('altitude')}m")
                
                def on_telemetry(packet, interface):
                    print(f"\n  >> TELEMETRY UPDATE RECEIVED")
                    tel = packet.get('decoded', {}).get('telemetry', {})
                    
                    dm = tel.get('deviceMetrics', {})
                    if dm:
                        if dm.get('batteryLevel'):
                            print(f"     Battery: {dm.get('batteryLevel')}%")
                        if dm.get('voltage'):
                            print(f"     Voltage: {dm.get('voltage')}V")
                    
                    em = tel.get('environmentMetrics', {})
                    if em:
                        if em.get('temperature') is not None:
                            temp_c = em.get('temperature')
                            temp_f = (temp_c * 9/5) + 32
                            print(f"     Temperature: {temp_c:.1f}°C / {temp_f:.1f}°F")
                        if em.get('relativeHumidity') is not None:
                            print(f"     Humidity: {em.get('relativeHumidity')}%")
                        if em.get('barometricPressure') is not None:
                            print(f"     Pressure: {em.get('barometricPressure')} hPa")
                
                pub.subscribe(on_position, "meshtastic.receive.position")
                pub.subscribe(on_telemetry, "meshtastic.receive.data.67")
                
                print(f"  Waiting for updates (Ctrl+C to stop early)...")
                
                start_time = time.time()
                while time.time() - start_time < wait_for_data:
                    time.sleep(1)
                
                print("\n  Refreshing data after wait period...")
                self.get_position()
                self.get_telemetry()
                self.get_environmental_metrics()
            
            print(f"\n{'='*60}")
            print("TEST COMPLETE")
            print(f"{'='*60}")
            
            return self.collected_data
            
        finally:
            self.close()
    
    def close(self):
        if self.interface:
            try:
                self.interface.close()
                print("\nConnection closed.")
            except Exception as e:
                logger.debug(f"Close error: {e}")
    
    def export_json(self, filename: str = None):
        if not filename:
            filename = f"sensecap_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        output = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'device_type': 'SenseCAP Card Tracker T1000-E',
            'connection_type': self.connection_type,
            **self.collected_data
        }
        
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2, default=str)
        
        print(f"\nData exported to: {filename}")
        return filename


def main():
    parser = argparse.ArgumentParser(
        description='Test SenseCAP Card Tracker T1000-E - retrieve GPS and sensor data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_sensecap.py                        # Auto-detect serial connection
  python test_sensecap.py -p /dev/ttyUSB0        # Specific serial port
  python test_sensecap.py -t tcp -H 192.168.1.100  # TCP/WiFi connection
  
  # Bluetooth (BLE) connections:
  python test_sensecap.py --scan-ble             # Scan for BLE devices
  python test_sensecap.py -t ble                 # BLE with auto-detect
  python test_sensecap.py -t ble -p AA:BB:CC:DD:EE:FF  # BLE with MAC address
  
  # Default BLE pairing PIN: 123456
  
  # Wait for environmental data:
  python test_sensecap.py -w 60                  # Wait 60s for temp/humidity
  python test_sensecap.py -t ble -w 120 -o data.json  # BLE + wait + export

T1000-E Button Actions:
  - Single press: Wake device
  - Double press: Force position update
  - Triple press: Toggle GPS
  - Long press (5s): Shutdown

BLE Pairing:
  - Default PIN: 123456
        """
    )
    
    parser.add_argument('-t', '--type', choices=['serial', 'tcp', 'ble'], 
                        default='serial', help='Connection type (default: serial)')
    parser.add_argument('-p', '--port', help='Serial port, BLE MAC/UUID, or device path')
    parser.add_argument('-H', '--host', help='TCP hostname for WiFi connection')
    parser.add_argument('-w', '--wait', type=int, default=0,
                        help='Wait N seconds for live data updates (recommended: 60+ for environmental)')
    parser.add_argument('-o', '--output', help='Export results to JSON file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--scan-ble', action='store_true', help='Scan for BLE devices and exit')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger('bleak').setLevel(logging.DEBUG)
    
    if args.scan_ble:
        print("Scanning for Meshtastic BLE devices...")
        devices = scan_ble_devices()
        if devices:
            print(f"\nFound {len(devices)} Meshtastic device(s):")
            for d in devices:
                print(f"  {d['name']} - {d['address']} (RSSI: {d.get('rssi', 'N/A')})")
            print(f"\nConnect with: python test_sensecap.py -t ble -p {devices[0]['address']}")
        else:
            print("\nNo Meshtastic devices found. Make sure:")
            print("  1. Device is powered on")
            print("  2. Bluetooth is enabled on your computer")
            print("  3. Device is in range")
        return
    
    tester = SenseCapTester(
        connection_type=args.type,
        device_path=args.port,
        hostname=args.host
    )
    
    try:
        data = tester.run_full_test(wait_for_data=args.wait)
        
        if args.output:
            tester.export_json(args.output)
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        tester.close()
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=args.verbose)
        sys.exit(1)


if __name__ == '__main__':
    main()
