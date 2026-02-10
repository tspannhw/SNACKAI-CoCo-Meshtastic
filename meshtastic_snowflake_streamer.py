#!/usr/bin/env python3
"""
Meshtastic to Snowflake Streamer

Streams mesh network data from Meshtastic devices to Snowflake
using Snowpipe Streaming v2 REST API - HIGH PERFORMANCE MODE.

NO direct SQL INSERTs - ONLY Snowpipe Streaming v2 REST API endpoints.
"""
import os
import json
import logging
import time
import signal
import sys
import threading
import atexit
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional
from queue import Queue, Empty

from snowpipe_streaming_client import SnowpipeStreamingClient
from meshtastic_interface import MeshtasticReceiver

logger = logging.getLogger(__name__)


class MeshtasticSnowflakeStreamer:
    def __init__(self, config_path: str = 'snowflake_config.json'):
        self.config_path = config_path
        self.config = self._load_config(config_path)
        
        self.meshtastic_config = self.config.get('meshtastic', {})
        self.batch_size = self.config.get('batch_size', 10)
        self.flush_interval = self.config.get('flush_interval_seconds', 5)
        
        self.message_queue = Queue()
        self.running = False
        self.stats = {
            'messages_received': 0,
            'messages_sent': 0,
            'batches_sent': 0,
            'errors': 0,
            'start_time': None
        }
        
        self.streaming_client = None
        self.meshtastic_receiver = None
        
        self.slack_config = self.config.get('slack', {})
        self.slack_webhook = self.slack_config.get('webhook_url')
        self.slack_channel = self.slack_config.get('channel')
        self.slack_alerts_enabled = self.slack_config.get('enabled', False)
        self.low_battery_threshold = self.slack_config.get('low_battery_threshold', 20)
        
        self._shutdown_event = threading.Event()
        self._setup_signal_handlers()
    
    def _load_config(self, config_path: str) -> Dict:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        atexit.register(self._cleanup)
    
    def _signal_handler(self, signum, frame):
        if self._shutdown_event.is_set():
            logger.info("Force exit")
            sys.exit(1)
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self._shutdown_event.set()
        self.running = False
    
    def _cleanup(self):
        if self.running:
            self.stop()
    
    def _on_meshtastic_message(self, message: Dict):
        self.message_queue.put(message)
        self.stats['messages_received'] += 1
        pkt_type = message.get('packet_type', 'unknown')
        lat = message.get('latitude')
        lon = message.get('longitude')
        temp = message.get('temperature')
        bat = message.get('battery_level')
        logger.debug(f"Queued {pkt_type} message: lat={lat}, lon={lon}, temp={temp}, bat={bat} (queue: {self.message_queue.qsize()})")
        
        if self.slack_alerts_enabled and self.slack_webhook:
            self._check_slack_alerts(message)
    
    def _send_slack_message(self, message: str) -> bool:
        try:
            payload = {"text": message}
            if self.slack_channel:
                payload["channel"] = self.slack_channel
            response = requests.post(self.slack_webhook, json=payload, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Slack error: {e}")
            return False
    
    def _check_slack_alerts(self, message: Dict):
        device_id = message.get('from_id', 'unknown')
        pkt_type = message.get('packet_type', '')
        
        battery = message.get('battery_level')
        if battery is not None and battery <= self.low_battery_threshold:
            voltage = message.get('voltage', 'N/A')
            alert = (
                f"🔋 *Low Battery Alert*\n"
                f"Device: `{device_id}`\n"
                f"• Battery: {battery}%\n"
                f"• Voltage: {voltage}V\n"
                f"• Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            self._send_slack_message(alert)
            logger.info(f"Sent low battery alert for {device_id}")
        
        if pkt_type == 'position':
            lat = message.get('latitude')
            lon = message.get('longitude')
            alt = message.get('altitude')
            if lat and lon:
                speed = message.get('ground_speed')
                heading = message.get('ground_track')
                pos_msg = (
                    f"📍 *Position Update*\n"
                    f"Device: `{device_id}`\n"
                    f"• Location: {lat:.6f}, {lon:.6f}\n"
                    f"• Altitude: {alt or 'N/A'}m\n"
                    f"• Speed: {speed or 'N/A'} m/s\n"
                    f"• Heading: {heading or 'N/A'}°\n"
                    f"• Satellites: {message.get('sats_in_view', 'N/A')}\n"
                    f"• Map: https://maps.google.com/?q={lat},{lon}\n"
                    f"• Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                if self.slack_config.get('notify_position', False):
                    self._send_slack_message(pos_msg)
        
        if pkt_type == 'telemetry':
            self._send_telemetry_slack(message, device_id)
        
        if pkt_type == 'text':
            text = message.get('text', '')
            if text and self.slack_config.get('notify_text', True):
                text_msg = (
                    f"💬 *Text Message*\n"
                    f"From: `{device_id}`\n"
                    f"Message: {text}\n"
                    f"• SNR: {message.get('rx_snr', 'N/A')} dB\n"
                    f"• Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                self._send_slack_message(text_msg)
    
    def _send_telemetry_slack(self, message: Dict, device_id: str):
        """Send comprehensive telemetry data to Slack"""
        if not self.slack_config.get('notify_telemetry', True):
            return
        
        temp = message.get('temperature')
        humidity = message.get('relative_humidity')
        pressure = message.get('barometric_pressure')
        battery = message.get('battery_level')
        voltage = message.get('voltage')
        
        has_env = temp is not None or humidity is not None or pressure is not None
        has_device = battery is not None or voltage is not None
        
        if has_env:
            env_msg = f"🌡️ *Environmental Sensors*\nDevice: `{device_id}`\n"
            if temp is not None:
                env_msg += f"• Temperature: {temp:.1f}°C ({temp * 9/5 + 32:.1f}°F)\n"
            if humidity is not None:
                env_msg += f"• Humidity: {humidity:.1f}%\n"
            if pressure is not None:
                env_msg += f"• Pressure: {pressure:.1f} hPa\n"
            
            iaq = message.get('iaq')
            gas = message.get('gas_resistance')
            if iaq is not None:
                env_msg += f"• Air Quality (IAQ): {iaq}\n"
            if gas is not None:
                env_msg += f"• Gas Resistance: {gas} Ω\n"
            
            env_msg += f"• Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            self._send_slack_message(env_msg)
        
        if has_device and self.slack_config.get('notify_device_metrics', False):
            dev_msg = f"📊 *Device Metrics*\nDevice: `{device_id}`\n"
            if battery is not None:
                icon = "🔴" if battery <= 10 else "🟡" if battery <= 20 else "🟢" if battery <= 80 else "🔵"
                dev_msg += f"• Battery: {icon} {battery}%\n"
            if voltage is not None:
                dev_msg += f"• Voltage: {voltage:.2f}V\n"
            
            ch_util = message.get('channel_utilization')
            air_util = message.get('air_util_tx')
            uptime = message.get('uptime_seconds')
            
            if ch_util is not None:
                dev_msg += f"• Channel Util: {ch_util:.1f}%\n"
            if air_util is not None:
                dev_msg += f"• Air Util TX: {air_util:.1f}%\n"
            if uptime is not None:
                hours = uptime // 3600
                mins = (uptime % 3600) // 60
                dev_msg += f"• Uptime: {hours}h {mins}m\n"
            
            snr = message.get('rx_snr')
            rssi = message.get('rx_rssi')
            if snr is not None:
                dev_msg += f"• SNR: {snr} dB\n"
            if rssi is not None:
                dev_msg += f"• RSSI: {rssi} dBm\n"
            
            dev_msg += f"• Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            self._send_slack_message(dev_msg)
    
    def _prepare_row(self, message: Dict) -> Dict:
        def convert_value(v, depth=0):
            if depth > 10:
                return str(v)
            if isinstance(v, bytes):
                return v.hex()
            elif isinstance(v, dict):
                return {k: convert_value(val, depth+1) for k, val in v.items()}
            elif isinstance(v, (list, tuple)):
                return [convert_value(item, depth+1) for item in v]
            elif hasattr(v, '__dict__'):
                return str(v)
            return v
        
        def safe_json(obj):
            try:
                return json.dumps(convert_value(obj))
            except:
                return str(obj)
        
        row = {
            'ingested_at': datetime.now(timezone.utc).isoformat(),
            'packet_type': message.get('packet_type'),
            'from_id': message.get('from_id'),
            'from_num': message.get('from_num'),
            'to_id': message.get('to_id'),
            'to_num': message.get('to_num'),
            'channel': message.get('channel'),
            'rx_snr': message.get('rx_snr'),
            'rx_rssi': message.get('rx_rssi'),
            'hop_limit': message.get('hop_limit'),
            'hop_start': message.get('hop_start'),
            'latitude': message.get('latitude'),
            'longitude': message.get('longitude'),
            'altitude': message.get('altitude'),
            'ground_speed': message.get('ground_speed'),
            'ground_track': message.get('ground_track'),
            'sats_in_view': message.get('sats_in_view'),
            'pdop': message.get('pdop'),
            'hdop': message.get('hdop'),
            'vdop': message.get('vdop'),
            'gps_timestamp': message.get('gps_timestamp') or message.get('telemetry_time'),
            'precision_bits': message.get('precision_bits'),
            'text_message': message.get('text'),
            'battery_level': message.get('battery_level'),
            'voltage': message.get('voltage'),
            'temperature': message.get('temperature'),
            'temperature_f': message.get('temperature_f'),
            'relative_humidity': message.get('relative_humidity'),
            'barometric_pressure': message.get('barometric_pressure'),
            'gas_resistance': message.get('gas_resistance'),
            'iaq': message.get('iaq'),
            'lux': message.get('lux'),
            'white_lux': message.get('white_lux'),
            'ir_lux': message.get('ir_lux'),
            'uv_lux': message.get('uv_lux'),
            'wind_direction': message.get('wind_direction'),
            'wind_speed': message.get('wind_speed'),
            'wind_gust': message.get('wind_gust'),
            'weight': message.get('weight'),
            'distance': message.get('distance'),
            'pm10_standard': message.get('pm10_standard'),
            'pm25_standard': message.get('pm25_standard'),
            'pm100_standard': message.get('pm100_standard'),
            'pm10_environmental': message.get('pm10_environmental'),
            'pm25_environmental': message.get('pm25_environmental'),
            'pm100_environmental': message.get('pm100_environmental'),
            'co2': message.get('co2'),
            'ch1_voltage': message.get('ch1_voltage'),
            'ch1_current': message.get('ch1_current'),
            'ch2_voltage': message.get('ch2_voltage'),
            'ch2_current': message.get('ch2_current'),
            'ch3_voltage': message.get('ch3_voltage'),
            'ch3_current': message.get('ch3_current'),
            'channel_utilization': message.get('channel_utilization'),
            'air_util_tx': message.get('air_util_tx'),
            'uptime_seconds': message.get('uptime_seconds'),
            'raw_packet': safe_json(message.get('raw_packet')) if message.get('raw_packet') else None
        }
        
        cleaned_row = {}
        for k, v in row.items():
            if v is not None:
                cleaned_row[k] = convert_value(v)
        
        return cleaned_row
    
    def _flush_batch(self, messages: List[Dict]) -> bool:
        if not messages:
            return True
        
        try:
            rows = [self._prepare_row(msg) for msg in messages]
            
            for i, row in enumerate(rows):
                pkt_type = row.get('packet_type')
                lat = row.get('latitude')
                lon = row.get('longitude')
                logger.debug(f"Row {i}: type={pkt_type}, lat={lat}, lon={lon}")
            
            count = self.streaming_client.insert_rows(rows)
            
            self.stats['messages_sent'] += count
            self.stats['batches_sent'] += 1
            
            logger.info(f"Flushed batch of {count} messages via Snowpipe Streaming v2 (total sent: {self.stats['messages_sent']})")
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Error flushing batch: {e}", exc_info=True)
            return False
    
    def _streaming_worker(self):
        batch = []
        last_flush = time.time()
        
        while self.running or not self.message_queue.empty():
            try:
                try:
                    message = self.message_queue.get(timeout=0.5)
                    batch.append(message)
                except Empty:
                    pass
                
                should_flush = (
                    len(batch) >= self.batch_size or
                    (batch and time.time() - last_flush >= self.flush_interval)
                )
                
                if should_flush and batch:
                    self._flush_batch(batch)
                    batch = []
                    last_flush = time.time()
                
                if self._shutdown_event.is_set() and self.message_queue.empty():
                    break
                    
            except Exception as e:
                logger.error(f"Error in streaming worker: {e}", exc_info=True)
                time.sleep(0.5)
        
        if batch:
            self._flush_batch(batch)
        
        logger.info("Streaming worker stopped")
    
    def connect_snowflake(self):
        logger.info("Connecting to Snowflake via Snowpipe Streaming v2...")
        
        self.streaming_client = SnowpipeStreamingClient(self.config_path)
        
        self.streaming_client.discover_ingest_host()
        
        self.streaming_client.open_channel()
        
        logger.info(f"Connected via Snowpipe Streaming v2: {self.config['database']}.{self.config['schema']}")
        logger.info(f"Pipe: {self.config.get('pipe', self.config.get('table'))}")
        logger.info(f"Channel: {self.streaming_client.channel_name}")
        
        return self
    
    def connect_meshtastic(self):
        logger.info("Connecting to Meshtastic device...")
        
        connection_type = self.meshtastic_config.get('connection_type', 'auto')
        device_path = self.meshtastic_config.get('device_path')
        hostname = self.meshtastic_config.get('hostname')
        ble_address = self.meshtastic_config.get('ble_address')
        
        self.meshtastic_receiver = MeshtasticReceiver(
            connection_type=connection_type,
            device_path=device_path,
            hostname=hostname,
            ble_address=ble_address,
            on_message_callback=self._on_meshtastic_message
        )
        
        self.meshtastic_receiver.connect()
        logger.info("Connected to Meshtastic device")
        
        node_info = self.meshtastic_receiver.get_local_node_info()
        if node_info:
            logger.info(f"Local node: {node_info}")
        
        return self
    
    def start(self):
        logger.info("=" * 70)
        logger.info("MESHTASTIC-SNOWFLAKE STREAMER - SNOWPIPE STREAMING V2 MODE")
        logger.info("Using ONLY Snowpipe Streaming v2 REST API - NO SQL INSERTs")
        logger.info("=" * 70)
        
        self.connect_snowflake()
        self.connect_meshtastic()
        
        self.running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        
        self.worker_thread = threading.Thread(target=self._streaming_worker, daemon=True)
        self.worker_thread.start()
        
        logger.info("Streamer started. Listening for Meshtastic messages...")
        
        try:
            while self.running and not self._shutdown_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            if not self._shutdown_event.is_set():
                self._shutdown_event.set()
            self.stop()
    
    def stop(self):
        if not self.running and self._shutdown_event.is_set():
            return
        
        logger.info("Stopping streamer...")
        self.running = False
        self._shutdown_event.set()
        
        if self.meshtastic_receiver:
            try:
                self.meshtastic_receiver.close()
            except Exception as e:
                logger.debug(f"Error closing meshtastic: {e}")
        
        if hasattr(self, 'worker_thread') and self.worker_thread.is_alive():
            logger.info("Waiting for worker thread...")
            self.worker_thread.join(timeout=5)
        
        if self.streaming_client:
            try:
                self.streaming_client.close_channel()
                self.streaming_client.print_statistics()
            except Exception as e:
                logger.debug(f"Error closing streaming client: {e}")
        
        self._print_stats()
        logger.info("Streamer stopped cleanly")
    
    def _print_stats(self):
        duration = None
        if self.stats['start_time']:
            duration = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()
        
        print("\n" + "=" * 50)
        print("STREAMING SESSION STATISTICS")
        print("=" * 50)
        print(f"Messages received:  {self.stats['messages_received']}")
        print(f"Messages sent:      {self.stats['messages_sent']}")
        print(f"Batches sent:       {self.stats['batches_sent']}")
        print(f"Errors:             {self.stats['errors']}")
        if duration:
            print(f"Duration:           {duration:.1f} seconds")
            if self.stats['messages_sent'] > 0:
                print(f"Throughput:         {self.stats['messages_sent']/duration:.2f} msg/sec")
        print("=" * 50 + "\n")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('meshtastic_streaming.log')
        ]
    )
    
    config_path = os.environ.get('MESHTASTIC_CONFIG', 'snowflake_config.json')
    
    streamer = MeshtasticSnowflakeStreamer(config_path)
    streamer.start()


if __name__ == '__main__':
    main()
