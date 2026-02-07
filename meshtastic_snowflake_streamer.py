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
        
        connection_type = self.meshtastic_config.get('connection_type', 'serial')
        device_path = self.meshtastic_config.get('device_path')
        hostname = self.meshtastic_config.get('hostname')
        
        self.meshtastic_receiver = MeshtasticReceiver(
            connection_type=connection_type,
            device_path=device_path,
            hostname=hostname,
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
