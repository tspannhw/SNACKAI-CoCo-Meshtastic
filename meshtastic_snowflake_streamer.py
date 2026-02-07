#!/usr/bin/env python3
import os
import json
import logging
import time
import signal
import sys
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional
from queue import Queue

from snowflake_jwt_auth import SnowflakeJWTAuth
from snowpipe_streaming_client import SnowpipeStreamingClient
from meshtastic_interface import MeshtasticReceiver

logger = logging.getLogger(__name__)


class MeshtasticSnowflakeStreamer:
    def __init__(self, config_path: str = 'snowflake_config.json'):
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
        
        self.auth = None
        self.streaming_client = None
        self.meshtastic_receiver = None
        
        self._setup_signal_handlers()
    
    def _load_config(self, config_path: str) -> Dict:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def _on_meshtastic_message(self, message: Dict):
        self.message_queue.put(message)
        self.stats['messages_received'] += 1
        logger.debug(f"Queued message (queue size: {self.message_queue.qsize()})")
    
    def _prepare_row(self, message: Dict) -> Dict:
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
            'latitude': message.get('latitude'),
            'longitude': message.get('longitude'),
            'altitude': message.get('altitude'),
            'ground_speed': message.get('ground_speed'),
            'sats_in_view': message.get('sats_in_view'),
            'text_message': message.get('text'),
            'battery_level': message.get('battery_level'),
            'voltage': message.get('voltage'),
            'temperature': message.get('temperature'),
            'relative_humidity': message.get('relative_humidity'),
            'barometric_pressure': message.get('barometric_pressure'),
            'channel_utilization': message.get('channel_utilization'),
            'air_util_tx': message.get('air_util_tx'),
            'uptime_seconds': message.get('uptime_seconds'),
            'raw_packet': message.get('raw_packet')
        }
        
        row = {k: v for k, v in row.items() if v is not None}
        
        return row
    
    def _flush_batch(self, messages: List[Dict]) -> bool:
        if not messages:
            return True
        
        try:
            rows = [self._prepare_row(msg) for msg in messages]
            
            offset_token = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
            
            self.streaming_client.append_rows(rows, offset_token=offset_token)
            
            self.stats['messages_sent'] += len(rows)
            self.stats['batches_sent'] += 1
            
            logger.info(f"Flushed batch of {len(rows)} messages (total sent: {self.stats['messages_sent']})")
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
                    message = self.message_queue.get(timeout=1)
                    batch.append(message)
                except:
                    pass
                
                should_flush = (
                    len(batch) >= self.batch_size or
                    (batch and time.time() - last_flush >= self.flush_interval)
                )
                
                if should_flush and batch:
                    self._flush_batch(batch)
                    batch = []
                    last_flush = time.time()
                    
            except Exception as e:
                logger.error(f"Error in streaming worker: {e}", exc_info=True)
                time.sleep(1)
        
        if batch:
            self._flush_batch(batch)
    
    def connect_snowflake(self):
        logger.info("Connecting to Snowflake...")
        
        self.auth = SnowflakeJWTAuth(self.config)
        self.streaming_client = SnowpipeStreamingClient(self.config, self.auth)
        
        self.streaming_client.get_hostname()
        logger.info(f"Got streaming hostname: {self.streaming_client.hostname}")
        
        self.streaming_client.open_channel()
        logger.info(f"Opened streaming channel: {self.streaming_client.channel_name}")
        
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
        logger.info("Starting Meshtastic-Snowflake streamer...")
        
        self.connect_snowflake()
        self.connect_meshtastic()
        
        self.running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        
        self.worker_thread = threading.Thread(target=self._streaming_worker, daemon=True)
        self.worker_thread.start()
        
        logger.info("Streamer started. Listening for Meshtastic messages...")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.stop()
    
    def stop(self):
        logger.info("Stopping streamer...")
        self.running = False
        
        if self.meshtastic_receiver:
            self.meshtastic_receiver.close()
        
        if hasattr(self, 'worker_thread') and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=10)
        
        self._print_stats()
        logger.info("Streamer stopped")
    
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
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    config_path = os.environ.get('MESHTASTIC_CONFIG', 'snowflake_config.json')
    
    streamer = MeshtasticSnowflakeStreamer(config_path)
    streamer.start()


if __name__ == '__main__':
    main()
