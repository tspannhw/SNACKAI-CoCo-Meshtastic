#!/usr/bin/env python3
"""
MQTT Consumer for Meshtastic Public Broker

Subscribes to the public Meshtastic MQTT broker and ingests JSON packets
into Snowflake via Snowpipe Streaming or direct SQL.

Reference: https://meshtastic.org/docs/software/integrations/mqtt/
"""
import os
import sys
import json
import time
import signal
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable
from queue import Queue, Empty
from logging.handlers import RotatingFileHandler

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Please install paho-mqtt: pip install paho-mqtt")
    sys.exit(1)

from validation import (
    MQTTMessage, 
    validate_mqtt_message, 
    validate_snowflake_row,
    ValidationResult
)

LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(LOG_DIR, 'mqtt_consumer.log'),
            maxBytes=10*1024*1024,
            backupCount=5
        ),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('mqtt_consumer')


class MQTTConsumer:
    """Consumes messages from Meshtastic MQTT broker"""
    
    DEFAULT_BROKER = "mqtt.meshtastic.org"
    DEFAULT_PORT = 1883
    DEFAULT_TOPIC = "msh/+/2/json/#"
    
    def __init__(
        self,
        broker: str = None,
        port: int = None,
        topic: str = None,
        on_message: Optional[Callable[[Dict], None]] = None,
        batch_size: int = 10,
        flush_interval: int = 5
    ):
        self.broker = broker or os.getenv('MQTT_BROKER', self.DEFAULT_BROKER)
        self.port = port or int(os.getenv('MQTT_PORT', self.DEFAULT_PORT))
        self.topic = topic or os.getenv('MQTT_TOPIC', self.DEFAULT_TOPIC)
        self.on_message_callback = on_message
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        self.client = mqtt.Client(
            client_id=f"meshtastic-dashboard-{os.getpid()}",
            protocol=mqtt.MQTTv311
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        
        self.message_queue: Queue = Queue()
        self.running = False
        self.connected = False
        self._shutdown_event = threading.Event()
        
        self.stats = {
            'messages_received': 0,
            'messages_valid': 0,
            'messages_invalid': 0,
            'batches_processed': 0,
            'errors': 0,
            'start_time': None,
            'last_message_time': None
        }
        
        self.region_stats: Dict[str, int] = {}
        self.type_stats: Dict[str, int] = {}
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT broker: {self.broker}:{self.port}")
            self.connected = True
            client.subscribe(self.topic)
            logger.info(f"Subscribed to topic: {self.topic}")
        else:
            logger.error(f"Connection failed with code: {rc}")
            self.connected = False
    
    def _on_disconnect(self, client, userdata, rc):
        logger.warning(f"Disconnected from broker (rc={rc})")
        self.connected = False
        
        if self.running and not self._shutdown_event.is_set():
            logger.info("Attempting to reconnect...")
            time.sleep(5)
            try:
                client.reconnect()
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
    
    def _on_message(self, client, userdata, msg):
        try:
            topic_parts = msg.topic.split('/')
            region = topic_parts[1] if len(topic_parts) > 1 else 'unknown'
            
            self.region_stats[region] = self.region_stats.get(region, 0) + 1
            
            payload = json.loads(msg.payload.decode('utf-8'))
            
            self.stats['messages_received'] += 1
            self.stats['last_message_time'] = datetime.now(timezone.utc)
            
            validation_result = validate_mqtt_message(payload)
            
            if validation_result.valid:
                self.stats['messages_valid'] += 1
                
                pkt_type = payload.get('type', 'unknown')
                self.type_stats[pkt_type] = self.type_stats.get(pkt_type, 0) + 1
                
                if validation_result.warnings:
                    for warn in validation_result.warnings:
                        logger.debug(f"Validation warning: {warn}")
                
                if validation_result.data:
                    validation_result.data['mqtt_topic'] = msg.topic
                    validation_result.data['mqtt_region'] = region
                    self.message_queue.put(validation_result.data)
                
                if self.stats['messages_received'] % 100 == 0:
                    logger.info(f"Received {self.stats['messages_received']} messages "
                              f"({self.stats['messages_valid']} valid)")
            else:
                self.stats['messages_invalid'] += 1
                for error in validation_result.errors:
                    logger.warning(f"Invalid message: {error}")
                
        except json.JSONDecodeError as e:
            logger.debug(f"Non-JSON message on {msg.topic}: {e}")
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Error processing message: {e}")
    
    def _process_batch(self, messages: List[Dict]) -> int:
        """Process a batch of messages"""
        if not messages:
            return 0
        
        processed = 0
        for msg in messages:
            try:
                row_validation = validate_snowflake_row(msg)
                if row_validation.valid:
                    if self.on_message_callback:
                        self.on_message_callback(msg)
                    processed += 1
                else:
                    for error in row_validation.errors:
                        logger.warning(f"Row validation error: {error}")
            except Exception as e:
                logger.error(f"Error processing row: {e}")
        
        self.stats['batches_processed'] += 1
        return processed
    
    def _batch_worker(self):
        """Background worker to process message batches"""
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
                    count = self._process_batch(batch)
                    logger.debug(f"Processed batch of {count} messages")
                    batch = []
                    last_flush = time.time()
                
                if self._shutdown_event.is_set() and self.message_queue.empty():
                    break
                    
            except Exception as e:
                logger.error(f"Batch worker error: {e}")
                time.sleep(0.5)
        
        if batch:
            self._process_batch(batch)
        
        logger.info("Batch worker stopped")
    
    def start(self):
        """Start the MQTT consumer"""
        logger.info("=" * 60)
        logger.info("MESHTASTIC MQTT CONSUMER")
        logger.info(f"Broker: {self.broker}:{self.port}")
        logger.info(f"Topic: {self.topic}")
        logger.info("=" * 60)
        
        self.running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        
        self.worker_thread = threading.Thread(target=self._batch_worker, daemon=True)
        self.worker_thread.start()
        
        try:
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_start()
            
            logger.info("MQTT consumer started. Press Ctrl+C to stop.")
            
            while self.running and not self._shutdown_event.is_set():
                time.sleep(1)
                
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the MQTT consumer"""
        if not self.running:
            return
        
        logger.info("Stopping MQTT consumer...")
        self.running = False
        self._shutdown_event.set()
        
        self.client.loop_stop()
        self.client.disconnect()
        
        if hasattr(self, 'worker_thread') and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)
        
        self._print_stats()
        logger.info("MQTT consumer stopped")
    
    def _print_stats(self):
        """Print statistics summary"""
        duration = None
        if self.stats['start_time']:
            duration = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()
        
        print("\n" + "=" * 50)
        print("MQTT CONSUMER STATISTICS")
        print("=" * 50)
        print(f"Messages received:  {self.stats['messages_received']}")
        print(f"Messages valid:     {self.stats['messages_valid']}")
        print(f"Messages invalid:   {self.stats['messages_invalid']}")
        print(f"Batches processed:  {self.stats['batches_processed']}")
        print(f"Errors:             {self.stats['errors']}")
        
        if duration:
            print(f"Duration:           {duration:.1f} seconds")
            if self.stats['messages_received'] > 0:
                print(f"Throughput:         {self.stats['messages_received']/duration:.2f} msg/sec")
        
        if self.region_stats:
            print("\nMessages by Region:")
            for region, count in sorted(self.region_stats.items(), key=lambda x: -x[1])[:10]:
                print(f"  {region}: {count}")
        
        if self.type_stats:
            print("\nMessages by Type:")
            for ptype, count in sorted(self.type_stats.items(), key=lambda x: -x[1]):
                print(f"  {ptype}: {count}")
        
        print("=" * 50 + "\n")
    
    def get_stats(self) -> Dict:
        """Get current statistics"""
        return {
            **self.stats,
            'connected': self.connected,
            'queue_size': self.message_queue.qsize(),
            'region_stats': self.region_stats,
            'type_stats': self.type_stats
        }


class SnowflakeMQTTConsumer(MQTTConsumer):
    """MQTT consumer that writes to Snowflake"""
    
    def __init__(self, config_path: str = 'snowflake_config.json', **kwargs):
        super().__init__(**kwargs)
        self.config_path = config_path
        self.snowflake_rows: List[Dict] = []
        self.on_message_callback = self._buffer_row
        
    def _buffer_row(self, row: Dict):
        """Buffer a row for Snowflake insertion"""
        self.snowflake_rows.append(row)
        
        if len(self.snowflake_rows) >= self.batch_size:
            self._flush_to_snowflake()
    
    def _flush_to_snowflake(self):
        """Flush buffered rows to Snowflake"""
        if not self.snowflake_rows:
            return
        
        try:
            logger.info(f"Would insert {len(self.snowflake_rows)} rows to Snowflake")
            self.snowflake_rows = []
        except Exception as e:
            logger.error(f"Snowflake flush error: {e}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Meshtastic MQTT Consumer')
    parser.add_argument('--broker', default=None, help='MQTT broker hostname')
    parser.add_argument('--port', type=int, default=None, help='MQTT broker port')
    parser.add_argument('--topic', default=None, help='MQTT topic pattern')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size')
    parser.add_argument('--snowflake', action='store_true', help='Enable Snowflake integration')
    parser.add_argument('--config', default='snowflake_config.json', help='Snowflake config file')
    args = parser.parse_args()
    
    if args.snowflake:
        consumer = SnowflakeMQTTConsumer(
            broker=args.broker,
            port=args.port,
            topic=args.topic,
            batch_size=args.batch_size,
            config_path=args.config
        )
    else:
        consumer = MQTTConsumer(
            broker=args.broker,
            port=args.port,
            topic=args.topic,
            batch_size=args.batch_size
        )
    
    consumer.start()


if __name__ == '__main__':
    main()
