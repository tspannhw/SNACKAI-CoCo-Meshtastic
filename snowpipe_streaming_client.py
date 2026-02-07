#!/usr/bin/env python3
"""
Snowpipe Streaming v2 REST API Client for Meshtastic Mesh Network Data

PRODUCTION MODE - HIGH-PERFORMANCE STREAMING ONLY

This client EXCLUSIVELY uses the Snowpipe Streaming v2 REST API for
high-performance data ingestion. It does NOT use:
  - Direct INSERT statements (no Snowflake Connector)
  - Batch loading via COPY INTO
  - Stage-based ingestion
  
ONLY Snowpipe Streaming REST API endpoints are used:
  - /v2/streaming/hostname (discover ingest host)
  - /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel} (open/manage channel)
  - /v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/rows (append data)
  - /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}:bulk-channel-status (status)

Based on Snowflake's Snowpipe Streaming v2 REST API:
https://docs.snowflake.com/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api
"""

import json
import logging
import time
import sys
import os
from datetime import datetime, timezone
from typing import Dict, Optional, List
import requests

logger = logging.getLogger(__name__)


class SnowflakeJWTAuth:
    """Handles authentication for Snowflake (PAT or JWT)."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.account = config['account'].upper()
        self.user = config['user'].upper()
        
        if 'pat' in config and config['pat']:
            self.auth_method = 'pat'
            self.pat = config['pat']
            logger.info(f"PAT authentication initialized for user: {self.user}")
        elif 'private_key_file' in config and config['private_key_file']:
            self.auth_method = 'jwt'
            self.private_key = self._load_private_key()
            self.qualified_username = f"{self.account}.{self.user}"
            logger.info(f"JWT auth initialized for user: {self.qualified_username}")
        else:
            raise ValueError(
                "No authentication method configured. "
                "Provide either 'pat' (Programmatic Access Token) or 'private_key_file' in config."
            )
    
    def _load_private_key(self):
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        
        private_key_file = self.config['private_key_file']
        
        with open(private_key_file, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        
        logger.info(f"Private key loaded from {private_key_file}")
        return private_key
    
    def generate_jwt_token(self) -> str:
        import jwt
        from cryptography.hazmat.primitives import serialization
        from datetime import timedelta
        from hashlib import sha256
        
        public_key_bytes = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        public_key_fp = 'SHA256:' + sha256(public_key_bytes).hexdigest().upper()
        
        now = datetime.now(timezone.utc)
        iat = int(now.timestamp())
        exp = int((now + timedelta(hours=1)).timestamp())
        
        payload = {
            'iss': f"{self.qualified_username}.{public_key_fp}",
            'sub': self.qualified_username,
            'iat': iat,
            'exp': exp
        }
        
        token = jwt.encode(payload, self.private_key, algorithm='RS256')
        logger.debug("JWT token generated")
        return token
    
    def get_scoped_token(self, scope: str = None) -> str:
        if self.auth_method == 'pat':
            logger.debug("Using Programmatic Access Token (PAT)")
            return self.pat
        
        elif self.auth_method == 'jwt':
            return self._get_jwt_oauth_token(scope)
        
        else:
            raise ValueError(f"Unknown auth method: {self.auth_method}")
    
    def _get_jwt_oauth_token(self, scope: str = None) -> str:
        logger.info("Exchanging JWT for OAuth token...")
        
        jwt_token = self.generate_jwt_token()
        
        account = self.config['account'].lower()
        token_url = f"https://{account}.snowflakecomputing.com/oauth/token"
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        role = self.config.get('role', 'PUBLIC').upper()
        
        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': jwt_token,
            'scope': scope or f'session:role:{role}'
        }
        
        response = requests.post(token_url, headers=headers, data=data, timeout=30)
        response.raise_for_status()
        
        token_data = response.json()
        access_token = token_data.get('access_token')
        
        if not access_token:
            raise ValueError("No access_token in response")
        
        logger.info("OAuth token obtained successfully")
        return access_token


class SnowpipeStreamingClient:
    """
    Client for Snowpipe Streaming v2 REST API - PRODUCTION MODE
    
    This client EXCLUSIVELY uses Snowpipe Streaming v2 high-performance REST API.
    NO direct inserts, NO batch loading, ONLY streaming via REST endpoints.
    """
    
    def __init__(self, config_file: str = 'snowflake_config.json'):
        logger.info("=" * 70)
        logger.info("SNOWPIPE STREAMING CLIENT - PRODUCTION MODE")
        logger.info("Using ONLY Snowpipe Streaming v2 REST API")
        logger.info("NO direct inserts - HIGH-PERFORMANCE STREAMING ONLY")
        logger.info("=" * 70)
        
        self.config = self._load_config(config_file)
        self.jwt_auth = SnowflakeJWTAuth(self.config)
        self.is_pat = self.jwt_auth.auth_method == 'pat'
        
        self.control_host = None
        self.ingest_host = None
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_channel = self.config.get('channel_name', 'MESH_CHNL')
        self.channel_name = f"{base_channel}_{timestamp}"
        self.continuation_token = None
        self.offset_token = 0
        self.scoped_token = None
        self.token_expiry = None

        self.stats = {
            'total_rows_sent': 0,
            'total_batches': 0,
            'total_bytes_sent': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        logger.info("SnowpipeStreamingClient initialized")
        logger.info(f"Database: {self.config['database']}")
        logger.info(f"Schema: {self.config['schema']}")
        logger.info(f"Pipe: {self.config.get('pipe', 'N/A')}")
        logger.info(f"Channel: {self.channel_name}")
    
    def _load_config(self, config_file: str) -> Dict:
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from {config_file}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file {config_file} not found")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise
    
    def _ensure_valid_token(self):
        if self.scoped_token is None or (self.token_expiry and time.time() >= self.token_expiry):
            logger.info("Obtaining new scoped token...")
            self.scoped_token = self.jwt_auth.get_scoped_token()
            self.token_expiry = time.time() + 3000
            logger.info("New scoped token obtained")
    
    def _get_headers(self, compress: bool = False) -> Dict:
        headers = {
            'Authorization': f'Bearer {self.scoped_token}',
            'Content-Type': 'application/json',
        }
        
        if self.is_pat:
            headers['X-Snowflake-Authorization-Token-Type'] = 'PROGRAMMATIC_ACCESS_TOKEN'
        
        if compress:
            headers['Content-Encoding'] = 'gzip'
        
        return headers
    
    def discover_ingest_host(self) -> str:
        logger.info("Discovering ingest host...")
        
        account = self.config['account'].lower()
        self.control_host = f"https://{account}.snowflakecomputing.com"
        
        url = f"{self.control_host}/v2/streaming/hostname"
        
        self._ensure_valid_token()
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=30)
            response.raise_for_status()
            
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response body: {response.text}")
            
            if response.headers.get('Content-Type', '').startswith('application/json'):
                data = response.json()
                self.ingest_host = data.get('hostname') or data.get('ingest_host')
            else:
                self.ingest_host = response.text.strip()
            
            if not self.ingest_host:
                raise ValueError("No hostname returned from endpoint")
            
            logger.info(f"Ingest host discovered: {self.ingest_host}")
            return self.ingest_host
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to discover ingest host: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            raise
    
    def open_channel(self) -> Dict:
        logger.info(f"Opening channel: {self.channel_name}")
        
        if not self.ingest_host:
            self.discover_ingest_host()

        self._ensure_valid_token()
        
        db = self.config['database']
        schema = self.config['schema']
        pipe = self.config.get('pipe', self.config.get('table'))
        
        url = (
            f"https://{self.ingest_host}/v2/streaming"
            f"/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{self.channel_name}"
        )
        
        payload = {}
        
        try:
            response = requests.put(url, headers=self._get_headers(), json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            logger.debug(f"Open channel response: {json.dumps(data, indent=2)}")
            
            self.continuation_token = data.get('next_continuation_token')
            channel_status = data.get('channel_status', {})
            self.offset_token = channel_status.get('last_committed_offset_token')
            
            if self.offset_token is None:
                self.offset_token = 0
            
            logger.info(f"Channel opened successfully")
            logger.info(f"Continuation token: {self.continuation_token}")
            logger.info(f"Initial offset token: {self.offset_token}")
            
            if not self.continuation_token:
                logger.warning("No continuation token received! This may cause issues.")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to open channel: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response text: {e.response.text}")
            raise
    
    def insert_rows(self, rows: List[Dict]) -> int:
        if not rows:
            logger.warning("No rows to insert")
            return 0
        
        self.append_rows(rows)
        return len(rows)
    
    def append_rows(self, rows: List[Dict]) -> Dict:
        if not rows:
            logger.warning("No rows to append")
            return {}
        
        logger.info(f"Appending {len(rows)} rows...")
        
        if not self.ingest_host or not self.continuation_token:
            raise RuntimeError("Channel not opened. Call open_channel() first.")
        
        self._ensure_valid_token()
        
        new_offset = self.offset_token + 1
        
        db = self.config['database']
        schema = self.config['schema']
        pipe = self.config.get('pipe', self.config.get('table'))
        
        url = (
            f"https://{self.ingest_host}/v2/streaming/data"
            f"/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{self.channel_name}/rows"
        )
        
        params = {
            'continuationToken': self.continuation_token,
            'offsetToken': str(new_offset)
        }
        
        logger.debug(f"Append URL: {url}")
        logger.debug(f"Params: {params}")
        
        headers = self._get_headers()
        headers['Content-Type'] = 'application/x-ndjson'
        
        def convert_value(v, depth=0):
            if depth > 10:
                return str(v)
            if isinstance(v, bytes):
                return v.hex()
            elif isinstance(v, dict):
                return {k: convert_value(val, depth+1) for k, val in v.items()}
            elif isinstance(v, (list, tuple)):
                return [convert_value(item, depth+1) for item in v]
            elif hasattr(v, '__dict__') and not isinstance(v, (str, int, float, bool, type(None))):
                return str(v)
            return v
        
        def serialize_row(row):
            cleaned = convert_value(row)
            return json.dumps(cleaned, default=str)
        
        ndjson_data = '\n'.join(serialize_row(row) for row in rows)
        
        try:
            response = requests.post(
                url,
                params=params,
                headers=headers,
                data=ndjson_data.encode('utf-8'),
                timeout=30
            )
            
            if response.status_code >= 400:
                logger.error(f"Append failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
            
            response.raise_for_status()
            
            data = response.json()
            
            self.continuation_token = data.get('next_continuation_token')
            
            self.offset_token = new_offset
            
            self.stats['total_rows_sent'] += len(rows)
            self.stats['total_batches'] += 1
            self.stats['total_bytes_sent'] += len(ndjson_data)
            
            logger.info(f"Successfully appended {len(rows)} rows")
            logger.debug(f"New offset token: {self.offset_token}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.stats['errors'] += 1
            logger.error(f"Failed to append rows: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise
    
    def get_channel_status(self) -> Dict:
        logger.debug(f"Getting channel status: {self.channel_name}")
        
        if not self.ingest_host:
            raise RuntimeError("Ingest host not discovered. Call discover_ingest_host() first.")
        
        self._ensure_valid_token()
        
        db = self.config['database']
        schema = self.config['schema']
        pipe = self.config.get('pipe', self.config.get('table'))
        
        url = (
            f"https://{self.ingest_host}/v2/streaming"
            f"/databases/{db}/schemas/{schema}/pipes/{pipe}:bulk-channel-status"
        )
        
        payload = {
            'channel_names': [self.channel_name]
        }
        
        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            channel_statuses = data.get('channel_statuses', {})
            channel_status = channel_statuses.get(self.channel_name, {})
            
            committed_offset = channel_status.get('committed_offset_token', 0)
            logger.debug(f"Channel committed offset: {committed_offset}")
            
            return channel_status
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get channel status: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise
    
    def wait_for_commit(self, expected_offset: int, timeout: int = 60, poll_interval: int = 2) -> bool:
        logger.info(f"Waiting for offset {expected_offset} to be committed...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                status = self.get_channel_status()
                committed_offset = status.get('committed_offset_token', 0)
                
                if committed_offset >= expected_offset:
                    logger.info(f"Data committed! Offset: {committed_offset}")
                    return True
                
                logger.debug(f"Waiting... committed={committed_offset}, expected={expected_offset}")
                time.sleep(poll_interval)
                
            except Exception as e:
                logger.warning(f"Error checking status: {e}")
                time.sleep(poll_interval)
        
        logger.warning(f"Timeout waiting for commit after {timeout}s")
        return False
    
    def close_channel(self):
        logger.info(f"Closing channel: {self.channel_name}")
        logger.info("Channel will auto-close after inactivity period")
    
    def print_statistics(self):
        elapsed_time = time.time() - self.stats['start_time']
        
        logger.info("=" * 60)
        logger.info("INGESTION STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total rows sent: {self.stats['total_rows_sent']}")
        logger.info(f"Total batches: {self.stats['total_batches']}")
        logger.info(f"Total bytes sent: {self.stats['total_bytes_sent']:,}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
        
        if self.stats['total_rows_sent'] > 0:
            rows_per_sec = self.stats['total_rows_sent'] / elapsed_time
            logger.info(f"Average throughput: {rows_per_sec:.2f} rows/sec")
        
        logger.info(f"Current offset token: {self.offset_token}")
        logger.info("=" * 60)


def main():
    """Main entry point for testing."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('meshtastic_streaming.log')
        ]
    )
    
    logger.info("Starting Meshtastic Streaming Client Test")
    
    try:
        client = SnowpipeStreamingClient('snowflake_config.json')
        
        client.discover_ingest_host()
        
        client.open_channel()
        
        sample_data = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "packet_type": "TEST",
            "from_id": "!test1234",
            "from_num": 12345678,
            "to_id": "!test5678",
            "to_num": 87654321,
            "channel": 0,
            "rx_snr": 7.5,
            "rx_rssi": -65,
            "hop_limit": 3,
            "hop_start": 3,
            "latitude": 40.7128,
            "longitude": -74.0060,
            "altitude": 10.0,
            "battery_level": 85,
            "voltage": 3.8,
            "temperature": 22.5,
            "temperature_f": 72.5,
            "relative_humidity": 45.0,
            "barometric_pressure": 101325.0
        }
        
        rows = [sample_data]
        client.append_rows(rows)
        
        client.wait_for_commit(client.offset_token, timeout=30)
        
        client.print_statistics()
        
        logger.info("Test completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
