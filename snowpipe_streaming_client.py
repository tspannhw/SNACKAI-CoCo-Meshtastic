#!/usr/bin/env python3
import json
import logging
import requests
import gzip
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class SnowpipeStreamingClient:
    def __init__(self, config: Dict, auth):
        self.config = config
        self.auth = auth
        self.account = config['account'].lower()
        self.database = config['database'].upper()
        self.schema = config['schema'].upper()
        self.pipe = config.get('pipe', f"SNOWPIPE_STREAMING_{config['table'].upper()}").upper()
        self.table = config['table'].upper()
        self.channel_name = config.get('channel_name', 'meshtastic_channel').upper()
        
        self.hostname = None
        self.continuation_token = None
        self.session = requests.Session()
        
    def _get_base_url(self) -> str:
        if self.hostname:
            return f"https://{self.hostname}"
        return f"https://{self.account}.snowflakecomputing.com"
    
    def _get_headers(self, scoped_token: str = None, compress: bool = False) -> Dict:
        if scoped_token is None:
            scoped_token = self.auth.get_scoped_token()
        
        headers = {
            'Authorization': f'Bearer {scoped_token}',
            'Content-Type': 'application/json',
            'X-Snowflake-Authorization-Token-Type': 'OAUTH'
        }
        
        if compress:
            headers['Content-Encoding'] = 'gzip'
        
        return headers
    
    def get_hostname(self) -> str:
        url = f"{self._get_base_url()}/v2/streaming/hostname"
        
        logger.info(f"Getting streaming hostname from {url}")
        
        response = self.session.get(
            url,
            headers=self._get_headers(),
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        self.hostname = data.get('hostname')
        
        logger.info(f"Got streaming hostname: {self.hostname}")
        return self.hostname
    
    def get_scoped_streaming_token(self) -> str:
        if not self.hostname:
            self.get_hostname()
        
        scope = f"session:role:{self.config.get('role', 'PUBLIC').upper()} {self.hostname}"
        return self.auth.get_scoped_token(scope)
    
    def open_channel(self, offset_token: str = None) -> Dict:
        if not self.hostname:
            self.get_hostname()
        
        url = (
            f"https://{self.hostname}/v2/streaming/databases/{self.database}"
            f"/schemas/{self.schema}/pipes/{self.pipe}/channels/{self.channel_name}"
        )
        
        request_id = str(uuid.uuid4())
        url = f"{url}?requestId={request_id}"
        
        logger.info(f"Opening channel: {self.channel_name}")
        
        payload = {}
        if offset_token:
            payload['offset_token'] = offset_token
        
        scoped_token = self.get_scoped_streaming_token()
        
        response = self.session.put(
            url,
            headers=self._get_headers(scoped_token),
            json=payload if payload else None,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        self.continuation_token = data.get('next_continuation_token')
        
        logger.info(f"Channel opened. Status: {data.get('channel_status', {}).get('channel_status_code')}")
        logger.debug(f"Continuation token: {self.continuation_token}")
        
        return data
    
    def append_rows(self, rows: List[Dict], offset_token: str = None, compress: bool = True) -> Dict:
        if not self.continuation_token:
            raise ValueError("Channel not opened. Call open_channel() first.")
        
        if not self.hostname:
            self.get_hostname()
        
        url = (
            f"https://{self.hostname}/v2/streaming/data/databases/{self.database}"
            f"/schemas/{self.schema}/pipes/{self.pipe}/channels/{self.channel_name}/rows"
        )
        
        request_id = str(uuid.uuid4())
        params = {
            'requestId': request_id,
            'continuationToken': self.continuation_token
        }
        if offset_token:
            params['offsetToken'] = offset_token
        
        url = f"{url}?" + "&".join(f"{k}={v}" for k, v in params.items())
        
        ndjson_lines = [json.dumps(row) for row in rows]
        payload = "\n".join(ndjson_lines) + "\n"
        
        scoped_token = self.get_scoped_streaming_token()
        headers = self._get_headers(scoped_token, compress=compress)
        
        if compress:
            payload_bytes = gzip.compress(payload.encode('utf-8'))
        else:
            payload_bytes = payload.encode('utf-8')
            headers['Content-Type'] = 'application/x-ndjson'
        
        logger.debug(f"Appending {len(rows)} rows to channel")
        
        response = self.session.post(
            url,
            headers=headers,
            data=payload_bytes,
            timeout=60
        )
        response.raise_for_status()
        
        data = response.json()
        self.continuation_token = data.get('next_continuation_token')
        
        logger.debug(f"Rows appended. New continuation token received.")
        return data
    
    def get_channel_status(self) -> Dict:
        if not self.hostname:
            self.get_hostname()
        
        url = (
            f"https://{self.hostname}/v2/streaming/databases/{self.database}"
            f"/schemas/{self.schema}/pipes/{self.pipe}:bulk-channel-status"
        )
        
        payload = {
            'channel_names': [self.channel_name]
        }
        
        scoped_token = self.get_scoped_streaming_token()
        
        response = self.session.post(
            url,
            headers=self._get_headers(scoped_token),
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        return data.get('channel_statuses', {}).get(self.channel_name, {})
    
    def drop_channel(self) -> bool:
        if not self.hostname:
            self.get_hostname()
        
        url = (
            f"https://{self.hostname}/v2/streaming/databases/{self.database}"
            f"/schemas/{self.schema}/pipes/{self.pipe}/channels/{self.channel_name}"
        )
        
        request_id = str(uuid.uuid4())
        url = f"{url}?requestId={request_id}"
        
        logger.info(f"Dropping channel: {self.channel_name}")
        
        scoped_token = self.get_scoped_streaming_token()
        
        response = self.session.delete(
            url,
            headers=self._get_headers(scoped_token),
            timeout=30
        )
        response.raise_for_status()
        
        self.continuation_token = None
        logger.info("Channel dropped successfully")
        return True
    
    def insert_row(self, row: Dict, offset_token: str = None) -> Dict:
        return self.append_rows([row], offset_token)
    
    def insert_batch(self, rows: List[Dict], batch_size: int = 1000) -> int:
        total_inserted = 0
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            offset_token = f"batch_{i}_{datetime.now(timezone.utc).isoformat()}"
            
            self.append_rows(batch, offset_token)
            total_inserted += len(batch)
            
            logger.info(f"Inserted batch {i // batch_size + 1}: {len(batch)} rows (total: {total_inserted})")
        
        return total_inserted


def main():
    import json
    from snowflake_jwt_auth import SnowflakeJWTAuth
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        with open('snowflake_config.json', 'r') as f:
            config = json.load(f)
        
        auth = SnowflakeJWTAuth(config)
        client = SnowpipeStreamingClient(config, auth)
        
        client.get_hostname()
        print(f"Streaming hostname: {client.hostname}")
        
        client.open_channel()
        print("Channel opened successfully")
        
        test_row = {
            "test_field": "hello",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        client.insert_row(test_row)
        print("Test row inserted")
        
        status = client.get_channel_status()
        print(f"Channel status: {status}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)


if __name__ == '__main__':
    main()
