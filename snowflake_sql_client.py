#!/usr/bin/env python3
"""
Snowflake SQL Client - Direct INSERT via REST API
Uses PAT authentication for simple, fast data insertion
"""
import json
import logging
import requests
import uuid
from typing import Dict, List, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class SnowflakeSQLClient:
    def __init__(self, config: Dict):
        self.config = config
        self.account = config['account'].lower()
        self.user = config['user'].upper()
        self.database = config['database'].upper()
        self.schema = config['schema'].upper()
        self.table = config['table'].upper()
        self.role = config.get('role', 'PUBLIC').upper()
        self.warehouse = config.get('warehouse', 'COMPUTE_WH').upper()
        
        if 'pat' in config and config['pat']:
            self.token = config['pat']
            self.token_type = 'PROGRAMMATIC_ACCESS_TOKEN'
        else:
            raise ValueError("PAT token required in config")
        
        self.base_url = f"https://{self.account}.snowflakecomputing.com"
        self.session = requests.Session()
        
        logger.info(f"SQL client initialized for {self.database}.{self.schema}.{self.table}")
    
    def _get_headers(self) -> Dict:
        return {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Snowflake-Authorization-Token-Type': self.token_type
        }
    
    def execute_sql(self, sql: str, bindings: Dict = None) -> Dict:
        url = f"{self.base_url}/api/v2/statements"
        
        payload = {
            'statement': sql,
            'timeout': 60,
            'database': self.database,
            'schema': self.schema,
            'warehouse': self.warehouse,
            'role': self.role
        }
        
        if bindings:
            payload['bindings'] = bindings
        
        response = self.session.post(
            url,
            headers=self._get_headers(),
            json=payload,
            timeout=120
        )
        
        if response.status_code not in [200, 202]:
            logger.error(f"SQL error: {response.status_code} - {response.text}")
            response.raise_for_status()
        
        return response.json()
    
    def insert_row(self, row: Dict) -> bool:
        columns = list(row.keys())
        placeholders = ', '.join([f'?' for _ in columns])
        col_names = ', '.join([c.upper() for c in columns])
        
        sql = f"INSERT INTO {self.database}.{self.schema}.{self.table} ({col_names}) VALUES ({placeholders})"
        
        bindings = {}
        for i, (col, val) in enumerate(row.items(), 1):
            if val is None:
                bindings[str(i)] = {'type': 'TEXT', 'value': None}
            elif isinstance(val, bool):
                bindings[str(i)] = {'type': 'BOOLEAN', 'value': str(val).lower()}
            elif isinstance(val, int):
                bindings[str(i)] = {'type': 'FIXED', 'value': str(val)}
            elif isinstance(val, float):
                bindings[str(i)] = {'type': 'REAL', 'value': str(val)}
            elif isinstance(val, dict):
                bindings[str(i)] = {'type': 'TEXT', 'value': json.dumps(val)}
            else:
                bindings[str(i)] = {'type': 'TEXT', 'value': str(val)}
        
        result = self.execute_sql(sql, bindings)
        return result.get('statementStatusUrl') is not None
    
    def insert_rows(self, rows: List[Dict]) -> int:
        if not rows:
            return 0
        
        columns = list(rows[0].keys())
        col_names = ', '.join([c.upper() for c in columns])
        
        values_list = []
        for row in rows:
            vals = []
            for col in columns:
                val = row.get(col)
                if val is None:
                    vals.append('NULL')
                elif isinstance(val, bool):
                    vals.append('TRUE' if val else 'FALSE')
                elif isinstance(val, (int, float)):
                    vals.append(str(val))
                elif isinstance(val, dict):
                    vals.append(f"PARSE_JSON('{json.dumps(val)}')")
                else:
                    escaped = str(val).replace("'", "''")
                    vals.append(f"'{escaped}'")
            values_list.append(f"({', '.join(vals)})")
        
        sql = f"INSERT INTO {self.database}.{self.schema}.{self.table} ({col_names}) VALUES {', '.join(values_list)}"
        
        try:
            result = self.execute_sql(sql)
            logger.info(f"Inserted {len(rows)} rows")
            return len(rows)
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            return 0
    
    def insert_batch(self, rows: List[Dict], batch_size: int = 100) -> int:
        total = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            total += self.insert_rows(batch)
        return total


def main():
    logging.basicConfig(level=logging.INFO)
    
    try:
        with open('snowflake_config.json', 'r') as f:
            config = json.load(f)
        
        config['warehouse'] = 'INGEST'
        
        client = SnowflakeSQLClient(config)
        
        test_row = {
            'packet_type': 'test',
            'from_id': '!test1234',
            'temperature': 22.5,
            'battery_level': 85,
            'latitude': 40.7128,
            'longitude': -74.0060
        }
        
        result = client.insert_rows([test_row])
        print(f"Inserted {result} test row(s)")
        
        verify = client.execute_sql(f"SELECT COUNT(*) FROM {config['database']}.{config['schema']}.{config['table']}")
        print(f"Query result: {verify}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)


if __name__ == '__main__':
    main()
