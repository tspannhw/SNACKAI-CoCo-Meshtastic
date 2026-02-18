# Snowpipe Streaming V2 High-Performance Configuration

## Overview

Snowpipe Streaming V2 High-Performance Architecture provides:
- **Up to 10 GB/s** throughput per table
- **5-10 second** end-to-end latency (ingest to query)
- **Pre-clustering** during ingestion for faster queries
- **Rust-based client** core for lower resource usage

## Optimized Pipe Created

```sql
-- High-Performance Streaming Pipe with Pre-Clustering
PIPE: DEMO.DEMO.MESHTASTIC_HP_STREAM_PIPE
KIND: STREAMING
CLUSTER_AT_INGEST_TIME: TRUE
TABLE CLUSTERING: LINEAR(from_id, ingested_at)
```

## Client Configuration (Python SDK)

### Install SDK
```bash
pip install snowpipe-streaming
```

### profile.json (Optimized)
```json
{
  "authorization_type": "JWT",
  "url": "https://SFSENORTHAMERICA-TSPANN-AWS1.snowflakecomputing.com",
  "user": "kafkaguy",
  "account": "SFSENORTHAMERICA-TSPANN-AWS1",
  "private_key_file": "/path/to/rsa_key.p8",
  "role": "ACCOUNTADMIN"
}
```

### Environment Variables (Speed Optimizations)
```bash
# Enable metrics for monitoring throughput
export SS_ENABLE_METRICS=TRUE
export SS_METRICS_PORT=50000
export SS_METRICS_IP=0.0.0.0

# Reduce logging overhead in production
export SS_LOG_LEVEL=warn
```

### Python Client Code
```python
from snowpipe_streaming import SnowpipeStreamingClient

# Initialize client with optimized settings
client = SnowpipeStreamingClient(
    profile_path="profile.json"
)

# Open channel against the HIGH-PERFORMANCE pipe
channel = client.open_channel(
    pipe_name="DEMO.DEMO.MESHTASTIC_HP_STREAM_PIPE",
    channel_name="meshtastic_channel_1"
)

# Insert rows (batched for performance)
rows = [
    {
        "ingested_at": "2026-02-11T15:00:00Z",
        "packet_type": "position",
        "from_id": "!abcd1234",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "battery_level": 85,
        "rx_snr": 12.5
    }
    # ... more rows
]

# Batch insert for maximum throughput
channel.insert_rows(rows)

# Get offset token for exactly-once delivery
offset_token = channel.get_latest_committed_offset_token()
```

## Java SDK Configuration

### Maven Dependency
```xml
<dependency>
    <groupId>com.snowflake</groupId>
    <artifactId>snowpipe-streaming</artifactId>
    <version>LATEST</version>
</dependency>
```

### Java Client Code
```java
import com.snowflake.ingest.streaming.*;

Map<String, Object> config = new HashMap<>();
config.put("authorization_type", "JWT");
config.put("url", "https://SFSENORTHAMERICA-TSPANN-AWS1.snowflakecomputing.com");
config.put("user", "kafkaguy");
config.put("account", "SFSENORTHAMERICA-TSPANN-AWS1");
config.put("private_key_file", "/path/to/rsa_key.p8");
config.put("role", "ACCOUNTADMIN");

// Create client
SnowpipeStreamingClient client = SnowpipeStreamingClient.builder()
    .setProperties(config)
    .build();

// Open channel against HP pipe
SnowpipeStreamingChannel channel = client.openChannel(
    OpenChannelRequest.builder()
        .setPipeName("DEMO.DEMO.MESHTASTIC_HP_STREAM_PIPE")
        .setChannelName("meshtastic_channel_java")
        .build()
);

// Insert with batching for speed
Map<String, Object> row = new HashMap<>();
row.put("ingested_at", Instant.now().toString());
row.put("packet_type", "position");
row.put("from_id", "!abcd1234");
row.put("latitude", 40.7128);
row.put("longitude", -74.0060);

InsertValidationResponse response = channel.insertRow(row, String.valueOf(offset));
```

## Performance Tuning Tips

### 1. Batch Size
```python
# Optimal batch size: 1,000 - 10,000 rows per insert
BATCH_SIZE = 5000
buffer = []

for event in event_stream:
    buffer.append(event)
    if len(buffer) >= BATCH_SIZE:
        channel.insert_rows(buffer)
        buffer = []
```

### 2. Multiple Channels (Parallel Ingestion)
```python
# Use multiple channels for higher throughput
channels = []
for i in range(4):  # 4 parallel channels
    ch = client.open_channel(
        pipe_name="DEMO.DEMO.MESHTASTIC_HP_STREAM_PIPE",
        channel_name=f"meshtastic_channel_{i}"
    )
    channels.append(ch)

# Round-robin distribution
for i, row in enumerate(rows):
    channels[i % len(channels)].insert_row(row)
```

### 3. Pre-Clustering Benefits
With `CLUSTER_AT_INGEST_TIME = TRUE`:
- Data is sorted during ingestion
- Queries on `from_id` and `ingested_at` are 2-5x faster
- Reduces reclustering costs

### 4. Schema Evolution
```sql
-- Enable schema evolution on table
ALTER TABLE DEMO.DEMO.MESHTASTIC_DATA 
    SET ENABLE_SCHEMA_EVOLUTION = TRUE;
```

## Monitoring Performance

### Channel History View
```sql
-- Monitor ingestion latency
SELECT 
    channel_name,
    start_time,
    end_time,
    DATEDIFF(second, start_time, end_time) as latency_seconds,
    row_count,
    bytes_inserted
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY
WHERE pipe_name = 'MESHTASTIC_HP_STREAM_PIPE'
ORDER BY start_time DESC
LIMIT 20;
```

### Throughput Metrics
```sql
-- Calculate throughput
SELECT 
    DATE_TRUNC('minute', start_time) as minute,
    SUM(row_count) as rows_per_minute,
    SUM(bytes_inserted) / 1024 / 1024 as mb_per_minute,
    AVG(DATEDIFF(second, start_time, end_time)) as avg_latency_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY
WHERE pipe_name = 'MESHTASTIC_HP_STREAM_PIPE'
GROUP BY 1
ORDER BY 1 DESC;
```

### Prometheus Metrics
With `SS_ENABLE_METRICS=TRUE`, access metrics at:
```
http://localhost:50000/metrics
```

Key metrics:
- `snowpipe_streaming_rows_inserted_total`
- `snowpipe_streaming_bytes_inserted_total`
- `snowpipe_streaming_insert_latency_seconds`

## Cost Optimization

High-Performance Architecture uses **throughput-based billing**:
- Charged per TB ingested
- No compute charges for ingestion
- Pre-clustering included in ingestion cost

```sql
-- Monitor streaming costs
SELECT 
    service_type,
    SUM(credits_used) as total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE service_type = 'SNOWPIPE_STREAMING'
    AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY service_type;
```

## Summary

| Setting | Value |
|---------|-------|
| Pipe | `MESHTASTIC_HP_STREAM_PIPE` |
| Architecture | High-Performance V2 |
| Pre-Clustering | Enabled |
| Table Clustering | `(from_id, ingested_at)` |
| Target Latency | 5-10 seconds |
| Max Throughput | 10 GB/s |
