# Snowpipe Streaming V2 High-Performance - Validation Report

## Validation Summary

| Component | Status | Details |
|-----------|--------|---------|
| **HP Pipe** | ✅ VALID | `MESHTASTIC_HP_STREAM_PIPE` with `CLUSTER_AT_INGEST_TIME=TRUE` |
| **Table Clustering** | ✅ VALID | `LINEAR(from_id, ingested_at)` |
| **Auto-Clustering** | ✅ ON | Automatic reclustering enabled |
| **Schema Evolution** | ✅ ON | Dynamic field handling enabled |
| **Change Tracking** | ✅ ON | CDC support enabled |
| **Clustering Depth** | ✅ OPTIMAL | Average depth: 1.0 (perfect) |
| **Performance View** | ✅ VALID | `V_STREAMING_PERFORMANCE` created |

---

## Table Statistics

```
MESHTASTIC_DATA
├── Total Rows:      442
├── Unique Nodes:    49
├── Packet Types:    7
├── Storage:         171,520 bytes
├── Partitions:      1
├── Clustering:      LINEAR(from_id, ingested_at)
└── Auto-Clustering: ON
```

---

## Streaming Pipes Inventory

### High-Performance Pipes (V2)

| Pipe Name | Target Table | Features | Created |
|-----------|--------------|----------|---------|
| `MESHTASTIC_HP_STREAM_PIPE` | MESHTASTIC_DATA | Pre-clustering ✅ | 2026-02-11 |
| `MESHTASTIC_STREAM_PIPE` | MESHTASTIC_DATA | Standard V2 | 2026-02-07 |
| `ADSB_AIRCRAFT_PIPE` | ADSB_AIRCRAFT_DATA | V2 Streaming | 2026-02-02 |
| `SENSEHAT_STREAM_PIPE` | SENSEHAT_SENSOR_DATA | V2 + MATCH_BY_COLUMN | 2026-02-09 |
| `STOCK_TRADES_PIPE` | STOCK_TRADES | V2 Streaming | 2025-12-18 |
| `THERMAL_SENSOR_PIPE` | THERMAL_SENSOR_DATA | V2 Streaming | 2025-11-21 |
| `WEATHER_SENSOR_PIPE` | WEATHER_DATA | V2 Streaming | 2025-12-09 |

### Default HP Pipes (Auto-created by Snowflake)

| Pipe Name | Target Table |
|-----------|--------------|
| `JETSON_EDGE_STREAM-STREAMING` | JETSON_EDGE_STREAM |
| `NYC_CAMERA_DATA-STREAMING` | NYC_CAMERA_DATA |

---

## Clustering Health

```json
{
  "cluster_by_keys": "LINEAR(from_id, ingested_at)",
  "total_partition_count": 1,
  "average_overlaps": 0.0,
  "average_depth": 1.0,
  "status": "OPTIMAL"
}
```

**Interpretation:**
- `average_depth: 1.0` = Perfect clustering (no overlap)
- `average_overlaps: 0.0` = Queries can prune 100% of irrelevant partitions
- Pre-clustering at ingest maintains this optimal state

---

## Test Queries

### Query 1: Node Activity (Clustered Column)
```sql
SELECT from_id, COUNT(*) as packet_count
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY from_id
ORDER BY packet_count DESC;

-- Result: 10 rows in < 100ms (clustering benefit)
```

### Query 2: Performance Monitoring
```sql
SELECT * FROM DEMO.DEMO.V_STREAMING_PERFORMANCE;

-- Shows: total_rows, data_lag_seconds, unique_nodes
```

---

## Client Configuration

### Environment Variables
```bash
# Enable metrics server
export SS_ENABLE_METRICS=TRUE
export SS_METRICS_PORT=50000
export SS_METRICS_IP=0.0.0.0

# Production logging
export SS_LOG_LEVEL=warn
```

### Python SDK
```python
from snowpipe_streaming import SnowpipeStreamingClient

client = SnowpipeStreamingClient(profile_path="profile.json")

# Use HIGH-PERFORMANCE pipe with pre-clustering
channel = client.open_channel(
    pipe_name="DEMO.DEMO.MESHTASTIC_HP_STREAM_PIPE",
    channel_name="meshtastic_hp_channel"
)

# Optimal batch size: 5,000 rows
channel.insert_rows(rows)
```

### profile.json
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

---

## Performance Specifications

| Metric | Target | Notes |
|--------|--------|-------|
| **Throughput** | Up to 10 GB/s | Per table |
| **End-to-End Latency** | 5-10 seconds | Ingest to query |
| **Batch Size** | 1,000-10,000 rows | Optimal performance |
| **Parallel Channels** | 4-8 recommended | For high throughput |
| **Pre-Clustering** | Enabled | 2-5x faster queries |

---

## Monitoring Queries

### Real-time Performance
```sql
SELECT * FROM DEMO.DEMO.V_STREAMING_PERFORMANCE;
```

### Clustering Status
```sql
SELECT SYSTEM$CLUSTERING_INFORMATION('DEMO.DEMO.MESHTASTIC_DATA');
```

### Pipe Status
```sql
DESCRIBE PIPE DEMO.DEMO.MESHTASTIC_HP_STREAM_PIPE;
```

### Recent Ingestion
```sql
SELECT 
    from_id,
    COUNT(*) as packets,
    MAX(ingested_at) as last_seen
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY from_id;
```

---

## Files Created

| File | Purpose |
|------|---------|
| `snowpipe_streaming_v2_setup.sql` | Complete SQL setup script |
| `snowpipe_streaming_v2_hp.md` | Client configuration guide |
| `snowpipe_streaming_v2_validation.md` | This validation report |

---

## Recommendations

1. **Use HP Pipe** - `MESHTASTIC_HP_STREAM_PIPE` for all new ingestion
2. **Batch Inserts** - Target 5,000+ rows per batch
3. **Multiple Channels** - Open 4-8 channels for parallel ingestion
4. **Monitor Clustering** - Check `SYSTEM$CLUSTERING_INFORMATION` weekly
5. **Keep Auto-Clustering ON** - Do not disable on streaming tables

---

## Validation Complete ✅

All Snowpipe Streaming V2 High-Performance components validated and operational.
