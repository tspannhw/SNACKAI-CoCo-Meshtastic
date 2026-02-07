# Example Prompts for Meshtastic Semantic View

Use these prompts with Cortex Analyst against the `DEMO.DEMO.MESHTASTIC_SEMANTIC_VIEW` semantic view.

## Node & Network Overview

1. **How many unique nodes are in the mesh network?**

2. **Show me all active nodes with their last seen time**

3. **What is the total packet count by type?**

4. **Which nodes have been most active in the last 24 hours?**

5. **What's the average signal quality across the network?**

## GPS & Location Queries

6. **Show me all node positions with coordinates**

7. **Which nodes have GPS data and where are they located?**

8. **What is the average altitude of nodes with position data?**

9. **Find nodes that moved more than 1km in the last hour**

10. **How many satellites do nodes typically see?**

## Battery & Power Analysis

11. **What is the average battery level across all nodes?**

12. **Which nodes have low battery (below 20%)?**

13. **Show battery trends over the last 24 hours**

14. **What's the minimum and maximum battery level observed?**

15. **Which node has the lowest battery right now?**

## Environmental Sensors

16. **What is the average temperature reported by nodes?**

17. **Show humidity and temperature readings over time**

18. **What's the barometric pressure trend today?**

19. **Which node recorded the highest temperature?**

20. **Compare environmental readings between different nodes**

## Signal Quality Analysis

21. **What is the average SNR (signal-to-noise ratio)?**

22. **Which nodes have the best signal quality?**

23. **Show SNR trends over the last 6 hours**

24. **Find packets with poor signal quality (SNR below 0)**

25. **What's the correlation between distance and signal strength?**

## Message Analysis

26. **How many text messages were sent today?**

27. **Show me the most recent messages**

28. **Which nodes send the most messages?**

29. **What channels are most active for messaging?**

30. **Find all broadcast messages (to_id = ^all)**

## Time-Based Analysis

31. **What's the hourly packet count trend?**

32. **When is the network most active?**

33. **Compare today's activity to yesterday**

34. **Show packet counts by hour of day**

35. **What was the network activity last week?**

## Device Health

36. **What's the average uptime across nodes?**

37. **Which nodes have been running the longest?**

38. **Show channel utilization over time**

39. **What's the airtime TX utilization?**

40. **Find nodes with high channel utilization (above 50%)**

## Advanced Analytics

41. **Group nodes by their typical battery level range**

42. **Calculate the packet rate per node per hour**

43. **Find nodes that haven't reported in the last hour**

44. **What percentage of packets are position updates vs telemetry?**

45. **Show the distribution of hop limits across packets**

## Comparative Queries

46. **Compare battery drain rates between different node types**

47. **Which node has the most consistent signal quality?**

48. **Compare morning vs afternoon network activity**

49. **Rank nodes by total packets sent**

50. **Find the node with the best GPS accuracy (most satellites)**

---

## Sample SQL Generated

For prompt: "What is the average battery level across all nodes?"

```sql
SELECT 
    AVG(battery_level) as avg_battery
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE battery_level IS NOT NULL
```

For prompt: "Show me nodes with low battery"

```sql
SELECT 
    from_id,
    battery_level,
    voltage,
    MAX(ingested_at) as last_seen
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE battery_level IS NOT NULL 
  AND battery_level < 20
GROUP BY from_id, battery_level, voltage
ORDER BY battery_level ASC
```

For prompt: "What's the hourly packet count trend?"

```sql
SELECT 
    DATE_TRUNC('hour', ingested_at) as hour,
    packet_type,
    COUNT(*) as packet_count,
    COUNT(DISTINCT from_id) as unique_nodes
FROM DEMO.DEMO.MESHTASTIC_DATA
GROUP BY DATE_TRUNC('hour', ingested_at), packet_type
ORDER BY hour DESC
```
