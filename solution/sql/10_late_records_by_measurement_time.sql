-- Report 10: Late records attributed by measurement time.
-- Grain: one row per (measurement hour of event_timestamp, event family).
-- Window: the 6 hours before the newest connectivity measurement. Anchored to the
--   data's own clock rather than current_timestamp, because the simulator clock can
--   lag wall time by hours (docs/exploring.md step 1).
-- Late: when the record arrived, Kafka already held a record from the same device and
--   family measured more than one reporting interval later (5 s connectivity, 30 s
--   health, 60 s throughput; 60 s for event-driven families). Both times are from the
--   source clock. A device's event times keep increasing across sessions, so ordering
--   per device (not per session) is enough.
-- lateness_seconds = how far behind that newer measurement the record was.
-- Rows are counted as stored (source duplicates, ~0.25%, included): de-duplicating
--   would hold every event_id in memory alongside the window sort.
WITH bounds AS (
    SELECT max(event_timestamp) - INTERVAL '6' HOUR AS since
    FROM iceberg.solution.connectivity_events
    WHERE event_timestamp >= current_timestamp - INTERVAL '2' DAY
),
arrived AS (
    SELECT 'connectivity_metric' AS event_family, 5 AS cadence_seconds, device_id, event_timestamp, _kafka_offset
    FROM iceberg.solution.connectivity_events, bounds WHERE event_timestamp >= since
    UNION ALL
    SELECT 'throughput_test', 60, device_id, event_timestamp, _kafka_offset
    FROM iceberg.solution.throughput_events, bounds WHERE event_timestamp >= since
    UNION ALL
    SELECT 'device_health', 30, device_id, event_timestamp, _kafka_offset
    FROM iceberg.solution.device_health_events, bounds WHERE event_timestamp >= since
    UNION ALL
    SELECT 'connectivity_state', 60, device_id, event_timestamp, _kafka_offset
    FROM iceberg.solution.connectivity_state_events, bounds WHERE event_timestamp >= since
    UNION ALL
    SELECT 'infrastructure_event', 60, device_id, event_timestamp, _kafka_offset
    FROM iceberg.solution.infrastructure_events, bounds WHERE event_timestamp >= since
),
lateness AS (
    SELECT event_family, event_timestamp, cadence_seconds,
           to_milliseconds(max(event_timestamp) OVER (
               PARTITION BY event_family, device_id
               ORDER BY _kafka_offset
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) - event_timestamp) / 1000.0 AS lateness_seconds
    FROM arrived
)
SELECT date_trunc('hour', event_timestamp)                                          AS measurement_hour,
       event_family,
       count(*)                                                                     AS records,
       count_if(lateness_seconds > cadence_seconds)                                 AS late_records,
       round(100.0 * count_if(lateness_seconds > cadence_seconds) / count(*), 4)    AS late_pct,
       max(IF(lateness_seconds > cadence_seconds, lateness_seconds))                AS max_lateness_seconds
FROM lateness
GROUP BY 1, 2
ORDER BY measurement_hour, event_family
