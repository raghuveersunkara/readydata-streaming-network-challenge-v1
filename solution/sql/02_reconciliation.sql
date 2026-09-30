-- Report 2: Reconciliation of duplicates, sequence gaps, rejects and
-- duplicate-safe counts. (Late data: see the perspective 10 report.)
-- Grain: one row per Kafka topic (one topic per event family).
-- Window: records that arrived in Kafka in the 6 hours before the newest arrival. Arrival
--   time (_kafka_timestamp) is used for event tables and rejected_events alike, so both
--   sides of delivered = accepted + rejected share one clock. An exact distinct count
--   over all history exceeds Trino's per-query memory on this stack.
-- unique_events = duplicate-safe count; duplicate_rows = stored_rows - unique_events.
-- missing_sequence_numbers = holes inside each (device, session) stream's observed
--   sequence range; rejected records also show up as holes.
WITH bounds AS (
    SELECT max(_kafka_timestamp) - INTERVAL '6' HOUR AS since
    FROM iceberg.solution.connectivity_events
),
events AS (
    SELECT _kafka_topic, event_id, device_id, device_session_id, sequence_number FROM iceberg.solution.connectivity_events
        WHERE _kafka_timestamp >= (SELECT since FROM bounds)
    UNION ALL
    SELECT _kafka_topic, event_id, device_id, device_session_id, sequence_number FROM iceberg.solution.throughput_events
        WHERE _kafka_timestamp >= (SELECT since FROM bounds)
    UNION ALL
    SELECT _kafka_topic, event_id, device_id, device_session_id, sequence_number FROM iceberg.solution.device_health_events
        WHERE _kafka_timestamp >= (SELECT since FROM bounds)
    UNION ALL
    SELECT _kafka_topic, event_id, device_id, device_session_id, sequence_number FROM iceberg.solution.connectivity_state_events
        WHERE _kafka_timestamp >= (SELECT since FROM bounds)
    UNION ALL
    SELECT _kafka_topic, event_id, device_id, device_session_id, sequence_number FROM iceberg.solution.infrastructure_events
        WHERE _kafka_timestamp >= (SELECT since FROM bounds)
),
streams AS (
    SELECT _kafka_topic,
           count(*)                                                                          AS stored_rows,
           count(DISTINCT event_id)                                                          AS unique_events,
           max(sequence_number) - min(sequence_number) + 1 - count(DISTINCT sequence_number) AS missing_sequence_numbers
    FROM events
    GROUP BY _kafka_topic, device_id, device_session_id
),
rejects AS (
    SELECT _kafka_topic, count(*) AS rejected_rows
    FROM iceberg.solution.rejected_events
    WHERE _kafka_timestamp >= (SELECT since FROM bounds)
    GROUP BY _kafka_topic
)
SELECT s._kafka_topic                            AS kafka_topic,
       sum(s.stored_rows)                        AS stored_rows,
       sum(s.unique_events)                      AS unique_events,
       sum(s.stored_rows) - sum(s.unique_events) AS duplicate_rows,
       sum(s.missing_sequence_numbers)           AS missing_sequence_numbers,
       coalesce(max(r.rejected_rows), 0)         AS rejected_rows
FROM streams s
LEFT JOIN rejects r ON r._kafka_topic = s._kafka_topic
GROUP BY s._kafka_topic
ORDER BY kafka_topic
