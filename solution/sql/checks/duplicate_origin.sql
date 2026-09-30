-- Duplicate origin by event table, over a recent window.
--
-- Every duplicate row is classified by its Kafka coordinates:
--   replay_rows   = the same Kafka record (topic, partition, offset) stored
--                   more than once -> our pipeline wrote it twice (crash between
--                   the Iceberg append and the offset commit).
--   upstream_rows = different Kafka records carrying the same event_id -> the
--                   source/bridge published it twice (at-least-once transport).
-- extra_rows = replay_rows + upstream_rows. See solution/TRADEOFFS.md, "Delivery is at-least-once".
--
-- Read-only. The window is on Kafka arrival time (_kafka_timestamp), not
-- event_timestamp: the simulator's clock can lag real time by hours after the
-- Docker VM pauses (docs/exploring.md step 1), and a check must not depend on
-- the source clock. The looser event_timestamp bound only lets Trino prune old
-- daily partitions; widen both deliberately.
WITH window_bounds AS (
    SELECT current_timestamp - INTERVAL '6' HOUR AS since,
           current_timestamp - INTERVAL '2' DAY AS prune_before
),
events AS (
    SELECT 'connectivity_events' AS table_name, event_id, _kafka_topic, _kafka_partition, _kafka_offset
    FROM iceberg.solution.connectivity_events, window_bounds WHERE _kafka_timestamp >= since AND event_timestamp >= prune_before
    UNION ALL
    SELECT 'throughput_events', event_id, _kafka_topic, _kafka_partition, _kafka_offset
    FROM iceberg.solution.throughput_events, window_bounds WHERE _kafka_timestamp >= since AND event_timestamp >= prune_before
    UNION ALL
    SELECT 'device_health_events', event_id, _kafka_topic, _kafka_partition, _kafka_offset
    FROM iceberg.solution.device_health_events, window_bounds WHERE _kafka_timestamp >= since AND event_timestamp >= prune_before
    UNION ALL
    SELECT 'connectivity_state_events', event_id, _kafka_topic, _kafka_partition, _kafka_offset
    FROM iceberg.solution.connectivity_state_events, window_bounds WHERE _kafka_timestamp >= since AND event_timestamp >= prune_before
    UNION ALL
    SELECT 'infrastructure_events', event_id, _kafka_topic, _kafka_partition, _kafka_offset
    FROM iceberg.solution.infrastructure_events, window_bounds WHERE _kafka_timestamp >= since AND event_timestamp >= prune_before
),
per_event AS (
    SELECT table_name,
           event_id,
           count(*) AS copies,
           count(DISTINCT ROW(_kafka_topic, _kafka_partition, _kafka_offset)) AS distinct_kafka_records
    FROM events
    GROUP BY table_name, event_id
)
SELECT table_name,
       sum(copies) AS total_rows,
       count_if(copies > 1) AS duplicated_events,
       sum(copies - 1) AS extra_rows,
       sum(copies - distinct_kafka_records) AS replay_rows,
       sum(distinct_kafka_records - 1) AS upstream_rows,
       round(100.0 * sum(copies - 1) / sum(copies), 3) AS duplicate_pct
FROM per_event
GROUP BY table_name
ORDER BY table_name
