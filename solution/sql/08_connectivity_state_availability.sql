-- Report 8: Connectivity-state duration and availability from event time.
-- Grain: one row per access device that reported at least one state transition in
--   the last day. Before its first transition a device's state is unknown, so time
--   is counted from the first transition.
-- Each transition opens an interval in new_state that lasts until the device's next
--   transition. The last interval is closed at the newest connectivity measurement
--   (source clock, so simulator clock lag does not stretch it).
-- availability_pct = share of observed time not offline (degraded still carries traffic).
WITH states AS (
    SELECT DISTINCT event_id, device_id, event_timestamp, new_state
    FROM iceberg.solution.connectivity_state_events
    WHERE event_timestamp >= current_timestamp - INTERVAL '1' DAY
),
observation_end AS (
    SELECT max(event_timestamp) AS ended_at
    FROM iceberg.solution.connectivity_events
    WHERE event_timestamp >= current_timestamp - INTERVAL '1' DAY
),
intervals AS (
    SELECT device_id, new_state AS state,
           to_milliseconds(
               coalesce(lead(event_timestamp) OVER (PARTITION BY device_id ORDER BY event_timestamp, event_id),
                        (SELECT ended_at FROM observation_end))
               - event_timestamp) / 60000.0 AS minutes
    FROM states
)
SELECT device_id,
       round(sum(minutes), 1)                                          AS observed_minutes,
       round(sum(IF(state = 'online', minutes, 0)), 1)                 AS online_minutes,
       round(sum(IF(state = 'degraded', minutes, 0)), 1)               AS degraded_minutes,
       round(sum(IF(state = 'offline', minutes, 0)), 1)                AS offline_minutes,
       count_if(state = 'offline')                                     AS offline_periods,
       round(100.0 * sum(IF(state <> 'offline', minutes, 0)) / sum(minutes), 2) AS availability_pct
FROM intervals
GROUP BY device_id
ORDER BY availability_pct, device_id
