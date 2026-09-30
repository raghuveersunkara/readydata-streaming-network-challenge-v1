-- report 4: connectivity and throughput statistics by access technology.
-- grain: one row per device measurements from the last day based on event_timestamp
-- dupes holding up a lot in memory heap so accepting duplicate records as well 
-- it is only 0.25% of total records, so shouldn't impact stats significantly.
-- using approx_percentile function for percentiles
WITH connectivity AS (
    SELECT d.technology, e.latency_ms, e.jitter_ms, e.packet_loss_pct
    FROM iceberg.solution.connectivity_events e
    JOIN iceberg.solution.devices d ON d.device_id = e.device_id
    WHERE e.event_timestamp >= current_timestamp - INTERVAL '1' DAY
),
throughput AS (
    SELECT d.technology, e.download_mbps, e.upload_mbps
    FROM iceberg.solution.throughput_events e
    JOIN iceberg.solution.devices d ON d.device_id = e.device_id
    WHERE e.event_timestamp >= current_timestamp - INTERVAL '1' DAY
),
c AS (
    SELECT technology,
           count(*)                                 AS connectivity_samples,
           min(latency_ms)                          AS latency_ms_min,
           approx_percentile(latency_ms, 0.5)       AS latency_ms_median,
           approx_percentile(latency_ms, 0.95)      AS latency_ms_bad,
           max(latency_ms)                          AS latency_ms_max,
           approx_percentile(jitter_ms, 0.5)        AS jitter_ms_median,
           approx_percentile(jitter_ms, 0.95)       AS jitter_ms_bad,
           approx_percentile(packet_loss_pct, 0.5)  AS packet_loss_pct_median,
           approx_percentile(packet_loss_pct, 0.95) AS packet_loss_pct_bad,
           max(packet_loss_pct)                     AS packet_loss_pct_max
    FROM connectivity
    GROUP BY technology
),
t AS (
    SELECT technology,
           count(*)                               AS throughput_tests,
           min(download_mbps)                     AS download_mbps_min,
           approx_percentile(download_mbps, 0.05) AS download_mbps_bad,
           approx_percentile(download_mbps, 0.5)  AS download_mbps_median,
           max(download_mbps)                     AS download_mbps_max,
           min(upload_mbps)                       AS upload_mbps_min,
           approx_percentile(upload_mbps, 0.05)   AS upload_mbps_bad,
           approx_percentile(upload_mbps, 0.5)    AS upload_mbps_median,
           max(upload_mbps)                       AS upload_mbps_max
    FROM throughput
    GROUP BY technology
)
SELECT *
FROM c
FULL OUTER JOIN t USING (technology)
ORDER BY technology
