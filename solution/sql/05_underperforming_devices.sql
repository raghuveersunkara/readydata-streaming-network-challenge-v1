-- report 5: access devices underperforming their peer group.
-- grain: one row per underperforming access device, 
-- assumptions: 
--    - records only from the last day.
--    - peer group same technology AND same advertised download tier\
--    - device measure nased on median download (throughput) and median latency (connectivity) 
--    - devices need atleast 10 samples each
--    - peer groups need atleast 5 devices
--    - underperforming case is median download < 70% of the peer median OR median latency
--    - > 1.5x the peer median (took AI help t define these filters but can change this)
WITH device_stats AS (
    SELECT d.device_id, d.provider_name, d.technology, d.advertised_download_mbps,
           t.median_download_mbps, c.median_latency_ms
    FROM iceberg.solution.devices d
    JOIN (SELECT device_id, approx_percentile(download_mbps, 0.5) AS median_download_mbps, count(*) AS tests
          FROM (SELECT device_id, download_mbps
                FROM iceberg.solution.throughput_events
                WHERE event_timestamp >= current_timestamp - INTERVAL '1' DAY)
          GROUP BY device_id) t ON t.device_id = d.device_id
    JOIN (SELECT device_id, approx_percentile(latency_ms, 0.5) AS median_latency_ms, count(*) AS samples
          FROM (SELECT device_id, latency_ms
                FROM iceberg.solution.connectivity_events
                WHERE event_timestamp >= current_timestamp - INTERVAL '1' DAY)
          GROUP BY device_id) c ON c.device_id = d.device_id
    WHERE t.tests >= 10 AND c.samples >= 10
),
peers AS (
    SELECT technology, advertised_download_mbps,
           count(*)                                     AS peer_devices,
           approx_percentile(median_download_mbps, 0.5) AS peer_median_download_mbps,
           approx_percentile(median_latency_ms, 0.5)    AS peer_median_latency_ms
    FROM device_stats
    GROUP BY technology, advertised_download_mbps
)
SELECT s.device_id,
       s.provider_name,
       s.technology,
       s.advertised_download_mbps,
       p.peer_devices,
       round(s.median_download_mbps, 1)                               AS median_download_mbps,
       round(p.peer_median_download_mbps, 1)                          AS peer_median_download_mbps,
       round(s.median_download_mbps / p.peer_median_download_mbps, 3) AS download_vs_peer_ratio,
       round(s.median_latency_ms, 1)                                  AS median_latency_ms,
       round(p.peer_median_latency_ms, 1)                             AS peer_median_latency_ms,
       round(s.median_latency_ms / p.peer_median_latency_ms, 3)       AS latency_vs_peer_ratio
FROM device_stats s
JOIN peers p ON p.technology = s.technology AND p.advertised_download_mbps = s.advertised_download_mbps
WHERE p.peer_devices >= 5
  AND (s.median_download_mbps < 0.7 * p.peer_median_download_mbps
       OR s.median_latency_ms > 1.5 * p.peer_median_latency_ms)
ORDER BY download_vs_peer_ratio, device_id
