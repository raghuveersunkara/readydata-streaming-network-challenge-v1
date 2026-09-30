-- report 6: provider, technology, or service-cohort comparison.
-- dupes retianed for same reasons as report 3 and 4.
--grain: one row per (provider_name, technology) of access devices in the last day.
-- assumptions: 
--   - download is compared as % of each device's advertised plan, so cohorts with
--   different plan mixes are comparable
WITH throughput AS (
    SELECT d.provider_name, d.technology, d.device_id,
           100.0 * e.download_mbps / d.advertised_download_mbps AS pct_of_advertised
    FROM iceberg.solution.throughput_events e
    JOIN iceberg.solution.devices d ON d.device_id = e.device_id
    WHERE e.event_timestamp >= current_timestamp - INTERVAL '1' DAY
),
connectivity AS (
    SELECT d.provider_name, d.technology, e.latency_ms, e.packet_loss_pct
    FROM iceberg.solution.connectivity_events e
    JOIN iceberg.solution.devices d ON d.device_id = e.device_id
    WHERE e.event_timestamp >= current_timestamp - INTERVAL '1' DAY
),
t AS (
    SELECT provider_name, technology,
           count(DISTINCT device_id)                          AS devices,
           count(*)                                           AS throughput_tests,
           round(approx_percentile(pct_of_advertised, 0.5), 1)  AS download_pct_of_advertised_median,
           round(approx_percentile(pct_of_advertised, 0.05), 1) AS download_pct_of_advertised_bad
    FROM throughput
    GROUP BY provider_name, technology
),
c AS (
    SELECT provider_name, technology,
           count(*)                                              AS connectivity_samples,
           round(approx_percentile(latency_ms, 0.5), 1)          AS latency_ms_median,
           round(approx_percentile(latency_ms, 0.95), 1)         AS latency_ms_bad,
           round(approx_percentile(packet_loss_pct, 0.95), 3)    AS packet_loss_pct_bad
    FROM connectivity
    GROUP BY provider_name, technology
)
SELECT *
FROM t
FULL OUTER JOIN c USING (provider_name, technology)
ORDER BY provider_name, technology
