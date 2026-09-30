-- Report 7: Geographic performance patterns with Census context.
-- Duplicates are NOT removed here: a resent event is an identical copy (~0.25% of rows), so
--   min/max are unchanged and medians/percentiles move far less than approx_percentile's
--   own error. De-duplicating (SELECT DISTINCT event_id) would hold millions of ids in
--   memory. Sample counts therefore include source duplicates.
-- Grain: one row per (region, inside_census_urban_area) for access devices; last day.
-- Location: devices -> sites (region, urban flag) -> census_block_groups (density).
-- median_block_group_density = median population density of the devices' block groups,
--   to show whether "rural" cohorts really are sparse.
WITH access AS (
    SELECT d.device_id, d.advertised_download_mbps, s.region, s.inside_census_urban_area,
           c.population_density_per_sq_mile
    FROM iceberg.solution.devices d
    JOIN iceberg.solution.sites s ON s.site_id = d.site_id
    JOIN iceberg.solution.census_block_groups c ON c.block_group_geoid = s.block_group_geoid
    WHERE d.infrastructure_role = 'subscriber_edge'
),
geo AS (
    SELECT region, inside_census_urban_area,
           count(*)                                                     AS devices,
           round(approx_percentile(population_density_per_sq_mile, 0.5), 1) AS median_block_group_density
    FROM access
    GROUP BY region, inside_census_urban_area
),
t AS (
    SELECT a.region, a.inside_census_urban_area,
           round(approx_percentile(100.0 * e.download_mbps / a.advertised_download_mbps, 0.5), 1) AS download_pct_of_advertised_median
    FROM (SELECT device_id, download_mbps
          FROM iceberg.solution.throughput_events
          WHERE event_timestamp >= current_timestamp - INTERVAL '1' DAY) e
    JOIN access a ON a.device_id = e.device_id
    GROUP BY a.region, a.inside_census_urban_area
),
c AS (
    SELECT a.region, a.inside_census_urban_area,
           round(approx_percentile(e.latency_ms, 0.5), 1)  AS latency_ms_median,
           round(approx_percentile(e.latency_ms, 0.95), 1) AS latency_ms_p95
    FROM (SELECT device_id, latency_ms
          FROM iceberg.solution.connectivity_events
          WHERE event_timestamp >= current_timestamp - INTERVAL '1' DAY) e
    JOIN access a ON a.device_id = e.device_id
    GROUP BY a.region, a.inside_census_urban_area
)
SELECT *
FROM geo
LEFT JOIN t USING (region, inside_census_urban_area)
LEFT JOIN c USING (region, inside_census_urban_area)
ORDER BY region, inside_census_urban_area
