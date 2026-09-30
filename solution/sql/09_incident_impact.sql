-- Report 9: Shared-infrastructure incidents and downstream impact.
-- Grain: one row per incident (last day), on the infrastructure device that raised it.
-- Downstream devices: every access device whose upstream chain (device_dependencies:
--   access -> aggregation -> backhaul -> IXP, at most 3 hops) passes through that device.
-- devices_with_state_change: impacted devices that actually reported a state transition
--   between the incident start and 5 minutes after recovery (confirms real impact).
WITH incidents AS (
    SELECT incident_id, device_id AS source_device_id,
           min(event_timestamp)                                   AS started_at,
           max(IF(event_kind = 'recovered', event_timestamp))     AS recovered_at,
           bool_or(event_kind = 'outage_started')                 AS had_outage
    FROM (SELECT DISTINCT event_id, incident_id, device_id, event_kind, event_timestamp
          FROM iceberg.solution.infrastructure_events
          WHERE event_timestamp >= current_timestamp - INTERVAL '1' DAY)
    GROUP BY incident_id, device_id
),
paths AS (
    SELECT a.downstream_device_id AS access_device_id,
           a.upstream_device_id AS hop1, b.upstream_device_id AS hop2, c.upstream_device_id AS hop3
    FROM iceberg.solution.device_dependencies a
    LEFT JOIN iceberg.solution.device_dependencies b ON b.downstream_device_id = a.upstream_device_id
    LEFT JOIN iceberg.solution.device_dependencies c ON c.downstream_device_id = b.upstream_device_id
    WHERE a.dependency_type = 'access_aggregation'
),
impacted AS (
    SELECT i.incident_id, p.access_device_id, s.site_id, s.critical_infrastructure
    FROM incidents i
    JOIN paths p ON i.source_device_id IN (p.hop1, p.hop2, p.hop3)
    JOIN iceberg.solution.devices d ON d.device_id = p.access_device_id
    JOIN iceberg.solution.sites s ON s.site_id = d.site_id
),
state_changes AS (
    SELECT im.incident_id, count(DISTINCT st.device_id) AS devices_with_state_change
    FROM impacted im
    JOIN incidents i ON i.incident_id = im.incident_id
    JOIN iceberg.solution.connectivity_state_events st
      ON st.device_id = im.access_device_id
     AND st.event_timestamp BETWEEN i.started_at
                                AND coalesce(i.recovered_at, i.started_at) + INTERVAL '5' MINUTE
    GROUP BY im.incident_id
)
SELECT i.incident_id,
       i.source_device_id,
       src.infrastructure_role                                               AS source_role,
       i.started_at,
       i.recovered_at,
       round(to_milliseconds(i.recovered_at - i.started_at) / 60000.0, 1)   AS duration_minutes,
       i.had_outage,
       count(DISTINCT im.access_device_id)                                   AS impacted_access_devices,
       count(DISTINCT IF(im.critical_infrastructure, im.site_id))            AS impacted_critical_sites,
       coalesce(max(sc.devices_with_state_change), 0)                        AS devices_with_state_change
FROM incidents i
JOIN iceberg.solution.devices src ON src.device_id = i.source_device_id
LEFT JOIN impacted im     ON im.incident_id = i.incident_id
LEFT JOIN state_changes sc ON sc.incident_id = i.incident_id
GROUP BY i.incident_id, i.source_device_id, src.infrastructure_role, i.started_at, i.recovered_at, i.had_outage
ORDER BY i.started_at, i.incident_id
