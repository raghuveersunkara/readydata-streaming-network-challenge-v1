-- Report 1: Accepted event totals by event family and inventory dimensions.
-- grain: event_type, provider_name, technology, region, infrastructure_role
-- assumptions: useful inventory dimensions are provider_name, technology, region, infrastructure_role
-- window limited to 6 hours before the newest connectivity measurement, as we have a cap on trino memory heap
-- accepted events are those that have passed contract validation and are in the five event tables
-- event_total counts each event_id only once (de-duplicated in unique_events, before the joins), so source duplicates (~0.25%) are not counted multiple times
with bounds as (
    select max(event_timestamp) - interval '6' hour as since
    from solution.connectivity_events
),
combined_events as (
    select  event_type, event_id, event_timestamp, device_id
    from solution.connectivity_events
    where event_timestamp >= (select since from bounds)
    union all
    select event_type, event_id, event_timestamp, device_id
    from solution.connectivity_state_events
    where event_timestamp >= (select since from bounds)
    union all
    select event_type, event_id, event_timestamp, device_id
    from solution.device_health_events
    where event_timestamp >= (select since from bounds)
    union all
    select event_type, event_id, event_timestamp, device_id
    from solution.infrastructure_events
    where event_timestamp >= (select since from bounds)
    union all
    select event_type, event_id, event_timestamp, device_id
    from solution.throughput_events
    where event_timestamp >= (select since from bounds)
),
unique_events as (
    SELECT distinct event_type, event_id, device_id
    from combined_events
)
SELECT event_type, d.provider_name, d.technology, s.region, d.infrastructure_role, count(*) as event_count
FROM unique_events ce
left join solution.devices d on ce.device_id = d.device_id
left join solution.sites s on d.site_id = s.site_id
GROUP by event_type, d.provider_name, d.technology, s.region, d.infrastructure_role
ORDER by event_type, provider_name, technology, region, infrastructure_role
