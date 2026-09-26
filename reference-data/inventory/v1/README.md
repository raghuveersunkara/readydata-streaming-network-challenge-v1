# Synthetic Arkansas network inventory V1

These files describe a deterministic fictional network placed within prepared
2020 Arkansas Census geography. No site is a real address or facility, provider
names are fictional, and no values represent observed connectivity or customer
records.

Candidate baseline inputs:

- `sites.csv` — one row per service or network-facility site. `site_id` is the
  primary key; `block_group_geoid` joins to the supplied Census block-group CSV.
- `devices.csv` — one row per access or infrastructure device. `device_id` is
  the primary key and `site_id` references `sites.csv`.
- `device_dependencies.csv` — directed immediate-upstream relationships keyed
  by device IDs. Access devices connect through aggregation and backhaul devices
  to the shared IXP layer.

`manifest.json` records the inventory and Census versions, exact row counts and
categorical distributions, and SHA-256 checksums for all three CSVs.

## Counts and relationships

| Contract | Rows | Relationship |
| --- | ---: | --- |
| Sites | 623 | 600 service sites and 23 shared network-facility sites |
| Devices | 654 | 600 access devices and 54 infrastructure devices |
| Dependencies | 650 | Every non-IXP device has one immediate upstream device |

Service sites contain one access device. Network facilities may host multiple
routers, so `site_id` and `device_id` represent distinct entity keys.
Dependency edges point from `downstream_device_id` to
`upstream_device_id`.

## File contracts

`sites.csv` fields:

```text
site_id, latitude, longitude, region, block_group_geoid,
inside_census_urban_area, urban_area_name,
distance_to_nearest_urban_area_km, nearest_census_place,
distance_to_nearest_place_km, site_type, critical_infrastructure
```

`devices.csv` fields:

```text
device_id, site_id, provider_id, provider_name, technology, device_class,
device_model, infrastructure_role, advertised_download_mbps,
advertised_upload_mbps, deployment_date
```

Advertised speeds are empty for infrastructure devices. Shared IXP devices also
have empty provider fields because they are not owned by a fictional access
provider.

`device_dependencies.csv` fields:

```text
downstream_device_id, upstream_device_id, dependency_type
```

All CSVs are UTF-8, header-bearing, LF-terminated, and sorted by their stable
identifier. Booleans are lowercase `true` or `false`, dates use ISO `YYYY-MM-DD`,
and coordinates use WGS 84 decimal degrees.
