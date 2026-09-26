# Arkansas 2020 Census reference data

These prepared files are derived from pinned, official 2020 U.S. Census Bureau
sources. They contain public aggregate geography and population context only.
They contain no addresses, households, real network facilities, or connectivity
measurements.

Candidate baseline input:

- `census_block_groups.csv` — block-group identifier, total population, land
  area, population density, and Census representative coordinate.

Optional geographic enrichment:

- `arkansas_boundary.geojson` — simplified Arkansas state boundary.
- `arkansas_block_groups.geojson` — simplified block-group polygons keyed by
  `block_group_geoid`.
- `arkansas_urban_areas.geojson` — original 2020 Urban Areas clipped to Arkansas.
- `census_places.csv` — Arkansas Census Places and representative coordinates.

`manifest.json` records source URLs and checksums, transformations, output
checksums, and row/feature counts. The generated site inventory references
`block_group_geoid`, allowing a conventional join to the required block-group
CSV. Geometry-aware ingestion is optional for this challenge.

The prepared snapshot is ready to use and requires no download or GIS tooling
during normal startup.
