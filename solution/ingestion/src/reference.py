"""Load, validate and publish the four required reference tables.

Flow: read all files -> validate everything -> write only if there are no
errors. Each table is replaced atomically with an Iceberg overwrite, and a
table is skipped when its current snapshot already holds the same source file
checksum and loader version, so re-running is a no-op.
"""
import csv
import hashlib
import json
import logging
import os
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List

import pyarrow as pa
import pyarrow.csv as pacsv

from .tables import ensure_table

logger = logging.getLogger(__name__)

# Bump when the table schemas or transformations change so unchanged source
# files are still reloaded into the new shape.
LOADER_VERSION = "1"

LINEAGE = [
    ("_source_file", pa.string()),
    ("_source_sha256", pa.string()),
    ("_loaded_at", pa.timestamp("us", tz="UTC")),
]


@dataclass(frozen=True)
class ReferenceSpec:
    table: str
    path: str               # relative to the reference-data root
    manifest: str           # manifest.json that lists this file's checksum
    schema: pa.Schema       # CSV columns in file order (lineage appended on write)
    key: List[str]          # primary key
    required: List[str]     # columns that must be non-empty


SPECS = [
    ReferenceSpec(
        table="census_block_groups",
        path="census/v1/census_block_groups.csv",
        manifest="census/v1/manifest.json",
        schema=pa.schema([
            ("block_group_geoid", pa.string()),  # string: leading zeros are significant
            ("population", pa.int64()),
            ("land_area_sq_miles", pa.float64()),
            ("population_density_per_sq_mile", pa.float64()),
            ("internal_latitude", pa.float64()),
            ("internal_longitude", pa.float64()),
        ]),
        key=["block_group_geoid"],
        required=["block_group_geoid", "population", "land_area_sq_miles",
                  "internal_latitude", "internal_longitude"],
    ),
    ReferenceSpec(
        table="sites",
        path="inventory/v1/sites.csv",
        manifest="inventory/v1/manifest.json",
        schema=pa.schema([
            ("site_id", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("region", pa.string()),
            ("block_group_geoid", pa.string()),
            ("inside_census_urban_area", pa.bool_()),
            ("urban_area_name", pa.string()),
            ("distance_to_nearest_urban_area_km", pa.float64()),
            ("nearest_census_place", pa.string()),
            ("distance_to_nearest_place_km", pa.float64()),
            ("site_type", pa.string()),
            ("critical_infrastructure", pa.bool_()),
        ]),
        key=["site_id"],
        required=["site_id", "latitude", "longitude", "region", "block_group_geoid",
                  "inside_census_urban_area", "site_type", "critical_infrastructure"],
    ),
    ReferenceSpec(
        table="devices",
        path="inventory/v1/devices.csv",
        manifest="inventory/v1/manifest.json",
        schema=pa.schema([
            ("device_id", pa.string()),
            ("site_id", pa.string()),
            ("provider_id", pa.string()),
            ("provider_name", pa.string()),
            ("technology", pa.string()),
            ("device_class", pa.string()),
            ("device_model", pa.string()),
            ("infrastructure_role", pa.string()),
            ("advertised_download_mbps", pa.float64()),
            ("advertised_upload_mbps", pa.float64()),
            ("deployment_date", pa.date32()),
        ]),
        key=["device_id"],
        required=["device_id", "site_id", "technology", "device_class",
                  "infrastructure_role", "deployment_date"],
    ),
    ReferenceSpec(
        table="device_dependencies",
        path="inventory/v1/device_dependencies.csv",
        manifest="inventory/v1/manifest.json",
        schema=pa.schema([
            ("downstream_device_id", pa.string()),
            ("upstream_device_id", pa.string()),
            ("dependency_type", pa.string()),
        ]),
        # The inventory contract gives every non-IXP device exactly one upstream.
        key=["downstream_device_id"],
        required=["downstream_device_id", "upstream_device_id", "dependency_type"],
    ),
]

# Loose Arkansas bounding box; catches swapped or zeroed coordinates.
LAT_RANGE = (32.9, 36.6)
LON_RANGE = (-94.7, -89.5)
GEOID_RE = re.compile(r"^05\d{10}$")  # 12-digit block group GEOID, Arkansas FIPS 05
ACCESS_ROLE = "subscriber_edge"
IXP_ROLE = "ixp"
NETWORK_SITE_TYPE = "network_facility"
MAX_EXAMPLES = 5


@dataclass
class ReferenceBundle:
    tables: Dict[str, pa.Table]
    headers: Dict[str, List[str]]
    checksums: Dict[str, str]
    manifests: Dict[str, dict]
    errors: List[str] = field(default_factory=list)


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_reference(root: str) -> ReferenceBundle:
    bundle = ReferenceBundle(tables={}, headers={}, checksums={}, manifests={})
    for spec in SPECS:
        path = os.path.join(root, spec.path)
        if spec.manifest not in bundle.manifests:
            with open(os.path.join(root, spec.manifest)) as f:
                bundle.manifests[spec.manifest] = json.load(f)
        with open(path, newline="", encoding="utf-8") as f:
            bundle.headers[spec.table] = next(csv.reader(f))
        bundle.checksums[spec.table] = _sha256(path)
        try:
            bundle.tables[spec.table] = pacsv.read_csv(
                path,
                convert_options=pacsv.ConvertOptions(
                    column_types={f.name: f.type for f in spec.schema},
                    include_columns=spec.schema.names,
                    strings_can_be_null=True,  # empty provider/speed fields become NULL
                    true_values=["true"],
                    false_values=["false"],
                ),
            ).select(spec.schema.names).cast(spec.schema)
        except (pa.ArrowInvalid, pa.ArrowTypeError, KeyError) as e:
            bundle.errors.append(f"{spec.table}: cannot parse {spec.path}: {e}")
    return bundle


def _examples(values) -> str:
    values = sorted({str(v) for v in values})
    more = f" (+{len(values) - MAX_EXAMPLES} more)" if len(values) > MAX_EXAMPLES else ""
    return ", ".join(values[:MAX_EXAMPLES]) + more


def validate(bundle: ReferenceBundle) -> List[str]:
    """Return every problem found; an empty list means the bundle is loadable."""
    errors = list(bundle.errors)
    specs = {s.table: s for s in SPECS}

    # File-level checks: header contract and checksum against the manifest.
    for spec in SPECS:
        header = bundle.headers.get(spec.table)
        if header != spec.schema.names:
            errors.append(f"{spec.table}: header {header} does not match contract {spec.schema.names}")
        expected_sha = (bundle.manifests[spec.manifest].get("outputs", {})
                        .get(os.path.basename(spec.path), {}).get("sha256"))
        if expected_sha != bundle.checksums.get(spec.table):
            errors.append(f"{spec.table}: sha256 {bundle.checksums.get(spec.table)} != manifest {expected_sha}")

    if len(bundle.tables) != len(SPECS):
        return errors  # a file failed to parse; row-level checks would be noise

    rows = {name: t.to_pylist() for name, t in bundle.tables.items()}

    # Required values and primary keys.
    for name, spec in specs.items():
        for col in spec.required:
            missing = [i for i, r in enumerate(rows[name]) if r[col] is None or r[col] == ""]
            if missing:
                errors.append(f"{name}: {len(missing)} rows with empty {col} (row numbers: {_examples(missing)})")
        key_counts = Counter(tuple(r[c] for c in spec.key) for r in rows[name])
        dupes = {k for k, n in key_counts.items() if n > 1}
        if dupes:
            errors.append(f"{name}: duplicate {spec.key}: {_examples(dupes)}")

    census, sites, devices, deps = (rows["census_block_groups"], rows["sites"],
                                    rows["devices"], rows["device_dependencies"])

    # Row counts against the manifests.
    inv_counts = bundle.manifests["inventory/v1/manifest.json"].get("counts", {})
    census_counts = bundle.manifests["census/v1/manifest.json"].get("counts", {})
    expected_counts = {
        "census_block_groups": census_counts.get("block_groups"),
        "sites": inv_counts.get("sites"),
        "devices": inv_counts.get("devices"),
        "device_dependencies": inv_counts.get("dependencies"),
        "access devices": inv_counts.get("access_devices"),
        "infrastructure devices": inv_counts.get("infrastructure_devices"),
        "service sites": inv_counts.get("service_sites"),
        "network sites": inv_counts.get("network_sites"),
    }
    actual_counts = {
        "census_block_groups": len(census),
        "sites": len(sites),
        "devices": len(devices),
        "device_dependencies": len(deps),
        "access devices": sum(d["infrastructure_role"] == ACCESS_ROLE for d in devices),
        "infrastructure devices": sum(d["infrastructure_role"] != ACCESS_ROLE for d in devices),
        "service sites": sum(s["site_type"] != NETWORK_SITE_TYPE for s in sites),
        "network sites": sum(s["site_type"] == NETWORK_SITE_TYPE for s in sites),
    }
    for label, expected in expected_counts.items():
        if expected != actual_counts[label]:
            errors.append(f"count mismatch for {label}: file has {actual_counts[label]}, manifest says {expected}")

    # Identifier formats and value domains.
    bad_geoids = [r["block_group_geoid"] for r in census + sites
                  if r["block_group_geoid"] and not GEOID_RE.match(r["block_group_geoid"])]
    if bad_geoids:
        errors.append(f"malformed block_group_geoid: {_examples(bad_geoids)}")
    bad_coords = [s["site_id"] for s in sites
                  if s["latitude"] is not None and s["longitude"] is not None
                  and not (LAT_RANGE[0] <= s["latitude"] <= LAT_RANGE[1]
                           and LON_RANGE[0] <= s["longitude"] <= LON_RANGE[1])]
    if bad_coords:
        errors.append(f"sites outside Arkansas bounds: {_examples(bad_coords)}")
    negatives = [f"{name}.{col}" for name, cols in {
        "census_block_groups": ["population", "land_area_sq_miles", "population_density_per_sq_mile"],
        "sites": ["distance_to_nearest_urban_area_km", "distance_to_nearest_place_km"],
        "devices": ["advertised_download_mbps", "advertised_upload_mbps"],
    }.items() for col in cols if any((r[col] or 0) < 0 for r in rows[name])]
    if negatives:
        errors.append(f"negative values in: {', '.join(negatives)}")

    # Relationships.
    geoids = {c["block_group_geoid"] for c in census}
    site_ids = {s["site_id"] for s in sites}
    device_by_id = {d["device_id"]: d for d in devices}
    orphans = {
        "sites.block_group_geoid -> census_block_groups": [s["site_id"] for s in sites if s["block_group_geoid"] not in geoids],
        "devices.site_id -> sites": [d["device_id"] for d in devices if d["site_id"] not in site_ids],
        "device_dependencies.downstream_device_id -> devices": [e["downstream_device_id"] for e in deps if e["downstream_device_id"] not in device_by_id],
        "device_dependencies.upstream_device_id -> devices": [e["upstream_device_id"] for e in deps if e["upstream_device_id"] not in device_by_id],
    }
    for relation, bad in orphans.items():
        if bad:
            errors.append(f"orphaned {relation}: {_examples(bad)}")

    # Inventory rules from reference-data/inventory/v1/README.md.
    access_without_speed = [d["device_id"] for d in devices if d["infrastructure_role"] == ACCESS_ROLE
                            and (d["advertised_download_mbps"] is None or d["provider_id"] is None)]
    infra_with_speed = [d["device_id"] for d in devices if d["infrastructure_role"] != ACCESS_ROLE
                        and d["advertised_download_mbps"] is not None]
    if access_without_speed:
        errors.append(f"access devices missing provider or advertised speed: {_examples(access_without_speed)}")
    if infra_with_speed:
        errors.append(f"infrastructure devices with advertised speed: {_examples(infra_with_speed)}")
    devices_per_site = Counter(d["site_id"] for d in devices)
    bad_service_sites = [s["site_id"] for s in sites if s["site_type"] != NETWORK_SITE_TYPE
                         and devices_per_site.get(s["site_id"], 0) != 1]
    if bad_service_sites:
        errors.append(f"service sites without exactly one device: {_examples(bad_service_sites)}")

    # Topology: IXPs are roots; every other device has one upstream and its
    # chain reaches an IXP without cycles.
    upstream = {e["downstream_device_id"]: e["upstream_device_id"] for e in deps}
    ixps = {d_id for d_id, d in device_by_id.items() if d["infrastructure_role"] == IXP_ROLE}
    if ixps & upstream.keys():
        errors.append(f"IXP devices must not have an upstream: {_examples(ixps & upstream.keys())}")
    no_upstream = [d_id for d_id in device_by_id if d_id not in ixps and d_id not in upstream]
    if no_upstream:
        errors.append(f"non-IXP devices without an upstream: {_examples(no_upstream)}")
    unrooted = []
    for d_id in device_by_id:
        seen, node = set(), d_id
        while node in upstream and node not in seen:
            seen.add(node)
            node = upstream[node]
        if node not in ixps:
            unrooted.append(d_id)
    if unrooted:
        errors.append(f"devices whose upstream chain has a cycle or does not reach an IXP: {_examples(unrooted)}")

    return errors


def write_reference(catalog, bundle: ReferenceBundle) -> Dict[str, str]:
    """Overwrite each table unless it already holds this exact source. Returns an action per table."""
    loaded_at = int(time.time() * 1_000_000)  # epoch microseconds
    actions = {}
    for spec in SPECS:
        sha = bundle.checksums[spec.table]
        full_schema = pa.schema(list(spec.schema) + LINEAGE)
        table = ensure_table(catalog, spec.table, full_schema)

        snapshot = table.current_snapshot()
        current = snapshot.summary.additional_properties if snapshot and snapshot.summary else {}
        if current.get("reference.source-sha256") == sha and current.get("reference.loader-version") == LOADER_VERSION:
            actions[spec.table] = "unchanged"
            continue

        data = bundle.tables[spec.table]
        n = data.num_rows
        data = (data
                .append_column("_source_file", pa.array([spec.path] * n, pa.string()))
                .append_column("_source_sha256", pa.array([sha] * n, pa.string()))
                .append_column("_loaded_at", pa.array([loaded_at] * n, pa.timestamp("us", tz="UTC"))))
        table.overwrite(data, snapshot_properties={
            "reference.source-file": spec.path,
            "reference.source-sha256": sha,
            "reference.loader-version": LOADER_VERSION,
        })
        actions[spec.table] = f"loaded {n} rows"
    return actions
