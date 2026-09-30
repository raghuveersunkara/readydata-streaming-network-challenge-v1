import hashlib
import json
import os
from unittest.mock import MagicMock

import pytest

from src.reference import SPECS, read_reference, validate, write_reference

SITES_HEADER = ("site_id,latitude,longitude,region,block_group_geoid,inside_census_urban_area,"
                "urban_area_name,distance_to_nearest_urban_area_km,nearest_census_place,"
                "distance_to_nearest_place_km,site_type,critical_infrastructure")
DEVICES_HEADER = ("device_id,site_id,provider_id,provider_name,technology,device_class,device_model,"
                  "infrastructure_role,advertised_download_mbps,advertised_upload_mbps,deployment_date")


def _valid_files():
    """Smallest consistent inventory: IXP <- aggregation router <- one access device."""
    return {
        "census/v1/census_block_groups.csv": [
            "block_group_geoid,population,land_area_sq_miles,population_density_per_sq_mile,internal_latitude,internal_longitude",
            "051190024033,1200,1.5,800.0,34.72,-92.36",
        ],
        "inventory/v1/sites.csv": [
            SITES_HEADER,
            'network-site-01,34.72,-92.36,central,051190024033,true,"Little Rock, AR",0.0,Little Rock city,0.6,network_facility,false',
            'site-0001,34.73,-92.37,central,051190024033,true,"Little Rock, AR",0.0,Little Rock city,0.9,residential,false',
        ],
        "inventory/v1/devices.csv": [
            DEVICES_HEADER,
            "ixp-01,network-site-01,,,fiber,ixp_router,RTR-CORE-9000,ixp,,,2019-01-01",
            "agg-01,network-site-01,prov,Prov,fiber,aggregation_router,RTR-EDGE-2000,aggregation,,,2020-01-01",
            "device-0001,site-0001,prov,Prov,fiber,ont,ONT-1,subscriber_edge,500,100,2021-01-01",
        ],
        "inventory/v1/device_dependencies.csv": [
            "downstream_device_id,upstream_device_id,dependency_type",
            "agg-01,ixp-01,backhaul_ixp",
            "device-0001,agg-01,access_aggregation",
        ],
    }


def _write_bundle(root, files, counts_override=None):
    """Write CSVs plus manifests whose checksums and counts match them."""
    outputs = {"census": {}, "inventory": {}}
    for rel_path, lines in files.items():
        path = os.path.join(root, rel_path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = ("\n".join(lines) + "\n").encode()
        with open(path, "wb") as f:
            f.write(data)
        outputs[rel_path.split("/")[0]][os.path.basename(rel_path)] = {"sha256": hashlib.sha256(data).hexdigest()}

    inventory_counts = {"sites": 2, "devices": 3, "dependencies": 2, "access_devices": 1,
                        "infrastructure_devices": 2, "service_sites": 1, "network_sites": 1}
    inventory_counts.update(counts_override or {})
    manifests = {
        "census/v1/manifest.json": {"counts": {"block_groups": 1}, "outputs": outputs["census"]},
        "inventory/v1/manifest.json": {"counts": inventory_counts, "outputs": outputs["inventory"]},
    }
    for rel_path, manifest in manifests.items():
        with open(os.path.join(root, rel_path), "w") as f:
            json.dump(manifest, f)
    return str(root)


def _errors_for(tmp_path, mutate=None, counts_override=None):
    files = _valid_files()
    if mutate:
        mutate(files)
    return validate(read_reference(_write_bundle(tmp_path, files, counts_override)))


def test_valid_bundle_has_no_errors(tmp_path):
    assert _errors_for(tmp_path) == []


def test_types_are_parsed_and_geoid_keeps_leading_zero(tmp_path):
    bundle = read_reference(_write_bundle(tmp_path, _valid_files()))
    sites = bundle.tables["sites"].to_pylist()
    assert sites[0]["block_group_geoid"] == "051190024033"
    assert sites[0]["inside_census_urban_area"] is True
    ixp = bundle.tables["devices"].to_pylist()[0]
    assert ixp["provider_id"] is None and ixp["advertised_download_mbps"] is None


def test_duplicate_primary_key(tmp_path):
    def dup_site(files):
        files["inventory/v1/sites.csv"].append(files["inventory/v1/sites.csv"][2])
    errors = _errors_for(tmp_path, dup_site, {"sites": 3, "service_sites": 2})
    assert any("sites: duplicate ['site_id']" in e for e in errors)


def test_orphaned_foreign_keys(tmp_path):
    def orphan(files):
        files["inventory/v1/devices.csv"][3] = files["inventory/v1/devices.csv"][3].replace("site-0001", "site-9999")
        files["inventory/v1/sites.csv"][2] = files["inventory/v1/sites.csv"][2].replace("051190024033", "051190099999")
    errors = _errors_for(tmp_path, orphan)
    assert any("orphaned devices.site_id -> sites: device-0001" in e for e in errors)
    assert any("orphaned sites.block_group_geoid -> census_block_groups: site-0001" in e for e in errors)


def test_dependency_cycle_is_detected(tmp_path):
    def cycle(files):
        files["inventory/v1/device_dependencies.csv"][1] = "agg-01,device-0001,backhaul_ixp"
    errors = _errors_for(tmp_path, cycle)
    assert any("cycle or does not reach an IXP" in e for e in errors)


def test_count_mismatch_against_manifest(tmp_path):
    errors = _errors_for(tmp_path, counts_override={"devices": 4})
    assert any("count mismatch for devices: file has 3, manifest says 4" in e for e in errors)


def test_checksum_mismatch(tmp_path):
    root = _write_bundle(tmp_path, _valid_files())
    with open(os.path.join(root, "census/v1/census_block_groups.csv"), "a") as f:
        f.write("050014801001,10,1.0,10.0,34.3,-91.2\n")
    errors = validate(read_reference(root))
    assert any("census_block_groups: sha256" in e for e in errors)


def test_header_drift(tmp_path):
    def rename(files):
        files["inventory/v1/device_dependencies.csv"][0] = "child,parent,dependency_type"
    errors = _errors_for(tmp_path, rename)
    assert any("device_dependencies" in e for e in errors)


def test_write_skips_tables_already_holding_same_source(tmp_path):
    bundle = read_reference(_write_bundle(tmp_path, _valid_files()))
    catalog = MagicMock()
    table = MagicMock()
    catalog.create_table_if_not_exists.return_value = table
    catalog.load_table.return_value = table

    table.current_snapshot.return_value = None
    actions = write_reference(catalog, bundle)
    assert all(a.startswith("loaded") for a in actions.values())
    overwrite_props = table.overwrite.call_args.kwargs["snapshot_properties"]

    table.overwrite.reset_mock()
    table.current_snapshot.return_value = MagicMock()
    table.current_snapshot.return_value.summary.additional_properties = overwrite_props
    # Every table now "holds" the last-written sha; only the table matching it is skipped.
    actions = write_reference(catalog, bundle)
    assert actions[SPECS[-1].table] == "unchanged"


REAL_ROOT = os.getenv("REFERENCE_DATA_DIR", "/app/reference-data")


@pytest.mark.skipif(not os.path.isdir(os.path.join(REAL_ROOT, "inventory")),
                    reason="Supplied reference data is not mounted")
def test_supplied_reference_data_is_valid():
    bundle = read_reference(REAL_ROOT)
    assert validate(bundle) == []
    assert {name: t.num_rows for name, t in bundle.tables.items()} == {
        "census_block_groups": 2294, "sites": 623, "devices": 654, "device_dependencies": 650,
    }


def _replace(path, index, old, new):
    def mutate(files):
        assert old in files[path][index], f"fixture changed: {old!r} not in {files[path][index]!r}"
        files[path][index] = files[path][index].replace(old, new)
    return mutate


def _append(path, line):
    return lambda files: files[path].append(line)


def _all(*mutations):
    def mutate(files):
        for m in mutations:
            m(files)
    return mutate


SITES, DEVICES, DEPS, CENSUS = ("inventory/v1/sites.csv", "inventory/v1/devices.csv",
                                "inventory/v1/device_dependencies.csv", "census/v1/census_block_groups.csv")

# (mutation, manifest count overrides, expected error substring). Each breaks one inventory rule.
RULE_VIOLATIONS = {
    "site outside Arkansas": (_replace(SITES, 2, "34.73,-92.37", "45.00,-92.37"), None,
                              "sites outside Arkansas bounds: site-0001"),
    "access device without advertised speed": (_replace(DEVICES, 3, ",500,100,", ",,100,"), None,
                                               "access devices missing provider or advertised speed: device-0001"),
    "infrastructure device with advertised speed": (_replace(DEVICES, 2, ",aggregation,,,", ",aggregation,100,10,"), None,
                                                    "infrastructure devices with advertised speed: agg-01"),
    "service site with no device": (
        _append(SITES, 'site-0002,34.74,-92.38,central,051190024033,true,"Little Rock, AR",0.0,Little Rock city,1.0,residential,false'),
        {"sites": 3, "service_sites": 2}, "service sites without exactly one device: site-0002"),
    "IXP with an upstream": (_append(DEPS, "ixp-01,agg-01,backhaul_ixp"), {"dependencies": 3},
                             "IXP devices must not have an upstream: ixp-01"),
    "device without an upstream": (lambda files: files[DEPS].pop(2), {"dependencies": 1},
                                   "non-IXP devices without an upstream: device-0001"),
    "empty required value": (_replace(SITES, 2, ",central,", ",,"), None, "sites: 1 rows with empty region"),
    "non-Arkansas GEOID": (_all(_replace(CENSUS, 1, "051190024033", "061190024033"),
                                _replace(SITES, 1, "051190024033", "061190024033"),
                                _replace(SITES, 2, "051190024033", "061190024033")),
                           None, "malformed block_group_geoid"),
    "negative population": (_replace(CENSUS, 1, ",1200,", ",-5,"), None,
                            "negative values in: census_block_groups.population"),
}


@pytest.mark.parametrize("case", sorted(RULE_VIOLATIONS))
def test_inventory_rule_violations_are_reported(tmp_path, case):
    mutate, counts, expected = RULE_VIOLATIONS[case]
    errors = _errors_for(tmp_path, mutate, counts)
    assert any(expected in e for e in errors), errors
