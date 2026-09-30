import json
import os

import pytest

from lakehouse_ops.quality import evaluate, event_checks, manifest_counts, reference_checks, summarize

COUNTS = {"sites": 623, "devices": 654, "device_dependencies": 650, "census_block_groups": 2294}


def test_fail_and_warn_are_distinguished():
    checks = {c.name: c for c in reference_checks("iceberg.solution", COUNTS) + event_checks("iceberg.solution", 6)}
    assert evaluate(checks["devices_orphan_site"], 3)["status"] == "fail"
    assert evaluate(checks["upstream_duplicate_pct"], 0.25)["status"] == "ok"
    assert evaluate(checks["upstream_duplicate_pct"], 2.0)["status"] == "warn"
    assert evaluate(checks["devices_orphan_site"], 0)["status"] == "ok"


def test_query_errors_count_as_failures():
    check = event_checks("iceberg.solution", 6)[0]
    finding = evaluate(check, None, error="table not found")
    assert finding["status"] == "fail" and finding["error"] == "table not found"


def test_reference_counts_come_from_the_manifest():
    names = {c.name: c for c in reference_checks("iceberg.solution", COUNTS)}
    assert "abs(count(*) - 654)" in names["devices_row_count_vs_manifest"].sql


def test_event_checks_use_the_requested_window_and_all_five_tables():
    sql = {c.name: c.sql for c in event_checks("iceberg.solution", 3)}["events_from_unknown_devices"]
    assert sql.count("INTERVAL '3' HOUR") == 5
    for table in ["connectivity_events", "throughput_events", "device_health_events",
                  "connectivity_state_events", "infrastructure_events"]:
        assert f"iceberg.solution.{table}" in sql


def test_check_names_are_unique():
    checks = reference_checks("iceberg.solution", COUNTS) + event_checks("iceberg.solution", 6)
    assert len({c.name for c in checks}) == len(checks)


def _finding(name, status):
    return {"name": name, "status": status}


def test_warnings_alone_do_not_fail_the_run():
    result = summarize([_finding("a", "ok")], [_finding("b", "warn"), _finding("c", "warn")])
    assert result["failed"] is False
    assert result["summary"] == {"fail": 0, "warn": 2, "ok": 1}


def test_any_failed_check_fails_the_run_and_is_listed_first():
    result = summarize([_finding("z_ok", "ok"), _finding("orphans", "fail")], [_finding("dups", "warn")])
    assert result["failed"] is True
    assert [f["status"] for f in result["findings"]] == ["fail", "warn", "ok"]


def test_a_check_group_that_did_not_run_fails_the_run():
    # The events task crashed, so there are no event findings at all: that is not a pass.
    result = summarize([_finding("a", "ok")], None)
    assert result["failed"] is True and result["missing_groups"] == ["event"]


def test_manifest_keys_map_to_table_names():
    inventory = {"sites": 623, "devices": 654, "dependencies": 650, "access_devices": 600}
    census = {"block_groups": 2294, "population": 3011524}
    assert manifest_counts(inventory, census) == COUNTS


REFERENCE_ROOT = os.getenv("READYDATA_REFERENCE_ROOT", "/opt/airflow/reference-data")


@pytest.mark.skipif(not os.path.isdir(os.path.join(REFERENCE_ROOT, "inventory")),
                    reason="reference-data not mounted")
def test_supplied_manifests_have_the_expected_count_keys():
    def load(path):
        with open(os.path.join(REFERENCE_ROOT, path, "v1", "manifest.json")) as f:
            return json.load(f)["counts"]
    assert manifest_counts(load("inventory"), load("census")) == COUNTS
