import pytest

from lakehouse_ops.maintenance import (
    MB, SOLUTION_TABLES, TableFileStats, decide, optimize_sql, validate_tables, verify_compaction,
)


def _stats(small_files=100, small_mb=10, spec_ids=(1,)):
    return TableFileStats("connectivity_events", data_files=small_files, small_files=small_files,
                          small_bytes=int(small_mb * MB), total_bytes=int(small_mb * MB),
                          records=1000, spec_ids=list(spec_ids))


def test_compacts_when_justified_and_bounded():
    plan = decide(_stats(small_files=100, small_mb=10), min_small_files=20, max_rewrite_mb=2048)
    assert plan.action == "compact"


def test_skips_when_too_few_small_files():
    plan = decide(_stats(small_files=5), min_small_files=20, max_rewrite_mb=2048)
    assert plan.action == "skip" and "min_small_files" in plan.reason


def test_skips_when_rewrite_exceeds_bound():
    plan = decide(_stats(small_files=100, small_mb=5000), min_small_files=20, max_rewrite_mb=2048)
    assert plan.action == "skip" and "max_rewrite_mb" in plan.reason


def test_mentions_legacy_partition_spec():
    plan = decide(_stats(spec_ids=(0, 1)), min_small_files=20, max_rewrite_mb=2048)
    assert "older partition spec" in plan.reason


def test_table_allowlist_blocks_unknown_and_injection():
    with pytest.raises(ValueError):
        validate_tables(["connectivity_events", 'x"; DROP TABLE sites; --'])
    with pytest.raises(ValueError):
        validate_tables([])
    assert validate_tables(["sites", "connectivity_events", "sites"]) == ["connectivity_events", "sites"]
    assert set(validate_tables(SOLUTION_TABLES)) == set(SOLUTION_TABLES)


def test_optimize_only_rewrites_files_below_threshold():
    sql = optimize_sql("iceberg", "solution", "throughput_events", 32)
    assert sql == "ALTER TABLE iceberg.solution.throughput_events EXECUTE optimize(file_size_threshold => '32MB')"


def test_verify_uses_replace_snapshot_when_still_current():
    assert verify_compaction((1, "replace", "128", "128", "2", "83"), 128, 128)["check"] == "replace-snapshot"
    assert verify_compaction((1, "replace", "128", "128", "2", "83"), 128, 128)["verified"]
    assert not verify_compaction((1, "replace", "127", "128", "2", "83"), 128, 127)["verified"]


def test_verify_falls_back_to_no_row_loss_after_concurrent_append():
    after_append = (2, "append", "500", None, "1", None)
    result = verify_compaction(after_append, records_before=1000, records_after=1500)
    assert result["check"] == "no-row-loss" and result["verified"]
    assert not verify_compaction(after_append, records_before=1000, records_after=900)["verified"]
