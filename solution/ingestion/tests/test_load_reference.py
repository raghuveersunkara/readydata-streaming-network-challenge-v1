"""Exit codes and all-or-nothing behaviour of the one-shot reference loader (load_reference.py).

0 = loaded or already current, 1 = validation failed and nothing was written,
2 = catalog/storage still failing after retries.
"""
import pytest

import load_reference
from tests.test_reference import _valid_files, _write_bundle


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(load_reference.time, "sleep", lambda s: None)


def test_validation_failure_exits_1_and_never_touches_the_catalog(tmp_path, monkeypatch):
    files = _valid_files()
    files["inventory/v1/devices.csv"].append(files["inventory/v1/devices.csv"][3])  # duplicate device
    monkeypatch.setattr(load_reference, "REFERENCE_DATA_DIR", _write_bundle(tmp_path, files))

    def catalog_must_not_be_used():
        raise AssertionError("catalog opened although validation failed")
    monkeypatch.setattr(load_reference, "load_solution_catalog", catalog_must_not_be_used)

    assert load_reference.main() == 1


def test_storage_failure_exits_2_after_retries(tmp_path, monkeypatch, no_sleep):
    monkeypatch.setattr(load_reference, "REFERENCE_DATA_DIR", _write_bundle(tmp_path, _valid_files()))
    attempts = []

    def unavailable():
        attempts.append(1)
        raise ConnectionError("nessie down")
    monkeypatch.setattr(load_reference, "load_solution_catalog", unavailable)

    assert load_reference.main() == 2
    assert len(attempts) == load_reference.MAX_ATTEMPTS


def test_success_after_a_transient_storage_error_exits_0(tmp_path, monkeypatch, no_sleep):
    monkeypatch.setattr(load_reference, "REFERENCE_DATA_DIR", _write_bundle(tmp_path, _valid_files()))
    calls = []

    class Catalog:
        def create_namespace_if_not_exists(self, name):
            pass

    def flaky_catalog():
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("not ready yet")
        return Catalog()
    monkeypatch.setattr(load_reference, "load_solution_catalog", flaky_catalog)
    monkeypatch.setattr(load_reference, "write_reference", lambda catalog, bundle: {"sites": "loaded 2 rows"})

    assert load_reference.main() == 0
    assert len(calls) == 2
