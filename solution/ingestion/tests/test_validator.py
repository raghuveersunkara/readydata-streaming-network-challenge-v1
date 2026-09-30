import os
import json
import pytest
from src.validator import ContractValidator


@pytest.fixture
def sample_contracts_dir(tmp_path):
    """Creates a temporary mock schema environment with envelope and event schemas."""
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()

    # 1. Envelope schema
    envelope_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": [
            "schema_version",
            "event_id",
            "event_type",
            "event_timestamp",
            "device_session_id",
            "sequence_number",
            "site_id",
            "device_id",
        ],
        "properties": {
            "schema_version": {"type": "integer"},
            "event_id": {"type": "string"},
            "event_type": {"type": "string"},
            "event_timestamp": {"type": "string"},
            "device_session_id": {"type": "string"},
            "sequence_number": {"type": "integer"},
            "site_id": {"type": "string"},
            "device_id": {"type": "string"},
        },
    }
    with open(contracts_dir / "common-envelope.v1.schema.json", "w") as f:
        json.dump(envelope_schema, f)

    # 2. Connectivity metric schema
    connectivity_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "allOf": [
            {"$ref": "common-envelope.v1.schema.json"},
            {
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["latency_ms"],
                        "properties": {
                            "latency_ms": {"type": "number"},
                            "jitter_ms": {"type": "number"},
                            "packet_loss_pct": {"type": "number"},
                        },
                    }
                },
                "required": ["payload"],
            },
        ],
    }
    with open(contracts_dir / "connectivity-metric.v1.schema.json", "w") as f:
        json.dump(connectivity_schema, f)

    return str(contracts_dir)


def test_validator_initialization_non_existent_path():
    validator = ContractValidator(contracts_path="/non/existent/path")
    is_valid, reason = validator.validate({"event_type": "connectivity_metric"})
    assert not is_valid
    assert "Unknown or missing event_type" in reason


def test_validator_unknown_event_type(sample_contracts_dir):
    validator = ContractValidator(contracts_path=sample_contracts_dir)
    invalid_event = {"event_type": "unknown_type"}
    is_valid, reason = validator.validate(invalid_event)
    assert not is_valid
    assert "Unknown or missing event_type: 'unknown_type'" in reason


def test_validator_missing_required_fields(sample_contracts_dir):
    validator = ContractValidator(contracts_path=sample_contracts_dir)
    payload_missing_fields = {
        "event_type": "connectivity_metric",
        "event_id": "evt-001",
    }
    is_valid, reason = validator.validate(payload_missing_fields)
    assert not is_valid
    assert "Schema validation error" in reason


def test_validator_success(sample_contracts_dir):
    validator = ContractValidator(contracts_path=sample_contracts_dir)
    valid_event = {
        "schema_version": 1,
        "event_id": "evt-001",
        "event_type": "connectivity_metric",
        "event_timestamp": "2026-09-27T18:00:00Z",
        "device_session_id": "sess-100",
        "sequence_number": 1,
        "site_id": "site-austin",
        "device_id": "dev-404",
        "payload": {
            "latency_ms": 12.4,
            "jitter_ms": 1.1,
            "packet_loss_pct": 0.0,
        },
    }
    is_valid, reason = validator.validate(valid_event)
    assert is_valid
    assert reason == ""


REAL_CONTRACTS_DIR = os.getenv("CONTRACTS_DIR", "/app/contracts/events")


@pytest.mark.skipif(
    not os.path.isdir(os.path.join(REAL_CONTRACTS_DIR, "examples")),
    reason="Supplied contracts are not mounted",
)
@pytest.mark.parametrize(
    "example_file",
    sorted(os.listdir(os.path.join(REAL_CONTRACTS_DIR, "examples")))
    if os.path.isdir(os.path.join(REAL_CONTRACTS_DIR, "examples"))
    else [],
)
def test_supplied_examples_pass_real_contracts(example_file):
    validator = ContractValidator(contracts_path=REAL_CONTRACTS_DIR)
    with open(os.path.join(REAL_CONTRACTS_DIR, "examples", example_file)) as f:
        event = json.load(f)
    is_valid, reason = validator.validate(event)
    assert is_valid, f"{example_file}: {reason}"


def _connectivity_example():
    with open(os.path.join(REAL_CONTRACTS_DIR, "examples", "connectivity-metric.v1.json")) as f:
        return json.load(f)


def _without(d, key):
    d = dict(d)
    d.pop(key)
    return d


# Each mutation breaks one rule of the real connectivity contract and must be rejected.
BROKEN_EVENTS = {
    "missing payload field": lambda e: {**e, "payload": _without(e["payload"], "latency_ms")},
    "payload value of wrong type": lambda e: {**e, "payload": {**e["payload"], "latency_ms": "fast"}},
    "unexpected payload field": lambda e: {**e, "payload": {**e["payload"], "rtt_ms": 1.0}},
    "unexpected envelope field": lambda e: {**e, "region": "central"},
    "missing envelope field": lambda e: _without(e, "device_session_id"),
    "timestamp without milliseconds": lambda e: {**e, "event_timestamp": "2026-08-19T14:32:10Z"},
    "percentage above 100": lambda e: {**e, "payload": {**e["payload"], "packet_loss_pct": 150}},
    "negative latency": lambda e: {**e, "payload": {**e["payload"], "latency_ms": -1}},
    "sequence number below 1": lambda e: {**e, "sequence_number": 0},
    "unsupported schema version": lambda e: {**e, "schema_version": 2},
    "event_id not a uuid (format)": lambda e: {**e, "event_id": "not-a-uuid"},
    "device_id not matching pattern": lambda e: {**e, "device_id": "Device-1"},
}


@pytest.mark.skipif(
    not os.path.isdir(os.path.join(REAL_CONTRACTS_DIR, "examples")),
    reason="Supplied contracts are not mounted",
)
@pytest.mark.parametrize("case", sorted(BROKEN_EVENTS))
def test_real_contract_rejects_broken_events(case):
    validator = ContractValidator(contracts_path=REAL_CONTRACTS_DIR)
    event = BROKEN_EVENTS[case](_connectivity_example())
    is_valid, reason = validator.validate(event)
    assert not is_valid, f"{case} was accepted"
    assert reason.startswith("Schema validation error")


@pytest.mark.skipif(
    not os.path.isdir(os.path.join(REAL_CONTRACTS_DIR, "examples")),
    reason="Supplied contracts are not mounted",
)
def test_contract_does_not_enforce_one_technology_health_field():
    """Documents a boundary: the contract README says an access-device health payload has
    exactly one technology field, but the schema has no oneOf, so two fields pass the
    contract. That rule depends on the inventory, so it belongs in a data-quality check."""
    validator = ContractValidator(contracts_path=REAL_CONTRACTS_DIR)
    with open(os.path.join(REAL_CONTRACTS_DIR, "examples", "device-health-fiber.v1.json")) as f:
        event = json.load(f)
    event["payload"]["snr_db"] = 30.0
    assert validator.validate(event) == (True, "")
