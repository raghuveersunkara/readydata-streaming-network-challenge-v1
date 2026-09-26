from __future__ import annotations

import json
import math
import os
import string
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


class ConfigurationError(ValueError):
    """Raised when versioned or runtime configuration is invalid."""


EVENT_TYPES = ("connectivity_metric", "throughput_test", "device_health")
CONTROL_EVENT_TYPES = ("connectivity_state", "infrastructure_event")
EXPECTED_TECHNOLOGIES = {"fiber", "coax", "fixed_wireless", "satellite"}
EXPECTED_ROLES = {"subscriber_edge", "aggregation", "backhaul", "ixp"}
TECHNOLOGY_SIGNAL_FIELDS = {
    "fiber": "optical_rx_power_dbm",
    "coax": "snr_db",
    "fixed_wireless": "signal_strength_dbm",
    "satellite": "signal_quality_pct",
}


@dataclass(frozen=True)
class RuntimeSettings:
    config: dict[str, Any]
    seed: int
    rate_multiplier: float
    mqtt_host: str
    mqtt_port: int
    run_id: uuid.UUID
    start_time: datetime
    max_events: int | None
    event_types: tuple[str, ...]

    def effective_interval_seconds(self, event_type: str) -> float:
        return (
            self.config["event_families"][event_type]["base_interval_seconds"]
            / self.rate_multiplier
        )


EXPECTED_TOP_LEVEL_KEYS = {
    "config_version",
    "schema_version",
    "inventory_version",
    "default_seed",
    "event_families",
    "control_event_families",
    "rate_multiplier",
    "scheduling",
    "data_quality",
    "metric_precision",
    "geography",
    "variation",
    "health",
    "technologies",
    "scenarios",
    "mqtt",
    "logging",
    "event_uuid_namespace",
}


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    actual = set(value)
    if actual != expected:
        raise ConfigurationError(
            f"{name} keys differ: missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}"
        )


def _require_number(value: Any, name: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ConfigurationError(f"{name} must be finite")
    if minimum is not None and result < minimum:
        raise ConfigurationError(f"{name} must be at least {minimum}")
    return result


def _require_range(
    value: Any,
    name: str,
    *,
    minimum: float | None = 0,
) -> tuple[float, float]:
    if not isinstance(value, list) or len(value) != 2:
        raise ConfigurationError(f"{name} must be a two-item array")
    low = _require_number(value[0], f"{name}[0]", minimum=minimum)
    high = _require_number(value[1], f"{name}[1]", minimum=minimum)
    if low > high:
        raise ConfigurationError(f"{name} lower bound exceeds upper bound")
    return low, high


def _validate_event_families(config: dict[str, Any]) -> None:
    families = config["event_families"]
    if not isinstance(families, dict) or tuple(families) != EVENT_TYPES:
        raise ConfigurationError("event_families must contain the ordered V1 family set")
    expected = {
        "connectivity_metric": (
            "network/{site_id}/{device_id}/connectivity",
            5.0,
            "subscriber_edge",
        ),
        "throughput_test": (
            "network/{site_id}/{device_id}/throughput",
            60.0,
            "subscriber_edge",
        ),
        "device_health": ("network/{site_id}/{device_id}/health", 30.0, "all"),
    }
    for event_type, values in families.items():
        name = f"event_families.{event_type}"
        if not isinstance(values, dict):
            raise ConfigurationError(f"{name} must be an object")
        _require_exact_keys(
            values,
            {"enabled", "topic_template", "base_interval_seconds", "population"},
            name,
        )
        if values["enabled"] is not True:
            raise ConfigurationError(f"{name}.enabled must be true in V2")
        if not isinstance(values["topic_template"], str):
            raise ConfigurationError(f"{name}.topic_template must be text")
        fields = {
            field_name
            for _literal, field_name, _format_spec, _conversion in string.Formatter().parse(
                values["topic_template"]
            )
            if field_name
        }
        if fields != {"site_id", "device_id"} or any(
            wildcard in values["topic_template"] for wildcard in ("+", "#")
        ):
            raise ConfigurationError(
                f"{name}.topic_template must contain only site_id and device_id fields"
            )
        topic, interval, population = expected[event_type]
        if (
            values["topic_template"] != topic
            or _require_number(
                values["base_interval_seconds"],
                f"{name}.base_interval_seconds",
                minimum=0.001,
            )
            != interval
            or values["population"] != population
        ):
                raise ConfigurationError(f"{name} differs from the V3 event-family contract")

    control_families = config["control_event_families"]
    if not isinstance(control_families, dict) or tuple(control_families) != CONTROL_EVENT_TYPES:
        raise ConfigurationError(
            "control_event_families must contain the ordered V1 control-family set"
        )
    expected_control = {
        "connectivity_state": "network/{site_id}/{device_id}/state",
        "infrastructure_event": "network/{site_id}/{device_id}/infrastructure",
    }
    for event_type, values in control_families.items():
        name = f"control_event_families.{event_type}"
        if not isinstance(values, dict):
            raise ConfigurationError(f"{name} must be an object")
        _require_exact_keys(values, {"enabled", "topic_template"}, name)
        if values["enabled"] is not True or values["topic_template"] != expected_control[event_type]:
            raise ConfigurationError(f"{name} differs from the V1 control-family contract")


def _validate_scenarios(config: dict[str, Any]) -> None:
    scenarios = config["scenarios"]
    _require_exact_keys(
        scenarios,
        {
            "selection",
            "peer_outlier",
            "degraded_geographic",
            "high_performing_geographic",
            "provider_technology",
            "shared_incident",
        },
        "scenarios",
    )
    selection = scenarios["selection"]
    _require_exact_keys(
        selection,
        {
            "geographic_cluster_size",
            "provider_technology_minimum_size",
            "peer_group_minimum_size",
            "minimum_anchor_separation_km",
        },
        "scenarios.selection",
    )
    for field in (
        "geographic_cluster_size",
        "provider_technology_minimum_size",
        "peer_group_minimum_size",
    ):
        value = selection[field]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ConfigurationError(f"scenarios.selection.{field} must be positive")
    _require_number(
        selection["minimum_anchor_separation_km"],
        "scenarios.selection.minimum_anchor_separation_km",
        minimum=0.001,
    )

    effect_keys = {
        "peer_outlier": {
            "latency_multiplier",
            "jitter_multiplier",
            "packet_loss_add_pct",
            "download_multiplier",
            "upload_multiplier",
        },
        "degraded_geographic": {
            "latency_add_ms",
            "jitter_add_ms",
            "packet_loss_add_pct",
            "download_multiplier",
            "upload_multiplier",
        },
        "high_performing_geographic": {
            "latency_multiplier",
            "jitter_multiplier",
            "packet_loss_multiplier",
            "download_multiplier",
            "upload_multiplier",
        },
        "provider_technology": {
            "upload_multiplier",
            "health_signal_adverse_fraction",
        },
    }
    for scenario, keys in effect_keys.items():
        values = scenarios[scenario]
        _require_exact_keys(values, keys, f"scenarios.{scenario}")
        for field, value in values.items():
            parsed = _require_number(value, f"scenarios.{scenario}.{field}", minimum=0)
            if "fraction" in field and parsed > 1:
                raise ConfigurationError(f"scenarios.{scenario}.{field} cannot exceed 1")

    shared = scenarios["shared_incident"]
    _require_exact_keys(
        shared,
        {
            "phase_durations_seconds",
            "latency_add_ms",
            "jitter_add_ms",
            "packet_loss_add_pct",
            "throughput_multiplier",
            "recovery_effect_fraction",
            "infrastructure_health_adverse_fraction",
            "max_control_queue_records",
        },
        "scenarios.shared_incident",
    )
    durations = shared["phase_durations_seconds"]
    _require_exact_keys(
        durations,
        {"healthy_warmup", "congested", "offline", "recovering", "healthy"},
        "scenarios.shared_incident.phase_durations_seconds",
    )
    for phase, value in durations.items():
        _require_number(
            value,
            f"scenarios.shared_incident.phase_durations_seconds.{phase}",
            minimum=0.001,
        )
    for field in (
        "latency_add_ms",
        "jitter_add_ms",
        "packet_loss_add_pct",
        "throughput_multiplier",
        "recovery_effect_fraction",
        "infrastructure_health_adverse_fraction",
    ):
        value = _require_number(shared[field], f"scenarios.shared_incident.{field}", minimum=0)
        if field.endswith(("multiplier", "fraction")) and value > 1:
            raise ConfigurationError(f"scenarios.shared_incident.{field} cannot exceed 1")
    maximum = shared["max_control_queue_records"]
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 1:
        raise ConfigurationError(
            "scenarios.shared_incident.max_control_queue_records must be positive"
        )


def _validate_metrics(config: dict[str, Any]) -> None:
    precision = config["metric_precision"]
    _require_exact_keys(
        precision,
        {
            "latency_ms",
            "jitter_ms",
            "packet_loss_pct",
            "download_mbps",
            "upload_mbps",
            "health",
        },
        "metric_precision",
    )
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in precision.values()
    ):
        raise ConfigurationError("metric precision values must be nonnegative integers")

    geography = config["geography"]
    _require_exact_keys(
        geography,
        {
            "density_weight",
            "urban_proximity_weight",
            "urban_distance_scale_km",
            "latency_multiplier",
            "jitter_multiplier",
            "packet_loss_multiplier",
            "throughput_multiplier",
        },
        "geography",
    )
    density_weight = _require_number(
        geography["density_weight"], "geography.density_weight", minimum=0
    )
    urban_weight = _require_number(
        geography["urban_proximity_weight"],
        "geography.urban_proximity_weight",
        minimum=0,
    )
    if not math.isclose(density_weight + urban_weight, 1.0):
        raise ConfigurationError("geographic weights must sum to 1")
    _require_number(
        geography["urban_distance_scale_km"],
        "geography.urban_distance_scale_km",
        minimum=0.001,
    )
    for field in (
        "latency_multiplier",
        "jitter_multiplier",
        "packet_loss_multiplier",
        "throughput_multiplier",
    ):
        _require_range(geography[field], f"geography.{field}")

    variation = config["variation"]
    _require_exact_keys(
        variation,
        {
            "device_multiplier",
            "latency_measurement_multiplier",
            "jitter_measurement_multiplier",
            "throughput_measurement_multiplier",
            "health_device_multiplier",
            "health_measurement_multiplier",
        },
        "variation",
    )
    for field, value in variation.items():
        _require_range(value, f"variation.{field}")


def _validate_data_quality(config: dict[str, Any]) -> None:
    data_quality = config["data_quality"]
    _require_exact_keys(
        data_quality,
        {
            "anomaly_rates",
            "late_arrival",
            "max_pending_records",
            "poison_strategy",
        },
        "data_quality",
    )
    rates = data_quality["anomaly_rates"]
    expected_rates = {
        "duplicate",
        "sequence_gap",
        "out_of_order",
        "late_arrival",
        "poison",
    }
    _require_exact_keys(rates, expected_rates, "data_quality.anomaly_rates")
    total = sum(
        _require_number(value, f"data_quality.anomaly_rates.{name}", minimum=0)
        for name, value in rates.items()
    )
    if total > 1:
        raise ConfigurationError("data-quality anomaly rates cannot sum above 1")

    late = data_quality["late_arrival"]
    _require_exact_keys(
        late,
        {"minimum_delay_seconds", "cadence_multiplier"},
        "data_quality.late_arrival",
    )
    _require_number(
        late["minimum_delay_seconds"],
        "data_quality.late_arrival.minimum_delay_seconds",
        minimum=0.001,
    )
    _require_number(
        late["cadence_multiplier"],
        "data_quality.late_arrival.cadence_multiplier",
        minimum=0.001,
    )
    maximum = data_quality["max_pending_records"]
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum <= 0:
        raise ConfigurationError("data_quality.max_pending_records must be a positive integer")
    if data_quality["poison_strategy"] != "truncate_final_byte":
        raise ConfigurationError(
            "data_quality.poison_strategy must be truncate_final_byte in V2"
        )


def _validate_health(config: dict[str, Any]) -> None:
    health = config["health"]
    _require_exact_keys(health, {"common_ranges", "role_baselines"}, "health")
    common_fields = {"cpu_pct", "memory_pct", "temperature_c"}
    ranges = health["common_ranges"]
    _require_exact_keys(ranges, common_fields, "health.common_ranges")
    parsed_ranges = {
        field: _require_range(value, f"health.common_ranges.{field}")
        for field, value in ranges.items()
    }
    roles = health["role_baselines"]
    _require_exact_keys(roles, EXPECTED_ROLES, "health.role_baselines")
    for role, baselines in roles.items():
        _require_exact_keys(baselines, common_fields, f"health.role_baselines.{role}")
        for field, value in baselines.items():
            baseline = _require_number(value, f"health.role_baselines.{role}.{field}")
            if not parsed_ranges[field][0] <= baseline <= parsed_ranges[field][1]:
                raise ConfigurationError(f"{role} {field} baseline is outside its range")


def _validate_technologies(config: dict[str, Any]) -> None:
    technologies = config["technologies"]
    if set(technologies) != EXPECTED_TECHNOLOGIES:
        raise ConfigurationError("configured technologies differ from the V1 inventory")
    keys = {
        "latency_baseline_ms",
        "latency_range_ms",
        "jitter_baseline_ms",
        "jitter_range_ms",
        "nonzero_packet_loss_probability",
        "packet_loss_cap_pct",
        "download_efficiency",
        "upload_efficiency",
        "health_signal_field",
        "health_signal_baseline",
        "health_signal_range",
        "health_signal_variation",
    }
    for technology, values in technologies.items():
        name = f"technologies.{technology}"
        _require_exact_keys(values, keys, name)
        latency_range = _require_range(values["latency_range_ms"], f"{name}.latency_range_ms")
        jitter_range = _require_range(values["jitter_range_ms"], f"{name}.jitter_range_ms")
        latency = _require_number(values["latency_baseline_ms"], f"{name}.latency_baseline_ms")
        jitter = _require_number(values["jitter_baseline_ms"], f"{name}.jitter_baseline_ms")
        if not latency_range[0] <= latency <= latency_range[1]:
            raise ConfigurationError(f"{technology} latency baseline is outside its range")
        if not jitter_range[0] <= jitter <= jitter_range[1]:
            raise ConfigurationError(f"{technology} jitter baseline is outside its range")
        probability = _require_number(
            values["nonzero_packet_loss_probability"],
            f"{name}.nonzero_packet_loss_probability",
        )
        if probability > 1:
            raise ConfigurationError(f"{technology} packet-loss probability exceeds 1")
        _require_number(values["packet_loss_cap_pct"], f"{name}.packet_loss_cap_pct")
        for field in ("download_efficiency", "upload_efficiency"):
            efficiency = _require_range(values[field], f"{name}.{field}")
            if efficiency[1] > 1:
                raise ConfigurationError(f"{name}.{field} cannot exceed 1")
        if values["health_signal_field"] != TECHNOLOGY_SIGNAL_FIELDS[technology]:
            raise ConfigurationError(f"{technology} health signal field differs from V1")
        signal_range = _require_range(
            values["health_signal_range"], f"{name}.health_signal_range", minimum=None
        )
        signal_baseline = _require_number(
            values["health_signal_baseline"], f"{name}.health_signal_baseline"
        )
        if not signal_range[0] <= signal_baseline <= signal_range[1]:
            raise ConfigurationError(f"{technology} health signal baseline is outside its range")
        _require_range(
            values["health_signal_variation"],
            f"{name}.health_signal_variation",
            minimum=None,
        )


def validate_config(config: dict[str, Any]) -> None:
    _require_exact_keys(config, EXPECTED_TOP_LEVEL_KEYS, "config")
    if config["config_version"] != 3 or config["schema_version"] != 1:
        raise ConfigurationError("only simulator config V3 and schema V1 are supported")
    if config["inventory_version"] != "v1":
        raise ConfigurationError("only inventory version v1 is supported")
    if isinstance(config["default_seed"], bool) or not isinstance(config["default_seed"], int):
        raise ConfigurationError("default_seed must be an integer")
    _validate_event_families(config)

    rate = config["rate_multiplier"]
    _require_exact_keys(rate, {"default", "minimum", "maximum"}, "rate_multiplier")
    rate_min = _require_number(rate["minimum"], "rate_multiplier.minimum", minimum=0.001)
    rate_max = _require_number(rate["maximum"], "rate_multiplier.maximum", minimum=rate_min)
    rate_default = _require_number(rate["default"], "rate_multiplier.default", minimum=rate_min)
    if rate_default > rate_max:
        raise ConfigurationError("default rate multiplier exceeds the maximum")

    scheduling = config["scheduling"]
    _require_exact_keys(scheduling, {"lateness_tolerance_fraction"}, "scheduling")
    lateness = _require_number(
        scheduling["lateness_tolerance_fraction"],
        "scheduling.lateness_tolerance_fraction",
        minimum=0,
    )
    if lateness > 1:
        raise ConfigurationError("scheduling lateness tolerance cannot exceed 1")
    _validate_data_quality(config)
    _validate_metrics(config)
    _validate_health(config)
    _validate_technologies(config)
    _validate_scenarios(config)

    mqtt = config["mqtt"]
    _require_exact_keys(
        mqtt,
        {
            "qos",
            "retain",
            "keepalive_seconds",
            "connect_timeout_seconds",
            "reconnect_min_seconds",
            "reconnect_max_seconds",
            "max_inflight_messages",
            "max_queued_messages",
            "shutdown_drain_seconds",
        },
        "mqtt",
    )
    if mqtt["qos"] != 1 or mqtt["retain"] is not False:
        raise ConfigurationError("simulator V2 requires QoS 1 and retain=false")
    for field in (
        "keepalive_seconds",
        "connect_timeout_seconds",
        "reconnect_min_seconds",
        "reconnect_max_seconds",
        "max_inflight_messages",
        "max_queued_messages",
        "shutdown_drain_seconds",
    ):
        _require_number(mqtt[field], f"mqtt.{field}", minimum=0.001)
    if mqtt["reconnect_min_seconds"] > mqtt["reconnect_max_seconds"]:
        raise ConfigurationError("MQTT reconnect minimum exceeds maximum")

    logging_config = config["logging"]
    _require_exact_keys(logging_config, {"summary_interval_seconds"}, "logging")
    _require_number(
        logging_config["summary_interval_seconds"],
        "logging.summary_interval_seconds",
        minimum=1,
    )
    try:
        uuid.UUID(config["event_uuid_namespace"])
    except (ValueError, TypeError, AttributeError) as error:
        raise ConfigurationError("event_uuid_namespace must be a UUID") from error


def _integer_environment(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name, str(default))
    try:
        return int(raw)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error


def load_settings(
    config_path: Path,
    environ: Mapping[str, str] | None = None,
    current_time: datetime | None = None,
) -> RuntimeSettings:
    environment = os.environ if environ is None else environ
    try:
        with config_path.open(encoding="utf-8") as handle:
            config = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise ConfigurationError(f"cannot read simulator config {config_path}: {error}") from error
    if not isinstance(config, dict):
        raise ConfigurationError("simulator config must be a JSON object")
    validate_config(config)

    seed = _integer_environment(environment, "SIMULATOR_SEED", config["default_seed"])
    raw_multiplier = environment.get(
        "EVENT_RATE_MULTIPLIER", str(config["rate_multiplier"]["default"])
    )
    try:
        rate_multiplier = float(raw_multiplier)
    except ValueError as error:
        raise ConfigurationError("EVENT_RATE_MULTIPLIER must be numeric") from error
    if not math.isfinite(rate_multiplier) or not (
        config["rate_multiplier"]["minimum"]
        <= rate_multiplier
        <= config["rate_multiplier"]["maximum"]
    ):
        raise ConfigurationError(
            "EVENT_RATE_MULTIPLIER must be between "
            f"{config['rate_multiplier']['minimum']} and {config['rate_multiplier']['maximum']}"
        )

    requested_types = environment.get("SIMULATOR_EVENT_TYPES")
    if requested_types:
        selected = tuple(part.strip() for part in requested_types.split(",") if part.strip())
        if not selected or len(set(selected)) != len(selected) or any(
            event_type not in EVENT_TYPES for event_type in selected
        ):
            raise ConfigurationError(
                "SIMULATOR_EVENT_TYPES must be a unique comma-separated V3 periodic-family subset"
            )
        event_types = tuple(event_type for event_type in EVENT_TYPES if event_type in selected)
    else:
        event_types = EVENT_TYPES

    mqtt_host = environment.get("MQTT_HOST", "mosquitto").strip()
    if not mqtt_host:
        raise ConfigurationError("MQTT_HOST cannot be empty")
    mqtt_port = _integer_environment(environment, "MQTT_PORT", 1883)
    if not 1 <= mqtt_port <= 65535:
        raise ConfigurationError("MQTT_PORT must be between 1 and 65535")

    raw_run_id = environment.get("SIMULATOR_RUN_ID")
    try:
        run_id = uuid.UUID(raw_run_id) if raw_run_id else uuid.uuid4()
    except ValueError as error:
        raise ConfigurationError("SIMULATOR_RUN_ID must be a UUID") from error

    raw_start = environment.get("SIMULATOR_START_TIME")
    if raw_start:
        if not raw_start.endswith("Z"):
            raise ConfigurationError("SIMULATOR_START_TIME must be UTC and end in Z")
        try:
            start_time = datetime.fromisoformat(raw_start.removesuffix("Z") + "+00:00")
        except ValueError as error:
            raise ConfigurationError("SIMULATOR_START_TIME must be an RFC 3339 timestamp") from error
    else:
        start_time = current_time or datetime.now(timezone.utc)
    start_time = start_time.astimezone(timezone.utc).replace(
        microsecond=(start_time.microsecond // 1000) * 1000
    )

    raw_max_events = environment.get("SIMULATOR_MAX_EVENTS")
    max_events: int | None = None
    if raw_max_events:
        try:
            max_events = int(raw_max_events)
        except ValueError as error:
            raise ConfigurationError("SIMULATOR_MAX_EVENTS must be an integer") from error
        if max_events <= 0:
            raise ConfigurationError("SIMULATOR_MAX_EVENTS must be positive")

    return RuntimeSettings(
        config=config,
        seed=seed,
        rate_multiplier=rate_multiplier,
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        run_id=run_id,
        start_time=start_time,
        max_events=max_events,
        event_types=event_types,
    )
