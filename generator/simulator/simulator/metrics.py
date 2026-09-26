from __future__ import annotations

import csv
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SITE_FIELDS = [
    "site_id",
    "latitude",
    "longitude",
    "region",
    "block_group_geoid",
    "inside_census_urban_area",
    "urban_area_name",
    "distance_to_nearest_urban_area_km",
    "nearest_census_place",
    "distance_to_nearest_place_km",
    "site_type",
    "critical_infrastructure",
]
DEVICE_FIELDS = [
    "device_id",
    "site_id",
    "provider_id",
    "provider_name",
    "technology",
    "device_class",
    "device_model",
    "infrastructure_role",
    "advertised_download_mbps",
    "advertised_upload_mbps",
    "deployment_date",
]
CENSUS_FIELDS = [
    "block_group_geoid",
    "population",
    "land_area_sq_miles",
    "population_density_per_sq_mile",
    "internal_latitude",
    "internal_longitude",
]
DEPENDENCY_FIELDS = [
    "downstream_device_id",
    "upstream_device_id",
    "dependency_type",
]


class InventoryError(ValueError):
    """Raised when the supplied static inventory violates its contract."""


@dataclass(frozen=True)
class DeviceProfile:
    device_id: str
    site_id: str
    provider_id: str
    technology: str
    device_class: str
    device_model: str
    infrastructure_role: str
    advertised_download_mbps: float | None
    advertised_upload_mbps: float | None
    block_group_geoid: str
    density_percentile: float
    inside_urban_area: bool
    distance_to_urban_area_km: float
    latitude: float
    longitude: float
    critical_infrastructure: bool
    upstream_device_id: str | None

    @property
    def is_subscriber_edge(self) -> bool:
        return self.infrastructure_role == "subscriber_edge"


@dataclass(frozen=True)
class ConnectivityMetrics:
    latency_ms: float
    jitter_ms: float
    packet_loss_pct: float


@dataclass(frozen=True)
class ThroughputMetrics:
    download_mbps: float
    upload_mbps: float


@dataclass(frozen=True)
class DeviceHealthMetrics:
    cpu_pct: float
    memory_pct: float
    temperature_c: float
    technology_signal_field: str | None = None
    technology_signal_value: float | None = None


def _read_csv(path: Path, expected_fields: list[str]) -> list[dict[str, str]]:
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            actual_fields = list(reader.fieldnames or [])
            if actual_fields != expected_fields:
                raise InventoryError(
                    f"{path.name} schema differs: expected {expected_fields}, got {actual_fields}"
                )
            return list(reader)
    except OSError as error:
        raise InventoryError(f"cannot read {path}: {error}") from error


def _unique_by(rows: Iterable[dict[str, str]], key: str, name: str) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        value = row[key]
        if not value or value in result:
            raise InventoryError(f"{name} has an empty or duplicate {key}: {value!r}")
        result[value] = row
    return result


def _density_percentiles(census_rows: list[dict[str, str]]) -> dict[str, float]:
    values: list[tuple[float, str]] = []
    for row in census_rows:
        try:
            density = float(row["population_density_per_sq_mile"])
        except ValueError as error:
            raise InventoryError(
                f"invalid density for block group {row['block_group_geoid']}"
            ) from error
        if not math.isfinite(density) or density < 0:
            raise InventoryError(f"invalid density for block group {row['block_group_geoid']}")
        values.append((density, row["block_group_geoid"]))
    values.sort(key=lambda item: (item[0], item[1]))

    result: dict[str, float] = {}
    index = 0
    total = len(values)
    while index < total:
        end = index + 1
        while end < total and values[end][0] == values[index][0]:
            end += 1
        average_rank = ((index + 1) + end) / 2
        percentile = average_rank / total
        for _density, geoid in values[index:end]:
            result[geoid] = percentile
        index = end
    return result


def load_device_profiles(
    sites_path: Path,
    devices_path: Path,
    census_path: Path,
    inventory_version: str,
    technologies: set[str],
    dependencies_path: Path | None = None,
) -> list[DeviceProfile]:
    manifest_path = devices_path.parent / "manifest.json"
    try:
        with manifest_path.open(encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise InventoryError(f"cannot read inventory manifest {manifest_path}: {error}") from error
    if manifest.get("inventory_version") != inventory_version:
        raise InventoryError(
            f"expected inventory {inventory_version}, got {manifest.get('inventory_version')!r}"
        )

    sites = _unique_by(_read_csv(sites_path, SITE_FIELDS), "site_id", "sites.csv")
    device_rows = _read_csv(devices_path, DEVICE_FIELDS)
    devices = _unique_by(device_rows, "device_id", "devices.csv")
    dependency_rows = _read_csv(
        dependencies_path or devices_path.parent / "device_dependencies.csv",
        DEPENDENCY_FIELDS,
    )
    expected_dependencies = manifest.get("counts", {}).get("dependencies")
    if len(dependency_rows) != expected_dependencies:
        raise InventoryError(
            f"expected {expected_dependencies} V1 dependencies, found {len(dependency_rows)}"
        )
    upstream_by_device: dict[str, str] = {}
    expected_dependency_types = {
        "subscriber_edge": "access_aggregation",
        "aggregation": "aggregation_backhaul",
        "backhaul": "backhaul_ixp",
    }
    expected_upstream_roles = {
        "access_aggregation": "aggregation",
        "aggregation_backhaul": "backhaul",
        "backhaul_ixp": "ixp",
    }
    for dependency in dependency_rows:
        downstream = dependency["downstream_device_id"]
        upstream = dependency["upstream_device_id"]
        if downstream not in devices or upstream not in devices:
            raise InventoryError(
                f"dependency references an unknown device: {downstream!r} -> {upstream!r}"
            )
        if downstream in upstream_by_device:
            raise InventoryError(f"device {downstream} has multiple immediate upstream devices")
        downstream_role = devices[downstream]["infrastructure_role"]
        expected_type = expected_dependency_types.get(downstream_role)
        if dependency["dependency_type"] != expected_type:
            raise InventoryError(
                f"device {downstream} has invalid dependency type "
                f"{dependency['dependency_type']!r}"
            )
        expected_upstream_role = expected_upstream_roles[dependency["dependency_type"]]
        if devices[upstream]["infrastructure_role"] != expected_upstream_role:
            raise InventoryError(
                f"dependency {downstream!r} -> {upstream!r} does not target an "
                f"{expected_upstream_role} device"
            )
        upstream_by_device[downstream] = upstream
    census_rows = _read_csv(census_path, CENSUS_FIELDS)
    census = _unique_by(census_rows, "block_group_geoid", "census_block_groups.csv")
    percentiles = _density_percentiles(census_rows)

    profiles: list[DeviceProfile] = []
    for device in device_rows:
        site = sites.get(device["site_id"])
        if site is None:
            raise InventoryError(
                f"device {device['device_id']} references missing site {device['site_id']}"
            )
        if device["technology"] not in technologies:
            raise InventoryError(
                f"device {device['device_id']} has unsupported technology {device['technology']}"
            )
        block_group = site["block_group_geoid"]
        if block_group not in census or block_group not in percentiles:
            raise InventoryError(
                f"site {site['site_id']} references missing Census block group {block_group}"
            )
        if site["inside_census_urban_area"] not in {"true", "false"}:
            raise InventoryError(f"site {site['site_id']} has an invalid urban-area boolean")
        if site["critical_infrastructure"] not in {"true", "false"}:
            raise InventoryError(
                f"site {site['site_id']} has an invalid critical-infrastructure boolean"
            )
        try:
            distance = float(site["distance_to_nearest_urban_area_km"])
            latitude = float(site["latitude"])
            longitude = float(site["longitude"])
        except ValueError as error:
            raise InventoryError(f"site {site['site_id']} has invalid coordinates") from error
        if (
            not math.isfinite(distance)
            or distance < 0
            or not math.isfinite(latitude)
            or not -90 <= latitude <= 90
            or not math.isfinite(longitude)
            or not -180 <= longitude <= 180
        ):
            raise InventoryError(f"site {site['site_id']} has invalid geographic values")
        if device["infrastructure_role"] not in {
            "subscriber_edge",
            "aggregation",
            "backhaul",
            "ixp",
        }:
            raise InventoryError(
                f"device {device['device_id']} has an unsupported infrastructure role"
            )
        advertised_values: list[float | None] = []
        for field in ("advertised_download_mbps", "advertised_upload_mbps"):
            raw = device[field]
            try:
                value = float(raw) if raw else None
            except ValueError as error:
                raise InventoryError(
                    f"device {device['device_id']} has invalid {field}"
                ) from error
            if value is not None and (not math.isfinite(value) or value <= 0):
                raise InventoryError(
                    f"device {device['device_id']} has invalid {field}"
                )
            advertised_values.append(value)
        if device["infrastructure_role"] == "subscriber_edge":
            if any(value is None for value in advertised_values):
                raise InventoryError(
                    f"access device {device['device_id']} lacks advertised speeds"
                )
        elif any(value is not None for value in advertised_values):
            raise InventoryError(
                f"infrastructure device {device['device_id']} unexpectedly has advertised speeds"
                )
        upstream = upstream_by_device.get(device["device_id"])
        if device["infrastructure_role"] == "ixp":
            if upstream is not None:
                raise InventoryError(
                    f"IXP device {device['device_id']} unexpectedly has an upstream device"
                )
        elif upstream is None:
            raise InventoryError(f"device {device['device_id']} lacks an upstream dependency")
        profiles.append(
            DeviceProfile(
                device_id=device["device_id"],
                site_id=site["site_id"],
                provider_id=device["provider_id"],
                technology=device["technology"],
                device_class=device["device_class"],
                device_model=device["device_model"],
                infrastructure_role=device["infrastructure_role"],
                advertised_download_mbps=advertised_values[0],
                advertised_upload_mbps=advertised_values[1],
                block_group_geoid=block_group,
                density_percentile=percentiles[block_group],
                inside_urban_area=site["inside_census_urban_area"] == "true",
                distance_to_urban_area_km=distance,
                latitude=latitude,
                longitude=longitude,
                critical_infrastructure=site["critical_infrastructure"] == "true",
                upstream_device_id=upstream,
            )
        )

    expected_count = manifest.get("counts", {}).get("devices")
    if expected_count != 654 or len(profiles) != expected_count:
        raise InventoryError(
            f"expected 654 devices from the V1 manifest, found {len(profiles)}"
        )
    profiles.sort(key=lambda profile: profile.device_id)
    return profiles


def stable_float(seed: int, *parts: Any) -> float:
    material = "|".join([str(seed), *(str(part) for part in parts)]).encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big") / 2**64


def _scale(value: float, bounds: list[float]) -> float:
    return bounds[0] + value * (bounds[1] - bounds[0])


def _clamp(value: float, bounds: list[float]) -> float:
    return min(max(value, bounds[0]), bounds[1])


def developed_score(profile: DeviceProfile, config: dict[str, Any]) -> float:
    geography = config["geography"]
    urban_proximity = (
        1.0
        if profile.inside_urban_area
        else math.exp(
            -profile.distance_to_urban_area_km / geography["urban_distance_scale_km"]
        )
    )
    return (
        geography["density_weight"] * profile.density_percentile
        + geography["urban_proximity_weight"] * urban_proximity
    )


def geographic_multipliers(
    profile: DeviceProfile, config: dict[str, Any]
) -> dict[str, float]:
    developed = developed_score(profile, config)
    geography = config["geography"]
    return {
        "latency": geography["latency_multiplier"][1]
        - (geography["latency_multiplier"][1] - geography["latency_multiplier"][0])
        * developed,
        "jitter": geography["jitter_multiplier"][1]
        - (geography["jitter_multiplier"][1] - geography["jitter_multiplier"][0])
        * developed,
        "packet_loss": geography["packet_loss_multiplier"][1]
        - (
            geography["packet_loss_multiplier"][1]
            - geography["packet_loss_multiplier"][0]
        )
        * developed,
        "throughput": geography["throughput_multiplier"][0]
        + (
            geography["throughput_multiplier"][1]
            - geography["throughput_multiplier"][0]
        )
        * developed,
    }


def generate_connectivity_metrics(
    profile: DeviceProfile,
    sequence_number: int,
    seed: int,
    config: dict[str, Any],
) -> ConnectivityMetrics:
    if sequence_number < 1:
        raise ValueError("sequence_number must be positive")
    technology = config["technologies"][profile.technology]
    variation = config["variation"]
    geography = geographic_multipliers(profile, config)

    latency_device = _scale(
        stable_float(seed, "device-latency", profile.device_id),
        variation["device_multiplier"],
    )
    jitter_device = _scale(
        stable_float(seed, "device-jitter", profile.device_id),
        variation["device_multiplier"],
    )
    latency_measurement = _scale(
        stable_float(seed, "measurement-latency", profile.device_id, sequence_number),
        variation["latency_measurement_multiplier"],
    )
    jitter_measurement = _scale(
        stable_float(seed, "measurement-jitter", profile.device_id, sequence_number),
        variation["jitter_measurement_multiplier"],
    )

    latency = _clamp(
        technology["latency_baseline_ms"]
        * latency_device
        * geography["latency"]
        * latency_measurement,
        technology["latency_range_ms"],
    )
    jitter = _clamp(
        technology["jitter_baseline_ms"]
        * jitter_device
        * geography["jitter"]
        * jitter_measurement,
        technology["jitter_range_ms"],
    )

    loss_occurrence = stable_float(
        seed, "measurement-loss-occurrence", profile.device_id, sequence_number
    )
    packet_loss = 0.0
    if loss_occurrence < technology["nonzero_packet_loss_probability"]:
        magnitude = stable_float(
            seed, "measurement-loss-magnitude", profile.device_id, sequence_number
        )
        packet_loss = min(
            technology["packet_loss_cap_pct"],
            technology["packet_loss_cap_pct"]
            * (0.05 + 0.95 * magnitude)
            * geography["packet_loss"],
        )

    precision = config["metric_precision"]
    return ConnectivityMetrics(
        latency_ms=round(latency, precision["latency_ms"]),
        jitter_ms=round(jitter, precision["jitter_ms"]),
        packet_loss_pct=round(packet_loss, precision["packet_loss_pct"]),
    )


def generate_throughput_metrics(
    profile: DeviceProfile,
    sequence_number: int,
    seed: int,
    config: dict[str, Any],
) -> ThroughputMetrics:
    if sequence_number < 1:
        raise ValueError("sequence_number must be positive")
    if not profile.is_subscriber_edge:
        raise ValueError("throughput is only defined for subscriber-edge devices")
    if profile.advertised_download_mbps is None or profile.advertised_upload_mbps is None:
        raise ValueError("throughput requires advertised download and upload speeds")

    technology = config["technologies"][profile.technology]
    measurement_bounds = config["variation"]["throughput_measurement_multiplier"]
    geography = geographic_multipliers(profile, config)["throughput"]

    def measured(metric: str, advertised: float, efficiency_bounds: list[float]) -> float:
        efficiency = _scale(
            stable_float(seed, f"device-{metric}-efficiency", profile.device_id),
            efficiency_bounds,
        )
        variation = _scale(
            stable_float(
                seed,
                f"measurement-{metric}-throughput",
                profile.device_id,
                sequence_number,
            ),
            measurement_bounds,
        )
        return min(advertised * 1.05, advertised * efficiency * geography * variation)

    precision = config["metric_precision"]
    return ThroughputMetrics(
        download_mbps=round(
            measured(
                "download",
                profile.advertised_download_mbps,
                technology["download_efficiency"],
            ),
            precision["download_mbps"],
        ),
        upload_mbps=round(
            measured(
                "upload",
                profile.advertised_upload_mbps,
                technology["upload_efficiency"],
            ),
            precision["upload_mbps"],
        ),
    )


def generate_device_health_metrics(
    profile: DeviceProfile,
    sequence_number: int,
    seed: int,
    config: dict[str, Any],
) -> DeviceHealthMetrics:
    if sequence_number < 1:
        raise ValueError("sequence_number must be positive")
    health = config["health"]
    baseline = health["role_baselines"][profile.infrastructure_role]
    device_bounds = config["variation"]["health_device_multiplier"]
    measurement_bounds = config["variation"]["health_measurement_multiplier"]
    precision = config["metric_precision"]["health"]

    common: dict[str, float] = {}
    for field in ("cpu_pct", "memory_pct", "temperature_c"):
        device = _scale(
            stable_float(seed, f"device-health-{field}", profile.device_id),
            device_bounds,
        )
        measurement = _scale(
            stable_float(
                seed,
                f"measurement-health-{field}",
                profile.device_id,
                sequence_number,
            ),
            measurement_bounds,
        )
        value = baseline[field] * device * measurement
        common[field] = round(_clamp(value, health["common_ranges"][field]), precision)

    signal_field: str | None = None
    signal_value: float | None = None
    if profile.is_subscriber_edge:
        technology = config["technologies"][profile.technology]
        signal_field = technology["health_signal_field"]
        variation_bounds = technology["health_signal_variation"]
        stable_variation = _scale(
            stable_float(seed, "device-health-signal", profile.device_id),
            variation_bounds,
        )
        measurement_variation = _scale(
            stable_float(
                seed,
                "measurement-health-signal",
                profile.device_id,
                sequence_number,
            ),
            variation_bounds,
        )
        value = (
            technology["health_signal_baseline"]
            + 0.7 * stable_variation
            + 0.3 * measurement_variation
        )
        signal_value = round(
            _clamp(value, technology["health_signal_range"]), precision
        )

    return DeviceHealthMetrics(
        cpu_pct=common["cpu_pct"],
        memory_pct=common["memory_pct"],
        temperature_c=common["temperature_c"],
        technology_signal_field=signal_field,
        technology_signal_value=signal_value,
    )
