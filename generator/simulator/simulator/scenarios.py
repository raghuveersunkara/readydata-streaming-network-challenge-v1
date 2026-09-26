from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from .metrics import (
    ConnectivityMetrics,
    DeviceHealthMetrics,
    DeviceProfile,
    ThroughputMetrics,
    stable_float,
)


class ScenarioError(ValueError):
    """Raised when the inventory cannot satisfy the configured scenario design."""


class IncidentPhase(str, Enum):
    HEALTHY_WARMUP = "healthy_warmup"
    CONGESTED = "congested"
    OFFLINE = "offline"
    RECOVERING = "recovering"
    HEALTHY = "healthy"


@dataclass(frozen=True)
class ScenarioCohorts:
    peer_outlier_device_id: str
    degraded_geographic_device_ids: frozenset[str]
    high_performing_geographic_device_ids: frozenset[str]
    provider_technology_device_ids: frozenset[str]
    provider_id: str
    technology: str
    shared_aggregation_device_id: str
    shared_incident_device_ids: frozenset[str]
    degraded_anchor_device_id: str
    high_performing_anchor_device_id: str

    @property
    def persistent_device_ids(self) -> frozenset[str]:
        return frozenset(
            {
                self.peer_outlier_device_id,
                *self.degraded_geographic_device_ids,
                *self.high_performing_geographic_device_ids,
                *self.provider_technology_device_ids,
            }
        )


@dataclass(frozen=True)
class IncidentBoundary:
    index: int
    cycle_index: int
    due_timestamp: datetime
    previous_state: str
    new_state: str
    reason: str
    event_kind: str
    severity: str


def _distance_km(first: DeviceProfile, second: DeviceProfile) -> float:
    radius_km = 6371.0088
    latitude_1 = math.radians(first.latitude)
    latitude_2 = math.radians(second.latitude)
    latitude_delta = latitude_2 - latitude_1
    longitude_delta = math.radians(second.longitude - first.longitude)
    haversine = (
        math.sin(latitude_delta / 2) ** 2
        + math.cos(latitude_1)
        * math.cos(latitude_2)
        * math.sin(longitude_delta / 2) ** 2
    )
    return radius_km * 2 * math.asin(math.sqrt(haversine))


def _rank(seed: int, label: str, value: str) -> tuple[float, str]:
    return stable_float(seed, "scenario-selection", label, value), value


def select_scenario_cohorts(
    profiles: list[DeviceProfile], seed: int, config: dict[str, Any]
) -> ScenarioCohorts:
    selection = config["scenarios"]["selection"]
    by_id = {profile.device_id: profile for profile in profiles}
    access = [profile for profile in profiles if profile.is_subscriber_edge]

    downstream_by_aggregation: dict[str, list[DeviceProfile]] = {}
    for profile in access:
        if profile.upstream_device_id is None:
            raise ScenarioError(f"access device {profile.device_id} lacks an aggregation")
        downstream_by_aggregation.setdefault(profile.upstream_device_id, []).append(profile)

    aggregation_candidates = [
        aggregation_id
        for aggregation_id, downstream in downstream_by_aggregation.items()
        if any(profile.critical_infrastructure for profile in downstream)
    ]
    if not aggregation_candidates:
        raise ScenarioError("no aggregation router has a critical downstream site")
    aggregation_id = min(
        aggregation_candidates,
        key=lambda value: _rank(seed, "shared-aggregation", value),
    )
    aggregation = by_id.get(aggregation_id)
    if aggregation is None or aggregation.infrastructure_role != "aggregation":
        raise ScenarioError(f"dependency target {aggregation_id} is not an aggregation router")
    shared = frozenset(
        profile.device_id for profile in downstream_by_aggregation[aggregation_id]
    )

    provider_groups: dict[tuple[str, str], list[DeviceProfile]] = {}
    for profile in access:
        provider_groups.setdefault((profile.provider_id, profile.technology), []).append(profile)
    minimum_provider_size = selection["provider_technology_minimum_size"]
    provider_candidates = [
        key
        for key, members in provider_groups.items()
        if key[0] != aggregation.provider_id
        and len(members) >= minimum_provider_size
        and shared.isdisjoint(profile.device_id for profile in members)
    ]
    if not provider_candidates:
        raise ScenarioError("no disjoint provider/technology cohort meets the minimum size")
    provider_id, technology = min(
        provider_candidates,
        key=lambda key: _rank(seed, "provider-technology", "|".join(key)),
    )
    provider_technology = frozenset(
        profile.device_id for profile in provider_groups[(provider_id, technology)]
    )

    used = set(shared | provider_technology)
    geographic_size = selection["geographic_cluster_size"]
    available = [profile for profile in access if profile.device_id not in used]
    if len(available) < geographic_size * 2 + 1:
        raise ScenarioError("not enough disjoint access devices for geographic scenarios")
    degraded_anchor = min(
        available,
        key=lambda profile: _rank(seed, "degraded-anchor", profile.device_id),
    )
    degraded_members = sorted(
        available,
        key=lambda profile: (
            _distance_km(degraded_anchor, profile),
            profile.device_id,
        ),
    )[:geographic_size]
    degraded = frozenset(profile.device_id for profile in degraded_members)
    used.update(degraded)

    high_anchor_candidates = [
        profile
        for profile in access
        if profile.device_id not in used
        and _distance_km(degraded_anchor, profile)
        >= selection["minimum_anchor_separation_km"]
    ]
    if not high_anchor_candidates:
        raise ScenarioError("no high-performing geographic anchor meets separation")
    high_anchor = min(
        high_anchor_candidates,
        key=lambda profile: _rank(seed, "high-performing-anchor", profile.device_id),
    )
    remaining = [profile for profile in access if profile.device_id not in used]
    high_members = sorted(
        remaining,
        key=lambda profile: (
            _distance_km(high_anchor, profile),
            profile.device_id,
        ),
    )[:geographic_size]
    if len(high_members) != geographic_size:
        raise ScenarioError("not enough devices for the high-performing geographic cluster")
    high = frozenset(profile.device_id for profile in high_members)
    used.update(high)

    peer_minimum = selection["peer_group_minimum_size"]
    model_counts: dict[str, int] = {}
    for profile in access:
        model_counts[profile.device_model] = model_counts.get(profile.device_model, 0) + 1
    peer_candidates = [
        profile
        for profile in access
        if profile.device_id not in used
        and not profile.critical_infrastructure
        and model_counts[profile.device_model] >= peer_minimum
    ]
    if not peer_candidates:
        raise ScenarioError("no disjoint device has a sufficiently large model peer group")
    peer = min(
        peer_candidates,
        key=lambda profile: _rank(seed, "peer-outlier", profile.device_id),
    )

    result = ScenarioCohorts(
        peer_outlier_device_id=peer.device_id,
        degraded_geographic_device_ids=degraded,
        high_performing_geographic_device_ids=high,
        provider_technology_device_ids=provider_technology,
        provider_id=provider_id,
        technology=technology,
        shared_aggregation_device_id=aggregation_id,
        shared_incident_device_ids=shared,
        degraded_anchor_device_id=degraded_anchor.device_id,
        high_performing_anchor_device_id=high_anchor.device_id,
    )
    populations = [
        {result.peer_outlier_device_id},
        set(result.degraded_geographic_device_ids),
        set(result.high_performing_geographic_device_ids),
        set(result.provider_technology_device_ids),
        set(result.shared_incident_device_ids),
    ]
    if sum(len(population) for population in populations) != len(
        set().union(*populations)
    ):
        raise ScenarioError("scenario cohort selection produced overlapping populations")
    return result


class ScenarioEngine:
    def __init__(
        self,
        profiles: list[DeviceProfile],
        seed: int,
        run_id: uuid.UUID,
        start_time: datetime,
        config: dict[str, Any],
    ) -> None:
        self.config = config
        self.start_time = start_time
        self.run_id = run_id
        self.cohorts = select_scenario_cohorts(profiles, seed, config)
        phases = config["scenarios"]["shared_incident"]["phase_durations_seconds"]
        self.cycle_seconds = sum(phases.values())
        self._boundary_offsets = (
            phases["healthy_warmup"],
            phases["healthy_warmup"] + phases["congested"],
            phases["healthy_warmup"] + phases["congested"] + phases["offline"],
            phases["healthy_warmup"]
            + phases["congested"]
            + phases["offline"]
            + phases["recovering"],
        )

    def phase_at(self, timestamp: datetime) -> IncidentPhase:
        elapsed = max(0.0, (timestamp - self.start_time).total_seconds())
        position = elapsed % self.cycle_seconds
        first, second, third, fourth = self._boundary_offsets
        if position < first:
            return IncidentPhase.HEALTHY_WARMUP
        if position < second:
            return IncidentPhase.CONGESTED
        if position < third:
            return IncidentPhase.OFFLINE
        if position < fourth:
            return IncidentPhase.RECOVERING
        return IncidentPhase.HEALTHY

    def should_suppress(self, profile: DeviceProfile, timestamp: datetime) -> bool:
        return (
            profile.device_id in self.cohorts.shared_incident_device_ids
            and self.phase_at(timestamp) is IncidentPhase.OFFLINE
        )

    def boundary(self, index: int) -> IncidentBoundary:
        if index < 0:
            raise ValueError("boundary index cannot be negative")
        cycle_index, transition_index = divmod(index, 4)
        transitions = (
            ("online", "degraded", "upstream_congestion", "congestion_started", "warning"),
            ("degraded", "offline", "upstream_outage", "outage_started", "critical"),
            ("offline", "degraded", "upstream_recovery", "recovery_started", "warning"),
            ("degraded", "online", "upstream_recovered", "recovered", "info"),
        )
        previous, new, reason, kind, severity = transitions[transition_index]
        offset = cycle_index * self.cycle_seconds + self._boundary_offsets[transition_index]
        return IncidentBoundary(
            index=index,
            cycle_index=cycle_index,
            due_timestamp=self.start_time + timedelta(seconds=offset),
            previous_state=previous,
            new_state=new,
            reason=reason,
            event_kind=kind,
            severity=severity,
        )

    def incident_id(self, cycle_index: int) -> uuid.UUID:
        return uuid.uuid5(
            self.run_id,
            f"network-incident|{self.cohorts.shared_aggregation_device_id}|{cycle_index}",
        )

    def apply(
        self,
        profile: DeviceProfile,
        metrics: ConnectivityMetrics | ThroughputMetrics | DeviceHealthMetrics,
        event_type: str,
        timestamp: datetime,
    ) -> ConnectivityMetrics | ThroughputMetrics | DeviceHealthMetrics:
        scenarios = self.config["scenarios"]
        device_id = profile.device_id
        if device_id == self.cohorts.peer_outlier_device_id:
            metrics = self._apply_persistent(metrics, "peer_outlier", profile)
        elif device_id in self.cohorts.degraded_geographic_device_ids:
            metrics = self._apply_persistent(metrics, "degraded_geographic", profile)
        elif device_id in self.cohorts.high_performing_geographic_device_ids:
            metrics = self._apply_persistent(metrics, "high_performing_geographic", profile)
        elif device_id in self.cohorts.provider_technology_device_ids:
            metrics = self._apply_persistent(metrics, "provider_technology", profile)

        phase = self.phase_at(timestamp)
        if device_id in self.cohorts.shared_incident_device_ids and phase in {
            IncidentPhase.CONGESTED,
            IncidentPhase.RECOVERING,
        }:
            fraction = (
                1.0
                if phase is IncidentPhase.CONGESTED
                else scenarios["shared_incident"]["recovery_effect_fraction"]
            )
            metrics = self._apply_shared_incident(metrics, fraction)
        elif (
            device_id == self.cohorts.shared_aggregation_device_id
            and isinstance(metrics, DeviceHealthMetrics)
            and phase in {IncidentPhase.CONGESTED, IncidentPhase.RECOVERING}
        ):
            fraction = scenarios["shared_incident"][
                "infrastructure_health_adverse_fraction"
            ]
            if phase is IncidentPhase.RECOVERING:
                fraction *= scenarios["shared_incident"]["recovery_effect_fraction"]
            metrics = self._move_common_health_toward_high(metrics, fraction)
        return metrics

    def _apply_persistent(
        self,
        metrics: ConnectivityMetrics | ThroughputMetrics | DeviceHealthMetrics,
        scenario: str,
        profile: DeviceProfile,
    ) -> ConnectivityMetrics | ThroughputMetrics | DeviceHealthMetrics:
        effect = self.config["scenarios"][scenario]
        precision = self.config["metric_precision"]
        if isinstance(metrics, ConnectivityMetrics):
            if "latency_multiplier" not in effect and "latency_add_ms" not in effect:
                return metrics
            return ConnectivityMetrics(
                latency_ms=round(
                    min(
                        60000.0,
                        metrics.latency_ms * effect.get("latency_multiplier", 1.0)
                        + effect.get("latency_add_ms", 0.0),
                    ),
                    precision["latency_ms"],
                ),
                jitter_ms=round(
                    min(
                        60000.0,
                        metrics.jitter_ms * effect.get("jitter_multiplier", 1.0)
                        + effect.get("jitter_add_ms", 0.0),
                    ),
                    precision["jitter_ms"],
                ),
                packet_loss_pct=round(
                    min(
                        100.0,
                        metrics.packet_loss_pct
                        * effect.get("packet_loss_multiplier", 1.0)
                        + effect.get("packet_loss_add_pct", 0.0),
                    ),
                    precision["packet_loss_pct"],
                ),
            )
        if isinstance(metrics, ThroughputMetrics):
            download = metrics.download_mbps * effect.get("download_multiplier", 1.0)
            upload = metrics.upload_mbps * effect.get("upload_multiplier", 1.0)
            if scenario == "high_performing_geographic":
                if (
                    profile.advertised_download_mbps is None
                    or profile.advertised_upload_mbps is None
                ):
                    raise ScenarioError("high-performing throughput requires advertised tiers")
                download = min(download, profile.advertised_download_mbps * 1.05)
                upload = min(upload, profile.advertised_upload_mbps * 1.05)
            return ThroughputMetrics(
                download_mbps=round(max(0.01, download), precision["download_mbps"]),
                upload_mbps=round(max(0.01, upload), precision["upload_mbps"]),
            )
        if scenario != "provider_technology" or metrics.technology_signal_value is None:
            return metrics
        adverse = self.config["technologies"][profile.technology]["health_signal_range"][0]
        fraction = effect["health_signal_adverse_fraction"]
        return replace(
            metrics,
            technology_signal_value=round(
                metrics.technology_signal_value
                + fraction * (adverse - metrics.technology_signal_value),
                precision["health"],
            ),
        )

    def _apply_shared_incident(
        self,
        metrics: ConnectivityMetrics | ThroughputMetrics | DeviceHealthMetrics,
        fraction: float,
    ) -> ConnectivityMetrics | ThroughputMetrics | DeviceHealthMetrics:
        effect = self.config["scenarios"]["shared_incident"]
        precision = self.config["metric_precision"]
        if isinstance(metrics, ConnectivityMetrics):
            return ConnectivityMetrics(
                latency_ms=round(
                    min(60000.0, metrics.latency_ms + effect["latency_add_ms"] * fraction),
                    precision["latency_ms"],
                ),
                jitter_ms=round(
                    min(60000.0, metrics.jitter_ms + effect["jitter_add_ms"] * fraction),
                    precision["jitter_ms"],
                ),
                packet_loss_pct=round(
                    min(
                        100.0,
                        metrics.packet_loss_pct + effect["packet_loss_add_pct"] * fraction,
                    ),
                    precision["packet_loss_pct"],
                ),
            )
        if isinstance(metrics, ThroughputMetrics):
            multiplier = 1.0 - (1.0 - effect["throughput_multiplier"]) * fraction
            return ThroughputMetrics(
                download_mbps=round(
                    max(0.01, metrics.download_mbps * multiplier),
                    precision["download_mbps"],
                ),
                upload_mbps=round(
                    max(0.01, metrics.upload_mbps * multiplier),
                    precision["upload_mbps"],
                ),
            )
        return metrics

    def _move_common_health_toward_high(
        self, metrics: DeviceHealthMetrics, fraction: float
    ) -> DeviceHealthMetrics:
        ranges = self.config["health"]["common_ranges"]
        precision = self.config["metric_precision"]["health"]

        def moved(field: str, value: float) -> float:
            return round(value + fraction * (ranges[field][1] - value), precision)

        return replace(
            metrics,
            cpu_pct=moved("cpu_pct", metrics.cpu_pct),
            memory_pct=moved("memory_pct", metrics.memory_pct),
            temperature_c=moved("temperature_c", metrics.temperature_c),
        )
