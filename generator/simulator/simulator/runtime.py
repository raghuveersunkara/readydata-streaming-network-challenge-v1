from __future__ import annotations

import heapq
import logging
import math
import signal
import threading
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Any

import paho.mqtt.client as mqtt

from .anomalies import (
    AnomalyAction,
    AnomalySelector,
    PendingRecordManager,
    PublicationRecord,
    late_delay_seconds,
    poison_payload,
)
from .config import CONTROL_EVENT_TYPES, RuntimeSettings
from .events import (
    build_control_event,
    build_event,
    device_session_id,
    event_topic,
    serialize_event,
)
from .metrics import (
    DeviceProfile,
    generate_connectivity_metrics,
    generate_device_health_metrics,
    generate_throughput_metrics,
    stable_float,
)
from .scenarios import IncidentBoundary, ScenarioEngine


LOGGER = logging.getLogger("readydata.simulator")


@dataclass
class DeviceRuntime:
    profile: DeviceProfile
    session_id: uuid.UUID
    sequence_numbers: dict[str, int] = field(default_factory=dict)


@dataclass(order=True)
class ScheduledEvent:
    due_monotonic: float
    event_type: str
    device_id: str
    due_timestamp: datetime


class NetworkSimulator:
    def __init__(
        self,
        settings: RuntimeSettings,
        profiles: list[DeviceProfile],
        readiness_path: Path,
        *,
        mqtt_client: mqtt.Client | None = None,
        anomaly_selector: AnomalySelector | None = None,
        scenario_engine: ScenarioEngine | None = None,
    ) -> None:
        self.settings = settings
        self.profiles = profiles
        self.readiness_path = readiness_path
        self.connected = threading.Event()
        self.stop_requested = threading.Event()
        self._rephase_requested = threading.Event()
        self._rephase_requested.set()
        self.logical_count = 0
        self.logical_by_type: Counter[str] = Counter()
        self.published_count = 0
        self.published_by_type: Counter[str] = Counter()
        self.selected_by_action: Counter[str] = Counter()
        self.released_by_action: Counter[str] = Counter()
        self.discarded_by_action: Counter[str] = Counter()
        self.suppressed_by_type: Counter[str] = Counter()
        self.control_queue_high_watermark = 0
        self._next_summary = 0.0
        self._pending: deque[mqtt.MQTTMessageInfo] = deque()
        self._states = {
            profile.device_id: DeviceRuntime(
                profile=profile,
                session_id=device_session_id(settings.run_id, profile.device_id),
                sequence_numbers={
                    event_type: 1
                    for event_type in (*settings.event_types, *CONTROL_EVENT_TYPES)
                },
            )
            for profile in profiles
        }
        self._schedule: list[ScheduledEvent] = []
        self._schedule_started_monotonic: float | None = None
        self._next_boundary_index = 0
        self._control_queue: deque[PublicationRecord] = deque()
        data_quality = settings.config["data_quality"]
        self._anomaly_selector = anomaly_selector or AnomalySelector(
            settings.seed,
            settings.run_id,
            data_quality["anomaly_rates"],
        )
        self._anomaly_pending = PendingRecordManager(
            data_quality["max_pending_records"]
        )
        self._scenario_engine = scenario_engine
        self._client = mqtt_client if mqtt_client is not None else self._create_client()

    def _create_client(self) -> mqtt.Client:
        client_id = f"readydata-network-simulator-{str(self.settings.run_id)[:8]}"
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        mqtt_config = self.settings.config["mqtt"]
        client.reconnect_delay_set(
            min_delay=mqtt_config["reconnect_min_seconds"],
            max_delay=mqtt_config["reconnect_max_seconds"],
        )
        client.max_inflight_messages_set(mqtt_config["max_inflight_messages"])
        client.max_queued_messages_set(mqtt_config["max_queued_messages"])
        return client

    def _on_connect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        if reason_code == 0:
            self.connected.set()
            self.readiness_path.parent.mkdir(parents=True, exist_ok=True)
            self.readiness_path.touch()
            LOGGER.info("connected to MQTT broker and marked simulator ready")
        else:
            self.connected.clear()
            self.readiness_path.unlink(missing_ok=True)
            LOGGER.warning("MQTT connection rejected: %s", reason_code)

    def _on_disconnect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        self.connected.clear()
        self._rephase_requested.set()
        self.readiness_path.unlink(missing_ok=True)
        if not self.stop_requested.is_set():
            LOGGER.warning("MQTT disconnected (%s); publishing paused", reason_code)

    def _request_stop(self, signum: int, _frame: FrameType | None) -> None:
        LOGGER.info("received signal %s; beginning graceful shutdown", signum)
        self.stop_requested.set()

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)

    def _eligible_profiles(self, event_type: str) -> list[DeviceProfile]:
        population = self.settings.config["event_families"][event_type]["population"]
        if population == "all":
            return self.profiles
        return [profile for profile in self.profiles if profile.is_subscriber_edge]

    def _initialize_schedule(self) -> None:
        started_monotonic = time.monotonic()
        self._schedule_started_monotonic = started_monotonic
        for event_type in self.settings.event_types:
            interval = self.settings.effective_interval_seconds(event_type)
            for profile in self._eligible_profiles(event_type):
                schedule_parts = (
                    ("schedule-offset", profile.device_id)
                    if event_type == "connectivity_metric"
                    else ("schedule-offset", event_type, profile.device_id)
                )
                offset = stable_float(self.settings.seed, *schedule_parts) * interval
                heapq.heappush(
                    self._schedule,
                    ScheduledEvent(
                        due_monotonic=started_monotonic + offset,
                        event_type=event_type,
                        device_id=profile.device_id,
                        due_timestamp=self.settings.start_time
                        + timedelta(seconds=offset),
                    ),
                )

    def _remove_acknowledged(self) -> None:
        if self._pending:
            self._pending = deque(
                info for info in self._pending if not info.is_published()
            )

    def _advance_past_missed_slots(
        self,
        scheduled: ScheduledEvent,
        now: float,
    ) -> ScheduledEvent:
        interval = self.settings.effective_interval_seconds(scheduled.event_type)
        if now <= scheduled.due_monotonic:
            return scheduled
        lateness = now - scheduled.due_monotonic
        tolerance = interval * self.settings.config["scheduling"][
            "lateness_tolerance_fraction"
        ]
        if lateness <= tolerance:
            return scheduled
        missed = math.floor(lateness / interval) + 1
        delta = interval * missed
        return ScheduledEvent(
            due_monotonic=scheduled.due_monotonic + delta,
            event_type=scheduled.event_type,
            device_id=scheduled.device_id,
            due_timestamp=scheduled.due_timestamp + timedelta(seconds=delta),
        )

    def _rephase_overdue_schedule(self) -> None:
        if not self._rephase_requested.is_set():
            return
        now = time.monotonic()
        skipped_slots = 0
        updated: list[ScheduledEvent] = []
        for scheduled in self._schedule:
            interval = self.settings.effective_interval_seconds(scheduled.event_type)
            if scheduled.due_monotonic <= now:
                skipped = math.floor((now - scheduled.due_monotonic) / interval) + 1
                delta = skipped * interval
                scheduled = ScheduledEvent(
                    due_monotonic=scheduled.due_monotonic + delta,
                    event_type=scheduled.event_type,
                    device_id=scheduled.device_id,
                    due_timestamp=scheduled.due_timestamp + timedelta(seconds=delta),
                )
                skipped_slots += skipped
            updated.append(scheduled)
        self._schedule = updated
        heapq.heapify(self._schedule)
        self._rephase_requested.clear()
        if skipped_slots:
            LOGGER.info(
                "re-phased overdue schedules and skipped %d unpublished slots",
                skipped_slots,
            )

    def _reschedule(self, scheduled: ScheduledEvent, now: float) -> None:
        interval = self.settings.effective_interval_seconds(scheduled.event_type)
        next_monotonic = scheduled.due_monotonic + interval
        next_timestamp = scheduled.due_timestamp + timedelta(seconds=interval)
        if next_monotonic <= now:
            skipped = math.floor((now - next_monotonic) / interval) + 1
            delta = skipped * interval
            next_monotonic += delta
            next_timestamp += timedelta(seconds=delta)
        heapq.heappush(
            self._schedule,
            ScheduledEvent(
                next_monotonic,
                scheduled.event_type,
                scheduled.device_id,
                next_timestamp,
            ),
        )

    def _metrics_for(
        self,
        state: DeviceRuntime,
        event_type: str,
        sequence_number: int,
        event_timestamp: datetime,
    ):
        if event_type == "connectivity_metric":
            metrics = generate_connectivity_metrics(
                state.profile, sequence_number, self.settings.seed, self.settings.config
            )
        elif event_type == "throughput_test":
            metrics = generate_throughput_metrics(
                state.profile, sequence_number, self.settings.seed, self.settings.config
            )
        elif event_type == "device_health":
            metrics = generate_device_health_metrics(
                state.profile, sequence_number, self.settings.seed, self.settings.config
            )
        else:
            raise ValueError(f"unsupported event type: {event_type}")
        if self._scenario_engine is None:
            return metrics
        return self._scenario_engine.apply(
            state.profile, metrics, event_type, event_timestamp
        )

    def _prepare_record(self, scheduled: ScheduledEvent) -> PublicationRecord:
        state = self._states[scheduled.device_id]
        sequence_number = state.sequence_numbers[scheduled.event_type]
        metrics = self._metrics_for(
            state,
            scheduled.event_type,
            sequence_number,
            scheduled.due_timestamp,
        )
        event = build_event(
            profile=state.profile,
            metrics=metrics,
            event_type=scheduled.event_type,
            session_id=state.session_id,
            sequence_number=sequence_number,
            event_timestamp=scheduled.due_timestamp,
            config=self.settings.config,
        )
        return PublicationRecord(
            event_type=scheduled.event_type,
            device_id=state.profile.device_id,
            device_session_id=str(state.session_id),
            sequence_number=sequence_number,
            topic=event_topic(
                state.profile,
                scheduled.event_type,
                self.settings.config,
            ),
            payload=serialize_event(event),
        )

    def _control_record(
        self,
        profile: DeviceProfile,
        event_type: str,
        payload: dict[str, str],
        event_timestamp: datetime,
    ) -> PublicationRecord:
        state = self._states[profile.device_id]
        sequence_number = state.sequence_numbers[event_type]
        event = build_control_event(
            profile=profile,
            payload=payload,
            event_type=event_type,
            session_id=state.session_id,
            sequence_number=sequence_number,
            event_timestamp=event_timestamp,
            config=self.settings.config,
        )
        state.sequence_numbers[event_type] += 1
        self.logical_count += 1
        self.logical_by_type[event_type] += 1
        return PublicationRecord(
            event_type=event_type,
            device_id=profile.device_id,
            device_session_id=str(state.session_id),
            sequence_number=sequence_number,
            topic=event_topic(profile, event_type, self.settings.config),
            payload=serialize_event(event),
        )

    def _records_for_boundary(self, boundary: IncidentBoundary) -> list[PublicationRecord]:
        if self._scenario_engine is None:
            raise RuntimeError("scenario engine is required for control events")
        cohorts = self._scenario_engine.cohorts
        router = self._states[cohorts.shared_aggregation_device_id].profile
        records = [
            self._control_record(
                router,
                "infrastructure_event",
                {
                    "incident_id": str(
                        self._scenario_engine.incident_id(boundary.cycle_index)
                    ),
                    "event_kind": boundary.event_kind,
                    "severity": boundary.severity,
                },
                boundary.due_timestamp,
            )
        ]
        for device_id in sorted(cohorts.shared_incident_device_ids):
            profile = self._states[device_id].profile
            records.append(
                self._control_record(
                    profile,
                    "connectivity_state",
                    {
                        "previous_state": boundary.previous_state,
                        "new_state": boundary.new_state,
                        "reason": boundary.reason,
                    },
                    boundary.due_timestamp,
                )
            )
        return records

    def _queue_due_control(self, now_monotonic: float) -> None:
        if self._scenario_engine is None:
            return
        if self._schedule_started_monotonic is None:
            raise RuntimeError("periodic schedule must be initialized before control events")
        configured_maximum = self.settings.config["scenarios"]["shared_incident"][
            "max_control_queue_records"
        ]
        one_cycle = 4 * (
            1 + len(self._scenario_engine.cohorts.shared_incident_device_ids)
        )
        maximum = min(configured_maximum, one_cycle)
        while True:
            boundary = self._scenario_engine.boundary(self._next_boundary_index)
            due = self._schedule_started_monotonic + (
                boundary.due_timestamp - self.settings.start_time
            ).total_seconds()
            if due > now_monotonic:
                return
            required = 1 + len(self._scenario_engine.cohorts.shared_incident_device_ids)
            if len(self._control_queue) + required > maximum:
                raise RuntimeError(
                    "control-event reconnect queue exhausted before MQTT recovered"
                )
            self._control_queue.extend(self._records_for_boundary(boundary))
            self.control_queue_high_watermark = max(
                self.control_queue_high_watermark,
                len(self._control_queue),
            )
            self._next_boundary_index += 1

    def _publish_next_control(self) -> bool:
        if not self._control_queue:
            return False
        if not self._publish_record(self._control_queue[0]):
            return False
        self._control_queue.popleft()
        return True

    def _publish_record(self, record: PublicationRecord) -> bool:
        mqtt_config = self.settings.config["mqtt"]
        info = self._client.publish(
            record.topic,
            payload=record.payload,
            qos=mqtt_config["qos"],
            retain=mqtt_config["retain"],
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.warning(
                "MQTT publish was not accepted for %s/%s: rc=%s",
                record.device_id,
                record.event_type,
                info.rc,
            )
            return False
        self._pending.append(info)
        self.published_count += 1
        self.published_by_type[record.event_type] += 1
        return True

    def _complete_logical_event(
        self,
        record: PublicationRecord,
        action: AnomalyAction,
    ) -> None:
        state = self._states[record.device_id]
        state.sequence_numbers[record.event_type] += 1
        self.logical_count += 1
        self.logical_by_type[record.event_type] += 1
        self.selected_by_action[action.value] += 1

    def _publish_scheduled(
        self,
        scheduled: ScheduledEvent,
        now_monotonic: float,
    ) -> bool:
        record = self._prepare_record(scheduled)
        held = self._anomaly_pending.reordered_for(record.stream_key)
        if held is not None:
            if not self._publish_record(record):
                return False
            self._complete_logical_event(record, AnomalyAction.CLEAN)
            if self._publish_record(held):
                self._anomaly_pending.complete_reordered(record.stream_key)
                self.released_by_action[AnomalyAction.OUT_OF_ORDER.value] += 1
            else:
                LOGGER.warning(
                    "reordered event remains pending after successor publication: %s/%s/%d",
                    held.device_id,
                    held.event_type,
                    held.sequence_number,
                )
            return True

        action = self._anomaly_selector.select(
            record.event_type,
            record.device_id,
            record.sequence_number,
        )
        if action is AnomalyAction.SEQUENCE_GAP:
            self._complete_logical_event(record, action)
            return True
        if action is AnomalyAction.OUT_OF_ORDER:
            self._anomaly_pending.hold_reordered(record)
            self._complete_logical_event(record, action)
            return True
        if action is AnomalyAction.LATE_ARRIVAL:
            self._anomaly_pending.hold_late(
                record,
                now_monotonic
                + late_delay_seconds(self.settings.config, record.event_type),
            )
            self._complete_logical_event(record, action)
            return True
        if action is AnomalyAction.POISON:
            record = PublicationRecord(
                event_type=record.event_type,
                device_id=record.device_id,
                device_session_id=record.device_session_id,
                sequence_number=record.sequence_number,
                topic=record.topic,
                payload=poison_payload(
                    record.payload,
                    self.settings.config["data_quality"]["poison_strategy"],
                ),
            )
        if not self._publish_record(record):
            return False
        if action is AnomalyAction.DUPLICATE and not self._publish_record(record):
            LOGGER.warning(
                "selected duplicate accepted only once: %s/%s/%d",
                record.device_id,
                record.event_type,
                record.sequence_number,
            )
        self._complete_logical_event(record, action)
        return True

    def _publish_due_late(self, now_monotonic: float) -> bool:
        record = self._anomaly_pending.pop_due_late(now_monotonic)
        if record is None:
            return False
        if self._publish_record(record):
            self.released_by_action[AnomalyAction.LATE_ARRIVAL.value] += 1
            return True
        self._anomaly_pending.hold_late(record, now_monotonic + 0.05)
        return False

    def _log_summary_if_due(self, now: float) -> None:
        if now < self._next_summary:
            return
        pending = sum(not info.is_published() for info in self._pending)
        LOGGER.info(
            "publication summary: logical=%d published=%d by_type=%s actions=%s "
            "released=%s discarded=%s pending_qos1=%d pending_anomalies=%d "
            "anomaly_high_water=%d suppressed=%s pending_control=%d "
            "control_high_water=%d rate_multiplier=%s",
            self.logical_count,
            self.published_count,
            dict(sorted(self.published_by_type.items())),
            dict(sorted(self.selected_by_action.items())),
            dict(sorted(self.released_by_action.items())),
            dict(sorted(self.discarded_by_action.items())),
            pending,
            len(self._anomaly_pending),
            self._anomaly_pending.high_watermark,
            dict(sorted(self.suppressed_by_type.items())),
            len(self._control_queue),
            self.control_queue_high_watermark,
            self.settings.rate_multiplier,
        )
        self._next_summary = now + self.settings.config["logging"][
            "summary_interval_seconds"
        ]

    def _drain_pending(self) -> None:
        deadline = time.monotonic() + self.settings.config["mqtt"][
            "shutdown_drain_seconds"
        ]
        for info in list(self._pending):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                info.wait_for_publish(timeout=remaining)
            except (RuntimeError, ValueError):
                break
        self._remove_acknowledged()
        if self._pending:
            LOGGER.warning("shutdown ended with %d unacknowledged messages", len(self._pending))

    def run(self) -> int:
        self._install_signal_handlers()
        self.readiness_path.unlink(missing_ok=True)
        self._initialize_schedule()
        LOGGER.info(
            "starting simulator: devices=%d streams=%d event_types=%s seed=%d run_id=%s",
            len(self.profiles),
            len(self._schedule),
            ",".join(self.settings.event_types),
            self.settings.seed,
            self.settings.run_id,
        )
        mqtt_config = self.settings.config["mqtt"]
        self._client.connect_async(
            self.settings.mqtt_host,
            self.settings.mqtt_port,
            keepalive=mqtt_config["keepalive_seconds"],
        )
        self._client.loop_start()
        self._next_summary = time.monotonic() + self.settings.config["logging"][
            "summary_interval_seconds"
        ]

        try:
            while not self.stop_requested.is_set():
                now = time.monotonic()
                self._queue_due_control(now)
                if (
                    self.settings.max_events is not None
                    and self.published_count >= self.settings.max_events
                ):
                    break
                if not self.connected.wait(timeout=0.2):
                    continue

                self._rephase_overdue_schedule()
                self._remove_acknowledged()
                max_queued = self.settings.config["mqtt"]["max_queued_messages"]
                if len(self._pending) >= max_queued:
                    self.stop_requested.wait(0.01)
                    continue

                if self._control_queue:
                    if not self._publish_next_control():
                        self.stop_requested.wait(0.05)
                    self._log_summary_if_due(now)
                    continue

                now = time.monotonic()
                next_late = self._anomaly_pending.next_late_release()
                if next_late is not None and next_late <= now:
                    if not self._publish_due_late(now):
                        self.stop_requested.wait(0.05)
                    self._log_summary_if_due(now)
                    continue

                scheduled = self._schedule[0]
                next_due = scheduled.due_monotonic
                if next_late is not None:
                    next_due = min(next_due, next_late)
                wait_seconds = next_due - now
                if wait_seconds > 0:
                    self.stop_requested.wait(min(wait_seconds, 0.2))
                    continue
                if scheduled.due_monotonic > now:
                    continue
                if not self.connected.is_set():
                    continue

                scheduled = heapq.heappop(self._schedule)
                now = time.monotonic()
                scheduled = self._advance_past_missed_slots(scheduled, now)
                if scheduled.due_monotonic > now:
                    heapq.heappush(self._schedule, scheduled)
                    continue
                profile = self._states[scheduled.device_id].profile
                if (
                    self._scenario_engine is not None
                    and self._scenario_engine.should_suppress(
                        profile, scheduled.due_timestamp
                    )
                ):
                    self.suppressed_by_type[scheduled.event_type] += 1
                    self._reschedule(scheduled, now)
                    self._log_summary_if_due(now)
                    continue
                if self._publish_scheduled(scheduled, now):
                    self._reschedule(scheduled, now)
                else:
                    heapq.heappush(self._schedule, scheduled)
                    self.stop_requested.wait(0.05)
                self._log_summary_if_due(now)
        finally:
            self.readiness_path.unlink(missing_ok=True)
            discarded = self._anomaly_pending.discard_all()
            self.discarded_by_action.update(discarded)
            if discarded:
                LOGGER.warning(
                    "discarded pending anomaly records during shutdown: %s",
                    dict(sorted(discarded.items())),
                )
            if self._control_queue:
                LOGGER.warning(
                    "discarded %d unpublished control events during shutdown",
                    len(self._control_queue),
                )
                self._control_queue.clear()
            self._drain_pending()
            try:
                self._client.disconnect()
            finally:
                self._client.loop_stop()
            LOGGER.info(
                "simulator stopped: logical=%d published=%d by_type=%s actions=%s "
                "released=%s discarded=%s anomaly_high_water=%d suppressed=%s "
                "control_high_water=%d",
                self.logical_count,
                self.published_count,
                dict(sorted(self.published_by_type.items())),
                dict(sorted(self.selected_by_action.items())),
                dict(sorted(self.released_by_action.items())),
                dict(sorted(self.discarded_by_action.items())),
                self._anomaly_pending.high_watermark,
                dict(sorted(self.suppressed_by_type.items())),
                self.control_queue_high_watermark,
            )
        return 0
