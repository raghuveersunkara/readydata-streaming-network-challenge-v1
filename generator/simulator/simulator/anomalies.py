from __future__ import annotations

import heapq
import json
import uuid
from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping

from .metrics import stable_float


class AnomalyAction(StrEnum):
    CLEAN = "clean"
    DUPLICATE = "duplicate"
    SEQUENCE_GAP = "sequence_gap"
    OUT_OF_ORDER = "out_of_order"
    LATE_ARRIVAL = "late_arrival"
    POISON = "poison"


ANOMALY_ACTIONS = (
    AnomalyAction.DUPLICATE,
    AnomalyAction.SEQUENCE_GAP,
    AnomalyAction.OUT_OF_ORDER,
    AnomalyAction.LATE_ARRIVAL,
    AnomalyAction.POISON,
)


@dataclass(frozen=True)
class PublicationRecord:
    event_type: str
    device_id: str
    device_session_id: str
    sequence_number: int
    topic: str
    payload: bytes

    @property
    def stream_key(self) -> tuple[str, str, str]:
        return (self.device_id, self.device_session_id, self.event_type)


class AnomalySelector:
    """Assign at most one stable data-quality action to a logical event."""

    def __init__(
        self,
        seed: int,
        run_id: uuid.UUID,
        rates: Mapping[str, float],
    ) -> None:
        self.seed = seed
        self.run_id = run_id
        self.rates = {
            action: float(rates[action.value]) for action in ANOMALY_ACTIONS
        }

    def select(
        self,
        event_type: str,
        device_id: str,
        sequence_number: int,
    ) -> AnomalyAction:
        selector = stable_float(
            self.seed,
            "data-quality-anomaly",
            self.run_id,
            event_type,
            device_id,
            sequence_number,
        )
        upper_bound = 0.0
        for action in ANOMALY_ACTIONS:
            upper_bound += self.rates[action]
            if selector < upper_bound:
                return action
        return AnomalyAction.CLEAN


def poison_payload(payload: bytes, strategy: str) -> bytes:
    if strategy != "truncate_final_byte":
        raise ValueError(f"unsupported poison strategy: {strategy}")
    if not payload:
        raise ValueError("cannot truncate an empty payload")
    poisoned = payload[:-1]
    try:
        json.loads(poisoned)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return poisoned
    raise ValueError("configured poison strategy unexpectedly produced valid JSON")


def late_delay_seconds(config: Mapping[str, Any], event_type: str) -> float:
    data_quality = config["data_quality"]
    late = data_quality["late_arrival"]
    cadence = config["event_families"][event_type]["base_interval_seconds"]
    return max(
        float(late["minimum_delay_seconds"]),
        float(late["cadence_multiplier"]) * float(cadence),
    )


@dataclass(order=True)
class _DelayedRecord:
    release_monotonic: float
    ordinal: int
    record: PublicationRecord = field(compare=False)


class PendingRecordManager:
    """Bound late and one-successor reorder state behind one runtime boundary."""

    def __init__(self, maximum_records: int) -> None:
        if maximum_records <= 0:
            raise ValueError("maximum pending records must be positive")
        self.maximum_records = maximum_records
        self.high_watermark = 0
        self._ordinal = 0
        self._late: list[_DelayedRecord] = []
        self._reordered: dict[tuple[str, str, str], PublicationRecord] = {}

    def __len__(self) -> int:
        return len(self._late) + len(self._reordered)

    def _reserve(self) -> None:
        if len(self) >= self.maximum_records:
            raise RuntimeError(
                f"data-quality pending-record cap exhausted: {self.maximum_records}"
            )

    def _update_high_watermark(self) -> None:
        self.high_watermark = max(self.high_watermark, len(self))

    def hold_late(self, record: PublicationRecord, release_monotonic: float) -> None:
        self._reserve()
        self._ordinal += 1
        heapq.heappush(
            self._late,
            _DelayedRecord(release_monotonic, self._ordinal, record),
        )
        self._update_high_watermark()

    def next_late_release(self) -> float | None:
        return self._late[0].release_monotonic if self._late else None

    def pop_due_late(self, now_monotonic: float) -> PublicationRecord | None:
        if not self._late or self._late[0].release_monotonic > now_monotonic:
            return None
        return heapq.heappop(self._late).record

    def hold_reordered(self, record: PublicationRecord) -> None:
        if record.stream_key in self._reordered:
            raise RuntimeError(f"reorder record already pending for {record.stream_key}")
        self._reserve()
        self._reordered[record.stream_key] = record
        self._update_high_watermark()

    def reordered_for(
        self, stream_key: tuple[str, str, str]
    ) -> PublicationRecord | None:
        return self._reordered.get(stream_key)

    def complete_reordered(self, stream_key: tuple[str, str, str]) -> None:
        del self._reordered[stream_key]

    def discard_all(self) -> Counter[str]:
        discarded: Counter[str] = Counter()
        if self._late:
            discarded[AnomalyAction.LATE_ARRIVAL.value] = len(self._late)
        if self._reordered:
            discarded[AnomalyAction.OUT_OF_ORDER.value] = len(self._reordered)
        self._late.clear()
        self._reordered.clear()
        return discarded
