"""Delivery guarantees of the consume loop (src/pipeline.py).

The fakes append to one shared `events` list, so tests can assert on ordering:
the core rule is that an offset commit never happens before the Iceberg write
of the rows it covers.
"""
from confluent_kafka import KafkaError, KafkaException

from src.pipeline import (
    FLUSH_RETRY_BACKOFF_MAX_SEC, LoopState, commit_offsets, consume_loop, detect_skipped_offsets,
    on_assign, on_lost, on_revoke,
)


class FakeError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class FakeMessage:
    def __init__(self, error_code=None):
        self._error = FakeError(error_code) if error_code is not None else None

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, events, messages=None, commit_errors=None):
        self.events = events
        self.messages = list(messages or [])
        self.commit_errors = list(commit_errors or [])

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, asynchronous):
        if self.commit_errors:
            code = self.commit_errors.pop(0)
            if code is not None:
                self.events.append("commit_failed")
                raise KafkaException(KafkaError(code))
        self.events.append("commit")

    def assignment(self):
        return ["p0"]

    def pause(self, partitions):
        self.events.append("pause")

    def resume(self, partitions):
        self.events.append("resume")

    def close(self):
        self.events.append("close")


class FakeIngestor:
    """Buffers one row per record; flush() results are scripted (default: succeed)."""

    def __init__(self, events, flush_results=None):
        self.events = events
        self.flush_results = list(flush_results or [])
        self.buffered = 0

    def process_record(self, msg):
        self.buffered += 1
        self.events.append("process")

    def total_buffered(self):
        return self.buffered

    def flush(self):
        ok = self.flush_results.pop(0) if self.flush_results else True
        self.events.append("flush_ok" if ok else "flush_failed")
        if ok:
            self.buffered = 0
        return ok

    def discard_buffers(self):
        self.buffered = 0
        self.events.append("discard")


class Clock:
    """Advances one second per call, so loop iterations move time forward."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        self.now += 1.0
        return self.now


def run(consumer, ingestor, iterations, batch_size=1, flush_interval=5.0):
    remaining = iter(range(iterations))
    consume_loop(consumer, ingestor, lambda: next(remaining, None) is not None,
                 batch_size, flush_interval, clock=Clock())


def test_offsets_are_committed_only_after_a_successful_write():
    events = []
    run(FakeConsumer(events, [FakeMessage()]), FakeIngestor(events), iterations=1)
    assert events[:3] == ["process", "flush_ok", "commit"]


def test_failed_write_pauses_consumption_and_never_commits():
    events = []
    run(FakeConsumer(events, [FakeMessage()]), FakeIngestor(events, [False, False, False]), iterations=3)
    loop_events = events[:events.index("close")]
    assert "commit" not in loop_events[:loop_events.index("flush_failed") + 1]
    assert loop_events.count("pause") == 1  # paused once, not on every retry


def test_retries_back_off_and_resume_after_recovery():
    events = []
    ingestor = FakeIngestor(events, [False, True])
    # Clock ticks 1 s per call (2 calls per iteration); the retry is due 5 s after the failure.
    run(FakeConsumer(events, [FakeMessage()]), ingestor, iterations=6, flush_interval=5.0)
    assert events.count("flush_failed") == 1  # no retry before the backoff delay passed
    first_ok = events.index("flush_ok")
    assert events[first_ok:first_ok + 3] == ["flush_ok", "commit", "resume"]


class RecordingIngestor(FakeIngestor):
    """Records the clock time of every flush attempt; every write fails."""

    def __init__(self, events, clock):
        super().__init__(events, flush_results=[False] * 1000)
        self.clock = clock
        self.attempts = []

    def flush(self):
        self.attempts.append(self.clock.now)
        return super().flush()


def test_retry_delay_doubles_and_is_capped():
    events, clock = [], Clock()
    ingestor = RecordingIngestor(events, clock)
    remaining = iter(range(300))
    consume_loop(FakeConsumer(events, [FakeMessage()]), ingestor,
                 lambda: next(remaining, None) is not None, 1, 5.0, clock=clock)
    gaps = [b - a for a, b in zip(ingestor.attempts, ingestor.attempts[1:-1])]  # last = final flush
    # The gap between attempts is the backoff delay: 5, 10, 20, 40, then capped at 60.
    assert [round(g) for g in gaps[:6]] == [5, 10, 20, 40, 60, 60]
    assert FLUSH_RETRY_BACKOFF_MAX_SEC == 60.0


def test_failed_commit_is_survivable_and_logged():
    events = []
    consumer = FakeConsumer(events, [FakeMessage(), FakeMessage()], commit_errors=[KafkaError._WAIT_COORD])
    run(consumer, FakeIngestor(events), iterations=2)  # must not raise
    assert events.count("commit_failed") == 1
    assert "commit" in events  # the next write's commit still happens


def test_commit_with_nothing_to_commit_is_not_an_error():
    events = []
    assert commit_offsets(FakeConsumer(events, commit_errors=[KafkaError._NO_OFFSET])) is True
    assert commit_offsets(FakeConsumer(events, commit_errors=[KafkaError._WAIT_COORD])) is False


def test_shutdown_flushes_commits_and_closes():
    events = []
    ingestor = FakeIngestor(events)
    ingestor.buffered = 3  # rows still buffered when the stop signal arrives
    run(FakeConsumer(events), ingestor, iterations=0)
    assert events == ["flush_ok", "commit", "close"]


def test_shutdown_with_failed_final_write_does_not_commit():
    events = []
    ingestor = FakeIngestor(events, [False])
    ingestor.buffered = 3
    run(FakeConsumer(events), ingestor, iterations=0)
    assert events == ["flush_failed", "close"]  # rows are re-read after restart


def test_kafka_errors_are_not_processed_as_records():
    events = []
    messages = [FakeMessage(KafkaError._PARTITION_EOF), FakeMessage(KafkaError._TRANSPORT)]
    run(FakeConsumer(events, messages), FakeIngestor(events), iterations=2)
    assert "process" not in events


def test_batch_size_triggers_a_write_before_the_interval():
    events = []
    run(FakeConsumer(events, [FakeMessage(), FakeMessage()]), FakeIngestor(events),
        iterations=2, batch_size=2, flush_interval=3600)
    assert events[:3] == ["process", "process", "flush_ok"]


# --- Rebalances and skipped offsets -------------------------------------------------

class Partition:
    def __init__(self, topic, partition, offset=-1001):
        self.topic, self.partition, self.offset = topic, partition, offset


class RebalanceConsumer(FakeConsumer):
    """Adds committed offsets and retained offset ranges per partition."""

    def __init__(self, events, committed=None, retained=None, **kwargs):
        super().__init__(events, **kwargs)
        self._committed = committed or {}   # (topic, partition) -> committed offset
        self._retained = retained or {}     # (topic, partition) -> (oldest, newest)

    def committed(self, partitions, timeout):
        return [Partition(p.topic, p.partition, self._committed.get((p.topic, p.partition), -1001))
                for p in partitions]

    def get_watermark_offsets(self, tp, timeout, cached):
        return self._retained[(tp.topic, tp.partition)]

    def pause(self, partitions):
        self.events.append(("pause", len(partitions)))


def test_revoke_writes_and_commits_while_we_still_own_the_partitions():
    events = []
    ingestor = FakeIngestor(events)
    ingestor.buffered = 3
    on_revoke(LoopState(), ingestor, FakeConsumer(events), ["p0"])
    assert events == ["flush_ok", "commit"]


def test_revoke_with_failed_write_discards_buffer_and_never_commits():
    # Uncommitted rows are re-read by the partitions' next owner; writing them later would duplicate.
    events = []
    ingestor = FakeIngestor(events, [False])
    ingestor.buffered = 3
    on_revoke(LoopState(), ingestor, FakeConsumer(events), ["p0"])
    assert events == ["flush_failed", "discard"]


def test_revoke_with_nothing_buffered_does_nothing():
    events = []
    on_revoke(LoopState(), FakeIngestor(events), FakeConsumer(events), ["p0"])
    assert events == []


def test_lost_partitions_discard_the_buffer_without_writing():
    events = []
    ingestor = FakeIngestor(events)
    ingestor.buffered = 3
    on_lost(LoopState(), ingestor, FakeConsumer(events), ["p0"])
    assert events == ["discard"]


def test_partitions_assigned_while_paused_are_paused_too():
    events = []
    consumer = RebalanceConsumer(events, retained={("network.connectivity", 0): (0, 10)})
    on_assign(LoopState(paused=True), consumer, [Partition("network.connectivity", 0)])
    assert ("pause", 1) in events


def test_partitions_assigned_normally_are_not_paused():
    events = []
    consumer = RebalanceConsumer(events, retained={("network.connectivity", 0): (0, 10)})
    on_assign(LoopState(), consumer, [Partition("network.connectivity", 0)])
    assert not any(isinstance(e, tuple) and e[0] == "pause" for e in events)


def test_offsets_deleted_by_retention_are_reported_as_data_loss(caplog):
    state = LoopState()
    consumer = RebalanceConsumer(
        [],
        committed={("network.connectivity", 0): 100, ("network.connectivity", 1): 700},
        retained={("network.connectivity", 0): (500, 900), ("network.connectivity", 1): (500, 900)},
    )
    skipped = detect_skipped_offsets(
        consumer, [Partition("network.connectivity", 0), Partition("network.connectivity", 1)], state)
    assert skipped == 400 and state.skipped_records == 400  # partition 1 (700 >= 500) is fine
    assert "DATA LOSS: network.connectivity[0]" in caplog.text and "400 records" in caplog.text


def test_first_start_without_committed_offsets_is_not_data_loss(caplog):
    state = LoopState()
    consumer = RebalanceConsumer([], retained={("network.state", 2): (5000, 9000)})
    assert detect_skipped_offsets(consumer, [Partition("network.state", 2)], state) == 0
    assert "DATA LOSS" not in caplog.text


def test_loop_and_callbacks_share_the_paused_state():
    events = []
    state = LoopState()
    run_consumer = FakeConsumer(events, [FakeMessage()])
    remaining = iter(range(1))
    consume_loop(run_consumer, FakeIngestor(events, [False, False]), lambda: next(remaining, None) is not None,
                 1, 5.0, clock=Clock(), state=state)
    assert state.paused is True  # a partition assigned now (on_assign) would be paused as well
