"""The consume -> buffer -> write -> commit loop, separated from process setup so the
delivery guarantees can be unit-tested with a fake consumer and a fake clock.

Delivery rule: Kafka offsets are committed only after the buffered rows were
written to Iceberg. A crash before the commit replays rows (at-least-once,
TRADEOFFS.md, "Delivery is at-least-once"); nothing is ever committed without being written.

Rebalances (partitions moving between consumers) are handled by the on_assign /
on_revoke / on_lost callbacks below, which main.py passes to consumer.subscribe().
"""
import logging
import time
from dataclasses import dataclass
from typing import Callable

from confluent_kafka import KafkaError, KafkaException

logger = logging.getLogger("ingestion_main")

FLUSH_RETRY_BACKOFF_MAX_SEC = 60.0
KAFKA_CALL_TIMEOUT_SEC = 10.0


@dataclass
class LoopState:
    """State shared by the consume loop and the rebalance callbacks."""
    paused: bool = False          # consumption paused because writes are failing
    skipped_records: int = 0      # records Kafka deleted before we read them (see detect_skipped_offsets)


def commit_offsets(consumer) -> bool:
    """Commit after a successful Iceberg write. A failure here is survivable:
    the rows are already in Iceberg, so the only consequence is that they are
    re-read and written again after a restart or rebalance."""
    try:
        consumer.commit(asynchronous=False)
        return True
    except KafkaException as e:
        if e.args[0].code() == KafkaError._NO_OFFSET:
            return True  # nothing consumed since the last commit
        logger.warning(f"Offset commit failed; rows already written may be replayed: {e}")
        return False


def detect_skipped_offsets(consumer, partitions, state: LoopState) -> int:
    """Kafka deletes records after its retention period (about 2 h here), read or not.
    If a partition's committed offset is older than the oldest record Kafka still holds,
    records were deleted before we read them. The consumer would then jump ahead
    (auto.offset.reset=earliest) without any error, so report it loudly here."""
    try:
        committed = consumer.committed(partitions, timeout=KAFKA_CALL_TIMEOUT_SEC)
    except Exception as e:
        logger.warning(f"Could not check committed offsets for skipped data: {e}")
        return 0
    skipped = 0
    for tp in committed:
        if tp.offset < 0:
            continue  # nothing committed yet for this partition: a fresh start, not a gap
        try:
            oldest, _ = consumer.get_watermark_offsets(tp, timeout=KAFKA_CALL_TIMEOUT_SEC, cached=False)
        except Exception as e:
            logger.warning(f"Could not read retained offsets for {tp.topic}[{tp.partition}]: {e}")
            continue
        if tp.offset < oldest:
            lost = oldest - tp.offset
            skipped += lost
            logger.error(
                f"DATA LOSS: {tp.topic}[{tp.partition}] resumes at committed offset {tp.offset}, but Kafka "
                f"only retains offsets from {oldest}. {lost} records were deleted by retention before "
                f"they were read."
            )
    state.skipped_records += skipped
    return skipped


def on_assign(state: LoopState, consumer, partitions) -> None:
    """Partitions were assigned to us: check for skipped data, and keep them paused
    if writes are currently failing (new assignments start unpaused)."""
    detect_skipped_offsets(consumer, partitions, state)
    if state.paused:
        consumer.pause(partitions)


def on_revoke(state: LoopState, ingestor, consumer, partitions) -> None:
    """Partitions are about to be taken away. Write and commit what's buffered while we
    still own them. If the write fails, drop the buffer: nothing was committed, so the
    partitions' next owner re-reads those records. Writing them later ourselves would
    duplicate them."""
    if ingestor.total_buffered() == 0:
        return
    if ingestor.flush():
        commit_offsets(consumer)
    else:
        logger.warning("Write failed while partitions were being revoked; discarding the buffer "
                       "(uncommitted, so it will be re-read).")
        ingestor.discard_buffers()


def on_lost(state: LoopState, ingestor, consumer, partitions) -> None:
    """Partitions were lost without a chance to commit (e.g. a session timeout while the
    broker or VM stalled). Committing is no longer possible, so drop the buffer; the
    records are re-read from the last committed offset instead of being written twice."""
    if ingestor.total_buffered():
        logger.warning("Partitions lost (session timeout?); discarding the uncommitted buffer, "
                       "which will be re-read.")
        ingestor.discard_buffers()


def consume_loop(
    consumer,
    ingestor,
    should_run: Callable[[], bool],
    batch_size: int,
    flush_interval_sec: float,
    clock: Callable[[], float] = time.time,
    state: LoopState = None,
) -> None:
    """Run until should_run() is False, then do a final flush and close the consumer.

    While a flush is failing, consumption is paused (poll() still runs so the
    consumer keeps its group membership) and retries back off exponentially,
    from flush_interval_sec up to FLUSH_RETRY_BACKOFF_MAX_SEC.
    """
    state = state or LoopState()
    last_flush_time = clock()
    retry_delay = flush_interval_sec
    next_retry_at = 0.0

    try:
        while should_run():
            msg = consumer.poll(timeout=1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Kafka error: {msg.error()}")
                else:
                    ingestor.process_record(msg)

            now = clock()
            if now < next_retry_at:
                continue

            buffered = ingestor.total_buffered()
            if buffered >= batch_size or (now - last_flush_time >= flush_interval_sec and buffered > 0):
                if ingestor.flush():
                    commit_offsets(consumer)
                    last_flush_time = clock()
                    retry_delay = flush_interval_sec
                    next_retry_at = 0.0
                    if state.paused:
                        consumer.resume(consumer.assignment())
                        state.paused = False
                        logger.info("Flush recovered; resumed consumption.")
                else:
                    if not state.paused:
                        consumer.pause(consumer.assignment())
                        state.paused = True
                    next_retry_at = now + retry_delay
                    logger.error(
                        f"Iceberg commit failed; consumption paused, Kafka offsets not committed. "
                        f"Retrying in {retry_delay:.0f}s."
                    )
                    retry_delay = min(retry_delay * 2, FLUSH_RETRY_BACKOFF_MAX_SEC)
    finally:
        logger.info("Performing final flush...")
        if ingestor.flush():
            commit_offsets(consumer)
        consumer.close()
        logger.info("Pipeline stopped cleanly.")
