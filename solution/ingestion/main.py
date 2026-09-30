import time
import signal
import logging
from functools import partial
from confluent_kafka import Consumer

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, CONTRACTS_DIR,
    BATCH_SIZE, FLUSH_INTERVAL_SEC, TOPIC_TO_TABLE
)
from src.validator import ContractValidator
from src.ingest import IcebergIngest
from src.pipeline import FLUSH_RETRY_BACKOFF_MAX_SEC, LoopState, consume_loop, on_assign, on_lost, on_revoke

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ingestion_main")


def ensure_tables_with_retry(ingestor: IcebergIngest) -> None:
    # Nessie/Garage may still be starting when this container comes up.
    delay = 2.0
    while True:
        try:
            ingestor.ensure_tables()
            return
        except Exception as e:
            logger.warning(f"Catalog not ready ({e}); retrying in {delay:.0f}s")
            time.sleep(delay)
            delay = min(delay * 2, FLUSH_RETRY_BACKOFF_MAX_SEC)


def run_pipeline():
    validator = ContractValidator(CONTRACTS_DIR)
    ingestor = IcebergIngest(validator)
    ensure_tables_with_retry(ingestor)

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = Consumer(conf)
    state = LoopState()
    consumer.subscribe(
        list(TOPIC_TO_TABLE.keys()),
        on_assign=partial(on_assign, state),
        on_revoke=partial(on_revoke, state, ingestor),
        on_lost=partial(on_lost, state, ingestor),
    )
    logger.info("Subscribed to Kafka topics.")

    running = True
    def shutdown(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received. Exiting loop...")
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consume_loop(consumer, ingestor, lambda: running, BATCH_SIZE, FLUSH_INTERVAL_SEC, state=state)


if __name__ == "__main__":
    run_pipeline()
