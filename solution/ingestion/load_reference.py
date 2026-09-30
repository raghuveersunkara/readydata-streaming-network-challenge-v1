"""One-shot job: validate the supplied reference files and publish them to Iceberg.

Exit codes: 0 = tables loaded or already current, 1 = validation failed (nothing
written), 2 = catalog/storage error after retries.
"""
import logging
import sys
import time

from src.config import REFERENCE_DATA_DIR
from src.reference import read_reference, validate, write_reference
from src.tables import NAMESPACE, load_solution_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("reference_loader")

MAX_ATTEMPTS = 6


def main() -> int:
    bundle = read_reference(REFERENCE_DATA_DIR)
    errors = validate(bundle)
    if errors:
        for error in errors:
            logger.error(f"Validation failed: {error}")
        logger.error(f"{len(errors)} validation error(s); no reference tables were written.")
        return 1
    logger.info("Reference data passed validation: " + ", ".join(
        f"{name}={table.num_rows}" for name, table in bundle.tables.items()))

    delay = 2.0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            catalog = load_solution_catalog()
            catalog.create_namespace_if_not_exists(NAMESPACE)
            for table, action in write_reference(catalog, bundle).items():
                logger.info(f"{NAMESPACE}.{table}: {action}")
            return 0
        except Exception as e:
            logger.warning(f"Write attempt {attempt}/{MAX_ATTEMPTS} failed: {e}")
            time.sleep(delay)
            delay *= 2
    logger.error("Giving up: catalog or object storage unavailable.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
