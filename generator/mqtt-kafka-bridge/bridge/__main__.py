from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import ConfigurationError, load_settings
from .runtime import MqttKafkaBridge


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forward MQTT connectivity telemetry to Kafka")
    parser.add_argument("--config", type=Path, default=Path("/app/config.json"))
    parser.add_argument(
        "--readiness-path",
        type=Path,
        default=Path("/tmp/mqtt-kafka-bridge-ready"),
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args()
    settings = load_settings(args.config)
    return MqttKafkaBridge(settings, args.readiness_path).run()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ConfigurationError as error:
        print(f"error - {error}", file=sys.stderr)
        sys.exit(2)
