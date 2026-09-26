from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import ConfigurationError, load_settings
from .metrics import InventoryError, load_device_profiles
from .runtime import NetworkSimulator
from .scenarios import ScenarioEngine, ScenarioError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish deterministic network telemetry")
    parser.add_argument("--config", type=Path, default=Path("/app/config.json"))
    parser.add_argument(
        "--sites",
        type=Path,
        default=Path("/reference-data/inventory/v1/sites.csv"),
    )
    parser.add_argument(
        "--devices",
        type=Path,
        default=Path("/reference-data/inventory/v1/devices.csv"),
    )
    parser.add_argument(
        "--census-block-groups",
        type=Path,
        default=Path("/reference-data/census/v1/census_block_groups.csv"),
    )
    parser.add_argument(
        "--device-dependencies",
        type=Path,
        default=None,
        help="defaults to device_dependencies.csv beside --devices",
    )
    parser.add_argument(
        "--readiness-path",
        type=Path,
        default=Path("/tmp/simulator-ready"),
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args()
    settings = load_settings(args.config)
    profiles = load_device_profiles(
        sites_path=args.sites,
        devices_path=args.devices,
        census_path=args.census_block_groups,
        inventory_version=settings.config["inventory_version"],
        technologies=set(settings.config["technologies"]),
        dependencies_path=args.device_dependencies,
    )
    scenario_engine = ScenarioEngine(
        profiles,
        settings.seed,
        settings.run_id,
        settings.start_time,
        settings.config,
    )
    simulator = NetworkSimulator(
        settings,
        profiles,
        args.readiness_path,
        scenario_engine=scenario_engine,
    )
    return simulator.run()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ConfigurationError, InventoryError, ScenarioError) as error:
        print(f"error - {error}", file=sys.stderr)
        sys.exit(2)
