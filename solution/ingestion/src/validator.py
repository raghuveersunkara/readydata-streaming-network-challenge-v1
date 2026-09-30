import json
import logging
import os
from typing import Dict, Any, Tuple
from jsonschema import Draft202012Validator, RefResolver

logger = logging.getLogger(__name__)

class ContractValidator:
    def __init__(self, contracts_path: str):
        self.validators = {}
        envelope_filename = "common-envelope.v1.schema.json"
        envelope_path = os.path.join(contracts_path, envelope_filename)

        if not os.path.exists(envelope_path):
            logger.warning(f"Envelope schema missing at '{envelope_path}'. Contracts path: {contracts_path}")
            return

        with open(envelope_path) as f:
            envelope_schema = json.load(f)

        resolver = RefResolver(
            base_uri=f"file://{os.path.abspath(contracts_path)}/",
            referrer=envelope_schema
        )

        for filename in os.listdir(contracts_path):
            if filename.endswith(".v1.schema.json") and filename != envelope_filename:
                # Contract files are hyphenated but event_type values use underscores:
                # 'connectivity-metric.v1.schema.json' -> 'connectivity_metric'
                event_type = filename.replace(".v1.schema.json", "").replace("-", "_")
                filepath = os.path.join(contracts_path, filename)
                with open(filepath) as f:
                    schema = json.load(f)
                    # The contracts declare draft 2020-12. A format checker is needed for
                    # `format` (e.g. uuid) to be enforced; without one it is only an annotation.
                    self.validators[event_type] = Draft202012Validator(
                        schema, resolver=resolver, format_checker=Draft202012Validator.FORMAT_CHECKER
                    )
        logger.info(f"Loaded {len(self.validators)} event contract validators.")

    def validate(self, event_data: Dict[str, Any]) -> Tuple[bool, str]:
        event_type = event_data.get("event_type")
        if not event_type or event_type not in self.validators:
            return False, f"Unknown or missing event_type: '{event_type}'"

        validator = self.validators[event_type]
        errors = sorted(validator.iter_errors(event_data), key=lambda e: e.path)
        if errors:
            return False, f"Schema validation error: {errors[0].message}"
        return True, ""
