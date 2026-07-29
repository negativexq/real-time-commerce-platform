"""Read-only fraud configuration and registry diagnostics."""

import argparse

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.registry import build_rule_registry


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["config", "rules"])
    command = parser.parse_args().command
    config = FraudConfig.from_environment()
    rules = build_rule_registry(config)
    if command == "config":
        print(
            "Fraud configuration valid: "
            f"review={config.fraud_review_threshold}, "
            f"block={config.fraud_block_threshold}, "
            f"ruleset={config.fraud_ruleset_version}."
        )
    else:
        enabled = frozenset(config.enabled_rule_ids)
        for rule in rules:
            print(
                f"{rule.rule_id}\t{rule.rule_version}\t"
                f"enabled={rule.rule_id in enabled}\tmax_score={rule.maximum_score}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
