"""Fixed allow-listed scenario metadata."""

from services.demo_control_api.models.scenarios import ScenarioDefinition, ScenarioType

SCENARIOS = {
    ScenarioType.NORMAL: ScenarioDefinition(
        scenario_type=ScenarioType.NORMAL,
        title="Normal customer",
        purpose="A stable customer journey.",
        expected_outcome="APPROVE; no alert.",
    ),
    ScenarioType.SUSPICIOUS: ScenarioDefinition(
        scenario_type=ScenarioType.SUSPICIOUS,
        title="Suspicious payment",
        purpose="Failures, identity change, and elevated amount.",
        expected_outcome="REVIEW or BLOCK.",
    ),
    ScenarioType.TAKEOVER: ScenarioDefinition(
        scenario_type=ScenarioType.TAKEOVER,
        title="Account takeover",
        purpose="Prior history followed by a rapid foreign high-value payment.",
        expected_outcome="BLOCK and alert.",
    ),
    ScenarioType.BOT: ScenarioDefinition(
        scenario_type=ScenarioType.BOT,
        title="Bot checkout",
        purpose="High browsing velocity with optional transaction.",
        expected_outcome="Browsing alone has no alert.",
        transaction_configurable=True,
    ),
    ScenarioType.REFUND: ScenarioDefinition(
        scenario_type=ScenarioType.REFUND,
        title="Refund abuse",
        purpose="Successful payment followed by rapid refund behavior.",
        expected_outcome="REVIEW or BLOCK.",
    ),
    ScenarioType.DUPLICATE: ScenarioDefinition(
        scenario_type=ScenarioType.DUPLICATE,
        title="Duplicate delivery",
        purpose="Identical source-event redelivery.",
        expected_outcome="One durable business effect.",
    ),
    ScenarioType.MALFORMED: ScenarioDefinition(
        scenario_type=ScenarioType.MALFORMED,
        title="Malformed event",
        purpose="A predefined invalid record.",
        expected_outcome="DLQ.",
    ),
    ScenarioType.MIXED: ScenarioDefinition(
        scenario_type=ScenarioType.MIXED,
        title="Mixed traffic",
        purpose="A bounded deterministic persona mixture.",
        expected_outcome="Mixed decisions and anomalies.",
    ),
}


def get_scenario(value: ScenarioType) -> ScenarioDefinition:
    return SCENARIOS[value]
