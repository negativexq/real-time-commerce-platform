import {renderToStaticMarkup} from "react-dom/server";
import {describe, expect, it} from "vitest";
import {
  decisionCounts,
  displayNumber,
  FraudScope,
  healthStateLabel,
  latencyMilliseconds,
  throughputEmptyMessage,
  type OverviewFraudSummary,
  ZeroDecisionState,
} from "./overview-dashboard";

const activeRun: OverviewFraudSummary = {
  run_id: "run-1",
  scenario_type: "account_takeover",
  run_status: "RUNNING",
  scope: "ACTIVE_RUN",
  approve_count: 8,
  review_count: 2,
  block_count: 3,
  fraud_alert_count: 3,
  total_decisions: 13,
};

describe("Overview data contracts", () => {
  it("renders zero metrics as zero and null metrics as unavailable", () => {
    expect(displayNumber(0, 2)).toBe("0.00");
    expect(displayNumber(null, 2)).toBe("N/A");
  });

  it("distinguishes loading from unavailable throughput metrics", () => {
    expect(throughputEmptyMessage(null)).toBe("Waiting for traffic");
    expect(
      throughputEmptyMessage({
        status: "degraded",
        values: {processed_rate: null},
      }),
    ).toBe("Metrics unavailable");
  });

  it("uses milliseconds for average latency", () => {
    expect(latencyMilliseconds(0.0125)).toBe(12.5);
  });

  it("renders the active-run scope label", () => {
    expect(
      renderToStaticMarkup(<FraudScope scope={activeRun.scope} />),
    ).toContain("Active run");
  });

  it("derives every donut value from the selected run", () => {
    expect(decisionCounts(activeRun)).toEqual({
      APPROVE: 8,
      REVIEW: 2,
      BLOCK: 3,
    });
  });

  it("renders an honest zero-decision state", () => {
    expect(renderToStaticMarkup(<ZeroDecisionState />)).toContain(
      "No decisions for selected run",
    );
  });

  it("labels unavailable and unmonitored services honestly", () => {
    expect(healthStateLabel("HEALTHY")).toBe("Healthy");
    expect(healthStateLabel("UNHEALTHY")).toBe("Unavailable");
    expect(healthStateLabel("NOT_MONITORED")).toBe("Not monitored");
  });
});
