import {renderToStaticMarkup} from "react-dom/server";
import {describe, expect, it} from "vitest";
import {formatDateTime} from "../lib/formatting";
import {expectedOutcomes, fromLogPosition, MAX_EVENTS, toLogPosition} from "../lib/run-limits";
import {humanize, toneOf} from "../lib/tone";
import {gaugeFraction, percent, RadialGauge, thresholdTone} from "./gauges";
import {streamStateAfterError} from "./live-run";
import {Meter} from "./meter";
import {alertKey, bellSummary, visibleAlerts} from "./notification-bell";
import {sampleDelta} from "./metric-band";
import {appendHistory, dlqShare, relativeTime} from "./overview-dashboard";
import {estimatedDuration} from "./scenario-form";

describe("gauges", () => {
  it("judges higher-is-worse values against thresholds", () => {
    expect(thresholdTone(5, 100, 250)).toBe("ok");
    expect(thresholdTone(100, 100, 250)).toBe("warn");
    expect(thresholdTone(400, 100, 250)).toBe("bad");
    expect(thresholdTone(null, 100, 250)).toBe("neutral");
  });

  it("clamps the arc and spreads the low end on a sqrt scale", () => {
    expect(gaugeFraction(null, 10)).toBeNull();
    expect(gaugeFraction(20, 10)).toBe(1);
    expect(gaugeFraction(5, 500)).toBeCloseTo(0.01);
    expect(gaugeFraction(5, 500, "sqrt")).toBeCloseTo(0.1);
  });

  it("renders an unknown value without filling the arc", () => {
    const markup = renderToStaticMarkup(
      <RadialGauge label="Consumer lag" value={null} max={100} display="N/A" tone="neutral" />,
    );
    expect(markup).toContain('aria-label="Consumer lag: N/A"');
    expect(markup.match(/<path/g)).toHaveLength(1);
  });

  it("keeps a tiny non-zero share visible on a meter", () => {
    expect(renderToStaticMarkup(<Meter value={2} max={4396} />)).toContain("min-w-1.5");
    expect(renderToStaticMarkup(<Meter value={0} max={4396} />)).not.toContain("min-w-1.5");
  });

  it("formats shares with a decimal only below ten percent", () => {
    expect(percent(1, 0)).toBe("—");
    expect(percent(258, 16062)).toBe("1.6%");
    expect(percent(15260, 16062)).toBe("95%");
  });
});

describe("live metrics", () => {
  it("reports the change between the last two samples", () => {
    expect(sampleDelta([4])).toBeNull();
    expect(sampleDelta([4, 4])).toBe(0);
    expect(sampleDelta([4, 5])).toBeCloseTo(0.25);
    expect(sampleDelta([0, 3])).toBe(Infinity);
  });

  it("keeps a bounded history and skips missing samples", () => {
    let history = appendHistory(
      {processed_rate: [], p95_latency: [], consumer_lag: [], fraud_alert_rate: [], outbox_pending: [], dlq_rate: []},
      {processed_rate: 1, consumer_lag: null},
    );
    for (let i = 0; i < 50; i += 1) history = appendHistory(history, {processed_rate: i});
    expect(history.processed_rate).toHaveLength(36);
    expect(history.processed_rate.at(-1)).toBe(49);
    expect(history.consumer_lag).toEqual([]);
  });

  it("computes the dead-letter share only when traffic is flowing", () => {
    expect(dlqShare(0, 0)).toBeNull();
    expect(dlqShare(null, 1)).toBeNull();
    expect(dlqShare(99, 1)).toBeCloseTo(0.01);
  });

  it("describes detection time relative to now", () => {
    const now = Date.parse("2026-11-27T12:00:00Z");
    expect(relativeTime("2026-11-27T11:59:58Z", now)).toBe("just now");
    expect(relativeTime("2026-11-27T11:15:00Z", now)).toBe("45m ago");
    expect(relativeTime("2026-11-25T12:00:00Z", now)).toBe("2d ago");
  });
});

describe("run stream", () => {
  it("treats the server closing a finished run's stream as a normal end", () => {
    expect(streamStateAfterError("COMPLETED", 2)).toBe("finished");
    expect(streamStateAfterError("RUNNING", 0)).toBe("reconnecting");
    expect(streamStateAfterError("RUNNING", 2)).toBe("interrupted");
  });
});

describe("formatting", () => {
  it("estimates run duration from count and rate", () => {
    expect(estimatedDuration(500, 20)).toBe("25 s");
    expect(estimatedDuration(4000, 40)).toBe("1 min 40 s");
    expect(estimatedDuration(1200, 20)).toBe("1 min");
    expect(estimatedDuration(0, 20)).toBe("—");
    expect(estimatedDuration(100000, 10)).toBe("2 h 47 min");
  });

  it("maps the log-scale slider to readable values and back", () => {
    expect(fromLogPosition(0, MAX_EVENTS)).toBe(1);
    expect(fromLogPosition(1, MAX_EVENTS)).toBe(MAX_EVENTS);
    expect(fromLogPosition(toLogPosition(500, MAX_EVENTS), MAX_EVENTS)).toBe(500);
    expect(fromLogPosition(0.5, MAX_EVENTS) % 50).toBe(0);
  });

  it("reads expected decisions from a scenario's outcome text", () => {
    expect(expectedOutcomes("REVIEW or BLOCK.")).toEqual(["REVIEW", "BLOCK"]);
    expect(expectedOutcomes("APPROVE; no alert.")).toEqual(["APPROVE"]);
    expect(expectedOutcomes("Mixed decisions and anomalies.")).toEqual([]);
  });

  it("formats timestamps in a fixed locale", () => {
    expect(formatDateTime(null)).toBe("—");
    expect(formatDateTime("not a date")).toBe("—");
    expect(formatDateTime("2026-11-27T10:16:33Z")).toMatch(/^Nov 27, \d{2}:16:33$/);
    expect(formatDateTime("2026-11-27T10:16:33Z", "UTC")).toBe("Nov 27, 10:16:33");
    expect(formatDateTime("2026-11-27T10:16:33Z", "Europe/Istanbul")).toBe("Nov 27, 13:16:33");
  });

  it("maps states to tones", () => {
    expect(toneOf("BLOCK")).toBe("bad");
    expect(toneOf("running")).toBe("flow");
    expect(toneOf(undefined)).toBe("neutral");
  });

  it("humanizes codes and keeps acronyms upper case", () => {
    expect(humanize("malformed_json")).toBe("Malformed JSON");
    expect(humanize("missing_business_dependency")).toBe("Missing business dependency");
    expect(humanize("DLQ")).toBe("DLQ");
  });
});

describe("notification bell", () => {
  const alert = (state: string, severity: string) => ({name: "A", state, severity, summary: "", description: "", source: null, active_at: null});

  it("counts only firing alerts and turns red for a critical one", () => {
    expect(bellSummary(null)).toMatchObject({count: 0, tone: "neutral"});
    expect(bellSummary({status: "unavailable", items: [], firing: 0}).label).toContain("unreachable");
    expect(bellSummary({status: "available", items: [alert("pending", "critical")], firing: 0})).toMatchObject({count: 0, tone: "neutral"});
    expect(bellSummary({status: "available", items: [alert("firing", "warning")], firing: 1})).toMatchObject({count: 1, tone: "warn"});
    expect(bellSummary({status: "available", items: [alert("firing", "warning"), alert("firing", "critical")], firing: 2})).toMatchObject({
      count: 2,
      tone: "bad",
      label: "Alerts: 2 firing",
    });
  });

  it("clears warnings but keeps critical alerts until they resolve", () => {
    const warning = {...alert("firing", "warning"), name: "HighDLQRate", active_at: "2026-11-27T10:00:00Z"};
    const critical = {...alert("firing", "critical"), name: "DLQSinkDown"};
    const cleared = new Set([alertKey(warning), alertKey(critical)]);
    expect(visibleAlerts([warning, critical], cleared)).toEqual([critical]);
    expect(bellSummary({status: "available", items: [warning, critical], firing: 2}, cleared)).toMatchObject({count: 1, tone: "bad"});
    const refired = {...warning, active_at: "2026-11-27T12:00:00Z"};
    expect(visibleAlerts([refired], cleared)).toEqual([refired]);
  });
});
