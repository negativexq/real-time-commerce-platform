"use client";

import {ArrowUpRight, Play} from "lucide-react";
import Link from "next/link";
import {useRouter} from "next/navigation";
import {useCallback, useEffect, useMemo, useState} from "react";
import {Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis} from "recharts";
import {api} from "../lib/api";
import {cn} from "../lib/cn";
import {relativeTime} from "../lib/formatting";
import {usePolling} from "../lib/use-polling";
import {humanize, TONE_HEX, toneOf, type Tone} from "../lib/tone";
import type {HealthState, Run} from "../lib/types";
import {apiDocsUrl} from "../lib/urls";
import {RadialGauge, SegmentMeter, thresholdTone, percent} from "./gauges";
import {MetricBand, type BandMetric} from "./metric-band";
import {Meter} from "./meter";
import {NumberTicker} from "./number-ticker";
import {PipelineStrip, type PipelineStage} from "./pipeline-strip";
import {Badge, buttonVariants, DataTable, Panel, StatusDot} from "./ui";

type Metrics = {
  status: string;
  values: {
    processed_rate?: number | null;
    average_latency_seconds?: number | null;
    p95_latency?: number | null;
    consumer_lag?: number | null;
    outbox_pending?: number | null;
    dlq_rate?: number | null;
    fraud_alert_rate?: number | null;
  };
};
type HealthService = {name: string; state: HealthState};
type Health = {overall: HealthState; checked_at: string; services: HealthService[]};
type FraudAlert = {
  alert_id: string;
  run_id: string | null;
  severity: string;
  score: number;
  reason_codes: string[];
  created_at: string;
  detected_at?: string | null;
};
export type OverviewFraudSummary = {
  run_id: string | null;
  scenario_type: string | null;
  run_status: string | null;
  scope: "ACTIVE_RUN" | "LATEST_COMPLETED_RUN" | null;
  approve_count: number;
  review_count: number;
  block_count: number;
  fraud_alert_count: number;
  total_decisions: number;
};
type DlqPage = {items: unknown[]; total?: number};
type Page<T> = {items: T[]};
type ThroughputPoint = {time: string; value: number};
type HistoryKey = "processed_rate" | "p95_latency" | "consumer_lag" | "fraud_alert_rate" | "outbox_pending" | "dlq_rate";

const HISTORY_KEYS: HistoryKey[] = ["processed_rate", "p95_latency", "consumer_lag", "fraud_alert_rate", "outbox_pending", "dlq_rate"];
const EMPTY_HISTORY = Object.fromEntries(HISTORY_KEYS.map((k) => [k, []])) as unknown as Record<HistoryKey, number[]>;

/** Keeps the last 36 samples (three minutes at a 5 s refresh) per metric. */
export function appendHistory(current: Record<HistoryKey, number[]>, values: Metrics["values"]) {
  const next = {...current};
  for (const key of HISTORY_KEYS) {
    const value = values[key];
    if (value != null && Number.isFinite(value)) next[key] = [...current[key].slice(-35), value];
  }
  return next;
}

const ACTIVE_STATUSES = new Set(["PENDING", "STARTING", "RUNNING", "STOP_REQUESTED"]);

/* Thresholds the gauges are judged against. */
const P95_WARN_MS = 100;
const P95_BAD_MS = 250;
const LAG_WARN = 500;
const LAG_BAD = 2000;
const DLQ_WARN = 0.01;
const DLQ_BAD = 0.05;

/* Helpers kept as named exports: the overview contract tests use them. */

export function displayNumber(value: number | null | undefined, digits = 0) {
  if (value == null || !Number.isFinite(value)) return "N/A";
  return value.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function latencyMilliseconds(value: number | null | undefined) {
  return value == null ? null : value * 1000;
}

export function fraudScopeLabel(scope: OverviewFraudSummary["scope"]) {
  if (scope === "ACTIVE_RUN") return "Active run";
  if (scope === "LATEST_COMPLETED_RUN") return "Latest completed run";
  return null;
}

export function decisionCounts(summary: OverviewFraudSummary | null) {
  return {
    APPROVE: summary?.approve_count ?? 0,
    REVIEW: summary?.review_count ?? 0,
    BLOCK: summary?.block_count ?? 0,
  };
}

export function healthStateLabel(state: HealthState) {
  if (state === "NOT_MONITORED") return "Not monitored";
  if (state === "UNHEALTHY") return "Unavailable";
  return state.charAt(0) + state.slice(1).toLowerCase();
}

export function FraudScope({scope}: {scope: OverviewFraudSummary["scope"]}) {
  const label = fraudScopeLabel(scope);
  return label ? <span className="text-[13px] text-muted">{label}</span> : null;
}

export function ZeroDecisionState() {
  return <div className="py-6 text-center text-sm text-muted">No decisions for selected run</div>;
}

export function throughputEmptyMessage(metrics: Metrics | null) {
  if (metrics === null) return "Waiting for traffic";
  if (metrics.status === "degraded" || metrics.values.processed_rate == null) {
    return "Metrics unavailable";
  }
  return "Waiting for traffic";
}

/** Share of terminal events that went to the DLQ, or null when idle/unknown. */
export function dlqShare(processed: number | null | undefined, dlq: number | null | undefined) {
  if (processed == null || dlq == null) return null;
  const total = processed + dlq;
  return total > 0 ? dlq / total : null;
}

function formatDuration(run: Run) {
  const elapsed = new Date(run.updated_at).getTime() - new Date(run.created_at).getTime();
  if (!Number.isFinite(elapsed) || elapsed < 0) return "—";
  const seconds = Math.round(elapsed / 1000);
  return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

export function QuickActions() {
  return (
    <div className="flex flex-wrap gap-2" aria-label="Quick actions">
      <Link className={buttonVariants({variant: "primary", size: "sm"})} href="/scenarios">
        <Play aria-hidden="true" />
        Launch scenario
      </Link>
      <a className={buttonVariants({variant: "secondary", size: "sm"})} href="http://localhost:3002" target="_blank" rel="noreferrer">
        Open Grafana ↗
      </a>
      <a className={buttonVariants({variant: "secondary", size: "sm"})} href="http://localhost:8080" target="_blank" rel="noreferrer">
        Open Kafka UI ↗
      </a>
      <a className={buttonVariants({variant: "secondary", size: "sm"})} href={apiDocsUrl} target="_blank" rel="noreferrer">
        Open API Docs ↗
      </a>
    </div>
  );
}

function stateOf(health: Health | null, name: string): HealthState {
  return health?.services.find((s) => s.name === name)?.state ?? "UNKNOWN";
}

export default function OverviewDashboard() {
  const router = useRouter();
  const [runs, setRuns] = useState<Run[]>([]);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [health, setHealth] = useState<Health | null>(null);
  const [alerts, setAlerts] = useState<FraudAlert[]>([]);
  const [fraudSummary, setFraudSummary] = useState<OverviewFraudSummary | null>(null);
  const [dlqTotal, setDlqTotal] = useState<number | null>(null);
  const [throughput, setThroughput] = useState<ThroughputPoint[]>([]);
  const [history, setHistory] = useState<Record<HistoryKey, number[]>>(EMPTY_HISTORY);
  const [error, setError] = useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [runPage, metricSummary, platformHealth, alertPage, selectedFraud, dlqPage] = await Promise.all([
        api<Page<Run>>("/api/v1/runs?page_size=20"),
        api<Metrics>("/api/v1/platform/metrics/summary"),
        api<Health>("/api/v1/platform/health"),
        api<Page<FraudAlert>>("/api/v1/fraud/alerts?page_size=8"),
        api<OverviewFraudSummary>("/api/v1/overview/fraud-summary"),
        api<DlqPage>("/api/v1/dlq?page_size=1"),
      ]);
      setRuns(runPage.items);
      setMetrics(metricSummary);
      setHealth(platformHealth);
      setAlerts(alertPage.items);
      setFraudSummary(selectedFraud);
      setDlqTotal(dlqPage.total ?? dlqPage.items.length);
      setUpdatedAt(new Date());
      setError(null);
      setHistory((current) => appendHistory(current, metricSummary.values));
      const rate = metricSummary.values.processed_rate;
      if (rate != null && Number.isFinite(rate)) {
        setThroughput((current) => [
          ...current.slice(-35),
          {
            time: new Date().toLocaleTimeString([], {hour: "2-digit", minute: "2-digit", second: "2-digit"}),
            value: rate,
          },
        ]);
      }
    } catch {
      setError("Overview data is temporarily unavailable.");
    }
  }, []);

  usePolling(refresh, 5000);

  const v = metrics?.values ?? {};
  const activeRuns = useMemo(() => runs.filter((run) => ACTIVE_STATUSES.has(run.status)).length, [runs]);
  const p95Ms = latencyMilliseconds(v.p95_latency);
  const avgMs = latencyMilliseconds(v.average_latency_seconds);
  const share = dlqShare(v.processed_rate, v.dlq_rate);
  const monitored = useMemo(() => {
    if (!health) return null;
    const list = health.services.filter((s) => s.state !== "UNKNOWN" && s.state !== "NOT_MONITORED");
    return {healthy: list.filter((s) => s.state === "HEALTHY").length, total: list.length};
  }, [health]);
  const flowing = (v.processed_rate ?? 0) > 0;

  const stages: PipelineStage[] = [
    {
      name: "Generator",
      state: stateOf(health, "Generator"),
      figure: displayNumber(activeRuns),
      unit: activeRuns === 1 ? "active run" : "active runs",
    },
    {
      name: "Kafka",
      state: stateOf(health, "Kafka"),
      figure: displayNumber(v.consumer_lag),
      unit: "messages behind",
      tone: thresholdTone(v.consumer_lag, LAG_WARN, LAG_BAD),
    },
    {
      name: "Event processor",
      state: stateOf(health, "Event Processor"),
      figure: displayNumber(v.processed_rate, 1),
      unit: "events / s",
      stores: [
        {name: "Redis", state: stateOf(health, "Redis")},
        {name: "PostgreSQL", state: stateOf(health, "PostgreSQL")},
      ],
    },
    {
      name: "Fraud engine",
      // Runs inside the processor's transaction, so it shares its health.
      state: stateOf(health, "Event Processor"),
      figure: v.fraud_alert_rate == null ? "N/A" : displayNumber(v.fraud_alert_rate * 60, 1),
      unit: "alerts / min",
    },
    {
      name: "Outbox publisher",
      state: stateOf(health, "Fraud Outbox Publisher"),
      figure: displayNumber(v.outbox_pending),
      unit: "pending",
      tone: (v.outbox_pending ?? 0) > 0 ? "warn" : undefined,
    },
  ];

  const band: BandMetric[] = [
    {
      label: "Throughput",
      value: v.processed_rate,
      digits: 1,
      unit: "events / s",
      history: history.processed_rate,
      tone: "flow",
      higherIsBetter: true,
    },
    {
      label: "p95 latency",
      value: p95Ms,
      digits: p95Ms != null && p95Ms < 10 ? 1 : 0,
      unit: "ms",
      history: history.p95_latency.map((x) => x * 1000),
      tone: thresholdTone(p95Ms, P95_WARN_MS, P95_BAD_MS),
    },
    {
      label: "Consumer lag",
      value: v.consumer_lag,
      unit: "messages",
      history: history.consumer_lag,
      tone: thresholdTone(v.consumer_lag, LAG_WARN, LAG_BAD),
    },
    {
      label: "Fraud alerts",
      value: v.fraud_alert_rate == null ? null : v.fraud_alert_rate * 60,
      digits: 1,
      unit: "per min",
      history: history.fraud_alert_rate.map((x) => x * 60),
      tone: (v.fraud_alert_rate ?? 0) > 0 ? "bad" : "neutral",
    },
    {
      label: "Dead letters",
      value: dlqTotal,
      unit: "stored",
      history: history.dlq_rate,
      tone: (dlqTotal ?? 0) > 0 ? "warn" : "ok",
    },
    {
      label: "Outbox backlog",
      value: v.outbox_pending,
      unit: "pending",
      history: history.outbox_pending,
      tone: (v.outbox_pending ?? 0) > 0 ? "warn" : "ok",
    },
  ];

  const decisions = decisionCounts(fraudSummary);
  const totalDecisions = fraudSummary?.total_decisions ?? 0;
  const servicesTone: Tone =
    monitored == null ? "neutral" : monitored.healthy === monitored.total ? "ok" : monitored.total - monitored.healthy > 1 ? "bad" : "warn";

  const openRun = (runId: string) => router.push(`/runs/${runId}`);
  const rowKeyDown = (runId: string) => (event: React.KeyboardEvent) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      openRun(runId);
    }
  };

  return (
    <div className="grid gap-5">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Pipeline overview</h1>
          <p className="mt-1 flex items-center gap-2 text-sm text-muted" role="status">
            <StatusDot tone={error ? "bad" : updatedAt ? "ok" : "neutral"} />
            {error ? "Data is stale. Check that the demo API is running." : updatedAt ? `Updated ${updatedAt.toLocaleTimeString()}, refreshes every 5 s` : "Connecting to the demo API"}
          </p>
        </div>
        <QuickActions />
      </div>

      <MetricBand metrics={band} />

      <PipelineStrip stages={stages} flowing={flowing} />

      <Panel title="Service levels" description="Live from Prometheus over the last few minutes">
        <div className="grid grid-cols-2 gap-x-4 gap-y-6 md:grid-cols-4">
          <RadialGauge
            label="p95 latency"
            value={p95Ms}
            max={P95_BAD_MS * 2}
            marks={[P95_WARN_MS, P95_BAD_MS]}
            scale="sqrt"
            display={p95Ms == null ? "N/A" : displayNumber(p95Ms, p95Ms < 10 ? 1 : 0)}
            unit="ms"
            tone={thresholdTone(p95Ms, P95_WARN_MS, P95_BAD_MS)}
            caption={avgMs == null ? `Budget ${P95_BAD_MS} ms` : `Average ${displayNumber(avgMs, 1)} ms`}
          />
          <RadialGauge
            label="Consumer lag"
            value={v.consumer_lag}
            max={LAG_BAD * 2}
            marks={[LAG_WARN, LAG_BAD]}
            scale="sqrt"
            display={displayNumber(v.consumer_lag)}
            unit="messages"
            tone={thresholdTone(v.consumer_lag, LAG_WARN, LAG_BAD)}
            caption={`Alert above ${LAG_BAD.toLocaleString("en-US")}`}
          />
          <RadialGauge
            label="Dead-letter share"
            value={share}
            max={DLQ_BAD * 2}
            marks={[DLQ_WARN, DLQ_BAD]}
            scale="sqrt"
            display={share == null ? "0" : displayNumber(share * 100, 1)}
            unit="% of events"
            tone={share == null ? "ok" : thresholdTone(share, DLQ_WARN, DLQ_BAD)}
            caption={dlqTotal == null ? undefined : `${dlqTotal.toLocaleString("en-US")} stored in total`}
          />
          <RadialGauge
            label="Services up"
            value={monitored?.healthy}
            max={monitored?.total || 1}
            display={monitored == null ? "N/A" : `${monitored.healthy}/${monitored.total}`}
            unit="monitored"
            tone={servicesTone}
            caption={<Link className="hover:text-ink" href="/infrastructure">See infrastructure</Link>}
          />
        </div>
      </Panel>

      <div className="grid gap-5 xl:grid-cols-[minmax(0,2fr)_minmax(320px,1fr)]">
        <Panel
          title="Throughput"
          description="Processed events per second, sampled on each refresh"
          action={
            <span className="figure text-2xl font-semibold text-ink">
              {v.processed_rate == null ? "N/A" : <NumberTicker value={v.processed_rate} digits={1} />}
              <span className="ml-1 text-sm font-medium text-muted">events / s</span>
            </span>
          }
        >
          <div className="h-60 text-flow" role="img" aria-label="Processed events per second over recent refreshes">
            {throughput.length < 2 ? (
              <div className="flex h-full items-center justify-center text-sm text-muted">{throughputEmptyMessage(metrics)}</div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={throughput} margin={{top: 6, right: 4, left: -24, bottom: 0}}>
                  <defs>
                    <linearGradient id="throughputFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" style={{stopColor: "currentColor"}} stopOpacity={0.28} />
                      <stop offset="100%" style={{stopColor: "currentColor"}} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <XAxis dataKey="time" tick={{fill: "var(--color-faint)", fontSize: 11}} tickLine={false} axisLine={false} minTickGap={48} />
                  <YAxis tick={{fill: "var(--color-faint)", fontSize: 11}} tickLine={false} axisLine={false} width={48} />
                  <Tooltip
                    cursor={{stroke: "var(--color-rule-strong)"}}
                    contentStyle={{background: "var(--color-panel)", border: "1px solid var(--color-rule-strong)", borderRadius: 6, fontSize: 12, color: "var(--color-ink)"}}
                    labelStyle={{color: "var(--color-muted)"}}
                    formatter={(value) => [`${displayNumber(Number(value), 2)} events / s`, "Processed"]}
                  />
                  <Area type="monotone" dataKey="value" stroke="currentColor" strokeWidth={2} fill="url(#throughputFill)" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            )}
          </div>
        </Panel>

        <Panel
          title="Fraud decisions"
          description={<FraudScope scope={fraudSummary?.scope ?? null} />}
          action={fraudSummary?.run_id ? <Link className="text-muted hover:text-ink" href={`/runs/${fraudSummary.run_id}`}>Open run</Link> : null}
        >
          {totalDecisions === 0 ? (
            <ZeroDecisionState />
          ) : (
            <div className="grid gap-5">
              <div className="flex items-end justify-between">
                <div>
                  <div className="figure text-4xl font-semibold leading-none">
                    <NumberTicker value={totalDecisions} />
                  </div>
                  <div className="mt-1 text-[13px] text-muted">
                    decisions in {fraudSummary?.scenario_type ? humanize(fraudSummary.scenario_type).toLowerCase() : "this run"}
                  </div>
                </div>
                <div className="text-right">
                  <div className="figure text-2xl font-semibold leading-none text-bad">{displayNumber(fraudSummary?.fraud_alert_count)}</div>
                  <div className="mt-1 text-[13px] text-muted">alerts raised</div>
                </div>
              </div>
              <SegmentMeter
                total={totalDecisions}
                segments={[
                  {key: "APPROVE", label: "Approve", value: decisions.APPROVE, tone: "ok"},
                  {key: "REVIEW", label: "Review", value: decisions.REVIEW, tone: "warn"},
                  {key: "BLOCK", label: "Block", value: decisions.BLOCK, tone: "bad"},
                ]}
              />
              <dl className="grid grid-cols-3 gap-3">
                {(["APPROVE", "REVIEW", "BLOCK"] as const).map((name) => (
                  <div key={name} className="rounded-[var(--radius-control)] bg-raised/60 px-3 py-2.5">
                    <dt className="flex items-center gap-1.5 text-xs text-muted">
                      <StatusDot tone={toneOf(name)} />
                      {humanize(name)}
                    </dt>
                    <dd className="figure mt-1 text-xl font-semibold">{decisions[name].toLocaleString("en-US")}</dd>
                    <dd className="text-xs text-faint">{percent(decisions[name], totalDecisions)}</dd>
                  </div>
                ))}
              </dl>
            </div>
          )}
        </Panel>
      </div>

      <div className="grid gap-5 xl:grid-cols-[minmax(0,2fr)_minmax(320px,1fr)]">
        <Panel title="Recent runs" action={<Link className="text-muted hover:text-ink" href="/runs">All runs</Link>}>
          {runs.length === 0 ? (
            <div className="py-6 text-center text-sm text-muted">
              No runs yet. <Link className="text-flow hover:underline" href="/scenarios">Launch a scenario</Link> to send traffic.
            </div>
          ) : (
            <DataTable fit minWidth={560}>
              <thead>
                <tr>
                  <th className="w-[34%]">Scenario</th>
                  <th>Processed</th>
                  <th>Decisions</th>
                  <th className="w-16 text-right">Alerts</th>
                  <th className="w-24 text-right">Started</th>
                </tr>
              </thead>
              <tbody>
                {runs.slice(0, 6).map((run) => {
                  const decided = run.approve_count + run.review_count + run.block_count;
                  return (
                    <tr key={run.run_id} className="cursor-pointer" tabIndex={0} onClick={() => openRun(run.run_id)} onKeyDown={rowKeyDown(run.run_id)}>
                      <td>
                        <div className="truncate font-medium text-ink">{humanize(run.scenario_type)}</div>
                        <Badge value={run.status} dot className="mt-1" />
                      </td>
                      <td>
                        <Meter
                          value={run.processed_event_count}
                          max={run.requested_event_count}
                          tone={run.status === "FAILED" ? "bad" : "flow"}
                          className="w-full max-w-36"
                        />
                        <div className="figure mt-1 truncate text-xs text-muted">
                          {run.processed_event_count.toLocaleString("en-US")} of {run.requested_event_count.toLocaleString("en-US")}
                        </div>
                      </td>
                      <td>
                        <SegmentMeter
                          className="w-full max-w-36"
                          total={decided}
                          segments={[
                            {key: "a", label: "Approve", value: run.approve_count, tone: "ok"},
                            {key: "r", label: "Review", value: run.review_count, tone: "warn"},
                            {key: "b", label: "Block", value: run.block_count, tone: "bad"},
                          ]}
                        />
                        <div className="figure mt-1 truncate text-xs text-muted">{run.block_count.toLocaleString("en-US")} blocked</div>
                      </td>
                      <td className="figure text-right">{run.fraud_alert_count.toLocaleString("en-US")}</td>
                      <td className="text-right text-muted" title={new Date(run.created_at).toLocaleString()}>
                        {relativeTime(run.created_at)}
                        <div className="figure text-xs text-faint">{formatDuration(run)}</div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </DataTable>
          )}
        </Panel>

        <Panel title="Latest alerts" description="Newest detected first" action={<Link className="text-muted hover:text-ink" href="/fraud">All alerts</Link>}>
          {alerts.length === 0 ? (
            <div className="py-6 text-center text-sm text-muted">No alerts yet.</div>
          ) : (
            <ul className="-mx-2 grid">
              {alerts.slice(0, 7).map((alert) => {
                const when = alert.detected_at ?? alert.created_at;
                const clickable = Boolean(alert.run_id);
                return (
                  <li key={alert.alert_id}>
                    <button
                      type="button"
                      disabled={!clickable}
                      onClick={() => alert.run_id && openRun(alert.run_id)}
                      className={cn(
                        "grid w-full grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3 rounded-[var(--radius-control)] px-2 py-2 text-left",
                        clickable && "hover:bg-raised/60",
                      )}
                    >
                      <span
                        className="figure flex size-9 items-center justify-center rounded-[var(--radius-control)] text-sm font-semibold"
                        style={{background: `color-mix(in oklab, ${TONE_HEX[toneOf(alert.severity)]} 14%, transparent)`, color: TONE_HEX[toneOf(alert.severity)]}}
                        title={`Score ${alert.score}`}
                      >
                        {alert.score}
                      </span>
                      <span className="min-w-0">
                        <span className="block truncate text-sm font-medium text-ink" title={alert.reason_codes.join(", ")}>
                          {alert.reason_codes[0] ? humanize(alert.reason_codes[0]) : "Unspecified rule"}
                          {alert.reason_codes.length > 1 && <span className="text-muted"> +{alert.reason_codes.length - 1}</span>}
                        </span>
                        <span className="block text-xs text-muted">{humanize(alert.severity)} severity</span>
                      </span>
                      <span className="flex items-center gap-1 text-xs text-faint" title={new Date(when).toLocaleString()}>
                        {relativeTime(when)}
                        {clickable && <ArrowUpRight className="size-3.5" aria-hidden="true" />}
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </Panel>
      </div>
    </div>
  );
}

export {relativeTime};
