"use client";

import Link from "next/link";
import {useRouter} from "next/navigation";
import {useCallback, useEffect, useMemo, useState} from "react";
import {
  Area,
  AreaChart,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {api} from "../lib/api";
import type {HealthState, Run} from "../lib/types";
import {apiDocsUrl} from "../lib/urls";

type Metrics = {
  status: string;
  values: {
    processed_rate?: number | null;
    average_latency_seconds?: number | null;
    consumer_lag?: number | null;
    outbox_pending?: number | null;
  };
};
type HealthService = {name: string; state: HealthState};
type Health = {
  overall: HealthState;
  checked_at: string;
  services: HealthService[];
};
type FraudAlert = {
  alert_id: string;
  run_id: string | null;
  severity: string;
  score: number;
  reason_codes: string[];
  created_at: string;
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
type DlqRecord = {id: number};
type Page<T> = {items: T[]; total?: number};
type ThroughputPoint = {time: string; value: number};

const ACTIVE_STATUSES = new Set([
  "PENDING",
  "STARTING",
  "RUNNING",
  "STOP_REQUESTED",
]);
const DECISION_COLORS = {
  APPROVE: "#46c98b",
  REVIEW: "#f2b84b",
  BLOCK: "#f26767",
};
const SERVICE_NAMES = [
  "Generator",
  "Kafka",
  "Processor",
  "Redis",
  "PostgreSQL",
  "Fraud Engine",
  "Outbox Publisher",
  "Prometheus",
  "Grafana",
  "Kafka UI",
] as const;
const SERVICE_ALIASES: Record<string, string> = {
  Processor: "Event Processor",
  "Outbox Publisher": "Fraud Outbox Publisher",
};

export function displayNumber(value: number | null | undefined, digits = 0) {
  if (value == null || !Number.isFinite(value)) return "N/A";
  return value.toLocaleString(undefined, {
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

export function FraudScope({
  scope,
}: {
  scope: OverviewFraudSummary["scope"];
}) {
  const label = fraudScopeLabel(scope);
  return label ? <small className="scope-label">{label}</small> : null;
}

export function ZeroDecisionState() {
  return <div className="decision-empty">No decisions for selected run</div>;
}

function formatDuration(run: Run) {
  const elapsed =
    new Date(run.updated_at).getTime() - new Date(run.created_at).getTime();
  if (!Number.isFinite(elapsed) || elapsed < 0) return "N/A";
  const seconds = Math.round(elapsed / 1000);
  return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function relativeTime(value: string) {
  const seconds = Math.max(
    0,
    Math.round((Date.now() - new Date(value).getTime()) / 1000),
  );
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3600)}h ago`;
}

function Icon({children}: {children: string}) {
  return (
    <span className="overview-kpi-icon" aria-hidden="true">
      {children}
    </span>
  );
}

export function QuickActions() {
  return (
    <section className="overview-actions" aria-label="Quick actions">
      <Link className="action primary" href="/scenarios">
        Launch Scenario
      </Link>
      <a className="action" href="http://localhost:3002" target="_blank" rel="noreferrer">
        Open Grafana ↗
      </a>
      <a className="action" href="http://localhost:8080" target="_blank" rel="noreferrer">
        Open Kafka UI ↗
      </a>
      <a className="action" href={apiDocsUrl} target="_blank" rel="noreferrer">
        Open API Docs ↗
      </a>
    </section>
  );
}

export default function OverviewDashboard() {
  const router = useRouter();
  const [runs, setRuns] = useState<Run[]>([]);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [health, setHealth] = useState<Health | null>(null);
  const [alerts, setAlerts] = useState<FraudAlert[]>([]);
  const [fraudSummary, setFraudSummary] =
    useState<OverviewFraudSummary | null>(null);
  const [dlq, setDlq] = useState<DlqRecord[]>([]);
  const [throughput, setThroughput] = useState<ThroughputPoint[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [runPage, metricSummary, platformHealth, alertPage, selectedFraud, dlqPage] =
        await Promise.all([
          api<Page<Run>>("/api/v1/runs?page_size=100"),
          api<Metrics>("/api/v1/platform/metrics/summary"),
          api<Health>("/api/v1/platform/health"),
          api<Page<FraudAlert>>("/api/v1/fraud/alerts?page_size=100"),
          api<OverviewFraudSummary>("/api/v1/overview/fraud-summary"),
          api<Page<DlqRecord>>("/api/v1/dlq?page_size=100"),
        ]);
      setRuns(runPage.items);
      setMetrics(metricSummary);
      setHealth(platformHealth);
      setAlerts(alertPage.items);
      setFraudSummary(selectedFraud);
      setDlq(dlqPage.items);
      setUpdatedAt(new Date());
      setError(null);
      const rate = metricSummary.values.processed_rate;
      if (rate != null && Number.isFinite(rate)) {
        setThroughput((current) => [
          ...current.slice(-23),
          {
            time: new Date().toLocaleTimeString([], {
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            }),
            value: rate,
          },
        ]);
      }
    } catch {
      setError("Overview data is temporarily unavailable.");
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  const activeRuns = useMemo(
    () => runs.filter((run) => ACTIVE_STATUSES.has(run.status)).length,
    [runs],
  );
  const recentRuns = useMemo(() => runs.slice(0, 6), [runs]);
  const decisionData = useMemo(() => {
    const counts = decisionCounts(fraudSummary);
    return Object.entries(counts).map(([name, value]) => ({
      name: name as keyof typeof DECISION_COLORS,
      value,
    }));
  }, [fraudSummary]);
  const totalDecisions = fraudSummary?.total_decisions ?? 0;
  const selectedFraudScope = fraudScopeLabel(fraudSummary?.scope ?? null);
  const serviceCards = useMemo(
    () =>
      SERVICE_NAMES.map((name) => {
        const apiName = SERVICE_ALIASES[name] ?? name;
        return {
          name,
          state:
            health?.services.find((service) => service.name === apiName)?.state ??
            "UNKNOWN",
        };
      }),
    [health],
  );
  const healthyServices = useMemo(
    () => health?.services.filter((service) => service.state === "HEALTHY").length,
    [health],
  );

  const openRun = (runId: string) => router.push(`/runs/${runId}`);
  const rowKeyDown = (runId: string) => (event: React.KeyboardEvent) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      openRun(runId);
    }
  };

  const kpis = [
    {icon: "▶", title: "Active Runs", value: displayNumber(activeRuns)},
    {
      icon: "↗",
      title: "Events/sec",
      value: displayNumber(metrics?.values.processed_rate, 2),
    },
    {
      icon: "≋",
      title: "Consumer Lag",
      value: displayNumber(metrics?.values.consumer_lag),
    },
    {
      icon: "◆",
      title: "Fraud Alerts",
      value: fraudSummary?.run_id
        ? displayNumber(fraudSummary.fraud_alert_count)
        : "N/A",
      context: selectedFraudScope,
    },
    {icon: "!", title: "DLQ Events", value: displayNumber(dlq.length)},
    {
      icon: "□",
      title: "Outbox Pending",
      value: displayNumber(metrics?.values.outbox_pending),
    },
    {
      icon: "◷",
      title: "Average Latency",
      value:
        metrics?.values.average_latency_seconds == null
          ? "N/A"
          : `${displayNumber(latencyMilliseconds(metrics.values.average_latency_seconds), 2)} ms`,
    },
    {
      icon: "✓",
      title: "Healthy Services",
      value:
        healthyServices == null
          ? "N/A"
          : `${healthyServices}/${health?.services.length ?? 0}`,
    },
  ];

  return (
    <div className="overview">
      <div className="overview-toolbar">
        <div>
          <h2>Platform overview</h2>
          <p>Live commerce streaming and fraud operations</p>
        </div>
        <div className="refresh-state">
          <span className={error ? "status-dot unavailable" : "status-dot healthy"} />
          {error
            ? "Data stale"
            : updatedAt
              ? `Updated ${updatedAt.toLocaleTimeString()}`
              : "Connecting"}
        </div>
      </div>

      <section className="overview-kpis" aria-label="Platform key metrics">
        {kpis.map((kpi) => (
          <article className="overview-kpi" key={kpi.title}>
            <Icon>{kpi.icon}</Icon>
            <div>
              <span>{kpi.title}</span>
              <strong>{kpi.value}</strong>
              {"context" in kpi && kpi.context && (
                <small className="kpi-context">{kpi.context}</small>
              )}
            </div>
          </article>
        ))}
      </section>

      <section className="overview-chart-grid">
        <article className="overview-panel throughput-panel">
          <div className="panel-heading">
            <div>
              <span className="eyebrow">STREAMING</span>
              <h3>Event Throughput</h3>
            </div>
            <strong>
              {displayNumber(metrics?.values.processed_rate, 2)}
              {metrics?.values.processed_rate != null && <small> events/sec</small>}
            </strong>
          </div>
          <div
            className="chart-frame"
            role="img"
            aria-label="Processed events per second over recent refreshes"
          >
            {throughput.length === 0 ? (
              <div className="chart-empty">
                {metrics?.status === "degraded" ||
                metrics?.values.processed_rate == null
                  ? "Metrics unavailable"
                  : "Waiting for traffic"}
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={throughput} margin={{top: 8, right: 6, left: -26, bottom: 0}}>
                  <defs>
                    <linearGradient id="throughputFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#49a6ff" stopOpacity={0.32} />
                      <stop offset="100%" stopColor="#49a6ff" stopOpacity={0.02} />
                    </linearGradient>
                  </defs>
                  <XAxis dataKey="time" tick={{fill: "#718399", fontSize: 10}} tickLine={false} axisLine={false} minTickGap={35} />
                  <YAxis tick={{fill: "#718399", fontSize: 10}} tickLine={false} axisLine={false} />
                  <Tooltip contentStyle={{background: "#0d141d", border: "1px solid #263343", borderRadius: 8}} formatter={(value) => [`${displayNumber(Number(value), 2)} events/sec`, "Processed"]} />
                  <Area type="monotone" dataKey="value" stroke="#49a6ff" strokeWidth={2} fill="url(#throughputFill)" isAnimationActive />
                </AreaChart>
              </ResponsiveContainer>
            )}
          </div>
        </article>

        <article className="overview-panel decision-panel">
          <div className="panel-heading">
            <div>
              <span className="eyebrow">FRAUD ENGINE</span>
              <h3>Decision Distribution</h3>
              <FraudScope scope={fraudSummary?.scope ?? null} />
            </div>
          </div>
          <div
            className="decision-content"
            role="img"
            aria-label={`Fraud decisions: ${totalDecisions} total`}
          >
            <div className="donut-wrap">
              {totalDecisions > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie data={decisionData} dataKey="value" nameKey="name" innerRadius={58} outerRadius={78} paddingAngle={2} stroke="none">
                      {decisionData.map((item) => (
                        <Cell key={item.name} fill={DECISION_COLORS[item.name]} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={{background: "#0d141d", border: "1px solid #263343", borderRadius: 8}} />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <ZeroDecisionState />
              )}
              <div className="donut-center">
                <strong>{fraudSummary?.run_id ? totalDecisions : "N/A"}</strong>
                <span>Total Decisions</span>
              </div>
            </div>
            <div className="decision-legend">
              {decisionData.map((item) => (
                <div key={item.name}>
                  <span className="legend-dot" style={{background: DECISION_COLORS[item.name]}} />
                  <span>{item.name}</span>
                  <strong>{item.value}</strong>
                  <small>
                    {totalDecisions
                      ? `${Math.round((item.value / totalDecisions) * 100)}%`
                      : "N/A"}
                  </small>
                </div>
              ))}
            </div>
          </div>
        </article>
      </section>

      <section className="overview-data-grid">
        <article className="overview-panel recent-runs">
          <div className="panel-heading">
            <div>
              <span className="eyebrow">SCENARIO ACTIVITY</span>
              <h3>Recent Runs</h3>
            </div>
            <Link href="/runs">View all</Link>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr><th>Scenario</th><th>Status</th><th>Generated</th><th>Processed</th><th>BLOCK</th><th>Alerts</th><th>Duration</th><th>Created</th></tr>
              </thead>
              <tbody>
                {recentRuns.map((run) => (
                  <tr className="clickable-row" key={run.run_id} tabIndex={0} onClick={() => openRun(run.run_id)} onKeyDown={rowKeyDown(run.run_id)}>
                    <td>{run.scenario_type.replaceAll("_", " ")}</td>
                    <td><span className={`badge ${run.status.toLowerCase()}`}>{run.status}</span></td>
                    <td>{run.generated_event_count.toLocaleString()}</td>
                    <td>{run.processed_event_count.toLocaleString()}</td>
                    <td>{run.block_count.toLocaleString()}</td>
                    <td>{run.fraud_alert_count.toLocaleString()}</td>
                    <td>{formatDuration(run)}</td>
                    <td title={new Date(run.created_at).toLocaleString()}>{relativeTime(run.created_at)}</td>
                  </tr>
                ))}
                {recentRuns.length === 0 && <tr><td colSpan={8} className="table-empty">No runs yet.</td></tr>}
              </tbody>
            </table>
          </div>
        </article>

        <article className="overview-panel recent-alerts">
          <div className="panel-heading">
            <div>
              <span className="eyebrow">RISK SIGNALS</span>
              <h3>Recent Fraud Alerts</h3>
            </div>
            <Link href="/fraud">View all</Link>
          </div>
          <div className="table-scroll">
            <table>
              <thead><tr><th>Severity</th><th>Rule</th><th>Score</th><th>Time</th></tr></thead>
              <tbody>
                {alerts.slice(0, 6).map((alert) => (
                  <tr
                    className={alert.run_id ? "clickable-row" : undefined}
                    key={alert.alert_id}
                    tabIndex={alert.run_id ? 0 : undefined}
                    onClick={() => alert.run_id && openRun(alert.run_id)}
                    onKeyDown={alert.run_id ? rowKeyDown(alert.run_id) : undefined}
                  >
                    <td><span className={`severity ${alert.severity.toLowerCase()}`}>{alert.severity}</span></td>
                    <td title={alert.reason_codes.join(", ")}>{alert.reason_codes[0]?.replaceAll("_", " ") ?? "N/A"}</td>
                    <td className="score-cell">{alert.score}</td>
                    <td title={new Date(alert.created_at).toLocaleString()}>{relativeTime(alert.created_at)}</td>
                  </tr>
                ))}
                {alerts.length === 0 && <tr><td colSpan={4} className="table-empty">No recent alerts.</td></tr>}
              </tbody>
            </table>
          </div>
        </article>
      </section>

      <section className="overview-panel health-panel">
        <div className="panel-heading">
          <div>
            <span className="eyebrow">INFRASTRUCTURE</span>
            <h3>Platform Health</h3>
          </div>
          <span className={`badge ${(health?.overall ?? "UNKNOWN").toLowerCase()}`}>
            {health?.overall ?? "UNKNOWN"}
          </span>
        </div>
        <div className="health-grid">
          {serviceCards.map((service) => (
            <article className="health-service" key={service.name}>
              <span className={`status-dot ${service.state.toLowerCase()}`} />
              <div><strong>{service.name}</strong><small>{service.state === "UNHEALTHY" ? "Unavailable" : service.state.charAt(0) + service.state.slice(1).toLowerCase()}</small></div>
            </article>
          ))}
        </div>
      </section>

      <QuickActions />
    </div>
  );
}
