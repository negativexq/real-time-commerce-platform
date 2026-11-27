import Link from "next/link";
import {percent, SegmentMeter} from "../../components/gauges";
import {Meter} from "../../components/meter";
import {Badge, DataTable, Empty, PageHeader, Panel, StatusDot} from "../../components/ui";
import {api} from "../../lib/api";
import {formatCount} from "../../lib/formatting";
import {humanize, TONE_HEX, toneOf} from "../../lib/tone";
import {Time} from "../../components/time";

export const dynamic = "force-dynamic";

type FraudSummary = {
  decisions: {APPROVE: number; REVIEW: number; BLOCK: number};
  total_evaluations: number;
  alert_severities: Record<string, number>;
  total_alerts: number;
};
type Alert = {
  alert_id: string;
  run_id: string | null;
  decision: string;
  severity: string;
  score: number;
  reason_codes: string[];
  outbox_status: string | null;
  created_at: string;
  detected_at: string | null;
};

const SEVERITIES = ["CRITICAL", "HIGH", "MEDIUM", "LOW"];

export default async function Fraud() {
  const [summary, alerts] = await Promise.all([
    api<FraudSummary>("/api/v1/fraud/summary"),
    api<{items: Alert[]}>("/api/v1/fraud/alerts?page_size=50"),
  ]);
  const total = summary.total_evaluations;
  const decisions = summary.decisions;

  return (
    <div className="grid gap-5">
      <PageHeader title="Fraud" description="Decisions from the rule engine across every run, and the alerts it raised." />

      <div className="grid gap-5 xl:grid-cols-[minmax(0,3fr)_minmax(320px,2fr)]">
        <Panel title="Decisions" description="All evaluations stored in PostgreSQL">
          <div className="grid gap-5">
            <div className="grid grid-cols-2 gap-6 md:grid-cols-4">
              <div>
                <div className="text-[13px] text-muted">Evaluated</div>
                <div className="figure mt-1 text-[40px] font-semibold leading-none">{formatCount(total)}</div>
              </div>
              {(["APPROVE", "REVIEW", "BLOCK"] as const).map((name) => (
                <div key={name}>
                  <div className="flex items-center gap-1.5 text-[13px] text-muted">
                    <StatusDot tone={toneOf(name)} />
                    {humanize(name)}
                  </div>
                  <div className="figure mt-1 text-[40px] font-semibold leading-none" style={{color: TONE_HEX[toneOf(name)]}}>
                    {formatCount(decisions[name])}
                  </div>
                  <div className="figure mt-1 text-xs text-faint">{percent(decisions[name], total)}</div>
                </div>
              ))}
            </div>
            <SegmentMeter
              total={total}
              segments={[
                {key: "a", label: "Approve", value: decisions.APPROVE, tone: "ok"},
                {key: "r", label: "Review", value: decisions.REVIEW, tone: "warn"},
                {key: "b", label: "Block", value: decisions.BLOCK, tone: "bad"},
              ]}
            />
          </div>
        </Panel>

        <Panel title="Alerts by severity" description={`${formatCount(summary.total_alerts)} alerts raised`}>
          <div className="grid gap-4">
            {SEVERITIES.filter((s) => s in summary.alert_severities || s !== "LOW").map((severity) => {
              const count = summary.alert_severities[severity] ?? 0;
              return (
                <Meter
                  key={severity}
                  label={humanize(severity)}
                  value={count}
                  max={summary.total_alerts}
                  tone={toneOf(severity) === "neutral" ? "flow" : toneOf(severity)}
                  valueText={`${formatCount(count)}  (${percent(count, summary.total_alerts)})`}
                />
              );
            })}
          </div>
        </Panel>
      </div>

      <Panel title="Latest alerts" description="Newest detected first">
        {!alerts.items.length ? (
          <Empty text="No alerts yet. Suspicious, bot or account-takeover scenarios raise them." />
        ) : (
          <DataTable fit minWidth={880}>
            <thead>
              <tr>
                <th className="w-[84px]">Score</th>
                <th className="w-[112px]">Decision</th>
                <th className="w-[96px]">Severity</th>
                <th>Rules matched</th>
                <th className="w-[124px]">Published</th>
                <th className="w-[104px]">Run</th>
                <th className="w-[136px] text-right">Detected</th>
              </tr>
            </thead>
            <tbody>
              {alerts.items.map((a) => (
                <tr key={a.alert_id}>
                  <td>
                    <span
                      className="figure inline-flex h-7 min-w-10 items-center justify-center rounded-[var(--radius-control)] px-2 text-sm font-semibold"
                      style={{background: `color-mix(in oklab, ${TONE_HEX[toneOf(a.severity)]} 14%, transparent)`, color: TONE_HEX[toneOf(a.severity)]}}
                    >
                      {a.score}
                    </span>
                  </td>
                  <td>
                    <Badge value={a.decision} />
                  </td>
                  <td className="text-ink-soft">{humanize(a.severity)}</td>
                  <td>
                    <div className="flex flex-wrap gap-1">
                      {a.reason_codes.map((code) => (
                        <span key={code} className="rounded bg-raised px-1.5 py-0.5 text-xs text-ink-soft">
                          {humanize(code)}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td>{a.outbox_status ? <Badge value={a.outbox_status} /> : <span className="text-faint">—</span>}</td>
                  <td>
                    {a.run_id ? (
                      <Link className="figure text-flow hover:underline" href={`/runs/${a.run_id}`}>
                        {a.run_id.slice(0, 8)}
                      </Link>
                    ) : (
                      <span className="text-faint">—</span>
                    )}
                  </td>
                  <td className="text-right text-muted"><Time value={a.detected_at ?? a.created_at} /></td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </Panel>
    </div>
  );
}
