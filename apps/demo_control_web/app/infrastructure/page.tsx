import {RadialGauge} from "../../components/gauges";
import {Badge, PageHeader, Panel, StatusDot} from "../../components/ui";
import {api} from "../../lib/api";
import {toneOf} from "../../lib/tone";
import type {HealthState} from "../../lib/types";
import {Time} from "../../components/time";

export const dynamic = "force-dynamic";

type Health = {overall: HealthState; checked_at: string; services: {name: string; state: HealthState}[]};

const GROUPS: [title: string, description: string, names: string[]][] = [
  ["Pipeline", "Where events are produced, moved and decided", ["Generator", "Kafka", "Event Processor", "Fraud Engine", "Fraud Outbox Publisher", "DLQ Sink"]],
  ["Storage", "Idempotency leases and the durable record", ["Redis", "PostgreSQL"]],
  ["Observability", "Metrics, dashboards and the exporters behind them", ["Prometheus", "Grafana", "Kafka UI", "Kafka Exporter", "PostgreSQL Exporter", "Redis Exporter"]],
  ["Control", "This console's API", ["Demo Control API"]],
];

const NOTES: Record<string, string> = {
  "Fraud Engine": "Runs inside the event processor",
  Generator: "Only reachable while a run is active",
  "DLQ Sink": "Copies dead letters from Kafka into PostgreSQL",
};

function label(state: HealthState) {
  if (state === "NOT_MONITORED") return "NOT_MONITORED";
  if (state === "UNHEALTHY") return "DOWN";
  return state;
}

export default async function Infra() {
  const data = await api<Health>("/api/v1/platform/health");
  const byName = new Map(data.services.map((s) => [s.name, s.state]));
  const monitored = data.services.filter((s) => s.state !== "UNKNOWN" && s.state !== "NOT_MONITORED");
  const healthy = monitored.filter((s) => s.state === "HEALTHY").length;
  const listed = new Set(GROUPS.flatMap(([, , names]) => names));
  const others = data.services.filter((s) => !listed.has(s.name)).map((s) => s.name);
  const groups = others.length ? [...GROUPS, ["Other", "", others] as (typeof GROUPS)[number]] : GROUPS;

  return (
    <div className="grid gap-5">
      <PageHeader
        title="Infrastructure"
        description="Protocol checks and Prometheus scrape results, cached for a few seconds. A running container is not counted as healthy on its own."
      />
      <Panel bodyClassName="flex flex-wrap items-center gap-8">
        <RadialGauge
          label="Monitored services up"
          value={healthy}
          max={monitored.length || 1}
          display={`${healthy}/${monitored.length}`}
          tone={healthy === monitored.length ? "ok" : monitored.length - healthy > 1 ? "bad" : "warn"}
        />
        <div className="grid gap-1">
          <div className="text-[13px] text-muted">Platform</div>
          <div><Badge value={data.overall} dot /></div>
          <div className="mt-2 text-sm text-muted">Checked <Time value={data.checked_at} /></div>
        </div>
      </Panel>
      <div className="grid gap-5 lg:grid-cols-2">
        {groups.map(([title, description, names]) => (
          <Panel key={title} title={title} description={description || undefined} bodyClassName="pt-2">
            <ul className="divide-y divide-rule">
              {names.map((name) => {
                const state = byName.get(name) ?? "UNKNOWN";
                return (
                  <li key={name} className="flex items-center gap-3 py-2.5">
                    <StatusDot tone={toneOf(state)} />
                    <div className="min-w-0 flex-1">
                      <div className="truncate font-medium">{name}</div>
                      {NOTES[name] && <div className="text-xs text-faint">{NOTES[name]}</div>}
                    </div>
                    <Badge value={label(state)} tone={toneOf(state)} />
                  </li>
                );
              })}
            </ul>
          </Panel>
        ))}
      </div>
    </div>
  );
}
