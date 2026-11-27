import Link from "next/link";
import {SegmentMeter} from "../../components/gauges";
import {Meter} from "../../components/meter";
import {Badge, buttonVariants, DataTable, Empty, PageHeader, Panel, Stat} from "../../components/ui";
import {api} from "../../lib/api";
import {formatCount, shortId} from "../../lib/formatting";
import {humanize} from "../../lib/tone";
import type {Run} from "../../lib/types";
import {Time} from "../../components/time";

export const dynamic = "force-dynamic";

export default async function Runs() {
  const data = await api<{items: Run[]}>("/api/v1/runs?page_size=100");
  const runs = data.items;
  const processed = runs.reduce((n, r) => n + r.processed_event_count, 0);
  const alerts = runs.reduce((n, r) => n + r.fraud_alert_count, 0);
  const blocked = runs.reduce((n, r) => n + r.block_count, 0);
  const completed = runs.filter((r) => r.status === "COMPLETED").length;

  return (
    <div className="grid gap-5">
      <PageHeader
        title="Runs"
        description="Every scenario run, newest first. Open one to follow it live."
        action={
          <Link className={buttonVariants({variant: "primary", size: "sm"})} href="/scenarios">
            Launch scenario
          </Link>
        }
      />
      <Panel bodyClassName="grid grid-cols-2 gap-6 md:grid-cols-4">
        <Stat label="Runs" value={formatCount(runs.length)} hint={`${formatCount(completed)} completed`} />
        <Stat label="Events processed" value={formatCount(processed)} />
        <Stat label="Blocked" value={formatCount(blocked)} tone={blocked ? "bad" : undefined} />
        <Stat label="Fraud alerts" value={formatCount(alerts)} tone={alerts ? "warn" : undefined} />
      </Panel>
      <Panel title="Run history">
        {!runs.length ? (
          <Empty
            text="No runs yet. Launch a scenario to send traffic through the pipeline."
            action={
              <Link className={buttonVariants({size: "sm"})} href="/scenarios">
                Open scenarios
              </Link>
            }
          />
        ) : (
          <DataTable fit minWidth={920}>
            <thead>
              <tr>
                <th>Run</th>
                <th className="w-[116px]">Status</th>
                <th className="w-[160px]">Processed</th>
                <th className="w-[220px]">Decisions</th>
                <th className="w-[72px] text-right">Alerts</th>
                <th className="w-[92px] text-right">Dead letters</th>
                <th className="w-[124px] text-right">Started</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r) => {
                const decided = r.approve_count + r.review_count + r.block_count;
                return (
                  <tr key={r.run_id}>
                    <td>
                      <div className="truncate font-medium">{humanize(r.scenario_type)}</div>
                      <Link className="figure text-xs text-flow hover:underline" href={`/runs/${r.run_id}`}>
                        {shortId(r.run_id)}
                      </Link>
                    </td>
                    <td>
                      <Badge value={r.status} dot />
                    </td>
                    <td>
                      <Meter value={r.processed_event_count} max={r.requested_event_count} tone={r.status === "FAILED" ? "bad" : "flow"} className="w-full max-w-40" />
                      <div className="figure mt-1 text-xs text-muted">
                        {formatCount(r.processed_event_count)} of {formatCount(r.requested_event_count)}
                      </div>
                    </td>
                    <td>
                      <SegmentMeter
                        className="w-full max-w-48"
                        total={decided}
                        segments={[
                          {key: "a", label: "Approve", value: r.approve_count, tone: "ok"},
                          {key: "r", label: "Review", value: r.review_count, tone: "warn"},
                          {key: "b", label: "Block", value: r.block_count, tone: "bad"},
                        ]}
                      />
                      <div className="figure mt-1 flex flex-wrap gap-x-2 text-xs">
                        <span className="text-ok">{formatCount(r.approve_count)} approved</span>
                        <span className="text-warn">{formatCount(r.review_count)} review</span>
                        <span className="text-bad">{formatCount(r.block_count)} blocked</span>
                      </div>
                    </td>
                    <td className="figure text-right">{formatCount(r.fraud_alert_count)}</td>
                    <td className={r.dlq_count ? "figure text-right text-warn" : "figure text-right text-muted"}>{formatCount(r.dlq_count)}</td>
                    <td className="text-right text-muted"><Time value={r.created_at} /></td>
                  </tr>
                );
              })}
            </tbody>
          </DataTable>
        )}
      </Panel>
    </div>
  );
}
