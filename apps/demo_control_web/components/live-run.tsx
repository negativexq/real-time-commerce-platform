"use client";

import {Square} from "lucide-react";
import {useEffect, useRef, useState} from "react";
import {api, publicApiBase} from "../lib/api";
import {formatCount} from "../lib/formatting";
import {humanize} from "../lib/tone";
import type {Run} from "../lib/types";
import {percent, SegmentMeter} from "./gauges";
import {Meter} from "./meter";
import {NumberTicker} from "./number-ticker";
import {Badge, Button, Panel, StatusDot} from "./ui";
import {Time} from "./time";

export const TERMINAL_STATUSES = new Set(["COMPLETED", "FAILED", "STOPPED"]);

export type StreamState = "live" | "reconnecting" | "interrupted" | "finished";

/**
 * What the page should say about the stream after an EventSource error.
 * The API closes the stream itself once a run is terminal, which also fires
 * onerror; that is a normal end, not an interruption.
 */
export function streamStateAfterError(status: string, readyState: number): StreamState {
  if (TERMINAL_STATUSES.has(status)) return "finished";
  return readyState === 2 /* EventSource.CLOSED */ ? "interrupted" : "reconnecting";
}

function BigFigure({label, value, tone}: {label: string; value: number; tone?: string}) {
  return (
    <div className="min-w-0">
      <div className="text-[13px] text-muted">{label}</div>
      <div className={`figure mt-1 text-[34px] font-semibold leading-none ${tone ?? "text-ink"}`}>
        <NumberTicker value={value} />
      </div>
    </div>
  );
}

export function LiveRun({initial}: {initial: Run}) {
  const [run, setRun] = useState(initial);
  const [stream, setStream] = useState<StreamState>(TERMINAL_STATUSES.has(initial.status) ? "finished" : "live");
  const [stopping, setStopping] = useState(false);
  const statusRef = useRef(initial.status as string);

  useEffect(() => {
    if (TERMINAL_STATUSES.has(statusRef.current)) return;
    const source = new EventSource(`${publicApiBase()}/api/v1/runs/${initial.run_id}/stream`);
    source.addEventListener("progress", (event) => {
      const next = JSON.parse((event as MessageEvent).data) as Run;
      statusRef.current = next.status;
      setRun(next);
      setStream(TERMINAL_STATUSES.has(next.status) ? "finished" : "live");
    });
    source.addEventListener("completed", () => {
      // The server ends the stream here; close so EventSource does not reconnect.
      source.close();
      setStream("finished");
      void api<Run>(`/api/v1/runs/${initial.run_id}`).then(setRun).catch(() => undefined);
    });
    source.onerror = () => {
      const next = streamStateAfterError(statusRef.current, source.readyState);
      if (next === "finished") source.close();
      setStream(next);
    };
    return () => source.close();
  }, [initial.run_id]);

  async function stop() {
    setStopping(true);
    try {
      setRun(await api<Run>(`/api/v1/runs/${run.run_id}/stop`, {method: "POST"}));
    } finally {
      setStopping(false);
    }
  }

  const progress = run.requested_event_count ? run.generated_event_count / run.requested_event_count : 0;
  const decided = run.approve_count + run.review_count + run.block_count;
  const canStop = run.status === "RUNNING" || run.status === "STARTING";

  return (
    <div className="grid gap-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-3">
            <h1 className="text-2xl font-semibold tracking-tight">{humanize(run.scenario_type)}</h1>
            <Badge value={run.status} dot />
          </div>
          <p className="figure mt-1 truncate text-sm text-muted">
            {run.run_id}, started <Time value={run.created_at} />
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span className="flex items-center gap-2 text-sm text-muted" role="status">
            <StatusDot tone={stream === "live" ? "flow" : stream === "finished" ? "ok" : "warn"} pulse={stream === "live"} />
            {stream === "live" && "Live"}
            {stream === "reconnecting" && "Reconnecting to the live stream"}
            {stream === "interrupted" && "Live stream lost. Refresh for current data."}
            {stream === "finished" && "Run finished"}
          </span>
          {canStop && (
            <Button variant="danger" size="sm" onClick={stop} disabled={stopping}>
              <Square aria-hidden="true" />
              {stopping ? "Stopping" : "Stop run"}
            </Button>
          )}
        </div>
      </div>

      <Panel>
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <div className="text-[13px] text-muted">Generated</div>
            <div className="figure text-[56px] font-semibold leading-none">
              {percent(run.generated_event_count, run.requested_event_count)}
            </div>
          </div>
          <div className="figure text-right text-sm text-muted">
            {formatCount(run.generated_event_count)} of {formatCount(run.requested_event_count)} events
          </div>
        </div>
        <Meter className="mt-4" value={progress * 100} max={100} tone={run.status === "FAILED" ? "bad" : run.status === "COMPLETED" ? "ok" : "flow"} />
      </Panel>

      <div className="grid gap-5 lg:grid-cols-2">
        <Panel title="Pipeline">
          <div className="grid grid-cols-3 gap-6">
            <BigFigure label="Generated" value={run.generated_event_count} />
            <BigFigure label="Processed" value={run.processed_event_count} />
            <BigFigure label="Dead letters" value={run.dlq_count} tone={run.dlq_count ? "text-warn" : undefined} />
          </div>
        </Panel>
        <Panel title="Fraud decisions" description={decided ? `${formatCount(decided)} decided, ${formatCount(run.fraud_alert_count)} alerts raised` : "No decisions yet"}>
          <div className="grid gap-4">
            <div className="grid grid-cols-3 gap-6">
              <BigFigure label="Approve" value={run.approve_count} tone="text-ok" />
              <BigFigure label="Review" value={run.review_count} tone="text-warn" />
              <BigFigure label="Block" value={run.block_count} tone="text-bad" />
            </div>
            <SegmentMeter
              total={decided}
              segments={[
                {key: "a", label: "Approve", value: run.approve_count, tone: "ok"},
                {key: "r", label: "Review", value: run.review_count, tone: "warn"},
                {key: "b", label: "Block", value: run.block_count, tone: "bad"},
              ]}
            />
          </div>
        </Panel>
      </div>
    </div>
  );
}
