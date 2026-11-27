"use client";

import {ArrowDownRight, ArrowUpRight, Minus} from "lucide-react";
import {cn} from "../lib/cn";
import type {Tone} from "../lib/tone";
import {Sparkline} from "./gauges";
import {NumberTicker} from "./number-ticker";
import {textTone} from "./ui";

export type BandMetric = {
  label: string;
  value: number | null | undefined;
  digits?: number;
  unit: string;
  history: number[];
  tone: Tone;
  /** Throughput goes up when things are good; latency, lag and backlog do not. */
  higherIsBetter?: boolean;
};

/** Change between the last two samples, relative to the earlier one. */
export function sampleDelta(history: number[]) {
  if (history.length < 2) return null;
  const prev = history[history.length - 2];
  const last = history[history.length - 1];
  if (prev === last) return 0;
  if (prev === 0) return last > 0 ? Infinity : -Infinity;
  return (last - prev) / Math.abs(prev);
}

function Delta({history, higherIsBetter}: {history: number[]; higherIsBetter?: boolean}) {
  const delta = sampleDelta(history);
  if (delta === null) return <span className="text-faint">Collecting</span>;
  if (Math.abs(delta) < 0.005) {
    return (
      <span className="inline-flex items-center gap-0.5 text-faint">
        <Minus className="size-3.5" aria-hidden="true" /> Steady
      </span>
    );
  }
  const up = delta > 0;
  const good = higherIsBetter ? up : !up;
  const Icon = up ? ArrowUpRight : ArrowDownRight;
  const text = Number.isFinite(delta) ? `${Math.round(Math.abs(delta) * 100)}%` : "from 0";
  return (
    <span className={cn("figure inline-flex items-center gap-0.5", good ? "text-ok" : "text-warn")}>
      <Icon className="size-3.5" aria-hidden="true" />
      {text}
      <span className="sr-only">{up ? "up" : "down"} since last sample</span>
    </span>
  );
}

/**
 * The headline numbers. One surface split by hairlines rather than a row
 * of identical cards, so the figures read as a single instrument.
 */
export function MetricBand({metrics}: {metrics: BandMetric[]}) {
  return (
    <section
      aria-label="Headline metrics"
      className="grid grid-cols-2 overflow-hidden rounded-[var(--radius-panel)] border border-rule bg-panel sm:grid-cols-3 xl:grid-cols-6"
    >
      {metrics.map((m) => {
        const known = m.value != null && Number.isFinite(m.value);
        return (
          <div key={m.label} className="flex min-w-0 flex-col gap-2 border-rule p-5 pb-3 [box-shadow:inset_-1px_-1px_0_var(--color-rule)]">
            <div className="flex items-center justify-between gap-2 text-[13px]">
              <span className="truncate text-muted">{m.label}</span>
              <Delta history={m.history} higherIsBetter={m.higherIsBetter} />
            </div>
            <div className="flex items-baseline gap-1.5">
              <span className={cn("figure text-[44px] font-semibold leading-none", known ? textTone[m.tone === "flow" ? "neutral" : m.tone] : "text-faint")}>
                {known ? <NumberTicker value={m.value as number} digits={m.digits ?? 0} /> : "N/A"}
              </span>
              <span className="truncate text-sm text-muted">{m.unit}</span>
            </div>
            <Sparkline values={m.history} tone={m.tone === "neutral" ? "flow" : m.tone} />
          </div>
        );
      })}
    </section>
  );
}
