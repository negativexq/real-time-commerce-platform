"use client";

import {
  Bot,
  Copy,
  CreditCard,
  Dices,
  FileWarning,
  KeyRound,
  Play,
  Shuffle,
  Undo2,
  User,
  type LucideIcon,
} from "lucide-react";
import {useRouter} from "next/navigation";
import {useEffect, useState} from "react";
import {api} from "../lib/api";
import {cn} from "../lib/cn";
import {formatCount} from "../lib/formatting";
import {EVENT_PRESETS, expectedOutcomes, MAX_EVENTS, MAX_RATE, MAX_RUN_SECONDS, MIXED_DISTRIBUTION, RATE_PRESETS} from "../lib/run-limits";
import {humanize, toneOf, type Tone} from "../lib/tone";
import type {Run, Scenario} from "../lib/types";
import {SegmentMeter} from "./gauges";
import {NumberTicker} from "./number-ticker";
import {RangeField} from "./range-field";
import {Badge, Button, Panel, StatusDot} from "./ui";

const ICONS: Record<string, LucideIcon> = {
  normal_customer: User,
  suspicious_payment: CreditCard,
  account_takeover: KeyRound,
  bot_checkout: Bot,
  refund_abuse: Undo2,
  duplicate_delivery: Copy,
  malformed_event: FileWarning,
  mixed_traffic: Shuffle,
};

const PERSONA_TONES: Record<string, Tone> = {
  normal: "ok",
  suspicious: "warn",
  bot: "bad",
  account_takeover: "bad",
  discount_hunter: "flow",
  indecisive: "neutral",
};

export function estimatedDuration(count: number, rate: number) {
  if (!(count > 0) || !(rate > 0)) return "—";
  const seconds = Math.ceil(count / rate);
  if (seconds < 60) return `${seconds} s`;
  if (seconds < 3600) {
    const minutes = Math.floor(seconds / 60);
    return seconds % 60 ? `${minutes} min ${seconds % 60} s` : `${minutes} min`;
  }
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.round((seconds % 3600) / 60);
  return minutes ? `${hours} h ${minutes} min` : `${hours} h`;
}

function finishTime(now: number | null, count: number, rate: number) {
  if (now === null || !(count > 0) || !(rate > 0)) return "—";
  return new Date(now + Math.ceil(count / rate) * 1000).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function SummaryFigure({label, children, hint}: {label: string; children: React.ReactNode; hint?: string}) {
  return (
    <div className="min-w-0">
      <dt className="text-[13px] text-muted">{label}</dt>
      <dd className="figure mt-0.5 truncate text-[28px] font-semibold leading-none text-ink">{children}</dd>
      {hint && <dd className="mt-1 text-xs text-faint">{hint}</dd>}
    </div>
  );
}

export function ScenarioForm({scenarios}: {scenarios: Scenario[]}) {
  const router = useRouter();
  const [selected, setSelected] = useState(scenarios[0]?.scenario_type ?? "normal_customer");
  const [count, setCount] = useState(500);
  const [rate, setRate] = useState(20);
  const [seed, setSeed] = useState(42);
  const [error, setError] = useState("");
  const [starting, setStarting] = useState(false);
  // Wall clock only after mount, so the server render never disagrees with it.
  const [now, setNow] = useState<number | null>(null);

  useEffect(() => {
    setNow(Date.now());
    const timer = window.setInterval(() => setNow(Date.now()), 15_000);
    return () => window.clearInterval(timer);
  }, []);

  const current = scenarios.find((s) => s.scenario_type === selected);
  const countOk = Number.isInteger(count) && count >= 1 && count <= MAX_EVENTS;
  const rateOk = Number.isInteger(rate) && rate >= 1 && rate <= MAX_RATE;
  const seconds = countOk && rateOk ? Math.ceil(count / rate) : 0;
  const tooLong = seconds > MAX_RUN_SECONDS;
  const valid = countOk && rateOk && !tooLong && Number.isInteger(seed);
  const minRate = countOk ? Math.ceil(count / MAX_RUN_SECONDS) : 1;

  async function start() {
    setError("");
    setStarting(true);
    try {
      const body: Record<string, unknown> = {scenario_type: selected, event_count: count, events_per_second: rate, seed};
      if (selected === "mixed_traffic") body.persona_distribution = MIXED_DISTRIBUTION;
      const run = await api<Run>("/api/v1/runs", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(body),
      });
      router.push(`/runs/${run.run_id}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "The run could not be started.");
      setStarting(false);
    }
  }

  return (
    <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(360px,420px)]">
      <Panel title="Choose a scenario" description="Each one replays a fixed customer behaviour. The badges show what the fraud engine should decide.">
        <div role="radiogroup" aria-label="Scenario" className="grid gap-2 sm:grid-cols-2">
          {scenarios.map((s) => {
            const active = s.scenario_type === selected;
            const Icon = ICONS[s.scenario_type] ?? Play;
            const outcomes = expectedOutcomes(s.expected_outcome);
            return (
              <button
                key={s.scenario_type}
                type="button"
                role="radio"
                aria-checked={active}
                onClick={() => setSelected(s.scenario_type)}
                className={cn(
                  "group grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-2 rounded-[var(--radius-control)] border p-3.5 text-left transition-colors",
                  active ? "border-flow bg-flow-deep/50 ring-1 ring-flow" : "border-rule hover:border-rule-strong hover:bg-raised/50",
                )}
              >
                <span
                  className={cn(
                    "row-span-2 flex size-9 items-center justify-center rounded-[var(--radius-control)] transition-colors [&_svg]:size-[18px]",
                    active ? "bg-flow text-on-accent" : "bg-raised text-muted group-hover:text-ink",
                  )}
                >
                  <Icon aria-hidden="true" />
                </span>
                <span className="min-w-0">
                  <span className="block font-semibold text-ink">{s.title}</span>
                  <span className="block text-sm text-muted">{s.purpose}</span>
                </span>
                <span className="flex flex-wrap gap-1">
                  {outcomes.length ? (
                    outcomes.map((o) => <Badge key={o} value={o} tone={o === "DLQ" ? "warn" : toneOf(o)} />)
                  ) : (
                    <span className="text-xs text-faint">{s.expected_outcome}</span>
                  )}
                </span>
              </button>
            );
          })}
        </div>
      </Panel>

      <Panel title="Run settings" description={current ? `${current.title}: ${current.expected_outcome}` : undefined} className="self-start lg:sticky lg:top-8">
        <div className="grid gap-6">
          <RangeField label="Events" unit="events" value={count} max={MAX_EVENTS} presets={EVENT_PRESETS} onChange={setCount} />
          <RangeField label="Send rate" unit="per second" value={rate} max={MAX_RATE} presets={RATE_PRESETS} onChange={setRate} />

          <div className="grid gap-1.5">
            <label htmlFor="seed" className="text-[13px] font-medium text-ink-soft">
              Seed
            </label>
            <div className="flex gap-2">
              <input
                id="seed"
                type="number"
                inputMode="numeric"
                value={Number.isNaN(seed) ? "" : seed}
                onChange={(e) => setSeed(e.target.valueAsNumber)}
                className="figure h-10 min-w-0 flex-1 rounded-[var(--radius-control)] border border-rule bg-panel px-3 text-lg text-ink transition-colors hover:border-rule-strong focus-visible:border-flow focus-visible:outline-none"
              />
              <Button type="button" variant="secondary" onClick={() => setSeed(Math.floor(Math.random() * 1_000_000))}>
                <Dices aria-hidden="true" />
                Random
              </Button>
            </div>
            <span className="text-xs text-faint">The same seed and settings replay exactly the same traffic.</span>
          </div>

          {selected === "mixed_traffic" && (
            <div className="grid gap-2">
              <span className="text-[13px] font-medium text-ink-soft">Persona mix</span>
              <SegmentMeter
                segments={Object.entries(MIXED_DISTRIBUTION).map(([key, value]) => ({key, label: humanize(key), value, tone: PERSONA_TONES[key] ?? "neutral"}))}
              />
              <ul className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-muted">
                {Object.entries(MIXED_DISTRIBUTION).map(([key, value]) => (
                  <li key={key} className="flex items-center gap-1.5">
                    <StatusDot tone={PERSONA_TONES[key] ?? "neutral"} />
                    {humanize(key)}
                    <span className="figure ml-auto text-ink-soft">{value}%</span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          <dl className="grid grid-cols-2 gap-x-4 gap-y-5 rounded-[var(--radius-control)] bg-raised/70 p-4" aria-live="polite">
            <SummaryFigure label="Takes about">{countOk && rateOk ? <span className={tooLong ? "text-bad" : undefined}>{estimatedDuration(count, rate)}</span> : "—"}</SummaryFigure>
            <SummaryFigure label="Finishes around">{valid ? finishTime(now, count, rate) : "—"}</SummaryFigure>
            <SummaryFigure label="Events">{countOk ? <NumberTicker value={count} duration={300} /> : "—"}</SummaryFigure>
            <SummaryFigure label="Per second">{rateOk ? <NumberTicker value={rate} duration={300} /> : "—"}</SummaryFigure>
          </dl>
          {tooLong ? (
            <div role="alert" className="-mt-3 flex flex-wrap items-center justify-between gap-2 rounded-[var(--radius-control)] bg-bad-deep px-3 py-2 text-sm text-bad">
              <span>Runs stop after one hour. At this rate it would need {estimatedDuration(count, rate)}.</span>
              <button type="button" onClick={() => setRate(minRate)} className="font-semibold underline underline-offset-2">
                Use {formatCount(minRate)} per second
              </button>
            </div>
          ) : (
            valid &&
            seconds > 600 && <p className="-mt-3 text-xs text-warn">This run takes over ten minutes. You can stop it from its run page at any time.</p>
          )}

          {error && (
            <p role="alert" className="rounded-[var(--radius-control)] bg-bad-deep px-3 py-2 text-sm text-bad">
              {error}
            </p>
          )}
          <Button variant="primary" onClick={start} disabled={!valid || starting} className="h-11">
            <Play aria-hidden="true" />
            {starting ? "Starting" : valid ? `Send ${formatCount(count)} events` : "Fix the settings to start"}
          </Button>
        </div>
      </Panel>
    </div>
  );
}
