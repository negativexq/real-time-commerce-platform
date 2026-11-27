import {Fragment} from "react";
import {cn} from "../lib/cn";
import {TONE_HEX, toneOf, type Tone} from "../lib/tone";
import type {HealthState} from "../lib/types";
import {StatusDot, textTone} from "./ui";

export type PipelineStage = {
  name: string;
  state: HealthState;
  figure: string;
  unit: string;
  /** Colour for the figure when it carries a judgement (lag, backlog). */
  tone?: Tone;
  stores?: {name: string; state: HealthState}[];
};

function stateText(state: HealthState) {
  if (state === "NOT_MONITORED") return "Not monitored";
  if (state === "UNHEALTHY") return "Down";
  return state.charAt(0) + state.slice(1).toLowerCase();
}

/** Events travel left to right; the dashes move only while they do. */
function Link({flowing}: {flowing: boolean}) {
  return (
    <li role="presentation" aria-hidden="true" className="hidden items-center xl:flex">
    <svg
      className="pipeline-link h-3 w-full min-w-6"
      data-flowing={flowing}
      viewBox="0 0 48 12"
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      <line x1="0" y1="6" x2="42" y2="6" style={{stroke: flowing ? TONE_HEX.flow : "var(--color-rule-strong)"}} strokeWidth="2" strokeDasharray="6 6" vectorEffect="non-scaling-stroke" />
      <path d="M42 2 L48 6 L42 10 Z" style={{fill: flowing ? TONE_HEX.flow : "var(--color-rule-strong)"}} />
    </svg>
    </li>
  );
}

export function PipelineStrip({stages, flowing}: {stages: PipelineStage[]; flowing: boolean}) {
  return (
    <section aria-label="Event pipeline" className="rounded-[var(--radius-panel)] border border-rule bg-panel p-5">
      <div className="mb-4 flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="text-[15px] font-semibold">Event pipeline</h2>
        <span className="text-[13px] text-muted">{flowing ? "Events are flowing" : "Idle, no events in the last two minutes"}</span>
      </div>
      <ol className="grid grid-cols-1 gap-3 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-[repeat(4,minmax(0,1fr)_36px)_minmax(0,1fr)] xl:gap-0">
        {stages.map((stage, index) => {
          const health = toneOf(stage.state);
          return (
            <Fragment key={stage.name}>
              <li
                className={cn(
                  "flex min-w-0 flex-col gap-3 rounded-[var(--radius-control)] border bg-raised/50 p-4",
                  health === "bad" ? "border-bad/60" : health === "warn" ? "border-warn/50" : "border-rule",
                )}
              >
                <div className="flex items-center gap-2" title={`${stage.name}: ${stateText(stage.state)}`}>
                  <StatusDot tone={health} pulse={flowing && health === "ok"} />
                  <span className="truncate text-sm font-medium text-ink">{stage.name}</span>
                  <span className={cn("sr-only", stage.state !== "HEALTHY" && "not-sr-only ml-auto shrink-0 text-xs", health === "bad" ? "text-bad" : health === "warn" ? "text-warn" : "text-faint")}>
                    {stateText(stage.state)}
                  </span>
                </div>
                <div>
                  <div className={cn("figure text-[32px] font-semibold leading-none", stage.figure === "N/A" ? "text-faint" : textTone[stage.tone ?? "neutral"])}>
                    {stage.figure}
                  </div>
                  <div className="mt-1 text-[13px] text-muted">{stage.unit}</div>
                </div>
                {stage.stores && (
                  <div className="flex flex-wrap gap-1.5 border-t border-rule pt-3">
                    {stage.stores.map((store) => (
                      <span key={store.name} className="inline-flex items-center gap-1.5 rounded-full bg-base/70 px-2 py-0.5 text-xs text-ink-soft" title={`${store.name}: ${stateText(store.state)}`}>
                        <StatusDot tone={toneOf(store.state)} />
                        {store.name}
                      </span>
                    ))}
                  </div>
                )}
              </li>
              {index < stages.length - 1 && <Link flowing={flowing} />}
            </Fragment>
          );
        })}
      </ol>
    </section>
  );
}
