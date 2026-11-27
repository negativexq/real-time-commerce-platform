import type {ReactNode} from "react";
import {cn} from "../lib/cn";
import {TONE_HEX, type Tone} from "../lib/tone";

/* Radial gauge -------------------------------------------------------- */

const SWEEP = 240; // degrees of arc, opening at the bottom
const START = 90 + (360 - SWEEP) / 2; // measured clockwise from 3 o'clock

function polar(cx: number, cy: number, r: number, degrees: number) {
  const radians = (degrees * Math.PI) / 180;
  // Rounded so server and client render identical attribute strings.
  const round = (n: number) => Math.round(n * 100) / 100;
  return {x: round(cx + r * Math.cos(radians)), y: round(cy + r * Math.sin(radians))};
}

function arcPath(cx: number, cy: number, r: number, from: number, to: number) {
  const a = polar(cx, cy, r, from);
  const b = polar(cx, cy, r, to);
  const large = to - from > 180 ? 1 : 0;
  return `M ${a.x} ${a.y} A ${r} ${r} 0 ${large} 1 ${b.x} ${b.y}`;
}

/** Tone for a value where higher is worse (latency, lag). */
export function thresholdTone(
  value: number | null | undefined,
  warn: number,
  bad: number,
): Tone {
  if (value == null || !Number.isFinite(value)) return "neutral";
  if (value >= bad) return "bad";
  if (value >= warn) return "warn";
  return "ok";
}

export type GaugeScale = "linear" | "sqrt";

/**
 * Fraction of the arc to fill, clamped to [0, 1]; null when unknown.
 * "sqrt" spreads out the low end, so a healthy 5 ms still moves the needle
 * on a scale that has to reach the 250 ms alert line.
 */
export function gaugeFraction(value: number | null | undefined, max: number, scale: GaugeScale = "linear") {
  if (value == null || !Number.isFinite(value) || max <= 0) return null;
  const linear = Math.min(1, Math.max(0, value / max));
  return scale === "sqrt" ? Math.sqrt(linear) : linear;
}

export function RadialGauge({
  label,
  value,
  max,
  display,
  unit,
  tone,
  marks = [],
  scale = "linear",
  caption,
}: {
  label: string;
  value: number | null | undefined;
  max: number;
  display: string;
  unit?: string;
  tone: Tone;
  /** Values on the scale to tick, such as the warn and alert thresholds. */
  marks?: number[];
  scale?: GaugeScale;
  caption?: ReactNode;
}) {
  const size = 132;
  const c = size / 2;
  const r = 52;
  const fraction = gaugeFraction(value, max, scale);
  const end = START + SWEEP * (fraction ?? 0);
  const color = TONE_HEX[tone];
  return (
    <figure className="flex min-w-0 flex-col items-center" aria-label={`${label}: ${display}${unit ? ` ${unit}` : ""}`}>
      <div className="relative" style={{width: size, height: size * 0.86}}>
        <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size} className="absolute inset-x-0 top-0" aria-hidden="true">
          <path d={arcPath(c, c, r, START, START + SWEEP)} fill="none" style={{stroke: "var(--color-raised)"}} strokeWidth={9} strokeLinecap="round" />
          {fraction !== null && fraction > 0 && (
            <path
              d={arcPath(c, c, r, START, end)}
              fill="none"
              style={{stroke: color, transition: "d 600ms ease"}}
              strokeWidth={9}
              strokeLinecap="round"
            />
          )}
          {marks.map((mark) => {
            const at = START + SWEEP * (gaugeFraction(mark, max, scale) ?? 0);
            const inner = polar(c, c, r - 9, at);
            const outer = polar(c, c, r + 9, at);
            return <line key={mark} x1={inner.x} y1={inner.y} x2={outer.x} y2={outer.y} style={{stroke: "var(--color-faint)"}} strokeWidth={1.5} />;
          })}
        </svg>
        <div className="absolute inset-x-0 top-[38%] text-center">
          <div className="figure text-[26px] font-semibold leading-none text-ink">{display}</div>
          {unit && <div className="mt-1 text-xs text-muted">{unit}</div>}
        </div>
      </div>
      <figcaption className="-mt-1 text-center">
        <div className="text-sm font-medium text-ink-soft">{label}</div>
        {caption && <div className="mt-0.5 text-xs text-faint">{caption}</div>}
      </figcaption>
    </figure>
  );
}

/* Sparkline ----------------------------------------------------------- */

export function Sparkline({
  values,
  tone = "flow",
  height = 36,
  className,
}: {
  values: number[];
  tone?: Tone;
  height?: number;
  className?: string;
}) {
  const width = 120;
  if (values.length < 2) {
    return <div className={cn("h-9", className)} aria-hidden="true" />;
  }
  const max = Math.max(...values, 1e-9);
  const step = width / (values.length - 1);
  const points = values.map((v, i) => `${(i * step).toFixed(1)},${(height - 2 - (v / max) * (height - 4)).toFixed(1)}`);
  const color = TONE_HEX[tone];
  return (
    <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" className={cn("h-9 w-full", className)} aria-hidden="true">
      <polygon points={`0,${height} ${points.join(" ")} ${width},${height}`} style={{fill: color}} opacity={0.14} />
      <polyline points={points.join(" ")} fill="none" style={{stroke: color}} strokeWidth={1.5} vectorEffect="non-scaling-stroke" strokeLinejoin="round" />
    </svg>
  );
}

/* Segmented meter ----------------------------------------------------- */

export type Segment = {key: string; label: string; value: number; tone: Tone};

export function SegmentMeter({
  segments,
  total,
  className,
}: {
  segments: Segment[];
  total?: number;
  className?: string;
}) {
  const sum = total ?? segments.reduce((acc, s) => acc + s.value, 0);
  return (
    <div className={cn("w-full", className)}>
      <div className="flex h-2.5 w-full gap-[3px] overflow-hidden rounded-full bg-raised" role="img" aria-label={segments.map((s) => `${s.label} ${s.value}`).join(", ")}>
        {sum > 0 &&
          segments
            .filter((s) => s.value > 0)
            .map((s) => (
              <span
                key={s.key}
                className="h-full transition-[width] duration-500 first:rounded-l-full last:rounded-r-full"
                style={{width: `${(s.value / sum) * 100}%`, background: TONE_HEX[s.tone], minWidth: 4}}
              />
            ))}
      </div>
    </div>
  );
}

export function percent(part: number, whole: number) {
  if (!whole) return "—";
  const value = (part / whole) * 100;
  return `${value < 10 && value > 0 ? value.toFixed(1) : Math.round(value)}%`;
}
