import {Meter as BaseMeter} from "@base-ui/react/meter";
import type {ReactNode} from "react";
import {cn} from "../lib/cn";
import {TONE_HEX, type Tone} from "../lib/tone";

/**
 * Linear value-in-range meter: the coss ui Meter pattern on Base UI's
 * accessible Meter primitive (role="meter" with aria values).
 */
export function Meter({
  value,
  max,
  label,
  valueText,
  tone = "flow",
  className,
}: {
  value: number;
  max: number;
  label?: ReactNode;
  /** Text shown on the right of the label row; defaults to value / max. */
  valueText?: ReactNode;
  tone?: Tone;
  className?: string;
}) {
  const safeMax = max > 0 ? max : 1;
  const clamped = Math.min(Math.max(value, 0), safeMax);
  return (
    <BaseMeter.Root value={clamped} max={safeMax} locale="en-US" className={cn("grid w-full gap-1.5", className)}>
      {label !== undefined && (
        <div className="flex items-baseline justify-between gap-3 text-[13px]">
          <BaseMeter.Label className="text-muted">{label}</BaseMeter.Label>
          <span className="figure text-ink-soft">
            {valueText ?? `${clamped.toLocaleString("en-US")} / ${safeMax.toLocaleString("en-US")}`}
          </span>
        </div>
      )}
      <BaseMeter.Track className="block h-1.5 w-full overflow-hidden rounded-full bg-raised">
        <BaseMeter.Indicator
          // A non-zero share stays visible even when it rounds to 0% of the track.
          className={cn("block h-full rounded-full transition-[width] duration-500", clamped > 0 && "min-w-1.5")}
          style={{background: TONE_HEX[tone]}}
        />
      </BaseMeter.Track>
    </BaseMeter.Root>
  );
}
