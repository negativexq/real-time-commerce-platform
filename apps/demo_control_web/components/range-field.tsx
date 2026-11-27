"use client";

import {Slider} from "@base-ui/react/slider";
import {useId} from "react";
import {cn} from "../lib/cn";
import {formatCount} from "../lib/formatting";
import {fromLogPosition, toLogPosition} from "../lib/run-limits";

/**
 * A number with three ways in: type it, drag a log-scale slider (Base UI),
 * or pick a preset. All three write the same state.
 */
export function RangeField({
  label,
  unit,
  value,
  max,
  presets,
  onChange,
}: {
  label: string;
  unit: string;
  value: number;
  max: number;
  presets: number[];
  onChange: (value: number) => void;
}) {
  const id = useId();
  const valid = Number.isFinite(value) && value >= 1 && value <= max;
  return (
    <div className="grid gap-2.5">
      <div className="flex items-end justify-between gap-3">
        <label htmlFor={id} className="text-[13px] font-medium text-ink-soft">
          {label}
        </label>
        <div className="flex items-baseline gap-1.5">
          <input
            id={id}
            type="number"
            inputMode="numeric"
            min={1}
            max={max}
            value={Number.isNaN(value) ? "" : value}
            onChange={(e) => onChange(e.target.valueAsNumber)}
            aria-invalid={!valid}
            className={cn(
              "figure h-10 w-32 rounded-[var(--radius-control)] border bg-panel px-3 text-right text-xl font-semibold text-ink transition-colors focus-visible:outline-none",
              valid ? "border-rule hover:border-rule-strong focus-visible:border-flow" : "border-bad text-bad",
            )}
          />
          <span className="text-sm whitespace-nowrap text-muted">{unit}</span>
        </div>
      </div>
      <Slider.Root
        value={toLogPosition(valid ? value : 1, max) * 1000}
        min={0}
        max={1000}
        step={1}
        onValueChange={(position) => onChange(fromLogPosition((position as number) / 1000, max))}
        aria-label={`${label} slider`}
      >
        <Slider.Control className="flex h-6 w-full touch-none items-center select-none">
          <Slider.Track className="relative h-1.5 w-full rounded-full bg-raised">
            <Slider.Indicator className="rounded-full bg-flow" />
            <Slider.Thumb
              getAriaValueText={() => `${formatCount(valid ? value : 0)} ${unit}`}
              className="size-4.5 rounded-full border-2 border-flow bg-panel shadow-sm transition-transform outline-none focus-visible:ring-4 focus-visible:ring-flow/25 data-[dragging]:scale-110"
            />
          </Slider.Track>
        </Slider.Control>
      </Slider.Root>
      <div className="flex flex-wrap gap-1.5" role="group" aria-label={`${label} presets`}>
        {presets.map((preset) => (
          <button
            key={preset}
            type="button"
            onClick={() => onChange(preset)}
            aria-pressed={value === preset}
            className={cn(
              "figure rounded-full border px-2.5 py-0.5 text-xs font-medium transition-colors",
              value === preset ? "border-flow bg-flow-deep text-flow" : "border-rule text-muted hover:border-rule-strong hover:text-ink",
            )}
          >
            {formatCount(preset)}
          </button>
        ))}
      </div>
      {!valid && (
        <p className="text-xs text-bad" role="alert">
          Enter a whole number from 1 to {formatCount(max)}.
        </p>
      )}
    </div>
  );
}
