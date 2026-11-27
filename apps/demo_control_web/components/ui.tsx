import {Slot} from "@radix-ui/react-slot";
import {cva, type VariantProps} from "class-variance-authority";
import type {ComponentProps, ReactNode} from "react";
import {cn} from "../lib/cn";
import {humanize, toneOf, type Tone} from "../lib/tone";

/* Panels -------------------------------------------------------------- */

export function Panel({
  title,
  description,
  action,
  children,
  className,
  bodyClassName,
}: {
  title?: ReactNode;
  description?: ReactNode;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
}) {
  return (
    <section
      className={cn(
        "min-w-0 rounded-[var(--radius-panel)] border border-rule bg-panel",
        className,
      )}
    >
      {(title || action) && (
        <header className="flex items-start justify-between gap-4 px-5 pt-4">
          <div className="min-w-0">
            {title && <h2 className="text-[15px] font-semibold text-ink">{title}</h2>}
            {description && <p className="mt-0.5 text-[13px] text-muted">{description}</p>}
          </div>
          {action && <div className="shrink-0 text-[13px]">{action}</div>}
        </header>
      )}
      <div className={cn("p-5", title || action ? "pt-4" : "", bodyClassName)}>{children}</div>
    </section>
  );
}

/** Kept for older call sites: a panel with a plain title. */
export function Card({title, children}: {title: string; children: ReactNode}) {
  return <Panel title={title}>{children}</Panel>;
}

export function PageHeader({
  title,
  description,
  action,
}: {
  title: string;
  description?: string;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-ink">{title}</h1>
        {description && <p className="mt-1 max-w-[70ch] text-muted">{description}</p>}
      </div>
      {action}
    </div>
  );
}

/* Badges and dots ----------------------------------------------------- */

const badge = cva(
  "inline-flex items-center gap-1.5 whitespace-nowrap rounded-full px-2 py-0.5 text-xs font-medium",
  {
    variants: {
      tone: {
        flow: "bg-flow-deep text-flow",
        ok: "bg-ok-deep text-ok",
        warn: "bg-warn-deep text-warn",
        bad: "bg-bad-deep text-bad",
        neutral: "bg-raised text-ink-soft",
      },
    },
    defaultVariants: {tone: "neutral"},
  },
);

export function Badge({
  value,
  tone,
  dot = false,
  className,
}: {
  value: string;
  tone?: Tone;
  dot?: boolean;
  className?: string;
}) {
  const resolved = tone ?? toneOf(value);
  return (
    <span className={cn(badge({tone: resolved}), className)} data-tone={resolved}>
      {dot && <StatusDot tone={resolved} pulse={resolved === "flow"} />}
      {humanize(value)}
    </span>
  );
}

const dotTone: Record<Tone, string> = {
  flow: "bg-flow",
  ok: "bg-ok",
  warn: "bg-warn",
  bad: "bg-bad",
  neutral: "bg-faint",
};

export function StatusDot({
  tone,
  pulse = false,
  className,
}: {
  tone: Tone;
  pulse?: boolean;
  className?: string;
}) {
  return (
    <span aria-hidden="true" className={cn("relative inline-flex size-2 shrink-0", className)}>
      {pulse && (
        <span
          className={cn(
            "absolute inset-0 rounded-full opacity-60 motion-safe:animate-ping",
            dotTone[tone],
          )}
        />
      )}
      <span className={cn("relative inline-flex size-2 rounded-full", dotTone[tone])} />
    </span>
  );
}

/* Buttons ------------------------------------------------------------- */

export const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-[var(--radius-control)] text-sm font-semibold transition-colors disabled:pointer-events-none disabled:opacity-50 [&_svg]:size-4 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        primary: "bg-flow text-on-accent hover:bg-flow/85",
        secondary: "border border-rule bg-raised text-ink hover:border-rule-strong hover:bg-rule/60",
        ghost: "text-ink-soft hover:bg-raised hover:text-ink",
        danger: "bg-bad text-on-accent hover:bg-bad/85",
      },
      size: {
        sm: "h-8 px-3",
        md: "h-10 px-4",
      },
    },
    defaultVariants: {variant: "secondary", size: "md"},
  },
);

export function Button({
  className,
  variant,
  size,
  asChild = false,
  ...props
}: ComponentProps<"button"> & VariantProps<typeof buttonVariants> & {asChild?: boolean}) {
  const Component = asChild ? Slot : "button";
  return <Component className={cn(buttonVariants({variant, size}), className)} {...props} />;
}

/* Figures ------------------------------------------------------------- */

export const textTone: Record<Tone, string> = {
  flow: "text-flow",
  ok: "text-ok",
  warn: "text-warn",
  bad: "text-bad",
  neutral: "text-ink",
};

export function Stat({
  label,
  value,
  unit,
  hint,
  tone,
  className,
}: {
  label: ReactNode;
  value: ReactNode;
  unit?: string;
  hint?: ReactNode;
  tone?: Tone;
  className?: string;
}) {
  return (
    <div className={cn("min-w-0", className)}>
      <div className="text-[13px] text-muted">{label}</div>
      <div
        className={cn(
          "figure mt-0.5 text-[28px] font-semibold leading-none text-ink",
          tone && textTone[tone],
        )}
      >
        {value ?? "—"}
        {unit && <span className="ml-1 text-sm font-medium text-muted">{unit}</span>}
      </div>
      {hint && <div className="mt-1.5 text-xs text-faint">{hint}</div>}
    </div>
  );
}

/** Back-compat alias for the old metric tile. */
export function Metric({
  label,
  value,
  unit,
}: {
  label: string;
  value: string | number | null | undefined;
  unit?: string;
}) {
  return <Stat label={label} value={value ?? "—"} unit={unit} />;
}

export function Empty({text, action}: {text: string; action?: ReactNode}) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 rounded-[var(--radius-control)] border border-dashed border-rule px-6 py-10 text-center text-sm text-muted">
      <p>{text}</p>
      {action}
    </div>
  );
}

/* Tables -------------------------------------------------------------- */

/** `fit` lays the table out to its panel width instead of scrolling sideways. */
/**
 * `fit` lays columns out to the panel's width with no horizontal scroll.
 * `minWidth` is the narrowest the columns stay readable at: below it (tablets,
 * phones) the table scrolls inside its panel instead of crushing a column to 0.
 */
export function DataTable({
  children,
  className,
  fit = false,
  minWidth = 640,
}: {
  children: ReactNode;
  className?: string;
  fit?: boolean;
  minWidth?: number;
}) {
  return (
    <div className={cn("-mx-5 overflow-x-auto", className)}>
      <table className={cn("w-full border-collapse text-sm [&_td]:border-t [&_td]:border-rule [&_td]:px-5 [&_td]:py-2.5 [&_th]:px-5 [&_th]:pb-2 [&_th]:text-left [&_th]:text-xs [&_th]:font-medium [&_th]:text-muted [&_tbody_tr]:transition-colors [&_tbody_tr:hover]:bg-raised/60", fit
            ? "table-fixed [&_td]:px-3 [&_th]:px-3 [&_td:first-child]:pl-5 [&_th:first-child]:pl-5 [&_td:last-child]:pr-5 [&_th:last-child]:pr-5"
            : "min-w-max")} style={fit ? {minWidth} : undefined}>
        {children}
      </table>
    </div>
  );
}
