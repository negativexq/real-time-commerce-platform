"use client";

import {Popover} from "@base-ui/react/popover";
import {Bell, BellOff, CircleCheck, Lock, X} from "lucide-react";
import Link from "next/link";
import {useCallback, useEffect, useState} from "react";
import {api} from "../lib/api";
import {cn} from "../lib/cn";
import {relativeTime} from "../lib/formatting";
import {usePolling} from "../lib/use-polling";
import type {PlatformAlert, PlatformAlerts} from "../lib/types";
import {StatusDot} from "./ui";

const POLL_MS = 15_000;
export const CLEARED_STORAGE_KEY = "commerce-console-cleared-alerts";

/**
 * One occurrence of an alert. The start time is part of it, so a warning that
 * resolves and fires again comes back even after it was cleared.
 */
export function alertKey(alert: PlatformAlert) {
  return `${alert.name}|${alert.source ?? ""}|${alert.active_at ?? ""}`;
}

/** Critical alerts cannot be cleared: they leave the list only when they resolve. */
export function canClear(alert: PlatformAlert) {
  return alert.severity !== "critical";
}

export function visibleAlerts(items: PlatformAlert[], cleared: ReadonlySet<string>) {
  return items.filter((alert) => !canClear(alert) || !cleared.has(alertKey(alert)));
}

/** What the bell's badge says: firing alerts still in the list; pending ones may still clear. */
export function bellSummary(
  data: PlatformAlerts | null,
  cleared: ReadonlySet<string> = new Set(),
): {count: number; tone: "bad" | "warn" | "neutral"; label: string} {
  if (!data) return {count: 0, tone: "neutral", label: "Alerts: loading"};
  if (data.status === "unavailable") return {count: 0, tone: "neutral", label: "Alerts: Prometheus unreachable"};
  const firing = visibleAlerts(data.items, cleared).filter((a) => a.state === "firing");
  const tone = firing.some((a) => a.severity === "critical") ? "bad" : firing.length ? "warn" : "neutral";
  const label = firing.length ? `Alerts: ${firing.length} firing` : "Alerts: none firing";
  return {count: firing.length, tone, label};
}

function readCleared(): Set<string> {
  try {
    const raw = JSON.parse(localStorage.getItem(CLEARED_STORAGE_KEY) ?? "[]");
    return new Set(Array.isArray(raw) ? raw.filter((k): k is string => typeof k === "string") : []);
  } catch {
    return new Set();
  }
}

function writeCleared(cleared: Set<string>) {
  try {
    localStorage.setItem(CLEARED_STORAGE_KEY, JSON.stringify([...cleared]));
  } catch {
    // Storage can be unavailable (private mode); clearing still works for this page.
  }
}

function AlertRow({alert, now, onClear}: {alert: PlatformAlert; now: number | null; onClear: () => void}) {
  const firing = alert.state === "firing";
  const critical = alert.severity === "critical";
  const tone = critical ? "bad" : firing ? "warn" : "neutral";
  return (
    <li className="group grid grid-cols-[auto_minmax(0,1fr)_auto] gap-x-2.5 px-4 py-3">
      <StatusDot tone={tone} className="mt-1.5" />
      <div className="min-w-0">
        <div className="font-medium text-ink">{alert.summary || alert.name}</div>
        {alert.description && <p className="mt-0.5 text-sm text-muted">{alert.description}</p>}
        <p className="figure mt-1 flex flex-wrap items-center gap-x-1.5 text-xs text-faint">
          <span>
            {alert.name}
            {alert.source ? `, from ${alert.source}` : ""}
          </span>
          <span>
            {firing ? "Firing" : "Pending"}
            {alert.active_at && now !== null ? ` since ${relativeTime(alert.active_at, now)}` : ""}
          </span>
        </p>
        {critical && (
          <p className="mt-1.5 inline-flex items-center gap-1 text-xs font-medium text-bad">
            <Lock aria-hidden="true" className="size-3" />
            Stays until it resolves
          </p>
        )}
      </div>
      {!critical && (
        <button
          type="button"
          onClick={onClear}
          aria-label={`Clear ${alert.summary || alert.name}`}
          className="-mt-0.5 -mr-1.5 flex size-7 items-center justify-center rounded-[var(--radius-control)] text-faint transition-colors hover:bg-raised hover:text-ink [&_svg]:size-4"
        >
          <X aria-hidden="true" />
        </button>
      )}
    </li>
  );
}

export function NotificationBell({className}: {className?: string}) {
  const [data, setData] = useState<PlatformAlerts | null>(null);
  const [cleared, setCleared] = useState<Set<string>>(() => new Set());
  // Wall clock only after mount, like every other relative time in the console.
  const [now, setNow] = useState<number | null>(null);

  useEffect(() => setCleared(readCleared()), []);

  const load = useCallback(async () => {
    try {
      const next = await api<PlatformAlerts>("/api/v1/platform/alerts");
      setData(next);
      // Forget cleared occurrences that have resolved, so storage stays small.
      if (next.status === "available") {
        const active = new Set(next.items.map(alertKey));
        setCleared((prev) => {
          const kept = new Set([...prev].filter((key) => active.has(key)));
          if (kept.size === prev.size) return prev;
          writeCleared(kept);
          return kept;
        });
      }
    } catch {
      setData({status: "unavailable", items: [], firing: 0});
    }
    setNow(Date.now());
  }, []);

  usePolling(load, POLL_MS);

  const update = useCallback((next: Set<string>) => {
    writeCleared(next);
    setCleared(next);
  }, []);

  const items = data?.status === "available" ? data.items : [];
  const visible = visibleAlerts(items, cleared);
  const clearable = visible.filter(canClear);
  const hidden = items.length - visible.length;
  const {count, tone, label} = bellSummary(data, cleared);
  const unavailable = data?.status === "unavailable";

  return (
    <Popover.Root>
      <Popover.Trigger
        aria-label={label}
        className={cn(
          "relative flex size-9 shrink-0 items-center justify-center rounded-[var(--radius-control)] text-muted transition-colors hover:bg-raised hover:text-ink data-[popup-open]:bg-raised data-[popup-open]:text-ink [&_svg]:size-[18px]",
          className,
        )}
      >
        {unavailable ? <BellOff aria-hidden="true" /> : <Bell aria-hidden="true" />}
        {count > 0 && (
          <span
            aria-hidden="true"
            className={cn(
              "figure absolute -top-0.5 -right-0.5 flex h-[18px] min-w-[18px] items-center justify-center rounded-full px-1 text-[11px] font-semibold text-on-accent ring-2 ring-panel",
              tone === "bad" ? "bg-bad" : "bg-warn",
            )}
          >
            {count > 9 ? "9+" : count}
          </span>
        )}
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner side="bottom" align="start" sideOffset={8} collisionPadding={12} className="z-50">
          <Popover.Popup className="w-[min(380px,calc(100vw-24px))] origin-[var(--transform-origin)] overflow-hidden rounded-[var(--radius-panel)] border border-rule bg-panel shadow-[0_12px_32px_-12px_rgb(15_23_42/0.28)] transition-[opacity,transform] duration-150 data-[ending-style]:scale-95 data-[ending-style]:opacity-0 data-[starting-style]:scale-95 data-[starting-style]:opacity-0">
            <div className="flex items-center justify-between gap-3 border-b border-rule px-4 py-2.5">
              <div>
                <Popover.Title className="font-semibold text-ink">Alerts</Popover.Title>
                <Popover.Description className="text-xs text-muted">
                  {unavailable ? "Prometheus is unreachable" : count ? `${count} firing` : "Checked every 15 s"}
                </Popover.Description>
              </div>
              <button
                type="button"
                disabled={clearable.length === 0}
                onClick={() => update(new Set([...cleared, ...clearable.map(alertKey)]))}
                className="rounded-[var(--radius-control)] px-2 py-1 text-sm font-medium text-flow transition-colors hover:bg-raised disabled:cursor-not-allowed disabled:text-faint disabled:hover:bg-transparent"
              >
                Clear notifications
              </button>
            </div>
            {!data ? (
              <p className="px-4 py-6 text-sm text-muted">Checking Prometheus for alerts.</p>
            ) : unavailable ? (
              <p className="px-4 py-6 text-sm text-muted">Alerts can&apos;t be checked. Start the observability profile to see them.</p>
            ) : visible.length === 0 ? (
              <div className="flex items-center gap-2.5 px-4 py-6 text-sm text-muted">
                <CircleCheck aria-hidden="true" className="size-5 shrink-0 text-ok" />
                {hidden ? "Nothing new. Cleared warnings come back if they fire again." : "All clear. No alert rules are firing."}
              </div>
            ) : (
              <ul className="max-h-[min(420px,60vh)] divide-y divide-rule overflow-y-auto">
                {visible.map((alert, index) => (
                  <AlertRow
                    key={`${alertKey(alert)}-${index}`}
                    alert={alert}
                    now={now}
                    onClear={() => update(new Set([...cleared, alertKey(alert)]))}
                  />
                ))}
              </ul>
            )}
            <div className="flex items-center justify-between gap-3 border-t border-rule px-4 py-2.5 text-sm">
              <Popover.Close render={<Link href="/infrastructure" className="font-medium text-flow hover:underline" />}>
                See service health
              </Popover.Close>
              {hidden > 0 && (
                <button type="button" onClick={() => update(new Set())} className="text-muted hover:text-ink hover:underline">
                  Show {hidden} cleared
                </button>
              )}
            </div>
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  );
}
