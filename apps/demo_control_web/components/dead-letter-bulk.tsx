"use client";

import {Popover} from "@base-ui/react/popover";
import {MoreHorizontal} from "lucide-react";
import {useRouter} from "next/navigation";
import {useState} from "react";
import {api} from "../lib/api";
import {formatCount} from "../lib/formatting";
import {humanize} from "../lib/tone";
import {apiErrorText} from "./dead-letter-table";
import {Button} from "./ui";

type Action = "TRIAGED" | "DISCARDED";

const VERB: Record<Action, {button: string; done: string}> = {
  TRIAGED: {button: "Mark all triaged", done: "triaged"},
  DISCARDED: {button: "Discard all", done: "discarded"},
};

/** Moves every NEW record of one category at once; handled records are left alone. */
export function CategoryBulkActions({category, newCount}: {category: string; newCount: number}) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [confirming, setConfirming] = useState<Action | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function apply(status: Action) {
    setBusy(true);
    setError("");
    try {
      await api<{updated: number}>("/api/v1/dlq/bulk-status", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({error_category: category, status}),
      });
      setOpen(false);
      setConfirming(null);
      router.refresh();
    } catch (e) {
      setError(apiErrorText(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Popover.Root
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        if (!next) {
          setConfirming(null);
          setError("");
        }
      }}
    >
      <Popover.Trigger
        aria-label={`Actions for ${humanize(category)}`}
        className="flex size-7 items-center justify-center rounded-[var(--radius-control)] text-faint transition-colors hover:bg-raised hover:text-ink data-[popup-open]:bg-raised data-[popup-open]:text-ink [&_svg]:size-4"
      >
        <MoreHorizontal aria-hidden="true" />
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner side="bottom" align="end" sideOffset={6} collisionPadding={12} className="z-50">
          <Popover.Popup className="w-72 rounded-[var(--radius-panel)] border border-rule bg-panel p-3 shadow-[0_12px_32px_-12px_rgb(15_23_42/0.28)]">
            {confirming ? (
              <div className="grid gap-3">
                <Popover.Title className="text-sm text-ink">
                  {confirming === "DISCARDED" ? "Discard" : "Mark as triaged"} all {formatCount(newCount)} new {humanize(category)} records?
                </Popover.Title>
                <Popover.Description className="text-xs text-muted">
                  Records you already handled are left alone. Everything stays stored and can be reopened one by one.
                </Popover.Description>
                {error && <p className="text-xs text-bad">{error}</p>}
                <div className="flex gap-2">
                  <Button size="sm" variant={confirming === "DISCARDED" ? "danger" : "primary"} disabled={busy} onClick={() => apply(confirming)}>
                    {busy ? "Working" : `${VERB[confirming].button} (${formatCount(newCount)})`}
                  </Button>
                  <Button size="sm" variant="ghost" disabled={busy} onClick={() => setConfirming(null)}>
                    Cancel
                  </Button>
                </div>
              </div>
            ) : (
              <div className="grid gap-1">
                <Popover.Title className="px-2 pb-1 text-xs text-muted">{formatCount(newCount)} new in {humanize(category)}</Popover.Title>
                {(["TRIAGED", "DISCARDED"] as const).map((action) => (
                  <button
                    key={action}
                    type="button"
                    onClick={() => setConfirming(action)}
                    className="rounded-[var(--radius-control)] px-2 py-1.5 text-left text-sm text-ink hover:bg-raised"
                  >
                    {VERB[action].button}
                  </button>
                ))}
              </div>
            )}
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  );
}
