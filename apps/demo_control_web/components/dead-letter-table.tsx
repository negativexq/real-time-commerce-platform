"use client";

import {Dialog} from "@base-ui/react/dialog";
import {CheckCheck, RotateCcw, Send, Trash2, X} from "lucide-react";
import {useRouter} from "next/navigation";
import {useEffect, useState} from "react";
import {api} from "../lib/api";
import {STATUS_TONE, type DeadLetter, type DeadLetterDetail} from "../lib/dead-letters";
import {formatDateTime, shortId} from "../lib/formatting";
import {humanize} from "../lib/tone";
import {Badge, Button, DataTable} from "./ui";
import {Time} from "./time";

const post = (path: string, body?: unknown) =>
  api<DeadLetterDetail>(path, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: body === undefined ? undefined : JSON.stringify(body),
  });

/** The API answers errors as `API 409: {"detail": "..."}`; show only the reason. */
export function apiErrorText(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  const json = message.indexOf("{");
  if (json >= 0) {
    try {
      const detail = JSON.parse(message.slice(json)).detail;
      if (typeof detail === "string") return detail;
    } catch {
      // Not JSON: fall through to the raw message.
    }
  }
  return message || "The request failed.";
}

function Fact({label, children}: {label: string; children: React.ReactNode}) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted">{label}</dt>
      <dd className="figure truncate text-sm text-ink">{children}</dd>
    </div>
  );
}

function StatusCell({record}: {record: DeadLetter}) {
  return (
    <div className="flex flex-wrap items-center gap-1">
      <Badge value={record.status} tone={STATUS_TONE[record.status]} />
      {record.status === "REPLAYED" && record.processed_at && <Badge value="Applied" tone="ok" />}
    </div>
  );
}

function DetailDialog({recordId, onClose, onOpen}: {recordId: string | null; onClose: () => void; onOpen: (id: string) => void}) {
  const router = useRouter();
  const [detail, setDetail] = useState<DeadLetterDetail | null>(null);
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [done, setDone] = useState("");

  useEffect(() => {
    if (!recordId) return;
    let current = true;
    setDetail(null);
    setError("");
    setDone("");
    setNote("");
    api<DeadLetterDetail>(`/api/v1/dlq/${recordId}`)
      .then((d) => {
        if (!current) return;
        setDetail(d);
        setNote(d.triage_note ?? "");
      })
      .catch((e) => current && setError(apiErrorText(e)));
    return () => {
      current = false;
    };
  }, [recordId]);

  async function act(name: string, run: () => Promise<DeadLetterDetail>, message: string) {
    setBusy(name);
    setError("");
    setDone("");
    try {
      const next = await run();
      setDetail(next);
      setDone(message);
      router.refresh();
    } catch (e) {
      setError(apiErrorText(e));
    } finally {
      setBusy(null);
    }
  }

  const id = detail?.dlq_record_id;
  const trimmed = note.trim() || null;
  const canReplay = detail !== null && detail.replay_blocked_reason === null;

  return (
    <Dialog.Root open={recordId !== null} onOpenChange={(open) => !open && onClose()}>
      <Dialog.Portal>
        <Dialog.Backdrop className="fixed inset-0 z-40 bg-ink/30 transition-opacity duration-150 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0 dark:bg-black/50" />
        <Dialog.Popup className="fixed inset-y-0 right-0 z-50 flex w-[min(560px,100vw)] flex-col border-l border-rule bg-panel shadow-[0_0_48px_-12px_rgb(15_23_42/0.35)] transition-transform duration-200 data-[ending-style]:translate-x-full data-[starting-style]:translate-x-full">
          <div className="flex items-start justify-between gap-3 border-b border-rule px-5 py-4">
            <div className="min-w-0">
              <Dialog.Title className="text-lg font-semibold text-ink">
                {detail ? humanize(detail.error_category) : "Dead letter"}
              </Dialog.Title>
              <Dialog.Description className="text-sm text-muted">
                {detail
                  ? `${detail.original_event_type ? humanize(detail.original_event_type) : "Unknown event type"}, failed ${formatDateTime(detail.failed_at)}`
                  : "Loading the stored record."}
              </Dialog.Description>
            </div>
            <Dialog.Close aria-label="Close" className="flex size-8 shrink-0 items-center justify-center rounded-[var(--radius-control)] text-muted hover:bg-raised hover:text-ink [&_svg]:size-4">
              <X aria-hidden="true" />
            </Dialog.Close>
          </div>

          <div className="grid flex-1 content-start gap-5 overflow-y-auto px-5 py-4">
            {!detail && !error && <p className="text-sm text-muted">Loading…</p>}
            {detail && (
              <>
                <div className="flex flex-wrap items-center gap-2">
                  <Badge value={detail.status} tone={STATUS_TONE[detail.status]} />
                  {detail.processed_at && <Badge value="Applied" tone="ok" />}
                  {detail.status_changed_at && <span className="text-xs text-muted">since {formatDateTime(detail.status_changed_at)}</span>}
                </div>
                {detail.failed_again_record_id && (
                  <p className="flex flex-wrap items-center gap-x-2 rounded-[var(--radius-control)] bg-warn-deep px-3 py-2 text-sm text-warn">
                    The replay failed: this event dead-lettered again.
                    <button type="button" onClick={() => onOpen(detail.failed_again_record_id!)} className="font-semibold underline underline-offset-2">
                      Open the new record
                    </button>
                  </p>
                )}
                <section>
                  <h3 className="text-xs text-muted">Reason</h3>
                  <p className="mt-1 break-words text-sm text-ink">{detail.error_message}</p>
                </section>
                <dl className="grid grid-cols-2 gap-x-4 gap-y-3 sm:grid-cols-3">
                  <Fact label="Event">
                    <span title={detail.original_event_id ?? undefined}>{detail.original_event_id ? shortId(detail.original_event_id) : "none"}</span>
                  </Fact>
                  <Fact label="Source">
                    p{detail.source_partition ?? "—"} @ {detail.source_offset ?? "—"}
                  </Fact>
                  <Fact label="Attempts">{detail.processing_attempts ?? "—"}</Fact>
                  <Fact label="Replays">{detail.replay_count}</Fact>
                  <Fact label="Applied">{detail.processed_at ? formatDateTime(detail.processed_at) : detail.failed_again_record_id ? "Failed again" : "Not yet"}</Fact>
                  <Fact label="Anomaly">{detail.anomaly_type ? humanize(detail.anomaly_type) : "—"}</Fact>
                </dl>
                <section>
                  <h3 className="flex items-baseline justify-between text-xs text-muted">
                    Original record
                    {detail.payload.truncated && <span className="text-warn">Truncated</span>}
                  </h3>
                  {detail.payload.text ? (
                    <pre className="figure mt-1 max-h-72 overflow-auto rounded-[var(--radius-control)] bg-raised p-3 text-xs leading-relaxed text-ink-soft">
                      {detail.payload.text}
                    </pre>
                  ) : (
                    <p className="mt-1 text-sm text-muted">
                      {detail.payload.kind === "binary" ? "The payload is binary." : detail.payload.kind === "missing" ? "No payload was stored." : "The payload can't be decoded."}
                    </p>
                  )}
                </section>
                {Object.keys(detail.headers).length > 0 && (
                  <section>
                    <h3 className="text-xs text-muted">Headers</h3>
                    <dl className="mt-1 grid grid-cols-[auto_minmax(0,1fr)] gap-x-4 gap-y-1 text-xs">
                      {Object.entries(detail.headers).map(([name, value]) => (
                        <div key={name} className="contents">
                          <dt className="text-muted">{name}</dt>
                          <dd className="figure truncate text-ink-soft" title={value}>
                            {value}
                          </dd>
                        </div>
                      ))}
                    </dl>
                  </section>
                )}
                <section className="grid gap-1.5">
                  <label htmlFor="triage-note" className="text-xs text-muted">
                    Note
                  </label>
                  <textarea
                    id="triage-note"
                    value={note}
                    maxLength={500}
                    rows={2}
                    onChange={(e) => setNote(e.target.value)}
                    placeholder="What you found, for whoever looks next"
                    className="resize-y rounded-[var(--radius-control)] border border-rule bg-panel px-3 py-2 text-sm text-ink placeholder:text-faint hover:border-rule-strong focus-visible:border-flow focus-visible:outline-none"
                  />
                </section>
              </>
            )}
          </div>

          <div className="grid gap-3 border-t border-rule px-5 py-4">
            {error && (
              <p role="alert" className="rounded-[var(--radius-control)] bg-bad-deep px-3 py-2 text-sm text-bad">
                {error}
              </p>
            )}
            {done && !error && (
              <p role="status" className="text-sm text-ok">
                {done}
              </p>
            )}
            {detail && !canReplay && <p className="text-xs text-muted">Replay unavailable: {detail.replay_blocked_reason}</p>}
            {detail && id && (
              <div className="flex flex-wrap gap-2">
                <Button
                  variant="primary"
                  disabled={!canReplay || busy !== null}
                  onClick={() => act("replay", () => post(`/api/v1/dlq/${id}/replay`), "Sent back to the pipeline. The event applies or dead-letters again as a new record.")}
                >
                  <Send aria-hidden="true" />
                  {busy === "replay" ? "Replaying" : "Replay"}
                </Button>
                {detail.status !== "TRIAGED" && (
                  <Button
                    variant="secondary"
                    disabled={busy !== null}
                    onClick={() => act("triage", () => post(`/api/v1/dlq/${id}/status`, {status: "TRIAGED", note: trimmed}), "Marked as triaged.")}
                  >
                    <CheckCheck aria-hidden="true" />
                    Mark triaged
                  </Button>
                )}
                {detail.status !== "DISCARDED" && (
                  <Button
                    variant="secondary"
                    disabled={busy !== null}
                    onClick={() => act("discard", () => post(`/api/v1/dlq/${id}/status`, {status: "DISCARDED", note: trimmed}), "Discarded. It stays stored and can be reopened.")}
                  >
                    <Trash2 aria-hidden="true" />
                    Discard
                  </Button>
                )}
                {detail.status !== "NEW" && (
                  <Button
                    variant="ghost"
                    disabled={busy !== null}
                    onClick={() => act("reopen", () => post(`/api/v1/dlq/${id}/status`, {status: "NEW", note: trimmed}), "Reopened as new.")}
                  >
                    <RotateCcw aria-hidden="true" />
                    Reopen
                  </Button>
                )}
              </div>
            )}
          </div>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

export function DeadLetterTable({items}: {items: DeadLetter[]}) {
  const [open, setOpen] = useState<string | null>(null);
  return (
    <>
      <DataTable fit minWidth={880}>
        <thead>
          <tr>
            <th className="w-[17%]">Category</th>
            <th className="w-[23%]">Event</th>
            <th>Reason</th>
            <th className="w-[86px] text-right">Attempts</th>
            <th className="w-[132px]">Status</th>
            <th className="w-[136px] text-right">Failed</th>
          </tr>
        </thead>
        <tbody>
          {items.map((x) => (
            <tr
              key={x.dlq_record_id}
              onClick={() => setOpen(x.dlq_record_id)}
              className="cursor-pointer"
            >
              <td className="break-words font-medium text-warn">
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    setOpen(x.dlq_record_id);
                  }}
                  className="text-left hover:underline"
                >
                  {humanize(x.error_category)}
                </button>
              </td>
              <td>
                <div className="truncate text-ink-soft">{x.original_event_type ? humanize(x.original_event_type) : "Unknown type"}</div>
                <div className="figure truncate text-xs text-faint" title={x.original_event_id ?? undefined}>
                  {x.original_event_id ? shortId(x.original_event_id) : "no event id"}, partition {x.source_partition ?? "—"} offset {x.source_offset ?? "—"}
                </div>
              </td>
              <td className="break-words text-ink-soft">{x.error_message}</td>
              <td className="figure text-right">{x.processing_attempts ?? "—"}</td>
              <td>
                <StatusCell record={x} />
              </td>
              <td className="text-right text-muted"><Time value={x.failed_at} /></td>
            </tr>
          ))}
        </tbody>
      </DataTable>
      <DetailDialog recordId={open} onClose={() => setOpen(null)} onOpen={setOpen} />
    </>
  );
}
