import type {Tone} from "./tone";

export const DLQ_STATUSES = ["NEW", "TRIAGED", "REPLAYED", "DISCARDED"] as const;
export type DlqStatus = (typeof DLQ_STATUSES)[number];

export type DeadLetter = {
  dlq_record_id: string;
  error_category: string;
  error_type: string | null;
  error_message: string;
  source_topic: string;
  source_partition: number | null;
  source_offset: number | null;
  original_event_id: string | null;
  original_event_type: string | null;
  anomaly_type: string | null;
  processing_attempts: number | null;
  failed_at: string;
  status: DlqStatus;
  status_changed_at: string | null;
  triage_note: string | null;
  replay_count: number;
  last_replayed_at: string | null;
  /** When the original event was applied: set once a replay has gone through. */
  processed_at: string | null;
  replay_blocked_reason: string | null;
};

export type DeadLetterDetail = DeadLetter & {
  consumer_group: string | null;
  /** A newer dead letter for the same event after the last replay: the replay failed. */
  failed_again_record_id: string | null;
  ingested_at: string;
  key: string | null;
  headers: Record<string, string>;
  payload: {kind: "json" | "text" | "binary" | "missing" | "unreadable"; text: string | null; truncated: boolean};
};

export type DlqCategory = {error_category: string; count: number; new: number; replayable: boolean};

export type DlqPage = {
  items: DeadLetter[];
  total: number;
  matching: number;
  categories: DlqCategory[];
  statuses: Partial<Record<DlqStatus, number>>;
  last_ingested_at: string | null;
};

export const STATUS_TONE: Record<DlqStatus, Tone> = {
  NEW: "warn",
  TRIAGED: "flow",
  REPLAYED: "ok",
  DISCARDED: "neutral",
};

export type DlqFilters = {status?: DlqStatus; category?: string};

/** Reads filters from the page URL, dropping anything that is not a known status. */
export function readFilters(params: Record<string, string | string[] | undefined>): DlqFilters {
  const first = (value: string | string[] | undefined) => (Array.isArray(value) ? value[0] : value);
  const status = first(params.status)?.toUpperCase();
  const category = first(params.category)?.trim();
  return {
    status: DLQ_STATUSES.includes(status as DlqStatus) ? (status as DlqStatus) : undefined,
    category: category ? category.slice(0, 100) : undefined,
  };
}

/** The page URL for a set of filters, so filters can be linked and shared. */
export function filterHref(filters: DlqFilters) {
  const params = new URLSearchParams();
  if (filters.status) params.set("status", filters.status.toLowerCase());
  if (filters.category) params.set("category", filters.category);
  const query = params.toString();
  return query ? `/dlq?${query}` : "/dlq";
}

export function apiQuery(filters: DlqFilters, pageSize = 100) {
  const params = new URLSearchParams({page_size: String(pageSize)});
  if (filters.status) params.set("status", filters.status);
  if (filters.category) params.set("error_category", filters.category);
  return `/api/v1/dlq?${params}`;
}
