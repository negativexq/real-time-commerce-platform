export type Tone = "flow" | "ok" | "warn" | "bad" | "neutral";

const TONES: Record<string, Tone> = {
  // run status
  PENDING: "flow",
  STARTING: "flow",
  RUNNING: "flow",
  STOP_REQUESTED: "warn",
  STOPPED: "neutral",
  COMPLETED: "ok",
  FAILED: "bad",
  // health
  HEALTHY: "ok",
  DEGRADED: "warn",
  UNHEALTHY: "bad",
  UNKNOWN: "neutral",
  NOT_MONITORED: "neutral",
  // fraud decisions and severities
  APPROVE: "ok",
  REVIEW: "warn",
  BLOCK: "bad",
  CRITICAL: "bad",
  HIGH: "bad",
  MEDIUM: "warn",
  LOW: "neutral",
  // outbox
  PUBLISHED: "ok",
  PENDING_PUBLISH: "warn",
};

export function toneOf(value: string | null | undefined): Tone {
  return (value && TONES[value.toUpperCase()]) || "neutral";
}

/** CSS colour values (theme variables), usable in SVG style props. */
export const TONE_HEX: Record<Tone, string> = {
  flow: "var(--color-flow)",
  ok: "var(--color-ok)",
  warn: "var(--color-warn)",
  bad: "var(--color-bad)",
  neutral: "var(--color-faint)",
};

export const DECISION_HEX = {
  APPROVE: TONE_HEX.ok,
  REVIEW: TONE_HEX.warn,
  BLOCK: TONE_HEX.bad,
} as const;

export function humanize(value: string) {
  if (/^[A-Z]{2,3}$/.test(value)) return value; // acronyms such as DLQ
  const text = value.replaceAll("_", " ").toLowerCase();
  const cased = text.replace(/\b(json|id|api|http|sql|uuid|dlq)\b/g, (word) => word.toUpperCase());
  return cased.charAt(0).toUpperCase() + cased.slice(1);
}
