/**
 * Mirrors the demo API's RunCreate validation
 * (services/demo_control_api/models/scenarios.py). The API is the authority;
 * these only keep the form from offering values it would reject.
 */
export const MAX_EVENTS = 100_000;
export const MAX_RATE = 1_000;
/** The runner stops generation after DEMO_RUN_TIMEOUT_SECONDS (default 3600). */
export const MAX_RUN_SECONDS = 3_600;

export const EVENT_PRESETS = [100, 500, 2_000, 10_000, 50_000];
export const RATE_PRESETS = [10, 50, 200, 500, 1_000];

export const MIXED_DISTRIBUTION = {
  normal: 50,
  suspicious: 20,
  bot: 10,
  account_takeover: 10,
  discount_hunter: 5,
  indecisive: 5,
} as const;

/** Slider position (0..1) to a value on a log scale, rounded to a readable step. */
export function fromLogPosition(position: number, max: number) {
  const raw = Math.pow(max, Math.min(1, Math.max(0, position)));
  const step = raw < 20 ? 1 : raw < 200 ? 5 : raw < 2_000 ? 50 : raw < 20_000 ? 500 : 1_000;
  return Math.min(max, Math.max(1, Math.round(raw / step) * step));
}

export function toLogPosition(value: number, max: number) {
  if (!(value >= 1)) return 0;
  return Math.min(1, Math.log(value) / Math.log(max));
}

/** Decisions or outcomes named in a scenario's expected-outcome text. */
export function expectedOutcomes(text: string) {
  const found = ["APPROVE", "REVIEW", "BLOCK", "DLQ"].filter((word) => text.toUpperCase().includes(word));
  return found.length ? found : [];
}
