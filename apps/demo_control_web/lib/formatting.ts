export function shortId(value: string): string {
  return value.slice(0, 8);
}

export function formatCount(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}

const formatters = new Map<string, Intl.DateTimeFormat>();

function dateTime(timeZone?: string) {
  const key = timeZone ?? "local";
  let formatter = formatters.get(key);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat("en-US", {month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone});
    formatters.set(key, formatter);
  }
  return formatter;
}

/** Fixed locale; the timezone is the runtime's unless one is given. Server-rendered pages use the Time component. */
export function formatDateTime(value: string | null | undefined, timeZone?: string): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : dateTime(timeZone).format(date);
}

export function relativeTime(value: string, now = Date.now()) {
  const seconds = Math.round((now - new Date(value).getTime()) / 1000);
  if (!Number.isFinite(seconds)) return "—";
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}
