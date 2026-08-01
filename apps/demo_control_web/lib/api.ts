import {resolveDemoApiUrl} from "./urls";

const serverBase = process.env.DEMO_API_INTERNAL_URL ?? "http://demo-control-api:8080";

function browserBase() {
  return resolveDemoApiUrl(
    process.env.NEXT_PUBLIC_DEMO_API_URL ?? "http://localhost:8082",
    typeof window === "undefined" ? undefined : window.location.hostname,
  );
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const base = typeof window === "undefined" ? serverBase : browserBase();
    const response = await fetch(`${base}${path}`, {...init, signal: controller.signal, cache: "no-store"});
    if (!response.ok) throw new Error(`API ${response.status}: ${await response.text()}`);
    return await response.json() as T;
  } finally { clearTimeout(timeout); }
}

export function publicApiBase(): string {
  return browserBase();
}
