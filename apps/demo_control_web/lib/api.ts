const browserBase = process.env.NEXT_PUBLIC_DEMO_API_URL ?? "http://localhost:8082";
const serverBase = process.env.DEMO_API_INTERNAL_URL ?? "http://demo-control-api:8080";

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const response = await fetch(`${typeof window === "undefined" ? serverBase : browserBase}${path}`, {...init, signal: controller.signal, cache: "no-store"});
    if (!response.ok) throw new Error(`API ${response.status}: ${await response.text()}`);
    return await response.json() as T;
  } finally { clearTimeout(timeout); }
}
export const publicApiBase = browserBase;
