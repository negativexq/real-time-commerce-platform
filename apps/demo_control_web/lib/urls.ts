const defaultDemoApiUrl = "http://localhost:8082";

export function resolveDemoApiUrl(
  apiUrl = process.env.NEXT_PUBLIC_DEMO_API_URL ?? defaultDemoApiUrl,
  browserHostname?: string,
): string {
  if (!browserHostname) return apiUrl.replace(/\/+$/, "");
  try {
    const resolved = new URL(apiUrl);
    if (resolved.hostname === "localhost" || resolved.hostname === "127.0.0.1") {
      resolved.hostname = browserHostname;
    }
    return resolved.toString().replace(/\/+$/, "");
  } catch {
    return apiUrl.replace(/\/+$/, "");
  }
}

export function buildApiDocsUrl(
  apiUrl = process.env.NEXT_PUBLIC_DEMO_API_URL ?? defaultDemoApiUrl,
): string {
  return `${resolveDemoApiUrl(apiUrl)}/docs`;
}

export const apiDocsUrl = buildApiDocsUrl();
