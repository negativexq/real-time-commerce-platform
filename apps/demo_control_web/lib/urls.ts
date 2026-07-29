const defaultDemoApiUrl = "http://localhost:8082";

export function buildApiDocsUrl(
  apiUrl = process.env.NEXT_PUBLIC_DEMO_API_URL ?? defaultDemoApiUrl,
): string {
  return `${apiUrl.replace(/\/+$/, "")}/docs`;
}

export const apiDocsUrl = buildApiDocsUrl();
