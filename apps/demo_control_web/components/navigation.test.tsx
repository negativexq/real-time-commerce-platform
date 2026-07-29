import {renderToStaticMarkup} from "react-dom/server";
import {describe, expect, it} from "vitest";
import {QuickStart} from "../app/page";
import {buildApiDocsUrl} from "../lib/urls";
import {AppShell} from "./app-shell";

describe("API Docs navigation", () => {
  it("renders the sidebar link as an external destination", () => {
    const markup = renderToStaticMarkup(<AppShell>content</AppShell>);

    expect(markup).toContain(
      'href="http://localhost:8082/docs" target="_blank"',
    );
    expect(markup).toContain("API Docs ↗");
  });

  it("renders the API Docs link in Quick Start", () => {
    const markup = renderToStaticMarkup(<QuickStart />);

    expect(markup).toContain(
      'href="http://localhost:8082/docs" target="_blank"',
    );
    expect(markup).toContain("Open API Docs ↗");
  });

  it("builds the docs URL from an overridden public API URL", () => {
    expect(buildApiDocsUrl("https://demo.example.test/api/")).toBe(
      "https://demo.example.test/api/docs",
    );
  });
});
