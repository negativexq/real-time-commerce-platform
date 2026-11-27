import {describe, expect, it} from "vitest";
import {apiErrorText} from "../components/dead-letter-table";
import {apiQuery, filterHref, readFilters} from "./dead-letters";

describe("dead-letter filters", () => {
  it("reads known statuses from the URL and ignores the rest", () => {
    expect(readFilters({status: "triaged", category: " malformed_json "})).toEqual({status: "TRIAGED", category: "malformed_json"});
    expect(readFilters({status: "deleted"})).toEqual({status: undefined, category: undefined});
    expect(readFilters({status: ["new", "replayed"]}).status).toBe("NEW");
  });

  it("round-trips filters through page links and API queries", () => {
    expect(filterHref({})).toBe("/dlq");
    expect(filterHref({status: "NEW", category: "retry_exhausted"})).toBe("/dlq?status=new&category=retry_exhausted");
    expect(apiQuery({status: "NEW", category: "retry_exhausted"})).toBe("/api/v1/dlq?page_size=100&status=NEW&error_category=retry_exhausted");
    expect(readFilters(Object.fromEntries(new URLSearchParams(filterHref({status: "DISCARDED"}).split("?")[1])))).toEqual({
      status: "DISCARDED",
      category: undefined,
    });
  });

  it("shows the API's reason instead of the raw error", () => {
    expect(apiErrorText(new Error('API 409: {"detail":"Already replayed. Reopen it first to replay it."}'))).toBe(
      "Already replayed. Reopen it first to replay it.",
    );
    expect(apiErrorText(new Error("API 502: gateway down"))).toBe("API 502: gateway down");
  });
});
