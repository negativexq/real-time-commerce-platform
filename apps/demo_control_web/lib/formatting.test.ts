import {describe, expect, it} from "vitest";
import {formatCount, shortId} from "./formatting";

describe("bounded display formatting", () => {
  it("shortens identifiers and formats counts", () => {
    expect(shortId("12345678-0000")).toBe("12345678");
    expect(formatCount(1200)).toBe("1,200");
  });
});
