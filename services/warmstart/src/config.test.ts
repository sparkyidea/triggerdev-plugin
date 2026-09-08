import { describe, expect, test } from "vitest";
import { readConfig } from "./config.ts";

describe("configuration", () => {
  test("bounds the canary and preserves disabled warm starts", () => {
    expect(readConfig({})).toMatchObject({
      maxWaitingRunners: 1,
      maxRequestBodyBytes: 4194304,
      keepaliveMs: 300000,
    });
    expect(
      readConfig({ KEEPALIVE_MS: "0", CONNECTION_TIMEOUT_MS: "0" }).keepaliveMs,
    ).toBe(0);
  });
  test.each(["", "-1", "NaN", "Infinity", "1.5", "2147483648"])(
    "rejects invalid duration %s",
    (value) => {
      expect(() => readConfig({ KEEPALIVE_MS: value })).toThrow();
    },
  );
  test("rejects inconsistent caps and margins without echoing values", () => {
    expect(() =>
      readConfig({ MAX_WAITING_RUNNERS: "2", MAX_IDLE_SESSIONS: "1" }),
    ).toThrow();
    expect(() => readConfig({ POLL_MATCH_MARGIN_MS: "30000" })).toThrow();
    expect(() =>
      readConfig({ MAX_REQUEST_BODY_BYTES: "secret-value" }),
    ).toThrow("MAX_REQUEST_BODY_BYTES");
  });
});
