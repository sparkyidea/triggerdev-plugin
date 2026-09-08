import { createHmac } from "node:crypto";
import { describe, expect, test } from "vitest";
import {
  deploymentIdentity,
  dispatchKey,
  HttpError,
  traceparent,
} from "./protocol.ts";

function token(payload: unknown) {
  const prefix =
    Buffer.from('{"alg":"HS256","typ":"JWT"}').toString("base64url") +
    "." +
    Buffer.from(JSON.stringify(payload)).toString("base64url");
  return (
    prefix +
    "." +
    createHmac("sha256", "test-secret").update(prefix).digest("base64url")
  );
}

describe("deployment identity", () => {
  test("normalizes a bare friendly ID and signed token to the same ID", () => {
    expect(deploymentIdentity("deployment_test")).toBe("deployment_test");
    expect(deploymentIdentity(token({ deployment: "deployment_test" }))).toBe(
      "deployment_test",
    );
  });
  test.each([
    undefined,
    "",
    "a..b",
    "a.%%.b",
    "a.e30.b",
    "a.bnVsbA.b",
    "a.W10.b",
    "a.e30=.b",
  ])("rejects malformed identity without echoing it: %s", (value) => {
    expect(() => deploymentIdentity(value)).toThrowError(
      new HttpError(400, "invalid_deployment_identity"),
    );
  });
  test("rejects a token with unusable deployment claim", () => {
    for (const claim of ["", {}, "a.b.c", "a\nb"]) {
      expect(() => deploymentIdentity(token({ deployment: claim }))).toThrow(
        "invalid_deployment_identity",
      );
    }
  });
});

test("missing dispatch dimensions are misses", () => {
  expect(dispatchKey({})).toBeUndefined();
  expect(
    dispatchKey({ deployment: { friendlyId: "deployment_test" } }),
  ).toBeUndefined();
});

test("traceparent only accepts a valid nonzero v00 trace", () => {
  const good = "00-" + "1".repeat(32) + "-" + "2".repeat(16) + "-01";
  expect(traceparent(good)).toBe(good);
  expect(
    traceparent("00-" + "0".repeat(32) + "-" + "2".repeat(16) + "-01"),
  ).toBeUndefined();
  expect(traceparent("raw-header-value")).toBeUndefined();
});
