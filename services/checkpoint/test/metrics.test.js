import assert from "node:assert/strict";
import test from "node:test";
import { measure } from "../src/metrics.js";

test("stage measurements allowlist identity and never log operation errors or payloads", async (t) => {
  const output = [];
  t.mock.method(console, "log", (line) => output.push(JSON.parse(line)));
  const identity = { runFriendlyId: "run_test", snapshotFriendlyId: "snapshot_test", secret: "do-not-log" };
  await assert.rejects(measure(true, "upload", identity, async () => { throw new Error("do-not-log"); }));
  assert.equal(output.length, 2);
  assert.equal(output[1].success, false);
  assert.equal(output[1].stage, "upload");
  assert.equal(JSON.stringify(output).includes("do-not-log"), false);
  assert.equal(await measure(false, "upload", identity, async () => 42), 42);
  assert.equal(output.length, 2);
});
