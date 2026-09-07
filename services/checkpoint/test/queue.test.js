import assert from "node:assert/strict";
import test from "node:test";
import { TaskQueue } from "../src/queue.js";

test("run returns task results while respecting the concurrency limit", async () => {
  const queue = new TaskQueue(1);
  let releaseFirst;
  let active = 0;
  let maximumActive = 0;
  const firstGate = new Promise((resolve) => {
    releaseFirst = resolve;
  });

  const run = (value, gate) =>
    queue.run(async () => {
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      if (gate) await gate;
      active -= 1;
      return value;
    });

  const first = run("first", firstGate);
  const second = run("second");
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(queue.running, 1);
  assert.equal(queue.pending.length, 1);

  releaseFirst();
  assert.deepEqual(await Promise.all([first, second]), ["first", "second"]);
  assert.equal(maximumActive, 1);
});
