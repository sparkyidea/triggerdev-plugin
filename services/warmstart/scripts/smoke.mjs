import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";

const idleMs = Number(process.env.SMOKE_IDLE_MS ?? 600);
let url = process.env.WARM_START_TEST_URL;
let child;
let childExit;
if (!url) {
  child = spawn(process.execPath, ["dist/index.js"], {
    env: {
      ...process.env,
      HOST: "127.0.0.1",
      PORT: "0",
      KEEPALIVE_MS: String(idleMs),
      CONNECTION_TIMEOUT_MS: String(Math.min(200, idleMs)),
      SESSION_EXPIRY_GRACE_MS: "100",
      MAX_WAITING_RUNNERS: "1",
    },
    stdio: ["ignore", "pipe", "inherit"],
  });
  childExit = once(child, "exit");
  let output = "";
  child.stdout.on("data", (data) => {
    output += data;
  });
  const end = Date.now() + 10_000;
  while (!url) {
    const line = output
      .split("\n")
      .find((line) => line.includes('"event":"listening"'));
    if (line) url = "http://127.0.0.1:" + JSON.parse(line).port;
    else if (Date.now() > end || child.exitCode !== null)
      throw new Error("Service did not start");
    else await delay(10);
  }
}
const headers = {
  "x-trigger-deployment-id": "deployment_smoke",
  "x-trigger-deployment-version": "1",
  "x-trigger-workload-controller-id": "controller_smoke",
  "x-trigger-machine-cpu": "0.5",
  "x-trigger-machine-memory": "0.5",
};
try {
  const config = await (await fetch(url + "/connect")).json();
  assert(config.keepaliveMs > 0);
  assert.equal((await fetch(url + "/ready")).status, 200);
  const controller = new AbortController();
  const poll = fetch(url + "/warm-start", {
    headers,
    signal: controller.signal,
  });
  void poll.catch(() => {});
  for (let i = 0; i < 100; i++) {
    if ((await (await fetch(url + "/health")).json()).waitingRunners === 1)
      break;
    await delay(10);
  }
  const message = {
    deployment: { friendlyId: "deployment_smoke" },
    backgroundWorker: { version: "1" },
    run: { friendlyId: "run_smoke", machine: { cpu: 0.5, memory: 0.5 } },
    futureField: { retained: true },
  };
  const dispatch = await fetch(url + "/warm-start", {
    method: "POST",
    body: JSON.stringify({ dequeuedMessage: message }),
  });
  assert.deepEqual(await dispatch.json(), { didWarmStart: true });
  assert.deepEqual(await (await poll).json(), message);
  const miss = await fetch(url + "/warm-start", {
    method: "POST",
    body: '{"dequeuedMessage":{}}',
  });
  assert.equal(miss.status, 200);
  assert.deepEqual(await miss.json(), { didWarmStart: false });
  assert(
    (await (await fetch(url + "/metrics")).text()).includes(
      "warm_start_dispatches_total",
    ),
  );

  // Reproduce the legacy client: it owns per-poll retries; a terminal non-2xx exits.
  const start = performance.now();
  let retries = 0;
  let expired = false;
  while (performance.now() - start < config.keepaliveMs + 5000) {
    try {
      const response = await fetch(url + "/warm-start", {
        headers,
        signal: AbortSignal.timeout(config.connectionTimeoutMs),
      });
      await response.arrayBuffer();
      assert.equal(response.status, 408);
      expired = true;
      break;
    } catch (error) {
      if (error.name !== "TimeoutError") throw error;
      retries++;
    }
  }
  assert(expired, "Idle session never returned terminal expiry");
  const elapsedMs = Math.round(performance.now() - start);
  assert(elapsedMs <= config.keepaliveMs + 5000);
  console.log(
    JSON.stringify({
      ok: true,
      runtime: process.version,
      idleMs: config.keepaliveMs,
      elapsedMs,
      retries,
    }),
  );
} finally {
  if (child) {
    child.kill("SIGTERM");
    const kill = setTimeout(() => child.kill("SIGKILL"), 12_000);
    await childExit;
    clearTimeout(kill);
  }
}
