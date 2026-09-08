import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { request } from "node:http";
import { fileURLToPath } from "node:url";

const waiters = Number(process.env.BENCH_WAITERS ?? 100);
const rounds = Number(process.env.BENCH_ROUNDS ?? 5);
const entry = process.env.BENCH_ENTRY ?? "dist/index.js";
const cwd = process.env.BENCH_CWD ?? process.cwd();
const port = process.env.BENCH_PORT ?? "18089";
const child = spawn(
  process.execPath,
  [
    "--import",
    fileURLToPath(new URL("./instrument.mjs", import.meta.url)),
    entry,
  ],
  {
    cwd,
    env: {
      ...process.env,
      HOST: "127.0.0.1",
      HOSTNAME: "127.0.0.1",
      PORT: port,
      MAX_WAITING_RUNNERS: String(waiters),
      MAX_IDLE_SESSIONS: String(Math.max(waiters * rounds * 2, 1024)),
      KEEPALIVE_MS: "300000",
      CONNECTION_TIMEOUT_MS: "30000",
      DEBUG_LOGGING: "false",
    },
    stdio: ["ignore", "ignore", "inherit", "ipc"],
  },
);
const exited = once(child, "exit");
const url = "http://127.0.0.1:" + port;
const started = performance.now();
const pending = new Set();
async function stats() {
  const measurement = new Promise((resolve) => {
    const onMessage = (value) => {
      if (value?.type === "measurement") {
        child.off("message", onMessage);
        resolve(value);
      }
    };
    child.on("message", onMessage);
  });
  child.send("measure");
  return measurement;
}
async function waitForCount(count) {
  const end = performance.now() + 30_000;
  while (performance.now() < end) {
    if ((await (await fetch(url + "/health")).json()).waitingRunners === count)
      return;
    await delay(5);
  }
  throw new Error("Runner count did not converge");
}
try {
  while (true) {
    try {
      if ((await fetch(url + "/health")).ok) break;
    } catch {}
    if (performance.now() - started > 30_000 || child.exitCode !== null)
      throw new Error("Startup failed");
    await delay(10);
  }
  const startupMs = performance.now() - started;
  const idle = await stats();
  let waiting;
  const durations = [];
  for (let round = 0; round < rounds; round++) {
    const polls = Array.from({ length: waiters }, (_, i) => {
      const req = request(url + "/warm-start", {
        agent: false,
        headers: {
          "x-trigger-deployment-id": "deployment_benchmark",
          "x-trigger-deployment-version": "1",
          "x-trigger-workload-controller-id": `controller_${round}_${i}`,
          "x-trigger-machine-cpu": "0.5",
          "x-trigger-machine-memory": "0.5",
        },
      });
      pending.add(req);
      const done = new Promise((resolve, reject) => {
        req.on("error", reject);
        req.on("response", (response) => {
          response.resume();
          response.on("error", reject);
          response.on("end", () => {
            pending.delete(req);
            resolve(response.statusCode);
          });
        });
      });
      void done.catch(() => {});
      req.end();
      return done;
    });
    await waitForCount(waiters);
    if (!waiting) waiting = await stats();
    for (let i = 0; i < waiters; i++) {
      const start = performance.now();
      const response = await fetch(url + "/warm-start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dequeuedMessage: {
            deployment: { friendlyId: "deployment_benchmark" },
            backgroundWorker: { version: "1" },
            run: {
              friendlyId: `run_${round}_${i}`,
              machine: { cpu: 0.5, memory: 0.5 },
            },
          },
        }),
      });
      assert.equal((await response.json()).didWarmStart, true);
      durations.push(performance.now() - start);
    }
    assert((await Promise.all(polls)).every((status) => status === 200));
  }
  const afterChurn = await stats();
  durations.sort((a, b) => a - b);
  const percentile = (q) =>
    durations[Math.min(durations.length - 1, Math.floor(durations.length * q))];
  console.log(
    JSON.stringify({
      entry,
      runtime: process.version,
      waiters,
      rounds,
      startupMs,
      idle,
      waiting,
      afterChurn,
      rssPerWaiterBytes: (waiting.rssBytes - idle.rssBytes) / waiters,
      dispatchMs: {
        p50: percentile(0.5),
        p95: percentile(0.95),
        p99: percentile(0.99),
      },
    }),
  );
} finally {
  for (const req of pending) req.destroy();
  child.kill("SIGTERM");
  const kill = setTimeout(() => child.kill("SIGKILL"), 12_000);
  await exited;
  clearTimeout(kill);
}
