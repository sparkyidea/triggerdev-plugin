import { afterEach, describe, expect, test } from "vitest";
import { request } from "node:http";
import type { ClientRequest } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { createHmac } from "node:crypto";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readConfig } from "./config.ts";
import { createWarmStartService } from "./server.ts";

type Service = ReturnType<typeof createWarmStartService>;
const services: Service[] = [];
const requests: ClientRequest[] = [];
afterEach(async () => {
  for (const req of requests.splice(0)) req.destroy();
  await Promise.all(services.splice(0).map((service) => service.close()));
});
async function until(predicate: () => boolean, timeout = 3000) {
  const end = performance.now() + timeout;
  while (!predicate()) {
    if (performance.now() > end) throw new Error("Condition timed out");
    await delay(5);
  }
}
async function start(env: NodeJS.ProcessEnv = {}) {
  const service = createWarmStartService(
    readConfig({
      PORT: "0",
      HOST: "127.0.0.1",
      KEEPALIVE_MS: "5000",
      CONNECTION_TIMEOUT_MS: "1000",
      SHUTDOWN_GRACE_MS: "100",
      SESSION_EXPIRY_GRACE_MS: "100",
      ...env,
    }),
  );
  services.push(service);
  service.server.listen(0, "127.0.0.1");
  await once(service.server, "listening");
  const address = service.server.address();
  if (!address || typeof address === "string") throw new Error("Missing port");
  return { service, url: `http://127.0.0.1:${address.port}` };
}
const identity = {
  "x-trigger-deployment-id": "deployment_test",
  "x-trigger-deployment-version": "20260907.1",
  "x-trigger-workload-controller-id": "controller_test",
  "x-trigger-machine-cpu": "0.5",
  "x-trigger-machine-memory": "0.5",
};
function message(id = "run_test") {
  return {
    deployment: { friendlyId: "deployment_test" },
    backgroundWorker: { version: "20260907.1" },
    run: { friendlyId: id, machine: { cpu: 0.5, memory: 0.5 } },
    snapshot: { friendlyId: "snapshot_test" },
    completedWaitpoints: [{ output: { nested: ["original", 123] } }],
    unknownFutureField: { retained: true },
  };
}
function poll(
  url: string,
  headers: Record<string, string> = {},
  path = "/warm-start",
) {
  let receivedHeaders = false;
  const req = request(url + path, {
    headers: { ...identity, ...headers },
    agent: false,
  });
  requests.push(req);
  const result = new Promise<{ status: number; body: string }>(
    (resolve, reject) => {
      req.on("error", reject);
      req.on("response", (res) => {
        receivedHeaders = true;
        const chunks: Buffer[] = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("error", reject);
        res.on("end", () =>
          resolve({
            status: res.statusCode!,
            body: Buffer.concat(chunks).toString(),
          }),
        );
      });
    },
  );
  void result.catch(() => {});
  req.end();
  return { req, result, receivedHeaders: () => receivedHeaders };
}
async function dispatch(url: string, value: unknown = message()) {
  const res = await fetch(url + "/warm-start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dequeuedMessage: value }),
  });
  return { status: res.status, body: await res.json() };
}
function jwt() {
  const data =
    Buffer.from('{"alg":"HS256"}').toString("base64url") +
    "." +
    Buffer.from(
      JSON.stringify({ deployment: "deployment_test", ver: 1 }),
    ).toString("base64url");
  return (
    data +
    "." +
    createHmac("sha256", "test-only").update(data).digest("base64url")
  );
}

describe("wire compatibility", () => {
  test("withholds headers, then returns the complete message and a true POST result", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    await delay(30);
    expect(runner.receivedHeaders()).toBe(false);
    expect(await dispatch(url)).toEqual({
      status: 200,
      body: { didWarmStart: true },
    });
    expect(JSON.parse((await runner.result).body)).toEqual(message());
    expect(service.registry.stats).toMatchObject({
      waiting: 0,
      claimed: 0,
      sessions: 0,
    });
  });

  test("token and bare waiters share a FIFO bucket", async () => {
    const { service, url } = await start({ MAX_WAITING_RUNNERS: "2" });
    const first = poll(url, {
      "x-trigger-deployment-id": jwt(),
      "x-trigger-workload-controller-id": "controller_first",
    });
    await until(() => service.registry.stats.waiting === 1);
    const second = poll(url, {
      "x-trigger-workload-controller-id": "controller_second",
    });
    await until(() => service.registry.stats.waiting === 2);
    await dispatch(url, message("run_first"));
    expect(JSON.parse((await first.result).body).run.friendlyId).toBe(
      "run_first",
    );
    expect(second.receivedHeaders()).toBe(false);
    await dispatch(url, message("run_second"));
    expect(JSON.parse((await second.result).body).run.friendlyId).toBe(
      "run_second",
    );
  });

  test("each compatibility dimension prevents matching", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    const a = message();
    a.deployment.friendlyId = "deployment_other";
    const b = message();
    b.backgroundWorker.version = "20260907.2";
    const c = message();
    c.run.machine.cpu = 1;
    const d = message();
    d.run.machine.memory = 1;
    for (const value of [a, b, c, d, {}, { deployment: {} }]) {
      expect(await dispatch(url, value)).toEqual({
        status: 200,
        body: { didWarmStart: false },
      });
    }
    expect(runner.receivedHeaders()).toBe(false);
    expect(service.registry.stats.waiting).toBe(1);
  });

  test("only one simultaneous dispatch can claim one runner", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    const results = await Promise.all([
      dispatch(url, message("run_a")),
      dispatch(url, message("run_b")),
    ]);
    expect(results.map((r) => r.body.didWarmStart).sort()).toEqual([
      false,
      true,
    ]);
    expect(["run_a", "run_b"]).toContain(
      JSON.parse((await runner.result).body).run.friendlyId,
    );
  });

  test("aliases, health, disabled settings, and removed dashboard routes", async () => {
    const { service, url } = await start({ KEEPALIVE_MS: "0" });
    expect(await (await fetch(url + "/api/connect")).json()).toEqual({
      connectionTimeoutMs: 1000,
      keepaliveMs: 0,
    });
    expect(await (await fetch(url + "/api/health")).json()).toEqual({
      status: "ok",
      waitingRunners: 0,
      deployments: 0,
    });
    for (const path of ["/", "/api/dashboard"])
      expect((await fetch(url + path)).status).toBe(404);
    expect((await poll(url, {}, "/api/warm-start").result).status).toBe(503);
    expect(service.registry.stats.sessions).toBe(0);
  });
});

describe("admission and idle lifecycle", () => {
  test("cap rejection leaves the original intact; reconnect replaces its generation", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    expect(
      (
        await poll(url, {
          "x-trigger-workload-controller-id": "controller_other",
        }).result
      ).status,
    ).toBe(503);
    expect(runner.receivedHeaders()).toBe(false);
    const replacement = poll(url);
    expect((await runner.result).status).toBe(409);
    expect(service.registry.stats.waiting).toBe(1);
    await dispatch(url);
    expect((await replacement.result).status).toBe(200);
    expect(service.metrics.rejections.waiting_cap).toBe(1);
  });

  test("client abort releases occupancy, preserves the session and allows reconnect", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    runner.req.destroy();
    await until(() => service.registry.stats.waiting === 0);
    expect(service.registry.stats.sessions).toBe(1);
    const again = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    await dispatch(url);
    expect((await again.result).status).toBe(200);
    expect(service.registry.stats.sessions).toBe(0);
  });

  test("reconnect does not reset the idle deadline, including an expiry/abort race", async () => {
    const { service, url } = await start({
      KEEPALIVE_MS: "350",
      SESSION_EXPIRY_GRACE_MS: "500",
    });
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    const startTime = performance.now();
    await delay(200);
    runner.req.destroy();
    await until(() => service.registry.stats.waiting === 0);
    const again = poll(url);
    expect((await again.result).status).toBe(408);
    expect(performance.now() - startTime).toBeLessThan(500);
    expect((await poll(url).result).status).toBe(408);
    await until(() => service.registry.stats.sessions === 0);
    expect(service.metrics.expirations).toBe(1);
  });

  test("changed identity on a reconnect cannot reset or hijack a session", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    runner.req.destroy();
    await until(() => service.registry.stats.waiting === 0);
    expect(
      (await poll(url, { "x-trigger-deployment-version": "other" }).result)
        .status,
    ).toBe(409);
  });

  test("disconnected metadata stays bounded and expires", async () => {
    const { service, url } = await start({
      MAX_IDLE_SESSIONS: "1",
      KEEPALIVE_MS: "100",
    });
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    runner.req.destroy();
    await until(() => service.registry.stats.waiting === 0);
    expect(
      (await poll(url, { "x-trigger-workload-controller-id": "other" }).result)
        .status,
    ).toBe(503);
    expect(service.metrics.rejections.session_cap).toBe(1);
    await until(() => service.registry.stats.sessions === 0);
  });

  test("poll margin prevents a late claim without closing the poll", async () => {
    const { service, url } = await start({
      CONNECTION_TIMEOUT_MS: "150",
      POLL_MATCH_MARGIN_MS: "80",
    });
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    await delay(100);
    expect((await dispatch(url)).body.didWarmStart).toBe(false);
    expect(runner.receivedHeaders()).toBe(false);
  });

  test("claimed writes still count against admission, and known failure keeps deadline", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    const key = JSON.stringify(["deployment_test", "20260907.1", "0.5", "0.5"]);
    const claimed = service.registry.claim(key)!;
    expect(service.registry.stats).toMatchObject({ waiting: 0, claimed: 1 });
    expect(
      (await poll(url, { "x-trigger-workload-controller-id": "other" }).result)
        .status,
    ).toBe(503);
    const deadline = claimed.session.deadline;
    runner.req.destroy();
    await delay(20);
    service.registry.complete(claimed, false);
    const again = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    const secondClaim = service.registry.claim(key)!;
    expect(secondClaim.session.deadline).toBe(deadline);
    service.registry.complete(secondClaim, false);
    again.req.destroy();
  });

  test("draining fails readiness and terminates admitted polls with 503", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    service.registry.drain();
    expect((await fetch(url + "/ready")).status).toBe(503);
    expect((await runner.result).status).toBe(503);
    expect((await dispatch(url)).status).toBe(503);
    await service.close();
  });
});

describe("validation and telemetry", () => {
  test("registration errors do not expose tokens", async () => {
    const { service, url } = await start();
    const raw = "abc.not-valid-json.xyz";
    const result = await poll(url, { "x-trigger-deployment-id": raw }).result;
    expect(result.status).toBe(400);
    expect(result.body).not.toContain(raw);
    expect(service.registry.stats.sessions).toBe(0);
  });

  test("malformed messages, methods and declared/chunked oversize bodies", async () => {
    const { service, url } = await start({ MAX_REQUEST_BODY_BYTES: "512" });
    for (const body of [
      "{",
      "{}",
      '{"dequeuedMessage":null}',
      '{"dequeuedMessage":[]}',
    ]) {
      expect(
        (await fetch(url + "/warm-start", { method: "POST", body })).status,
      ).toBe(400);
    }
    expect((await fetch(url + "/warm-start", { method: "PUT" })).status).toBe(
      405,
    );
    expect(
      (
        await fetch(url + "/warm-start", {
          method: "POST",
          body: "x".repeat(600),
        })
      ).status,
    ).toBe(413);
    const result = await new Promise<number>((resolve) => {
      const req = request(
        url + "/warm-start",
        { method: "POST", headers: { "Transfer-Encoding": "chunked" } },
        (res) => {
          res.resume();
          resolve(res.statusCode!);
        },
      );
      requests.push(req);
      req.on("error", () => {});
      req.write("x".repeat(300));
      req.end("x".repeat(300));
    });
    expect(result).toBe(413);
    expect(service.metrics.bodyRejections).toBe(2);
  });

  test("large waitpoint outputs are forwarded intact under the default limit", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    const value = {
      ...message(),
      completedWaitpoints: [{ output: "x".repeat(3 * 1024 * 1024) }],
    };
    expect((await dispatch(url, value)).body.didWarmStart).toBe(true);
    expect(JSON.parse((await runner.result).body)).toEqual(value);
  });

  test("Prometheus exposition has bounded labels and cumulative buckets", async () => {
    const { service, url } = await start();
    const runner = poll(url);
    await until(() => service.registry.stats.waiting === 1);
    await dispatch(url);
    await runner.result;
    await dispatch(url, {});
    const response = await fetch(url + "/metrics");
    const body = await response.text();
    expect(response.headers.get("content-type")).toContain("version=0.0.4");
    expect(body).toContain('warm_start_dispatches_total{outcome="matched"} 1');
    expect(body).toContain('warm_start_dispatches_total{outcome="miss"} 1');
    expect(body).toContain(
      'warm_start_dispatch_duration_seconds_bucket{le="+Inf"} 2',
    );
    expect(body).toContain("warm_start_dispatch_duration_seconds_count 2");
    expect(body).not.toContain("deployment_test");
    const buckets = [...body.matchAll(/_bucket\{le="[^"]+"\} (\d+)/g)].map(
      (m) => Number(m[1]),
    );
    expect(buckets).toEqual([...buckets].sort((a, b) => a - b));
  });

  test.each(["false", "true"])(
    "subprocess logs redact identity tokens with debug=%s",
    async (debug) => {
      const child = spawn(
        process.execPath,
        ["--experimental-strip-types", "src/index.ts"],
        {
          env: {
            ...process.env,
            HOST: "127.0.0.1",
            PORT: "0",
            DEBUG_LOGGING: debug,
          },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      let output = "";
      child.stdout.on("data", (data) => {
        output += data.toString();
      });
      child.stderr.on("data", (data) => {
        output += data.toString();
      });
      const exited = once(child, "exit");
      try {
        // Node startup can exceed 3s under QEMU-emulated multi-arch CI builds.
        await until(() => output.includes('"event":"listening"'), 30_000);
        const line = output
          .split("\n")
          .find((l) => l.includes('"event":"listening"'))!;
        const url = "http://127.0.0.1:" + JSON.parse(line).port;
        const token = jwt();
        const runner = poll(url, { "x-trigger-deployment-id": token });
        await until(() => runner.req.writableFinished);
        // Health check confirms registration was processed, not merely sent.
        for (let i = 0; i < 100; i++) {
          if (
            (await (await fetch(url + "/health")).json()).waitingRunners === 1
          )
            break;
          await delay(10);
        }
        const trace = "00-" + "1".repeat(32) + "-" + "2".repeat(16) + "-01";
        const res = await fetch(url + "/warm-start", {
          method: "POST",
          headers: { traceparent: trace },
          body: JSON.stringify({ dequeuedMessage: message() }),
        });
        expect((await res.json()).didWarmStart).toBe(true);
        await runner.result;
        const invalid =
          token.split(".")[0] + ".invalid_payload." + token.split(".")[2];
        expect(
          (await poll(url, { "x-trigger-deployment-id": invalid }).result)
            .status,
        ).toBe(400);
        child.kill("SIGTERM");
        await exited;
        expect(output).not.toContain(token);
        expect(output).not.toContain(invalid);
        expect(output).toContain(trace);
        expect(output).toContain("deployment_test");
      } finally {
        if (child.exitCode === null) child.kill("SIGKILL");
        await exited;
      }
    },
    45_000,
  );
});

test("stalled runner writes are bounded and do not dispatch to another waiter", async () => {
  const { service, url } = await start({
    MAX_WAITING_RUNNERS: "2",
    MAX_REQUEST_BODY_BYTES: String(24 * 1024 * 1024),
    DISPATCH_WRITE_TIMEOUT_MS: "150",
    KEEPALIVE_MS: "10000",
  });
  const req = request(url + "/warm-start", { headers: identity, agent: false });
  requests.push(req);
  req.on("error", () => {});
  req.on("response", (response) => {
    response.pause();
    response.on("error", () => {});
  });
  req.end();
  await until(() => service.registry.stats.waiting === 1);
  const other = poll(url, {
    "x-trigger-workload-controller-id": "controller_other",
  });
  await until(() => service.registry.stats.waiting === 2);
  const value = {
    ...message(),
    completedWaitpoints: [{ output: "x".repeat(16 * 1024 * 1024) }],
  };
  expect((await dispatch(url, value)).body.didWarmStart).toBe(true);
  expect(service.metrics.dispatches.ambiguous).toBe(1);
  expect(service.metrics.dispatches.matched).toBe(0);
  expect(service.registry.stats.claimed).toBe(0);
  expect(other.receivedHeaders()).toBe(false);
  expect(service.registry.stats.waiting).toBe(1);
});

test("shutdown bounds an incomplete POST body", async () => {
  const { service, url } = await start({ SHUTDOWN_GRACE_MS: "50" });
  const req = request(url + "/warm-start", {
    method: "POST",
    headers: { "Content-Length": "100" },
  });
  requests.push(req);
  req.on("error", () => {});
  req.write("{");
  await delay(30);
  const started = performance.now();
  await service.close();
  expect(performance.now() - started).toBeLessThan(1000);
  expect(service.registry.stats.sessions).toBe(0);
});
