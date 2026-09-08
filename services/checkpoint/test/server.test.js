import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import http from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { promisify } from "node:util";
import { CheckpointService } from "../src/checkpoint-service.js";
import { DockerClient } from "../src/docker.js";
import { LocalStorage } from "../src/storage.js";
import { createServer } from "../src/server.js";
import { loadConfig } from "../src/config.js";

const execute = promisify(execFile);
const probe = (url) => execute("curl", ["--fail", "--silent", "--show-error", "--max-time", "4", "--output", "/dev/null", url], { timeout: 6000 });

test("HTTP routes validate requests, reject untrusted callers and report Docker failures", { timeout: 15000 }, async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "checkpoint-http-test-"));
  let hung = false;
  const dockerServer = http.createServer((request, response) => {
    if (hung) return;
    response.writeHead(request.url === "/_ping" ? 200 : 404);
    response.end("OK");
  });
  await new Promise((resolve) => dockerServer.listen(0, "127.0.0.1", resolve));
  const docker = new DockerClient({ baseUrl: `http://127.0.0.1:${dockerServer.address().port}` });
  const config = loadConfig({ CHECKPOINT_ROOT: root, CHECKPOINT_ROOTFS_MODE: "none",
    CHECKPOINT_REAPER_ENABLED: "false", CHECKPOINT_ALLOW_UNTRUSTED_CONTROL: "true",
    TRIGGER_API_URL: "http://fixture.invalid", TRIGGER_WORKER_TOKEN: "fixture", TRIGGER_WORKER_INSTANCE_NAME: "fixture" });
  const service = new CheckpointService({ config, docker, storage: new LocalStorage(path.join(root, "archives")) });
  const server = createServer(service, config);
  try {
    await service.init();
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const base = `http://127.0.0.1:${server.address().port}`;
    assert.equal((await (await fetch(`${base}/health`)).json()).ok, true);
    assert.deepEqual(await (await fetch(`${base}/env`)).json(), {});
    const invalid = await fetch(`${base}/api/v1/runs/run_test/snapshots/snapshot_test/restore`, {
      method: "POST", body: JSON.stringify({ checkpoint: { type: "COLD" } }) });
    assert.equal(invalid.status, 400);
    service.isControlRequestAllowed = async () => false;
    assert.equal((await fetch(`${base}/api/v1/runs/run_test/checkpoints/delete`, { method: "POST" })).status, 403);

    // CI runs this mode inside the built image to check its packaged curl client.
    if (process.env.CHECKPOINT_TEST_IMAGE === "1") {
      await probe(`${base}/health`);
      hung = true;
      await assert.rejects(probe(`${base}/health`), (error) => error.code === 28);
      hung = false;
    }
    dockerServer.closeAllConnections();
    await new Promise((resolve) => dockerServer.close(resolve));
    assert.equal((await fetch(`${base}/health`)).status, 500);
    if (process.env.CHECKPOINT_TEST_IMAGE === "1") {
      await assert.rejects(probe(`${base}/health`), (error) => error.code === 22);
    }
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    if (process.env.CHECKPOINT_TEST_IMAGE === "1") {
      await assert.rejects(probe(`${base}/health`), (error) => error.code === 7);
    }
  } finally {
    server.closeAllConnections(); server.close();
    dockerServer.closeAllConnections(); dockerServer.close();
    await docker.dispatcher.close();
    await rm(root, { recursive: true, force: true });
  }
});
