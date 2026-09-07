// Run explicitly inside the checkpoint service on an isolated disposable worker.
// Uses real Docker/CRIU, registry and S3; mocks ONLY the Trigger completion callback.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import path from "node:path";
import { CheckpointService, snapshotImageRef } from "../src/checkpoint-service.js";
import { loadConfig } from "../src/config.js";
import { DockerClient, containerIpAddresses } from "../src/docker.js";
import { createStorage } from "../src/storage.js";
import { RegistryClient } from "../src/registry.js";

if (process.env.CHECKPOINT_SMOKE_CONFIRM !== "disposable-worker") {
  throw new Error("Set CHECKPOINT_SMOKE_CONFIRM=disposable-worker to authorize test containers and storage writes");
}
const config = loadConfig();
assert.equal(config.rootfs.mode, "registry", "This smoke test requires registry-backed rootfs");
assert.equal(config.storage.driver, "s3", "This smoke test requires S3 storage");
const suffix = randomUUID().replaceAll("-", "");
const runId = `run_smoke_${suffix}`;
config.checkpointRoot = path.join(config.checkpointRoot, `smoke-${suffix}`);
config.reaper.enabled = false;
config.allowUntrustedControl = true;
const docker = new DockerClient({ baseUrl: config.dockerUrl });
const storage = createStorage(config.storage);
const registry = new RegistryClient({ apiUrl: config.rootfs.apiUrl, username: config.rootfs.username, password: config.rootfs.password });
const callbacks = [];
const service = new CheckpointService({ config, docker, storage, registry, fetchImpl: async (_url, init) => {
  callbacks.push(JSON.parse(init.body));
  return Response.json({ ok: true });
} });
const sourceName = `runner-smoke-${suffix}`;
const program = 'const http=require("node:http");const marker=require("node:crypto").randomUUID();let count=0;setInterval(()=>count++,100);http.createServer((q,r)=>r.end(JSON.stringify({marker,count}))).listen(8787,"0.0.0.0");';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function readMarker(id) {
  const ip = containerIpAddresses(await docker.inspectContainer(id))[0];
  for (let attempt = 0; attempt < 30; attempt++) {
    try { return await (await fetch(`http://${ip}:8787`, { signal: AbortSignal.timeout(1000) })).json(); }
    catch { await sleep(250); }
  }
  throw new Error("Test HTTP process did not respond");
}
async function suspend(runnerId, snapshotId) {
  await service.acceptSuspend({ runFriendlyId: runId, snapshotFriendlyId: snapshotId, body: {
    type: "DOCKER", runId, snapshotId, runnerId, projectRef: "proj_smoke", deploymentVersion: "smoke",
  } });
  for (let n = 0; n < 1200; n++) {
    const job = service.jobs.get(`${runId}:${snapshotId}`);
    if (job?.state === "completed" || job?.state === "failed") {
      while (service.queue.running) await sleep(100);
      return job;
    }
    await sleep(250);
  }
  throw new Error("Smoke checkpoint exceeded five minutes; artifacts retained");
}

console.log("SMOKE_BEGIN", runId);
await service.init();
const source = await docker.createContainer(sourceName, {
  Image: process.env.CHECKPOINT_SMOKE_IMAGE || "ghcr.io/sparkyidea/trigger-checkpoint-service:latest",
  Entrypoint: ["node"], Cmd: ["-e", program], Healthcheck: { Test: ["NONE"] },
  HostConfig: { AutoRemove: false, NetworkMode: "supervisor" },
  Labels: { "dev.trigger.checkpoint.smoke": runId },
});
await docker.startContainer(source.Id);
const before = await readMarker(source.Id);
console.log("SMOKE_BEFORE", before);
const job = await suspend(sourceName, "snapshot_smoke_1");
assert.equal(job.state, "completed", job.error);
assert.equal(await docker.inspectContainerOrNull(source.Id), null, "source must be deleted before restore");
assert.equal(callbacks.at(-1).success, true);
const restored = await service.restore({ runFriendlyId: runId, snapshotFriendlyId: "snapshot_smoke_2", body: {
  checkpoint: { id: `checkpoint_${suffix}`, type: "DOCKER", location: job.location, imageRef: job.rootfsImageRef },
} });
const after = await readMarker(restored.runnerId);
assert.equal(after.marker, before.marker);
assert.ok(after.count >= before.count);
console.log("SMOKE_FRESH_RESTORE_PASS", { before, after, runnerId: restored.runnerId });

// Exercise the failure boundary that stranded the real parent, without a cold start.
const pushImage = docker.pushImage.bind(docker);
docker.pushImage = async () => { throw new Error("injected smoke image-push failure"); };
const recovery = await suspend(restored.runnerId, "snapshot_smoke_recovery");
docker.pushImage = pushImage;
assert.equal(recovery.state, "failed");
assert.equal(recovery.recoveryError, undefined);
const recovered = await readMarker(restored.runnerId);
assert.equal(recovered.marker, before.marker);
assert.ok(recovered.count >= after.count);
console.log("SMOKE_SOURCE_RECOVERY_PASS", recovered);

// Cleanup is limited to this UUID-labelled test and its unique object/image tags.
await docker.removeContainer(restored.runnerId, true);
await service.deleteRun(runId);
await docker.removeImage(snapshotImageRef(config.rootfs.registryImage, runId, "snapshot_smoke_recovery"));
await docker.dispatcher.close();
storage.client?.destroy();
console.log("SMOKE_PASS", runId);
