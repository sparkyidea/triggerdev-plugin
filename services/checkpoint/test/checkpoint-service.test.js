import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { CheckpointService, restoreRunnerId } from "../src/checkpoint-service.js";
import { LocalStorage } from "../src/storage.js";

function testConfig(root) {
  return {
    maxConcurrentJobs: 1,
    checkpointRoot: root,
    storage: { driver: "local" },
    rootfs: { mode: "none", username: "", password: "", serverAddress: "" },
    trigger: {
      apiUrl: "http://webapp.test",
      workerToken: "token",
      managedWorkerSecret: "managed",
      workerInstanceName: "worker-1",
      supervisorProtocol: "http",
      supervisorDomain: "supervisor",
      supervisorPort: 8020,
    },
    compatibility: { requireKernelMatch: false },
    reaper: { enabled: false, intervalMs: 60_000, graceSeconds: 600 },
    allowUntrustedControl: true,
    controlAllowedHost: "supervisor",
  };
}

function createFakeDocker() {
  const calls = [];
  let restoredContainerStarted = false;
  const oldInspect = {
    Config: { Image: "registry/tasks:v1", Env: ["A=1"], Cmd: ["node", "app.js"] },
    HostConfig: { AutoRemove: false, NetworkMode: "supervisor", Memory: 1234 },
    NetworkSettings: { Networks: { supervisor: { Aliases: ["runner-old"] } } },
    State: { Running: true },
  };
  return {
    calls,
    async ping() {},
    async info() {
      return { KernelVersion: "6.8.0" };
    },
    async version() {
      return { Arch: "amd64", Os: "linux", Version: "29.2.1" };
    },
    async inspectContainer(id) {
      calls.push(["inspect", id]);
      if (id === "container-created") {
        return {
          State: { Running: restoredContainerStarted },
          NetworkSettings: {
            Networks: {
              supervisor: { IPAddress: restoredContainerStarted ? "172.20.0.12" : "" },
            },
          },
        };
      }
      return oldInspect;
    },
    async inspectContainerOrNull() {
      return null;
    },
    async checkpointContainer(id, checkpointId, checkpointDir) {
      calls.push(["checkpoint", id, checkpointId]);
      const destination = path.join(checkpointDir, checkpointId);
      await mkdir(destination, { recursive: true });
      await writeFile(path.join(destination, "inventory.img"), "checkpoint-data");
    },
    async removeContainer(id) {
      calls.push(["remove", id]);
    },
    async inspectImageOrNull() {
      return { Id: "image" };
    },
    async createContainer(name, spec) {
      calls.push(["create", name, spec]);
      return { Id: "container-created" };
    },
    async startContainer(id, options) {
      calls.push(["start", id, options]);
      if (id === "container-created") restoredContainerStarted = true;
    },
    async listExitedRunnerContainers() {
      return [];
    },
    async removeImage() {},
  };
}

async function waitFor(predicate, timeout = 3000) {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeout) throw new Error("Timed out waiting for condition");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

test("suspends, archives, calls back, and restores a runner", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const config = testConfig(root);
  const docker = createFakeDocker();
  const storage = new LocalStorage(path.join(root, "archives"));
  const callbacks = [];
  const service = new CheckpointService({
    config,
    docker,
    storage,
    registry: { enabled: false },
    fetchImpl: async (url, init) => {
      callbacks.push({ url, init });
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    },
  });
  await service.init();

  await service.acceptSuspend({
    runFriendlyId: "run_abc",
    snapshotFriendlyId: "snapshot_1",
    body: {
      type: "DOCKER",
      runId: "run_abc",
      snapshotId: "snapshot_1",
      runnerId: "runner-abc",
      projectRef: "proj_1",
      deploymentVersion: "20260906.1",
    },
  });
  await waitFor(() => service.jobs.get("run_abc:snapshot_1")?.state === "completed");

  assert.equal(callbacks.length, 1);
  const callbackBody = JSON.parse(callbacks[0].init.body);
  assert.equal(callbackBody.success, true);
  assert.match(callbackBody.checkpoint.location, /^file:/);
  assert.equal(callbackBody.checkpoint.type, "DOCKER");
  assert.ok(docker.calls.some(([operation]) => operation === "checkpoint"));

  const databaseCheckpointId = "checkpoint_db_12345678";
  const result = await service.restore({
    runFriendlyId: "run_abc",
    snapshotFriendlyId: "snapshot_2",
    body: {
      version: "1",
      image: "registry/tasks:v1",
      checkpoint: {
        id: databaseCheckpointId,
        type: "DOCKER",
        location: callbackBody.checkpoint.location,
        imageRef: null,
      },
    },
  });
  assert.equal(result.runnerId, restoreRunnerId("run_abc", databaseCheckpointId));
  assert.equal((await service.metadataFor("172.20.0.12")).TRIGGER_SNAPSHOT_ID, "snapshot_2");
  const startIndex = docker.calls.findIndex(
    ([operation, id]) => operation === "start" && id === "container-created"
  );
  const postStartInspectIndex = docker.calls.findIndex(
    ([operation, id], index) => operation === "inspect" && id === "container-created" && index > startIndex
  );
  assert.ok(startIndex >= 0);
  assert.ok(postStartInspectIndex > startIndex);
});

test("keeps a successful checkpoint completed when local cleanup fails", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const config = testConfig(root);
  const docker = createFakeDocker();
  docker.removeContainer = async () => {
    throw new Error("cleanup failed");
  };
  const callbacks = [];
  const service = new CheckpointService({
    config,
    docker,
    storage: new LocalStorage(path.join(root, "archives")),
    registry: { enabled: false },
    fetchImpl: async (_url, init) => {
      callbacks.push(JSON.parse(init.body));
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    },
  });
  await service.init();
  await service.acceptSuspend({
    runFriendlyId: "run_cleanup",
    snapshotFriendlyId: "snapshot_1",
    body: {
      type: "DOCKER",
      runId: "run_cleanup",
      snapshotId: "snapshot_1",
      runnerId: "runner-cleanup",
      projectRef: "proj_1",
      deploymentVersion: "1",
    },
  });

  await waitFor(() => service.jobs.get("run_cleanup:snapshot_1")?.state === "completed");
  assert.deepEqual(callbacks.map((body) => body.success), [true]);
});

test("does not report failure after the success callback if completion persistence fails", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const config = testConfig(root);
  const callbacks = [];
  const service = new CheckpointService({
    config,
    docker: createFakeDocker(),
    storage: new LocalStorage(path.join(root, "archives")),
    registry: { enabled: false },
    fetchImpl: async (_url, init) => {
      callbacks.push(JSON.parse(init.body));
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    },
  });
  const saveJob = service.saveJob.bind(service);
  service.saveJob = (job) =>
    job.state === "completed" ? Promise.reject(new Error("disk full")) : saveJob(job);
  await service.init();
  await service.acceptSuspend({
    runFriendlyId: "run_persistence",
    snapshotFriendlyId: "snapshot_1",
    body: {
      type: "DOCKER",
      runId: "run_persistence",
      snapshotId: "snapshot_1",
      runnerId: "runner-persistence",
      projectRef: "proj_1",
      deploymentVersion: "1",
    },
  });

  await waitFor(() => service.jobs.get("run_persistence:snapshot_1")?.state === "completed");
  assert.deepEqual(callbacks.map((body) => body.success), [true]);
});

test("holds an early restored-runner metadata lookup until its IP is registered", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const service = new CheckpointService({
    config: testConfig(root),
    docker: createFakeDocker(),
    storage: new LocalStorage(path.join(root, "archives")),
    registry: { enabled: false },
  });
  service.pendingMetadataRegistrations = 1;
  const lookup = service.metadataFor("172.20.0.25");
  setImmediate(() => {
    service.metadataByIp.set("172.20.0.25", { TRIGGER_RUN_ID: "run_race" });
    service.pendingMetadataRegistrations = 0;
  });

  assert.equal((await lookup).TRIGGER_RUN_ID, "run_race");
});

test("reaper uses exit time and protects runners with in-flight checkpoint jobs", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const config = testConfig(root);
  const docker = createFakeDocker();
  const removed = [];
  const recent = new Date(Date.now() - 30_000).toISOString();
  const old = new Date(Date.now() - 700_000).toISOString();
  docker.listExitedRunnerContainers = async () => [
    { Id: "active-id", Names: ["/runner-active"], Created: 1 },
    { Id: "recent-id", Names: ["/runner-recent"], Created: 1 },
    { Id: "old-id", Names: ["/runner-old"], Created: Math.floor(Date.now() / 1000) },
  ];
  docker.inspectContainerOrNull = async (id) => ({
    State: { FinishedAt: id === "recent-id" ? recent : old },
  });
  docker.removeContainer = async (id) => removed.push(id);
  const service = new CheckpointService({
    config,
    docker,
    storage: new LocalStorage(path.join(root, "archives")),
    registry: { enabled: false },
  });
  service.jobs.set("run_active:snapshot_1", {
    state: "checkpointed",
    body: { runnerId: "runner-active" },
  });

  await service.reapExitedRunners();
  assert.deepEqual(removed, ["old-id"]);
});

test("never cold-starts a runner when checkpoint recovery fails", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const config = testConfig(root);
  const docker = createFakeDocker();
  const starts = [];
  docker.inspectContainerOrNull = async () => ({ State: { Running: false } });
  docker.startContainer = async (id, options) => {
    starts.push([id, options]);
    if (options?.checkpointId) throw new Error("partial checkpoint");
  };
  const service = new CheckpointService({
    config,
    docker,
    storage: new LocalStorage(path.join(root, "archives")),
    registry: { enabled: false },
  });
  const job = {
    state: "checkpointed",
    runFriendlyId: "run_recovery",
    snapshotFriendlyId: "snapshot_1",
    checkpointId: "checkpoint-test",
    body: { runnerId: "runner-recovery" },
  };
  await mkdir(path.join(service.jobDir(job.runFriendlyId, job.snapshotFriendlyId), "checkpoint", job.checkpointId), {
    recursive: true,
  });

  await assert.rejects(service.recoverSourceRunner(job), /partial checkpoint/);
  assert.equal(starts.length, 1);
  assert.equal(starts[0][1].checkpointId, job.checkpointId);
});

test("never cold-starts a stopped source with missing checkpoint files", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const docker = createFakeDocker();
  docker.inspectContainerOrNull = async () => ({ State: { Running: false } });
  const service = new CheckpointService({ config: testConfig(root), docker });
  await assert.rejects(service.recoverSourceRunner({
    state: "checkpointed", runFriendlyId: "run_missing", snapshotFriendlyId: "snapshot_1",
    checkpointId: "checkpoint-1", body: { runnerId: "runner-missing" },
  }), /refusing to cold-start/);
  assert.equal(docker.calls.some(([op]) => op === "start"), false);
});

test("reaper preserves failed recovery sources for manual recovery", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const docker = createFakeDocker();
  docker.listExitedRunnerContainers = async () => [{ Id: "source", Names: ["/runner-failed"] }];
  docker.inspectContainerOrNull = async () => ({ State: { FinishedAt: "2020-01-01T00:00:00Z" } });
  const service = new CheckpointService({ config: testConfig(root), docker });
  service.jobs.set("run_failed:snapshot_1", {
    state: "failed", recoveryError: "restore failed", body: { runnerId: "runner-failed" },
  });
  await service.reapExitedRunners();
  assert.equal(docker.calls.some(([op]) => op === "remove"), false);
});

test("restore shares the suspend queue concurrency limit", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const config = testConfig(root);
  const service = new CheckpointService({
    config,
    docker: createFakeDocker(),
    storage: new LocalStorage(path.join(root, "archives")),
    registry: { enabled: false },
  });
  let releaseSuspend;
  let restoreStarted = false;
  const suspendGate = new Promise((resolve) => {
    releaseSuspend = resolve;
  });
  service.queue.add(() => suspendGate);
  service.processRestore = async () => {
    restoreStarted = true;
    return { ok: true };
  };

  const restore = service.restore({});
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(restoreStarted, false);
  releaseSuspend();
  assert.deepEqual(await restore, { ok: true });
  assert.equal(restoreStarted, true);
});

test("refuses to checkpoint auto-remove runners before stopping them", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-checkpoint-test-"));
  const config = testConfig(root);
  const docker = createFakeDocker();
  docker.inspectContainer = async () => ({
    Config: { Image: "registry/tasks:v1" },
    HostConfig: { AutoRemove: true },
  });
  const callbacks = [];
  const service = new CheckpointService({
    config,
    docker,
    storage: new LocalStorage(path.join(root, "archives")),
    registry: { enabled: false },
    fetchImpl: async (_url, init) => {
      callbacks.push(JSON.parse(init.body));
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    },
  });
  await service.init();
  await service.acceptSuspend({
    runFriendlyId: "run_noauto",
    snapshotFriendlyId: "snapshot_1",
    body: {
      type: "DOCKER",
      runId: "run_noauto",
      snapshotId: "snapshot_1",
      runnerId: "runner-noauto",
      projectRef: "proj_1",
      deploymentVersion: "1",
    },
  });
  await waitFor(() => service.jobs.get("run_noauto:snapshot_1")?.state === "failed");
  assert.equal(callbacks.at(-1).success, false);
  assert.match(callbacks.at(-1).error, /AutoRemove enabled/);
});
