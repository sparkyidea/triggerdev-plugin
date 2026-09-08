import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, writeFile, rm, access } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { CheckpointService, restoreRunnerId, snapshotImageRef } from "../src/checkpoint-service.js";
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
  let restoredSpec;
  let restoredName;
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
      if (id === "a".repeat(64)) {
        return {
          Id: "a".repeat(64),
          Config: restoredSpec,
          State: { Running: restoredContainerStarted, Status: restoredContainerStarted ? "running" : "created" },
          NetworkSettings: {
            Networks: {
              supervisor: { IPAddress: restoredContainerStarted ? "172.20.0.12" : "" },
            },
          },
        };
      }
      return oldInspect;
    },
    async inspectContainerOrNull(id) {
      if (restoredSpec && [restoredName, "a".repeat(64)].includes(id)) return this.inspectContainer("a".repeat(64));
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
      restoredSpec = spec; restoredName = name;
      return { Id: "a".repeat(64) };
    },
    async startContainer(id, options) {
      calls.push(["start", id, options]);
      if (id === "a".repeat(64)) restoredContainerStarted = true;
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
    ([operation, id]) => operation === "start" && id === "a".repeat(64)
  );
  const postStartInspectIndex = docker.calls.findIndex(
    ([operation, id], index) => operation === "inspect" && id === "a".repeat(64) && index > startIndex
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
    const inspect = ownedContainer("b".repeat(64), "172.20.0.25", "run_race", "snapshot_1");
    service.docker.inspectContainerOrNull = async () => inspect;
    service.registerMetadata(inspect, { TRIGGER_RUN_ID: "run_race", TRIGGER_SNAPSHOT_ID: "snapshot_1" });
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

function ownedContainer(id, ip, run = "run_owner", snapshot = "snapshot_1") {
  return { Id: id, Config: { Labels: { "dev.trigger.checkpoint.run": run,
    "dev.trigger.checkpoint.snapshot": snapshot } }, State: { Running: true, Status: "running" },
    NetworkSettings: { Networks: { supervisor: { IPAddress: ip } } } };
}

async function fixture(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), "checkpoint-lifecycle-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const docker = createFakeDocker();
  const config = testConfig(root);
  const service = new CheckpointService({ config, docker,
    storage: new LocalStorage(path.join(root, "archives")), registry: { enabled: false },
    fetchImpl: async () => Response.json({ ok: true }) });
  await service.init();
  return { root, docker, config, service };
}

for (const state of ["removed", "exited", "different-ip", "different-labels", "docker-error"]) {
  test(`metadata never returns stale identity after ${state}`, async (t) => {
    const { service, docker } = await fixture(t);
    const owner = ownedContainer("b".repeat(64), "172.20.0.25");
    const metadata = { TRIGGER_RUN_ID: "run_owner", TRIGGER_SNAPSHOT_ID: "snapshot_1" };
    service.registerMetadata(owner, metadata);
    docker.inspectContainerOrNull = async () => {
      if (state === "docker-error") throw new Error("unavailable");
      if (state === "removed") return null;
      if (state === "exited") return { ...owner, State: { Running: false, Status: "exited" } };
      if (state === "different-ip") return ownedContainer(owner.Id, "172.20.0.26");
      return ownedContainer(owner.Id, "172.20.0.25", "run_other");
    };
    assert.deepEqual(await service.metadataFor("::ffff:172.20.0.25"), {});
    if (state !== "docker-error") assert.equal(service.metadataByIp.size, 0);
  });
}

test("old-owner cleanup and delayed lookup preserve a new owner's reused IP", async (t) => {
  const { service, docker } = await fixture(t);
  const old = ownedContainer("b".repeat(64), "172.20.0.25");
  const next = ownedContainer("c".repeat(64), "172.20.0.25", "run_next");
  service.registerMetadata(old, { TRIGGER_RUN_ID: "run_owner", TRIGGER_SNAPSHOT_ID: "snapshot_1" });
  let release;
  docker.inspectContainerOrNull = () => new Promise((resolve) => { release = resolve; });
  const pending = service.metadataFor("172.20.0.25");
  service.registerMetadata(next, { TRIGGER_RUN_ID: "run_next", TRIGGER_SNAPSHOT_ID: "snapshot_1" });
  release(null);
  assert.deepEqual(await pending, {});
  service.removeMetadata(old.Id);
  docker.inspectContainerOrNull = async () => next;
  assert.equal((await service.metadataFor("172.20.0.25")).TRIGGER_RUN_ID, "run_next");
});

async function completedRestore(service, restoreNow = true) {
  await service.acceptSuspend({ runFriendlyId: "run_dup", snapshotFriendlyId: "snapshot_source", body: {
    type: "DOCKER", runId: "run_dup", snapshotId: "snapshot_source", runnerId: "runner-source",
    projectRef: "proj_1", deploymentVersion: "1" } });
  await waitFor(() => service.jobs.get("run_dup:snapshot_source")?.state === "completed");
  const location = service.jobs.get("run_dup:snapshot_source").location;
  const request = { runFriendlyId: "run_dup", snapshotFriendlyId: "snapshot_restore", body: {
    checkpoint: { type: "DOCKER", id: "checkpoint_12345678", location } } };
  if (restoreNow) await service.restore(request);
  return request;
}

test("duplicate restore and checkpoint deletion preserve metadata across service restart", async (t) => {
  const { service, docker, config } = await fixture(t);
  const request = await completedRestore(service);
  const file = path.join(service.restoreDir("run_dup", "snapshot_restore"), "restore.json");
  const before = await readFile(file, "utf8");
  service.storage.getCheckpoint = async () => { throw new Error("duplicate must not download"); };
  assert.equal((await service.restore(request)).duplicate, true);
  assert.equal(await readFile(file, "utf8"), before);
  await service.deleteRun("run_dup");
  assert.equal(service.jobs.size, 0);
  assert.equal(await readFile(file, "utf8"), before);
  const restarted = new CheckpointService({ config, docker, storage: service.storage });
  await restarted.init();
  assert.equal((await restarted.metadataFor("172.20.0.12")).TRIGGER_RUN_ID, "run_dup");
  assert.equal((await restarted.restore(request)).duplicate, true);
});

for (const conflict of ["missing-record", "incomplete-record", "different-checkpoint", "different-container", "different-snapshot"]) {
  test(`duplicate restore preserves live files on ${conflict}`, async (t) => {
    const { service, docker } = await fixture(t);
    const request = await completedRestore(service);
    const directory = service.restoreDir("run_dup", "snapshot_restore");
    const recordFile = path.join(directory, "restore.json");
    if (conflict === "missing-record") await rm(recordFile);
    if (conflict === "incomplete-record") {
      const record = JSON.parse(await readFile(recordFile));
      delete record.checkpointDatabaseId;
      await writeFile(recordFile, JSON.stringify(record));
    }
    if (conflict === "different-checkpoint") request.body.checkpoint.id = "other_12345678";
    if (conflict === "different-snapshot") request.snapshotFriendlyId = "snapshot_other";
    if (conflict === "different-container") {
      const inspect = await docker.inspectContainer("a".repeat(64));
      docker.inspectContainerOrNull = async () => ({ ...inspect, Id: "d".repeat(64) });
    }
    const sentinel = path.join(directory, "keep.txt");
    await writeFile(sentinel, "live runner data");
    service.storage.getCheckpoint = async () => { throw new Error("must check identity before download"); };
    await assert.rejects(service.restore(request), /identity|ownership|live container/);
    assert.equal(await readFile(sentinel, "utf8"), "live runner data");
  });
}

test("concurrent duplicate restores serialize even with queue concurrency two", async (t) => {
  const { service, docker } = await fixture(t);
  service.queue.concurrency = 2;
  const request = await completedRestore(service, false);
  const results = await Promise.all([service.restore(request), service.restore(request)]);
  assert.equal(results.filter((result) => result.duplicate).length, 1);
  assert.equal(docker.calls.filter(([op]) => op === "create").length, 1);
  assert.equal(service.runLocks.tails.size, 0);
});

test("cleanup derives committed images without a persisted reference and retries failure after restart", async (t) => {
  const { service, docker, config } = await fixture(t);
  config.rootfs = { mode: "registry", registryImage: "registry.test/checkpoints" };
  const job = { schemaVersion: 2, rootfsRepository: config.rootfs.registryImage, state: "failed", runFriendlyId: "run_cleanup_image",
    snapshotFriendlyId: "snapshot_1", body: { runnerId: "runner-source" } };
  service.jobs.set("run_cleanup_image:snapshot_1", job);
  await service.saveJob(job);
  const expected = snapshotImageRef(config.rootfs.registryImage, job.runFriendlyId, job.snapshotFriendlyId);
  docker.removeImage = async (ref) => { assert.equal(ref, expected); throw new Error("image in use"); };
  await assert.rejects(service.deleteRun(job.runFriendlyId), /image in use/);
  assert.equal(service.jobs.size, 1);
  assert.equal(JSON.parse(await readFile(path.join(service.jobDir(job.runFriendlyId, job.snapshotFriendlyId), "job.json"))).deletionRequested, true);
  const removed = [];
  docker.removeImage = async (ref) => removed.push(ref);
  const restarted = new CheckpointService({ config, docker, storage: service.storage });
  await restarted.init();
  await waitFor(() => restarted.jobs.size === 0);
  assert.deepEqual(removed, [expected]);
  await restarted.deleteRun(job.runFriendlyId);
  assert.equal(restarted.jobs.size, 0);
});

test("cleanup preserves failed recovery and rejects foreign image references", async (t) => {
  const { service } = await fixture(t);
  const job = { schemaVersion: 2, state: "failed", recoveryError: "restore failed", runFriendlyId: "run_protected",
    snapshotFriendlyId: "snapshot_1", body: { runnerId: "runner-protected" } };
  service.jobs.set("run_protected:snapshot_1", job);
  await service.saveJob(job);
  await assert.rejects(service.deleteRun(job.runFriendlyId), /failed-recovery/);
  delete job.recoveryError;
  job.rootfsImageRef = "registry.test/tasks:base";
  await assert.rejects(service.deleteRun(job.runFriendlyId), /owned identity/);
  assert.equal(service.jobs.size, 1);
});

test("job deduplication lasts until explicit successful cleanup, including restart", async (t) => {
  const { service, docker, config } = await fixture(t);
  const request = { runFriendlyId: "run_dedup", snapshotFriendlyId: "snapshot_1", body: {
    type: "DOCKER", runId: "run_dedup", snapshotId: "snapshot_1", runnerId: "runner-source",
    projectRef: "proj_1", deploymentVersion: "1" } };
  for (let cycle = 0; cycle < 3; cycle++) {
    assert.equal((await service.acceptSuspend(request)).duplicate, undefined);
    await waitFor(() => service.jobs.get("run_dedup:snapshot_1")?.state === "completed");
    assert.equal((await service.acceptSuspend(request)).duplicate, true);
    await waitFor(() => service.queue.running === 0);
    const restarted = new CheckpointService({ config, docker, storage: service.storage });
    await restarted.init();
    assert.equal((await restarted.acceptSuspend(request)).duplicate, true);
    await service.deleteRun(request.runFriendlyId);
    assert.equal(service.jobs.size, 0);
    assert.equal(service.runLocks.tails.size, 0);
  }
});

test("cancelling an in-flight dump waits for recovery before cleanup and eviction", async (t) => {
  const { service, docker } = await fixture(t);
  service.queue.concurrency = 2;
  let release;
  let entered = false;
  const checkpoint = docker.checkpointContainer.bind(docker);
  docker.checkpointContainer = async (...args) => {
    await checkpoint(...args);
    entered = true;
    await new Promise((resolve) => { release = resolve; });
  };
  docker.inspectContainerOrNull = async () => ({ State: { Running: false } });
  await service.acceptSuspend({ runFriendlyId: "run_cancel", snapshotFriendlyId: "snapshot_1", body: {
    type: "DOCKER", runId: "run_cancel", snapshotId: "snapshot_1", runnerId: "runner-source",
    projectRef: "proj_1", deploymentVersion: "1" } });
  await waitFor(() => entered);
  await service.cancelRun("run_cancel");
  assert.equal(service.jobs.size, 1);
  release();
  await waitFor(() => service.jobs.size === 0);
  assert.ok(docker.calls.some(([op]) => op === "start"));
  assert.equal(service.abortControllers.size, 0);
});

test("push failure persists ownership and cleanup uses it after repository config changes", async (t) => {
  const { service, docker, config } = await fixture(t);
  config.rootfs = { mode: "registry", registryImage: "registry.test/checkpoints" };
  const commits = [];
  docker.commitContainer = async (_id, ref) => commits.push(ref);
  docker.pushImage = async () => { throw new Error("push failed"); };
  await service.acceptSuspend({ runFriendlyId: "run_push", snapshotFriendlyId: "snapshot_1", body: {
    type: "DOCKER", runId: "run_push", snapshotId: "snapshot_1", runnerId: "runner-source",
    projectRef: "proj_1", deploymentVersion: "1" } });
  await waitFor(() => service.jobs.get("run_push:snapshot_1")?.state === "failed");
  const job = service.jobs.get("run_push:snapshot_1");
  assert.equal(job.rootfsImageRef, commits[0]);
  assert.equal(JSON.parse(await readFile(path.join(service.jobDir("run_push", "snapshot_1"), "job.json"))).rootfsImageRef, commits[0]);
  config.rootfs.registryImage = "registry.test/new-checkpoints";
  const removed = [];
  docker.removeImage = async (ref) => removed.push(ref);
  await service.deleteRun("run_push");
  assert.deepEqual(removed, commits);
});

test("queued cancellation survives restart without executing a new dump", async (t) => {
  const { service, docker, config } = await fixture(t);
  service.queue.concurrency = 0;
  await service.acceptSuspend({ runFriendlyId: "run_restart_cancel", snapshotFriendlyId: "snapshot_1", body: {
    type: "DOCKER", runId: "run_restart_cancel", snapshotId: "snapshot_1", runnerId: "runner-source",
    projectRef: "proj_1", deploymentVersion: "1" } });
  await service.cancelRun("run_restart_cancel");
  const restarted = new CheckpointService({ config, docker, storage: service.storage,
    fetchImpl: async () => Response.json({ ok: true }) });
  await restarted.init();
  await waitFor(() => restarted.jobs.size === 0 && restarted.queue.running === 0);
  assert.equal(docker.calls.some(([op]) => op === "checkpoint"), false);
});

test("reaper removes restored metadata and durable files after runner exit", async (t) => {
  const { service, docker } = await fixture(t);
  await completedRestore(service);
  const container = await docker.inspectContainer("a".repeat(64));
  docker.inspectContainerOrNull = async () => ({ ...container, State: { Running: false, Status: "exited" } });
  await service.reapExitedRunners();
  assert.equal(service.metadataByIp.size, 0);
  await assert.rejects(access(path.join(service.restoreDir("run_dup", "snapshot_restore"), "restore.json")), { code: "ENOENT" });
});

test("duplicate suspend acknowledges immediately while the original dump is in flight", async (t) => {
  const { service, docker } = await fixture(t);
  let release;
  let entered = false;
  const checkpoint = docker.checkpointContainer.bind(docker);
  docker.checkpointContainer = async (...args) => {
    entered = true;
    await new Promise((resolve) => { release = resolve; });
    await checkpoint(...args);
  };
  const request = { runFriendlyId: "run_inflight", snapshotFriendlyId: "snapshot_1", body: {
    type: "DOCKER", runId: "run_inflight", snapshotId: "snapshot_1", runnerId: "runner-source",
    projectRef: "proj_1", deploymentVersion: "1" } };
  await service.acceptSuspend(request);
  await waitFor(() => entered);
  assert.equal((await service.acceptSuspend(request)).duplicate, true);
  release();
  await waitFor(() => service.jobs.get("run_inflight:snapshot_1")?.state === "completed");
  await service.deleteRun("run_inflight");
});

test("empty cancellation is reclaimed after restart", async (t) => {
  const { service, docker, config } = await fixture(t);
  service.queue.concurrency = 0;
  await service.cancelRun("run_empty");
  const restarted = new CheckpointService({ config, docker, storage: service.storage });
  await restarted.init();
  await waitFor(() => restarted.queue.running === 0 && restarted.queue.pending.length === 0);
  await assert.rejects(access(service.cancelFile("run_empty")), { code: "ENOENT" });
});

test("explicit cleanup removes failed restore downloads without deleting live restore records", async (t) => {
  const { service } = await fixture(t);
  await completedRestore(service);
  const incomplete = service.restoreDir("run_dup", "snapshot_incomplete");
  await mkdir(incomplete, { recursive: true });
  await writeFile(path.join(incomplete, "checkpoint.tar.gz"), "partial download");
  await service.deleteRun("run_dup");
  await assert.rejects(access(incomplete), { code: "ENOENT" });
  await access(path.join(service.restoreDir("run_dup", "snapshot_restore"), "restore.json"));
});
