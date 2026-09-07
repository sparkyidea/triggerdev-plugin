import { createHash } from "node:crypto";
import { lookup } from "node:dns/promises";
import { access, mkdir, readdir, rm } from "node:fs/promises";
import path from "node:path";
import {
  buildRestoreContainerSpec,
  containerIpAddresses,
  normalizeIp,
  registryAuthHeader,
} from "./docker.js";
import { readJson, runCommand, writeJsonAtomic } from "./files.js";
import { log } from "./log.js";
import { TaskQueue } from "./queue.js";

const TERMINAL_JOB_STATES = new Set(["completed", "cancelled", "failed"]);

export class CheckpointService {
  constructor({ config, docker, storage, registry, fetchImpl = fetch }) {
    this.config = config;
    this.docker = docker;
    this.storage = storage;
    this.registry = registry;
    this.fetch = fetchImpl;
    this.queue = new TaskQueue(config.maxConcurrentJobs, (error) =>
      log("error", "Background checkpoint job failed", { error: errorMessage(error) })
    );
    this.jobs = new Map();
    this.abortControllers = new Map();
    this.metadataByIp = new Map();
    this.pendingMetadataRegistrations = 0;
    this.allowedControlIps = new Set();
    this.reaperTimer = undefined;
  }

  async init() {
    await Promise.all([
      mkdir(this.jobsRoot(), { recursive: true }),
      mkdir(this.restoresRoot(), { recursive: true }),
      this.storage.init(),
      this.config.allowUntrustedControl ? Promise.resolve() : this.refreshAllowedControlIps(),
    ]);
    await this.recoverRestoreMappings();
    await this.recoverJobs();
    if (this.config.reaper.enabled) {
      this.reaperTimer = setInterval(
        () => this.reapExitedRunners().catch((error) => log("error", "Runner reaper failed", { error: errorMessage(error) })),
        this.config.reaper.intervalMs
      );
      this.reaperTimer.unref();
    }
  }

  async health() {
    await this.docker.ping();
    return {
      ok: true,
      storage: this.config.storage.driver,
      rootfs: this.config.rootfs.mode,
      queued: this.queue.pending.length,
      running: this.queue.running,
    };
  }

  async isControlRequestAllowed(remoteAddress) {
    if (this.config.allowUntrustedControl) return true;
    const address = normalizeIp(remoteAddress);
    if (this.allowedControlIps.has(address)) return true;
    await this.refreshAllowedControlIps();
    return this.allowedControlIps.has(address);
  }

  async refreshAllowedControlIps() {
    try {
      const records = await lookup(this.config.controlAllowedHost, { all: true });
      this.allowedControlIps = new Set(records.map((record) => normalizeIp(record.address)));
    } catch (error) {
      log("error", "Unable to resolve trusted checkpoint caller", {
        host: this.config.controlAllowedHost,
        error: errorMessage(error),
      });
    }
  }

  async metadataFor(remoteAddress) {
    const address = normalizeIp(remoteAddress);
    const existing = this.metadataByIp.get(address);
    if (existing) return existing;

    // startContainer can wake the restored process just before the post-start
    // inspect registers its newly allocated IP. Hold only during that narrow
    // window; ordinary runners still receive {} immediately.
    for (let attempt = 0; this.pendingMetadataRegistrations > 0 && attempt < 100; attempt += 1) {
      await delay(50);
      const metadata = this.metadataByIp.get(address);
      if (metadata) return metadata;
    }
    return {};
  }

  async acceptSuspend({ runFriendlyId, snapshotFriendlyId, body }) {
    validateId("runFriendlyId", runFriendlyId);
    validateId("snapshotFriendlyId", snapshotFriendlyId);
    validateSuspendBody(body, runFriendlyId, snapshotFriendlyId);
    const key = jobKey(runFriendlyId, snapshotFriendlyId);
    const existing = this.jobs.get(key);
    if (existing && existing.state !== "failed" && existing.state !== "cancelled") {
      return { ok: true, duplicate: true };
    }

    const job = {
      schemaVersion: 1,
      state: "accepted",
      acceptedAt: new Date().toISOString(),
      runFriendlyId,
      snapshotFriendlyId,
      checkpointId: checkpointIdFor(runFriendlyId, snapshotFriendlyId),
      body,
    };
    await this.saveJob(job);
    this.jobs.set(key, job);
    this.queue.add(() => this.processSuspend(job));
    return { ok: true };
  }

  async processSuspend(job) {
    const key = jobKey(job.runFriendlyId, job.snapshotFriendlyId);
    const abortController = new AbortController();
    this.abortControllers.set(key, abortController);
    try {
      const workDir = this.jobDir(job.runFriendlyId, job.snapshotFriendlyId);
      const checkpointDir = path.join(workDir, "checkpoint");
      const manifestFile = path.join(workDir, "manifest.json");
      const archiveFile = path.join(workDir, "checkpoint.tar.gz");
      await mkdir(checkpointDir, { recursive: true });

      if (job.state === "accepted") {
        throwIfAborted(abortController.signal);
        const [container, dockerInfo, dockerVersion] = await Promise.all([
          this.docker.inspectContainer(job.body.runnerId),
          this.docker.info(),
          this.docker.version(),
        ]);
        if (container.HostConfig?.AutoRemove) {
          throw new Error(
            "Runner has AutoRemove enabled. Set DOCKER_AUTOREMOVE_EXITED_CONTAINERS=0 before enabling checkpoints"
          );
        }
        const manifest = {
          schemaVersion: 1,
          createdAt: new Date().toISOString(),
          runFriendlyId: job.runFriendlyId,
          snapshotFriendlyId: job.snapshotFriendlyId,
          checkpointId: job.checkpointId,
          sourceRunnerId: job.body.runnerId,
          sourceWorkerInstanceName: this.config.trigger.workerInstanceName,
          source: {
            architecture: dockerVersion.Arch,
            os: dockerVersion.Os,
            kernelVersion: dockerInfo.KernelVersion,
            dockerVersion: dockerVersion.Version,
          },
          container,
          rootfsImageRef: container.Config?.Image,
          reason: job.body.reason,
        };
        await writeJsonAtomic(manifestFile, manifest);
        job.state = "checkpointing";
        await this.saveJob(job);
      }

      if (job.state === "checkpointing") {
        throwIfAborted(abortController.signal);
        const container = await this.docker.inspectContainer(job.body.runnerId);
        const checkpointPath = path.join(checkpointDir, job.checkpointId);
        if (container.State?.Running) {
          await rm(checkpointPath, { recursive: true, force: true });
          await this.docker.checkpointContainer(job.body.runnerId, job.checkpointId, checkpointDir);
        } else if (!(await exists(checkpointPath))) {
          throw new Error("Runner stopped during checkpoint, but no recoverable checkpoint exists");
        }
        job.state = "checkpointed";
        job.checkpointedAt = new Date().toISOString();
        await this.saveJob(job);
      }

      if (job.state === "checkpointed") {
        throwIfAborted(abortController.signal);
        const manifest = await readJson(manifestFile);
        if (this.config.rootfs.mode === "registry") {
          const imageRef = snapshotImageRef(
            this.config.rootfs.registryImage,
            job.runFriendlyId,
            job.snapshotFriendlyId
          );
          await this.docker.commitContainer(job.body.runnerId, imageRef);
          await this.docker.pushImage(imageRef, this.registryAuth());
          manifest.rootfsImageRef = imageRef;
          await writeJsonAtomic(manifestFile, manifest);
          job.rootfsImageRef = imageRef;
        }
        job.state = "rootfs_captured";
        await this.saveJob(job);
      }

      if (job.state === "rootfs_captured") {
        throwIfAborted(abortController.signal);
        await runCommand("tar", [
          "-C",
          workDir,
          "-czf",
          archiveFile,
          "manifest.json",
          path.join("checkpoint", job.checkpointId),
        ]);
        job.state = "archived";
        await this.saveJob(job);
      }

      if (job.state === "archived") {
        throwIfAborted(abortController.signal);
        job.location = await this.storage.putCheckpoint({
          runFriendlyId: job.runFriendlyId,
          snapshotFriendlyId: job.snapshotFriendlyId,
          archivePath: archiveFile,
        });
        job.state = "uploaded";
        await this.saveJob(job);
      }

      if (job.state === "uploaded") {
        throwIfAborted(abortController.signal);
        await retry(() =>
          this.submitSuspendCompletion(job, {
            success: true,
            checkpoint: {
              type: "DOCKER",
              location: job.location,
              imageRef: job.rootfsImageRef || null,
              reason: job.body.reason || null,
            },
          })
        );
        job.state = "completed";
        job.completedAt = new Date().toISOString();
        await this.saveJob(job).catch((error) =>
          log("error", "Checkpoint succeeded but completion state could not be persisted", {
            runFriendlyId: job.runFriendlyId,
            snapshotFriendlyId: job.snapshotFriendlyId,
            error: errorMessage(error),
          })
        );
        log("info", "Checkpoint completed", {
          runFriendlyId: job.runFriendlyId,
          snapshotFriendlyId: job.snapshotFriendlyId,
          location: job.location,
        });
        await this.cleanupCompletedSuspend(job, checkpointDir, archiveFile);
      }
    } catch (error) {
      if (job.state === "cancelled") return;
      const message = errorMessage(error);
      log("error", "Checkpoint failed", {
        runFriendlyId: job.runFriendlyId,
        snapshotFriendlyId: job.snapshotFriendlyId,
        state: job.state,
        error: message,
      });
      await this.recoverSourceRunner(job).catch((recoveryError) => {
        job.recoveryError = errorMessage(recoveryError);
        log("error", "Failed to recover source runner", {
          runFriendlyId: job.runFriendlyId,
          error: job.recoveryError,
        });
      });
      await this.submitSuspendCompletion(job, { success: false, error: message }).catch(() => {});
      job.state = "failed";
      job.error = message;
      job.failedAt = new Date().toISOString();
      await this.saveJob(job);
    } finally {
      this.abortControllers.delete(key);
    }
  }

  restore(request) {
    return this.queue.run(() => this.processRestore(request));
  }

  async processRestore({ runFriendlyId, snapshotFriendlyId, body }) {
    validateId("runFriendlyId", runFriendlyId);
    validateId("snapshotFriendlyId", snapshotFriendlyId);
    validateRestoreBody(body);
    const checkpoint = body.checkpoint;
    const restoreDir = this.restoreDir(runFriendlyId, snapshotFriendlyId);
    const archiveFile = path.join(restoreDir, "checkpoint.tar.gz");
    await rm(restoreDir, { recursive: true, force: true });
    await mkdir(restoreDir, { recursive: true });
    await this.storage.getCheckpoint(checkpoint.location, archiveFile);
    await runCommand("tar", ["-C", restoreDir, "-xzf", archiveFile]);
    const manifest = await readJson(path.join(restoreDir, "manifest.json"));
    validateManifest(manifest, runFriendlyId);
    await this.assertCompatible(manifest);

    const runnerId = restoreRunnerId(runFriendlyId, checkpoint.id);
    const existing = await this.docker.inspectContainerOrNull(runnerId);
    if (existing?.State?.Running) {
      return { ok: true, duplicate: true, runnerId };
    }
    if (existing) await this.docker.removeContainer(runnerId, true);

    const imageRef = manifest.rootfsImageRef || checkpoint.imageRef || body.image;
    if (!imageRef) throw new Error("No root filesystem image is available for restore");
    if (!(await this.docker.inspectImageOrNull(imageRef))) {
      await this.docker.pullImage(imageRef, this.registryAuth());
    }

    const spec = buildRestoreContainerSpec(manifest.container, imageRef, {
      "dev.trigger.checkpoint.run": runFriendlyId,
      "dev.trigger.checkpoint.snapshot": snapshotFriendlyId,
    });
    const created = await this.docker.createContainer(runnerId, spec);
    const metadata = {
      TRIGGER_RUN_ID: runFriendlyId,
      TRIGGER_SNAPSHOT_ID: snapshotFriendlyId,
      TRIGGER_SUPERVISOR_API_PROTOCOL: this.config.trigger.supervisorProtocol,
      TRIGGER_SUPERVISOR_API_DOMAIN: this.config.trigger.supervisorDomain,
      TRIGGER_SUPERVISOR_API_PORT: this.config.trigger.supervisorPort,
      TRIGGER_WORKER_INSTANCE_NAME: this.config.trigger.workerInstanceName,
      TRIGGER_RUNNER_ID: runnerId,
    };
    const restoreRecord = {
      schemaVersion: 1,
      runFriendlyId,
      snapshotFriendlyId,
      runnerId,
      containerId: created.Id,
      metadata,
    };
    await writeJsonAtomic(path.join(restoreDir, "restore.json"), restoreRecord);
    this.pendingMetadataRegistrations += 1;
    try {
      await this.docker.startContainer(created.Id, {
        checkpointId: manifest.checkpointId,
        checkpointDir: path.join(restoreDir, "checkpoint"),
      });
      // Docker assigns endpoint addresses while starting the container. Inspecting
      // immediately after create returns empty addresses and leaves /env unmapped.
      await this.registerStartedMetadata(created.Id, metadata);
    } finally {
      this.pendingMetadataRegistrations -= 1;
    }
    log("info", "Checkpoint restored", { runFriendlyId, snapshotFriendlyId, runnerId });
    return { ok: true, runnerId };
  }

  async cleanupCompletedSuspend(job, checkpointDir, archiveFile) {
    const results = await Promise.allSettled([
      this.docker.removeContainer(job.body.runnerId, true),
      rm(checkpointDir, { recursive: true, force: true }),
      rm(archiveFile, { force: true }),
    ]);
    for (const result of results) {
      if (result.status !== "rejected") continue;
      log("error", "Checkpoint succeeded but local cleanup failed", {
        runFriendlyId: job.runFriendlyId,
        snapshotFriendlyId: job.snapshotFriendlyId,
        error: errorMessage(result.reason),
      });
    }
  }

  async cancelRun(runFriendlyId) {
    validateId("runFriendlyId", runFriendlyId);
    for (const [key, job] of this.jobs) {
      if (job.runFriendlyId !== runFriendlyId || TERMINAL_JOB_STATES.has(job.state)) continue;
      const recoveryJob = { ...job };
      job.state = "cancelled";
      job.cancelledAt = new Date().toISOString();
      this.abortControllers.get(key)?.abort();
      await this.saveJob(job);
      await this.recoverSourceRunner(recoveryJob).catch(() => {});
    }
    this.queue.add(() => this.deleteRun(runFriendlyId));
  }

  async deleteRun(runFriendlyId) {
    validateId("runFriendlyId", runFriendlyId);
    const jobDir = path.join(this.jobsRoot(), runFriendlyId);
    const imageRefs = [];
    for (const file of await findNamedFiles(jobDir, "job.json")) {
      try {
        const job = await readJson(file);
        if (job.rootfsImageRef) imageRefs.push(job.rootfsImageRef);
      } catch {}
    }
    await this.storage.deleteRun(runFriendlyId);
    for (const imageRef of imageRefs) {
      await this.docker.removeImage(imageRef).catch(() => {});
      if (this.registry?.enabled) {
        await this.registry.deleteImage(imageRef).catch((error) =>
          log("error", "Unable to delete checkpoint image from registry", {
            imageRef,
            error: errorMessage(error),
          })
        );
      }
    }
    await Promise.all([
      rm(jobDir, { recursive: true, force: true }),
      rm(path.join(this.restoresRoot(), runFriendlyId), { recursive: true, force: true }),
    ]);
    log("info", "Deleted run checkpoints", { runFriendlyId });
  }

  async recoverSourceRunner(job) {
    if (!["checkpointing", "checkpointed", "rootfs_captured", "archived", "uploaded"].includes(job.state)) return;
    const inspect = await this.docker.inspectContainerOrNull(job.body.runnerId);
    if (!inspect || inspect.State?.Running) return;
    const checkpointDir = path.join(this.jobDir(job.runFriendlyId, job.snapshotFriendlyId), "checkpoint");
    const checkpointPath = path.join(checkpointDir, job.checkpointId);
    if (await exists(checkpointPath)) {
      try {
        await this.docker.startContainer(job.body.runnerId, {
          checkpointId: job.checkpointId,
          checkpointDir,
        });
        return;
      } catch (error) {
        const refreshed = await this.docker.inspectContainerOrNull(job.body.runnerId);
        if (refreshed?.State?.Running) return;
        // Starting normally loses the waiting process and attempts to execute an
        // already-started snapshot. Preserve the source and report the failure.
        throw error;
      }
    }
    throw new Error("No recoverable checkpoint exists; refusing to cold-start the source runner");
  }

  async submitSuspendCompletion(job, body) {
    const response = await this.fetch(
      `${this.config.trigger.apiUrl}/engine/v1/worker-actions/runs/${encodeURIComponent(job.runFriendlyId)}/snapshots/${encodeURIComponent(job.snapshotFriendlyId)}/suspend`,
      {
        method: "POST",
        headers: { ...this.workerHeaders(job.body.runnerId), "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }
    );
    if (!response.ok) {
      // The callback may have committed before its response was lost. Check the
      // platform before treating a retry as failure and reviving the source runner.
      if (body.success && (await this.isCheckpointRecorded(job))) return;
      throw new Error(`Trigger.dev suspend completion failed with ${response.status}: ${await response.text()}`);
    }
    const result = await response.json();
    if (result?.ok !== true) throw new Error("Trigger.dev returned an invalid suspend completion response");
  }

  async isCheckpointRecorded(job) {
    try {
      const response = await this.fetch(
        `${this.config.trigger.apiUrl}/engine/v1/worker-actions/runs/${encodeURIComponent(job.runFriendlyId)}/snapshots/latest`,
        { headers: this.workerHeaders(job.body.runnerId) }
      );
      if (!response.ok) return false;
      const result = await response.json();
      return result?.execution?.checkpoint?.location === job.location;
    } catch {
      return false;
    }
  }

  workerHeaders(runnerId) {
    return {
      Authorization: `Bearer ${this.config.trigger.workerToken}`,
      "x-trigger-worker-instance-name": this.config.trigger.workerInstanceName,
      ...(runnerId ? { "x-trigger-worker-runner-id": runnerId } : {}),
      ...(this.config.trigger.managedWorkerSecret
        ? { "x-trigger-worker-managed-secret": this.config.trigger.managedWorkerSecret }
        : {}),
    };
  }

  async assertCompatible(manifest) {
    const [info, version] = await Promise.all([this.docker.info(), this.docker.version()]);
    if (manifest.source.architecture !== version.Arch || manifest.source.os !== version.Os) {
      throw new Error(
        `Checkpoint platform ${manifest.source.os}/${manifest.source.architecture} does not match ${version.Os}/${version.Arch}`
      );
    }
    if (
      this.config.compatibility.requireKernelMatch &&
      manifest.source.kernelVersion !== info.KernelVersion
    ) {
      throw new Error(
        `Checkpoint kernel ${manifest.source.kernelVersion} does not match ${info.KernelVersion}`
      );
    }
  }

  registerMetadata(containerInspect, metadata) {
    const addresses = containerIpAddresses(containerInspect);
    for (const ip of addresses) this.metadataByIp.set(ip, metadata);
    return addresses.length;
  }

  async registerStartedMetadata(containerId, metadata) {
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const inspect = await this.docker.inspectContainer(containerId);
      if (this.registerMetadata(inspect, metadata) > 0) return;
      await delay(50);
    }
    throw new Error(`Restored runner ${containerId} has no allocated network address`);
  }

  async recoverJobs() {
    for (const file of await findNamedFiles(this.jobsRoot(), "job.json")) {
      try {
        const job = await readJson(file);
        const key = jobKey(job.runFriendlyId, job.snapshotFriendlyId);
        this.jobs.set(key, job);
        if (!TERMINAL_JOB_STATES.has(job.state)) this.queue.add(() => this.processSuspend(job));
      } catch (error) {
        log("error", "Unable to recover checkpoint job", { file, error: errorMessage(error) });
      }
    }
  }

  async recoverRestoreMappings() {
    for (const file of await findNamedFiles(this.restoresRoot(), "restore.json")) {
      try {
        const record = await readJson(file);
        const inspect = await this.docker.inspectContainerOrNull(record.containerId);
        if (inspect) this.registerMetadata(inspect, record.metadata);
      } catch (error) {
        log("error", "Unable to recover restore metadata", { file, error: errorMessage(error) });
      }
    }
  }

  async reapExitedRunners() {
    const containers = await this.docker.listExitedRunnerContainers();
    const protectedRunners = new Set(
      [...this.jobs.values()]
        .filter((job) => !TERMINAL_JOB_STATES.has(job.state) || job.recoveryError)
        .map((job) => job.body.runnerId)
    );
    const cutoff = Date.now() - this.config.reaper.graceSeconds * 1000;
    for (const container of containers) {
      if (containerMatchesRunner(container, protectedRunners)) continue;
      const inspect = await this.docker.inspectContainerOrNull(container.Id);
      const finishedAt = Date.parse(inspect?.State?.FinishedAt || "");
      // Missing/invalid FinishedAt is not safe evidence that the grace period elapsed.
      if (!Number.isFinite(finishedAt) || finishedAt > cutoff) continue;
      await this.docker.removeContainer(container.Id, true).catch((error) =>
        log("error", "Unable to reap exited runner", {
          containerId: container.Id,
          error: errorMessage(error),
        })
      );
    }
  }

  registryAuth() {
    return registryAuthHeader({
      username: this.config.rootfs.username,
      password: this.config.rootfs.password,
      serverAddress: this.config.rootfs.serverAddress,
    });
  }

  jobsRoot() {
    return path.join(this.config.checkpointRoot, "jobs");
  }

  restoresRoot() {
    return path.join(this.config.checkpointRoot, "restores");
  }

  jobDir(runFriendlyId, snapshotFriendlyId) {
    return path.join(this.jobsRoot(), runFriendlyId, snapshotFriendlyId);
  }

  restoreDir(runFriendlyId, snapshotFriendlyId) {
    return path.join(this.restoresRoot(), runFriendlyId, snapshotFriendlyId);
  }

  saveJob(job) {
    return writeJsonAtomic(path.join(this.jobDir(job.runFriendlyId, job.snapshotFriendlyId), "job.json"), job);
  }
}

export function checkpointIdFor(runFriendlyId, snapshotFriendlyId) {
  const digest = createHash("sha256")
    .update(`${runFriendlyId}:${snapshotFriendlyId}`)
    .digest("hex")
    .slice(0, 24);
  return `checkpoint-${digest}`;
}

export function restoreRunnerId(runFriendlyId, checkpointDatabaseId) {
  return `runner-${runFriendlyId.replace(/^run_/, "")}-${checkpointDatabaseId.slice(-8)}`;
}

export function snapshotImageRef(repository, runFriendlyId, snapshotFriendlyId) {
  const tag = createHash("sha256")
    .update(`${runFriendlyId}:${snapshotFriendlyId}`)
    .digest("hex")
    .slice(0, 32);
  return `${repository}:${tag}`;
}

function validateSuspendBody(body, runFriendlyId, snapshotFriendlyId) {
  if (!body || typeof body !== "object") throw new Error("Request body must be an object");
  if (body.type !== "DOCKER") throw new Error("Only DOCKER checkpoints are supported");
  for (const field of ["runId", "snapshotId", "runnerId", "projectRef", "deploymentVersion"]) {
    if (typeof body[field] !== "string" || !body[field]) throw new Error(`${field} is required`);
  }
  if (body.runId !== runFriendlyId || body.snapshotId !== snapshotFriendlyId) {
    throw new Error("Path and body run/snapshot IDs do not match");
  }
  validateId("runnerId", body.runnerId);
}

function validateRestoreBody(body) {
  if (!body || typeof body !== "object") throw new Error("Request body must be an object");
  if (!body.checkpoint || body.checkpoint.type !== "DOCKER") {
    throw new Error("A DOCKER checkpoint is required");
  }
  for (const field of ["id", "location"]) {
    if (typeof body.checkpoint[field] !== "string" || !body.checkpoint[field]) {
      throw new Error(`checkpoint.${field} is required`);
    }
  }
}

function validateManifest(manifest, runFriendlyId) {
  if (manifest?.schemaVersion !== 1) throw new Error("Unsupported checkpoint manifest version");
  if (manifest.runFriendlyId !== runFriendlyId) throw new Error("Checkpoint belongs to another run");
  validateId("checkpointId", manifest.checkpointId);
}

function validateId(name, value) {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]{1,200}$/.test(value)) {
    throw new Error(`${name} contains unsupported characters`);
  }
}

function jobKey(runFriendlyId, snapshotFriendlyId) {
  return `${runFriendlyId}:${snapshotFriendlyId}`;
}

function containerMatchesRunner(container, runnerIds) {
  if (runnerIds.has(container.Id)) return true;
  return (container.Names || []).some((name) => runnerIds.has(name.replace(/^\//, "")));
}

async function findNamedFiles(root, basename) {
  try {
    const found = [];
    const visit = async (directory) => {
      for (const entry of await readdir(directory, { withFileTypes: true })) {
        const target = path.join(directory, entry.name);
        if (entry.isDirectory()) await visit(target);
        else if (entry.isFile() && entry.name === basename) found.push(target);
      }
    };
    await visit(root);
    return found;
  } catch (error) {
    if (error?.code === "ENOENT") return [];
    throw error;
  }
}

async function retry(operation, attempts = 5) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt < attempts) await new Promise((resolve) => setTimeout(resolve, 250 * 2 ** attempt));
    }
  }
  throw lastError;
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function throwIfAborted(signal) {
  if (signal.aborted) throw new Error("Checkpoint job was cancelled");
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

async function exists(target) {
  try {
    await access(target);
    return true;
  } catch {
    return false;
  }
}
