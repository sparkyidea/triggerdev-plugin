import { stat } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import { log } from "./log.js";

// These are service-process counters, not dockerd/CRIU or host peak measurements.
export async function measure(enabled, stage, identity, operation) {
  if (!enabled) return operation();
  const fields = {
    stage,
    runFriendlyId: identity?.runFriendlyId,
    snapshotFriendlyId: identity?.snapshotFriendlyId,
  };
  const started = performance.now();
  const cpu = process.cpuUsage();
  const rssBefore = process.memoryUsage.rss();
  log("info", "Checkpoint stage started", fields);
  let success = false;
  try {
    const result = await operation();
    success = true;
    return result;
  } finally {
    const used = process.cpuUsage(cpu);
    const artifactBytes = success && identity?.artifactPath
      ? await stat(identity.artifactPath).then((file) => file.size, () => undefined) : undefined;
    log("info", "Checkpoint stage finished", {
      ...fields, success, artifactBytes, durationMs: Math.round((performance.now() - started) * 100) / 100,
      serviceCpuUserUs: used.user, serviceCpuSystemUs: used.system,
      serviceRssBefore: rssBefore, serviceRssAfter: process.memoryUsage.rss(),
    });
  }
}
