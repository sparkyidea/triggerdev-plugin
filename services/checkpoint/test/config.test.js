import assert from "node:assert/strict";
import test from "node:test";
import { loadConfig } from "../src/config.js";

const base = {
  TRIGGER_API_URL: "http://webapp:3000/",
  TRIGGER_WORKER_TOKEN: "worker-token",
  TRIGGER_WORKER_INSTANCE_NAME: "worker-1",
  CHECKPOINT_ROOTFS_MODE: "none",
};

test("loads a local development configuration", () => {
  const config = loadConfig(base);
  assert.equal(config.trigger.apiUrl, "http://webapp:3000");
  assert.equal(config.storage.driver, "local");
  assert.equal(config.rootfs.mode, "none");
  assert.equal(config.maxConcurrentJobs, 1);
});

test("requires S3 settings for distributed storage", () => {
  assert.throws(
    () => loadConfig({ ...base, CHECKPOINT_STORAGE_DRIVER: "s3" }),
    /CHECKPOINT_S3_ENDPOINT is required/
  );
});

test("requires a snapshot image repository in registry mode", () => {
  assert.throws(
    () => loadConfig({ ...base, CHECKPOINT_ROOTFS_MODE: "registry" }),
    /CHECKPOINT_REGISTRY_IMAGE is required/
  );
});
