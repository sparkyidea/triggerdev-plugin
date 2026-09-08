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

test("validates resource tuning without truncating malformed values", () => {
  const s3 = { ...base, CHECKPOINT_STORAGE_DRIVER: "s3", CHECKPOINT_S3_ENDPOINT: "http://localhost",
    CHECKPOINT_S3_BUCKET: "test", CHECKPOINT_S3_ACCESS_KEY_ID: "test", CHECKPOINT_S3_SECRET_ACCESS_KEY: "test" };
  assert.equal(loadConfig(s3).storage.s3.uploadQueueSize, 4);
  assert.equal(loadConfig(base).gzipLevel, 6);
  assert.equal(loadConfig({ ...s3, CHECKPOINT_S3_UPLOAD_QUEUE_SIZE: "1" }).storage.s3.uploadQueueSize, 1);
  for (const value of ["0", "-1", "1.5", "4parts", "17", "Infinity"]) {
    assert.throws(() => loadConfig({ ...s3, CHECKPOINT_S3_UPLOAD_QUEUE_SIZE: value }), /CHECKPOINT_S3_UPLOAD_QUEUE_SIZE/);
  }
  for (const value of ["0", "10", "1fast", "2.5"]) {
    assert.throws(() => loadConfig({ ...base, CHECKPOINT_GZIP_LEVEL: value }), /CHECKPOINT_GZIP_LEVEL/);
  }
});
