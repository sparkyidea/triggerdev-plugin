import assert from "node:assert/strict";
import { mkdtemp, rm, open } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { S3Storage } from "../src/storage.js";
import http from "node:http";
import { createHash } from "node:crypto";

async function uploadFixture(t, options = {}, size = 12 * 1024 * 1024, queueSize = 1) {
  const server = await s3Fixture(options);
  const directory = await mkdtemp(path.join(os.tmpdir(), "checkpoint-s3-"));
  const archivePath = path.join(directory, "archive.tar.gz");
  const file = await open(archivePath, "w"); await file.truncate(size); await file.close();
  const storage = new S3Storage({ endpoint: server.endpoint, region: "us-east-1", bucket: "test",
    accessKeyId: "fixture", secretAccessKey: "fixture", forcePathStyle: true, prefix: "test", uploadQueueSize: queueSize });
  t.after(async () => { storage.client.destroy(); await server.close(); await rm(directory, { recursive: true, force: true }); });
  const put = (signal) => storage.putCheckpoint({ runFriendlyId: "run_test", snapshotFriendlyId: "snapshot_test", archivePath, signal });
  return { ...server, storage, put, size };
}

for (const size of [1024, 12 * 1024 * 1024]) {
  for (const queueSize of [1, 4]) {
    test(`S3 streams ${size} bytes with queue size ${queueSize}`, async (t) => {
      const fixture = await uploadFixture(t, { delayMs: 25 }, size, queueSize);
      assert.match(await fixture.put(), /^s3:\/\/test\//);
      assert.equal(fixture.stats.bytes, size);
      assert.ok(fixture.stats.peak <= queueSize);
      if (size > 5 * 1024 * 1024) {
        assert.equal(fixture.stats.parts, 3);
        assert.equal(fixture.stats.complete, 1);
        if (queueSize === 4) assert.ok(fixture.stats.peak > 1);
      } else assert.equal(fixture.stats.uploads, 1);
    });
  }
}

test("S3 retries a transient part failure", async (t) => {
  const fixture = await uploadFixture(t, { failParts: 1 });
  await fixture.put();
  assert.equal(fixture.stats.retries, 1);
  assert.equal(fixture.stats.complete, 1);
  assert.equal(fixture.stats.aborts, 0);
});

for (const options of [{ permanentFailure: true }, { interrupt: true }]) {
  test(`S3 aborts failed multipart uploads (${JSON.stringify(options)})`, async (t) => {
    const fixture = await uploadFixture(t, options);
    await assert.rejects(fixture.put());
    assert.equal(fixture.stats.complete, 0);
    assert.equal(fixture.stats.aborts, 1);
  });
}

test("S3 cancellation aborts the multipart upload", async (t) => {
  const fixture = await uploadFixture(t, { delayMs: 100 });
  const controller = new AbortController();
  const upload = fixture.put(controller.signal);
  while (!fixture.stats.active) await new Promise((resolve) => setTimeout(resolve, 5));
  controller.abort();
  await assert.rejects(upload, /abort/i);
  for (let i = 0; i < 100 && !fixture.stats.aborts; i++) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(fixture.stats.aborts, 1);
  assert.equal(fixture.stats.complete, 0);
});

test("large known-length archives retain automatic multipart part sizing", async (t) => {
  const fixture = await uploadFixture(t, { delayMs: 100 }, 60 * 1024 ** 3);
  const controller = new AbortController();
  const upload = fixture.put(controller.signal);
  while (fixture.stats.bytes <= 5 * 1024 ** 2) await new Promise((resolve) => setTimeout(resolve, 5));
  controller.abort();
  await assert.rejects(upload, /abort/i);
  for (let i = 0; i < 100 && !fixture.stats.aborts; i++) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(fixture.stats.aborts, 1);
  assert.ok(fixture.stats.bytes > 5 * 1024 ** 2);
  assert.ok(fixture.stats.bytes < 10 * 1024 ** 2);
});

test("cancellation during completion waits for the final write before returning", async (t) => {
  const fixture = await uploadFixture(t, { completeDelayMs: 100 }, 12 * 1024 ** 2, 4);
  const controller = new AbortController();
  let settled = false;
  const upload = fixture.put(controller.signal).finally(() => { settled = true; });
  while (!fixture.stats.completing) await new Promise((resolve) => setTimeout(resolve, 5));
  controller.abort();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(settled, false, "cleanup must not race a pending completion write");
  await assert.rejects(upload, /abort/i);
  assert.equal(fixture.stats.complete, 1, "the final write precedes cancellation acknowledgement");
});

test("partial S3 deletion failure is reported for retry", async (t) => {
  const fixture = await uploadFixture(t, {}, 1024);
  fixture.storage.client.send = async (command) => command.constructor.name === "ListObjectsV2Command"
    ? { Contents: [{ Key: "test/run_test/snapshot_test.tar.gz" }] }
    : { Errors: [{ Code: "AccessDenied" }] };
  await assert.rejects(fixture.storage.deleteRun("run_test"), /deletion reported object failures/);
});

// Local streaming S3 protocol fixture used only by this test suite.
async function s3Fixture({ delayMs = 0, completeDelayMs = 0, failParts = 0, permanentFailure = false, interrupt = false } = {}) {
  const stats = { active: 0, peak: 0, bytes: 0, uploads: 0, parts: 0, aborts: 0, retries: 0, complete: 0 };
  const server = http.createServer(async (request, response) => {
    const url = new URL(request.url, "http://localhost");
    if (request.method === "DELETE") {
      stats.aborts++;
      response.writeHead(204); response.end(); return;
    }
    if (request.method === "POST" && url.searchParams.has("uploads")) {
      response.setHeader("Content-Type", "application/xml");
      response.end('<InitiateMultipartUploadResult><Bucket>test</Bucket><Key>archive</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>'); return;
    }
    if (request.method === "POST") {
      for await (const _chunk of request) { /* drain completion XML */ }
      stats.completing = true;
      if (completeDelayMs) await new Promise((resolve) => setTimeout(resolve, completeDelayMs));
      stats.complete++;
      response.setHeader("Content-Type", "application/xml");
      response.end('<CompleteMultipartUploadResult><Location>test</Location><Bucket>test</Bucket><Key>archive</Key><ETag>"complete"</ETag></CompleteMultipartUploadResult>'); return;
    }
    stats.active++; stats.peak = Math.max(stats.peak, stats.active);
    const hash = createHash("md5");
    try {
      for await (const chunk of request) { stats.bytes += chunk.length; hash.update(chunk); }
      if (delayMs) await new Promise((resolve) => setTimeout(resolve, delayMs));
      if (url.searchParams.has("partNumber")) {
        stats.parts++;
        if (interrupt) { stats.retries++; response.destroy(); return; }
        if (permanentFailure || failParts-- > 0) {
          stats.retries++;
          response.writeHead(503, { "Content-Type": "application/xml" });
          response.end('<Error><Code>SlowDown</Code><Message>Fixture failure</Message></Error>'); return;
        }
      } else stats.uploads++;
      response.writeHead(200, { ETag: `"${hash.digest("hex")}"` }); response.end();
    } catch { response.destroy(); }
    finally { stats.active--; }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { stats, endpoint: `http://127.0.0.1:${server.address().port}`,
    close: async () => { server.closeAllConnections(); await new Promise((resolve) => server.close(resolve)); } };
}
