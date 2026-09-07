import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { checkpointContainerDirectory, withStagedCheckpoint } from "../src/checkpoint-files.js";
import { DockerClient } from "../src/docker.js";

async function fixture(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), "trigger-staging-test-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const options = {
    dockerRootDir: path.join(root, "docker"), containerId: "a".repeat(64),
    checkpointDir: path.join(root, "source"), checkpointId: "checkpoint-original",
  };
  const container = path.join(options.dockerRootDir, "containers", options.containerId);
  const source = path.join(options.checkpointDir, options.checkpointId);
  await mkdir(container, { recursive: true });
  await mkdir(source, { recursive: true });
  await writeFile(path.join(source, "inventory.img"), "saved-memory");
  return { options, container, source };
}

test("stages atomically into default directory, preserves other checkpoints and source", async (t) => {
  const { options, container, source } = await fixture(t);
  const checkpoints = path.join(container, "checkpoints");
  await mkdir(path.join(checkpoints, "unrelated"), { recursive: true });
  const result = await withStagedCheckpoint(options, async (id) => {
    assert.match(id, /^trigger-stage-/);
    assert.equal(await readFile(path.join(checkpoints, id, "inventory.img"), "utf8"), "saved-memory");
    return "started";
  });
  assert.equal(result, "started");
  assert.deepEqual(await readdir(checkpoints), ["unrelated"]);
  assert.equal(await readFile(path.join(source, "inventory.img"), "utf8"), "saved-memory");
});

test("failed starts clean up only staging and retain the source", async (t) => {
  const { options, container, source } = await fixture(t);
  await assert.rejects(withStagedCheckpoint(options, async () => { throw new Error("restore failed"); }), /restore failed/);
  assert.deepEqual(await readdir(path.join(container, "checkpoints")), []);
  assert.equal(await readFile(path.join(source, "inventory.img"), "utf8"), "saved-memory");
});

test("rejects invalid IDs, missing host mounts, and symlinked checkpoint directories", async (t) => {
  const { options, container } = await fixture(t);
  await assert.rejects(checkpointContainerDirectory(options.dockerRootDir, "../escape"), /full Docker container ID/);
  await assert.rejects(checkpointContainerDirectory(options.dockerRootDir, "b".repeat(64)), /ENOENT/);
  await assert.rejects(withStagedCheckpoint({ ...options, checkpointId: "../escape" }, () => {}), /Invalid checkpoint ID/);
  await symlink(options.checkpointDir, path.join(container, "checkpoints"));
  await assert.rejects(withStagedCheckpoint(options, () => {}), /real directory/);
});

test("rejects checkpoint symlinks without starting a container", async (t) => {
  const { options, source } = await fixture(t);
  await symlink("/etc/passwd", path.join(source, "escape"));
  await assert.rejects(withStagedCheckpoint(options, () => assert.fail("must not start")), /refuses symlinks/);
});

test("Docker client stages for the inspected full ID and uses no custom-dir query", async (t) => {
  const { options, container } = await fixture(t);
  const starts = [];
  const client = new DockerClient({ baseUrl: "http://docker.test", fetchImpl: async (url, init) => {
    const parsed = new URL(url);
    if (parsed.pathname === "/info") return Response.json({ DockerRootDir: options.dockerRootDir });
    if (parsed.pathname.endsWith("/json")) return Response.json({ Id: options.containerId });
    assert.equal(init.method, "POST");
    assert.equal(parsed.pathname, `/containers/${options.containerId}/start`);
    assert.equal(parsed.searchParams.has("checkpoint-dir"), false);
    const id = parsed.searchParams.get("checkpoint");
    assert.equal(await readFile(path.join(container, "checkpoints", id, "inventory.img"), "utf8"), "saved-memory");
    starts.push(id);
    return new Response(null, { status: 204 });
  } });
  await client.startContainer("runner-name", options);
  assert.equal(starts.length, 1);
});

test("missing staging mount prevents checkpoint creation before freezing a runner", async (t) => {
  const { options } = await fixture(t);
  const client = new DockerClient({ baseUrl: "http://docker.test", fetchImpl: async (url, init) => {
    assert.notEqual(init.method, "POST");
    return Response.json(url.endsWith("/info")
      ? { DockerRootDir: options.dockerRootDir }
      : { Id: "b".repeat(64) });
  } });
  await assert.rejects(client.checkpointContainer("runner", options.checkpointId, options.checkpointDir), /ENOENT/);
});
