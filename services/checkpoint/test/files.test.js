import assert from "node:assert/strict";
import { mkdtemp, writeFile, readFile, rm, mkdir, access } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createArchive, runCommand } from "../src/files.js";

for (const level of [1, 6, 9]) {
  test(`gzip level ${level} preserves the archive payload`, async (t) => {
    const root = await mkdtemp(path.join(os.tmpdir(), "checkpoint-gzip-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    await writeFile(path.join(root, "input"), "memory marker\n".repeat(10000));
    const archive = path.join(root, "checkpoint.tar.gz");
    await createArchive(root, archive, ["input"], level);
    await mkdir(path.join(root, "out"));
    await runCommand("tar", ["-C", path.join(root, "out"), "-xzf", archive]);
    assert.deepEqual(await readFile(path.join(root, "out", "input")), await readFile(path.join(root, "input")));
  });
}

test("compression failure removes the incomplete archive", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "checkpoint-gzip-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const archive = path.join(root, "checkpoint.tar.gz");
  await assert.rejects(createArchive(root, archive, ["missing"], 1), /tar exited/);
  await assert.rejects(access(archive), { code: "ENOENT" });
});
