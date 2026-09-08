import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { mkdir, readFile, rename, writeFile, rm } from "node:fs/promises";
import path from "node:path";

export async function writeJsonAtomic(file, value) {
  await mkdir(path.dirname(file), { recursive: true });
  const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  await rename(temporary, file);
}

export async function readJson(file) {
  return JSON.parse(await readFile(file, "utf8"));
}

export async function runCommand(command, args) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.once("error", reject);
    child.once("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${command} exited ${code}: ${stderr || stdout}`));
    });
  });
}

// Keep the .tar.gz contract while selecting gzip level without shell evaluation.
export async function createArchive(directory, destination, entries, level = 6) {
  if (!Number.isInteger(level) || level < 1 || level > 9) throw new Error("gzip level must be between 1 and 9");
  const tar = spawn("tar", ["-C", directory, "-cf", "-", ...entries], { stdio: ["ignore", "pipe", "pipe"] });
  const gzip = spawn("gzip", [`-${level}`, "-c"], { stdio: ["pipe", "pipe", "pipe"] });
  const completion = (child, command) => new Promise((resolve, reject) => {
    let error = "";
    child.stderr.on("data", (chunk) => { error = (error + chunk).slice(-8192); });
    child.once("error", reject);
    child.once("close", (code) => code === 0 ? resolve() : reject(new Error(`${command} exited ${code}: ${error}`)));
  });
  const operations = [completion(tar, "tar"), completion(gzip, "gzip"),
    pipeline(tar.stdout, gzip.stdin), pipeline(gzip.stdout, createWriteStream(destination, { mode: 0o600 }))];
  try { await Promise.all(operations); }
  catch (error) {
    tar.kill(); gzip.kill();
    await Promise.allSettled(operations);
    await rm(destination, { force: true });
    throw error;
  }
}
