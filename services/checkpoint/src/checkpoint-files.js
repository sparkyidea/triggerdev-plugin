import { constants } from "node:fs";
import { access, cp, lstat, mkdir, mkdtemp, realpath, rename, rm } from "node:fs/promises";
import path from "node:path";

// The host's <DockerRootDir>/containers must be mounted at the same absolute
// path. Never manufacture a container directory: it must already belong to Docker.
export async function checkpointContainerDirectory(dockerRootDir, containerId) {
  if (typeof dockerRootDir !== "string" || !path.isAbsolute(dockerRootDir) || path.resolve(dockerRootDir) === "/") {
    throw new Error("Docker returned an invalid data root");
  }
  if (!/^[a-f0-9]{64}$/.test(containerId || "")) {
    throw new Error("Checkpoint staging requires a full Docker container ID");
  }
  const directory = path.join(dockerRootDir, "containers", containerId);
  await checkedDirectory(directory);
  await access(directory, constants.R_OK | constants.W_OK | constants.X_OK);
  return directory;
}

// A unique name prevents overwriting any Docker checkpoint, including leftovers
// from earlier attempts. Only this invocation's staging directory is removed.
export async function withStagedCheckpoint({ dockerRootDir, containerId, checkpointId, checkpointDir }, start) {
  if (!/^[A-Za-z0-9_-]{1,200}$/.test(checkpointId || "")) {
    throw new Error("Invalid checkpoint ID");
  }
  const containerDirectory = await checkpointContainerDirectory(dockerRootDir, containerId);
  const source = path.join(checkpointDir, checkpointId);
  await checkedDirectory(source);
  const inventory = await lstat(path.join(source, "inventory.img"));
  if (!inventory.isFile()) throw new Error("Checkpoint inventory must be a regular file");

  const checkpoints = path.join(containerDirectory, "checkpoints");
  await mkdir(checkpoints, { mode: 0o700 }).catch((error) => {
    if (error.code !== "EEXIST") throw error;
  });
  await checkedDirectory(checkpoints);
  const staging = await mkdtemp(path.join(checkpoints, ".trigger-stage-"));
  const stagedId = path.basename(staging).replace(/^\./, "");
  const destination = path.join(checkpoints, stagedId);
  let published = false;
  try {
    await cp(source, staging, {
      recursive: true,
      preserveTimestamps: true,
      filter: async (file) => {
        const stat = await lstat(file);
        if (!stat.isFile() && !stat.isDirectory()) {
          throw new Error("Checkpoint staging refuses symlinks and special files");
        }
        return true;
      },
    });
    await rename(staging, destination);
    published = true;
    return await start(stagedId);
  } finally {
    await rm(published ? destination : staging, { recursive: true, force: true });
  }
}

async function checkedDirectory(directory) {
  if (!(await lstat(directory)).isDirectory() || await realpath(directory) !== path.resolve(directory)) {
    throw new Error(`Checkpoint directory must be a real directory without symlinks: ${directory}`);
  }
}
