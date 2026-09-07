import { createReadStream, createWriteStream } from "node:fs";
import { copyFile, mkdir, rm, stat } from "node:fs/promises";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import {
  DeleteObjectsCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

export function createStorage(config) {
  return config.driver === "s3" ? new S3Storage(config.s3) : new LocalStorage(config.localArchiveRoot);
}

export class LocalStorage {
  constructor(root) {
    this.root = path.resolve(root);
  }

  async init() {
    await mkdir(this.root, { recursive: true });
  }

  async putCheckpoint({ runFriendlyId, snapshotFriendlyId, archivePath }) {
    const target = path.join(this.root, runFriendlyId, `${snapshotFriendlyId}.tar.gz`);
    await mkdir(path.dirname(target), { recursive: true });
    await copyFile(archivePath, target);
    return new URL(`file://${target}`).toString();
  }

  async getCheckpoint(location, destination) {
    const source = path.resolve(decodeURIComponent(new URL(location).pathname));
    if (!isWithin(this.root, source)) throw new Error("Checkpoint file is outside archive root");
    await copyFile(source, destination);
  }

  async deleteRun(runFriendlyId) {
    const target = path.join(this.root, runFriendlyId);
    if (!isWithin(this.root, target)) throw new Error("Invalid run archive path");
    await rm(target, { recursive: true, force: true });
  }
}

export class S3Storage {
  constructor(config) {
    this.config = config;
    this.client = new S3Client({
      endpoint: config.endpoint,
      region: config.region,
      forcePathStyle: config.forcePathStyle,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
    });
  }

  async init() {
    // Bucket creation is intentionally left to the operator. This prevents a
    // credential with accidental broad permissions from mutating storage.
  }

  key(runFriendlyId, snapshotFriendlyId) {
    return `${this.config.prefix}/${runFriendlyId}/${snapshotFriendlyId}.tar.gz`;
  }

  async putCheckpoint({ runFriendlyId, snapshotFriendlyId, archivePath }) {
    const key = this.key(runFriendlyId, snapshotFriendlyId);
    const file = await stat(archivePath);
    const upload = new Upload({
      client: this.client,
      params: {
        Bucket: this.config.bucket,
        Key: key,
        Body: createReadStream(archivePath),
        ContentLength: file.size,
        ContentType: "application/gzip",
      },
    });
    await upload.done();
    return `s3://${this.config.bucket}/${key}`;
  }

  async getCheckpoint(location, destination) {
    const parsed = parseS3Location(location);
    if (parsed.bucket !== this.config.bucket) {
      throw new Error(`Checkpoint bucket ${parsed.bucket} is not configured bucket ${this.config.bucket}`);
    }
    const result = await this.client.send(
      new GetObjectCommand({ Bucket: parsed.bucket, Key: parsed.key })
    );
    if (!result.Body) throw new Error("Checkpoint object has no body");
    await pipeline(result.Body, createWriteStream(destination, { mode: 0o600 }));
  }

  async deleteRun(runFriendlyId) {
    const prefix = `${this.config.prefix}/${runFriendlyId}/`;
    let continuationToken;
    do {
      const page = await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.config.bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        })
      );
      const objects = (page.Contents || []).flatMap((item) => (item.Key ? [{ Key: item.Key }] : []));
      if (objects.length > 0) {
        await this.client.send(
          new DeleteObjectsCommand({
            Bucket: this.config.bucket,
            Delete: { Objects: objects, Quiet: true },
          })
        );
      }
      continuationToken = page.IsTruncated ? page.NextContinuationToken : undefined;
    } while (continuationToken);
  }
}

export function parseS3Location(location) {
  const url = new URL(location);
  if (url.protocol !== "s3:") throw new Error("Checkpoint location is not an s3:// URL");
  return {
    bucket: url.hostname,
    key: decodeURIComponent(url.pathname.replace(/^\//, "")),
  };
}

function isWithin(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}
