import path from "node:path";

function required(name, env = process.env) {
  const value = env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function integer(name, fallback, env = process.env) {
  const raw = env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function bool(name, fallback, env = process.env) {
  const raw = env[name];
  if (raw === undefined || raw === "") return fallback;
  if (["1", "true", "yes"].includes(raw.toLowerCase())) return true;
  if (["0", "false", "no"].includes(raw.toLowerCase())) return false;
  throw new Error(`${name} must be true or false`);
}

export function loadConfig(env = process.env) {
  const storageDriver = (env.CHECKPOINT_STORAGE_DRIVER || "local").toLowerCase();
  if (!["local", "s3"].includes(storageDriver)) {
    throw new Error("CHECKPOINT_STORAGE_DRIVER must be local or s3");
  }

  const rootfsMode = (env.CHECKPOINT_ROOTFS_MODE || "registry").toLowerCase();
  if (!["registry", "none"].includes(rootfsMode)) {
    throw new Error("CHECKPOINT_ROOTFS_MODE must be registry or none");
  }

  const checkpointRoot = path.resolve(env.CHECKPOINT_ROOT || "/var/lib/trigger-checkpoints");
  const config = {
    port: integer("PORT", 8080, env),
    bodyLimitBytes: integer("CHECKPOINT_BODY_LIMIT_BYTES", 2 * 1024 * 1024, env),
    maxConcurrentJobs: integer("CHECKPOINT_MAX_CONCURRENT_JOBS", 1, env),
    checkpointRoot,
    dockerUrl: env.CHECKPOINT_DOCKER_URL || "http://docker-proxy:2375",
    dockerApiVersion: env.CHECKPOINT_DOCKER_API_VERSION || "",
    controlAllowedHost: env.CHECKPOINT_CONTROL_ALLOWED_HOST || "supervisor",
    allowUntrustedControl: bool("CHECKPOINT_ALLOW_UNTRUSTED_CONTROL", false, env),
    storage: {
      driver: storageDriver,
      localArchiveRoot: path.resolve(
        env.CHECKPOINT_LOCAL_ARCHIVE_ROOT || path.join(checkpointRoot, "archives")
      ),
      s3:
        storageDriver === "s3"
          ? {
              endpoint: required("CHECKPOINT_S3_ENDPOINT", env),
              region: env.CHECKPOINT_S3_REGION || "us-east-1",
              bucket: required("CHECKPOINT_S3_BUCKET", env),
              accessKeyId: required("CHECKPOINT_S3_ACCESS_KEY_ID", env),
              secretAccessKey: required("CHECKPOINT_S3_SECRET_ACCESS_KEY", env),
              forcePathStyle: bool("CHECKPOINT_S3_FORCE_PATH_STYLE", true, env),
              prefix: (env.CHECKPOINT_S3_PREFIX || "trigger-checkpoints").replace(/^\/+|\/+$/g, ""),
            }
          : undefined,
    },
    rootfs: {
      mode: rootfsMode,
      registryImage:
        rootfsMode === "registry" ? required("CHECKPOINT_REGISTRY_IMAGE", env) : undefined,
      username: env.CHECKPOINT_REGISTRY_USERNAME || "",
      password: env.CHECKPOINT_REGISTRY_PASSWORD || "",
      serverAddress: env.CHECKPOINT_REGISTRY_SERVER || "",
      apiUrl: env.CHECKPOINT_REGISTRY_API_URL || "",
    },
    trigger: {
      apiUrl: required("TRIGGER_API_URL", env).replace(/\/$/, ""),
      workerToken: required("TRIGGER_WORKER_TOKEN", env),
      managedWorkerSecret: env.MANAGED_WORKER_SECRET || "",
      workerInstanceName: required("TRIGGER_WORKER_INSTANCE_NAME", env),
      supervisorProtocol: env.CHECKPOINT_SUPERVISOR_PROTOCOL || "http",
      supervisorDomain: env.CHECKPOINT_SUPERVISOR_DOMAIN || "supervisor",
      supervisorPort: integer("CHECKPOINT_SUPERVISOR_PORT", 8020, env),
    },
    compatibility: {
      requireKernelMatch: bool("CHECKPOINT_REQUIRE_KERNEL_MATCH", false, env),
    },
    reaper: {
      enabled: bool("CHECKPOINT_REAPER_ENABLED", true, env),
      intervalMs: integer("CHECKPOINT_REAPER_INTERVAL_MS", 60_000, env),
      graceSeconds: integer("CHECKPOINT_REAPER_GRACE_SECONDS", 600, env),
    },
  };

  return config;
}
