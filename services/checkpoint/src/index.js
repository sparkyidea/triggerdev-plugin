import { CheckpointService } from "./checkpoint-service.js";
import { loadConfig } from "./config.js";
import { DockerClient } from "./docker.js";
import { log } from "./log.js";
import { RegistryClient } from "./registry.js";
import { createServer } from "./server.js";
import { createStorage } from "./storage.js";

async function main() {
  const config = loadConfig();
  const docker = new DockerClient({
    baseUrl: config.dockerUrl,
    apiVersion: config.dockerApiVersion,
  });
  const storage = createStorage(config.storage);
  const registry = new RegistryClient({
    apiUrl: config.rootfs.apiUrl,
    username: config.rootfs.username,
    password: config.rootfs.password,
  });
  const service = new CheckpointService({ config, docker, storage, registry });
  await service.init();
  const server = createServer(service, config);
  server.listen(config.port, "0.0.0.0", () => {
    log("info", "Trigger.dev checkpoint service listening", {
      port: config.port,
      storage: config.storage.driver,
      rootfs: config.rootfs.mode,
      trustedCaller: config.controlAllowedHost,
    });
  });

  const shutdown = (signal) => {
    log("info", "Shutting down checkpoint service", { signal });
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 10_000).unref();
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

main().catch((error) => {
  log("error", "Checkpoint service failed to start", {
    error: error instanceof Error ? error.stack || error.message : String(error),
  });
  process.exit(1);
});
