import { readConfig } from "./config.ts";
import { createWarmStartService } from "./server.ts";
import { Logger } from "./log.ts";

const logger = new Logger(false);
try {
  const config = readConfig();
  const service = createWarmStartService(config);
  service.server.on("error", () => {
    logger.info("server_failed");
    process.exitCode = 1;
    void service.close();
  });
  service.server.listen(config.port, config.host, () => {
    const address = service.server.address();
    logger.info("listening", {
      port: typeof address === "object" && address ? address.port : config.port,
    });
  });
  const shutdown = () => {
    logger.info("draining");
    void service.close().then(() => logger.info("stopped"));
  };
  process.once("SIGTERM", shutdown);
  process.once("SIGINT", shutdown);
} catch {
  logger.info("startup_failed", { reason: "invalid_configuration" });
  process.exitCode = 1;
}
