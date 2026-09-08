export interface Config {
  host: string;
  port: number;
  connectionTimeoutMs: number;
  keepaliveMs: number;
  maxWaitingRunners: number;
  maxIdleSessions: number;
  maxRequestBodyBytes: number;
  pollMatchMarginMs: number;
  sessionExpiryGraceMs: number;
  dispatchWriteTimeoutMs: number;
  shutdownGraceMs: number;
  debugLogging: boolean;
}

export function readConfig(env: NodeJS.ProcessEnv = process.env): Config {
  function integer(
    name: string,
    fallback: number,
    min = 1,
    max = 2_147_483_647,
  ) {
    const raw = env[name];
    const value = raw === undefined ? fallback : Number(raw);
    if (
      raw === "" ||
      !Number.isSafeInteger(value) ||
      value < min ||
      value > max
    ) {
      throw new Error(`Invalid configuration: ${name}`);
    }
    return value;
  }
  const config = {
    host: env.HOST ?? "0.0.0.0",
    port: integer("PORT", 8080, 0, 65535),
    connectionTimeoutMs: integer("CONNECTION_TIMEOUT_MS", 30_000, 0),
    keepaliveMs: integer("KEEPALIVE_MS", 300_000, 0),
    maxWaitingRunners: integer("MAX_WAITING_RUNNERS", 1),
    maxIdleSessions: integer("MAX_IDLE_SESSIONS", 1024),
    maxRequestBodyBytes: integer("MAX_REQUEST_BODY_BYTES", 4 * 1024 * 1024),
    pollMatchMarginMs: integer("POLL_MATCH_MARGIN_MS", 0, 0),
    sessionExpiryGraceMs: integer("SESSION_EXPIRY_GRACE_MS", 60_000),
    dispatchWriteTimeoutMs: integer("DISPATCH_WRITE_TIMEOUT_MS", 5_000),
    shutdownGraceMs: integer("SHUTDOWN_GRACE_MS", 10_000),
    debugLogging: env.DEBUG_LOGGING === "true" || env.DEBUG_LOGGING === "1",
  };
  if (
    !config.host ||
    config.maxIdleSessions < config.maxWaitingRunners ||
    (config.connectionTimeoutMs > 0 &&
      config.pollMatchMarginMs >= config.connectionTimeoutMs)
  ) {
    throw new Error("Invalid configuration: host, session cap, or poll margin");
  }
  return config;
}
