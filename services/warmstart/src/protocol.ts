import type { IncomingHttpHeaders } from "node:http";

export class HttpError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string) {
    super(code);
    this.status = status;
    this.code = code;
  }
}

export interface Identity {
  controllerId: string;
  deploymentId: string;
  deploymentVersion: string;
  cpu: string;
  memory: string;
}

export function object(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function text(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 1024;
}

// Only normalized identifiers may leave this module. No thrown parser error is propagated.
export function deploymentIdentity(raw: unknown): string {
  if (!text(raw) && !(typeof raw === "string" && raw.length <= 16_384)) {
    throw new HttpError(400, "invalid_deployment_identity");
  }
  const parts = (raw as string).split(".");
  if (parts.length === 3 && parts.every((part) => part.length > 0)) {
    try {
      if (!parts.every((part) => /^[A-Za-z0-9_-]+$/.test(part)))
        throw new Error();
      const payload = Buffer.from(parts[1]!, "base64url");
      if (payload.toString("base64url") !== parts[1]) throw new Error();
      const claims: unknown = JSON.parse(
        new TextDecoder("utf-8", { fatal: true }).decode(payload),
      );
      if (!object(claims) || !bareId(claims.deployment)) throw new Error();
      return claims.deployment;
    } catch {
      throw new HttpError(400, "invalid_deployment_identity");
    }
  }
  if (!bareId(raw)) throw new HttpError(400, "invalid_deployment_identity");
  return raw;
}

function bareId(value: unknown): value is string {
  // Deployed friendly IDs contain no dots. Reject malformed token-like strings, too.
  return text(value) && /^[A-Za-z0-9_-]+$/.test(value);
}

function resource(value: unknown): value is string {
  return (
    text(value) &&
    /^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(value) &&
    Number.isFinite(Number(value)) &&
    Number(value) > 0
  );
}

export function registration(headers: IncomingHttpHeaders): Identity {
  const controllerId = headers["x-trigger-workload-controller-id"];
  const deploymentVersion = headers["x-trigger-deployment-version"];
  const cpu = headers["x-trigger-machine-cpu"];
  const memory = headers["x-trigger-machine-memory"];
  if (
    !bareId(controllerId) ||
    !text(deploymentVersion) ||
    !/^[A-Za-z0-9_.-]+$/.test(deploymentVersion) ||
    !resource(cpu) ||
    !resource(memory)
  ) {
    throw new HttpError(400, "invalid_registration");
  }
  return {
    controllerId,
    deploymentId: deploymentIdentity(headers["x-trigger-deployment-id"]),
    deploymentVersion,
    cpu,
    memory,
  };
}

export function matchKey(identity: Omit<Identity, "controllerId">): string {
  return JSON.stringify([
    identity.deploymentId,
    identity.deploymentVersion,
    identity.cpu,
    identity.memory,
  ]);
}

export function dispatchKey(
  message: Record<string, unknown>,
): string | undefined {
  const deployment = message.deployment;
  const worker = message.backgroundWorker;
  const run = message.run;
  if (
    !object(deployment) ||
    !bareId(deployment.friendlyId) ||
    !object(worker) ||
    !text(worker.version) ||
    !object(run) ||
    !object(run.machine)
  )
    return;
  const cpu = run.machine.cpu;
  const memory = run.machine.memory;
  if (
    typeof cpu !== "number" ||
    typeof memory !== "number" ||
    !Number.isFinite(cpu) ||
    !Number.isFinite(memory) ||
    cpu <= 0 ||
    memory <= 0
  )
    return;
  return matchKey({
    deploymentId: deployment.friendlyId,
    deploymentVersion: worker.version,
    cpu: String(cpu),
    memory: String(memory),
  });
}

export function runId(message: Record<string, unknown>): string | undefined {
  return object(message.run) && bareId(message.run.friendlyId)
    ? message.run.friendlyId
    : undefined;
}

export function traceparent(value: unknown): string | undefined {
  if (
    typeof value !== "string" ||
    !/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/.test(value)
  )
    return;
  if (
    value.slice(3, 35) === "0".repeat(32) ||
    value.slice(36, 52) === "0".repeat(16)
  )
    return;
  return value;
}
