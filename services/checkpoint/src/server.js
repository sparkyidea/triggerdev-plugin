import http from "node:http";
import { normalizeIp } from "./docker.js";
import { log } from "./log.js";

const ROUTES = {
  suspend: /^\/api\/v1\/runs\/([^/]+)\/snapshots\/([^/]+)\/suspend$/,
  restore: /^\/api\/v1\/runs\/([^/]+)\/snapshots\/([^/]+)\/restore$/,
  delete: /^\/api\/v1\/runs\/([^/]+)\/checkpoints\/delete$/,
  cancel: /^\/api\/v1\/runs\/([^/]+)\/checkpoints\/cancel$/,
};

export function createServer(service, config) {
  return http.createServer(async (request, response) => {
    const url = new URL(request.url || "/", "http://checkpoint.invalid");
    const remoteAddress = normalizeIp(request.socket.remoteAddress);
    try {
      if (request.method === "GET" && url.pathname === "/health") {
        return json(response, 200, await service.health());
      }

      if (request.method === "GET" && url.pathname === "/env") {
        return json(response, 200, await service.metadataFor(remoteAddress));
      }

      if (request.method !== "POST") return json(response, 404, { error: "Not found" });
      if (!(await service.isControlRequestAllowed(remoteAddress))) {
        log("error", "Rejected untrusted checkpoint control request", {
          remoteAddress,
          path: url.pathname,
        });
        return json(response, 403, { error: "Forbidden" });
      }

      let match = url.pathname.match(ROUTES.suspend);
      if (match) {
        const body = await readJsonBody(request, config.bodyLimitBytes);
        const result = await service.acceptSuspend({
          runFriendlyId: decodeURIComponent(match[1]),
          snapshotFriendlyId: decodeURIComponent(match[2]),
          body,
        });
        return json(response, 202, { ok: true, ...(result.duplicate ? { duplicate: true } : {}) });
      }

      match = url.pathname.match(ROUTES.restore);
      if (match) {
        const body = await readJsonBody(request, config.bodyLimitBytes);
        const result = await service.restore({
          runFriendlyId: decodeURIComponent(match[1]),
          snapshotFriendlyId: decodeURIComponent(match[2]),
          body,
        });
        return json(response, 200, result);
      }

      match = url.pathname.match(ROUTES.delete);
      if (match) {
        const runFriendlyId = decodeURIComponent(match[1]);
        await readJsonBody(request, config.bodyLimitBytes, true);
        service.queue.add(() => service.deleteRun(runFriendlyId));
        return json(response, 202, { ok: true });
      }

      match = url.pathname.match(ROUTES.cancel);
      if (match) {
        await service.cancelRun(decodeURIComponent(match[1]));
        return json(response, 202, { ok: true });
      }

      return json(response, 404, { error: "Not found" });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log("error", "Checkpoint API request failed", {
        method: request.method,
        path: url.pathname,
        remoteAddress,
        error: message,
      });
      const status = /required|unsupported|match|characters|body|DOCKER/.test(message) ? 400 : 500;
      return json(response, status, { error: message });
    }
  });
}

async function readJsonBody(request, limit, allowEmpty = false) {
  let size = 0;
  const chunks = [];
  for await (const chunk of request) {
    size += chunk.length;
    if (size > limit) throw new Error(`Request body exceeds ${limit} bytes`);
    chunks.push(chunk);
  }
  if (chunks.length === 0 && allowEmpty) return {};
  if (chunks.length === 0) throw new Error("Request body is required");
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new Error("Request body must be valid JSON");
  }
}

function json(response, status, body) {
  response.writeHead(status, {
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
  });
  response.end(JSON.stringify(body));
}
