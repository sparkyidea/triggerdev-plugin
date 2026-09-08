import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { Socket } from "node:net";
import type { Config } from "./config.ts";
import { HttpError, object, registration, traceparent } from "./protocol.ts";
import { reply } from "./response.ts";
import { Registry } from "./registry.ts";
import { Dispatcher } from "./dispatcher.ts";
import { Metrics } from "./metrics.ts";
import { Logger } from "./log.ts";

export function createWarmStartService(config: Config) {
  const metrics = new Metrics();
  const logger = new Logger(config.debugLogging);
  const registry = new Registry(config, metrics, logger);
  const dispatcher = new Dispatcher(config, registry, metrics, logger);
  const sockets = new Set<Socket>();
  const active = new Set<Promise<void>>();
  let closing: Promise<void> | undefined;

  const server = createServer(
    {
      requestTimeout: 300_000,
      headersTimeout: 60_000,
      keepAliveTimeout: 5_000,
      maxHeaderSize: 16_384,
    },
    (request, response) => {
      // Never log request objects; they can contain a deployment JWT.
      request.on("error", () => {});
      response.on("error", () => {});
      const operation = route(request, response).catch((error: unknown) => {
        if (error instanceof HttpError) {
          if (error.status === 400 && request.method === "GET")
            metrics.rejections.invalid++;
          reply(response, error.status, { error: error.code });
        } else {
          logger.info("request_failed");
          reply(response, 500, { error: "internal_error" });
        }
      });
      active.add(operation);
      void operation.finally(() => active.delete(operation));
    },
  );
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });

  async function route(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<void> {
    const path = (request.url ?? "").split("?")[0];
    const aliases: Record<string, string> = {
      "/api/warm-start": "/warm-start",
      "/api/connect": "/connect",
      "/api/health": "/health",
    };
    const routePath = path ? (aliases[path] ?? path) : "";
    const methods: Record<string, string[]> = {
      "/warm-start": ["GET", "POST"],
      "/connect": ["GET"],
      "/health": ["GET"],
      "/ready": ["GET"],
      "/metrics": ["GET"],
    };
    const allowed = methods[routePath];
    if (!allowed) {
      reply(response, 404, { error: "not_found" });
      return;
    }
    if (!allowed.includes(request.method ?? "")) {
      response.setHeader("Allow", allowed.join(", "));
      throw new HttpError(405, "method_not_allowed");
    }
    if (routePath === "/health") {
      reply(response, 200, {
        status: "ok",
        waitingRunners: registry.stats.waiting,
        deployments: registry.stats.deployments,
      });
    } else if (routePath === "/ready") {
      reply(response, registry.draining ? 503 : 200, {
        ready: !registry.draining,
      });
    } else if (routePath === "/metrics") {
      response.writeHead(200, {
        "Content-Type": "text/plain; version=0.0.4; charset=utf-8",
        "Cache-Control": "no-store",
      });
      response.end(metrics.render(registry.stats));
    } else if (routePath === "/connect") {
      reply(response, registry.draining ? 503 : 200, {
        connectionTimeoutMs: config.connectionTimeoutMs,
        keepaliveMs: config.keepaliveMs,
      });
    } else if (request.method === "GET") {
      const identity = registration(request.headers);
      // Remove credential material once normalized; downstream state only retains friendly IDs.
      delete request.headers["x-trigger-deployment-id"];
      for (let i = 0; i < request.rawHeaders.length; i += 2) {
        if (request.rawHeaders[i]?.toLowerCase() === "x-trigger-deployment-id")
          request.rawHeaders[i + 1] = "[redacted]";
      }
      registry.register(identity, response);
    } else {
      if (registry.draining) {
        reply(response, 503, { error: "draining" });
        return;
      }
      const body = await readBody(request, response);
      if (!object(body) || !object(body.dequeuedMessage))
        throw new HttpError(400, "invalid_message");
      const didWarmStart = await dispatcher.dispatch(
        body.dequeuedMessage,
        traceparent(request.headers.traceparent),
      );
      reply(response, 200, { didWarmStart });
    }
  }

  function readBody(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<unknown> {
    return new Promise((resolve, reject) => {
      let size = 0;
      let chunks: Buffer[] = [];
      const declared = Number(request.headers["content-length"]);
      function clean() {
        request.off("data", data);
        request.off("end", end);
        request.off("error", failed);
        request.off("aborted", failed);
      }
      function failed() {
        clean();
        chunks = [];
        reject(new HttpError(400, "incomplete_body"));
      }
      function tooLarge() {
        clean();
        chunks = [];
        metrics.bodyRejections++;
        logger.info("body_rejected", {
          observedBytes: size,
          declaredBytes: Number.isFinite(declared) ? declared : undefined,
          limitBytes: config.maxRequestBodyBytes,
        });
        response.setHeader("Connection", "close");
        request.resume();
        reject(new HttpError(413, "body_too_large"));
      }
      function data(chunk: Buffer) {
        size += chunk.length;
        if (size > config.maxRequestBodyBytes) {
          tooLarge();
          return;
        }
        chunks.push(chunk);
      }
      function end() {
        clean();
        try {
          resolve(
            JSON.parse(
              new TextDecoder("utf-8", { fatal: true }).decode(
                Buffer.concat(chunks),
              ),
            ),
          );
        } catch {
          reject(new HttpError(400, "invalid_json"));
        }
        chunks = [];
      }
      if (declared > config.maxRequestBodyBytes) {
        tooLarge();
        return;
      }
      request.on("data", data);
      request.once("end", end);
      request.once("error", failed);
      request.once("aborted", failed);
    });
  }

  function close(): Promise<void> {
    if (closing) return closing;
    registry.drain();
    closing = (async () => {
      const force = setTimeout(() => {
        for (const socket of sockets) socket.destroy();
      }, config.shutdownGraceMs);
      force.unref();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await Promise.allSettled([...active]);
      clearTimeout(force);
      registry.dispose();
    })();
    return closing;
  }

  return { server, registry, metrics, close };
}
