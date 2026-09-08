import type { Config } from "./config.ts";
import { Registry } from "./registry.ts";
import type { Waiter } from "./registry.ts";
import { Metrics } from "./metrics.ts";
import type { DispatchOutcome } from "./metrics.ts";
import { Logger } from "./log.ts";
import { dispatchKey, runId } from "./protocol.ts";

export class Dispatcher {
  private readonly config: Config;
  private readonly registry: Registry;
  private readonly metrics: Metrics;
  private readonly logger: Logger;

  constructor(
    config: Config,
    registry: Registry,
    metrics: Metrics,
    logger: Logger,
  ) {
    this.config = config;
    this.registry = registry;
    this.metrics = metrics;
    this.logger = logger;
  }

  async dispatch(
    message: Record<string, unknown>,
    trace?: string,
  ): Promise<boolean> {
    const start = performance.now();
    const key = dispatchKey(message);
    let outcome: DispatchOutcome = "miss";
    let waiter: Waiter | undefined;
    if (key) {
      const json = JSON.stringify(message);
      while ((waiter = this.registry.claim(key))) {
        const result = await this.write(waiter, json);
        this.registry.complete(waiter, result === "matched");
        outcome = result;
        // Only a failure known to precede any write may try another runner.
        if (result !== "write_failed") break;
      }
    }
    this.metrics.observeDispatch(outcome, (performance.now() - start) / 1000);
    this.logger.info("dispatch", {
      outcome,
      runId: runId(message),
      controllerId: waiter?.identity.controllerId,
      deploymentId: waiter?.identity.deploymentId,
      traceparent: trace,
    });
    // After an ambiguous write, don't request an immediate cold start that may race delivery.
    // The upgraded supervisor verifier / platform recovery owns this uncertainty.
    return outcome === "matched" || outcome === "ambiguous";
  }

  private write(waiter: Waiter, body: string): Promise<DispatchOutcome> {
    const response = waiter.response;
    if (response.destroyed || response.writableEnded)
      return Promise.resolve("write_failed");
    return new Promise((resolve) => {
      let settled = false;
      const done = (outcome: DispatchOutcome) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        response.off("finish", finished);
        response.off("close", closed);
        response.off("error", closed);
        resolve(outcome);
      };
      const finished = () => done("matched");
      const closed = () => done("ambiguous");
      const timer = setTimeout(() => {
        done("ambiguous");
        response.destroy();
      }, this.config.dispatchWriteTimeoutMs);
      timer.unref();
      response.once("finish", finished);
      response.once("close", closed);
      response.once("error", closed);
      try {
        response.writeHead(200, {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
          "Cache-Control": "no-store",
        });
        response.end(body);
      } catch {
        done("ambiguous");
        response.destroy();
      }
    });
  }
}
