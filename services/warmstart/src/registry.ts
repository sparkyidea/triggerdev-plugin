import type { ServerResponse } from "node:http";
import type { Config } from "./config.ts";
import type { Identity } from "./protocol.ts";
import { matchKey } from "./protocol.ts";
import { Metrics } from "./metrics.ts";
import type { Rejection } from "./metrics.ts";
import { Logger } from "./log.ts";
import { reply } from "./response.ts";

interface Session {
  identity: Identity;
  key: string;
  deadline: number;
  expired: boolean;
  timer: NodeJS.Timeout;
  waiter?: Waiter;
}

export interface Waiter {
  identity: Identity;
  response: ServerResponse;
  connectedAt: number;
  state: "waiting" | "claimed" | "closed";
  session: Session;
  onClose: () => void;
}

export class Registry {
  private readonly sessions = new Map<string, Session>();
  private readonly buckets = new Map<string, Set<Waiter>>();
  private occupied = 0;
  private claimed = 0;
  draining = false;

  private readonly config: Config;
  private readonly metrics: Metrics;
  private readonly logger: Logger;

  constructor(config: Config, metrics: Metrics, logger: Logger) {
    this.config = config;
    this.metrics = metrics;
    this.logger = logger;
  }

  get stats() {
    return {
      waiting: this.occupied - this.claimed,
      claimed: this.claimed,
      sessions: this.sessions.size,
      deployments: this.buckets.size,
      ready: !this.draining,
    };
  }

  register(identity: Identity, response: ServerResponse): void {
    if (this.draining) return this.reject(response, "draining", 503);
    if (!this.config.connectionTimeoutMs || !this.config.keepaliveMs) {
      return this.reject(response, "disabled", 503);
    }
    const key = matchKey(identity);
    let session = this.sessions.get(identity.controllerId);
    if (session && session.key !== key)
      return this.reject(response, "identity_changed", 409);
    if (session && (session.expired || performance.now() >= session.deadline)) {
      if (!session.expired) this.expire(session);
      reply(response, 408, { error: "idle_expired" });
      return;
    }
    if (session?.waiter) {
      if (session.waiter.state === "claimed") {
        return this.reject(response, "duplicate", 409);
      }
      // A client's retry can arrive before the old socket's close event. The new
      // connection replaces that generation without resetting its idle deadline.
      const previous = session.waiter;
      this.release(previous);
      reply(previous.response, 409, { error: "poll_replaced" });
    }
    if (this.occupied >= this.config.maxWaitingRunners)
      return this.reject(response, "waiting_cap", 503);
    if (!session && this.sessions.size >= this.config.maxIdleSessions) {
      return this.reject(response, "session_cap", 503);
    }
    if (!session) {
      session = {
        identity,
        key,
        deadline: performance.now() + this.config.keepaliveMs,
        expired: false,
        timer: undefined as unknown as NodeJS.Timeout,
      };
      const created = session;
      created.timer = setTimeout(
        () => this.expire(created),
        this.config.keepaliveMs,
      );
      created.timer.unref();
      this.sessions.set(identity.controllerId, created);
    }
    const waiter: Waiter = {
      identity,
      response,
      connectedAt: performance.now(),
      state: "waiting",
      session,
      onClose: () => {
        if (waiter.state !== "waiting") return;
        this.metrics.disconnects++;
        this.release(waiter);
        this.logger.debug("poll_disconnected", {
          controllerId: identity.controllerId,
          deploymentId: identity.deploymentId,
        });
      },
    };
    session.waiter = waiter;
    const bucket = this.buckets.get(key) ?? new Set<Waiter>();
    bucket.add(waiter);
    this.buckets.set(key, bucket);
    this.occupied++;
    response.once("close", waiter.onClose);
    this.logger.debug("runner_waiting", {
      controllerId: identity.controllerId,
      deploymentId: identity.deploymentId,
    });
  }

  claim(key: string): Waiter | undefined {
    if (this.draining) return;
    const bucket = this.buckets.get(key);
    if (!bucket) return;
    for (const waiter of bucket) {
      if (waiter.response.destroyed || waiter.response.writableEnded) {
        this.release(waiter);
        continue;
      }
      const now = performance.now();
      if (waiter.session.deadline <= now) {
        this.expire(waiter.session);
        continue;
      }
      const remaining =
        this.config.connectionTimeoutMs - (now - waiter.connectedAt);
      if (
        this.config.pollMatchMarginMs > 0 &&
        remaining <= this.config.pollMatchMarginMs
      )
        continue;
      if (remaining <= 2000) this.metrics.nearDeadlineClaims++;
      this.removeFromBucket(waiter);
      waiter.state = "claimed";
      this.claimed++;
      return waiter;
    }
  }

  complete(waiter: Waiter, delivered: boolean): void {
    this.release(waiter);
    if (delivered) this.removeSession(waiter.session);
  }

  drain(): void {
    this.draining = true;
    for (const session of this.sessions.values()) {
      if (session.waiter?.state === "waiting") {
        const waiter = session.waiter;
        this.release(waiter);
        reply(waiter.response, 503, { error: "draining" });
      }
    }
  }

  dispose(): void {
    for (const session of this.sessions.values()) clearTimeout(session.timer);
    this.sessions.clear();
    this.buckets.clear();
  }

  private expire(session: Session): void {
    if (session.expired) return;
    clearTimeout(session.timer);
    session.expired = true;
    this.metrics.expirations++;
    if (session.waiter?.state === "waiting") {
      const waiter = session.waiter;
      this.release(waiter);
      reply(waiter.response, 408, { error: "idle_expired" });
    }
    // Keep a bounded terminal tombstone: an abort racing expiry must not get a fresh budget.
    session.timer = setTimeout(
      () => this.removeSession(session),
      this.config.sessionExpiryGraceMs,
    );
    session.timer.unref();
  }

  private removeSession(session: Session): void {
    clearTimeout(session.timer);
    if (session.waiter?.state === "claimed") {
      session.timer = setTimeout(
        () => this.removeSession(session),
        this.config.dispatchWriteTimeoutMs,
      );
      session.timer.unref();
      return;
    }
    if (this.sessions.get(session.identity.controllerId) === session) {
      this.sessions.delete(session.identity.controllerId);
    }
  }

  private removeFromBucket(waiter: Waiter): void {
    const bucket = this.buckets.get(waiter.session.key);
    bucket?.delete(waiter);
    if (bucket?.size === 0) this.buckets.delete(waiter.session.key);
  }

  private release(waiter: Waiter): void {
    if (waiter.state === "closed") return;
    if (waiter.state === "claimed") this.claimed--;
    this.occupied--;
    this.removeFromBucket(waiter);
    waiter.state = "closed";
    waiter.response.off("close", waiter.onClose);
    if (waiter.session.waiter === waiter) waiter.session.waiter = undefined;
  }

  private reject(
    response: ServerResponse,
    reason: Rejection,
    status: number,
  ): void {
    this.metrics.rejections[reason]++;
    reply(response, status, { error: reason });
  }
}
