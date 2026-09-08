# Warm-start service

A service-only Trigger.dev warm-start matcher using TypeScript and native Node.js HTTP.
It reuses idle runner containers with the same deployment, version, CPU, and memory.
There is no dashboard, frontend, database, Docker socket access, or runtime npm dependency.

## Run locally

Use Node 22.23.1 (also recorded in `.node-version`).

```sh
cd services/warmstart
npm ci
npm run check
npm start
```

`npm run dev` watches the TypeScript sources using Node's native type stripping.
`npm run check` typechecks, runs Vitest against real HTTP connections, and compiles production JavaScript.

## Docker

From the repository root:

```sh
docker build -t trigger-warmstart:local services/warmstart
docker run --rm -p 127.0.0.1:8080:8080 -e KEEPALIVE_MS=90000 trigger-warmstart:local
```

The image runs as the `node` user on a pinned Node 22.23.1 Debian base. Its base digest
is the multi-platform index, supporting amd64 and arm64. The production image contains compiled
JavaScript and diagnostic scripts, with no `node_modules`.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | Listen address. |
| `PORT` | `8080` | Listen port; 0 permits an ephemeral port for tests. |
| `CONNECTION_TIMEOUT_MS` | `30000` | Advertised client per-poll timeout. |
| `KEEPALIVE_MS` | `300000` | Absolute idle budget per controller session; 90000 selects 90 seconds. |
| `MAX_WAITING_RUNNERS` | `1` | Per-process admitted warm connections, including claimed writes. |
| `MAX_IDLE_SESSIONS` | `1024` | Total active, disconnected, and recently expired session records. |
| `MAX_REQUEST_BODY_BYTES` | `4194304` | Maximum POST body (4 MiB), including completed-waitpoint outputs. |
| `POLL_MATCH_MARGIN_MS` | `0` | Stop claiming a poll near its estimated client abort; measure before enabling. |
| `SESSION_EXPIRY_GRACE_MS` | `60000` | Retain an expired identity briefly so an expiry/retry race returns terminal 408. |
| `DISPATCH_WRITE_TIMEOUT_MS` | `5000` | Bound a stalled write to a runner. |
| `SHUTDOWN_GRACE_MS` | `10000` | Finish active work before forcing remaining sockets closed. |
| `DEBUG_LOGGING` | `false` | `true` or `1` enables registration/disconnect logs; tokens remain redacted. |

Zero connection timeout or keepalive advertises disabled warm starts and rejects registration.
Durations and limits must be bounded integers; the session cap must be at least the waiting cap.
An enabled poll margin must be smaller than the advertised connection timeout.

HTTP request/header/transport keep-alive timeouts are explicitly 300000/60000/5000 ms.
These do not implement the idle budget; a separate monotonic session deadline does.

## HTTP contract

| Method/path | Response |
| --- | --- |
| `GET /connect` | `{connectionTimeoutMs, keepaliveMs}` |
| `GET /warm-start` | Held response; full dequeued message as JSON when matched. |
| `POST /warm-start` | Accepts `{dequeuedMessage}`; returns `{didWarmStart}`. |
| `GET /health` | `{status:"ok", waitingRunners, deployments}` |
| `GET /ready` | 200 when ready, 503 while draining. |
| `GET /metrics` | Prometheus text exposition version 0.0.4. |

`/api/connect`, `/api/warm-start`, and `/api/health` remain aliases. Other routes,
including `/` and `/api/dashboard`, return 404.

Registration requires `x-trigger-workload-controller-id`, `x-trigger-deployment-id`,
`x-trigger-deployment-version`, `x-trigger-machine-cpu`, and `x-trigger-machine-memory`.
The worker-instance-name header is accepted but not used for matching.

Deployment identity accepts a bare friendly ID or the JWT-shaped value sent by token-enabled
4.5.12 runners. The JWT payload's `deployment` claim supplies the matching identity.
Decoding is compatibility normalization, **not authentication**. Keep this service on the private
supervisor network. It does not need `WORKLOAD_TOKEN_SECRET`. Raw identity headers are never logged.

POSTs with an object message missing a deployment ID or usable matching dimensions return
**200 with `didWarmStart:false`**. Malformed JSON/invalid envelopes return 400, oversize bodies
413, and unsupported methods 405. Invalid GET registration returns 400; admission limits or
disabled/draining service return 503.

## Lifecycle and delivery

Compatible connections are FIFO. Claiming a connection happens before asynchronous writes.
The same controller reconnects with its original idle deadline. If its new poll arrives before
the previous socket-close event, it replaces the previous connection generation; the old
response receives 409 if still writable. A duplicate during an already claimed write returns
409. Changing a controller's matching identity within a session also returns 409.

Successful dispatch ends the idle session. A known pre-delivery write failure retains the
original deadline. Expiry returns terminal 408. Expired records stay for the bounded grace
period to catch late retries; they then disappear. Very late reappearance after that retention
window, or a service restart, cannot recover the old deadline from memory.

Response headers are withheld until a message or terminal status exists. There is no periodic
server “poll again” response: the runner owns routine poll cancellation/retry. Existing admitted
polls are not disconnected to shed load. SIGTERM/SIGINT drain the service, return 503 to waiting
polls, and bound completion of outstanding writes and request bodies.

`didWarmStart:true` means transport dispatch, **not confirmed task execution**. A write that
has started but fails ambiguously also returns true, with outcome `ambiguous`, to avoid immediately
cold-starting a potentially delivered run. No second runner is chosen after such an ambiguous write.
The platform's existing recovery handles this on legacy supervisors. On an upgraded supervisor,
explicitly enable and test `TRIGGER_WARM_START_VERIFY_ENABLED`. This service cannot provide
exactly-once execution or add verification to supervisor v4.4.6.

## Deployment

Run one process per supervisor/host. Open polls and matching state must meet in the same process;
arbitrary load balancing and Node cluster workers do not share the registry.

A private-network Compose service can use:

```yaml
services:
  warmstart:
    image: ghcr.io/sparkyidea/trigger-warm-start-service:sha-<commit>
    restart: unless-stopped
    environment:
      KEEPALIVE_MS: "90000"
      MAX_WAITING_RUNNERS: "1"
    networks: [trigger-supervisor]
    stop_grace_period: 15s
```

Add it to the existing network and the supervisor's health dependencies. Set
`TRIGGER_WARM_START_URL=http://warmstart:8080` on the supervisor, which passes it to runners.
Newer supervisors also support `TRIGGER_WARM_START_DISPATCH_URL`; when set, it must reach the
same matcher process as the runner URL. Leave it unset on the reviewed v4.4.6 deployment.

Idle containers still reserve their machine allocations. The one-runner A1 cap limits warm
occupancy; it does not fix the supervisor's full-host dequeue gate. Warm-only dequeue, safe
reclamation, runner friendly-ID changes, and verifier rollout are separate platform work.

The reviewed worker Compose files have warm starts disabled. Canary first. Rollback means
removing both warm-start URL settings, draining this service, and ensuring new runners inherit
the restored configuration. For later upgrades, pin supervisor/runner/service versions together.
Never roll a token-enabled fleet back to the unmodified reference matcher: it misses and logs tokens.

## Operations and validation

```sh
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/metrics
npm run smoke
SMOKE_IDLE_MS=90000 npm run smoke
node scripts/http-timeouts.mjs
BENCH_WAITERS=100 BENCH_ROUNDS=5 npm run benchmark
```

Smoke launches an isolated compiled service unless `WARM_START_TEST_URL` selects an existing
test instance. It checks match, miss, health, metrics, and expiry across client retries.
Use only an isolated instance: the smoke test registers and dispatches synthetic work.
`http-timeouts.mjs` checks that a fully received GET can remain held beyond Node's request timeout.

Metrics include waiting/claimed/session gauges, dispatch outcomes, registration rejections,
disconnects, expirations, near-deadline claims, body rejections, and dispatch-duration buckets.
Only fixed outcome/reason labels are used. Run/controller/deployment IDs and validated dispatch
`traceparent` belong in structured logs. No full task payloads, deployment tokens, running-task
inventory, or recent-match history are retained for monitoring.

The benchmark starts a subprocess with an instrumentation preload used only for measurements.
`BENCH_ENTRY`, `BENCH_CWD`, and `BENCH_PORT` select another compiled service, including the
reference standalone Next.js server. Both services should use the same Node binary and host.
Results include startup, idle/waiting/post-churn RSS, CPU, event-loop delay, and dispatch percentiles.
Small one/two-request samples are sanity checks, not latency estimates. See [VALIDATION.md](VALIDATION.md)
for measured results and deployment checks still required.

Rebuilt from the reference warm-start service. Its MIT notice is retained in [LICENSE](LICENSE).
