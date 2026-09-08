# Validation record

Measured 2026-09-07 (America/New_York), with the final service-only implementation.

## Completed checks

- TypeScript typecheck and production compilation pass.
- **41 Vitest tests pass** on Node 22.23.1 in both linux/arm64 and linux/amd64 images. amd64 was tested through Docker Desktop emulation on an ARM host. The host Node 26.8.1 run also passes.
- Tests use real HTTP connections, real subprocess logs, and real timers. They cover signed-token normalization/redaction, bare/token FIFO, exact machine/version matching, missing-deployment 200 misses, malformed/oversize bodies, reconnect generation races, admission limits, idle-session retention, stalled writes, ambiguous delivery, and bounded shutdown with an incomplete body.
- The compiled Node 22 service completed a **90,000 ms idle budget in 90,013 ms**, across 434 client-owned 200 ms poll retries. This is a deliberately aggressive reconnect test. No deadline reset occurred.
- Published `@trigger.dev/core` **4.4.6 and 4.5.12 WarmStartClient** implementations were exercised against the service with a complete schema-valid dequeued message. Legacy bare ID, current bare ID, and current JWT identity all matched. With a 300 ms idle budget and 100 ms polls, the clients returned null after 303/305/308 ms respectively. These tests exercised the actual published client and schema, not an HTTP-client mock; no supervisor/platform was running in that harness.
- The Node request-timeout probe passed on pinned 22.23.1: a fully received GET held for about 3.2 seconds survived a 1.5-second `requestTimeout`.
- Prometheus `promtool` v3.5.0 accepts `/metrics` with exit code 0. The HTTP tests additionally check cumulative buckets, count consistency, bounded labels, and dispatch outcomes.
- Runtime images are non-root and have no runtime `node_modules`. Both architectures build locally. The workflow and publishing configuration are prepared but have not run on GitHub or published an image.

## Reference timing finding

The review correctly identified the client bug: it clears its request timer after response headers and then awaits the body without that timer. An early-flushing server/proxy can therefore expose undici's longer body timeout.

However, the reference built here from its lockfile (Next.js **16.1.6**) did **not** deliver response headers during a two-second idle GET probe. With the real 4.4.6 client, `KEEPALIVE_MS=1000` and `CONNECTION_TIMEOUT_MS=200`, it returned null after **1,134 ms**, rather than five minutes. The review's five-minute prediction was conditional on immediate header flushing and is not reproduced by this reference build. The replacement still deliberately withholds headers and enforces its own absolute idle deadline.

## Exploratory comparison

Both implementations ran sequentially under the **same Node 22.23.1 binary and Docker Desktop linux/arm64 environment**. The reference's standalone artifact was built from the supplied source, then run on Node 22 for this comparison. Each case used five rounds, with the specified number of held connections registered before serial POST dispatches. All dispatched messages matched. Waiting/session caps were increased for stress cases. `POLL_MATCH_MARGIN_MS` remained 0.

Startup is process spawn to first successful health response, not container startup or task execution. RSS is process resident memory, not machine-preset allocation. The instrumentation preload runs only in the benchmark. Parent HTTP-client and Docker overhead are included in dispatch latency. These are single exploratory passes on a shared development machine, not a production performance gate. One/two-waiter results have only 5/10 samples.

| Service | Waiters | Startup ms | Idle RSS MiB | Waiting RSS MiB | After churn RSS MiB | POST p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Node service | 1 | 78 | 55.9 | 56.4 | 56.8 | 4.06 | 7.48 | 7.48 |
| Next.js reference | 1 | 837 | 87.9 | 89.6 | 96.7 | 7.78 | 17.81 | 17.81 |
| Node service | 2 | 101 | 55.8 | 56.5 | 57.2 | 4.86 | 9.33 | 9.33 |
| Next.js reference | 2 | 963 | 86.8 | 89.4 | 97.4 | 8.05 | 12.28 | 12.28 |
| Node service | 100 | 74 | 55.8 | 57.9 | 71.7 | 3.14 | 5.23 | 26.25 |
| Next.js reference | 100 | 922 | 86.9 | 110.2 | 148.7 | 4.73 | 10.95 | 20.71 |
| Node service | 1000 | 102 | 55.8 | 76.6 | 130.4 | 2.92 | 5.70 | 12.31 |
| Next.js reference | 1000 | 806 | 87.1 | 188.0 | 409.0 | 4.36 | 8.29 | 19.46 |

The Node service used less memory and started sooner in these samples. Tail latency varied; the 100-waiter p99 did not improve in this pass. Do not infer an A1/E2 throughput limit, task-start SLO, or universal speedup from these measurements. Compare repeated canary runs before choosing a poll margin or changing production limits.

Raw records, including CPU usage and event-loop p99, are in [node.jsonl](validation/node.jsonl) and [reference.jsonl](validation/reference.jsonl). The repository's `scripts/benchmark.mjs` reproduces the methodology. The original reference image used a Node 20 base; image-size comparison across those differing bases is intentionally not presented as framework overhead.

## Implementation decisions clarified by testing

- A client retry can reach the server before the old socket-close event. A new unclaimed connection replaces the old generation without resetting its deadline; rejecting every live-looking duplicate would incorrectly terminate normal retries. The superseded response gets 409 if writable.
- Expired session tombstones live for `SESSION_EXPIRY_GRACE_MS` (60 seconds by default), under the same session cap, to reject expiry/reconnect races with 408. Retention is bounded; restart or a reappearance after retention cannot preserve that old deadline.
- An uncertain started write returns `didWarmStart:true` with metric outcome `ambiguous`. It does not increment `matched` or try another waiter. Existing platform recovery, or an upgraded supervisor verifier, must resolve delivery uncertainty.

## Still required before production rollout

- Run complete tasks on a real supervisor/platform and rebuilt runner images, including cold fallback, service restart, and upgrade/rollback. Published-client compatibility does not validate workload execution or token enforcement at the supervisor boundary.
- Canary on the actual A1 host with `MAX_WAITING_RUNNERS=1`; verify real allocations and private-network/proxy timeouts. The reviewed Compose files currently disable warm starts.
- Measure 0/1000/2000 ms poll margins on representative networks. The margin behavior is tested, but no nonzero production margin is selected.
- Establish a disabled-warm-start operational baseline and repeated production regression budgets. The standalone matcher benchmark cannot measure platform cold-start latency.
- Stage 2 remains separate: upgrade v4.4.6, fix runner friendly-ID/deadline handling, enable delivery verification, and implement warm-only dequeue and safe capacity reclamation. No full-host scheduling fix is claimed by this service.

No production deployment, registry publication, or supervisor changes were made.
