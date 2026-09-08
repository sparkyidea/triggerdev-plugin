# Checkpoint correctness and resource optimization across supported machines

Status: implementation and local validation complete; qualified-host gates pending.
Deployment and optional host qualification remain subject to the rollout boundaries below.
Date: 2026-09-07
Scope: `services/checkpoint/` in this repository, followed by explicitly approved
deployment changes in `trigger-worker-swarm`.

## Objective

Improve checkpoint correctness and reduce memory, CPU, disk, and background
process overhead across all supported worker machines without weakening durability,
recovery, or isolation. A1 is the currently qualified validation platform, not the
limit of the optimization scope.

Success means corrected lifecycle behavior, bounded retained state and owned disk
artifacts, and measured resource savings with acceptable end-to-end latency and
throughput on representative supported configurations. Choose defaults from those
tradeoffs; expose validated tuning where machine capacity or workload changes the
best setting. Do not optimize every machine for the smallest host's constraints.

Helping E2 is a welcome optional benefit, not a goal, acceptance requirement, or
reason to delay these changes. E2 qualification is a separate experiment and may
remain unattempted or unsuccessful without affecting completion of this plan.

## Evidence and current limitations

- Worker 00005 has completed a real Trigger parent/child checkpoint and resume.
- Its checkpoint service was measured at approximately 69 MiB idle memory.
- A successful run showed about 44.1 seconds in Docker's checkpoint request,
  1.3 seconds in commit, and 19.5 seconds in Docker's checkpoint-start request.
  These are wall-clock durations, not measured CPU-seconds or CRIU-only timings.
- The registry bottleneck was separately repaired: stale source-repository
  metadata caused base-layer uploads. After cache repair, publication took
  2.3 seconds for an existing snapshot and 4.0 seconds for another snapshot,
  reusing all 14 base layers and uploading only the new roughly 6.6 MB layer.
- The latest inspected successful task used `delayMs: 60000`. Do not present
  its results as a two-minute-wait benchmark.
- Worker 00006 was stopped during the comparison. Its lower host RAM usage was
  not a valid no-checkpoint baseline. Docker/kernel versions also differ.
- Archive transfers already stream through files; they do not load the complete
  checkpoint into a single application buffer. Checkpoint operation concurrency
  already defaults to one, but task execution concurrency is separate.

## Safety requirements

- Do not modify Docker core or its internal layer metadata files.
- Keep registry-backed filesystem snapshots and durable S3 checkpoint storage.
  Do not use `CHECKPOINT_ROOTFS_MODE=none` as a resource optimization.
- Preserve source recovery after upload failure. Never substitute a cold start
  for a failed checkpoint restore.
- Preserve full-container-ID validation, symlink/special-file rejection, unique
  staging directories, and non-overwriting publication of staged checkpoints.
- Keep one combined private Docker proxy if desired. Do not expose its ports,
  join runners to its management network, or relax permissions for this work.
- Do not add hard management-container limits without accounting for the
  supervisor's current admission logic, which subtracts those limits from its
  task budget. A Node heap limit is not a total process-memory limit either.
- Keep credentials, task environment variables, and full manifest contents out
  of benchmark logs. Use timings, sizes, resource counters, and opaque IDs.
- Do not enable E2 checkpoints fleet-wide. Any E2 pilot must use an isolated,
  homogeneous x86 worker group and separately pass host/runtime qualification.

## Phase 0 — establish a reproducible baseline

Before optimization, record the plugin image digest, task image digest, runtime,
Docker/containerd/runc/CRIU versions, kernel, CPU architecture, container limits,
swap configuration, and worker-group isolation. Start on the qualified A1 host;
compare representative CPU/memory capacities and archive sizes on other supported
configurations where available. Report the coverage actually measured and keep
new host/runtime qualification separate from optimization validation.

Add lightweight, structured measurements around:

1. Runner inspection and preparation.
2. Docker checkpoint request.
3. Docker commit and registry push, separately.
4. Archive compression and S3 upload, separately.
5. S3 download and archive extraction, separately.
6. Restore staging copy, container creation, and Docker checkpoint-start.
7. Metadata registration and the parent's first post-resume task log.
8. Cleanup and failure recovery.

Measure the host and all participating processes, not only the checkpoint
container: plugin RSS/peak memory, supervisor and runner memory, dockerd/CRIU
CPU and peak memory where observable, host `MemAvailable`, swap-in/out, disk
usage/I/O, network bytes, and OOM events. Capture short-lived processes while
they run; their cgroups may disappear after completion. Record observer overhead.

Sample swap-in specifically during the Docker checkpoint request, correlated
with dump latency, major page faults, and minimum `MemAvailable`. CRIU must read
the runner's dumpable memory, which can fault swapped-out pages back in. Idle
swap usage alone cannot establish that swap remains a cushion during the dump.

Establish warm-cache results first, with base-layer reuse verified. Report cold
image pulls separately. Use the same task image, payload, and presets for each
comparison. Prefer an on/off comparison on the same disposable host to remove
hardware and Docker-version differences. Do not attempt instance targeting
through the shared pull queue; use isolated groups or controlled active workers.

## Phase 1 — correctness fixes first

### 1. Prevent stale runner metadata and IP reuse

Files: `src/checkpoint-service.js`, lifecycle tests.

`metadataByIp` never removes records. Docker reuses bridge IPs, and ordinary
runners also call `/env`, so a fresh runner can receive a previous restored
run's identity. Treat this as a correctness and isolation fix, with the strongest
lifecycle regression coverage; keep job-map eviction in a separate change.

- Track metadata by container identity as well as IP. Remove stale IP mappings
  when the owning container exits/disappears. Validate live ownership before
  serving cached metadata so IP reuse between reaper passes cannot leak identity.
  Cleanup of an old owner must not remove a newer owner's mapping at the same IP.
- Do not immediately remove metadata merely because a checkpoint was reclaimed:
  the restored runner may still be running and requesting metadata.
- Preserve durable restore metadata needed after a service restart while its
  runner remains live, including across checkpoint deletion.
- Test IP reuse by an ordinary runner before the next reaper pass (must return
  `{}`), reuse by another restored runner, late cleanup of the previous owner,
  container exit/removal, registration races, repeated deletion, and service
  restart. Assert the right live-run metadata survives and stale identity is
  never returned, including when ownership verification fails.

### 2. Preserve live restore records on duplicate requests

Files: `src/checkpoint-service.js`, restore/recovery tests.

`processRestore()` deletes the entire restore directory before downloading and
extracting the archive, then checks for an existing running runner. A duplicate
request therefore deletes that live runner's `restore.json` and returns without
recreating it. The next service restart loses the runner's `/env` mapping. Fix
this correctness bug before treating duplicate restores as an efficiency gain.

- Check persisted restore identity and the matching live container before any
  destructive directory cleanup, download, or extraction.
- Return early only when container ID, ownership, run, snapshot, and checkpoint
  identity match; do not trust the deterministic container name alone. Persist
  sufficient checkpoint identity for this verification. Use the new persisted
  format without migration or fallback for older state, per user direction.
  Preserve a current live runner's files on ambiguous or conflicting identity.
- Preserve `restore.json` and re-register verified metadata on a live duplicate.
  Coordinate concurrent requests so cleanup cannot race a live restore.
- Test restore → duplicate restore → service restart → `/env`, asserting the
  original identity and durable record survive and no transfer/extraction occurs.
  Also cover stale/missing records, conflicting containers, different checkpoints
  for the same run, interrupted restores, and concurrent duplicates.

## Phase 2 — disk hygiene and measured resource savings

### 3. Discover owned snapshot images deterministically during cleanup

Files: `src/checkpoint-service.js`, failure/recovery tests.

- Primary fix: for every validated job record found by `deleteRun()`, derive the
  owned image tag with `snapshotImageRef(repository, runFriendlyId,
  snapshotFriendlyId)` instead of depending on `job.rootfsImageRef` being set.
  This finds images after both push failure and a crash between commit and the
  next state write. Treat an absent image as an idempotent cleanup case.
- Retain validated persisted references for repository-configuration changes;
  deduplicate candidates and constrain deletion to owned snapshot repositories
  and identities. Deterministic derivation requires the original repository;
  preserve it before commit if configuration may change across recovery.
- Persisting the image reference immediately after commit, before push, is a
  secondary bookkeeping improvement, not the mechanism that closes the crash gap.
- Keep recoverable artifacts until recovery or explicit cleanup is safe. Preserve
  cleanup retry information on deletion failure; never use a broad Docker prune.
- Test commit success followed by push failure, crash before reference persistence,
  state-write failure, restart, cancellation, retry, absent images, repository
  changes, and cleanup failure. Never delete task base images or another run's
  active snapshot.

### 4. Bound retained jobs separately from metadata cleanup

Files: `src/checkpoint-service.js`, lifecycle/recovery tests.

`deleteRun()` removes disk state but never removes entries from `this.jobs`;
the reaper rescans every retained job each interval. Memory savings are modest
(roughly 1 KB per job as an order-of-magnitude estimate; measure actual retention),
while eviction affects deduplication, cancellation, reaper protection, and restart
recovery. This is a separate, higher-risk lifecycle change.

- Evict only after coordinated cleanup, preserving queued/in-flight operations
  and failed-recovery records that protect recoverable source runners.
- Preserve duplicate-request behavior using a bounded, documented retention
  mechanism where needed, including its restart semantics. The implementation
  retains deduplication until explicit successful run deletion, which ends the
  retention window; active/failed-recovery records have no time-based expiry.
- Test repeated suspend/restore/delete cycles, repeated deletion, duplicate
  requests before and after retention expiry, queued/in-flight cancellation,
  restart recovery, and failed source recovery. Assert retained state stays
  bounded without dropping protection for active or recoverable runners.

### 5. Bound multipart upload buffering

Files: `src/storage.js`, `src/config.js`, storage/config tests.

- Introduce a validated upload concurrency setting and compare `queueSize: 1`
  against the current `4` on representative archive sizes and network conditions.
  Select the general default from measured memory/throughput tradeoffs, retaining
  tuning for constrained and higher-capacity hosts. Do not make `1` universal
  solely to accommodate E2. Only queue size changes: 5 MiB is already the SDK's default minimum part size.
  Leave `partSize` unset to preserve automatic scaling for large known-length
  objects (the installed SDK uses `max(5 MiB, ceil(ContentLength / 10000))`,
  increasing above about 50 GiB).
- The current SDK uses four concurrent parts. For ordinary archive
  sizes, this changes the multipart-buffer budget from about 20 MiB to 5 MiB;
  it is not a guarantee of a 15 MiB drop in total RSS.
- Preserve streaming, content length, retries, checksums, and multipart abort
  behavior. Handle the service's supported maximum archive size explicitly.
- Test small single-part objects, multipart objects, slow uploads, failures,
  retries, and interrupted uploads. Verify actual peak RSS and upload duration.

### 6. Replace the Node-based health-check subprocess

Files: `Dockerfile`; corresponding Compose health check in the deployment repo.

- Use a lightweight HTTP client rather than starting a second Node runtime
  every ten seconds. Preserve HTTP failure detection and Docker connectivity
  checks performed by `/health`.
- Evaluate a longer interval against failure-detection needs.
- Update the effective Compose override as well as the image default; changing
  only the Dockerfile will not replace a Compose-specified probe.
- Install a lightweight HTTP client package explicitly: the current slim image
  has neither curl nor wget. Include that package's footprint in the comparison.
  The supervisor's Node probe has a similar cost but belongs to a separate image;
  include it in host measurements without assuming this change removes its cost.
- Verify healthy, unavailable, and hung endpoints, timeout behavior, and idle
  CPU/peak-memory differences. Account for the small added image dependency.

### 7. Benchmark faster archive compression

Files: `src/checkpoint-service.js`, `src/files.js`, configuration and tests.

- Make archive gzip level explicit and benchmark level 1 against the current
  default. Keep the `.tar.gz` contract for this pass; backward compatibility
  with old persisted job/restore records is not a requirement.
- Measure compression CPU-seconds, peak RSS, archive size, upload duration, and
  total suspend latency. Select based on end-to-end cost, not compression alone.
- Evaluate CPU capacity, contention, and peak resources before increasing
  compression threads or overlapping commit/push/compression. A constrained
  host's best setting need not be the default for higher-capacity machines.
- This does not optimize the time already spent inside Docker checkpoint/start.
- Zstd is a promising larger follow-up, but requires dual-format restore support
  and format identification before publication changes. Defer it from this pass.

### 8. Consider fewer restore copies only after profiling

The current path stores a downloaded archive, extracts it, then copies CRIU
files into Docker's staging directory. Profile bytes and time for each step.

Possible later work: remove an unneeded downloaded archive after safe extraction,
or restructure validated staging to avoid a duplicate copy. Reflinks are an
optional filesystem-dependent optimization, not a portable assumption. Do not
replace copies with shared writable hard links, shift disk use into RAM without
a measured host-memory budget, or discard the only recovery copy. A local cache would require a bounded size,
ownership/integrity checks, and S3 fallback; it is outside the first pass.

## Phase 3 — qualification and rollout decision

### General regression and performance gate (A1 first)

- Run the existing unit tests, syntax checks, isolated service smoke test, and
  real Trigger parent/child validation.
- The review reports all 29 current unit tests passing. On macOS, use a
  symlink-free `TMPDIR` (resolve it with `realpath`); staging correctly rejects
  the `/var` symlink. Preserve that check; normal Linux `/tmp` needs no workaround.
- Verify the source disappears, the parent resumes exactly once with its
  original in-memory marker, and cleanup completes.
- Repeat upload-failure/source-recovery and service-restart tests.
- Compare baseline versus each optimization independently before combining.
  Report idle and peak memory, CPU-seconds, disk retention/I/O, network bytes,
  suspend/resume latency, and throughput for the same workload and host setup.
- Correctness fixes must pass regardless of memory savings. Resource changes
  must show a useful measured benefit with acceptable latency/throughput costs;
  document machine-specific tradeoffs and retain tuning where appropriate.
- Verify no OOM kills, unexpected management restarts, duplicated execution, or
  lost jobs, and bounded retained state/artifacts across repeated lifecycle tests.
  Preserve failed-recovery artifacts intentionally. Assess memory headroom against
  each supported configuration's workload and admission budget; the E2-specific
  128 MiB pilot target below is not a general acceptance threshold.
- Validate other supported configurations where available and document gaps.
  Passing on A1 does not automatically qualify a different architecture/runtime.
  Completion of this gate does not depend on E2 support or an E2 pilot.

### Optional E2 follow-up — outside required completion

This section records a possible follow-up, not an optimization target. Complete
and assess the general changes regardless of whether this pilot is attempted or
passes. Re-measure the documented idle baseline before using it in a qualification
report.

Expected E2 outcome: keep checkpoints disabled. The documented idle E2 baseline
is approximately 497 MiB used and 456 MiB available, before adding the checkpoint
service and a second Docker proxy if deployed separately (the inspected A1
Compose now shares one proxy). With two overlapping runners and the peaks
from CRIU dump, commit, and gzip across the workflow, the provisional 128 MiB
minimum available-memory target is unlikely to hold. This is a planning inference,
not a measured checkpoint-on result. Even a possible 15–30 MiB reduction in Node
RSS from phases 1–2 would not materially change that assessment; that range is
an unmeasured estimate, not an additive savings guarantee.

An Oracle E2.1.Micro has approximately 1 GB RAM, with 2 GB swap in this setup,
and exposes 2 vCPUs with a burstable baseline of one eighth of an OCPU. See
[Oracle's shape definitions](https://docs.oracle.com/en-us/iaas/Content/Compute/References/computeshapes.htm)
and [Always Free resources](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm).
An optional isolated pilot may test the headroom forecast; it is not a prerequisite
for shipping the general improvements. A working ARM64 A1 checkpoint does not qualify x86 E2,
and reducing Node RSS does not establish that dockerd, CRIU, and runners fit.

Only after explicit approval, qualify the x86 Docker/CRIU host and deploy one
isolated E2 with a rollback path. Start with micro tasks and one checkpoint
operation at a time. A parent/child test requires room for both runners while
they overlap; do not impose a queue limit that prevents the child from starting.

Use 60-second and 240-second waits with the existing validation task. Longer
waits require a dedicated benchmark task because the current child validates a
maximum of 240 seconds. Run at least five warm repetitions per case, plus a
bounded contention test and failure drills. Do not use five samples to claim
reliable tail-percentile performance.

Initial acceptance criteria:

- Every correctness/failure-recovery test passes; no duplicated task execution.
- No host or cgroup OOM kills, unexpected management restarts, or lost jobs.
- At least 128 MiB minimum sampled host `MemAvailable` during the initial
  micro-task pilot, plus explicit reporting of sampling gaps and short peaks.
  This is a provisional safety target, not proof against all OOM conditions.
- No sustained swap churn or loss of host responsiveness. Swap remains a
  safety cushion, not additional RAM in the scheduling budget. Report swap-in
  during each Docker checkpoint window explicitly; idle swap figures cannot
  satisfy this criterion.
- Retained in-memory state and owned disk artifacts remain bounded over repeated
  completed runs; failed-recovery artifacts remain intentionally protected.
- Report completed tasks per unit time, parent resume delay, CPU-seconds,
  peak memory, and transferred bytes for checkpoint-on versus checkpoint-off.
  A rollout requires a demonstrated benefit for the intended workload, not
  merely a successful restore or lower idle plugin memory.

If the complete system still lacks memory headroom or checkpoint overhead exceeds
the benefit, keep E2 checkpoints disabled. Continue using swap and conservative
task admission without checkpoints; do not weaken durability to pass the test.

## Implementation evidence

See `services/checkpoint/docs/resource-optimization.md` for the implementation,
recorded component measurements and outstanding host gates. One-off benchmark
scripts and raw logs have been removed; the maintained suite and the real-service
smoke script cover ongoing validation.
New job/restore records use schema version 2 with no legacy migration path.
All 71 tests pass locally and in the Linux ARM64 image; syntax, HTTP smoke, probe,
Compose and whitespace checks pass. General defaults remain upload queue 4 and
gzip level 6 based on measured component tradeoffs, with validated tuning.
Restore-copy restructuring remains deferred pending qualified-host profiling.
PR CI tests the candidate without publishing an image. Local source/Compose edits
do not deploy or enable checkpoints on additional workers.

## Deliverables and boundaries

1. Baseline measurements and a repeatable, secret-safe benchmark procedure.
2. Small independently reviewable changes with regression tests for phases 1–2.
3. Regression and resource-comparison results on supported configurations,
   beginning with A1, with measured coverage and remaining validation gaps.
   An E2 qualification report is optional and only required if that pilot is approved.
4. Updated README/config examples documenting defaults and measured tradeoffs.
5. Explicit approval before deploying to additional workers or enabling a fleet
   default. Keep security redesign, runtime upgrades, and language rewrites out
   of this optimization pass unless measurements justify a separate proposal.

References: [AWS multipart buffering](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-lib-storage/Interface/Configuration/),
[gzip compression levels](https://www.gnu.org/s/gzip/manual/gzip.html),
and the layer-reuse repair report in
`trigger-worker-swarm/docs/machines/worker-00005-checkpoint-layer-reuse.md`.
The E2 idle baseline is recorded in
`trigger-worker-swarm/docs/machines/oracle-e2-1ocpu-1gb.md`.
