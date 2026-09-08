# Resource optimization validation

Date: 2026-09-07 (America/New_York). Scope: general checkpoint-service correctness
and resource efficiency. E2 support is optional and was not tested.

## Implementation

- `/env` caches the owning full container ID with metadata, checks current Docker
  ownership/address on every cache hit, and rejects stale identity before the next
  reaper pass. Ordinary runners with no mapping require no Docker lookup. Reaper
  passes cannot overlap and idle per-run locks are removed.
- Per-run mutation locks cover suspend, restore and cleanup even when operation
  concurrency is greater than one. An in-flight suspend duplicate still receives
  an immediate acknowledgement.
- Restore checks the live container, persisted run/snapshot/checkpoint identity,
  and labels before modifying its directory. A live duplicate does no download or
  extraction; its record and `/env` mapping survive service restart and checkpoint
  reclamation. Live records are retained until the container exits/disappears.
- Jobs store their snapshot repository when accepted. Cleanup derives the image
  from that repository and the run/snapshot IDs, including when commit succeeded
  but reference persistence/push failed. Image deletion is non-forcing and errors
  retain records for retry. Base images are not cleanup candidates.
- Successful explicit run cleanup evicts jobs. Deduplication lasts until that
  cleanup succeeds, including across restart; explicit deletion ends its retention
  window. There is no time-based expiry that could discard source-recovery state.
  Jobs awaiting platform reclamation and failed recovery remain intentionally
  retained. Cleanup retries run on restart and reaper passes (or explicit retry
  when the reaper is disabled).
- Cancellation has a durable marker and lets the active operation recover its
  source before cleanup. Failed recovery retains the checkpoint and reaper
  protection. Queued cancellation also survives restart.
- Upload concurrency and gzip level are validated settings. S3 body streaming,
  automatic part sizing, SDK retries/checksums and multipart abort remain enabled.
  Cancellation stops file reading and waits for SDK transfer/abort completion
  before the service can reclaim the run, preventing late writes after cleanup.
- The image and deployment Compose use curl for `/health`, retaining the 10-second
  interval, 5-second Docker timeout and 4-second HTTP timeout. The supervisor's
  separate probe is unchanged.
- Opt-in structured stage measurements cover preparation, checkpoint request,
  commit, push, compression, upload, download/extraction, container creation,
  staging copy, checkpoint start, metadata registration, recovery and cleanup.
  Only IDs, timings and counters are logged; archive size is logged after
  compression/download. Service CPU/RSS counters do not measure dockerd/CRIU.

Persisted job and restore records use schema version **2**. There is no migration
or fallback for version 1. Test this release with a fresh `CHECKPOINT_ROOT` and
new runs; do not point a version 2 test at outstanding version 1 checkpoints.
The tar/gzip archive contract remains unchanged; the new state format has no
backward-compatibility requirement.

## Validation result

- Baseline: 29 tests passed before editing.
- Candidate after cleanup: **71/71 tests pass** on macOS with Node 26.8.1 and
  inside the built Linux ARM64 image with Node 22.23.1. HTTP checks are part of
  `npm test`; the image run also verifies the packaged curl probe.
- Syntax checks, local and packaged HTTP smoke tests, healthy/failing/hung probe
  checks, Compose validation and whitespace checks pass.
- Local review image: `trigger-checkpoint-service:scripts-cleanup`, image ID
  `sha256:faa15788966d6fdfbea76f948adeb311e1f5e233b05e8915b3472014af0f6d3d`.
- Pull-request CI now runs tests, syntax/HTTP checks, an image build and the
  packaged probe check without publishing an image. No branch/merge or deployment
  was performed as part of this implementation.

One local Docker cache export failed with a missing parent snapshot. An uncached
rebuild succeeded, followed by a successful final build and tests. No Docker
internal metadata, global prune or runtime configuration was changed.

## Local measurements

The tests below are synthetic component comparisons, not A1 host or end-to-end
Trigger measurements. Docker Desktop: Linux ARM64, Docker 29.2.1,
`6.12.76-linuxkit`, 8 CPUs, 8,217,341,952 bytes RAM, experimental mode disabled.
Uploads/probes used Node 22.23.1 inside the review image. Compression used native
macOS tar/gzip. Archive inputs contain only generated zero/random bytes.

The one-off benchmark runners, sampler and raw logs were removed after the
measurements. The summarized results below retain the evidence for the default
settings; they are not a maintained benchmark harness. Medians below use three
upload/compression repetitions per case and ten healthy probe invocations; these sample counts cannot establish tail percentiles.

| Upload input | Queue | Median time | Median process peak RSS |
|---|---:|---:|---:|
| 16 MiB | 4 | 174 ms | 118.8 MiB |
| 16 MiB | 1 | 349 ms | 123.2 MiB |
| 64 MiB | 4 | 499 ms | 151.9 MiB |
| 64 MiB | 1 | 1,172 ms | 133.6 MiB |

The local S3 protocol fixture imposes a 50 ms response delay per part and discards
streamed bodies. Each measurement uses a fresh process. Peak RSS includes the
fixture, SDK, checksums and allocator behavior; it is not an isolated estimate
of service buffer memory. At 64 MiB, queue 1 saved about 18.3 MiB but took 2.35×
as long. The 16 MiB case did not show an RSS saving. Keep **queue 4** as the general
default; queue 1 is available for workloads where measured memory pressure
justifies the throughput cost. The nominal default-part buffer budget is 20 MiB
versus 5 MiB; total process RSS need not fall by exactly 15 MiB.

| 32 MiB synthetic content | Gzip | Median time | Archive bytes | Child CPU seconds |
|---|---:|---:|---:|---:|
| Zeros | 6 | 87 ms | 32,996 | 0.09 |
| Zeros | 1 | 47 ms | 146,858 | 0.05 |
| Half zeros, half random | 6 | 349 ms | 16,820,914 | 0.35 |
| Half zeros, half random | 1 | 332 ms | 16,876,275 | 0.32 |
| Random | 6 | 606 ms | 33,565,190 | 0.61 |
| Random | 1 | 585 ms | 33,565,229 | 0.59 |

Level 1 reduces CPU time but increases archive size. A slow uplink can outweigh
that CPU saving. Keep **gzip 6** as the general default pending representative
CRIU archives and end-to-end timings; level 1 is available for evaluation. No
multithreaded compression, zstd format or restore-copy redesign is introduced.

The healthy Node probe took about **50 ms** with **55 MiB** median sampled peak
RSS; curl took about **5 ms** with **9 MiB**. Samples every 2 ms can miss short
peaks, particularly for curl. Both detected HTTP 503 and a refused connection;
curl also failed the hung endpoint after approximately four seconds. A second
Node process is therefore removed from each health-check cycle, although this
is transient process cost rather than a continuous service-RSS reduction.
Compared with the original Dockerfile, the package manifests show 15 additional
packages totaling 6,044 KiB (about 5.9 MiB) of installed files.

## Maintained validation

Keep `scripts/smoke-service.mjs` for real Docker/CRIU, registry/S3 and recovery
validation on a disposable qualified worker. Regression coverage lives under
`test/`; the S3 fixture is local to its tests and is not shipped in the image.

From `services/checkpoint/`:

```sh
TMPDIR="$(realpath "${TMPDIR:-/tmp}")" npm test
npm run check
docker build -t checkpoint-review .
docker run --rm --network none -e CHECKPOINT_TEST_IMAGE=1 \
  --mount "type=bind,source=$PWD/test,target=/app/test,readonly" \
  checkpoint-review node --test test/server.test.js
```

Upload tests use an actual local HTTP endpoint with the installed S3 SDK. They
cover small/multipart bodies, queues 1/4, transient failures, permanent failures,
interrupted connections, cancellation, and automatic part scaling with a sparse
60 GiB file (only one part is transferred). That is not a 60 GiB throughput test.

## Qualified-host measurement procedure — pending

1. Use the isolated A1 preview worker with the qualified Docker 27.5.1/CRIU profile.
   Record plugin/task image digests, worker-group isolation, payload and presets.
   Confirm base layers are reused. Keep cold pulls in a separate comparison.
2. Capture the unchanged version first using host monitoring (for example,
   `vmstat`, `pidstat`, Docker stats and cgroup-v2 counters). Record host
   `MemAvailable`, swap-in/out, major faults, OOM events, process RSS/CPU, disk I/O
   and network counters, correlated with stage timestamps. Account for short-lived
   CRIU/tar/gzip processes, sampling gaps and monitoring overhead. Do not collect
   task environment variables or checkpoint contents.
3. Use a fresh state root for the candidate. Enable `CHECKPOINT_METRICS_ENABLED=true`.
   Run `scripts/smoke-service.mjs` inside the candidate service on the disposable
   worker using the existing `CHECKPOINT_SMOKE_CONFIRM=disposable-worker` setting.
   It checks real registry/S3, source disappearance, preserved in-memory marker,
   duplicate restore/restart metadata and injected push-failure source recovery.
4. Run the real `checkpoint-validation-parent` at 60 and 240 seconds. Record the
   parent log before and after resume, the unchanged in-memory marker, source
   disappearance and completed cleanup. Do not use >240 seconds with that task.
5. Compare each tuning change independently against the baseline, then combine.
   Repeat at least five warm runs for each case plus bounded contention/failure
   drills. Evaluate useful resource savings, throughput and resume latency for
   representative supported CPU/memory capacities. Keep admission logic unchanged.
6. Correlate **swap-in during each Docker checkpoint window**, not only idle swap.
   Include dockerd/CRIU and overlapping runners in the memory assessment. Verify
   no OOMs, duplicate execution, lost jobs or unexpected management restarts.

Live A1 baseline, service smoke, real parent/child validation, and additional
supported-host measurements have not been run for this candidate. Local Docker
29 with checkpoint support disabled cannot satisfy those gates. E2 qualification,
fleet rollout and any format/copy redesign remain outside this local validation.
