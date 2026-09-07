# Trigger.dev Docker checkpoint service

An **experimental**, self-hosted implementation of the checkpoint HTTP contract used by
Trigger.dev supervisor v4.5.14. It checkpoints runner containers with Docker/CRIU, stores
the process snapshot in local or S3-compatible storage, captures the writable root
filesystem as a registry layer, and restores the runner when Trigger.dev dequeues it again.

This is not an upstream-supported Trigger.dev component. Treat it as a controlled pilot,
not as a production durability guarantee.

> **Note:** this repository holds the service source and its image build only. Paths like
> `deploy/…`, `provisioning/…`, and `trigger/…` below refer to the private
> `trigger-worker-swarm` deployment repo, whose worker Compose files consume the image
> published from here.

## Why this service exists

Trigger.dev exposes `TRIGGER_CHECKPOINT_URL` and the client-side routes in the open-source
supervisor, but it does not publish the server behind those routes. The old open-source
`apps/docker-provider` did contain direct `docker checkpoint create/start` calls, but that
provider belonged to the retired V1 engine and restored only the same container on the same
host. It cannot be plugged into the current supervisor.

This service implements the current routes:

- `POST /api/v1/runs/:runId/snapshots/:snapshotId/suspend`
- `POST /api/v1/runs/:runId/snapshots/:snapshotId/restore`
- `POST /api/v1/runs/:runId/checkpoints/delete`
- `POST /api/v1/runs/:runId/checkpoints/cancel`
- `GET /env` for restore-time runner/supervisor identity overrides
- `GET /health`

## Data path

1. The runner asks its local supervisor to suspend.
2. The supervisor receives `{ok:true}` immediately and sends the suspend job here.
3. This service inspects the runner, creates an `Exit=true` Docker checkpoint, commits and
   pushes its writable filesystem layer, archives the CRIU images, and uploads the archive.
4. It authenticates to the webapp with the worker token and reports the completed checkpoint.
5. When the waitpoint completes, a supervisor calls the restore endpoint with the stored
   checkpoint location.
6. This service pulls the snapshot image, recreates and starts the container from the
   downloaded CRIU checkpoint, then registers metadata against its newly allocated IP.
   An early `/env` request waits for that post-start registration.

The service intentionally refuses to checkpoint a runner with Docker AutoRemove enabled.
Otherwise Docker may erase the stopped source container before its writable layer can be
captured. Set `DOCKER_AUTOREMOVE_EXITED_CONTAINERS=0`; the built-in reaper removes ordinary
exited `runner-*` containers after a grace period.

## Supported pilot topology

- Linux host (Ubuntu/Debian is the provided setup path)
- Docker Engine with experimental features enabled
- CRIU installed and passing `criu check`
- Trigger.dev webapp and supervisor pinned to v4.5.14
- A dedicated ARM64 worker group whose every worker enables checkpoints
- Shared S3-compatible storage for a multi-worker group
- A registry reachable by every worker, with a dedicated checkpoint image repository

The first pilot target is the Ubuntu Oracle A1 ARM64 worker with 6 GB RAM. The 1 GB E2 does
not have enough validated checkpoint-time headroom, and Fedora Asahi/SELinux on the Mac mini
is not covered by the provided host setup. Never let an ARM64 checkpoint be dequeued by an
AMD64 worker.

## Published image

The GitHub Actions workflow at `.github/workflows/checkpoint-image.yml` builds both
`linux/amd64` and `linux/arm64` and publishes them as one multi-platform image:

```text
ghcr.io/sparkyidea/trigger-checkpoint-service
```

A push to `main` that changes `services/checkpoint/` publishes `latest` and an immutable
`sha-<full-commit-sha>` tag; a push to `dev` publishes a `dev` tag the same way. A
`checkpoint-v*` Git tag publishes that tag, and the workflow
can also be run manually with an optional additional tag. The deployment Compose directly
uses the published `latest` image during the pilot. After the pilot stabilizes, replace it
with the tested immutable `sha-<full-commit-sha>` tag so unrelated pushes cannot update
workers automatically.

The workflow authenticates with its automatic `GITHUB_TOKEN`; no registry secret is needed
to publish from this repository. If the GHCR package is private, log each worker's Docker
daemon into `ghcr.io` with a token that has `read:packages`, or change the package visibility
to public in GitHub before deploying it.

## Enable on a test worker

The webapp stack provides both shared storage services:

- Assign an HTTPS domain such as `checkpoint-storage.example.com` in Dokploy to the `minio`
  service's internal port `9000`. MinIO has no published host port.
- Set `CHECKPOINT_S3_ACCESS_KEY_ID` and `CHECKPOINT_S3_SECRET_ACCESS_KEY` in the webapp
  `.env`. On a fresh MinIO data volume, the MinIO service's first-boot command creates the
  `trigger-checkpoints` bucket, installs `deploy/webapp/minio/checkpoint-policy.json`,
  creates that restricted user, attaches the policy, records an initialization marker in
  the data volume, and then starts the normal MinIO server. No extra service remains.
- Copy those same `CHECKPOINT_S3_*` credentials into each checkpoint-enabled A1 worker
  `.env`.
- Keep using the existing Docker registry domain and credentials; checkpoint root filesystems
  are stored in its `trigger-checkpoints` repository.

After deploying the webapp stack, verify the initialization and restricted identity:

```bash
docker compose --env-file deploy/webapp/.env \
  -f deploy/webapp/docker-compose.yml logs minio
docker compose --env-file deploy/webapp/.env \
  -f deploy/webapp/docker-compose.yml exec minio \
  /bin/bash -ec 'mc --config-dir /.mc admin user info local "$CHECKPOINT_S3_ACCESS_KEY_ID"'
```

The user should be enabled with the `trigger-checkpoints` policy. Later MinIO restarts see
the marker and skip identity creation. This path intentionally targets a fresh deployment;
changing the checkpoint credentials later requires a fresh MinIO volume or an explicit
manual credential rotation.

The MinIO domain must accept large, long-running S3 `PUT`, `GET`, and `DELETE` requests.
Do not put interactive authentication in front of S3. Prefer a private domain or DNS-only
origin rather than a CDN proxy with request-size limits.

Before deploying, move the A1 onto a dedicated ARM64 worker-group token. Then, from a
checkout of the deployment repo on that worker host:

```bash
sudo provisioning/checkpoint/enable-checkpoints-ubuntu.sh
cp deploy/worker/orc-a1cpu1ram6/.env.example deploy/worker/orc-a1cpu1ram6/.env
# Fill the normal worker .env, including its three CHECKPOINT_S3_* values.
docker compose --env-file deploy/worker/orc-a1cpu1ram6/.env \
  -f deploy/worker/orc-a1cpu1ram6/docker-compose.yml up -d
```

The same worker `.env` contains the only additional settings:

```dotenv
CHECKPOINT_S3_ENDPOINT=https://checkpoint-storage.example.com
CHECKPOINT_S3_ACCESS_KEY_ID=...
CHECKPOINT_S3_SECRET_ACCESS_KEY=...
```

The Compose hardcodes the internal checkpoint URLs, disables runner AutoRemove, derives the
checkpoint registry repository from `DOCKER_REGISTRY_URL`, and starts the checkpoint service
and its isolated Docker proxy. Confirm the integrated stack:

```bash
docker compose --env-file deploy/worker/orc-a1cpu1ram6/.env \
  -f deploy/worker/orc-a1cpu1ram6/docker-compose.yml ps
docker compose --env-file deploy/worker/orc-a1cpu1ram6/.env \
  -f deploy/worker/orc-a1cpu1ram6/docker-compose.yml logs checkpoint
docker compose --env-file deploy/worker/orc-a1cpu1ram6/.env \
  -f deploy/worker/orc-a1cpu1ram6/docker-compose.yml logs supervisor | grep -i checkpoint
```

The expected supervisor line is `Checkpoints enabled`.

## End-to-end validation

Deploy and trigger `checkpoint-validation-parent` from `trigger/checkpoint-validation.ts`.
Its child stays alive for two minutes with a normal timer, forcing only the parent through
`triggerAndWait()`. Follow the checkpoint and supervisor logs during the run.

Success requires all of the following:

1. The parent logs `before triggerAndWait` once.
2. The service logs `Checkpoint completed`, and the original parent runner disappears while
   the child continues.
3. After the child finishes, the service logs `Checkpoint restored`.
4. The parent logs `after triggerAndWait` with the same `memoryMarker` and completes.
5. The code before `triggerAndWait()` is not executed again.
6. MinIO objects and registry manifests are deleted after the run is reclaimed; registry
   garbage collection later reclaims unreferenced blob space.

Task completion alone does not prove checkpointing because `triggerAndWait()` also works
without a checkpoint backend. The suspend/remove/restore sequence and preserved marker are
the proof.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `CHECKPOINT_STORAGE_DRIVER` | `local` | `local` for single-host testing or `s3` for a fleet |
| `CHECKPOINT_ROOTFS_MODE` | `registry` | `registry` captures runtime filesystem changes; `none` is unsafe outside constrained tests |
| `CHECKPOINT_MAX_CONCURRENT_JOBS` | `1` | Bounds CPU, disk, and network bursts per worker |
| `CHECKPOINT_REQUIRE_KERNEL_MATCH` | `false` | Enforce exact source/destination kernel string |
| `CHECKPOINT_CONTROL_ALLOWED_HOST` | `supervisor` | Only this resolved container address may call mutating routes |
| `CHECKPOINT_REAPER_GRACE_SECONDS` | `600` | Age before stopped `runner-*` containers are removed |

The companion stack runs two small management containers: the Node service and a dedicated
Docker socket proxy. The checkpoint service runs as root because dockerd/runc creates CRIU
output as root with mode `0700`; its Docker access is already root-equivalent and the isolated
proxy remains the API boundary. Their idle footprint is modest, but checkpoint/commit,
compression/upload, and restore cause burst CPU, RAM, disk, and network load. Suspends and
restores share one queue whose concurrency defaults to one operation per host.

See `deploy/worker/orc-a1cpu1ram6/.env.example` in the deployment repo
for the worker and storage settings. Registry and Trigger.dev credentials are reused from the
normal worker configuration.

## Security and limitations

- CRIU images contain task memory, including credentials and payload data. Use private
  networking, TLS to object storage, encryption at rest, and narrowly scoped credentials.
- Docker access is root-equivalent. The service reaches Docker through its own isolated
  socket proxy; task runners cannot join that proxy network. Control routes also verify that
  the caller's source IP resolves to `supervisor`;
  `CHECKPOINT_ALLOW_UNTRUSTED_CONTROL=true` exists only for isolated development tests.
- Cross-host restore depends on compatible CPU architecture, kernel features, CRIU, runc,
  Docker, seccomp, mounts, and network behavior. This implementation always checks OS and
  architecture; exact kernel checking is optional.
- CRIU is configured to restore established TCP sockets closed so normal clients can
  reconnect. Libraries that do not reconnect cleanly may fail after resume.
- Registry manifest deletion is attempted when Trigger.dev reclaims a run. The registry
  still needs periodic offline garbage collection to reclaim unreferenced blob space. This
  repository does not schedule registry GC; add a manager maintenance job only after choosing
  a safe read-only/offline window for the registry.
- A service crash after Docker stops a runner but before the webapp callback is a difficult
  failure boundary. Jobs are persisted locally and recovered after restart, and the service
  attempts to restart the source container when a checkpoint job fails, but this is not a
  substitute for extensive failure testing.

## Development

```bash
cd services/checkpoint
npm install
npm test
npm run check
docker build -t trigger-checkpoint-service:test .
```
