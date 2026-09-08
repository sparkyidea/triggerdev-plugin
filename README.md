# triggerdev-plugin

Self-hosted companion services for [Trigger.dev](https://trigger.dev) v4 deployments.

## Contents

- [`services/checkpoint/`](./services/checkpoint/) — experimental Docker/CRIU checkpoint
  service implementing the supervisor's `TRIGGER_CHECKPOINT_URL` contract, so a parent run
  can release its machine while blocked in `triggerAndWait()`. Built and published by
  [`.github/workflows/checkpoint-image.yml`](./.github/workflows/checkpoint-image.yml) as
  the multi-arch image `ghcr.io/sparkyidea/trigger-checkpoint-service`
  (`latest` from `main`, `dev` from `dev`, plus immutable `sha-<commit>` tags).

- [`services/warmstart/`](./services/warmstart/) — native Node.js warm-start matcher with
  bounded idle sessions, token-compatible deployment identity, and Prometheus metrics.
  Service only; no dashboard. See its README for compatibility, rollout, and validation.

The deployment topology that consumes these images (worker Compose files, host
provisioning, validation tasks) lives in the separate `trigger-worker-swarm` repo.

## Development

```bash
cd services/checkpoint
npm ci
npm test
npm run check
```
