FROM node:22.23.1-bookworm-slim AS dependencies

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

FROM node:22.23.1-bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates tar gzip curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV NODE_ENV=production

COPY --from=dependencies /app/node_modules ./node_modules
COPY package.json ./
COPY src ./src
COPY scripts/smoke-service.mjs ./scripts/smoke-service.mjs

# dockerd/runc creates CRIU output below CHECKPOINT_ROOT as root with mode 0700.
# Docker API access is already root-equivalent and is constrained by the proxy.
USER root
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
  CMD curl --fail --silent --show-error --max-time 4 --output /dev/null http://127.0.0.1:8080/health

CMD ["node", "src/index.js"]
