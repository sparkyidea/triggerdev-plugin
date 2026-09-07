import { Writable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { Agent } from "undici";

const DOCKER_HEADERS_TIMEOUT_MS = 30 * 60 * 1000;

export class DockerError extends Error {
  constructor(message, { status, body } = {}) {
    super(message);
    this.name = "DockerError";
    this.status = status;
    this.body = body;
  }
}

export class DockerClient {
  constructor({ baseUrl, apiVersion = "", fetchImpl = fetch, dispatcher }) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.apiPrefix = apiVersion ? `/v${apiVersion.replace(/^v/, "")}` : "";
    this.fetch = fetchImpl;
    this.dispatcher =
      dispatcher ||
      (fetchImpl === fetch
        ? new Agent({ headersTimeout: DOCKER_HEADERS_TIMEOUT_MS, bodyTimeout: 0 })
        : undefined);
  }

  async request(pathname, { method = "GET", body, headers = {}, allow = [] } = {}) {
    const response = await this.fetch(`${this.baseUrl}${this.apiPrefix}${pathname}`, {
      method,
      headers: {
        ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        ...headers,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      ...(this.dispatcher ? { dispatcher: this.dispatcher } : {}),
    });

    if (!response.ok && !allow.includes(response.status)) {
      const text = await response.text();
      throw new DockerError(`Docker ${method} ${pathname} failed with ${response.status}`, {
        status: response.status,
        body: text,
      });
    }
    return response;
  }

  async json(pathname, options) {
    const response = await this.request(pathname, options);
    return response.status === 204 ? undefined : response.json();
  }

  ping() {
    return this.request("/_ping");
  }

  version() {
    return this.json("/version");
  }

  info() {
    return this.json("/info");
  }

  inspectContainer(id) {
    return this.json(`/containers/${encodeURIComponent(id)}/json`);
  }

  async inspectContainerOrNull(id) {
    const response = await this.request(`/containers/${encodeURIComponent(id)}/json`, {
      allow: [404],
    });
    return response.status === 404 ? null : response.json();
  }

  async checkpointContainer(id, checkpointId, checkpointDir) {
    await this.request(`/containers/${encodeURIComponent(id)}/checkpoints`, {
      method: "POST",
      body: { CheckpointID: checkpointId, CheckpointDir: checkpointDir, Exit: true },
    });
  }

  async createContainer(name, spec) {
    return this.json(`/containers/create?name=${encodeURIComponent(name)}`, {
      method: "POST",
      body: spec,
    });
  }

  async startContainer(id, { checkpointId, checkpointDir } = {}) {
    const query = checkpointId
      ? `?checkpoint=${encodeURIComponent(checkpointId)}&checkpoint-dir=${encodeURIComponent(checkpointDir)}`
      : "";
    await this.request(`/containers/${encodeURIComponent(id)}/start${query}`, { method: "POST" });
  }

  async removeContainer(id, force = false) {
    await this.request(`/containers/${encodeURIComponent(id)}?force=${force ? "1" : "0"}&v=1`, {
      method: "DELETE",
      allow: [404],
    });
  }

  async commitContainer(containerId, imageRef) {
    const { repository, tag } = splitImageRef(imageRef);
    return this.json(
      `/commit?container=${encodeURIComponent(containerId)}&repo=${encodeURIComponent(repository)}&tag=${encodeURIComponent(tag)}&pause=0`,
      { method: "POST", body: {} }
    );
  }

  async inspectImageOrNull(imageRef) {
    const response = await this.request(`/images/${encodeURIComponent(imageRef)}/json`, {
      allow: [404],
    });
    return response.status === 404 ? null : response.json();
  }

  async pullImage(imageRef, registryAuth) {
    const response = await this.request(`/images/create?fromImage=${encodeURIComponent(imageRef)}`, {
      method: "POST",
      headers: registryAuth ? { "X-Registry-Auth": registryAuth } : {},
    });
    await assertDockerStream(response, "pull");
  }

  async pushImage(imageRef, registryAuth) {
    const response = await this.request(`/images/${encodeURIComponent(imageRef)}/push`, {
      method: "POST",
      headers: registryAuth ? { "X-Registry-Auth": registryAuth } : {},
    });
    await assertDockerStream(response, "push");
  }

  async removeImage(imageRef) {
    await this.request(`/images/${encodeURIComponent(imageRef)}?force=1`, {
      method: "DELETE",
      allow: [404, 409],
    });
  }

  async listExitedRunnerContainers() {
    const filters = encodeURIComponent(
      JSON.stringify({ name: ["runner-"], status: ["exited", "dead"] })
    );
    return this.json(`/containers/json?all=1&filters=${filters}`);
  }
}

export function registryAuthHeader({ username, password, serverAddress }) {
  if (!username && !password && !serverAddress) return undefined;
  return Buffer.from(
    JSON.stringify({ username, password, serveraddress: serverAddress }),
    "utf8"
  ).toString("base64url");
}

export function buildRestoreContainerSpec(inspect, imageRef, labels = {}) {
  const config = pick(inspect.Config || {}, [
    "Hostname",
    "Domainname",
    "User",
    "AttachStdin",
    "AttachStdout",
    "AttachStderr",
    "ExposedPorts",
    "Tty",
    "OpenStdin",
    "StdinOnce",
    "Env",
    "Cmd",
    "Healthcheck",
    "ArgsEscaped",
    "Image",
    "Volumes",
    "WorkingDir",
    "Entrypoint",
    "NetworkDisabled",
    "MacAddress",
    "OnBuild",
    "StopSignal",
    "StopTimeout",
    "Shell",
  ]);
  config.Image = imageRef;
  config.Labels = { ...(inspect.Config?.Labels || {}), ...labels };

  const hostConfig = pick(inspect.HostConfig || {}, [
    "Binds",
    "ContainerIDFile",
    "LogConfig",
    "NetworkMode",
    "PortBindings",
    "RestartPolicy",
    "VolumeDriver",
    "VolumesFrom",
    "CapAdd",
    "CapDrop",
    "CgroupnsMode",
    "Dns",
    "DnsOptions",
    "DnsSearch",
    "ExtraHosts",
    "GroupAdd",
    "IpcMode",
    "Cgroup",
    "Links",
    "OomScoreAdj",
    "PidMode",
    "Privileged",
    "PublishAllPorts",
    "ReadonlyRootfs",
    "SecurityOpt",
    "StorageOpt",
    "Tmpfs",
    "UTSMode",
    "UsernsMode",
    "ShmSize",
    "Sysctls",
    "Runtime",
    "ConsoleSize",
    "Isolation",
    "CpuShares",
    "Memory",
    "NanoCpus",
    "CgroupParent",
    "BlkioWeight",
    "CpuPeriod",
    "CpuQuota",
    "CpuRealtimePeriod",
    "CpuRealtimeRuntime",
    "CpusetCpus",
    "CpusetMems",
    "Devices",
    "DeviceCgroupRules",
    "DeviceRequests",
    "MemoryReservation",
    "MemorySwap",
    "MemorySwappiness",
    "OomKillDisable",
    "PidsLimit",
    "Ulimits",
    "CpuCount",
    "CpuPercent",
    "IOMaximumIOps",
    "IOMaximumBandwidth",
    "MaskedPaths",
    "ReadonlyPaths",
    "Mounts",
    "Init",
  ]);
  // Checkpointable runners must survive an Exit=true dump. The reaper removes
  // ordinary completed runners after a grace period.
  hostConfig.AutoRemove = false;

  const endpoints = {};
  for (const [networkName, endpoint] of Object.entries(inspect.NetworkSettings?.Networks || {})) {
    endpoints[networkName] = {
      // Docker automatically adds the new container name. Carrying the old
      // runner alias across a same-host restore can collide with the stopped source.
      Aliases: (endpoint.Aliases || []).filter(
        (alias) => alias && !alias.startsWith("runner-") && alias !== inspect.Id
      ),
      Links: endpoint.Links || undefined,
      DriverOpts: endpoint.DriverOpts || undefined,
    };
  }

  return {
    ...config,
    HostConfig: hostConfig,
    NetworkingConfig: { EndpointsConfig: endpoints },
  };
}

export function containerIpAddresses(inspect) {
  return Object.values(inspect.NetworkSettings?.Networks || {})
    .flatMap((network) => [network.IPAddress, network.GlobalIPv6Address])
    .filter(Boolean)
    .map(normalizeIp);
}

export function normalizeIp(value = "") {
  const withoutZone = value.split("%")[0];
  return withoutZone.startsWith("::ffff:") ? withoutZone.slice(7) : withoutZone;
}

function pick(source, fields) {
  return Object.fromEntries(
    fields.filter((field) => source[field] !== undefined && source[field] !== null).map((field) => [field, source[field]])
  );
}

function splitImageRef(imageRef) {
  const lastSlash = imageRef.lastIndexOf("/");
  const lastColon = imageRef.lastIndexOf(":");
  if (lastColon <= lastSlash) return { repository: imageRef, tag: "latest" };
  return { repository: imageRef.slice(0, lastColon), tag: imageRef.slice(lastColon + 1) };
}

async function assertDockerStream(response, operation) {
  if (!response.body) return;
  let text = "";
  const sink = new WritableText((chunk) => {
    text += chunk;
    for (const line of text.split("\n")) {
      if (!line.trim()) continue;
      try {
        const event = JSON.parse(line);
        if (event.error) throw new DockerError(`Docker image ${operation} failed`, { body: event.error });
      } catch (error) {
        if (error instanceof DockerError) throw error;
      }
    }
    text = text.slice(text.lastIndexOf("\n") + 1);
  });
  await pipeline(response.body, sink);
}

class WritableText extends Writable {
  constructor(onChunk) {
    super({
      write(chunk, _encoding, callback) {
        try {
          onChunk(chunk.toString("utf8"));
          callback();
        } catch (error) {
          callback(error);
        }
      },
    });
  }
}
