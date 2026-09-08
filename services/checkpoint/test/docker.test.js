import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import {
  buildRestoreContainerSpec,
  containerIpAddresses,
  DockerClient,
  registryAuthHeader,
} from "../src/docker.js";

test("default fetch and dispatcher can call a real Docker HTTP endpoint", { timeout: 5000 }, async () => {
  const server = http.createServer((request, response) => {
    if (request.url === "/_ping") {
      response.end("OK");
    } else {
      response.writeHead(404);
      response.end();
    }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  let client;
  try {
    client = new DockerClient({ baseUrl: `http://127.0.0.1:${server.address().port}` });
    const response = await client.ping();
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "OK");
  } finally {
    await client?.dispatcher?.close();
    await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test("passes the configured dispatcher to slow Docker API requests", async () => {
  const dispatcher = {};
  let request;
  const client = new DockerClient({
    baseUrl: "http://docker.test",
    dispatcher,
    fetchImpl: async (_url, init) => {
      request = init;
      return new Response(JSON.stringify({ Id: "image" }), {
        status: 201,
        headers: { "Content-Type": "application/json" },
      });
    },
  });

  await client.commitContainer("runner-1", "registry.test/checkpoint:one");
  assert.equal(request.dispatcher, dispatcher);
});

test("buildRestoreContainerSpec preserves runner settings but disables auto-remove", () => {
  const result = buildRestoreContainerSpec(
    {
      Config: {
        Image: "registry/tasks:v1",
        Env: ["A=1"],
        Cmd: ["node", "app.js"],
        Labels: { original: "yes" },
      },
      HostConfig: {
        AutoRemove: true,
        NetworkMode: "supervisor",
        Memory: 536870912,
        NanoCpus: 500000000,
      },
      NetworkSettings: {
        Networks: {
          supervisor: {
            Aliases: ["runner-old"],
            IPAddress: "172.20.0.5",
          },
        },
      },
    },
    "registry/checkpoints:abc",
    { restored: "yes" }
  );

  assert.equal(result.Image, "registry/checkpoints:abc");
  assert.equal(result.HostConfig.AutoRemove, false);
  assert.equal(result.HostConfig.Memory, 536870912);
  assert.deepEqual(result.Labels, { original: "yes", restored: "yes" });
  assert.deepEqual(result.NetworkingConfig.EndpointsConfig.supervisor.Aliases, []);
  assert.equal(result.NetworkingConfig.EndpointsConfig.supervisor.IPAddress, undefined);
});

test("extracts normalized container addresses", () => {
  assert.deepEqual(
    containerIpAddresses({
      NetworkSettings: {
        Networks: {
          one: { IPAddress: "172.20.0.9", GlobalIPv6Address: "" },
          two: { IPAddress: "", GlobalIPv6Address: "::ffff:10.0.0.4" },
        },
      },
    }),
    ["172.20.0.9", "10.0.0.4"]
  );
});

test("encodes Docker registry auth", () => {
  const value = registryAuthHeader({ username: "u", password: "p", serverAddress: "r.test" });
  assert.deepEqual(JSON.parse(Buffer.from(value, "base64url").toString("utf8")), {
    username: "u",
    password: "p",
    serveraddress: "r.test",
  });
});

test("registry auth retains Go-compatible padding for every payload length", () => {
  for (const password of ["p", "pp", "ppp", "secret-🔑"]) {
    const auth = { username: "u", password, serverAddress: "registry.test" };
    const value = registryAuthHeader(auth);
    assert.equal(value.length % 4, 0);
    assert.match(value, /^[A-Za-z0-9_-]+={0,2}$/);
    assert.equal(value, Buffer.from(JSON.stringify({
      username: auth.username, password, serveraddress: auth.serverAddress,
    })).toString("base64").replace(/\+/g, "-").replace(/\//g, "_"));
  }
});

test("default-directory restore never sends checkpoint-dir, even as undefined", async () => {
  const urls = [];
  const client = new DockerClient({
    baseUrl: "http://docker.test",
    fetchImpl: async (url) => {
      urls.push(url);
      return new Response(null, { status: 204 });
    },
  });
  await client.startContainer("runner", { checkpointId: "checkpoint-1" });
  await client.startContainer("runner");
  assert.deepEqual(urls, [
    "http://docker.test/containers/runner/start?checkpoint=checkpoint-1",
    "http://docker.test/containers/runner/start",
  ]);
});

test("image cleanup preserves conflicts for retry and never forces removal", async () => {
  let url;
  const client = new DockerClient({ baseUrl: "http://docker.test", fetchImpl: async (value) => {
    url = value; return new Response("in use", { status: 409 });
  } });
  await assert.rejects(client.removeImage("registry.test/checkpoints:tag"), { status: 409 });
  assert.match(url, /force=0$/);
});
