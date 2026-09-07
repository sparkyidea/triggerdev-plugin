import assert from "node:assert/strict";
import test from "node:test";
import {
  buildRestoreContainerSpec,
  containerIpAddresses,
  DockerClient,
  registryAuthHeader,
} from "../src/docker.js";

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
