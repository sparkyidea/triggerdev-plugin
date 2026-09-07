import assert from "node:assert/strict";
import test from "node:test";
import { parseRegistryImage, RegistryClient } from "../src/registry.js";

test("parses a checkpoint repository and tag", () => {
  assert.deepEqual(
    parseRegistryImage(
      "https://registry.example.com",
      "registry.example.com/trigger-checkpoints:abc123"
    ),
    { repository: "trigger-checkpoints", reference: "abc123" }
  );
});

test("deletes a manifest by digest", async () => {
  const requests = [];
  const client = new RegistryClient({
    apiUrl: "https://registry.example.com",
    username: "user",
    password: "password",
    fetchImpl: async (url, init) => {
      requests.push({ url, init });
      if (init.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: { "Docker-Content-Digest": "sha256:deadbeef" },
        });
      }
      return new Response(null, { status: 202 });
    },
  });
  assert.equal(
    await client.deleteImage("registry.example.com/trigger-checkpoints:abc123"),
    true
  );
  assert.equal(requests.length, 2);
  assert.match(requests[1].url, /manifests\/sha256%3Adeadbeef$/);
  assert.match(requests[0].init.headers.Authorization, /^Basic /);
});
