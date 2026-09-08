import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";

const server = createServer(
  {
    requestTimeout: 1500,
    headersTimeout: 1000,
    connectionsCheckingInterval: 50,
    keepAliveTimeout: 5000,
  },
  (_request, response) => setTimeout(() => response.end("held"), 3000),
);
server.listen(0, "127.0.0.1");
await once(server, "listening");
try {
  const start = performance.now();
  const response = await fetch("http://127.0.0.1:" + server.address().port);
  assert.equal(await response.text(), "held");
  assert.equal(response.status, 200);
  assert(performance.now() - start >= 2900);
  console.log(
    JSON.stringify({
      ok: true,
      runtime: process.version,
      heldMs: Math.round(performance.now() - start),
    }),
  );
} finally {
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
}
