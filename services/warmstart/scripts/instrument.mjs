// Benchmark-only preload, never imported by the production service.
import { monitorEventLoopDelay } from "node:perf_hooks";
const histogram = monitorEventLoopDelay({ resolution: 10 });
histogram.enable();
const report = () => {
  if (!process.connected) return;
  process.send({
    type: "measurement",
    rssBytes: process.memoryUsage().rss,
    cpuMicros: process.cpuUsage(),
    eventLoopP99Ms: histogram.percentile(99) / 1e6,
  });
  histogram.reset();
};
process.on("message", (message) => {
  if (message === "measure") report();
});
// Instrumentation must not keep a gracefully drained service alive.
process.channel?.unref();
