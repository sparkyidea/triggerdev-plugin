export type DispatchOutcome = "matched" | "miss" | "write_failed" | "ambiguous";
export type Rejection =
  | "waiting_cap"
  | "session_cap"
  | "duplicate"
  | "identity_changed"
  | "disabled"
  | "draining"
  | "invalid";
const bounds = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5, 10];

export interface Gauges {
  waiting: number;
  claimed: number;
  sessions: number;
  deployments: number;
  ready: boolean;
}

export class Metrics {
  readonly dispatches: Record<DispatchOutcome, number> = {
    matched: 0,
    miss: 0,
    write_failed: 0,
    ambiguous: 0,
  };
  readonly rejections: Record<Rejection, number> = {
    waiting_cap: 0,
    session_cap: 0,
    duplicate: 0,
    identity_changed: 0,
    disabled: 0,
    draining: 0,
    invalid: 0,
  };
  disconnects = 0;
  expirations = 0;
  nearDeadlineClaims = 0;
  bodyRejections = 0;
  private buckets = bounds.map(() => 0);
  private count = 0;
  private sum = 0;

  observeDispatch(outcome: DispatchOutcome, seconds: number): void {
    this.dispatches[outcome]++;
    this.count++;
    this.sum += seconds;
    bounds.forEach((bound, index) => {
      if (seconds <= bound) this.buckets[index]!++;
    });
  }

  render(gauges: Gauges): string {
    const lines: string[] = [];
    function metric(
      name: string,
      help: string,
      type: string,
      samples: string[],
    ) {
      const fullName = `warm_start_${name}`;
      lines.push(
        `# HELP ${fullName} ${help}`,
        `# TYPE ${fullName} ${type}`,
        ...samples.map((s) => fullName + s),
      );
    }
    for (const [name, value] of Object.entries(gauges)) {
      metric(name, `Current ${name}.`, "gauge", [` ${Number(value)}`]);
    }
    metric(
      "dispatches_total",
      "Dispatch outcomes; matched does not confirm execution.",
      "counter",
      Object.entries(this.dispatches).map(
        ([outcome, value]) => `{outcome="${outcome}"} ${value}`,
      ),
    );
    metric(
      "registration_rejections_total",
      "Rejected runner registrations.",
      "counter",
      Object.entries(this.rejections).map(
        ([reason, value]) => `{reason="${reason}"} ${value}`,
      ),
    );
    for (const [name, value] of [
      ["poll_disconnects_total", this.disconnects],
      ["idle_expirations_total", this.expirations],
      ["near_poll_deadline_claims_total", this.nearDeadlineClaims],
      ["body_rejections_total", this.bodyRejections],
    ] as const)
      metric(name, name.replaceAll("_", " ") + ".", "counter", [` ${value}`]);
    metric(
      "dispatch_duration_seconds",
      "Time handling valid dispatch messages.",
      "histogram",
      [
        ...bounds.map(
          (bound, index) => `_bucket{le="${bound}"} ${this.buckets[index]}`,
        ),
        `_bucket{le="+Inf"} ${this.count}`,
        `_sum ${this.sum}`,
        `_count ${this.count}`,
      ],
    );
    return lines.join("\n") + "\n";
  }
}
