type Fields = Record<string, string | number | boolean | undefined>;

// Callers pass explicit safe fields, never request objects, raw errors, or headers.
export class Logger {
  private readonly debugEnabled: boolean;

  constructor(debugEnabled: boolean) {
    this.debugEnabled = debugEnabled;
  }

  info(event: string, fields: Fields = {}): void {
    process.stdout.write(
      JSON.stringify({
        time: new Date().toISOString(),
        level: "info",
        event,
        ...fields,
      }) + "\n",
    );
  }

  debug(event: string, fields: Fields = {}): void {
    if (this.debugEnabled) this.info(event, fields);
  }
}
