export class TaskQueue {
  constructor(concurrency, onError = () => {}) {
    this.concurrency = concurrency;
    this.onError = onError;
    this.running = 0;
    this.pending = [];
  }

  add(task) {
    this.pending.push({ task });
    this.drain();
  }

  run(task) {
    return new Promise((resolve, reject) => {
      this.pending.push({ task, resolve, reject });
      this.drain();
    });
  }

  drain() {
    while (this.running < this.concurrency && this.pending.length > 0) {
      const entry = this.pending.shift();
      this.running += 1;
      Promise.resolve()
        .then(entry.task)
        .then(
          (value) => entry.resolve?.(value),
          (error) => {
            if (entry.reject) entry.reject(error);
            else this.onError(error);
          }
        )
        .finally(() => {
          this.running -= 1;
          this.drain();
        });
    }
  }
}

// Serialize mutations for one run even when the shared queue has concurrency > 1.
// Remove idle keys so the lock itself does not retain completed run identities.
export class KeyedLock {
  constructor() { this.tails = new Map(); }

  async run(key, operation) {
    const previous = this.tails.get(key) || Promise.resolve();
    let release;
    const next = new Promise((resolve) => { release = resolve; });
    this.tails.set(key, next);
    await previous;
    try { return await operation(); }
    finally {
      release();
      if (this.tails.get(key) === next) this.tails.delete(key);
    }
  }
}
