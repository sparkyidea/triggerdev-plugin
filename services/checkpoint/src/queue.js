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
