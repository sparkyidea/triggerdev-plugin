export function log(level, message, properties = {}) {
  const entry = {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...properties,
  };
  const output = JSON.stringify(entry);
  if (level === "error") console.error(output);
  else console.log(output);
}
