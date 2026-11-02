import type { SqlOperations } from "./sql-operations";

// Install the handler before loading the SDK: its top-level WASM initialization
// is asynchronous, and requests can arrive while it is still loading.
// SQL operations and WASM startup both run outside the browser's UI thread.
const ready = import("./sql-operations");
self.onmessage = async (event: MessageEvent<{ id: number; method: keyof SqlOperations; args: unknown[] }>) => {
  const { id, method, args } = event.data;
  try {
    const { operations } = await ready;
    const operation = operations[method] as (...args: unknown[]) => unknown;
    self.postMessage({ id, result: operation(...args) });
  } catch (error) {
    self.postMessage({ id, error: String(error) });
  }
};
