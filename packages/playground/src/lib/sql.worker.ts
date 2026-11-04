import type { SqlOperations } from "./sql-operations";

// Install the handler before loading the SDK and its version-pinned WASM.
// Requests can arrive while the CDN download is still in progress.
// SQL operations and WASM startup both run outside the browser's UI thread.
const ready = import("./sql-operations").then(async (sdk) => {
  await sdk.initializeSql();
  return sdk;
});
// Report startup failures through request replies, even if loading fails
// before the first request arrives.
void ready.catch(() => {});
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
