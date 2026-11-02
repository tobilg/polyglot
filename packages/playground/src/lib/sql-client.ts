import type { SqlOperations } from "./sql-operations";

let worker: Worker | undefined;
let nextId = 0;
const pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();

function dispose(error: Error) {
  worker?.terminate();
  worker = undefined;
  for (const request of pending.values()) request.reject(error);
  pending.clear();
}

function getWorker(): Worker {
  if (!worker) {
    const created = new Worker(new URL("./sql.worker.ts", import.meta.url), { type: "module" });
    worker = created;
    created.onmessage = (event: MessageEvent<{ id: number; result?: unknown; error?: string }>) => {
      const request = pending.get(event.data.id);
      if (!request) return;
      pending.delete(event.data.id);
      if (event.data.error !== undefined) request.reject(new Error(event.data.error));
      else request.resolve(event.data.result);
    };
    created.onerror = (event) => {
      if (worker === created) dispose(new Error(event.message || "SQL worker failed to load"));
    };
    created.onmessageerror = () => {
      if (worker === created) dispose(new Error("Could not read the SQL worker response"));
    };
  }
  return worker;
}

export function runSql<K extends keyof SqlOperations>(
  method: K, ...args: Parameters<SqlOperations[K]>
): Promise<ReturnType<SqlOperations[K]>> {
  return new Promise((resolve, reject) => {
    const id = nextId++;
    try {
      const target = getWorker();
      pending.set(id, { resolve: (value) => resolve(value as ReturnType<SqlOperations[K]>), reject });
      target.postMessage({ id, method, args });
    } catch (error) {
      pending.delete(id);
      reject(error);
    }
  });
}

if (import.meta.hot) import.meta.hot.dispose(() => dispose(new Error("SQL worker reloaded")));
