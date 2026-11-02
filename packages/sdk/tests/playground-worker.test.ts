import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

class FakeWorker {
  static instances: FakeWorker[] = [];
  requests: Array<{ id: number; method: string; args: unknown[] }> = [];
  onmessage?: (event: { data: unknown }) => void;
  onerror?: (event: { message: string }) => void;
  onmessageerror?: () => void;
  terminated = false;
  constructor() { FakeWorker.instances.push(this); }
  postMessage(request: { id: number; method: string; args: unknown[] }) { this.requests.push(request); }
  terminate() { this.terminated = true; }
}

beforeEach(() => {
  vi.resetModules();
  FakeWorker.instances = [];
  vi.stubGlobal('Worker', FakeWorker);
});
afterEach(() => vi.unstubAllGlobals());

describe('playground worker client', () => {
  it('matches concurrent replies by request ID', async () => {
    const { runSql } = await import('../../playground/src/lib/sql-client');
    const first = runSql('parse', 'SELECT 1');
    const second = runSql('parse', 'SELECT 2');
    const worker = FakeWorker.instances[0];
    worker.onmessage!({ data: { id: worker.requests[1].id, result: { success: true, ast: [2] } } });
    worker.onmessage!({ data: { id: worker.requests[0].id, result: { success: true, ast: [1] } } });
    expect(await first).toEqual({ success: true, ast: [1] });
    expect(await second).toEqual({ success: true, ast: [2] });
    expect(FakeWorker.instances).toHaveLength(1);
  });

  it('rejects pending work on worker failure and allows a fresh worker', async () => {
    const { runSql } = await import('../../playground/src/lib/sql-client');
    const failure = expect(runSql('metadata')).rejects.toThrow('load failed');
    const worker = FakeWorker.instances[0];
    worker.onerror!({ message: 'load failed' });
    await failure;
    expect(worker.terminated).toBe(true);
    const retry = runSql('metadata');
    const next = FakeWorker.instances[1];
    worker.onmessageerror!();
    expect(next.terminated).toBe(false);
    next.onmessage!({ data: { id: next.requests[0].id, result: { version: 'test', dialects: [] } } });
    expect(await retry).toEqual({ version: 'test', dialects: [] });
  });
});
