import { afterEach, beforeEach, expect, it, vi } from 'vitest';

beforeEach(() => vi.resetModules());
afterEach(() => {
  vi.unstubAllGlobals();
  vi.doUnmock('../../playground/src/lib/sql-operations');
});

it('queues requests received before WASM initialization finishes', async () => {
  let finish!: (value: unknown) => void;
  const loaded = new Promise(resolve => { finish = resolve; });
  vi.doMock('../../playground/src/lib/sql-operations', () => loaded);
  const worker = { postMessage: vi.fn(), onmessage: undefined as any };
  vi.stubGlobal('self', worker);
  await import('../../playground/src/lib/sql.worker');
  const first = worker.onmessage({ data: { id: 1, method: 'metadata', args: [] } });
  const second = worker.onmessage({ data: { id: 2, method: 'parse', args: ['SELECT 1'] } });
  expect(worker.postMessage).not.toHaveBeenCalled();
  finish({
    initializeSql: vi.fn().mockResolvedValue(undefined),
    operations: { metadata: () => ({ version: 'test' }), parse: (sql: string) => sql },
  });
  await Promise.all([first, second]);
  expect(worker.postMessage.mock.calls).toEqual([
    [{ id: 1, result: { version: 'test' } }],
    [{ id: 2, result: 'SELECT 1' }],
  ]);
});

it('waits for CDN initialization before running queued operations', async () => {
  let finish!: () => void;
  const initializeSql = vi.fn(() => new Promise<void>(resolve => { finish = resolve; }));
  const metadata = vi.fn(() => ({ version: 'test' }));
  vi.doMock('../../playground/src/lib/sql-operations', () => ({ initializeSql, operations: { metadata } }));
  const worker = { postMessage: vi.fn(), onmessage: undefined as any };
  vi.stubGlobal('self', worker);
  await import('../../playground/src/lib/sql.worker');
  const request = worker.onmessage({ data: { id: 4, method: 'metadata', args: [] } });
  await vi.waitFor(() => expect(initializeSql).toHaveBeenCalledOnce());
  expect(metadata).not.toHaveBeenCalled();
  finish();
  await request;
  expect(worker.postMessage).toHaveBeenCalledWith({ id: 4, result: { version: 'test' } });
});

it('reports CDN initialization errors to every queued request', async () => {
  const metadata = vi.fn();
  vi.doMock('../../playground/src/lib/sql-operations', () => ({
    initializeSql: vi.fn().mockRejectedValue(new Error('Both CDN downloads failed')),
    operations: { metadata },
  }));
  const worker = { postMessage: vi.fn(), onmessage: undefined as any };
  vi.stubGlobal('self', worker);
  await import('../../playground/src/lib/sql.worker');
  await Promise.all([1, 2].map(id => worker.onmessage({ data: { id, method: 'metadata', args: [] } })));
  expect(metadata).not.toHaveBeenCalled();
  expect(worker.postMessage.mock.calls).toEqual([
    [{ id: 1, error: 'Error: Both CDN downloads failed' }],
    [{ id: 2, error: 'Error: Both CDN downloads failed' }],
  ]);
});

it('reports initialization failure to queued requests', async () => {
  let fail!: (error: Error) => void;
  const loaded = new Promise((_resolve, reject) => { fail = reject; });
  vi.doMock('../../playground/src/lib/sql-operations', () => loaded);
  const worker = { postMessage: vi.fn(), onmessage: undefined as any };
  vi.stubGlobal('self', worker);
  await import('../../playground/src/lib/sql.worker');
  const request = worker.onmessage({ data: { id: 3, method: 'metadata', args: [] } });
  fail(new Error('WASM load failed'));
  await request;
  // Vitest wraps a rejected module factory in its own Error. The worker must
  // still return an error to the waiting request rather than leaving it pending.
  expect(worker.postMessage).toHaveBeenCalledWith({ id: 3, error: expect.stringContaining('Error:') });
});
