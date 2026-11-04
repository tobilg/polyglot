import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { version } from '../package.json';

const { init, getVersion } = vi.hoisted(() => ({ init: vi.fn(), getVersion: vi.fn() }));
vi.mock('@polyglot-sql/sdk/manual', () => ({ init, getVersion }));
import { initializeSql } from '../../playground/src/lib/sql-wasm';

const jsDelivr = `https://cdn.jsdelivr.net/npm/@polyglot-sql/sdk@${version}/dist/polyglot_sql.wasm`;
const unpkg = `https://unpkg.com/@polyglot-sql/sdk@${version}/dist/polyglot_sql.wasm`;

beforeEach(() => {
  vi.stubEnv('DEV', false);
  init.mockReset().mockResolvedValue(undefined);
  getVersion.mockReset().mockReturnValue(version);
});
afterEach(() => { vi.unstubAllEnvs(); vi.unstubAllGlobals(); });

it('loads the exact SDK release from jsDelivr', async () => {
  await initializeSql();
  expect(init.mock.calls).toEqual([[{ wasmUrl: jsDelivr }]]);
  expect(getVersion).toHaveBeenCalledOnce();
});

it('falls back to the same release on UNPKG after a download failure', async () => {
  init.mockRejectedValueOnce(new Error('Network error'));
  await initializeSql();
  expect(init.mock.calls).toEqual([[{ wasmUrl: jsDelivr }], [{ wasmUrl: unpkg }]]);
});

it('reports failure if both CDNs are unavailable', async () => {
  init.mockRejectedValue(new Error('Network error'));
  await expect(initializeSql()).rejects.toThrow('Could not load the SQL engine');
  expect(init).toHaveBeenCalledTimes(2);
  expect(getVersion).not.toHaveBeenCalled();
});

it('rejects a mismatched WASM version before SQL operations can run', async () => {
  getVersion.mockReturnValue('0.0.0');
  await expect(initializeSql()).rejects.toThrow(`expected ${version}, received 0.0.0`);
  expect(init).toHaveBeenCalledOnce();
});

it('uses the local WASM build during development', async () => {
  vi.stubEnv('DEV', true);
  vi.stubGlobal('__POLYGLOT_LOCAL_WASM_URL__', '/@fs/local/sdk/dist/polyglot_sql.wasm');
  await initializeSql();
  expect(init.mock.calls).toEqual([[{ wasmUrl: '/@fs/local/sdk/dist/polyglot_sql.wasm' }]]);
});
