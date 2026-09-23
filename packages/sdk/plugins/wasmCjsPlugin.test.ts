import { join } from 'node:path';
import { runInNewContext } from 'node:vm';
import { describe, expect, it, vi } from 'vitest';
import { wasmCjsPlugin } from './wasmCjsPlugin';

describe('CJS WASM startup', () => {
  it.each([
    true,
    false,
  ])('defers initialization and runs optional startup once (startup=%s)', async (startup) => {
    const chunk = {
      type: 'chunk',
      code: `
let wasm;
function __wbg_set_wasm(value) { wasm = value; }
const __vite__wasmUrl = new URL("./polyglot_sql.wasm", import.meta.url).href;
const __vite__wasmModule = await __vite__initWasm({}, __vite__wasmUrl);
const __wbindgen_start = __vite__wasmModule.__wbindgen_start;
__wbg_set_wasm(wasm$2);
${startup ? '__wbindgen_start();' : ''}
async function init() { return Promise.resolve(); }
function isInitialized() { return true; }
export { init, isInitialized };
`,
    };
    const hook = wasmCjsPlugin().generateBundle;
    if (typeof hook !== 'function')
      throw new Error('Expected generateBundle hook');
    Reflect.apply(hook, {}, [{}, { 'index.cjs': chunk }, false]);

    const start = vi.fn();
    const instantiate = vi.fn(async () => ({
      instance: { exports: { __wbindgen_start: start } },
    }));
    const readFileSync = vi.fn(() => new Uint8Array());
    const module = {
      exports: {} as { init(): Promise<void>; isInitialized(): boolean },
    };
    runInNewContext(chunk.code, {
      module,
      exports: module.exports,
      __dirname: '/virtual',
      require: (name: string) =>
        name === 'path' ? { join } : { readFileSync },
      WebAssembly: { instantiate },
    });

    expect(module.exports.isInitialized()).toBe(false);
    expect(readFileSync).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
    await Promise.all([module.exports.init(), module.exports.init()]);
    await module.exports.init();
    expect(module.exports.isInitialized()).toBe(true);
    expect(instantiate).toHaveBeenCalledTimes(1);
    expect(start).toHaveBeenCalledTimes(startup ? 1 : 0);
  });
});
