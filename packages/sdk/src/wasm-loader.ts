/**
 * WASM Loader
 *
 * Central module for loading the WebAssembly bindings via dynamic import.
 * This avoids top-level await, enabling CJS output compatibility.
 * All other modules must use getWasmSync() or loadWasm() from here.
 *
 * - ESM: uses static path so bundler can create a chunk
 * - CJS: uses __dirname to resolve polyglot_sql_wasm.js in dist/
 */

type WasmModule = typeof import('../wasm/polyglot_sql_wasm.js');

let cached: WasmModule | null = null;

/**
 * Load the WASM module (async). Resolves when ready.
 * Caches the result for subsequent calls.
 */
export async function loadWasm(): Promise<WasmModule> {
  if (cached) return cached as WasmModule;
  if (typeof __dirname !== 'undefined') {
    // CJS: resolve to polyglot_sql_wasm.js
    // In dist/: same dir as index.cjs. In test/source: ../wasm/ relative to src/
    const path = require('node:path') as typeof import('node:path');
    const url = require('node:url') as typeof import('node:url');
    const fs = require('node:fs') as typeof import('node:fs');
    let resolved = path.join(__dirname, 'polyglot_sql_wasm.js');
    try {
      fs.accessSync(resolved);
    } catch {
      resolved = path.join(__dirname, '../wasm/polyglot_sql_wasm.js');
    }
    const wasmPath = url.pathToFileURL(resolved).href;
    cached = await import(/* @vite-ignore */ wasmPath);
  } else {
    // ESM: static path for bundler resolution
    cached = await import('../wasm/polyglot_sql_wasm.js');
  }
  return cached as WasmModule;
}

/**
 * Get the loaded WASM module synchronously.
 * Throws if init() has not been called yet.
 */
export function getWasmSync(): WasmModule {
  if (!cached) {
    throw new Error(
      'Polyglot SDK not initialized. Call await init() before using the SDK.',
    );
  }
  return cached;
}

/** Check if the WASM module has been loaded. */
export function isWasmLoaded(): boolean {
  return cached !== null;
}
