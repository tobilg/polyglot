#!/usr/bin/env node
/**
 * Post-build script: extract the base64-inlined WASM from JS bundles,
 * save as a separate .wasm file, and replace the data URL with a file reference.
 *
 * This reduces the JS bundle size by ~93% (base64 WASM is ~98.9% of each bundle)
 * and adds Node.js compatibility (Node's fetch doesn't support file:// URLs).
 *
 * Works with both minified and unminified bundles — matches by data URL pattern,
 * not by specific variable names.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const distDir = resolve(__dirname, '..', 'dist');

const WASM_FILENAME = 'polyglot_sql_wasm_bg.wasm';

/**
 * Extract WASM from a JS bundle file.
 * @param {string} jsPath - Path to the JS bundle
 * @param {string} wasmRelativePath - Relative path from JS file to the .wasm file
 * @param {boolean} extractWasm - Whether to extract and save the .wasm binary
 */
function processBundle(jsPath, wasmRelativePath, extractWasm) {
  let js;
  try {
    js = readFileSync(jsPath, 'utf-8');
  } catch {
    console.log(`  Skipping ${jsPath} (not found)`);
    return;
  }

  // Match the base64 WASM data URL — works regardless of variable name
  const dataUrlRegex = /(=\s*)"data:application\/wasm;base64,([A-Za-z0-9+/=]+)"/;
  const match = js.match(dataUrlRegex);
  if (!match) {
    console.log(`  Skipping ${jsPath} (no base64 WASM found)`);
    return;
  }

  if (extractWasm) {
    const wasmBytes = Buffer.from(match[2], 'base64');
    const wasmPath = resolve(distDir, WASM_FILENAME);
    writeFileSync(wasmPath, wasmBytes);
    console.log(`  Extracted WASM: ${(wasmBytes.length / 1024 / 1024).toFixed(1)} MB → ${wasmPath}`);
  }

  const jsSizeBefore = js.length;

  // Replace the base64 data URL with a URL reference to the .wasm file
  js = js.replace(
    dataUrlRegex,
    `$1new URL("${wasmRelativePath}",import.meta.url).href`
  );

  // Replace the vite-plugin-wasm init function with a Node.js-compatible version.
  //
  // The init function is an async arrow: `async (opts = {}, url) => { ... }`
  // assigned to a variable. It contains WebAssembly.instantiate calls.
  //
  // We find it by locating the `await __vite__initWasm(` call (unminified) or
  // by matching the structure. We replace the entire function body.
  //
  // Strategy: find the data URL variable name, then find the init function
  // that references it, and replace the init function.

  // Find the init function call site to get the init variable name.
  // Pattern: `await <initFnName>({...}, <wasmUrlVar>)` near the end of the file.
  // The wasmUrlVar is what we just replaced the data URL in.

  // Instead of complex regex, replace the fetch() call inside the init function
  // with Node.js-compatible code. The init function has a unique structure:
  // it calls `fetch(url)` inside an `else` block after checking for data: URLs.
  //
  // In unminified code:
  //   const response = await fetch(url);
  //   ...
  //   result = await WebAssembly.instantiateStreaming(response, opts);
  //
  // In minified code:
  //   const C = await fetch(A);
  //   ...
  //   B = await WebAssembly.instantiateStreaming(C, g);

  // Approach: wrap the entire bundle with a fetch polyfill for file:// URLs in Node.js.
  // This is simpler and more robust than trying to match the init function structure.
  const nodeCompat = [
    '(()=>{if(typeof globalThis.process<"u"&&globalThis.process.versions?.node){',
    'const _f=globalThis.fetch;',
    'globalThis.fetch=async function(u,...a){',
    'if(typeof u==="string"&&u.startsWith("file:")||u instanceof URL&&u.protocol==="file:"){',
    'const{readFileSync:r}=await import("node:fs");',
    'const{fileURLToPath:p}=await import("node:url");',
    'const b=r(p(typeof u==="string"?u:u.href));',
    'return new Response(b,{headers:{"Content-Type":"application/wasm"}})',
    '}return _f(u,...a)',
    '}}})();',
  ].join('');

  // Only add the compat wrapper if it's not already present
  if (!js.includes('globalThis.process.versions?.node')) {
    js = nodeCompat + '\n' + js;
  }

  writeFileSync(jsPath, js);
  const jsSizeAfter = js.length;
  console.log(
    `  Updated ${jsPath}: ${(jsSizeBefore / 1024 / 1024).toFixed(1)} MB → ${(jsSizeAfter / 1024).toFixed(0)} KB`
  );
}

// Main
console.log('Extracting WASM from bundles...');

// ESM bundle: extract WASM and replace data URL
processBundle(resolve(distDir, 'index.js'), `./${WASM_FILENAME}`, true);

// CDN bundle: replace data URL (WASM already extracted above)
processBundle(resolve(distDir, 'cdn', 'polyglot.esm.js'), `../${WASM_FILENAME}`, false);

console.log('Done.');
