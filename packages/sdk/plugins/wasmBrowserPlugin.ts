import type { Plugin } from 'vite';

/**
 * Vite plugin that transforms the ESM bundle for browser/bundler compatibility.
 * Removes top-level await and uses lazy init so that:
 * - Bundlers (Next.js, webpack, Turbopack) don't get async module interop issues
 * - Dialect, format, transpile, etc. are available synchronously after init()
 *
 * Used with keepDataUrl: true in wasmExtractPlugin so WASM is inlined as base64,
 * avoiding import.meta.url which can break with webpack.
 */
export function wasmBrowserPlugin(): Plugin {
  return {
    name: 'polyglot-wasm-browser',
    apply: 'build',
    generateBundle(_, bundle) {
      for (const item of Object.values(bundle)) {
        if (item.type !== 'chunk') continue;

        let code = item.code;

        // ── Transform 1: Remove NODE_FILE_FETCH_COMPAT shim ──────────
        // Browser build doesn't need file:// URL support
        code = code.replace(
          /^\(\(\)=>\{if\(typeof globalThis\.process<"u".*?\}\}\)\(\);\n?/,
          '',
        );

        // __vite__wasmUrl is either a data URL (with keepDataUrl) or new URL(...)
        // We keep it as-is; no fs/path needed

        // ── Transform 2: Defer WASM initialization ───────────────────
        const initWasmCallRe =
          /const __vite__wasmModule = await __vite__initWasm\((\{[\s\S]*?\})\s*,\s*__vite__wasmUrl\);/;
        const initMatch = code.match(initWasmCallRe);
        if (initMatch) {
          const importsObj = initMatch[1];
          const replacement = [
            'let __vite__wasmModule;',
            `const __vite__wasmImports = ${importsObj};`,
            'let __initPromise;',
            'async function __polyglot_init_wasm() {',
            '  if (__vite__wasmModule) return;',
            '  if (__initPromise) return __initPromise;',
            '  __initPromise = (async () => {',
            '    const result = await __vite__initWasm(__vite__wasmImports, __vite__wasmUrl);',
            '    __vite__wasmModule = result;',
            '    __wbg_set_wasm(__vite__wasmModule);',
            '  })();',
            '  return __initPromise;',
            '}',
          ].join('\n');
          code = code.replace(initWasmCallRe, replacement);
        }

        // ── Transform 3: Defer WASM export bindings ─────────────────
        const wasmBindingRe = /^const ([\w$]+) = __vite__wasmModule\.([\w$]+);$/gm;
        const bindingAssignments: string[] = [];
        code = code.replace(wasmBindingRe, (_match, varName, propName) => {
          bindingAssignments.push(`  ${varName} = __vite__wasmModule.${propName};`);
          return `let ${varName};`;
        });
        if (bindingAssignments.length > 0) {
          code = code.replace(
            '    __vite__wasmModule = result;',
            '    __vite__wasmModule = result;\n' + bindingAssignments.join('\n'),
          );
        }

        // ── Transform 4: Remove top-level __wbg_set_wasm(wasm$2) ────
        code = code.replace(/\n__wbg_set_wasm\(wasm\$2\);\n/, '\n');

        // ── Transform 5: Replace init() and isInitialized() ─────────
        code = code.replace(
          /async function init\(\)\s*\{[\s\S]*?return Promise\.resolve\(\);\s*\}/,
          'async function init() {\n  await __polyglot_init_wasm();\n}',
        );
        code = code.replace(
          /function isInitialized\(\)\s*\{[\s\S]*?return true;\s*\}/,
          'function isInitialized() {\n  return !!__vite__wasmModule;\n}',
        );

        // ── Transform 6: Remove __vite__initWasm from top-level (keep the function)
        // Actually we need to keep __vite__initWasm - we call it from __polyglot_init_wasm
        // So we don't remove it. Done.

        item.code = code;
      }
    },
  };
}
