import type { Plugin } from 'vite';

/**
 * Vite plugin that transforms the ESM-style WASM bundle output into a
 * CommonJS-compatible module. It operates in the `generateBundle` hook
 * (post-bundle), performing text-level transforms on the already-bundled code.
 *
 * The CJS build is first produced as ESM (to avoid Rollup's top-level-await
 * restriction in CJS format), then this plugin rewrites the output:
 *
 * 1. Remove the Node fetch shim (CJS uses fs.readFileSync directly)
 * 2. Replace import.meta.url WASM URL with __dirname-based path
 * 3. Defer WASM initialisation behind an async init() function
 * 4. Move __wbg_set_wasm() call inside the deferred init
 * 5. Rewrite init() / isInitialized() to use the deferred pattern
 * 6. Remove the now-unused __vite__initWasm helper
 * 7. Convert ESM export statement to CJS module.exports
 */
export function wasmCjsPlugin(): Plugin {
  return {
    name: 'polyglot-wasm-cjs',
    apply: 'build',
    generateBundle(_, bundle) {
      for (const item of Object.values(bundle)) {
        if (item.type !== 'chunk') continue;

        let code = item.code;

        // ── Transform 1: Remove NODE_FILE_FETCH_COMPAT shim ──────────
        // The shim starts at the very beginning of the file and wraps
        // globalThis.fetch for file:// URLs. Not needed in CJS.
        code = code.replace(
          /^\(\(\)=>\{if\(typeof globalThis\.process<"u".*?\}\}\)\(\);\n?/,
          '',
        );

        // ── Transform 2: Replace WASM URL ────────────────────────────
        // ESM: new URL("./polyglot_sql_wasm_bg.wasm", import.meta.url).href
        // CJS: require("path").join(__dirname, "polyglot_sql_wasm_bg.wasm")
        code = code.replace(
          /const __vite__wasmUrl\s*=\s*new URL\(["']\.\/polyglot_sql_wasm_bg\.wasm["'],\s*import\.meta\.url\)\.href;/,
          'const __vite__wasmUrl = require("path").join(__dirname, "polyglot_sql_wasm_bg.wasm");',
        );

        // ── Transform 3: Defer WASM initialization ───────────────────
        // Capture the full `await __vite__initWasm({...}, __vite__wasmUrl);`
        // call and the import-object contents inside it.
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
            '    const wasmPath = require("path").join(__dirname, "polyglot_sql_wasm_bg.wasm");',
            '    const bytes = require("fs").readFileSync(wasmPath);',
            '    const result = await WebAssembly.instantiate(bytes, __vite__wasmImports);',
            '    __vite__wasmModule = result.instance.exports;',
            '    __wbg_set_wasm(__vite__wasmModule);',
            '  })();',
            '  return __initPromise;',
            '}',
          ].join('\n');
          code = code.replace(initWasmCallRe, replacement);
        }

        // ── Transform 3b: Defer WASM export bindings ─────────────────
        // Lines like `const memory = __vite__wasmModule.memory;` execute
        // at module load time, but __vite__wasmModule is undefined until
        // init() is called. Convert them to `let` declarations and assign
        // them inside __polyglot_init_wasm().
        const wasmBindingRe = /^const ([\w$]+) = __vite__wasmModule\.([\w$]+);$/gm;
        const bindingAssignments: string[] = [];
        code = code.replace(wasmBindingRe, (_match, varName, propName) => {
          bindingAssignments.push(`  ${varName} = __vite__wasmModule.${propName};`);
          return `let ${varName};`;
        });
        // Inject the binding assignments into __polyglot_init_wasm, right
        // after `__vite__wasmModule = result.instance.exports;`
        if (bindingAssignments.length > 0) {
          code = code.replace(
            '    __vite__wasmModule = result.instance.exports;',
            '    __vite__wasmModule = result.instance.exports;\n' +
              bindingAssignments.join('\n'),
          );
        }

        // ── Transform 4: Remove top-level __wbg_set_wasm(wasm$2) ────
        // It's now inside __polyglot_init_wasm(). Remove standalone call
        // that appears after the wasm$2 module definition.
        // Only remove the standalone call, not the one inside our init function.
        code = code.replace(
          /\n__wbg_set_wasm\(wasm\$2\);\n/,
          '\n',
        );

        // ── Transform 5: Replace init() and isInitialized() ─────────
        code = code.replace(
          /async function init\(\)\s*\{[\s\S]*?return Promise\.resolve\(\);\s*\}/,
          'async function init() {\n  await __polyglot_init_wasm();\n}',
        );
        code = code.replace(
          /function isInitialized\(\)\s*\{[\s\S]*?return true;\s*\}/,
          'function isInitialized() {\n  return !!__vite__wasmModule;\n}',
        );

        // ── Transform 6: Remove __vite__initWasm function ────────────
        code = code.replace(
          /const __vite__initWasm = async \(opts = \{\}, url\) => \{[\s\S]*?return result\.instance\.exports;\s*\};\n*/,
          '',
        );

        // ── Transform 7: Convert ESM exports to CJS ─────────────────
        // Replace the final `export { ... };` with CJS module.exports.
        // The export block can be multi-line:
        //   export {\n  Foo,\n  Bar as baz,\n  qux as default\n};
        const exportRe = /\nexport \{([\s\S]*?)\};\s*$/;
        const exportMatch = code.match(exportRe);
        if (exportMatch) {
          const exportsList = exportMatch[1];
          const assignments: string[] = [];
          for (const part of exportsList.split(',')) {
            const trimmed = part.trim();
            if (!trimmed) continue;
            const asMatch = trimmed.match(/^(\S+)\s+as\s+(\S+)$/);
            if (asMatch) {
              const [, local, exported] = asMatch;
              if (exported === 'default') {
                assignments.push(`exports.default = ${local};`);
                assignments.push(`module.exports = ${local};`);
                assignments.push(`Object.assign(module.exports, exports);`);
              } else {
                assignments.push(`exports.${exported} = ${local};`);
              }
            } else {
              assignments.push(`exports.${trimmed} = ${trimmed};`);
            }
          }

          // Place default export handling at the end
          const defaultLines = assignments.filter(l => l.includes('module.exports'));
          const namedLines = assignments.filter(l => !l.includes('module.exports'));
          const cjsExports = '\n' + [...namedLines, ...defaultLines].join('\n') + '\n';
          // Escape $ in replacement to prevent regex back-reference interpretation
          // (e.g. variable names like `index$1` would be treated as capture groups)
          code = code.replace(exportRe, cjsExports.replace(/\$/g, '$$$$'));
        }

        // Add CJS header
        code = `"use strict";\n${code}`;

        item.code = code;
      }
    },
  };
}
