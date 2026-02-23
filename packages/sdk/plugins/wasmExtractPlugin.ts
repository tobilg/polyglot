import type { Plugin } from 'vite';

type WasmExtractPluginOptions = {
  wasmFilename: string;
  wasmRelativePath: string;
  extractWasm: boolean;
  injectNodeCompat?: boolean;
};

const NODE_FILE_FETCH_COMPAT = [
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

const DATA_URL_REGEX = /(=\s*)(['"])data:application\/wasm;base64,([A-Za-z0-9+/=]+)\2/;

export function wasmExtractPlugin(options: WasmExtractPluginOptions): Plugin {
  const { wasmFilename, wasmRelativePath, extractWasm } = options;
  const injectNodeCompat = options.injectNodeCompat ?? true;
  let wroteWasm = false;

  return {
    name: 'polyglot-wasm-extract',
    apply: 'build',
    generateBundle(_, bundle) {
      for (const item of Object.values(bundle)) {
        if (item.type !== 'chunk') {
          continue;
        }

        const match = item.code.match(DATA_URL_REGEX);
        if (!match) {
          continue;
        }

        if (extractWasm && !wroteWasm) {
          const wasmBytes = Buffer.from(match[3], 'base64');
          this.emitFile({
            type: 'asset',
            fileName: wasmFilename,
            source: wasmBytes,
          });
          wroteWasm = true;
        }

        item.code = item.code.replace(
          DATA_URL_REGEX,
          `$1new URL("${wasmRelativePath}",import.meta.url).href`
        );

        if (injectNodeCompat && !item.code.includes('globalThis.process.versions?.node')) {
          item.code = `${NODE_FILE_FETCH_COMPAT}\n${item.code}`;
        }
      }
    },
  };
}
