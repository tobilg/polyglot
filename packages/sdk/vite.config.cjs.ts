import { defineConfig } from 'vite';
import { resolve } from 'path';
import wasm from 'vite-plugin-wasm';
import { wasmExtractPlugin } from './plugins/wasmExtractPlugin';
import { wasmCjsPlugin } from './plugins/wasmCjsPlugin';

export default defineConfig({
  plugins: [
    wasm(),
    wasmExtractPlugin({
      wasmFilename: 'polyglot_sql_wasm_bg.wasm',
      wasmRelativePath: './polyglot_sql_wasm_bg.wasm',
      extractWasm: false,        // ESM build already extracts it
      injectNodeCompat: false,   // CJS plugin handles Node compat differently
    }),
    wasmCjsPlugin(),
  ],
  build: {
    lib: {
      entry: resolve(__dirname, 'src/index.ts'),
      name: 'PolyglotSQL',
      // Build as ESM first (Rollup rejects CJS with top-level await).
      // The wasmCjsPlugin converts the output to CJS in generateBundle.
      formats: ['es'],
      fileName: () => 'index.cjs',
    },
    rollupOptions: {
      external: ['path', 'fs', 'url', 'node:path', 'node:fs', 'node:url'],
      output: { exports: 'named' },
    },
    target: 'node18',
    sourcemap: false,
    minify: false,
    emptyOutDir: false,  // ESM build runs first; don't wipe dist/
  },
  assetsInclude: ['**/*.wasm'],
  optimizeDeps: {
    exclude: ['./wasm/polyglot_sql_wasm.js'],
  },
});
