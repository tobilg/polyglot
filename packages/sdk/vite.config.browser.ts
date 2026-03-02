import { defineConfig } from 'vite';
import { resolve } from 'path';
import wasm from 'vite-plugin-wasm';
import { wasmExtractPlugin } from './plugins/wasmExtractPlugin';
import { wasmBrowserPlugin } from './plugins/wasmBrowserPlugin';

/**
 * Browser-specific ESM build.
 *
 * - No top-level await (lazy init via init())
 * - WASM inlined as base64 (no import.meta.url)
 * - Compatible with Next.js, webpack, Turbopack
 *
 * Consumers must call await init() before using format, transpile, etc.
 */
export default defineConfig({
  plugins: [
    wasm(),
    wasmExtractPlugin({
      wasmFilename: 'polyglot_sql_wasm_bg.wasm',
      wasmRelativePath: './polyglot_sql_wasm_bg.wasm',
      extractWasm: false,
      injectNodeCompat: false,
      keepDataUrl: true,
    }),
    wasmBrowserPlugin(),
  ],
  build: {
    lib: {
      entry: resolve(__dirname, 'src/index.ts'),
      name: 'PolyglotSQL',
      formats: ['es'],
      fileName: () => 'index.browser.js',
    },
    rollupOptions: {
      output: { exports: 'named' },
    },
    target: 'esnext',
    sourcemap: false,
    minify: false,
    emptyOutDir: false,
  },
  assetsInclude: ['**/*.wasm'],
  optimizeDeps: {
    exclude: ['./wasm/polyglot_sql_wasm.js'],
  },
});
