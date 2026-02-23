import { defineConfig } from 'vite';
import { resolve } from 'path';
import wasm from 'vite-plugin-wasm';
import { wasmExtractPlugin } from './plugins/wasmExtractPlugin';

/**
 * UMD Build Configuration
 *
 * Note: Since WASM modules use top-level await, we build as ESM format
 * which can be loaded via <script type="module"> tags.
 *
 * For traditional script tag usage, the module will expose window.PolyglotSQL
 * after being loaded as a module.
 *
 * Usage:
 * ```html
 * <script type="module">
 *   import * as PolyglotSQL from './polyglot.esm.js';
 *   window.PolyglotSQL = PolyglotSQL;
 *   // or use directly
 *   const { transpile, Dialect } = PolyglotSQL;
 * </script>
 * ```
 */
export default defineConfig({
  plugins: [
    wasm(),
    wasmExtractPlugin({
      wasmFilename: 'polyglot_sql_wasm_bg.wasm',
      wasmRelativePath: '../polyglot_sql_wasm_bg.wasm',
      extractWasm: false,
    }),
  ],
  build: {
    lib: {
      entry: resolve(__dirname, 'src/umd.ts'),
      name: 'PolyglotSQL',
      formats: ['es'],
      fileName: () => 'polyglot.esm.js',
    },
    outDir: 'dist/cdn',
    rollupOptions: {
      output: {
        exports: 'named',
        // Inline dynamic imports for single file distribution
        inlineDynamicImports: true,
      },
    },
    target: 'esnext',
    sourcemap: false,
    minify: 'esbuild',
  },
  assetsInclude: ['**/*.wasm'],
  optimizeDeps: {
    exclude: ['./wasm/polyglot_sql_wasm.js'],
  },
  define: {
    'process.env.NODE_ENV': '"production"',
  },
});
