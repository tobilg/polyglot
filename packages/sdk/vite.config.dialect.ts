import { defineConfig } from 'vite';
import { resolve } from 'path';
import wasm from 'vite-plugin-wasm';

const dialect = process.env.POLYGLOT_DIALECT;
if (!dialect) {
  throw new Error('POLYGLOT_DIALECT environment variable is required');
}

/**
 * Per-Dialect Build Configuration
 *
 * Uses Vite resolve.alias to swap the WASM import path at build time,
 * pointing to the per-dialect WASM binary instead of the full build.
 *
 * Usage:
 *   POLYGLOT_DIALECT=clickhouse pnpm run build:dialect
 */
export default defineConfig({
  plugins: [wasm()],
  resolve: {
    alias: {
      [resolve(__dirname, 'wasm/polyglot_sql_wasm.js')]:
        resolve(__dirname, `wasm/${dialect}/polyglot_sql_wasm.js`),
    },
  },
  build: {
    lib: {
      entry: resolve(__dirname, 'src/index.ts'),
      formats: ['es'],
      fileName: () => 'index.js',
    },
    outDir: `dist/dialects/${dialect}`,
    rollupOptions: {
      external: [],
      output: {
        exports: 'named',
      },
    },
    target: 'esnext',
    sourcemap: true,
    minify: false,
  },
  assetsInclude: ['**/*.wasm'],
  optimizeDeps: {
    exclude: [`./wasm/${dialect}/polyglot_sql_wasm.js`],
  },
});
