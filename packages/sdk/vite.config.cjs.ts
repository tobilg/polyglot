/**
 * CJS-only build config.
 * Externals the WASM module to avoid top-level await in CJS.
 * Run build:esm first (extracts .wasm), then copy wasm .js files to dist.
 */
import { defineConfig } from 'vite';
import { resolve } from 'path';
import dts from 'vite-plugin-dts';

export default defineConfig({
  plugins: [
    dts({
      include: ['src/**/*.ts'],
      exclude: ['src/**/*.test.ts', 'src/test-setup.ts'],
      rollupTypes: true,
    }),
  ],
  build: {
    lib: {
      entry: resolve(__dirname, 'src/index.ts'),
      name: 'PolyglotSQL',
      formats: ['cjs'],
      fileName: () => 'index.cjs',
    },
    rollupOptions: {
      external: [
        /^\.\.\/wasm\/polyglot_sql_wasm/,
        resolve(__dirname, 'wasm/polyglot_sql_wasm.js'),
      ],
      output: {
        exports: 'named',
      },
    },
    target: 'esnext',
    sourcemap: false,
    minify: false,
    outDir: 'dist',
    emptyOutDir: false, // Preserve ESM output and .wasm from main build
  },
});
