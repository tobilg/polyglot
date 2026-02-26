import { defineConfig } from 'vite';
import { resolve } from 'path';
import dts from 'vite-plugin-dts';
import wasm from 'vite-plugin-wasm';
import { wasmExtractPlugin } from './plugins/wasmExtractPlugin';

export default defineConfig({
  plugins: [
    wasm(),
    wasmExtractPlugin({
      wasmFilename: 'polyglot_sql_wasm_bg.wasm',
      wasmRelativePath: './polyglot_sql_wasm_bg.wasm',
      extractWasm: true,
    }),
    dts({
      include: ['src/**/*.ts'],
      exclude: ['src/**/*.test.ts'],
      rollupTypes: true,
    }),
  ],
  build: {
    lib: {
      entry: resolve(__dirname, 'src/index.ts'),
      name: 'PolyglotSQL',
      formats: ['es'],
      fileName: () => 'index.js',
    },
    rollupOptions: {
      external: [],
      output: {
        exports: 'named',
      },
    },
    target: 'esnext',
    sourcemap: false,
    minify: false,
  },
  assetsInclude: ['**/*.wasm'],
  optimizeDeps: {
    exclude: ['./wasm/polyglot_sql_wasm.js'],
  },
  test: {
    globals: true,
    environment: 'node',
    setupFiles: ['src/test-setup.ts'],
    include: ['src/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
    },
  },
});
