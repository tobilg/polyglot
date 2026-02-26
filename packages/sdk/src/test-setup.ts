/**
 * Vitest setup - initializes WASM before tests run.
 */
import { beforeAll } from 'vitest';
import { loadWasm } from './wasm-loader';

beforeAll(async () => {
  await loadWasm();
});
