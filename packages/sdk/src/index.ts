/**
 * Polyglot SQL Dialect Translator
 *
 * A WebAssembly-powered SQL dialect translator that can convert SQL
 * between different database dialects (PostgreSQL, MySQL, BigQuery, etc.)
 */

import { loadWasm } from './wasm-loader';

// ESM only: eagerly load WASM on import so init() is not needed (backward compat)
await loadWasm();

export * from './core';
export { default } from './core';
