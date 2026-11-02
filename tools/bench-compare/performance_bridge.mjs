// Compare bridge strategies in the same Vite-built Node SDK artifact.
// Usage: node performance_bridge.mjs /absolute/path/to/dist/index.node.js
import { readFile } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import { pathToFileURL } from 'node:url';

const url = pathToFileURL(process.argv[2]);
const sdk = await import(url.href);
const source = await readFile(url, 'utf8');
const chunkPath = source.match(/from "(\.\/compat-[^"]+\.js)"/)[1];
const chunk = await import(new URL(chunkPath, url));
const wasm = Object.values(chunk).find(value => value && typeof value.ast_query_value === 'function');
if (!wasm) throw new Error('This experiment requires the structured-query WASM export');

function measure(operation, size, fn) {
  for (let i = 0; i < 5; i++) fn();
  const samples_us = [];
  for (let batch = 0; batch < 5; batch++) {
    const start = performance.now();
    let count = 0;
    do { fn(); count++; } while (performance.now() - start < 100);
    samples_us.push((performance.now() - start) * 1000 / count);
  }
  samples_us.sort((a,b) => a-b);
  console.log(JSON.stringify({operation, size, median_us:samples_us[2], samples_us}));
}

for (const size of [25, 1000]) {
  const sql = `SELECT ${Array.from({length:size},(_,i)=>`x + ${i} AS c${i}`).join(', ')} FROM t`;
  const parsed = sdk.parse(sql, 'postgresql');
  if (!parsed.success) throw new Error(JSON.stringify(parsed));
  const tree = parsed.ast[0];
  for (const [query, legacy] of [
    ['node_count', wasm.ast_node_count],
    ['column_names', wasm.ast_get_column_names],
    ['table_names', wasm.ast_get_table_names],
  ]) {
    const structured = () => wasm.ast_query_value(tree, query).result;
    const json = () => JSON.parse(legacy(JSON.stringify(tree))).result;
    if (JSON.stringify(structured()) !== JSON.stringify(json())) throw new Error(`Mismatch: ${query}`);
    measure(`structured_${query}`,size,structured);
    measure(`json_${query}`,size,json);
  }
}
