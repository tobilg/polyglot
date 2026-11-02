// Usage: node performance_review.mjs /absolute/path/to/sdk/dist/index.node.js
import { pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';
const start = performance.now();
const sdk = await import(pathToFileURL(process.argv[2]).href);
await sdk.init();
console.log(JSON.stringify({ operation: 'sdk_init', size: 1, median_us: (performance.now() - start) * 1000 }));

function measure(operation, size, fn) {
  for (let i = 0; i < 5; i++) fn();
  const samples_us = [];
  for (let batch = 0; batch < 5; batch++) {
    const start = performance.now();
    let count = 0;
    do { fn(); count++; } while (performance.now() - start < 100);
    samples_us.push((performance.now() - start) * 1000 / count);
  }
  samples_us.sort((a, b) => a - b);
  console.log(JSON.stringify({ operation, size, median_us: samples_us[2], samples_us }));
}

for (const width of [25, 100, 1000]) {
  const sql = `SELECT ${Array.from({ length: width }, (_, i) => `x + ${i} AS c${i}`).join(', ')} FROM t`;
  const result = sdk.parse(sql, 'postgresql');
  if (!result.success) throw new Error(JSON.stringify(result));
  const tree = result.ast[0];
  for (const checked of [sdk.format(sql, 'postgresql'), sdk.transpile(sql, 'postgresql', 'duckdb')]) {
    if (!checked.success || !checked.sql?.join('\n').includes(`c${width - 1}`)) {
      throw new Error(`Benchmark operation failed: ${JSON.stringify(checked)}`);
    }
  }
  const columns = sdk.ast.getColumnNames(tree);
  if (JSON.stringify(sdk.ast.getTableNames(tree)) !== '["t"]'
      || columns.length !== width || columns.some(name => name !== 'x')
      || sdk.ast.nodeCount(tree) <= width
      || sdk.ast.findFirst(tree, () => true) !== tree) {
    throw new Error('AST benchmark did not produce the expected result');
  }
  measure('sdk_parse', width, () => sdk.parse(sql, 'postgresql'));
  measure('sdk_format', width, () => sdk.format(sql, 'postgresql'));
  measure('sdk_transpile', width, () => sdk.transpile(sql, 'postgresql', 'duckdb'));
  measure('sdk_get_table_names', width, () => sdk.ast.getTableNames(tree));
  measure('sdk_get_column_names', width, () => sdk.ast.getColumnNames(tree));
  measure('sdk_node_count', width, () => sdk.ast.nodeCount(tree));
  measure('sdk_find_first_root', width, () => sdk.ast.findFirst(tree, () => true));
}
