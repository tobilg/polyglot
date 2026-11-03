import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { Client, isObject } from './client.ts';
import type { JsonValue } from './client.ts';

export const modulePath =
  process.env.POLYGLOT_WASI_MODULE ??
  new URL('./polyglot_sql_wasi.wasm', import.meta.url);

// This sample query has one table. Preserve all other AST fields when editing.
function addPartition(value: JsonValue): number {
  let count = 0;
  if (isObject(value)) {
    if (isObject(value.table)) {
      value.table.partitions = [{ name: 'P1', quoted: false }];
      count++;
    }
    for (const child of Object.values(value)) count += addPartition(child);
  } else if (Array.isArray(value)) {
    for (const child of value) count += addPartition(child);
  }
  return count;
}

export async function demonstrate(client: Client): Promise<string[]> {
  const transpiled = await client.call({
    action: 'transpile',
    sql: 'SELECT IFNULL(a, b) FROM t',
    read_dialect: 'mysql',
    write_dialect: 'postgres',
  });
  const ast = await client.call({
    action: 'parse',
    dialect: 'oracle',
    sql: "SELECT ID FROM SALES.ORDERS WHERE REGION = 'EU'",
  });
  const count = addPartition(ast);
  if (count !== 1)
    throw new Error(`Expected one table in the sample AST, got ${count}`);
  const generated = await client.call({
    action: 'generate',
    ast,
    dialect: 'oracle',
  });
  return [...transpiled, ...generated];
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  try {
    const client = await Client.open(modulePath);
    for (const sql of await demonstrate(client)) console.log(sql);
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
}
