import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Client, SqlError } from './client.ts';
import { demonstrate, modulePath } from './main.ts';

test('documented examples and errors against the real WASI module', async () => {
  const client = await Client.open(modulePath);
  assert.deepEqual(await demonstrate(client), [
    'SELECT COALESCE(a, b) FROM t',
    "SELECT ID FROM SALES.ORDERS PARTITION(P1) WHERE REGION = 'EU'",
  ]);
  const formatted = await client.call({
    action: 'format',
    sql: 'select a from t where b=1',
    dialect: 'postgres',
  });
  assert.equal(formatted.length, 1);
  assert.ok(formatted[0] !== undefined);
  assert.match(formatted[0], /\n/);
  assert.deepEqual(
    await client.call({ action: 'transpile', sql: 'SELECT 1; SELECT 2' }),
    ['SELECT 1', 'SELECT 2'],
  );
  assert.deepEqual(await client.call({ action: 'transpile', sql: '' }), []);
  // A zero exit status must not hide a structured SQL/protocol error.
  await assert.rejects(
    client.call({ action: 'parse', sql: 'SELECT 1', dialect: 'not-a-dialect' }),
    (error: unknown) =>
      error instanceof SqlError && /Unknown dialect/.test(error.message),
  );
  // Failure must not affect the next request on the same compiled module.
  assert.deepEqual(
    await client.call({
      action: 'generate',
      ast: await client.call({ action: 'parse', sql: 'SELECT 1' }),
    }),
    ['SELECT 1'],
  );
});
