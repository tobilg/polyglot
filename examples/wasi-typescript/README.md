# TypeScript + Node.js WASI example

Requires Node.js 24 and pnpm 12. This example uses `node:wasi` and Node's native
TypeScript support. Its typechecking dependencies are pinned and locked; it
does not depend on `@polyglot-sql/sdk` or run in browsers.

From the repository root, with Rust and Make installed:

```sh
rustup target add wasm32-wasip1
make prepare-wasi-examples
cd examples/wasi-typescript
pnpm install --frozen-lockfile
pnpm typecheck
pnpm start
pnpm test
```

Alternatively, copy a verified downloaded `polyglot_sql_wasi.wasm` into this
directory, or set `POLYGLOT_WASI_MODULE` to its absolute path. Rust is only
required to build the module.

Expected stdout (Node may also emit an experimental WASI warning on stderr):

```text
SELECT COALESCE(a, b) FROM t
SELECT ID FROM SALES.ORDERS PARTITION(P1) WHERE REGION = 'EU'
```

## Use the module in your application

Save this as `app.ts` in this directory and run `node app.ts`:

```typescript
import { Client, SqlError } from './client.ts';

try {
  // Compile once and reuse the client. Each call gets a fresh WASI instance.
  const client = await Client.open(
    new URL('./polyglot_sql_wasi.wasm', import.meta.url),
  );
  const sql = await client.call({
    action: 'transpile',
    sql: 'SELECT IFNULL(a, b) FROM t',
    read_dialect: 'mysql',
    write_dialect: 'postgres',
  });
  console.log(sql[0]); // SELECT COALESCE(a, b) FROM t

  const ast = await client.call({ action: 'parse', sql: 'SELECT 1', dialect: 'postgres' });
  const generated = await client.call({ action: 'generate', ast, dialect: 'postgres' });
  console.log(generated[0]); // SELECT 1
} catch (error) {
  if (error instanceof SqlError) console.error('SQL request failed:', error.message);
  else console.error('WASI integration failed:', error);
  process.exitCode = 1;
}
```

`Client` comes from the local [`client.ts`](client.ts), not an npm package.
For your own project, copy that file and `polyglot_sql_wasi.wasm` alongside
your application file, use Node.js 24, and set `"type": "module"` in
`package.json`. `call()` returns the result array or throws; it handles the
JSON envelope and cleans up temporary stdio files for you.

The [WASI guide](../../docs/wasi.md#instantiate-a-client-and-call-the-module)
also shows formatting, an AST edit, and
[direct `node:wasi` instantiation](../../docs/wasi.md#instantiate-directly-with-nodewasi).

`main.ts` demonstrates transpilation and Oracle parse → AST edit → generate.
`client.ts` compiles once, creates a new instance per request, captures stdio
through temporary file descriptors, validates responses, and cleans up files.
Calls execute synchronously inside `wasi.start()`; use a worker for a responsive
server. This adapter is example code, not a published SDK.

See the [WASI guide](../../docs/wasi.md) for artifact downloads and the protocol.
For the full SDK API in Node.js or a browser, use the
[TypeScript SDK](../../packages/sdk/README.md).
