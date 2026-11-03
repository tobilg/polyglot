# Using Polyglot through WASI

The experimental `polyglot-sql-wasi` command supports **parse, generate,
transpile, and format** for all dialects. It accepts one JSON request on stdin,
writes one JSON response on stdout, and exits. A host can embed the same
`polyglot_sql_wasi.wasm` file on each platform supported by its WASI runtime.

## Choose an integration

| Application | Start here | Runtime artifact |
| --- | --- | --- |
| TypeScript in Node.js or a browser, using the full SDK API | [TypeScript SDK](../packages/sdk/README.md) | The npm package's wasm-bindgen module |
| Go, using validation, lineage, builders, or the other SDK APIs | [Go SDK](../packages/go/README.md) | A matching native FFI shared library; PureGo does not need cgo |
| Go, embedding a portable module for the four operations above | [Go WASI example](../examples/wasi-go/) | `polyglot_sql_wasi.wasm`, embedded with `go:embed`; wazero does not need cgo |
| TypeScript in Node.js, explicitly consuming the WASI command | [TypeScript WASI example](../examples/wasi-typescript/) | `polyglot_sql_wasi.wasm`, loaded with `node:wasi` |

The SDK's `polyglot_sql.wasm` and this `polyglot_sql_wasi.wasm` have different
interfaces and cannot be substituted for each other. The Node.js WASI example
does not run in a browser. Use the TypeScript SDK's browser entry point there.
WASI currently has no options for validation, lineage, query analysis, builders,
or strict unsupported-syntax handling. The protocol and raw AST schema are
experimental; pin the module and code that edits its AST to the same version.

## Obtain the module

### Build from this checkout

Install Rust with rustup and use Make. From the repository root:

```sh
rustup target add wasm32-wasip1
make build-wasi
```

The release artifact is
`target/wasm32-wasip1/release/polyglot_sql_wasi.wasm`. No wasm-pack or JavaScript
build is required. `make build-wasi WASI_PROFILE=dev` creates
`target/wasm32-wasip1/debug/polyglot_sql_wasi.wasm` for debugging. Setting
`CARGO_TARGET_DIR` changes the `target` prefix; `WASI_OUT_DIR` can override the
artifact directory when using another Cargo configuration.

Build and copy the release module into both example directories in one step:

```sh
make prepare-wasi-examples
```

### Download a tested build

The `wasi-module` CI job uploads the tested module and its SHA-256 checksum as
the `polyglot-sql-wasi` Actions artifact. Select the workflow run for your desired
commit in [GitHub Actions](https://github.com/tobilg/polyglot/actions/workflows/ci.yml).
For a run that includes this workflow change, GitHub CLI users can download it:

```sh
gh run download RUN_ID --repo tobilg/polyglot --name polyglot-sql-wasi --dir wasi-artifact
cd wasi-artifact
shasum -a 256 -c polyglot_sql_wasi.wasm.sha256
```

On release tags, CI also attaches `polyglot_sql_wasi.wasm` and
`polyglot_sql_wasi.wasm.sha256` to the corresponding
[GitHub release](https://github.com/tobilg/polyglot/releases). This begins with
releases containing the WASI workflow; older releases do not have these assets.
Until such a release is published, use the source build or Actions artifact.
For a published tag containing the assets, replace `vX.Y.Z` below:

```sh
version=vX.Y.Z
gh release download "$version" --repo tobilg/polyglot \
  --pattern polyglot_sql_wasi.wasm --pattern polyglot_sql_wasi.wasm.sha256 \
  --dir wasi-artifact
cd wasi-artifact
shasum -a 256 -c polyglot_sql_wasi.wasm.sha256
```

Linux also supports `sha256sum -c polyglot_sql_wasi.wasm.sha256`. Copy the
verified module into each example directory as `polyglot_sql_wasi.wasm`.
These local copies and compiled binaries are excluded from Git.

## Go quickstart

Requires Go 1.24 or newer; CI uses Go 1.25 with **wazero 1.11.0**, pinned in
the example's `go.mod` and `go.sum`. From the repository root:

```sh
make prepare-wasi-examples
cd examples/wasi-go
CGO_ENABLED=0 go run .
```

Expected stdout:

```text
SELECT COALESCE(a, b) FROM t
SELECT ID FROM SALES.ORDERS PARTITION(P1) WHERE REGION = 'EU'
```

The first request converts MySQL to PostgreSQL. The second flow parses Oracle
SQL, adds a partition to the table AST, and generates Oracle SQL. The example
preserves other AST fields and uses `json.Number` to avoid converting JSON
numbers to floating point while editing.

`CGO_ENABLED=0 go build -o wasi-go .` produces a binary containing the module;
the `.wasm` file is needed at build time, not at runtime. Copy `client.go` into
your application as a starting point. It checks initialization and compilation
errors, decodes the response envelope, captures stderr, and closes runtime
resources. It is an example adapter, not an additional published Go SDK.

## TypeScript / Node.js quickstart

Requires Node.js 24 and pnpm 12. The example runs TypeScript using Node's
[built-in type stripping](https://nodejs.org/docs/latest-v24.x/api/typescript.html);
TypeScript 5.9.3 and Node 24 type declarations are pinned for separate checking.
From the repository root:

```sh
make prepare-wasi-examples
cd examples/wasi-typescript
pnpm install --frozen-lockfile
pnpm typecheck
pnpm start
```

Expected stdout is the same two SQL statements as the Go example. Node may
print an experimental WASI warning on stderr. To load a module elsewhere:

```sh
POLYGLOT_WASI_MODULE=/absolute/path/polyglot_sql_wasi.wasm pnpm start
```

### Instantiate a client and call the module

Save the following as `app.ts` in `examples/wasi-typescript`, then run
`node app.ts`. For your own application, copy
[`client.ts`](../examples/wasi-typescript/client.ts) and the verified
`polyglot_sql_wasi.wasm` next to `app.ts`, and set `"type": "module"` in your
`package.json`. `Client` is the local example adapter; it is not an export of
`@polyglot-sql/sdk` or a separately installed package.

```typescript
import { Client, SqlError, isObject } from './client.ts';

try {
  // Compile once. Reuse this client for subsequent requests.
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

  const formatted = await client.call({
    action: 'format',
    sql: 'select a from t where b=1',
    dialect: 'postgres',
  });
  console.log(formatted[0]); // Multiline SQL

  // Parse returns the AST array directly, without the response envelope.
  const ast = await client.call({
    action: 'parse',
    sql: "SELECT ID FROM SALES.ORDERS WHERE REGION = 'EU'",
    dialect: 'oracle',
  });

  // This sample query has one SELECT and one table. Check its shape before
  // editing; retain every other field from the parsed AST.
  const select = ast[0]?.select;
  const from = isObject(select) ? select.from : undefined;
  const tables = isObject(from) ? from.expressions : undefined;
  const first = Array.isArray(tables) ? tables[0] : undefined;
  const table = isObject(first) ? first.table : undefined;
  if (!isObject(table)) throw new Error('Expected a table in the sample AST');
  table.partitions = [{ name: 'P1', quoted: false }];

  const rewritten = await client.call({ action: 'generate', ast, dialect: 'oracle' });
  console.log(rewritten[0]);
  // SELECT ID FROM SALES.ORDERS PARTITION(P1) WHERE REGION = 'EU'
} catch (error) {
  if (error instanceof SqlError) {
    // The module returned { ok: false, error: "..." }.
    console.error('SQL request failed:', error.message);
  } else {
    // Includes missing modules, compilation failures, traps, and invalid responses.
    console.error('WASI integration failed:', error);
  }
  process.exitCode = 1;
}
```

`Client.open()` compiles the module once. Each `client.call()` creates a fresh
WASI instance, checks the JSON response's `ok` field, and returns its `result`
array. The adapter closes temporary file descriptors and removes request files
after every call; there is no client `close()` method. Keep the client around
while serving requests so you can reuse the compiled code. Pin the module
version when relying on specific AST fields such as `table.partitions`.

### Instantiate directly with `node:wasi`

For a command-line host that forwards one request from its own stdin to the
module and sends the JSON response to its own stdout, save this complete script
as `run-wasi.ts` beside `polyglot_sql_wasi.wasm` in the example directory:

```typescript
import { readFile } from 'node:fs/promises';
import { WASI } from 'node:wasi';

const compiled = await WebAssembly.compile(
  await readFile(new URL('./polyglot_sql_wasi.wasm', import.meta.url)),
);
const wasi = new WASI({
  version: 'preview1',
  args: [],
  env: {},
  preopens: {},
  stdin: 0,
  stdout: 1,
  stderr: 2,
  returnOnExit: true,
});
const instance = await WebAssembly.instantiate(compiled, {
  wasi_snapshot_preview1: wasi.wasiImport,
});
process.exitCode = wasi.start(instance);
```

Run it with one JSON request (the pipe reaches EOF after sending the request):

```sh
printf '%s' '{"action":"transpile","sql":"SELECT IFNULL(a, b) FROM t","read_dialect":"mysql","write_dialect":"postgres"}' | node run-wasi.ts
```

Stdout is `{"ok":true,"result":["SELECT COALESCE(a, b) FROM t"],"error":null}`.
This direct runner forwards the guest exit code; SQL errors still normally exit
zero and are reported as `ok: false` in stdout. Inspect that JSON response, or
use `Client.call()` above to get decoded results and exceptions in your application.
Create a new `WASI` object and instance for each request; do not call `start()`
again on the same instance. The compiled `WebAssembly.Module` can be reused.

### Node.js runtime behavior

The adapter in `client.ts` provides typed requests and results and validates
the response envelope. Node's [WASI API](https://nodejs.org/docs/latest-v24.x/api/wasi.html)
accepts numeric file descriptors for stdin/stdout/stderr. The example uses a
private temporary directory per call, closes every descriptor, and removes the
files on success or failure. It does not grant the guest directory access.
`wasi.start()` executes synchronously; move calls to a worker if your application
must keep the event loop responsive. Each call uses a new WASI instance with
`returnOnExit: true` so guest exit does not terminate Node.

## JSON protocol reference

Send exactly one UTF-8 JSON object, then close stdin (or provide a finite buffer
that reaches EOF). There is no line-based streaming protocol. Trailing whitespace
is allowed; concatenated requests or other trailing content are rejected.

| `action` | Required fields | Optional dialect fields | Success `result` |
| --- | --- | --- | --- |
| `parse` | `sql`: string | `dialect`: string | Array of AST expressions, one per statement |
| `generate` | `ast`: one AST expression or an array | `dialect`: string | Array of SQL strings |
| `transpile` | `sql`: string | `read_dialect`, `write_dialect`: strings | Array of SQL strings |
| `format` | `sql`: string | `dialect`: string | Array of formatted SQL strings |

Action names are lowercase. Omitted or null dialect fields select `generic`;
transpile's two dialects default independently. Names such as `postgres`,
`mysql`, `oracle`, `duckdb`, and `clickhouse` use the core library's dialect
resolution. Unknown names return an error. `dialect` is not a substitute for
`read_dialect` or `write_dialect` on a transpile request. Unknown fields are
currently ignored; do not expect SDK options sent as extra fields to take effect.

For example, send this **single** request:

```json
{"action":"transpile","sql":"SELECT IFNULL(a, b) FROM t","read_dialect":"mysql","write_dialect":"postgres"}
```

Response:

```json
{"ok":true,"result":["SELECT COALESCE(a, b) FROM t"],"error":null}
```

A request with `"dialect":"not-a-dialect"` produces:

```json
{"ok":false,"error":"Unknown dialect: not-a-dialect"}
```

Success includes `result` and `error: null`. Failure omits `result` and includes
an error string. A normal process exit, including exit code zero, does **not**
mean SQL processing succeeded: always inspect `ok`. Treat a trap, nonzero exit,
or invalid/missing JSON response as a host/runtime failure separately from
`ok: false`. Error text is descriptive and is not a stable error-code API.

Pass the array from `parse.result` as `generate.ast`; do not pass the entire
response envelope. Every successful result is an array, even for a single
statement; empty SQL can produce an empty array. AST fields mirror the Rust expression representation,
including tagged nodes such as `{"table": {...}}`. Preserve fields you do not
edit, and verify edits against the module version you ship. The example request
and response types live in [Go](../examples/wasi-go/client.go) and
[TypeScript](../examples/wasi-typescript/client.ts).

### Complexity limits

A request may nest up to **1536 JSON levels**, including the outer request
object. `parse` checks the serialized AST against the same limit, reserving
space for the outer `generate` request. Successful parse output can therefore
be sent back unchanged without exceeding this JSON depth bound. Core parser
and formatter complexity guards still apply. Editing an AST can increase its
depth; `generate` rejects requests over the bound. Reduce query complexity
when the response names a depth limit.

## Runtime lifecycle and performance

Compile the module once per process, then instantiate it once per request.
Do not call `_start` twice on an instance. The Go example uses an empty module
name and separate buffers so independent calls can share compiled code. Close
the runtime when all calls finish. It enables wazero's
`WithCloseOnContextDone(true)` so a cancelled call context can stop guest
execution. Choose timeouts appropriate for your workload; the demo allows two
minutes including cold compilation.

For short-lived Go applications, a disk compilation cache can reduce startup
cost. Create it with `wazero.NewCompilationCacheWithDir(cacheDir)`, check the
error, and pass it through `WithCompilationCache(cache)` on the runtime config.
Keep the cache open until all associated runtimes have closed, then call
`cache.Close(ctx)`. It is an optional optimization; the runnable example uses
only an in-memory runtime. See [wazero's documentation](https://wazero.io/docs/).

Earlier measurements on an arm64 laptop, with a release module:

| Go runtime mode | Initialization | Per call |
| --- | --- | --- |
| Compiled, cold | 10.6 s | 534 µs |
| Compiled, warm disk cache | 207 ms | 288 µs |
| Interpreter | 739 ms | 7.7 ms |

These are illustrative measurements, not latency guarantees. Cold compilation
under Go's race detector took about 2m32s. The tested runtime baseline is wazero
1.11.0; earlier versions have failed with `invalid table access`. The release
module is roughly 30 MB and includes every dialect. Use `WASI_PROFILE=dev` when
diagnosing a trap; the release profile aborts on Rust panics.

## Troubleshooting

| Symptom | Next step |
| --- | --- |
| Go reports `pattern polyglot_sql_wasi.wasm: no matching files found`, or Node reports `ENOENT` | Run `make prepare-wasi-examples`, or copy the verified module into the example directory before building/running. |
| Runtime reports missing imports or exports | Check that you loaded `polyglot_sql_wasi.wasm`, with WASI Preview 1 imports and the command's `_start` entry point. The SDK's wasm-bindgen artifact has a different interface. |
| Call waits indefinitely for a response | Supply a finite stdin buffer/file or close the input pipe after sending one request; the command reads until EOF. |
| Exit status is zero but there is no SQL | Inspect the response's `ok` and `error`, then the `result` array. Multiple statements produce multiple entries; empty SQL may produce an empty array. |
| `invalid table access` with an older wazero | Use the pinned wazero 1.11.0 baseline and rebuild the host. |
| Response mentions the JSON depth limit | Reduce query complexity or the depth of your AST edits; increasing the decoder limit alone can cause a WASM stack trap. |

## Check the integrations

From the repository root, with Rust, Go, Node.js, and pnpm installed:

```sh
pnpm --dir examples/wasi-typescript install --frozen-lockfile
make test-wasi-examples
make test-wasi-runtime
```

The first Make target builds/copies the module, runs Go vet, checks both
examples' expected SQL and error handling, typechecks TypeScript, and executes
both entry points. The second runs the broader protocol and depth regression
suite in wazero. CI runs both before uploading or publishing the module.
