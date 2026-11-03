# Go + WASI example

Requires Go 1.24 or newer (CI uses 1.25). wazero 1.11.0 is pinned in `go.mod`.
The example embeds `polyglot_sql_wasi.wasm`, compiles it once, and uses fresh
instances for each request. It demonstrates MySQL-to-PostgreSQL transpilation
and an Oracle parse → AST edit → generate flow.

From the repository root, with Rust and Make installed:

```sh
rustup target add wasm32-wasip1
make prepare-wasi-examples
cd examples/wasi-go
CGO_ENABLED=0 go run .
```

Alternatively, copy a verified downloaded `polyglot_sql_wasi.wasm` into this
directory before running Go; Rust is only required to build the module.

Expected stdout:

```text
SELECT COALESCE(a, b) FROM t
SELECT ID FROM SALES.ORDERS PARTITION(P1) WHERE REGION = 'EU'
```

To build a standalone binary and verify the integration:

```sh
CGO_ENABLED=0 go build -o wasi-go .
./wasi-go
CGO_ENABLED=0 go vet ./...
CGO_ENABLED=0 go test ./...
```

The binary needs no external `.wasm` or shared library at runtime. Cold module
compilation may take several seconds. See the [WASI guide](../../docs/wasi.md)
for artifact downloads, the JSON protocol, cancellation, caching, and the
differences from the [native Go SDK](../../packages/go/README.md).
