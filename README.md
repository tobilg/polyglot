# Polyglot

Rust/Wasm-powered SQL transpiler for 32+ dialects, inspired by [sqlglot](https://github.com/tobymao/sqlglot).

Polyglot parses, generates, transpiles, and formats SQL across 32+ database dialects. It ships as:
- a Rust crate ([`polyglot-sql`](https://crates.io/crates/polyglot-sql/))
- a TypeScript/WASM SDK ([`@polyglot-sql/sdk`](https://www.npmjs.com/package/@polyglot-sql/sdk))
- a Python package ([`polyglot-sql`](https://pypi.org/project/polyglot-sql/))

There's also a [playground](https://polyglot-playground.gh.tobilg.com/) where you can try it out in the browser, as well as the [Rust API Docs](https://docs.rs/polyglot-sql/latest/polyglot_sql/) and [TypeScript API Docs](https://polyglot.gh.tobilg.com/).

Release notes are tracked in [`CHANGELOG.md`](CHANGELOG.md).

## Features

- **Transpile** SQL between any pair of 32 dialects
- **Parse** SQL into a fully-typed AST
- **Generate** SQL back from AST nodes
- **Format** / pretty-print SQL
- **Fluent builder API** for constructing queries programmatically
- **Validation** with syntax, semantic, and schema-aware checks
- **AST visitor** utilities for walking, transforming, and analyzing queries
- **C FFI** shared/static library for multi-language bindings (`polyglot-sql-ffi`)
- **Python bindings** powered by PyO3 (`polyglot-sql` on PyPI)

## Supported Dialects (32)

| | | | | |
|---|---|---|---|---|
| Athena | BigQuery | ClickHouse | CockroachDB | Databricks |
| Doris | Dremio | Drill | Druid | DuckDB |
| Dune | Exasol | Fabric | Hive | Materialize |
| MySQL | Oracle | PostgreSQL | Presto | Redshift |
| RisingWave | SingleStore | Snowflake | Solr | Spark |
| SQLite | StarRocks | Tableau | Teradata | TiDB |
| Trino | TSQL | | | |

## Quick Start

### Rust

```rust
use polyglot_sql::{transpile, DialectType};

// Transpile MySQL to PostgreSQL
let result = transpile(
    "SELECT IFNULL(a, b) FROM t",
    DialectType::MySQL,
    DialectType::Postgres,
).unwrap();
assert_eq!(result[0], "SELECT COALESCE(a, b) FROM t");
```

```rust
use polyglot_sql::builder::*;

// Fluent query builder
let query = select(["id", "name"])
    .from("users")
    .where_(col("age").gt(lit(18)))
    .order_by(["name"])
    .limit(10)
    .build();
```

See the full [Rust crate README](crates/polyglot-sql/README.md) for more examples.

### TypeScript

```bash
npm install @polyglot-sql/sdk
```

```typescript
import { transpile, Dialect } from '@polyglot-sql/sdk';

// Transpile MySQL to PostgreSQL
const result = transpile(
  'SELECT IFNULL(a, b) FROM t',
  Dialect.MySQL,
  Dialect.PostgreSQL,
);
console.log(result.sql[0]); // SELECT COALESCE(a, b) FROM t
```

```typescript
import { select, col, lit } from '@polyglot-sql/sdk';

// Fluent query builder
const sql = select('id', 'name')
  .from('users')
  .where(col('age').gt(lit(18)))
  .orderBy(col('name').asc())
  .limit(10)
  .toSql('postgresql');
```

See the full [TypeScript SDK README](packages/sdk/README.md) for more examples.

### Python

```bash
pip install polyglot-sql
```

```python
import polyglot_sql

result = polyglot_sql.transpile(
    "SELECT IFNULL(a, b) FROM t",
    read="mysql",
    write="postgres",
)
print(result[0])  # SELECT COALESCE(a, b) FROM t
```

See the full [Python bindings README](crates/polyglot-sql-python/README.md).

## Format Guard Rails

SQL formatting runs through guard limits in Rust core to prevent pathological inputs from exhausting memory:

- `maxInputBytes`: `16 MiB` (default)
- `maxTokens`: `1_000_000` (default)
- `maxAstNodes`: `1_000_000` (default)
- `maxSetOpChain`: `256` (default)

Guard failures return error codes in the message (`E_GUARD_INPUT_TOO_LARGE`, `E_GUARD_TOKEN_BUDGET_EXCEEDED`, `E_GUARD_AST_BUDGET_EXCEEDED`, `E_GUARD_SET_OP_CHAIN_EXCEEDED`).

Configuration surface by runtime:
- Rust: configurable via `format_with_options`.
- WASM: configurable via `format_sql_with_options` / `format_sql_with_options_value`.
- TypeScript SDK: configurable via `formatWithOptions`.
- C FFI: configurable via `polyglot_format_with_options`.
- Python: configurable via keyword-only `format_sql(..., max_*)` overrides.

WASM low-level example (from `polyglot-sql-wasm` exports):

```javascript
import init, { format_sql_with_options } from "./polyglot_sql_wasm.js";

await init();
const raw = format_sql_with_options(
  "SELECT a,b FROM t",
  "generic",
  JSON.stringify({
    maxInputBytes: 2 * 1024 * 1024,
    maxTokens: 250000,
    maxAstNodes: 250000,
    maxSetOpChain: 128
  }),
);
const result = JSON.parse(raw);
```

## Project Structure

```
polyglot/
├── crates/
│   ├── polyglot-sql/           # Core Rust library (parser, generator, builder)
│   ├── polyglot-sql-function-catalogs/ # Optional dialect function catalogs (feature-gated data)
│   ├── polyglot-sql-wasm/      # WASM bindings
│   ├── polyglot-sql-ffi/       # C ABI bindings (.so/.dylib/.dll + .a/.lib + header)
│   └── polyglot-sql-python/    # Python bindings (PyO3 + maturin, published on PyPI)
├── packages/
│   ├── sdk/                    # TypeScript SDK (@polyglot-sql/sdk on npm)
│   └── playground/             # Playground for testing the SDK (React 19, Tailwind v4, Vite)
├── examples/
│   ├── rust/                   # Rust example
│   ├── typescript/             # TypeScript SDK example
│   └── c/                      # C FFI example
└── tools/
    ├── sqlglot-compare/        # Test extraction & comparison tool
    └── bench-compare/          # Performance benchmarks
```

## Examples

Standalone example projects are available in the [`examples/`](examples/) directory. Each one pulls the latest published package and can be run independently.

### Rust

```bash
cargo run --manifest-path examples/rust/Cargo.toml
```

### TypeScript

```bash
cd examples/typescript
pnpm install --ignore-workspace && pnpm start
```

## Building from Source

```bash
# Build Rust core
cargo build -p polyglot-sql

# Build C FFI crate (shared/static libs + generated header)
cargo build -p polyglot-sql-ffi --profile ffi_release

# Build Python extension / wheel
make develop-python
make build-python

# Build WASM + TypeScript SDK
make build-all

# Or step by step:
cd crates/polyglot-sql-wasm && wasm-pack build --target bundler --release
cd packages/sdk && npm run build
```

## C FFI

Polyglot provides a stable C ABI in `crates/polyglot-sql-ffi`.

- Crate README: [`crates/polyglot-sql-ffi/README.md`](crates/polyglot-sql-ffi/README.md)
- Generated header: `crates/polyglot-sql-ffi/polyglot_sql.h`
- Example program: `examples/c/main.c`
- Make targets:
  - `make build-ffi`
  - `make generate-ffi-header`
  - `make build-ffi-example`
  - `make test-ffi`

For tagged releases (`v*`), CI also attaches prebuilt FFI artifacts and checksums to GitHub Releases.

## Python Bindings

Polyglot provides first-party Python bindings in `crates/polyglot-sql-python`.

- Crate README: [`crates/polyglot-sql-python/README.md`](crates/polyglot-sql-python/README.md)
- Package name on PyPI: `polyglot-sql`
- Make targets:
  - `make develop-python`
  - `make test-python`
  - `make typecheck-python`
  - `make build-python`

## Function Catalogs

Optional dialect function catalogs are provided via `crates/polyglot-sql-function-catalogs`.

- Crate README: [`crates/polyglot-sql-function-catalogs/README.md`](crates/polyglot-sql-function-catalogs/README.md)
- Core feature flags:
  - `function-catalog-clickhouse`
  - `function-catalog-duckdb`
  - `function-catalog-all-dialects`
- Intended behavior: compile-time inclusion, one-time load in core, auto-use during schema validation type checks.

## Testing

Polyglot currently runs **10,220 SQLGlot fixture cases** plus additional project-specific suites. All strict pass/fail suites are at **100%** in the latest verification run.

| Category | Count | Pass Rate |
|----------|------:|:---------:|
| SQLGlot generic identity | 956 | 100% |
| SQLGlot dialect identity | 3,554 | 100% |
| SQLGlot transpilation | 5,513 | 100% |
| SQLGlot transpile (generic) | 145 | 100% |
| SQLGlot parser | 29 | 100% |
| SQLGlot pretty-print | 23 | 100% |
| Lib unit tests | 835 | 100% |
| Custom dialect identity | 276 | 100% |
| Custom dialect transpilation | 347 | 100% |
| ClickHouse parser corpus (non-skipped) | 7,047 | 100% |
| FFI integration tests | 20 | 100% |
| Python bindings tests (`make test-python`) | 69 | 100% |
| **Total (strict Rust/FFI pass/fail case count)** | **18,745** | **100%** |

```bash
# Setup fixtures (required once)
make setup-fixtures

# Run all tests
make test-rust-all          # All SQLGlot fixture suites
make test-rust-lib          # Lib unit tests (835)
make test-rust-verify       # Full strict verification suite
make test-ffi               # FFI crate integration tests

# Individual test suites
make test-rust-identity     # 956 generic identity cases
make test-rust-dialect      # 3,554 dialect identity cases
make test-rust-transpile    # 5,513 transpilation cases
make test-rust-transpile-generic # 145 generic transpile cases
make test-rust-parser       # 29 parser cases
make test-rust-pretty       # 23 pretty-print cases

# Additional tests
make test-rust-roundtrip    # Organized roundtrip unit tests
make test-rust-matrix       # Dialect matrix transpilation tests
make test-rust-compat       # SQLGlot compatibility tests
make test-rust-errors       # Error handling tests
make test-rust-functions    # Function normalization tests

# TypeScript SDK tests
cd packages/sdk && npm test

# Full comparison against Python SQLGlot
make test-compare
```

### Benchmarks

```bash
make bench-compare          # Compare polyglot-sql vs sqlglot performance
make bench-rust             # Rust benchmarks (JSON output)
make bench-python           # Python sqlglot benchmarks (JSON output)
cargo bench -p polyglot-sql  # Criterion benchmarks
```

### Fuzzing

```bash
cargo +nightly fuzz run fuzz_parser
cargo +nightly fuzz run fuzz_roundtrip
cargo +nightly fuzz run fuzz_transpile
```

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make help` | Show all available commands |
| `make build-all` | Build core release + FFI + Python + bindings + WASM/SDK |
| `make build-wasm` | Build WASM package + TypeScript SDK |
| `make build-ffi` | Build C FFI crate (`ffi_release` profile) |
| `make generate-ffi-header` | Generate C header via cbindgen/build.rs |
| `make build-ffi-example` | Build + run C example against FFI lib |
| `make develop-python` | Build/install Python extension in uv-managed env |
| `make build-python` | Build Python wheels with maturin |
| `make test-ffi` | Run FFI integration tests |
| `make test-rust` | Run SQLGlot-named Rust tests in `polyglot-sql` |
| `make test-rust-all` | Run all 10,220 SQLGlot fixture cases |
| `make test-rust-lib` | Run 835 lib unit tests |
| `make test-rust-verify` | Full verification suite |
| `make test-rust-clickhouse-parser` | Run strict ClickHouse parser suite |
| `make test-rust-clickhouse-coverage` | Run ClickHouse coverage suite (report-only) |
| `make test-compare` | Compare against Python sqlglot |
| `make bench-compare` | Performance comparison |
| `make bench-parse` | Core parse benchmark (polyglot vs sqlglot) |
| `make bench-parse-quick` | Faster core parse benchmark mode |
| `make bench-parse-full` | Parse benchmark including optional parsers |
| `make extract-fixtures` | Regenerate JSON fixtures from Python |
| `make setup-fixtures` | Create fixture symlink for Rust tests |
| `make generate-bindings` | Generate TypeScript type bindings |
| `make test-python` | Run Python bindings tests |
| `make typecheck-python` | Run Python bindings type-check |
| `make documentation-build` | Build documentation site |
| `make documentation-deploy` | Deploy documentation to Cloudflare Pages |
| `make playground-build` | Build playground |
| `make playground-deploy` | Deploy playground to Cloudflare Pages |
| `make clean` | Remove all build artifacts |

## Licenses

[MIT](LICENSE)
[sqlglot MIT](licenses/SQLGLOT_LICENSE.md)
