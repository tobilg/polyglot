# Polyglot

WASM-powered SQL transpiler for 32+ dialects, inspired by Python's [sqlglot](https://github.com/tobymao/sqlglot).

Polyglot parses, generates, transpiles, and formats SQL across 32+ database dialects. It ships as a Rust crate ([`polyglot-sql`](crates/polyglot-sql/)) and a TypeScript/WASM SDK ([`@polyglot-sql/sdk`](packages/sdk/) on npm).

## Features

- **Transpile** SQL between any pair of 32+ dialects
- **Parse** SQL into a fully-typed AST
- **Generate** SQL back from AST nodes
- **Format** / pretty-print SQL
- **Fluent builder API** for constructing queries programmatically
- **Validation** with syntax, semantic, and schema-aware checks
- **AST visitor** utilities for walking, transforming, and analyzing queries

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
  { read: Dialect.MySQL, write: Dialect.PostgreSQL }
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

## Project Structure

```
polyglot/
├── crates/
│   ├── polyglot-sql/          # Core Rust library (parser, generator, builder)
│   └── polyglot-sql-wasm/          # WASM bindings
├── packages/
│   └── sdk/                    # TypeScript SDK (@polyglot-sql/sdk on npm)
├── tools/
│   ├── sqlglot-compare/        # Test extraction & comparison tool
│   └── bench-compare/          # Performance benchmarks
└── external-projects/
    └── sqlglot/                # Reference Python implementation
```

## Building from Source

```bash
# Build Rust core
cargo build -p polyglot-sql

# Build WASM + TypeScript SDK
make build-all

# Or step by step:
cd crates/polyglot-sql-wasm && wasm-pack build --target bundler --release
cd packages/sdk && npm run build
```

## Testing

Polyglot maintains compatibility with sqlglot through **6,284 fixture tests** extracted from the Python reference implementation. All test suites pass at **100%**.

| Category | Count | Pass Rate |
|----------|------:|:---------:|
| Generic identity | 955 | 100% |
| Dialect identity | 3,489 | 100% |
| Transpilation | 1,816 | 100% |
| Pretty-print | 24 | 100% |
| Lib unit tests | 693 | 100% |
| **Total** | **6,977** | **100%** |

```bash
# Setup fixtures (required once)
make setup-fixtures

# Run all tests
make test-rust-all          # All 6,284 fixture tests
make test-rust-lib          # 693 lib unit tests
make test-rust-verify       # Full verification (lib + identity + dialect + transpilation)

# Individual test suites
make test-rust-identity     # 955 generic identity tests
make test-rust-dialect      # 3,489 dialect identity tests
make test-rust-transpile    # 1,816 transpilation tests
make test-rust-pretty       # 24 pretty-print tests

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
| `make build-all` | Build WASM + Rust (release) |
| `make build-wasm` | Build WASM package + TypeScript SDK |
| `make test-rust` | Run all sqlglot compatibility tests |
| `make test-rust-all` | Run all 6,284 fixture tests |
| `make test-rust-lib` | Run 693 lib unit tests |
| `make test-rust-verify` | Full verification suite |
| `make test-compare` | Compare against Python sqlglot |
| `make bench-compare` | Performance comparison |
| `make extract-fixtures` | Regenerate JSON fixtures from Python |
| `make setup-fixtures` | Create fixture symlink for Rust tests |
| `make generate-bindings` | Generate TypeScript type bindings |
| `make clean` | Remove all build artifacts |

## License

[MIT](LICENSE)
