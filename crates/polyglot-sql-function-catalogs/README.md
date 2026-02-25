# polyglot-sql-function-catalogs

Optional dialect function-catalog data for `polyglot-sql` semantic validation.

This crate intentionally does not depend on `polyglot-sql`. It exposes feature-gated
function lists through a small sink interface so the core crate can pull them in
at compile time without dependency cycles.

## Features

- `dialect-clickhouse`: include ClickHouse function signatures.
- `dialect-duckdb`: include DuckDB function signatures.
- `all-dialects`: currently aliases all available dialect features.

## Performance Model

- This crate emits static function lists into a caller-provided sink.
- No runtime globals are created in this crate.
- Compile-time feature flags decide which dialect lists are built.
- In `polyglot-sql`, enabled lists are loaded once into a `LazyLock` catalog.

## Usage

With `polyglot-sql` compile-time wiring (recommended):

```toml
[dependencies]
polyglot-sql = { version = "...", features = ["function-catalog-clickhouse"] }
```

Then run schema validation with type checks:

```rust
use polyglot_sql::{SchemaValidationOptions, ValidationSchema, validate_with_schema, DialectType};

let schema = ValidationSchema { tables: vec![], strict: Some(true) };
let options = SchemaValidationOptions {
    check_types: true,
    ..Default::default()
};

let result = validate_with_schema(
    "SELECT IF(1, 2, 3)",
    DialectType::ClickHouse,
    &schema,
    &options,
);
assert!(result.valid);
```

The core crate auto-injects embedded catalogs when:

- `check_types` is enabled, and
- no custom `function_catalog` is provided in options.

## What Catalog Validation Checks

Function catalogs are used during schema/type validation (not parser syntax validation).

When `check_types` is enabled in `SchemaValidationOptions`, core validation uses the catalog to check:

- **Function name presence per dialect**:
  - Unknown function name -> `E202` (`E_UNKNOWN_FUNCTION`)
- **Function arity / overloads**:
  - Name exists but argument count matches no signature -> `E203` (`E_INVALID_FUNCTION_ARITY`)

Catalog entries currently define:

- `min_arity`
- `max_arity` (`None` means variadic)
- multiple overloads per function name
- dialect-level casing behavior + optional per-function casing override

Catalog entries do **not** currently define:

- per-argument data types
- return types
- coercion rules
- named/optional parameter semantics

Advanced manual integration (custom sink) is also available via:

- `CatalogSink`
- `register_enabled_catalogs`
- `FunctionSignature`
- `FunctionNameCase`

## Feature Mapping Across Crates

This crate's features are consumed by `polyglot-sql` compile-time features:

- `polyglot-sql/function-catalog-clickhouse` -> `polyglot-sql-function-catalogs/dialect-clickhouse`
- `polyglot-sql/function-catalog-duckdb` -> `polyglot-sql-function-catalogs/dialect-duckdb`
- `polyglot-sql/function-catalog-all-dialects` -> `polyglot-sql-function-catalogs/all-dialects`

Bindings forward those same core features:

- `polyglot-sql-wasm/function-catalog-clickhouse`
- `polyglot-sql-wasm/function-catalog-duckdb`
- `polyglot-sql-wasm/function-catalog-all-dialects`
- `polyglot-sql-ffi/function-catalog-clickhouse`
- `polyglot-sql-ffi/function-catalog-duckdb`
- `polyglot-sql-ffi/function-catalog-all-dialects`
- `polyglot-sql-python/function-catalog-clickhouse`
- `polyglot-sql-python/function-catalog-duckdb`
- `polyglot-sql-python/function-catalog-all-dialects`

Build examples:

```bash
cargo check -p polyglot-sql --features function-catalog-clickhouse
cargo check -p polyglot-sql --features function-catalog-duckdb
cargo check -p polyglot-sql-wasm --features function-catalog-clickhouse
cargo check -p polyglot-sql-wasm --features function-catalog-duckdb
cargo check -p polyglot-sql-ffi --features function-catalog-clickhouse
cargo check -p polyglot-sql-ffi --features function-catalog-duckdb
cargo check -p polyglot-sql-python --features function-catalog-clickhouse
cargo check -p polyglot-sql-python --features function-catalog-duckdb
```

## Adding A New Dialect Function List

This crate is intentionally feature-gated so large function datasets do not inflate every binary.

### 1. Add a new dialect source file

Create `src/<dialect>.rs` and expose `register<S: CatalogSink>(sink: &mut S)`.

```rust
use crate::{CatalogSink, FunctionNameCase, FunctionSignature};

pub(crate) fn register<S: CatalogSink>(sink: &mut S) {
    let d = "postgresql";

    // Dialect-level default casing policy.
    sink.set_dialect_name_case(d, FunctionNameCase::Insensitive);

    // Optional function-level casing override.
    // sink.set_function_name_case(d, "SpecialFn", FunctionNameCase::Sensitive);

    sink.register(d, "abs", vec![FunctionSignature::exact(1)]);
    sink.register(d, "coalesce", vec![FunctionSignature::variadic(1)]);
    sink.register(d, "date_trunc", vec![FunctionSignature::exact(2)]);
}
```

### 2. Wire the module in `src/lib.rs`

```rust
#[cfg(feature = "dialect-postgresql")]
mod postgresql;

pub fn register_enabled_catalogs<S: CatalogSink>(sink: &mut S) {
    #[cfg(feature = "dialect-postgresql")]
    postgresql::register(sink);
}
```

### 3. Add feature flags in `Cargo.toml`

```toml
[features]
default = []
dialect-clickhouse = []
dialect-duckdb = []
all-dialects = ["dialect-clickhouse", "dialect-duckdb"]
```

### 4. (Optional) Add extraction tooling

Put source-specific extraction scripts in:

- `tools/<dialect>/extract_functions.py`

Current example path:

- `tools/clickhouse/extract_functions.py`
- `tools/duckdb/extract_functions.py`

ClickHouse extraction command (uses `chdb` via `uv run`, no local install required):

```bash
uv run --with chdb python crates/polyglot-sql-function-catalogs/tools/clickhouse/extract_functions.py \
  --output crates/polyglot-sql-function-catalogs/src/clickhouse.rs
```

DuckDB extraction command (requires Python package `duckdb`, installed on demand):

```bash
uv run --with duckdb python crates/polyglot-sql-function-catalogs/tools/duckdb/extract_functions.py \
  --output crates/polyglot-sql-function-catalogs/src/duckdb.rs
```

Useful optional flags:

- `--exclude-internal`: drop rows where `internal = true`
- `--function-type <type>` (repeatable): filter to specific `function_type` values

## How This Crate Is Wired To Other Crates

### `polyglot-sql` (core)

- Core has an optional dependency on this crate.
- Core features:
  - `function-catalog-clickhouse`
  - `function-catalog-all-dialects`
- When one of these features is enabled, core builds an embedded catalog once
  and auto-uses it for schema validation type checks (unless caller provides a custom catalog).
- Runtime override hook still exists via `SchemaValidationOptions.function_catalog`.

### `polyglot-sql-wasm`

- WASM forwards function-catalog features to `polyglot-sql`:
  - `function-catalog-clickhouse`
  - `function-catalog-all-dialects`
- Uses core schema validation, so catalog checks apply when `check_types` is enabled.
- If disabled, behavior is unchanged.

### Other bindings (`ffi`, `python`, `sdk`)

- `ffi` and `python` can enable core catalog features at compile time via pass-through features.
- Today, their exposed `validate` APIs are syntax-only, so catalog checks are effectively dormant
  until schema-aware validation APIs are exposed there.
- `sdk` consumes WASM and therefore follows WASM feature behavior.
- No direct dependency on this crate is required in those bindings.
