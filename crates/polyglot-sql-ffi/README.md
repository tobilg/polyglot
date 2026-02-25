# polyglot-sql-ffi

C Foreign Function Interface bindings for `polyglot-sql`.

`polyglot-sql-ffi` exposes core parsing/transpilation/formatting/validation/analysis features through a small, stable C ABI. It is intended to be used as the native layer for language-specific wrappers (Python, Go, Java, C#, Swift, etc.).

## What It Provides

- Shared and static libraries:
  - Linux: `.so` and `.a`
  - macOS: `.dylib` and `.a`
  - Windows: `.dll` and `.lib`
- Auto-generated C header:
  - `polyglot_sql.h`
- String-oriented API with JSON payloads for complex data
- Explicit memory ownership helpers
- Panic-protected FFI boundaries with status codes

## Build

Use the dedicated unwind profile for FFI builds:

```bash
cargo build -p polyglot-sql-ffi --profile ffi_release
```

This profile inherits from `release` and sets `panic = "unwind"` to ensure panics never unwind across FFI.

### Output Paths

- Local (no explicit target triple):
  - `target/ffi_release/`
- Cross-target:
  - `target/<target-triple>/ffi_release/`

### Header Generation

The header is generated automatically by `build.rs` (using `cbindgen`) when the crate is built:

- `crates/polyglot-sql-ffi/polyglot_sql.h`

It is intentionally not tracked in git.

## Quick C Example

See `examples/c/main.c` for a full end-to-end sample.

Minimal usage:

```c
#include "polyglot_sql.h"
#include <stdio.h>

int main(void) {
    polyglot_result_t result = polyglot_transpile(
        "SELECT IFNULL(a, b) FROM t",
        "mysql",
        "postgres"
    );

    if (result.status == 0) {
        printf("%s\n", result.data); // JSON array of SQL strings
    } else {
        printf("Error (%d): %s\n", result.status, result.error);
    }

    polyglot_free_result(result);
    return 0;
}
```

## API Design

### Core Return Type

Most functions return:

```c
typedef struct {
    char *data;   // owned by caller
    char *error;  // owned by caller, NULL on success
    int32_t status;
} polyglot_result_t;
```

Status `0` means success. On failure, `data == NULL` and `error` contains details.

### Validation Return Type

```c
typedef struct {
    int32_t valid;       // 1 valid, 0 invalid
    char *errors_json;   // owned by caller
    char *error;         // top-level error message (optional)
    int32_t status;
} polyglot_validation_result_t;
```

### Exported Functions

- `polyglot_transpile(sql, from_dialect, to_dialect)`
- `polyglot_parse(sql, dialect)`
- `polyglot_parse_one(sql, dialect)`
- `polyglot_generate(ast_json, dialect)` (expects `Vec<Expression>` JSON)
- `polyglot_format(sql, dialect)`
- `polyglot_format_with_options(sql, dialect, options_json)` (`FormatGuardOptions` JSON)
- `polyglot_validate(sql, dialect)`
- `polyglot_optimize(sql, dialect)` (full optimizer pipeline)
- `polyglot_lineage(column_name, sql, dialect)`
- `polyglot_source_tables(column_name, sql, dialect)`
- `polyglot_diff(sql1, sql2, dialect)`
- `polyglot_dialect_list()`
- `polyglot_dialect_count()`
- `polyglot_version()`
- `polyglot_free_string()`
- `polyglot_free_result()`
- `polyglot_free_validation_result()`

### Formatting Guard Behavior

`polyglot_format` uses Rust core formatting guards with default limits:
- input bytes: `16 * 1024 * 1024`
- tokens: `1_000_000`
- AST nodes: `1_000_000`
- set-op chain: `256`

When a guard is exceeded, `status != 0` and `error` contains one of:
- `E_GUARD_INPUT_TOO_LARGE`
- `E_GUARD_TOKEN_BUDGET_EXCEEDED`
- `E_GUARD_AST_BUDGET_EXCEEDED`
- `E_GUARD_SET_OP_CHAIN_EXCEEDED`

```c
#include "polyglot_sql.h"
#include <stdio.h>
#include <string.h>

polyglot_result_t r = polyglot_format("SELECT 1", "generic");
if (r.status != 0 && r.error && strstr(r.error, "E_GUARD_") != NULL) {
    printf("Formatting guard triggered: %s\n", r.error);
}
polyglot_free_result(r);
```

Per-call overrides are supported via `polyglot_format_with_options`:

```c
const char *opts = "{\"maxSetOpChain\":1024,\"maxInputBytes\":33554432}";
polyglot_result_t r = polyglot_format_with_options(sql, "generic", opts);
```

## JSON Payload Contracts

### Success payloads (`polyglot_result_t.data`)

- `polyglot_transpile`: JSON array of SQL strings
- `polyglot_parse`: JSON `Vec<Expression>`
- `polyglot_parse_one`: JSON `Expression`
- `polyglot_generate`: JSON array of SQL strings
- `polyglot_format`: JSON array of SQL strings
- `polyglot_format_with_options`: JSON array of SQL strings
- `polyglot_optimize`: JSON array of SQL strings
- `polyglot_lineage`: JSON `LineageNode`
- `polyglot_source_tables`: JSON array of source table names
- `polyglot_diff`: JSON array of diff edits
- `polyglot_dialect_list`: JSON array of dialect names

### Validation payloads

- `errors_json`: JSON array of validation error objects:
  - `message`
  - optional `line`
  - optional `column`
  - `severity`
  - `code`

## Error Codes

- `0`: success
- `1`: parse error
- `2`: generate error
- `3`: transpile error
- `4`: validation error
- `5`: invalid argument (NULL pointer, bad dialect, invalid UTF-8)
- `6`: JSON serialization/deserialization error
- `99`: internal panic/error

## Memory Ownership Rules

- Free every returned `char *` with `polyglot_free_string`.
- Free every `polyglot_result_t` with `polyglot_free_result`.
- Free every `polyglot_validation_result_t` with `polyglot_free_validation_result`.
- `polyglot_version()` returns a static pointer. Do not free.

## Dialect Names

Dialect identifiers are string names used in core `polyglot-sql`, for example:

- `generic`
- `postgres` / `postgresql`
- `mysql`
- `bigquery`
- `snowflake`
- `duckdb`
- `clickhouse`

For a complete runtime list, call `polyglot_dialect_list()`.

## Thread Safety

The FFI layer does not maintain mutable global state. Calls are safe to use from multiple threads concurrently.

## Make Targets

From repo root:

- `make build-ffi`
- `make build-ffi-static`
- `make generate-ffi-header`
- `make build-ffi-example`
- `make test-ffi`
- `make clean-ffi`

## Release Artifacts

For `v*` tags, CI publishes prebuilt FFI archives and `checksums.sha256` to the corresponding GitHub release.

Expected archive naming:

- `polyglot-sql-ffi-linux-x86_64.tar.gz`
- `polyglot-sql-ffi-linux-aarch64.tar.gz`
- `polyglot-sql-ffi-macos-x86_64.tar.gz`
- `polyglot-sql-ffi-macos-aarch64.tar.gz`
- `polyglot-sql-ffi-windows-x86_64.zip`

## Language Wrapper Guidance

- Python: load shared library via `ctypes`/`cffi`, parse JSON to Python objects
- Go: use `cgo`, convert `char*` via `C.GoString`, always call free helpers
- C#: P/Invoke with `IntPtr` + marshaling + explicit free calls
- Java: JNA/JNI wrapper around C signatures, parse JSON in JVM layer

Keep wrappers thin and treat this crate as the single source of behavior.
