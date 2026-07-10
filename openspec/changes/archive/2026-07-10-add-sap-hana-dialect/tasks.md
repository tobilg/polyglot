## 1. Scaffold and Feature Registration

- [x] 1.1 Add `dialect-hana = []` feature to `crates/polyglot-sql/Cargo.toml` and add `dialect-hana` to `all-dialects` list
- [x] 1.2 Add `dialect-hana` passthrough and `all-dialects` entry to `crates/polyglot-sql-wasm/Cargo.toml`
- [x] 1.3 Add `HANA` variant to `DialectType` enum, `Display` impl (`"hana"`), `FromStr` impl (`"hana" | "saphana" | "sap_hana"`) in `crates/polyglot-sql/src/dialects/mod.rs`
- [x] 1.4 Add `#[cfg(feature = "dialect-hana")] mod hana;` and `pub use hana::HanaDialect;` to `mod.rs`
- [x] 1.5 Add `cached_dialect!(CACHED_HANA, HanaDialect, "dialect-hana");` and `configs_for_dialect_type` match arm to `mod.rs`
- [x] 1.6 Create minimal `crates/polyglot-sql/src/dialects/hana.rs` — empty `HanaDialect` struct, `DialectImpl` impl with only `dialect_type()` returning `DialectType::HANA` (no transforms, all defaults)
- [x] 1.7 Verify `cargo test -p polyglot-sql --lib` compiles and passes (no regressions from scaffold)

## 2. Write Test Fixtures (RED phase — tests define expected behavior)

Write all test fixtures BEFORE implementing tokenizer/generator config. Some identity tests may fail initially if config is wrong; this confirms the tests are exercising the right behavior.

- [x] 2.1 Create `tests/custom_fixtures/hana/identity.json` — identity round-trip tests with edge cases: escaped single quotes (`'it''s'`), NULL literal, double-quoted reserved words (`"SELECT"`, `"ORDER"`), double-quoted mixed-case identifiers (`"MyColumn"`), multiple JOINs, nested subqueries, CTEs, window functions with PARTITION BY + ORDER BY, UNION ALL, LIMIT/OFFSET, CASE WHEN, LEFT JOIN with IS NULL, COALESCE, HANA native function pass-through (ADD_DAYS, NVL, SUBSTR — identity only, no transforms)
- [x] 2.2 Create `tests/custom_fixtures/hana/select.json` — SELECT-specific identity: DISTINCT, ALL, column aliases, star, expressions, CASE WHEN with multiple branches, COALESCE in SELECT, string literals
- [x] 2.3 Create `tests/custom_fixtures/hana/ddl.json` — CREATE TABLE identity with HANA types (SMALLDECIMAL, SECONDDATE, NVARCHAR, ALPHANUM, CLOB), types with precision/length args (NVARCHAR(255), SMALLDECIMAL(10,2)), CREATE TABLE with constraints (NOT NULL, PRIMARY KEY), DROP TABLE, ALTER TABLE ADD COLUMN
- [x] 2.3a Add unit tests in `dialects/mod.rs` for HANA `FromStr` (aliases `saphana`/`sap_hana`, case-insensitivity, `hanacloud` error) and `Display`
- [x] 2.3b Add identity fixture cases covering comments (single-line, multi-line, non-nested) and lowercase-keyword input (uppercase keyword output)
- [x] 2.4 Create `tests/custom_fixtures/hana/dml.json` — INSERT with VALUES, INSERT with multiple rows, INSERT with SELECT, INSERT with explicit column list, UPDATE with WHERE, DELETE with WHERE
- [x] 2.5 Run `cargo test -p polyglot-sql --test custom_dialect_tests -- --nocapture` and confirm HANA tests are discovered; note which identity tests fail (expected — tokenizer/generator config not yet implemented)

## 3. Implement Tokenizer and Generator Config (GREEN phase)

- [x] 3.1 Implement `tokenizer_config()` in `hana.rs`: double-quote identifiers, no nested comments, standard SQL string escapes (`''`)
- [x] 3.2 Implement `generator_config()` in `hana.rs`: double-quote identifier quoting, uppercase keywords, HANA-compatible null ordering, limit style, interval syntax (verify against HANA Cloud docs)
- [x] 3.2a Add HANA arm in `generator.rs` `DataType::Int` rendering: canonicalize `INT` to `INTEGER` (HANA's canonical integer type)
- [x] 3.2b Add HANA arm in `generator.rs` `NVARCHAR` rendering: preserve `NVARCHAR` (default arm folds it to `VARCHAR`, breaking DDL identity)
- [x] 3.2c Register `"hana" | "saphana" | "sap_hana"` in `tests/common/test_runner.rs` `parse_dialect` so fixtures are discovered
- [x] 3.3 Run `cargo test -p polyglot-sql --test custom_dialect_tests -- --nocapture` — all HANA identity/select/ddl/dml tests must pass
- [x] 3.4 Run `cargo test -p polyglot-sql --lib` — no regressions

## 4. FFI and SDK Registration

- [x] 4.1 Add `DialectType::HANA` to `DIALECTS` array in `crates/polyglot-sql-ffi/src/dialects.rs`, update array size 34→35
- [x] 4.2 Add `HANA = 'hana'` to TypeScript `DialectType` enum in `packages/sdk/src/index.ts`
- [x] 4.2a Add `"hana"` to Python `DIALECT_NAMES` in `crates/polyglot-sql-python/src/dialects.rs` and assert it in `tests/test_dialects.py`
- [x] 4.2b Assert `"hana"` in FFI `test_dialect_list_and_count` in `crates/polyglot-sql-ffi/tests/ffi_tests.rs`
- [x] 4.2c Add `hana` display name to playground `DIALECT_DISPLAY_NAMES` and update dialect lists/counts in READMEs and Python docs
- [x] 4.3 Verify FFI build: `cargo build -p polyglot-sql-ffi --profile ffi_release`
- [x] 4.4 Verify SDK typecheck: `cd packages/sdk && pnpm typecheck`

## 5. Full Verification and Refactor

- [x] 5.1 Run `cargo test -p polyglot-sql --test custom_dialect_tests -- --nocapture` — all HANA fixtures pass (identity + select + ddl + dml)
- [x] 5.2 Run `cargo test -p polyglot-sql --lib` — no regressions
- [x] 5.3 Run `cargo test -p polyglot-sql --features all-dialects` — full test suite passes
- [x] 5.4 Run `cargo clippy --all` — no warnings in hana.rs or related changes
- [x] 5.5 Run `make fmt` — formatting clean
- [x] 5.6 Final review: verify every spec scenario has a corresponding test case in fixtures, no gaps between spec and tests
