## Why

Polyglot supports 33 SQL dialects but lacks SAP HANA, an enterprise database used by many large organizations. Users need to parse and round-trip HANA Cloud SQL as a first step toward transpiling to Trino for data lakehouse federation and migration scenarios. There is no existing HANA dialect in polyglot or in SQLGlot (polyglot's primary reference), so this requires building from the SAP HANA Cloud SQL Reference Guide.

This is **Phase 1** of a two-phase effort. Phase 1 delivers a functional HANA dialect that can parse and round-trip HANA SQL (identity tests). Phase 2 (`add-sap-hana-transpilation`) will add the ~40 function transforms, date format conversion, and type mappings needed for HANA→Trino transpilation.

## What Changes

- Add `HANA` as a new `DialectType` variant with feature gate `dialect-hana`
- Create `crates/polyglot-sql/src/dialects/hana.rs` implementing `DialectImpl`
  - Tokenizer: double-quote identifiers, standard SQL string quoting (`''` escape), no nested comment support
  - Generator config: double-quote identifiers, uppercase keywords, HANA-compatible settings (null ordering, limit style, interval syntax)
  - No `transform_expr` overrides in Phase 1 (all defaults — identity round-trip only)
- Register HANA in all 7 touch points within `dialects/mod.rs` (module, re-export, enum, Display, FromStr, cached_dialect, configs_for_dialect_type)
- Add `dialect-hana` feature to `crates/polyglot-sql/Cargo.toml` `all-dialects` list
- Add `dialect-hana` feature passthrough to `crates/polyglot-sql-wasm/Cargo.toml`
- Add `DialectType::HANA` to FFI `DIALECTS` array in `crates/polyglot-sql-ffi/src/dialects.rs` (count 34→35)
- Add `HANA = 'hana'` to TypeScript `DialectType` enum in `packages/sdk/src/index.ts`
- Add `"hana"` to the Python binding's hardcoded `DIALECT_NAMES` list in `crates/polyglot-sql-python/src/dialects.rs`
- Add HANA generator arms in `generator.rs` needed for identity: `INT`→`INTEGER` canonical form and `NVARCHAR` preservation
- Create custom test fixtures in `crates/polyglot-sql/tests/custom_fixtures/hana/` for identity, select, DDL, and DML categories (no transpilation fixtures yet — those come in Phase 2)

### Non-Goals (Phase 1)

- Function transforms (ADD_DAYS, NVL, TO_VARCHAR, etc.) — Phase 2
- Date format string conversion — Phase 2
- Generator type mapping for HANA-specific types (SMALLDECIMAL, SECONDDATE, etc.) — Phase 2
- HANA→Trino transpilation verification — Phase 2
- SQLScript procedural language
- WITH HINT, CONTAINS/FUZZY, graph, spatial methods

## Capabilities

### New Capabilities
- `hana-dialect`: SAP HANA Cloud SQL dialect — tokenizer, generator config, identifier handling, and dialect registration for parsing HANA Cloud SQL and round-tripping it as identity. Transform and transpilation support is added in a follow-up change.

### Modified Capabilities
- (none — no existing specs are changing)

## Impact

- **Rust core** (`crates/polyglot-sql`): new dialect module, `Cargo.toml` feature, `dialects/mod.rs` registration
- **WASM crate** (`crates/polyglot-sql-wasm`): `Cargo.toml` feature passthrough
- **FFI crate** (`crates/polyglot-sql-ffi`): `DIALECTS` array size change (34→35), no API breaking change
- **TypeScript SDK** (`packages/sdk`): new enum variant in `DialectType`, no breaking change
- **Go package**: no changes (dialects fetched dynamically via FFI)
- **Python package** (`crates/polyglot-sql-python`): add `"hana"` to the hardcoded `DIALECT_NAMES` list in `src/dialects.rs` and assert it in `tests/test_dialects.py`
- **Generator** (`crates/polyglot-sql/src/generator.rs`): two HANA-specific arms required for identity round-trip — canonicalize `INT` to `INTEGER`, and preserve `NVARCHAR` (the default arm folds it to `VARCHAR`)
- **Docs/playground**: dialect lists and counts updated in READMEs, Python docs, and playground `DIALECT_DISPLAY_NAMES`
- **Tests**: new `tests/custom_fixtures/hana/` directory with 4 JSON fixture files (identity, select, ddl, dml); auto-discovered by existing `custom_dialect_tests.rs` runner
- **No breaking changes**: all additions are additive; existing dialects and APIs remain unchanged
