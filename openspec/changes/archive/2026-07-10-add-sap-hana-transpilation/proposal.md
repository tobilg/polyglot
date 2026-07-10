## Why

Phase 1 (`add-sap-hana-dialect`) established the HANA dialect scaffold with tokenizer, generator config, and identity round-trip support. However, HANA SQL cannot yet be transpiled to other dialects because no function transforms or type mappings are implemented. The primary use case is transpiling HANA Cloud SQL to Trino for data lakehouse federation and migration.

## What Changes

- Implement `transform_expr()` in `crates/polyglot-sql/src/dialects/hana.rs` with ~40 HANA-specific function transforms:
  - Date arithmetic: ADD_DAYS, ADD_MONTHS, ADD_SECONDS, ADD_YEARS → DATE_ADD with string unit
  - Date diff: DAYS_BETWEEN, MONTHS_BETWEEN, SECONDS_BETWEEN, YEARS_BETWEEN → DATE_DIFF with string unit
  - String: SUBSTR (pass-through, Trino handles), LCASE→LOWER, UCASE→UPPER, LOCATE (arg swap)→STRPOS
  - Null/conditional: NVL→COALESCE, IFNULL→COALESCE, IF(cond,a,b)→CASE WHEN
  - Conversion: TO_VARCHAR(no fmt)→CAST, TO_INTEGER→CAST, TO_DECIMAL→CAST, TO_REAL→CAST, TO_DOUBLE→CAST
  - Conversion with format: TO_VARCHAR(d,fmt)→DATE_FORMAT, TO_DATE(s,fmt)→DATE_PARSE, TO_TIMESTAMP(s,fmt)→DATE_PARSE
  - Datetime constants: CURRENT_UTCTIMESTAMP→CURRENT_TIMESTAMP, CURRENT_UTCDATE→CURRENT_DATE, CURRENT_UTCTIME→CURRENT_TIME, NOW→CURRENT_TIMESTAMP, SYSDATE→CURRENT_TIMESTAMP
  - Numeric: TRUNC(x,n)→TRUNCATE, BITAND→BITWISE_AND, BITOR→BITWISE_OR, BITNOT→BITWISE_NOT
  - Hex: HEX_TO_VARCHAR→FROM_HEX
  - ILIKE→LOWER() LIKE LOWER() (HANA doesn't support ILIKE)
  - TRY_CAST pass-through (both HANA and Trino support it)
- Implement `convert_hana_to_java_format()` for Oracle-style → Java SimpleDateFormat token conversion
- Add HANA-specific type mappings to `crates/polyglot-sql/src/generator.rs` `DataType::Custom` branch: SMALLDECIMAL→DECIMAL, SECONDDATE→TIMESTAMP, ALPHANUM→VARCHAR, NVARCHAR→VARCHAR, CLOB→VARCHAR, NCLOB→VARCHAR, BINARY→VARBINARY, FLOAT→DOUBLE (with precision/length args preserved)
- Create additional test fixtures: `transpilation.json` (HANA→Trino with edge cases), `types.json` (type mapping tests), `functions.json` (function identity tests)

## Capabilities

### New Capabilities
- `hana-transpilation`: HANA Cloud SQL function transforms, date format conversion, and type mappings that enable transpiling HANA SQL to Trino and other supported dialects.

### Modified Capabilities
- `hana-dialect`: The `HanaDialect` struct gains `transform_expr()` overrides (previously no-op in Phase 1). This changes the dialect's behavior from identity-only to supporting cross-dialect transpilation.

## Impact

- **Rust core** (`crates/polyglot-sql`): `hana.rs` grows from ~30 lines (scaffold) to ~500-700 lines (transforms + format conversion); `generator.rs` gets ~20-30 lines added to `DataType::Custom` branch
- **No new files**: all changes are additions to existing files from Phase 1
- **Tests**: 3 new fixture files in `tests/custom_fixtures/hana/` (transpilation, types, functions)
- **No breaking changes**: existing identity tests continue to pass; new transpilation tests verify HANA→Trino correctness
- **Depends on**: `add-sap-hana-dialect` (Phase 1) must be completed first
