## 1. Write Test Fixtures (RED phase — tests will fail)

Write all transpilation/type/function test fixtures BEFORE implementing transforms. These define the expected HANA→Trino behavior and will initially fail because `hana.rs` has no `transform_expr` yet.

- [x] 1.1 Create `tests/custom_fixtures/hana/transpilation.json` — HANA→Trino transpilation tests covering ALL function transforms with edge cases: negative date values (`ADD_DAYS(d, -1)`), zero values (`ADD_DAYS(d, 0)`), nested functions (`UPPER(ADD_DAYS(d,7))`, `ADD_DAYS(TO_DATE(s,fmt),7)`), functions in WHERE clauses, single-arg conversions (`TO_DATE(s)` without format), NULL literal args (`NVL(NULL, b)`), nested NVL, mixed-case function names (`add_days`), `write` targets for `trino`
- [x] 1.2 Create `tests/custom_fixtures/hana/types.json` — data type transpilation tests including edge cases: SMALLDECIMAL with and without precision args, NVARCHAR(255) vs NVARCHAR, ALPHANUM(10), SECONDDATE, CLOB, NCLOB, BINARY, FLOAT, standard types, types in CAST and CREATE TABLE contexts, `write` targets for `trino`
- [x] 1.3 Create `tests/custom_fixtures/hana/functions.json` — HANA function identity tests (HANA→HANA round-trip): ADD_DAYS, ADD_MONTHS, DAYS_BETWEEN, TO_VARCHAR, NVL, SUBSTR, LCASE, UCASE, LOCATE, CURRENT_UTCTIMESTAMP, TRUNC, BITAND, HEX_TO_VARCHAR, including mixed-case function names
- [x] 1.4 Run `cargo test -p polyglot-sql --test custom_dialect_tests -- --nocapture` and confirm new transpilation tests fail (expected — transforms not implemented), Phase 1 identity tests still pass

## 2. Implement Date Arithmetic Transforms (GREEN — incremental)

- [x] 2.1 Implement `transform_expr()` entry point and `transform_function()` helper for generic Function matching in `hana.rs`
- [x] 2.2 Add ADD_DAYS→`DATE_ADD('day', n, d)`, ADD_MONTHS→`DATE_ADD('month', n, d)`, ADD_SECONDS→`DATE_ADD('second', n, d)`, ADD_YEARS→`DATE_ADD('year', n, d)`
- [x] 2.3 Add DAYS_BETWEEN→`DATE_DIFF('day', d1, d2)`, MONTHS_BETWEEN→`DATE_DIFF('month', d1, d2)`, SECONDS_BETWEEN→`DATE_DIFF('second', d1, d2)`, YEARS_BETWEEN→`DATE_DIFF('year', d1, d2)`
- [x] 2.4 Run transpilation tests for date arithmetic — confirm all date-related transpilation tests pass including edge cases (negative, zero, nested, WHERE clause, mixed-case)

## 3. Implement String and Null/Conditional Transforms (GREEN — incremental)

- [x] 3.1 Add LCASE→`LOWER()`, UCASE→`UPPER()`, LOCATE (arg swap)→`STRPOS()` in `transform_function()`
- [x] 3.2 Add NVL→`COALESCE()`, IFNULL→`COALESCE()`, IF(cond,a,b)→`CASE WHEN cond THEN a ELSE b END` in `transform_expr()`
- [x] 3.3 Run transpilation tests for string/null/conditional — confirm all pass including edge cases (nested NVL, NULL literal arg, NVL in WHERE, SUBSTR with 2 args, LCASE nested in UCASE)

## 4. Implement Conversion and Datetime Constant Transforms (GREEN — incremental)

- [x] 4.1 Add TO_VARCHAR(no fmt)→`CAST AS VARCHAR`, TO_INTEGER→`CAST AS INTEGER`, TO_DECIMAL→`CAST AS DECIMAL`, TO_REAL→`CAST AS REAL`, TO_DOUBLE→`CAST AS DOUBLE` in `transform_function()`
- [x] 4.2 Add TO_VARCHAR(d,fmt)→`DATE_FORMAT(d, <converted fmt>)`, TO_DATE(s,fmt)→`DATE_PARSE(s, <converted fmt>)`, TO_TIMESTAMP(s,fmt)→`DATE_PARSE(s, <converted fmt>)` — depends on date format converter (step 5)
- [x] 4.3 Add TO_DATE(single arg)→`CAST AS DATE`, TO_TIMESTAMP(single arg)→`CAST AS TIMESTAMP`
- [x] 4.4 Add CURRENT_UTCTIMESTAMP→`CurrentTimestamp`, CURRENT_UTCDATE→`CurrentDate`, CURRENT_UTCTIME→`CurrentTime`, NOW→`CurrentTimestamp`, SYSDATE→`CurrentTimestamp` in `transform_expr()` and `transform_function()`
- [x] 4.5 Run transpilation tests for conversions/datetime — confirm all pass including edge cases (single-arg vs two-arg, nested TO_DATE inside ADD_DAYS, CURRENT_UTCTIMESTAMP in expressions, CURRENT_UTCTIMESTAMP with parens)

## 5. Implement Date Format Conversion (GREEN — incremental)

- [x] 5.1 Implement `convert_hana_to_java_format()` function in `hana.rs`: YYYY→yyyy, YY→yy, MM→MM, DD→dd, HH24→HH, HH12→hh, MI→mm, SS→ss, FF3→SSS, FF6→SSSSSS, DAY→EEEE, DY→EEE, MONTH→MMMM, MON→MMM, AM/PM→a
- [x] 5.2 Handle edge cases: unknown tokens pass through, literal text between tokens preserved, empty string, no-token strings
- [x] 5.3 Run transpilation tests for format conversion — confirm all pass including edge cases (12-hour with AM/PM, fractional seconds, two-digit year, slash separators, unknown tokens, empty format, literal-only format)

## 6. Implement Remaining Transforms (GREEN — incremental)

- [x] 6.1 Add ILIKE→`LOWER() LIKE LOWER()` in `transform_expr()` (handle NOT ILIKE)
- [x] 6.2 Add TRUNC(x,n)→`TRUNCATE(x,n)`, BITAND→`BITWISE_AND`, BITOR→`BITWISE_OR`, BITNOT→`BITWISE_NOT` in `transform_function()`
- [x] 6.3 Add HEX_TO_VARCHAR→`FROM_HEX` in `transform_function()`
- [x] 6.4 Add TRY_CAST pass-through (HANA supports it, no transform needed — verify it works)
- [x] 6.5 Run ALL transpilation tests — confirm 100% pass rate

## 7. Implement Generator Type Mapping (GREEN — incremental)

- [x] 7.1 Add HANA-specific type mappings to `crates/polyglot-sql/src/generator.rs` `DataType::Custom` branch: SMALLDECIMAL→DECIMAL, SECONDDATE→TIMESTAMP, ALPHANUM→VARCHAR, NVARCHAR→VARCHAR, CLOB→VARCHAR, NCLOB→VARCHAR, BINARY→VARBINARY, FLOAT→DOUBLE (with precision/length args preserved)
- [x] 7.2 Run type transpilation tests — confirm all pass including edge cases (types with args, types in CREATE TABLE, types in CAST)

## 8. Full Verification and Refactor

- [x] 8.1 Run `cargo test -p polyglot-sql --test custom_dialect_tests -- --nocapture` — ALL HANA fixtures pass (Phase 1 identity/select/ddl/dml + Phase 2 transpilation/types/functions)
- [x] 8.2 Run `cargo test -p polyglot-sql --lib` — no regressions in existing tests
- [x] 8.3 Run `cargo test -p polyglot-sql --features all-dialects` — full test suite passes
- [x] 8.4 Run `cargo clippy --all` — no warnings in hana.rs or generator.rs changes
- [x] 8.5 Run `make fmt` — formatting clean
- [x] 8.6 Review hana.rs for code quality: consistent match arm ordering, no dead code, all transforms have corresponding test coverage, edge case comments where logic is non-obvious
- [x] 8.7 Final review: verify every Phase 2 spec scenario has a corresponding test case in fixtures, no gaps between spec and tests
