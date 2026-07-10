## ADDED Requirements

### Requirement: HANA date arithmetic function transforms
The system SHALL transform HANA date arithmetic functions to generic `DATE_ADD` / `DATE_DIFF` functions with string unit arguments during `transform_expr`.

#### Scenario: ADD_DAYS to DATE_ADD
- **WHEN** transpiling `SELECT ADD_DAYS(CURRENT_DATE, 7) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_ADD('day', 7, CURRENT_DATE) FROM t`

#### Scenario: ADD_DAYS with negative value
- **WHEN** transpiling `SELECT ADD_DAYS(d, -1) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_ADD('day', -1, d) FROM t`

#### Scenario: ADD_DAYS with zero
- **WHEN** transpiling `SELECT ADD_DAYS(d, 0) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_ADD('day', 0, d) FROM t`

#### Scenario: ADD_DAYS nested in another function
- **WHEN** transpiling `SELECT UPPER(ADD_DAYS(d, 7)) FROM t` from HANA to Trino
- **THEN** the output is `SELECT UPPER(DATE_ADD('day', 7, d)) FROM t`

#### Scenario: ADD_DAYS in WHERE clause
- **WHEN** transpiling `SELECT * FROM t WHERE d > ADD_DAYS(CURRENT_DATE, -30)` from HANA to Trino
- **THEN** the output is `SELECT * FROM t WHERE d > DATE_ADD('day', -30, CURRENT_DATE)`

#### Scenario: ADD_MONTHS to DATE_ADD
- **WHEN** transpiling `SELECT ADD_MONTHS(CURRENT_DATE, 3) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_ADD('month', 3, CURRENT_DATE) FROM t`

#### Scenario: ADD_SECONDS to DATE_ADD
- **WHEN** transpiling `SELECT ADD_SECONDS(ts, 30) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_ADD('second', 30, ts) FROM t`

#### Scenario: ADD_YEARS to DATE_ADD
- **WHEN** transpiling `SELECT ADD_YEARS(d, 1) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_ADD('year', 1, d) FROM t`

#### Scenario: Mixed case function name add_days
- **WHEN** transpiling `SELECT add_days(d, 7) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_ADD('day', 7, d) FROM t` (case-insensitive match on function name)

#### Scenario: DAYS_BETWEEN to DATE_DIFF
- **WHEN** transpiling `SELECT DAYS_BETWEEN(d1, d2) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_DIFF('day', d1, d2) FROM t`

#### Scenario: MONTHS_BETWEEN to DATE_DIFF
- **WHEN** transpiling `SELECT MONTHS_BETWEEN(d1, d2) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_DIFF('month', d1, d2) FROM t`

#### Scenario: SECONDS_BETWEEN to DATE_DIFF
- **WHEN** transpiling `SELECT SECONDS_BETWEEN(d1, d2) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_DIFF('second', d1, d2) FROM t`

#### Scenario: YEARS_BETWEEN to DATE_DIFF
- **WHEN** transpiling `SELECT YEARS_BETWEEN(d1, d2) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_DIFF('year', d1, d2) FROM t`

#### Scenario: DAYS_BETWEEN with column expressions
- **WHEN** transpiling `SELECT DAYS_BETWEEN(start_date, end_date) AS duration FROM events` from HANA to Trino
- **THEN** the output is `SELECT DATE_DIFF('day', start_date, end_date) AS duration FROM events`

### Requirement: HANA string function transforms
The system SHALL transform HANA string functions to their generic or Trino-compatible equivalents during `transform_expr`.

#### Scenario: SUBSTR to SUBSTRING
- **WHEN** transpiling `SELECT SUBSTR(s, 1, 5) FROM t` from HANA to Trino
- **THEN** the output is `SELECT SUBSTRING(s, 1, 5) FROM t`

#### Scenario: SUBSTR with two args (no length)
- **WHEN** transpiling `SELECT SUBSTR(s, 3) FROM t` from HANA to Trino
- **THEN** the output is `SELECT SUBSTRING(s, 3) FROM t`

#### Scenario: LCASE to LOWER
- **WHEN** transpiling `SELECT LCASE(s) FROM t` from HANA to Trino
- **THEN** the output is `SELECT LOWER(s) FROM t`

#### Scenario: UCASE to UPPER
- **WHEN** transpiling `SELECT UCASE(s) FROM t` from HANA to Trino
- **THEN** the output is `SELECT UPPER(s) FROM t`

#### Scenario: LOCATE arg swap to STRPOS
- **WHEN** transpiling `SELECT LOCATE('abc', s) FROM t` from HANA to Trino (HANA LOCATE takes substring first, string second)
- **THEN** the output is `SELECT STRPOS(s, 'abc') FROM t`

#### Scenario: LCASE nested in UCASE
- **WHEN** transpiling `SELECT UCASE(LCASE(s)) FROM t` from HANA to Trino
- **THEN** the output is `SELECT UPPER(LOWER(s)) FROM t`

### Requirement: HANA null/conditional function transforms
The system SHALL transform HANA null-handling and conditional functions to generic AST equivalents during `transform_expr`.

#### Scenario: NVL to COALESCE
- **WHEN** transpiling `SELECT NVL(a, b) FROM t` from HANA to Trino
- **THEN** the output is `SELECT COALESCE(a, b) FROM t`

#### Scenario: IFNULL to COALESCE
- **WHEN** transpiling `SELECT IFNULL(a, b) FROM t` from HANA to Trino
- **THEN** the output is `SELECT COALESCE(a, b) FROM t`

#### Scenario: NVL with NULL literal as first arg
- **WHEN** transpiling `SELECT NVL(NULL, b) FROM t` from HANA to Trino
- **THEN** the output is `SELECT COALESCE(NULL, b) FROM t`

#### Scenario: NVL with column and literal
- **WHEN** transpiling `SELECT NVL(name, 'unknown') FROM employees` from HANA to Trino
- **THEN** the output is `SELECT COALESCE(name, 'unknown') FROM employees`

#### Scenario: IF to CASE WHEN
- **WHEN** transpiling `SELECT IF(cond, a, b) FROM t` from HANA to Trino
- **THEN** the output is `SELECT CASE WHEN cond THEN a ELSE b END FROM t`

#### Scenario: Nested NVL
- **WHEN** transpiling `SELECT NVL(NVL(a, b), c) FROM t` from HANA to Trino
- **THEN** the output is `SELECT COALESCE(COALESCE(a, b), c) FROM t`

#### Scenario: NVL in WHERE clause
- **WHEN** transpiling `SELECT * FROM t WHERE NVL(status, 0) = 1` from HANA to Trino
- **THEN** the output is `SELECT * FROM t WHERE COALESCE(status, 0) = 1`

### Requirement: HANA conversion function transforms
The system SHALL transform HANA type conversion functions to CAST or Trino-compatible functions during `transform_expr`.

#### Scenario: TO_VARCHAR without format
- **WHEN** transpiling `SELECT TO_VARCHAR(x) FROM t` from HANA to Trino
- **THEN** the output is `SELECT CAST(x AS VARCHAR) FROM t`

#### Scenario: TO_VARCHAR with numeric value (no format)
- **WHEN** transpiling `SELECT TO_VARCHAR(123) FROM t` from HANA to Trino
- **THEN** the output is `SELECT CAST(123 AS VARCHAR) FROM t`

#### Scenario: TO_VARCHAR with date format
- **WHEN** transpiling `SELECT TO_VARCHAR(d, 'YYYY-MM-DD') FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_FORMAT(d, 'yyyy-MM-dd') FROM t` (format converted to Java style)

#### Scenario: TO_DATE with format
- **WHEN** transpiling `SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD') FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_PARSE('2024-01-15', 'yyyy-MM-dd') FROM t`

#### Scenario: TO_DATE without format (single arg)
- **WHEN** transpiling `SELECT TO_DATE('2024-01-15') FROM t` from HANA to Trino
- **THEN** the output is `SELECT CAST('2024-01-15' AS DATE) FROM t`

#### Scenario: TO_TIMESTAMP with format
- **WHEN** transpiling `SELECT TO_TIMESTAMP('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS') FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_PARSE('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss') FROM t`

#### Scenario: TO_TIMESTAMP without format (single arg)
- **WHEN** transpiling `SELECT TO_TIMESTAMP('2024-01-15 10:30:00') FROM t` from HANA to Trino
- **THEN** the output is `SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP) FROM t`

#### Scenario: TO_INTEGER to CAST
- **WHEN** transpiling `SELECT TO_INTEGER(s) FROM t` from HANA to Trino
- **THEN** the output is `SELECT CAST(s AS INTEGER) FROM t`

#### Scenario: TO_DECIMAL to CAST
- **WHEN** transpiling `SELECT TO_DECIMAL(s) FROM t` from HANA to Trino
- **THEN** the output is `SELECT CAST(s AS DECIMAL) FROM t`

#### Scenario: TO_REAL to CAST
- **WHEN** transpiling `SELECT TO_REAL(s) FROM t` from HANA to Trino
- **THEN** the output is `SELECT CAST(s AS REAL) FROM t`

#### Scenario: TO_DOUBLE to CAST
- **WHEN** transpiling `SELECT TO_DOUBLE(s) FROM t` from HANA to Trino
- **THEN** the output is `SELECT CAST(s AS DOUBLE) FROM t`

#### Scenario: TO_VARCHAR with format in WHERE clause
- **WHEN** transpiling `SELECT * FROM t WHERE TO_VARCHAR(d, 'YYYY-MM') = '2024-01'` from HANA to Trino
- **THEN** the output is `SELECT * FROM t WHERE DATE_FORMAT(d, 'yyyy-MM') = '2024-01'`

#### Scenario: Nested TO_DATE inside ADD_DAYS
- **WHEN** transpiling `SELECT ADD_DAYS(TO_DATE('2024-01-15', 'YYYY-MM-DD'), 7) FROM t` from HANA to Trino
- **THEN** the output is `SELECT DATE_ADD('day', 7, DATE_PARSE('2024-01-15', 'yyyy-MM-dd')) FROM t`

### Requirement: HANA datetime constant transforms
The system SHALL transform HANA datetime constants to their generic equivalents during `transform_expr`.

#### Scenario: CURRENT_UTCTIMESTAMP
- **WHEN** transpiling `SELECT CURRENT_UTCTIMESTAMP FROM t` from HANA to Trino
- **THEN** the output is `SELECT CURRENT_TIMESTAMP FROM t`

#### Scenario: CURRENT_UTCTIMESTAMP with parentheses
- **WHEN** transpiling `SELECT CURRENT_UTCTIMESTAMP() FROM t` from HANA to Trino
- **THEN** the output is `SELECT CURRENT_TIMESTAMP FROM t`

#### Scenario: CURRENT_UTCDATE
- **WHEN** transpiling `SELECT CURRENT_UTCDATE FROM t` from HANA to Trino
- **THEN** the output is `SELECT CURRENT_DATE FROM t`

#### Scenario: CURRENT_UTCTIME
- **WHEN** transpiling `SELECT CURRENT_UTCTIME FROM t` from HANA to Trino
- **THEN** the output is `SELECT CURRENT_TIME FROM t`

#### Scenario: NOW
- **WHEN** transpiling `SELECT NOW() FROM t` from HANA to Trino
- **THEN** the output is `SELECT CURRENT_TIMESTAMP FROM t`

#### Scenario: SYSDATE
- **WHEN** transpiling `SELECT SYSDATE FROM t` from HANA to Trino
- **THEN** the output is `SELECT CURRENT_TIMESTAMP FROM t`

#### Scenario: CURRENT_UTCTIMESTAMP used in expression
- **WHEN** transpiling `SELECT EXTRACT(YEAR FROM CURRENT_UTCTIMESTAMP) FROM t` from HANA to Trino
- **THEN** the output is `SELECT EXTRACT(YEAR FROM CURRENT_TIMESTAMP) FROM t`

### Requirement: HANA data type mapping
The system SHALL map HANA-specific data types to Trino-compatible types during generation.

#### Scenario: SMALLDECIMAL to DECIMAL
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "SMALLDECIMAL" }`
- **THEN** the output type is `DECIMAL`

#### Scenario: SMALLDECIMAL with precision args
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "SMALLDECIMAL(10,2)" }`
- **THEN** the output type is `DECIMAL(10,2)` (precision args preserved)

#### Scenario: SECONDDATE to TIMESTAMP
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "SECONDDATE" }`
- **THEN** the output type is `TIMESTAMP`

#### Scenario: ALPHANUM to VARCHAR
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "ALPHANUM" }`
- **THEN** the output type is `VARCHAR`

#### Scenario: ALPHANUM with length
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "ALPHANUM(10)" }`
- **THEN** the output type is `VARCHAR(10)` (length preserved)

#### Scenario: NVARCHAR to VARCHAR
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "NVARCHAR" }`
- **THEN** the output type is `VARCHAR`

#### Scenario: NVARCHAR with length
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "NVARCHAR(255)" }`
- **THEN** the output type is `VARCHAR(255)` (length preserved)

#### Scenario: CLOB to VARCHAR
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "CLOB" }`
- **THEN** the output type is `VARCHAR`

#### Scenario: NCLOB to VARCHAR
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "NCLOB" }`
- **THEN** the output type is `VARCHAR`

#### Scenario: BINARY to VARBINARY
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "BINARY" }`
- **THEN** the output type is `VARBINARY`

#### Scenario: FLOAT to DOUBLE
- **WHEN** generating Trino SQL from an AST containing `DataType::Custom { name: "FLOAT" }`
- **THEN** the output type is `DOUBLE`

#### Scenario: Standard types pass through unchanged
- **WHEN** generating Trino SQL from an AST containing `DataType::Integer`
- **THEN** the output type is `INTEGER` (no HANA-specific mapping needed)

#### Scenario: HANA types in CREATE TABLE transpilation
- **WHEN** transpiling `CREATE TABLE t (id INTEGER, name NVARCHAR(100), salary SMALLDECIMAL, created SECONDDATE)` from HANA to Trino
- **THEN** the output is `CREATE TABLE t (id INTEGER, name VARCHAR(100), salary DECIMAL, created TIMESTAMP)`

### Requirement: HANA date format string conversion
The system SHALL convert HANA Oracle-style date format tokens to Java SimpleDateFormat tokens when transforming `TO_VARCHAR`, `TO_DATE`, and `TO_TIMESTAMP` functions.

#### Scenario: Basic date format
- **WHEN** converting HANA format `'YYYY-MM-DD'`
- **THEN** the Java format is `'yyyy-MM-dd'`

#### Scenario: Timestamp format with 24-hour time
- **WHEN** converting HANA format `'YYYY-MM-DD HH24:MI:SS'`
- **THEN** the Java format is `'yyyy-MM-dd HH:mm:ss'`

#### Scenario: Format with 12-hour time and AM/PM
- **WHEN** converting HANA format `'HH12:MI:SS AM'`
- **THEN** the Java format is `'hh:mm:ss a'`

#### Scenario: Format with month name
- **WHEN** converting HANA format `'DD MON YYYY'`
- **THEN** the Java format is `'dd MMM yyyy'`

#### Scenario: Format with full day name
- **WHEN** converting HANA format `'DAY, DD MONTH YYYY'`
- **THEN** the Java format is `'EEEE, dd MMMM yyyy'`

#### Scenario: Format with fractional seconds (millisecond)
- **WHEN** converting HANA format `'YYYY-MM-DD HH24:MI:SS.FF3'`
- **THEN** the Java format is `'yyyy-MM-dd HH:mm:ss.SSS'`

#### Scenario: Format with two-digit year
- **WHEN** converting HANA format `'DD-MM-YY'`
- **THEN** the Java format is `'dd-MM-yy'`

#### Scenario: Format with literal separators preserved
- **WHEN** converting HANA format `'YYYY/MM/DD HH24:MI:SS'`
- **THEN** the Java format is `'yyyy/MM/dd HH:mm:ss'` (slash separators preserved)

#### Scenario: Unknown format token passes through
- **WHEN** converting HANA format `'YYYY-FOO-DD'` where `FOO` is not a known HANA token
- **THEN** the Java format preserves the unknown token as-is: `'yyyy-FOO-dd'`

#### Scenario: Empty format string
- **WHEN** converting an empty HANA format `''`
- **THEN** the Java format is `''` (empty string returned as-is)

#### Scenario: Format with no tokens (literal only)
- **WHEN** converting HANA format `'hello world'` (no format tokens)
- **THEN** the Java format is `'hello world'` (preserved as-is)

### Requirement: HANA ILIKE handling
The system SHALL transform `ILIKE` to `LOWER() LIKE LOWER()` since HANA does not support `ILIKE` natively.

#### Scenario: ILIKE to LOWER LIKE
- **WHEN** transpiling `SELECT * FROM t WHERE name ILIKE '%john%'` from HANA to Trino
- **THEN** the output is `SELECT * FROM t WHERE LOWER(name) LIKE LOWER('%john%')`

#### Scenario: NOT ILIKE
- **WHEN** transpiling `SELECT * FROM t WHERE name NOT ILIKE '%test%'` from HANA to Trino
- **THEN** the output is `SELECT * FROM t WHERE NOT LOWER(name) LIKE LOWER('%test%')`

### Requirement: HANA TRY_CAST handling
The system SHALL handle `TRY_CAST` in HANA SQL by passing it through, as HANA Cloud supports `TRY_CAST`.

#### Scenario: TRY_CAST identity
- **WHEN** parsing `SELECT TRY_CAST(x AS INTEGER) FROM t` as HANA and generating as HANA
- **THEN** the output preserves `TRY_CAST`

#### Scenario: TRY_CAST to Trino
- **WHEN** transpiling `SELECT TRY_CAST(x AS INTEGER) FROM t` from HANA to Trino
- **THEN** the output preserves `TRY_CAST(x AS INTEGER)` (Trino supports TRY_CAST)

#### Scenario: TRY_CAST with VARCHAR type
- **WHEN** transpiling `SELECT TRY_CAST(x AS VARCHAR) FROM t` from HANA to Trino
- **THEN** the output is `SELECT TRY_CAST(x AS VARCHAR) FROM t`

### Requirement: HANA numeric function transforms
The system SHALL transform HANA numeric and bitwise functions to Trino-compatible equivalents during `transform_expr`.

#### Scenario: TRUNC to TRUNCATE
- **WHEN** transpiling `SELECT TRUNC(x, 2) FROM t` from HANA to Trino
- **THEN** the output is `SELECT TRUNCATE(x, 2) FROM t`

#### Scenario: BITAND to BITWISE_AND
- **WHEN** transpiling `SELECT BITAND(a, b) FROM t` from HANA to Trino
- **THEN** the output is `SELECT BITWISE_AND(a, b) FROM t`

#### Scenario: BITOR to BITWISE_OR
- **WHEN** transpiling `SELECT BITOR(a, b) FROM t` from HANA to Trino
- **THEN** the output is `SELECT BITWISE_OR(a, b) FROM t`

#### Scenario: BITNOT to BITWISE_NOT
- **WHEN** transpiling `SELECT BITNOT(a) FROM t` from HANA to Trino
- **THEN** the output is `SELECT BITWISE_NOT(a) FROM t`

### Requirement: HANA hex function transform
The system SHALL transform HANA hex conversion functions to Trino-compatible equivalents.

#### Scenario: HEX_TO_VARCHAR to FROM_HEX
- **WHEN** transpiling `SELECT HEX_TO_VARCHAR('414243') FROM t` from HANA to Trino
- **THEN** the output is `SELECT FROM_HEX('414243') FROM t`

### Requirement: HANA transpilation test fixtures
The system SHALL include transpilation, type, and function test fixtures in `tests/custom_fixtures/hana/` with edge case coverage, auto-discovered by the `custom_dialect_tests.rs` runner.

#### Scenario: Transpilation fixture with Trino target
- **WHEN** the test runner discovers `tests/custom_fixtures/hana/transpilation.json` containing `write` entries for `trino`
- **THEN** it transpiles each HANA SQL to Trino and compares against expected output

#### Scenario: Types fixture with Trino target
- **WHEN** the test runner discovers `tests/custom_fixtures/hana/types.json` containing `write` entries for `trino`
- **THEN** it transpiles HANA type declarations to Trino and compares against expected output

#### Scenario: Functions fixture identity
- **WHEN** the test runner discovers `tests/custom_fixtures/hana/functions.json`
- **THEN** it runs HANA function identity tests (HANA→HANA round-trip)

#### Scenario: Edge case coverage in transpilation fixtures
- **WHEN** the transpilation fixture file is loaded
- **THEN** it includes test cases for: negative date arithmetic values, nested function calls, functions in WHERE clauses, single-arg conversion functions (no format), mixed-case function names, NULL literal arguments, nested NVL, TRUNC with numeric and date contexts

#### Scenario: Edge case coverage in type fixtures
- **WHEN** the types fixture file is loaded
- **THEN** it includes test cases for: types with precision/scale args (SMALLDECIMAL(10,2), NVARCHAR(255), ALPHANUM(10)), types without args, types in CAST expressions, types in CREATE TABLE

### Requirement: Phase 1 identity tests continue to pass
The system SHALL maintain all Phase 1 identity test fixtures passing after Phase 2 transforms are added.

#### Scenario: Identity tests not broken by transforms
- **WHEN** running `cargo test -p polyglot-sql --test custom_dialect_tests` after Phase 2 implementation
- **THEN** all Phase 1 identity/select/ddl/dml tests still pass (transforms only apply when target dialect differs from source)
