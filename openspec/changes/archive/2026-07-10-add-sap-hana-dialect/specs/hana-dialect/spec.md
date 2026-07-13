## ADDED Requirements

### Requirement: HANA dialect registration
The system SHALL register `HANA` as a `DialectType` enum variant with feature gate `dialect-hana`, accepting `"hana"`, `"saphana"`, and `"sap_hana"` as string aliases via `FromStr`.

#### Scenario: Dialect lookup by name
- **WHEN** a user calls `DialectType::from_str("hana")`
- **THEN** the system returns `Ok(DialectType::HANA)`

#### Scenario: Dialect lookup by alias saphana
- **WHEN** a user calls `DialectType::from_str("saphana")`
- **THEN** the system returns `Ok(DialectType::HANA)`

#### Scenario: Dialect lookup by alias sap_hana
- **WHEN** a user calls `DialectType::from_str("sap_hana")`
- **THEN** the system returns `Ok(DialectType::HANA)`

#### Scenario: Unknown dialect still errors
- **WHEN** a user calls `DialectType::from_str("hanacloud")`
- **THEN** the system returns an error (not a valid alias)

#### Scenario: Display name
- **WHEN** `DialectType::HANA` is rendered as a string
- **THEN** the output is `"hana"`

#### Scenario: Case-insensitive lookup
- **WHEN** a user calls `DialectType::from_str("HANA")` or `DialectType::from_str("SapHANA")`
- **THEN** the system returns `Ok(DialectType::HANA)` (FromStr is case-insensitive)

### Requirement: HANA tokenizer configuration
The system SHALL configure the HANA tokenizer with double-quote (`"`) identifier quoting, standard SQL string quoting with single-quote escaping (`''`), and no nested comment support.

#### Scenario: Double-quoted identifier with spaces
- **WHEN** the tokenizer encounters `"my column"` in HANA SQL
- **THEN** it produces a single identifier token with value `my column` (case preserved)

#### Scenario: Double-quoted identifier preserves case
- **WHEN** the tokenizer encounters `"MyCamelCase"` in HANA SQL
- **THEN** it produces an identifier token with value `MyCamelCase` (case preserved, not folded)

#### Scenario: Double-quoted reserved word as identifier
- **WHEN** the tokenizer encounters `"SELECT"` or `"ORDER"` in HANA SQL
- **THEN** it produces an identifier token, not a keyword token

#### Scenario: Single-quote string escaping
- **WHEN** the tokenizer encounters `'it''s'` in HANA SQL
- **THEN** it produces a string literal token with value `it's`

#### Scenario: Single-line comment
- **WHEN** the tokenizer encounters `-- this is a comment` in HANA SQL
- **THEN** the comment is captured as trivia (not a SQL token) and re-emitted as a block comment (`/* this is a comment */`) in generated output

#### Scenario: Multi-line comment
- **WHEN** the tokenizer encounters `/* comment */` in HANA SQL
- **THEN** the comment is captured as trivia (not a SQL token) and preserved in generated output

#### Scenario: No nested comment support
- **WHEN** the tokenizer encounters `/* outer /* inner */ still comment */` in HANA SQL
- **THEN** the first `*/` closes the comment (non-nested behavior); `still comment */` is tokenized as SQL

### Requirement: HANA generator configuration
The system SHALL configure the HANA generator with double-quote identifier quoting, uppercase keywords, and HANA-compatible settings for null ordering, limit style, and interval syntax.

#### Scenario: Identifier quoting in generated SQL
- **WHEN** generating SQL for the HANA dialect with an identifier `order` (a reserved word)
- **THEN** the output uses double quotes: `"order"`

#### Scenario: Keyword casing
- **WHEN** generating SQL for the HANA dialect
- **THEN** keywords (SELECT, FROM, WHERE, etc.) are rendered in uppercase

#### Scenario: Non-reserved identifier not quoted
- **WHEN** generating SQL for the HANA dialect with an identifier `my_column` (not a reserved word)
- **THEN** the output does not add quotes: `my_column`

### Requirement: HANA identity round-trip
The system SHALL parse and regenerate HANA SQL preserving identity for all supported standard SQL constructs.

#### Scenario: Simple SELECT identity
- **WHEN** parsing `SELECT a, b FROM t WHERE x > 1` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: Double-quoted identifier identity
- **WHEN** parsing `SELECT "MyColumn" FROM "MyTable"` as HANA and generating as HANA
- **THEN** the output preserves the double-quoted identifiers with case intact

#### Scenario: Double-quoted reserved word identity
- **WHEN** parsing `SELECT "SELECT" FROM "ORDER"` as HANA and generating as HANA
- **THEN** the output preserves the double-quoted reserved words as identifiers

#### Scenario: Aggregate with GROUP BY identity
- **WHEN** parsing `SELECT dept, COUNT(*) FROM employees GROUP BY dept ORDER BY dept` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: JOIN identity
- **WHEN** parsing `SELECT a.x, b.y FROM a INNER JOIN b ON a.id = b.id` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: LEFT JOIN identity
- **WHEN** parsing `SELECT a.x, b.y FROM a LEFT JOIN b ON a.id = b.id WHERE b.y IS NULL` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: Multiple JOINs identity
- **WHEN** parsing `SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: Subquery identity
- **WHEN** parsing `SELECT * FROM (SELECT x FROM t WHERE x > 0) AS sub` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: Nested subquery in WHERE identity
- **WHEN** parsing `SELECT * FROM t WHERE x IN (SELECT y FROM s)` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: CTE identity
- **WHEN** parsing `WITH cte AS (SELECT x FROM t) SELECT * FROM cte` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: Window function identity
- **WHEN** parsing `SELECT x, ROW_NUMBER() OVER (PARTITION BY y ORDER BY z) AS rn FROM t` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: UNION ALL identity
- **WHEN** parsing `SELECT 1 UNION ALL SELECT 2` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: LIMIT and OFFSET identity
- **WHEN** parsing `SELECT * FROM t LIMIT 10 OFFSET 5` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: CASE WHEN identity
- **WHEN** parsing `SELECT CASE WHEN x > 1 THEN 'a' ELSE 'b' END FROM t` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: String literal with escaped quote identity
- **WHEN** parsing `SELECT 'it''s' AS val FROM t` as HANA and generating as HANA
- **THEN** the output preserves the escaped single quote

#### Scenario: NULL in SELECT identity
- **WHEN** parsing `SELECT NULL AS x FROM t` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: DISTINCT identity
- **WHEN** parsing `SELECT DISTINCT x FROM t` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: Column alias identity
- **WHEN** parsing `SELECT x AS alias_name FROM t` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: Star expression identity
- **WHEN** parsing `SELECT * FROM t` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: COALESCE identity
- **WHEN** parsing `SELECT COALESCE(a, b, c) FROM t` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: HANA native function identity
- **WHEN** parsing `SELECT ADD_DAYS(d, 7), NVL(a, b) FROM t` as HANA and generating as HANA
- **THEN** the output is `SELECT ADD_DAYS(d, 7), NVL(a, b) FROM t` (HANA functions preserved in identity round-trip, no transforms applied in Phase 1)

#### Scenario: SUBSTR canonicalized to SUBSTRING
- **WHEN** parsing `SELECT SUBSTR(s, 1, 3) FROM t` as HANA and generating as HANA
- **THEN** the output is `SELECT SUBSTRING(s, 1, 3) FROM t` (the parser canonicalizes SUBSTR to the generic SUBSTRING AST node; rendering SUBSTR for HANA output is a Phase 2 transform)

#### Scenario: Mixed-case function name identity
- **WHEN** parsing `SELECT add_days(d, 7) FROM t` as HANA and generating as HANA
- **THEN** the output preserves the function name as-is (no case normalization in Phase 1)

### Requirement: HANA DDL identity
The system SHALL parse and regenerate HANA DDL statements preserving identity.

#### Scenario: CREATE TABLE with HANA types identity
- **WHEN** parsing `CREATE TABLE t (id INTEGER, name NVARCHAR(100), salary SMALLDECIMAL, created SECONDDATE)` as HANA and generating as HANA
- **THEN** the output preserves HANA-specific type names (SMALLDECIMAL, SECONDDATE, NVARCHAR) as-is

#### Scenario: CREATE TABLE with constraints identity
- **WHEN** parsing `CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL)` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: DROP TABLE identity
- **WHEN** parsing `DROP TABLE t` as HANA and generating as HANA
- **THEN** the output matches the input

#### Scenario: ALTER TABLE ADD COLUMN identity
- **WHEN** parsing `ALTER TABLE t ADD COLUMN x INTEGER` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

### Requirement: HANA DML identity
The system SHALL parse and regenerate HANA DML statements preserving identity.

#### Scenario: INSERT with VALUES identity
- **WHEN** parsing `INSERT INTO t (a, b) VALUES (1, 'x')` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: INSERT with SELECT identity
- **WHEN** parsing `INSERT INTO t SELECT a, b FROM s` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: UPDATE with WHERE identity
- **WHEN** parsing `UPDATE t SET x = 1 WHERE y = 2` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: DELETE with WHERE identity
- **WHEN** parsing `DELETE FROM t WHERE x = 1` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

#### Scenario: INSERT multiple rows identity
- **WHEN** parsing `INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')` as HANA and generating as HANA
- **THEN** the output matches the input (modulo whitespace normalization)

### Requirement: HANA test fixtures
The system SHALL include custom test fixtures in `tests/custom_fixtures/hana/` covering identity, select, DDL, and DML categories, auto-discovered by the `custom_dialect_tests.rs` runner. Fixtures SHALL include both common cases and edge cases.

#### Scenario: Identity fixture file
- **WHEN** the test runner discovers `tests/custom_fixtures/hana/identity.json`
- **THEN** it runs all identity tests and reports pass/fail per test case

#### Scenario: Select fixture file
- **WHEN** the test runner discovers `tests/custom_fixtures/hana/select.json`
- **THEN** it runs all identity tests for SELECT constructs and reports pass/fail per test case

#### Scenario: DDL fixture file
- **WHEN** the test runner discovers `tests/custom_fixtures/hana/ddl.json`
- **THEN** it runs all identity tests for DDL constructs including HANA-specific data types

#### Scenario: DML fixture file
- **WHEN** the test runner discovers `tests/custom_fixtures/hana/dml.json`
- **THEN** it runs all identity tests for DML constructs and reports pass/fail per test case

#### Scenario: All fixture categories present
- **WHEN** the test runner scans `tests/custom_fixtures/hana/`
- **THEN** it finds fixture files for: identity, select, ddl, dml

#### Scenario: Edge case coverage in identity fixtures
- **WHEN** the identity fixture file is loaded
- **THEN** it includes test cases for: escaped single quotes in strings, NULL literal, reserved words as double-quoted identifiers, multiple JOINs, nested subqueries, window functions with PARTITION BY and ORDER BY, UNION ALL, CTEs, CASE WHEN, LEFT JOIN with IS NULL

#### Scenario: Edge case coverage in DDL fixtures
- **WHEN** the DDL fixture file is loaded
- **THEN** it includes test cases for: HANA-specific types (SMALLDECIMAL, SECONDDATE, NVARCHAR, ALPHANUM), types with precision/length args, CREATE TABLE with constraints, ALTER TABLE

#### Scenario: Edge case coverage in DML fixtures
- **WHEN** the DML fixture file is loaded
- **THEN** it includes test cases for: INSERT with multiple rows, INSERT with SELECT, UPDATE with WHERE, DELETE with WHERE, INSERT with explicit column list

### Requirement: HANA dialect available in all bindings
The system SHALL expose the HANA dialect through FFI, WASM/TypeScript SDK, and all language bindings that enumerate dialects.

#### Scenario: FFI dialect list includes HANA
- **WHEN** calling `polyglot_dialect_list()` via FFI
- **THEN** the returned JSON array includes `"hana"`

#### Scenario: FFI dialect count
- **WHEN** calling `polyglot_dialect_count()` via FFI
- **THEN** the returned count is 35 (34 existing + 1 HANA)

#### Scenario: TypeScript SDK DialectType enum
- **WHEN** a TypeScript user imports `DialectType`
- **THEN** the enum includes `HANA = 'hana'`
