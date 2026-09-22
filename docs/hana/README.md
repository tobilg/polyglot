# SAP HANA support

The `hana` dialect targets SAP HANA Cloud QRC 2/2026 and HANA Platform 2.0 SPS 08.
`hana`, `saphana`, and `sap_hana` are accepted Rust dialect names. The public SDK
name is `hana` (`Dialect.HANA` in TypeScript).

This implementation starts from [Torsten Glunde's PR #276](https://github.com/tobilg/polyglot/pull/276),
reviewed at `a452c0acd0a38892ef05e424b5ba94dc247bf3f0`, and replaces its unconditional
function and custom-type rewrites. Parser, expression, and generator changes are
integrated into the existing shared files.

## Coverage and limits

**This is not complete coverage of either SQL reference.** The inventories below
track the reference surface, including remaining work. Recognizing a function name,
parsing one example, and implementing every documented form are different levels
of support. A successful parse does not validate server privileges, object
existence, installed components, or runtime argument values.

Native forms covered by regression tests include:

- Ordinary queries, joins, CTEs, aggregates, window functions, common DML, and
  basic table/view DDL, using the existing shared AST.
- HANA function calls with source provenance, including conversion, date/time,
  string, bitwise, series-generation, and vector function names. Special argument
  grammars need explicit parser support; registration alone does not provide it.
- `LOCATE` with two to four arguments; `SUBSTRING`/`SUBSTR`; decimal and formatted
  date/string conversions; local and UTC current-time expressions and precision.
- Native numeric, string, LOB, spatial, and vector type names, including HANA's
  unsigned `TINYINT`, precision-dependent `FLOAT`, and floating-decimal types.
- `FOR JSON`/`FOR XML`, including options and `RETURNS`; `WITH HINT`; query collation;
  `FOR UPDATE` and `FOR SHARE LOCK`, including `IGNORE LOCKED`; `LIMIT ... TOTAL ROWCOUNT`.
- Grouping-set, rollup, and cube selection options (`BEST`, `LIMIT`, `OFFSET`,
  subtotal/balance/total), structured results, prefixes, and multiple result sets.
- `LOCALTOUTC` and `UTCTOLOCAL` dataset/schema arguments and `ON ERROR` policies.
- `JSON_VALUE`, `JSON_QUERY`, and `JSON_TABLE`, with structured paths, return types,
  wrapper behavior, defaults, error/empty behavior, ordinality, and nested columns.
- `UPSERT` values/query forms, partition and column lists, conditions, and
  `WITH PRIMARY KEY`.
- Row/column and local/global temporary table modifiers; parenthesized `ALTER
  TABLE ... ADD`; postfix array types; hash, round-robin, and balanced range
  partitioning, subpartitioning, selected storage properties, and generated columns.
- Boolean `UNKNOWN` as the documented synonym for `NULL`, while retaining quoted names.
- `LIKE_REGEXPR`, `SUBSTR_REGEXPR`/`SUBSTRING_REGEXPR`, `LOCATE_REGEXPR`,
  `OCCURRENCES_REGEXPR`, and `REPLACE_REGEXPR` clauses.
- `HIERARCHY` and `HIERARCHY_SPANTREE` source/options syntax; calculation-view
  `PLACEHOLDER` arguments and qualified, quoted table function names.
- Procedure and library-member `CALL`, including asynchronous calls and hints.

Remaining gaps include administrative and access-control grammar, advanced table,
view, index and temporal DDL, heterogeneous partition trees, association paths,
series specifications inside aggregates/windows,
additional hierarchy families, spatial method mappings, advanced search scoring, and special function
clauses. Procedure/function definitions are not implemented by the supported
`CALL` syntax. These are not covered merely because another spelling or a simpler
form parses. SQLScript body grammar is outside this dialect's scope.

Unknown statement fallbacks that would produce `Raw` or `Command` nodes are
rejected. Unsupported forms can therefore fail native parsing rather than appear
to have structured support. The shared parser is permissive and is not a complete
HANA grammar validator; for example, accepted SELECTs without a FROM clause are
rendered with `FROM DUMMY` for a HANA target.

## Translation policy

HANA-specific AST nodes retain source semantics through JSON serialization and
independent target generation. A known HANA function is not automatically treated
as a compatible target function. Unverified mappings return an unsupported error,
including at `Ignore` and `Warn`, rather than silently dropping a result-shaping
clause or emitting a known incompatible operation. Checks run before target
normalization can erase protected casts or query options.

Implemented scalar mappings are deliberately bounded:

| Source operation | Targets | Conditions |
|---|---|---|
| `COALESCE`, `IFNULL` | Existing SQL targets | Standard coalescing form |
| `LOCATE` | Trino family, PostgreSQL family, DuckDB | Literal BMP strings, nonnegative constant start, positive occurrence; repeated occurrence requires Trino family |
| `SUBSTRING`, `SUBSTR` | Same families | Literal BMP string and constant bounds; starts below 1 clamp to 1 and lengths below 1 clamp to 0 |
| `TO_DECIMAL(value,p,s)` and fixed-decimal casts | Same families | Proven exact numeric input and valid constant precision/scale; truncate before cast |
| Formatted date parsing/formatting | Trino family, DuckDB | Supported literal datetime mask; formatting requires datetime type evidence; date parsing retains a DATE result; timestamp parsing is restricted to Trino with an explicit TIMESTAMP(7) result |
| UTC clock expressions | Selected Trino/PostgreSQL/DuckDB forms | Explicit UTC zone and representable precision; other forms report unsupported |
| `TIMESTAMP` / `LONGDATE`, `TIME` | Trino | Explicit `TIMESTAMP(7)` / `TIME(0)`; targets with unverified precision report unsupported |
| `FLOAT(p)` | Trino, Presto, PostgreSQL, DuckDB | `p <= 24` maps to REAL; larger/default precision maps to double precision |
| `TINYINT` | DuckDB, ClickHouse | Unsigned type; selected constant casts also widen safely on Trino/Presto/PostgreSQL |

Unknown/binary/supplementary-Unicode string inputs, dynamic bounds or masks,
negative-start searches, empty-needle searches with offsets/repetition, floating-decimal conversions, and unsupported timestamp
precision are not approximated. HANA indexes CESU-8/UTF-16 character units, which
can differ from target Unicode code-point indexing. The `DEC`, `DAYDATE`, and `LONGDATE` aliases retain their canonical type semantics.
Qualified/quoted user-defined
names remain distinct from unqualified built-ins.

Basic SQL can be generated for a HANA target. Foreign operations with unverified
HANA semantics, including positional strings, formatted time conversion,
time-zone-sensitive clocks, and dialect date arithmetic, can report unsupported.
Transpilation does not migrate procedures, calculation views, catalogs, privileges,
or system tables such as `DUMMY` into another database.

## Reference inventory and evidence

- [Cloud reference inventory](cloud-reference.json): pinned QRC 2/2026 topics.
- [Platform function inventory](functions.json): Platform 2.0 SPS 08 function topics.
- [Platform statement inventory](statements.json): Platform 2.0 SPS 08 statement topics.
- Native fixtures: `crates/polyglot-sql/tests/custom_fixtures/hana/`.
- Source semantics, JSON AST round trips, quoting, boundary and all-target error
  policy tests: the existing `tests/dialect_matrix.rs` HANA regression module.
- Binding regressions: existing WASM, FFI, Python, Go, and TypeScript test files.

Reference-example probes are a syntax audit, not a HANA server execution suite.
Documentation examples containing placeholders, omitted separators, typographical
errors, or excluded SQLScript require manual review before becoming test fixtures.
DuckDB execution checks verify selected scalar result values. HANA server execution
has not been performed.

The adjacent SQLGlot Oracle, MySQL, Snowflake, and Presto/Trino implementations
informed argument ordering, format-token handling, and source/target separation.
The checked-in SQLGlot has no HANA dialect; its output is not a HANA specification.
No SQLGlot fixture exclusions were added for this implementation.

## Validation

Verified locally on 2026-09-22:

- `make test-rust-verify`, including existing SQLGlot fixtures, the 132 native
  HANA fixtures, ClickHouse parser and round-trip suites, and FFI tests.
- All 12 focused HANA regression tests in `dialect_matrix.rs`.
- Parse-only, generation, and transpilation builds with only `dialect-hana`,
  plus a HANA-only WASM discovery/native parsing test.
- Rebuilt WASM and TypeScript SDK: 613 tests; TypeScript type checking.
- Rebuilt Python extension: 321 tests passed, one skipped; Python type checking.
- Go tests against the rebuilt FFI library, with the test cache disabled.
- Formatting, project consistency, and unchanged released changelog entries.

Passing these checks establishes the tested behavior and regression baseline;
it does not establish complete SQL-reference or server-execution coverage.

## References

- [HANA Cloud SQL reference](https://help.sap.com/docs/HANA_CLOUD_DATABASE/c1d3f60099654ecfb3fe36ac93c121bb/209f5020751910148fd8fe88aa4d79d9.html?version=2026_2_QRC)
- [Platform 2.0 SPS 08 SQL reference](https://help.sap.com/doc/9b40bf74f8644b898fb07dabdd2a36ad/2.0.08/en-US/SAP_HANA_SQL_Reference_Guide_en.pdf)
- [HANA character encoding](https://help.sap.com/docs/SAP_HANA_CLIENT/f1b440ded6144a54ada97ff95dac7adf/5d221fc0e8b54c9d8eddd7748cdf398b.html)
- [Trino datetime functions](https://trino.io/docs/current/functions/datetime.html)
- [Trino string functions](https://trino.io/docs/current/functions/string.html)
- [Trino data types](https://trino.io/docs/current/language/types.html)
- [Trino numeric functions](https://trino.io/docs/current/functions/math.html)

Timestamp mappings are checked per target. See also the [Presto types reference](https://www.prestodb.io/docs/current/language/types.html) and [Athena timestamp restrictions](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html).
