# Changelog

All notable changes to this project are documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [0.6.2] - 2026-07-17

### Added
- TiDB parsing and generation now cover `AUTO_RANDOM` attributes, including
  shard/range parameters and executable comments; distributed table options
  such as `SHARD_ROW_ID_BITS`, `PRE_SPLIT_REGIONS`, and `AUTO_RANDOM_BASE`;
  placement and TTL options; their `ALTER TABLE` forms; and `SPLIT TABLE` and
  `FLASHBACK TABLE` statements. The structured AST is available consistently
  through the Rust, Python, C FFI, Go, WASM, and TypeScript parse/generate APIs.

### Changed
- CTE-heavy lineage now indexes each scope once and traverses stable scope IDs
  instead of repeatedly cloning nested CTE scope trees. The public `Scope` and
  lineage APIs remain unchanged, while benchmarks now cover flat, nested, and
  multiply referenced CTE shapes.
- Strict PostgreSQL/CockroachDB-to-T-SQL/Fabric transpilation now keeps the
  existing best-effort `VARCHAR(MAX)` conversion for column-to-text casts whose
  source type is unavailable, rather than rejecting otherwise valid queries.

### Fixed
- PostgreSQL-family parsing now accepts parenthesized `EXPLAIN` option lists,
  including comma-separated flags and values, while preserving parenthesized
  query bodies and normalizing `ANALYZE` consistently with the bare form.
- PostgreSQL compound intervals now retain all fields and lower through
  operand-aware `DATEADD` chains for T-SQL/Fabric, including fractional
  seconds, subtraction, and `TIME` wraparound. Strict mode rejects unresolved
  operand types and intervals that cannot be represented safely instead of
  emitting malformed or lossy SQL.
- PostgreSQL `MAKE_TIME` now lowers to `TIMEFROMPARTS` with fractional-second
  precision for T-SQL/Fabric. Single-value `CONCAT`/`CONCAT_WS` calls meet the
  target minimum arity, and mixed-type `||` operands are converted to strings
  before using T-SQL concatenation.
- PostgreSQL SQL/JSON constructors and aggregates now translate `RETURNING`
  clauses to compatible T-SQL/Fabric output: native JSON return types are
  omitted, supported character return types use an outer cast, and unsafe
  types fail in strict mode. `json_agg`/`jsonb_agg` preserve PostgreSQL null and
  ordering semantics while unsupported modifiers are rejected.
- Nested PostgreSQL grouping sets are flattened without being mistaken for row
  values, and inert `ORDER BY` clauses inside unbounded set-operation arms are
  removed for T-SQL/Fabric.
- PostgreSQL boolean predicates nested inside casts, aggregates, functions, or
  arithmetic now materialize as nullable scalar values before T-SQL/Fabric
  conversion, preserving three-valued logic.
- PostgreSQL `date_part` results now retain their `double precision` contract
  for T-SQL/Fabric, while `EXTRACT` keeps its field-specific type. Time-based
  second, millisecond, microsecond, and epoch calculations preserve complete
  values without overflow.
- PostgreSQL string semantics now map more faithfully to T-SQL/Fabric,
  including deparsed `LIKE_ESCAPE`, numeric-start `SUBSTRING`, text-cast trim
  sets, literal `TRANSLATE`, unpadded `to_hex`, hexadecimal encode/decode, and
  `SHA256`/`SHA512`. Strict mode rejects regular-expression functions,
  unsupported digest widths, and other string operations without a faithful
  target equivalent.
- PostgreSQL float-to-integer rounding now also recognizes parenthesized cast
  chains before applying the target-side rounding conversion.
- Snowflake string parsing and generation now round-trip doubled quotes,
  backslash escape sequences, control characters, dollar-quoted strings, and
  function-body literals without losing or double-escaping content.
- Shared AST transformations now recurse through `ALL`/`ANY` quantified
  comparisons and null-safe comparison operands, preserving bottom-up
  transformation order across all public transform entry points.

## [0.6.1] - 2026-07-15

### Fixed
- PostgreSQL `VALUES` derived tables without explicit column aliases now gain
  deterministic `column1`, `column2`, ... aliases when targeting T-SQL or
  Fabric, producing valid derived-table syntax while preserving user-provided
  aliases.
- PostgreSQL boolean operator functions, boolean literals, logical scalar
  values, and known boolean-to-text casts now lower with the correct T-SQL and
  Fabric value semantics, including nullable predicates. Strict mode rejects
  column-to-text casts when the source type is unknown rather than applying an
  unsafe boolean assumption.
- Simple `CASE operand WHEN value` expressions now keep their `WHEN` arms as
  scalar comparison values during T-SQL/Fabric boolean normalization, while
  searched `CASE WHEN predicate` expressions continue to use predicate
  rewriting.
- PostgreSQL casts from `REAL`, `FLOAT4`, `FLOAT8`, and `DOUBLE PRECISION` to
  integer types now apply PostgreSQL-compatible rounding before the
  truncating T-SQL/Fabric integer cast. Numeric casts and unresolved source
  types retain their existing behavior.
- PostgreSQL `EXTRACT` and `date_part` over `TIME` values now preserve complete
  fractional-second composition for `SECOND`, `MILLISECOND`, `MICROSECOND`,
  and `EPOCH` when targeting T-SQL/Fabric instead of returning only a
  `DATEPART` component.
- Strict PostgreSQL-to-T-SQL/Fabric transpilation now rejects unsupported
  scalar row/composite constructors, composite field access, and qualified
  whole-row casts instead of emitting invalid target SQL. Valid table `VALUES`
  constructors and supported JSON aggregation remain accepted.
- PostgreSQL `NULL::unknown` and `CAST(NULL AS unknown)` now lower to a bare
  `NULL` for T-SQL/Fabric. Other unresolved `unknown` casts are rejected in
  strict mode instead of generating a cast to a nonexistent target type.

## [0.6.0] - 2026-07-14

### Added
- Shared, generated AST-child traversal infrastructure through the new internal
  `polyglot-sql-ast-derive` crate, including immutable and mutable child-slot
  metadata, path-aware traversal checks, and dedicated traversal benchmarks.
- Optional Rust-core semantic validation through `ValidationOptions.semantic`
  and `validate_with_dialect`, covering warnings W001-W004 for `SELECT *`,
  mixed aggregates without `GROUP BY`, `DISTINCT` with `ORDER BY`, and `LIMIT`
  without `ORDER BY`. The same validation path is now exposed through Python,
  C FFI, Go, WASM, and TypeScript without changing schema-validation behavior.
- A machine-readable cross-language API capability contract with verification
  across Rust, Python, C FFI, Go, WASM, and TypeScript.
- Project-consistency tooling and Make targets for checking release versions,
  public dialect metadata, active documentation, the standalone Rust example,
  and generated API documentation.
- CI quality gates for Rust and TypeScript formatting, scoped Clippy checks,
  Biome linting, consistency checks, documentation builds, benchmark
  compilation, and TypeScript SDK coverage summaries and artifacts.
- Focused performance and allocation benchmarks for dialect construction, AST
  traversal, tokenization/parsing, Python concurrency, and native release
  profiles, with reproducible Make targets and a benchmark report.
- Regression coverage for the new T-SQL/Fabric strict-mode capability checks
  and rewrites, including regex aggregate filters, row-value membership,
  aggregate ordering, hypothetical-set aggregates, and supported control cases.

### Changed
- Cross-dialect normalization is now split into semantic modules for
  aggregates, collections, JSON, operators, scalar functions, statements,
  temporal expressions, and types while preserving the existing normalization
  pipeline and public API.
- Built-in dialects now share immutable tokenizer configurations. ASCII input
  uses a byte cursor, unchanged parser token text references one shared SQL
  source, and token guard statistics are collected during tokenization instead
  of requiring a second full scan; the public owned `Token` API is unchanged.
- Python native calls now execute directly while the GIL is detached instead
  of serializing through one global worker. Published Python artifacts use a
  dedicated `opt-level=2`/thin-LTO profile.
- Repository-built native artifacts now use the `opt-level=3` `native_release`
  profile. FFI/Go release artifacts use `opt-level=2` with thin LTO for higher
  query throughput while retaining unwind protection; WASM remains
  size-optimized.
- The benchmark comparison now uses the same production Python profile as
  published wheels.
- TypeScript now publishes one full WASM SDK containing all supported dialects;
  dialect-specific Rust/WASM builds remain available through Cargo features.
- Release metadata and active documentation now consistently use version
  `0.6.0` and canonical supported-dialect names rather than hard-coded dialect
  counts.

### Fixed
- Logical planner DAG construction now assigns globally unique preorder IDs to
  nested dependencies, includes every leaf node, and serializes complete plans
  through WASM. Previously ignored invalid-input WASM tests are active again.
- Strict PostgreSQL-to-T-SQL/Fabric transpilation now rejects unsupported
  `NTH_VALUE`, `SCALE`, `TRIM_SCALE`, `MIN_SCALE`, `FACTORIAL`, and `PG_LSN`
  calls; `GROUPS`, value-offset `RANGE`, and `EXCLUDE` window frames; frames and
  window functions requiring a missing `ORDER BY`; `JOIN ... USING`, `NATURAL
  JOIN`, unsupported base/joined-table column alias lists, and qualified
  whole-row aggregate arguments such as `COUNT(alias.*)`.
- Multi-column PostgreSQL `GROUPING(...)` now maps to `GROUPING_ID(...)` for
  T-SQL/Fabric, `GROUP BY DISTINCT` grouping sets are expanded and deduplicated,
  and Fabric drops `ORDER BY` keys that become constant through single-level
  grouping analysis.
- PostgreSQL boolean aggregates used as window functions now keep `OVER` on
  the aggregate inside the outer `BIT` cast. Scalar `BIT` expressions and
  boolean literals are also normalized correctly in predicate contexts and
  nested joined-table conditions.
- `LAG` and `LEAD` arguments now receive recursive target transformations, so
  numeric casts retain their precision and scale.
- Nested T-SQL/Fabric `ORDER BY` clauses without a row bound now receive the
  required `OFFSET 0 ROWS`. Unordered inert zero/`NULL` offsets are removed,
  while retained offsets and `FETCH` clauses receive a neutral
  `ORDER BY (SELECT NULL)` when needed.
- PostgreSQL lateral joins that reference earlier comma-separated siblings now
  build an explicit `CROSS JOIN` product before `CROSS APPLY`/`OUTER APPLY`,
  keeping every correlated source visible to the APPLY right-hand expression.
- PostgreSQL row-value `IN (VALUES ...)` and `NOT IN (VALUES ...)` predicates
  now lower to `EXISTS`/`NOT EXISTS` for T-SQL/Fabric, including scalar boolean
  contexts and null-aware `NOT IN` semantics. Strict mode rejects mismatched
  row and `VALUES` arities.
- T-SQL/Fabric ordering generation now disambiguates duplicate projected
  columns before adding null-ordering expressions. PostgreSQL `ORDER BY`
  clauses that are inert inside ordinary aggregates are removed, while window
  ordering and ordered `STRING_AGG` semantics remain intact.
- Strict PostgreSQL-to-T-SQL/Fabric transpilation now rejects hypothetical-set
  `RANK`, `DENSE_RANK`, `CUME_DIST`, and `PERCENT_RANK`; internal aggregate
  support functions such as `FLOAT8_*` and `BOOL*_STATEFUNC`; PostgreSQL array
  casts; `STRING_AGG` with `DISTINCT`; PostgreSQL collations; and date
  subtraction whose column type cannot be resolved safely.
- PostgreSQL `FROM ONLY` drops its unsupported inheritance modifier when
  targeting T-SQL/Fabric. `ANY_VALUE` lowers to `MAX` for T-SQL and remains
  native for Fabric.
- PostgreSQL numeric `TO_CHAR` format models now fail in strict T-SQL/Fabric
  transpilation instead of being mistaken for .NET format strings. Text-cast
  temporal format literals such as `'YYYY-MM-DD'::text` continue to lower to
  valid `FORMAT` expressions.

### Removed
- The unshipped TypeScript per-dialect package exports and build configuration;
  the supported npm surface is the full `@polyglot-sql/sdk` package.
- Duplicate TypeScript-only semantic validation rules, now that basic semantic
  validation is implemented consistently in the Rust core.

## [0.5.16] - 2026-07-11

### Added
- Expanded strict-mode regression coverage for unsupported transpilation
  leftovers, including recursive CTEs, residual lateral joins, unsupported
  `UNNEST`/`EXPLODE` targets, duplicate-preserving set operations, lossy array
  aggregates, regex predicates, residual `FETCH WITH TIES` / `OVERLAPS` /
  `DATE_BIN`, PostgreSQL-only scalar functions, PostgreSQL JSON functions, and
  `maxUnsupported` option handling.
- Regression coverage for PostgreSQL-to-T-SQL/Fabric rewrites across JSON
  operators and constructors, temporal functions, math/string functions,
  arrays, lateral joins, window frames, row-value subqueries, aggregate
  filters, ordered `STRING_AGG`, ordered-set percentiles, no-op limits,
  interval/date arithmetic, and `VALUES` set-operation operands.
- Regression coverage for structured PostgreSQL `PREPARE` / `EXECUTE`
  parsing and PostgreSQL replication protocol command fallbacks.
- Parser regression coverage for `TIMESTAMPTZ '...'` and
  `TIMESTAMPTZ(n) '...'` typed literals in generic, DuckDB, and PostgreSQL
  parsing paths.

### Changed
- Fabric and T-SQL now share more PostgreSQL compatibility rewrites while
  preserving Fabric-specific type mappings such as `NVARCHAR` to `VARCHAR`.

### Fixed
- PostgreSQL JSON extraction operators, path operators, JSON constructors,
  `json_agg` / `jsonb_agg`, and scalar `jsonb_array_elements(...)` projections
  now transpile to valid T-SQL/Fabric JSON functions such as `JSON_VALUE`,
  `JSON_QUERY`, `JSON_OBJECT`, `JSON_ARRAYAGG`, and `OPENJSON` where a safe
  mapping exists.
- Strict PostgreSQL-to-T-SQL/Fabric transpilation now rejects unsupported JSON
  row shapes, array literals/subscripts/functions, PostgreSQL-only scalar
  functions such as `GCD`, `LCM`, `ERF`, `QUOTE_LITERAL`, `AGE`, and
  `PG_TYPEOF`, unsupported statistical aggregates, residual array `ANY`
  semantics, and scalar interval casts instead of emitting target SQL that
  would fail at runtime.
- PostgreSQL regex and `SIMILAR TO` expressions now either lower to supported
  T-SQL/Fabric predicate forms for compatible patterns or fail in strict mode
  for unsupported SQL-regex patterns.
- PostgreSQL temporal rewrites targeting T-SQL/Fabric now handle current
  temporal niladics, `EXTRACT` / `date_part`, typed-text `date_part` fields,
  `date_trunc`, simple `date_bin` buckets, `to_timestamp`, `to_date`,
  `to_char`, and `format(...)` string interpolation with valid target
  signatures or strict-mode rejection for unsupported forms.
- PostgreSQL math, string, type, and operator rewrites targeting T-SQL/Fabric
  now cover `LOG`/`LN`/`LOG10`, `DIV`, `CBRT`, `REPEAT`, `CHR`, `OVERLAY`,
  `BTRIM`, `MD5`, `OCTET_LENGTH`, `BIT_LENGTH`, `TO_HEX`, hex `ENCODE`,
  simple `TO_NUMBER`, approximate numeric casts, function-style type casts,
  `ROUND(x)`, bitwise XOR `#`, absolute-value `@`, and `MOD(...)`.
- PostgreSQL UUID generator functions such as `gen_random_uuid()`,
  `uuid_generate_v4()`, and `uuidv4()` now map to `NEWID()` for T-SQL/Fabric.
- PostgreSQL-to-T-SQL/Fabric query-shape rewrites now strip unsupported CTE
  materialization hints, hoist nested CTEs from derived subqueries, map
  supported lateral joins to `CROSS APPLY` / `OUTER APPLY`, preserve valid
  null ordering and `DISTINCT ON` behavior through wrapper queries, handle
  positional `ORDER BY` safely, and keep recursive CTE output valid by omitting
  the unsupported `RECURSIVE` keyword.
- PostgreSQL row-value `IN` and equality subquery comparisons now rewrite to
  `EXISTS` predicates for T-SQL/Fabric when arity and projection shapes are
  safe, including casted and wrapped subquery projections.
- PostgreSQL boolean and null-safe comparison rewrites targeting T-SQL/Fabric
  now distinguish predicate contexts from scalar contexts, materializing scalar
  boolean results as `BIT` values while keeping predicates executable.
- PostgreSQL aggregate filters, ordered `STRING_AGG`, typed string-aggregate
  separators, and grouped `PERCENTILE_CONT` / `PERCENTILE_DISC` ordered-set
  aggregates now lower to valid T-SQL/Fabric forms; unsupported `MODE() WITHIN
  GROUP` fails in strict mode.
- PostgreSQL `VALUES` statements and `VALUES` set-operation operands now wrap
  as derived tables for T-SQL/Fabric, avoiding invalid top-level or set-operand
  `VALUES` SQL.
- PostgreSQL `LIMIT NULL`, `LIMIT ALL`, and `LIMIT (NULL)` are omitted for
  T-SQL/Fabric, offset-only queries keep a valid `OFFSET ... ROWS` shape, and
  `FETCH FIRST ... WITH TIES` maps to `TOP ... WITH TIES` where supported.
- PostgreSQL date/date and date/integer arithmetic now maps to `DATEDIFF` or
  day-based `DATEADD`, and common interval arithmetic including abbreviated
  time and month units maps to target `DATEADD` calls.
- The parser now accepts `TIMESTAMPTZ '...'` and `TIMESTAMPTZ(n) '...'` typed
  literals and normalizes them to explicit timestamp-with-time-zone casts,
  matching the behavior already available through explicit `CAST(...)` and
  PostgreSQL `::TIMESTAMPTZ` casts.

## [0.5.15] - 2026-07-09

### Added
- Regression coverage for PostgreSQL-only scalar and JSON functions when
  targeting T-SQL/Fabric strict mode, including `LPAD`, `RPAD`, `SPLIT_PART`,
  `INITCAP`, `TO_JSON`, `TO_JSONB`, `JSONB_AGG`, and `JSONB_OBJECT_AGG`.
- Regression coverage for PostgreSQL `SELECT DISTINCT ... ORDER BY` null
  ordering when targeting T-SQL/Fabric, including selected expressions,
  selected aliases, target-default null ordering, and unsupported unselected
  expressions.
- Regression coverage for PostgreSQL boolean tests targeting T-SQL/Fabric,
  including `IS TRUE`, `IS FALSE`, `IS NOT TRUE`, `IS NOT FALSE`, and
  `IS UNKNOWN` in predicate and scalar-value contexts.
- Regression coverage for bare boolean predicates targeting T-SQL/Fabric in
  `WHERE`, `HAVING`, `CASE WHEN`, `JOIN ... ON`, nested subqueries, and CTE
  bodies.
- Regression coverage for unsupported `EXCEPT ALL` and `INTERSECT ALL` when
  targeting T-SQL/Fabric strict mode.
- Regression coverage for PostgreSQL positional `ORDER BY` ordinals targeting
  T-SQL/Fabric, including standalone selects and wrapped set operations.

### Fixed
- Strict PostgreSQL-to-T-SQL/Fabric transpilation now rejects known
  PostgreSQL-only scalar and JSON functions instead of returning target SQL
  that the destination dialect cannot execute.
- `SELECT DISTINCT ... ORDER BY` with emulated null ordering now generates a
  valid T-SQL/Fabric wrapper query that projects stable internal sort keys,
  avoiding invalid `ORDER BY` expressions that are not present in the
  `DISTINCT` select list.
- PostgreSQL boolean tests now preserve three-valued logic when transpiling to
  T-SQL/Fabric: negated `IS TRUE` / `IS FALSE` predicates retain `NULL` rows,
  scalar boolean-test outputs are materialized as definite `BIT` values, and
  predicate operands are not compared directly to integer literals.
- Fabric now applies the same bare boolean predicate coercion as T-SQL, and
  the boolean coercion transform now reaches `JOIN ... ON`, nested subqueries,
  CTE bodies, and set-operation branches without inheriting unrelated T-SQL
  preprocessing behavior.
- T-SQL/Fabric strict mode now rejects `EXCEPT ALL` and `INTERSECT ALL` as
  unsupported duplicate-preserving set operations while keeping plain
  `EXCEPT` and `INTERSECT` valid.
- Positional `ORDER BY` ordinals targeting T-SQL/Fabric no longer produce
  invalid constant null-ordering CASE expressions such as
  `CASE WHEN 1 IS NULL ...`; default mode preserves the ordinal and strict mode
  reports unsupported positional null-ordering simulation.

## [0.5.14] - 2026-07-07

### Added
- Regression coverage for PostgreSQL positional bind parameters inside
  T-SQL/Fabric predicate expressions, ensuring `$1`, `$2`, ... are rendered as
  executable `@P1`, `@P2`, ... placeholders outside simple projection lists.

### Fixed
- PostgreSQL `$n` positional bind parameters now render as T-SQL/Fabric
  `@Pn` placeholders instead of invalid `$n` syntax when transpiling to those
  dialects.

## [0.5.13] - 2026-07-04

### Added
- Configurable complexity guards for recursion-heavy parse, transpile,
  transform, and generation paths, including limits for SQL input size, token
  count, AST node count, AST depth, parenthesis nesting, and function-call
  nesting.
- `TranspileOptions.complexityGuard` / `complexity_guard` support across Rust,
  C FFI JSON options, WASM/TypeScript, and Go, allowing callers to raise or
  disable individual guard limits for trusted inputs.
- Regression coverage for deeply nested unary functions such as PostgreSQL
  `abs(abs(...))` when transpiling to Fabric, including guard override coverage
  through Rust, C FFI, Go option encoding, and TypeScript typings.
- Regression coverage for PostgreSQL lateral joins targeting T-SQL and Fabric,
  including parenthesized join groups, comma-style `LATERAL` sources,
  non-trivial `ON` predicates, column alias preservation, and strict-mode
  rejection of residual unsupported lateral joins.
- Regression coverage for PostgreSQL function-style type casts targeting
  T-SQL and Fabric, including `numeric(...)`, `int4(...)`, `float8(...)`,
  `bool(...)`, `text(...)`, and unsafe strict-mode residuals.
- Regression coverage for PostgreSQL scalar-array membership with outer
  array casts targeting T-SQL and Fabric, including PostgreSQL
  `pg_get_querydef` / ruleutils-style `= ANY((ARRAY[...])::type[])` output.

### Fixed
- Excessively nested function-call inputs now fail with a regular
  `E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED` error instead of risking stack
  overflow or process aborts during parsing/transpilation.
- Moderate nested unary functions now parse more reliably through stack-growth
  protection and a lightweight parser fast path for common unary functions such
  as `ABS`, `SQRT`, `LOWER`, and `UPPER`.
- AST-depth guarding now preserves existing behavior for deep commentless
  `AND` / `OR` connector chains that are generated iteratively, avoiding
  regressions in optimizer and generator stress tests.
- Minimal `semantic` / `openlineage` feature builds no longer require the
  generator module for pivot suffix helpers.
- PostgreSQL `JOIN LATERAL` and `LEFT JOIN LATERAL` inside parenthesized
  `FROM` groups now transpile to valid T-SQL/Fabric `CROSS APPLY` and
  `OUTER APPLY` instead of emitting unsupported `JOIN LATERAL` SQL.
- PostgreSQL comma-style `FROM t, LATERAL (...)` now maps to T-SQL/Fabric
  `CROSS APPLY`, and lateral joins with non-trivial `ON` predicates now push
  the predicate into a filtered APPLY right-hand derived table while preserving
  the user-facing lateral alias.
- PostgreSQL function-style type casts now transpile to real T-SQL/Fabric
  `CAST(...)` expressions instead of invalid type-keyword function calls such
  as `NUMERIC(...)`, `INT4(...)`, or `FLOAT8(...)`; strict mode now rejects
  residual unsafe type-name calls that cannot be safely converted.
- PostgreSQL `= ANY((ARRAY[...])::type[])` scalar-array membership now
  transpiles to valid T-SQL/Fabric `IN (...)` predicates with the outer array
  element type pushed down to the individual values, and strict mode now
  rejects residual non-subquery `ANY` expressions for T-SQL/Fabric instead of
  returning invalid SQL.

## [0.5.12] - 2026-07-01

### Added
- Regression coverage for Trino/Presto `CREATE VIEW` parsing with `COMMENT`
  before `SECURITY`.
- Regression coverage for T-SQL `VARBINARY(MAX)`, `VARCHAR(MAX)`, and
  `NVARCHAR(MAX)` types across standalone datatype parsing, `CREATE TABLE`,
  `CAST`, and identity transpilation.
- Regression coverage for updated ClickHouse `* LIKE` / `* ILIKE` wildcard
  filters with trailing `EXCEPT` modifiers and nested wildcard filters inside
  `tuple(...)`.

### Changed
- Rust verification fixtures now track SQLGlot `v30.12.0` and ClickHouse
  `v26.6.1.1193-stable`.

### Fixed
- Trino/Presto `CREATE VIEW` parsing now accepts documented `COMMENT ...
  SECURITY ...` clause order while preserving the previously accepted
  `SECURITY ... COMMENT ...` order.
- T-SQL datatype parsing now accepts `VARBINARY(MAX)` consistently with the
  existing `VARCHAR(MAX)` and `NVARCHAR(MAX)` handling, so casts and DDL no
  longer fail with `Expected number`.
- SQLGlot identity verification compatibility for updated dialect fixtures,
  including DuckDB/PostgreSQL array contained-by operators, Databricks bracketed
  JSON path string keys, PostgreSQL JSON path `@?` predicates, SQLite ordered
  `ON CONFLICT` targets, BigQuery adjacent string literal comments, PostgreSQL
  function return/property normalization, and Snowflake `UNDROP`,
  `IDENTIFIER(...)()` calls, and inline table/view options.
- ClickHouse parser and normalized round-trip compatibility for updated
  fixtures, including escaped backticks in quoted identifiers, descending
  expressions inside table-property `ORDER BY (...)`, wildcard
  `LIKE`/`ILIKE ... EXCEPT` filters, nested wildcard filters in `tuple(...)`,
  and stable raw `EXPLAIN` subquery normalization.

## [0.5.11] - 2026-06-30

### Added
- Regression coverage for strict-mode rejection of PostgreSQL regex predicates
  when targeting Fabric or T-SQL.
- Regression coverage for T-SQL/Fabric `DATEPART` weekday, ISO week, and
  timezone-offset date-part generation.

### Fixed
- Strict Fabric/T-SQL transpilation now rejects remaining regular-expression
  predicates such as `SIMILAR TO`, `~`, `!~`, `~*`, `!~*`, and `REGEXP_LIKE`
  instead of reporting success with unsupported target SQL.
- T-SQL/Fabric `DATEPART` generation now maps internal canonical date parts
  back to valid target names such as `WEEKDAY`, `ISO_WEEK`, and `TZOFFSET`
  instead of emitting unsupported names like `DAYOFWEEK`.

## [0.5.10] - 2026-06-26

### Added
- Regression coverage for partial-schema `analyze_query` and
  `lineage_with_schema` calls across Rust core, C FFI, WASM/TypeScript, Python,
  and Go.
- Regression coverage for ordered `STRING_AGG`, `GROUP_CONCAT`, and `LISTAGG`
  transpilation to DuckDB.

### Fixed
- `analyze_query` and `lineage_with_schema` now tolerate partial schemas by
  resolving known columns while preserving unknown columns as best-effort
  lineage facts instead of aborting the analysis with `Unknown column`.
- DuckDB transpilation now emits ordered `STRING_AGG`, `GROUP_CONCAT`, and
  `LISTAGG` as `LISTAGG(expr, separator ORDER BY ...)` instead of the
  unsupported `WITHIN GROUP` form.

## [0.5.9] - 2026-06-23

### Added
- Regression coverage for schema-backed `analyze_query` lineage through nested
  set-operation derived tables, CTEs, and `UNNEST` output aliases across Rust
  core, C FFI, WASM/TypeScript, Python, and Go.

### Fixed
- `analyze_query` now resolves aliased and parenthesized query-like sources when
  deriving source output columns, so schema-backed nested `UNION` / `INTERSECT`
  / `EXCEPT` relations can still trace projections back to their physical input
  columns.
- `analyze_query` now recognizes `UNNEST`, lateral, and lateral-view virtual
  output aliases during schema-backed qualification, including both relation
  aliases and explicit output column aliases such as `UNNEST(t.arr) AS u(i)`.

## [0.5.8] - 2026-06-22

### Added
- Regression coverage for nested set-operation derived table lineage, scalar
  subqueries inside expression wrappers, same-select alias references,
  pivot/unpivot alias columns, and semi/anti join source inference across Rust
  core, C FFI, WASM/TypeScript, Python, and Go.

### Fixed
- Lineage now registers aliased query-like derived relations such as nested
  `UNION` expressions in `FROM`, so output columns trace back to each physical
  source table instead of stopping at the derived relation.
- Scalar subqueries wrapped in expressions such as `CASE`, `COALESCE`, `CAST`,
  `BETWEEN`, `IN`, `ANY`, `ALL`, and `EXISTS` now contribute their source-column
  lineage.
- Same-select alias references now resolve through earlier projection
  expressions before column qualification, avoiding unresolved or incorrect
  physical-column attribution.
- Pivot and unpivot lineage now preserves alias column lists and maps renamed or
  generated outputs back to implicit source columns and aggregate input columns.
- Star and source inference now ignores semi/anti join right-hand inputs,
  preventing non-output join sources from making schema-less star lineage
  ambiguous.

## [0.5.7] - 2026-06-19

### Added
- Regression coverage for recursive CTE, query-wrapper, set-operation,
  scalar-subquery, window, pivot, and unpivot lineage across Rust core,
  C FFI, WASM/TypeScript, Python, and Go.

### Changed
- Rust verification fixtures now track SQLGlot `v30.11.0` and ClickHouse
  `v26.5.2.39-stable`.

### Fixed
- Lineage now analyzes query-bearing wrappers such as `PREPARE`, CTAS,
  `CREATE VIEW`, and `INSERT ... SELECT` through their inner query instead of
  rejecting them as non-`SELECT` expressions.
- Recursive CTE lineage now terminates at the base case and preserves CTE
  provenance for recursive references instead of recursing indefinitely.
- Lineage now resolves parent CTEs through set-operation branches and scalar
  subqueries, so downstream nodes can trace back to the original base tables.
- Window-function lineage now includes dependencies from `PARTITION BY`,
  window `ORDER BY`, named window definitions, ordered aggregates, and
  `WITHIN GROUP` ordering.
- Pivot and unpivot lineage now maps generated output/value columns back to
  the underlying input columns.
- SQLGlot identity verification compatibility for updated dialect fixtures,
  including BigQuery `INFORMATION_SCHEMA` table qualification, ClickHouse
  variadic `xor(...)` and view schemas with `NOT NULL`, Databricks two-argument
  `DATEADD`, native `GET_JSON_OBJECT`, `CLUSTER BY NONE`, and collated string
  casts.
- Parser/generator round-tripping for newer dialect fixture cases, including
  MySQL `ALTER TABLE ... CHANGE COLUMN`, Oracle `TO_NUMBER(... DEFAULT ... ON
  CONVERSION ERROR)`, PostgreSQL `CREATE TYPE` variants, collated data types,
  PostgreSQL `<<->>` operators, T-SQL `FOR BROWSE`, and `NOT LIKE` / `NOT ILIKE`
  `ANY` and `ALL` forms.
- ClickHouse normalized round-tripping for updated table-function CTAS shapes,
  projection indexes, keyword CTE names such as `interval`, comma-style
  `OVERLAY(...)`, and newer parser tolerance cases from the ClickHouse corpus.
- Exasol transpilation now qualifies select-list aliases in `WHERE` and
  `HAVING` predicates with `LOCAL.<alias>`, matching Exasol's alias reference
  semantics.

## [0.5.6] - 2026-06-19

### Added
- Regression coverage for schema-less CTE star passthrough lineage across Rust
  core, C FFI, WASM/TypeScript, Python, and Go.

### Fixed
- Lineage now traces schema-less CTE `SELECT *` passthroughs when a single
  unambiguous source exists, so projections and aggregates such as
  `WITH c AS (SELECT * FROM t) SELECT SUM(c.x)` resolve back to `t.x` instead
  of stopping at the CTE column.
- The schema-less star fallback remains conservative for ambiguous multi-source
  stars and unresolved quoted table references, preserving quoted CTE/table case
  semantics.

## [0.5.5] - 2026-06-17

### Added
- Regression coverage for T-SQL `0x...` hex literals and nested
  query-analysis transform function detection.

### Fixed
- T-SQL `0x...` hex/binary literals now tokenize, parse, transpile, and
  round-trip in expressions such as predicates, `INSERT` values, and arithmetic
  expressions.
- T-SQL/Fabric generation now preserves hex string literals as native `0x...`
  values instead of emitting SQL-standard `x'...'` hex string syntax.
- `ProjectionFact.transformFunction` now reports a sole nested transform
  function inside passthrough wrappers such as `COALESCE(...)` and
  `CAST(... AS ...)`, while leaving ambiguous projections with multiple
  transform functions unset.
- Playground JSON tree panels now keep a bounded height and scroll correctly in
  the AST Explorer and Lineage JSON views.

## [0.5.4] - 2026-06-13

### Added
- `RelationFact` now includes nullable `catalog`, `schema`, and `table` fields
  for physical table relations while preserving the qualified `name` string.
- `ProjectionFact` now includes optional `transformFunction` /
  `transform_function` metadata for function-like projections, including the
  function name, literal arguments, and column arguments.
- AST transform helpers now support setting `ORDER BY` across Rust,
  C FFI, WASM/TypeScript, Python, and Go.
- C FFI, Python, and Go now expose AST transform helpers for setting `LIMIT`
  and `OFFSET`, matching the existing TypeScript SDK helper surface.
- Regression coverage for set-operation AST transforms, JOIN table-node
  discovery, nested dot-access builders, AST JSON compatibility, and
  function-projection facts across Rust, C FFI, WASM/TypeScript, Python, and Go.

### Changed
- Public AST JSON boundaries now use the shared compatibility deserializer for
  generation and AST transforms, so SDK-provided JSON shapes are normalized
  consistently before deserialization.

### Fixed
- `set_limit` / `setLimit` and `set_offset` / `setOffset` now apply to outer
  `UNION`, `INTERSECT`, and `EXCEPT` results instead of no-oping on set
  operations.
- TypeScript SDK `getTables(node)` now returns table expression nodes from JOIN
  operands instead of only the locally traversable table nodes.
- Rust and TypeScript builder output for `col("a.b.c")` now uses nested dot
  access (`a.b.c`) instead of treating `a.b` as a single quoted table
  qualifier.
- AST JSON inputs now accept the known legacy/unit-variant compatibility shapes
  at public API boundaries, including empty-object null payloads and `IS NULL`
  shorthand values.

## [0.5.3] - 2026-06-11

### Added
- `QueryAnalysis` now includes `cteFacts` / `cte_facts` for top-level CTE names,
  declared columns, original body SQL, and output columns.
- `QueryAnalysis` now includes `starProjections` / `star_projections` to retain
  provenance for original top-level `*` and `table.*` projections after schema
  expansion.
- `ProjectionFact` now includes conservative `nullability` classification:
  `non_null`, `nullable`, or `unknown`.
- Regression coverage for CTE facts, star projection provenance, and projection
  nullability across Rust, C FFI, WASM/TypeScript, Python, and Go.
- Regression coverage for `baseTables` discovery inside inline derived tables
  and derived-table set operations.

### Changed
- Validation schema documentation now explicitly covers the accepted JSON shape,
  including the `type` column key and nullability/constraint metadata.

### Fixed
- `QueryAnalysis.baseTables` now descends into inline derived tables
  (`FROM (SELECT ...) AS alias`), including derived-table set operations, so
  physical backing tables are reported consistently with CTE-backed queries.
- T-SQL parsing now accepts structured `SET IDENTITY_INSERT <table> ON|OFF`
  statements and round-trips them without falling back to an opaque command.

## [0.5.2] - 2026-06-11

### Added
- `QueryAnalysis` now includes `baseTables` / `base_tables`, a deduplicated
  list of transitive physical table dependencies collected across root queries,
  CTEs, derived tables, scalar subqueries, and set-operation branches.
- TypeScript SDK `analyzeQuery` now accepts a dialect shorthand argument such as
  `analyzeQuery(sql, Dialect.DuckDB)` in addition to the existing options
  object.
- Regression coverage for query-analysis aliases, qualified star expansion,
  schema-driven star expansion, aggregate classification, precise type hints,
  and transitive base-table reporting across Rust, C FFI, WASM/TypeScript,
  Python, and Go.

### Changed
- Compact query analysis now preserves parseable detailed validation-schema
  type strings such as `DECIMAL(10,2)` for projection `typeHint` values while
  leaving schema-aware validation's broad type-family behavior unchanged.
- Query-analysis documentation now distinguishes direct visible `relations`
  from transitive physical `baseTables` across Rust, Python, C FFI,
  WASM/TypeScript, and Go.

### Fixed
- Physical table aliases such as `orders AS o` are now preserved in
  query-analysis upstream references as `sourceAlias` while keeping the physical
  table identity as `orders`.
- Qualified star projections such as `o.*` now expand and trace only the
  matching source instead of all sources in the query.
- Schema-aware analysis now resolves columns qualified by query aliases before
  type annotation, so aliased table references keep correct upstream tables and
  type hints.
- Typed aggregate AST variants such as `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`,
  `COUNT_IF`, `SUM_IF`, `STRING_AGG`, `GROUP_CONCAT`, and `LISTAGG` are now
  classified as `aggregation` transforms in compact query analysis.

## [0.5.1] - 2026-06-09

### Added
- Standalone data-type parsing and generation convenience APIs across Rust,
  Python, C FFI, WASM/TypeScript, and Go, including Rust
  `parse_data_type` / `generate_data_type`, Python `parse_data_type` and
  `parse_one(..., into=polyglot_sql.DataType)`, FFI
  `polyglot_parse_data_type` / `polyglot_generate_data_type`, TypeScript
  `parseDataType` / `generateDataType`, and Go
  `ParseDataType` / `GenerateDataType`.
- Compact query-analysis convenience APIs across Rust, Python, C FFI,
  WASM/TypeScript, and Go, returning high-level query facts such as shape,
  projections, relations, CTEs, set-operation branches, and upstream column
  references without requiring callers to consume the full AST or lineage graph.
- Optional schema-aware query analysis that qualifies references and includes
  inferred projection types when validation schema metadata is supplied.
- PostgreSQL replication protocol command parsing for `BASE_BACKUP`,
  `CREATE_REPLICATION_SLOT`, `DROP_REPLICATION_SLOT`, `IDENTIFY_SYSTEM`,
  `READ_REPLICATION_SLOT`, `START_REPLICATION`, and `TIMELINE_HISTORY`.
- Regression coverage and documentation for the new data-type and query-analysis
  APIs across the Rust core and native SDKs.

### Fixed
- Snowflake `DATEDIFF`, `DATE_DIFF`, `TIMEDIFF`, and `TIMESTAMPDIFF`
  identity generation now preserves Snowflake's start/end argument order in
  direct, nested, aggregate, `CASE`, and `ORDER BY` contexts.
- T-SQL `SET STATISTICS TIME|IO|XML|PROFILE ON|OFF` statements now parse and
  round-trip as command statements, including comma-separated forms such as
  `SET STATISTICS IO, TIME ON`, while preserving structured parsing for simple
  options like `SET NOCOUNT ON`.
- PostgreSQL replication protocol commands now parse and generate instead of
  failing on command-style syntax such as
  `CREATE_REPLICATION_SLOT ... LOGICAL ...` and `START_REPLICATION ...`.

## [0.5.0] - 2026-06-07

### Added
- Strict unsupported-transpilation mode via `TranspileOptions.unsupported_level`,
  `TranspileOptions.max_unsupported`, and the `TranspileOptions::strict()`
  helper, with `UnsupportedLevel` re-exported from the Rust crate root and
  serialized as lower-case option values.
- Strict unsupported-transpilation options across C FFI, WASM, Python, Go, and
  the TypeScript SDK, including Python `unsupported_level` /
  `max_unsupported`, Go `UnsupportedLevel` constants, and TypeScript
  `TranspileOptions`.
- Regression coverage for strict unsupported transpilation in Rust core, C FFI,
  WASM, Python, Go option serialization, and TypeScript SDK type/API tests.
- Lineage and scope support for table-generating virtual sources beyond
  BigQuery `UNNEST`, including PostgreSQL/Presto/Trino `UNNEST` aliases,
  Spark/Hive `LATERAL VIEW EXPLODE`, and Snowflake `LATERAL FLATTEN`.

### Changed
- Transpilation now passes full `TranspileOptions` into the target generator,
  so generator unsupported diagnostics honor caller-selected
  `unsupported_level` and `max_unsupported` settings.
- Strict unsupported checks run after source normalization and target rewrites,
  allowing successful rewrites to proceed while rejecting only known unsupported
  constructs that still remain in the final output AST.

### Fixed
- Strict unsupported mode now rejects known lossy or unsupported target output
  such as Fabric/Hive recursive CTEs, remaining T-SQL/Fabric `LATERAL`, target
  unsupported `UNNEST`/`EXPLODE`, lossy `ARRAY_AGG`, and PostgreSQL-specific
  `JSONB_BUILD_OBJECT` / `TO_TSVECTOR` calls when the target dialect cannot
  safely represent them.
- Default transpilation remains permissive for the same constructs, preserving
  existing behavior unless callers explicitly opt into strict unsupported
  handling.
- Lineage for unqualified columns emitted by table-generating functions now
  resolves to the matching virtual source node and preserves downstream
  dependencies on the real table columns feeding the virtual source.
- Snowflake `LATERAL FLATTEN` lineage now treats `FLATTEN` output columns such
  as `value` as virtual source columns and traces them back to the referenced
  input expression.
- Column-reference collection now follows `LATERAL`, `LATERAL VIEW`, and JSON
  extraction expressions used inside virtual table-generating sources.

## [0.4.4] - 2026-06-03

### Added
- Lineage nodes now expose `source_kind` and optional `source_alias` metadata
  across Rust core, C FFI, WASM, Python, Go, and the TypeScript SDK so clients
  can distinguish physical tables, CTEs, derived tables, virtual sources, and
  unresolved sources.
- Regression coverage for BigQuery `UNNEST` lineage metadata across the native
  wrappers and SDKs, including Go and TypeScript SDK assertions.

### Changed
- OpenLineage column lineage now ignores purely virtual terminal sources such
  as standalone BigQuery `UNNEST(...) AS alias` values instead of emitting
  synthetic physical input fields, while table-backed `UNNEST(t.items)` still
  resolves to the real source column.
- PostgreSQL-to-T-SQL and PostgreSQL-to-Fabric transpilation now materializes
  scalar boolean value expressions as `CASE` values in projection, grouping,
  ordering, and window contexts while preserving predicate contexts such as
  `WHERE` and `HAVING`.
- The `test-go-integration` Makefile target now resolves the platform-specific
  FFI library name through an explicit shell variable before running Go SDK
  integration tests.

### Fixed
- BigQuery `UNNEST(...) AS alias` lineage now uses stable synthetic virtual
  source names such as `_0`/`_1`, preserves the user-written alias separately,
  and avoids treating real tables with the same name as virtual sources.
- PostgreSQL-to-T-SQL and PostgreSQL-to-Fabric transpilation now omits the
  unsupported `RECURSIVE` keyword from recursive CTE output.
- PostgreSQL `CROSS JOIN LATERAL`, `JOIN LATERAL`, `INNER JOIN LATERAL`, and
  `LEFT JOIN LATERAL` now transpile to T-SQL/Fabric `CROSS APPLY` or
  `OUTER APPLY` when the join has no meaningful join predicate, and Fabric now
  preserves native `APPLY` joins during generation.
- T-SQL and Fabric output now strips unsupported window frames from
  ranking/navigation window functions such as `ROW_NUMBER`, `RANK`, `NTILE`,
  `LEAD`, and `LAG`; framed named windows are inlined only for the affected
  function so aggregate uses of the same named window remain unchanged.
- PostgreSQL `MOD(a, b)` now lowers to the T-SQL/Fabric `%` operator and keeps
  compound arguments parenthesized to preserve precedence.
- PostgreSQL `POSITION(substr IN str)` and `STRPOS(str, substr)` now transpile
  to Fabric `CHARINDEX(substr, str)`.
- PostgreSQL row-value `IN` subqueries such as `(a, b) IN (SELECT x, y ...)`
  now transpile to correlated `EXISTS` predicates for T-SQL and Fabric when the
  arity matches, while `NOT IN` and mismatched arity cases are left unchanged.
- Fabric now maps unqualified PostgreSQL `NUMERIC`/`DECIMAL` casts to
  `DECIMAL(38, 10)` while preserving explicit precision and scale.
- PostgreSQL statistical aggregates `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`,
  and `VAR_POP` now map to the corresponding T-SQL/Fabric function names
  `STDEV`, `STDEVP`, `VAR`, and `VARP`.
- PostgreSQL boolean aggregates `BOOL_AND`, `BOOL_OR`, and `EVERY`, including
  filtered variants, now transpile to null-preserving T-SQL/Fabric `MIN`/`MAX`
  `CASE` aggregates cast to `BIT`.
- T-SQL and Fabric aggregate `FILTER (WHERE ...)` clauses now transpile to
  conditional aggregate inputs for counts, distinct counts, ordinary
  aggregates, and window aggregates.
- PostgreSQL `STRING_AGG(value, sep ORDER BY ...)` now emits T-SQL/Fabric
  `WITHIN GROUP (ORDER BY ...)`, including NULL-ordering emulation where
  needed.
- PostgreSQL `BPCHAR` casts, double-colon casts, and DDL column definitions now
  normalize to `CHAR` for T-SQL and Fabric.
- PostgreSQL `= ANY(ARRAY[...])` and `= ANY((...))` predicates now transpile to
  `IN (...)` for T-SQL and Fabric, with empty arrays lowering to an always
  false predicate.
- PostgreSQL-to-Fabric and PostgreSQL-to-T-SQL transpilation now attaches
  trailing set-operation `ORDER BY`, `LIMIT`, and `OFFSET` clauses to the
  outer `UNION`/`INTERSECT`/`EXCEPT` result instead of the right-hand operand.
- Fabric and T-SQL set operations that need row limiting or NULL-ordering
  emulation are now wrapped in an outer query before applying `ORDER BY` /
  `TOP` / `OFFSET ... FETCH`, avoiding invalid `CASE WHEN ... IS NULL` sort
  keys inside the set-operation `ORDER BY`.
- Set-operation `ORDER BY` expressions now participate in cross-dialect
  NULL-ordering normalization, preserving PostgreSQL NULL sort semantics when
  transpiling to Fabric or T-SQL.

## [0.4.3] - 2026-06-02

### Added
- Structured PostgreSQL/generic prepared statement support with a new
  `Prepare` AST node for `PREPARE name [(type, ...)] AS statement`.
- PostgreSQL-style prepared statement execution arguments on `Execute` nodes,
  allowing `EXECUTE name(...)` to parse and round-trip without falling back to
  opaque command text.
- Regression coverage for prepared statement parse/generate/source-table flows
  across Rust, C FFI, WASM, Python, and the TypeScript SDK.

### Fixed
- Lineage, source-table extraction, traversal, scope analysis, and OpenLineage
  output now inspect the body of structured `PREPARE` statements.
- BigQuery `UNNEST(...) AS alias` sources now participate in lineage and
  OpenLineage column lineage, avoiding empty field lineage for projections such
  as `SELECT date_val AS week_start FROM UNNEST(...) AS date_val`.
- Python bindings now expose the new `Prepare` expression subclass in the
  native dispatcher, package exports, and type stubs.
- Documentation builds no longer fail on TypeDoc analysis of the TypeScript SDK
  manual WASM loader internals.

## [0.4.2] - 2026-05-25

### Added
- TypeScript SDK manual WASM loader entry point at
  `@polyglot-sql/sdk/manual`, allowing bundlers such as esbuild to initialize
  the SDK with an explicitly imported WASM URL.
- Public TypeScript SDK WASM asset export at
  `@polyglot-sql/sdk/polyglot_sql.wasm`, published as
  `dist/polyglot_sql.wasm`.
- SDK bundler smoke coverage for esbuild, package export resolution, Node ESM,
  and CommonJS loading.

### Changed
- TypeScript SDK builds are now split into browser/default ESM, Node ESM,
  CommonJS, CDN ESM, and manual-loader artifacts so each runtime uses an
  appropriate WASM loading path.
- SDK, playground, and CI build flows now generate both wasm-bindgen bundler
  and web wrappers, verify the renamed WASM artifact set, and run the bundler
  smoke checks.
- TypeScript SDK documentation now includes explicit bundler guidance for Vite,
  esbuild, Next.js/webpack, Node ESM, CommonJS, and CDN/self-hosted usage.

### Fixed
- Browser/default TypeScript SDK bundles no longer include Node `node:fs` /
  `node:url` compatibility code, fixing browser bundlers that reject Node
  built-ins.
- esbuild users can now bundle without manually copying opaque SDK internals by
  importing the documented `polyglot_sql.wasm` asset and using the manual
  loader entry.

## [0.4.1] - 2026-05-23

### Changed
- `DropIndex.name` now uses the existing `TableRef` AST shape instead of a
  flattened `Identifier`, preserving qualified index targets and per-part
  quoted identifier metadata.
- The `semantic` Cargo feature no longer implies `generate`, so semantic-only
  Rust builds can compile without pulling in SQL generation.

### Fixed
- PostgreSQL `DROP INDEX` generation now preserves quoted mixed-case index
  names, including identity transpilation such as
  `DROP INDEX IF EXISTS "idx_tokenKey__pb_users_auth_"`.
- SQLite-to-PostgreSQL transpilation now converts backtick-quoted `DROP INDEX`
  names to PostgreSQL double-quoted identifiers instead of emitting an
  unquoted, lowercased-by-PostgreSQL name.
- Qualified quoted `DROP INDEX` targets such as `"public"."IdxName"` now
  round-trip without losing quote state on individual name parts.
- `openlineage` and `semantic` feature combinations without `generate` now
  compile; default/full builds continue to emit the same generated SQL
  transformation descriptions as before.

## [0.4.0] - 2026-05-22

### Added
- Official Go SDK at `packages/go`, published as the nested Go module
  `github.com/tobilg/polyglot/packages/go`.
- PureGo-based Go bindings over the native `polyglot-sql-ffi` shared library,
  with explicit client lifecycle management and no cgo or runtime native
  library downloads by default.
- Go SDK APIs for transpilation, formatting, optimization, generation,
  validation, dialect listing, parsing/tokenization, AST diff, table
  transforms, lineage/source-table extraction, and OpenLineage payload
  generation.
- Go SDK typed option/result structs, default-client helpers, error wrapping,
  native library path resolution, FFI layout checks, unit tests, integration
  tests, and setup documentation.
- C FFI OpenLineage exports:
  `polyglot_openlineage_column_lineage`,
  `polyglot_openlineage_job_event`, and
  `polyglot_openlineage_run_event`.
- Cargo capability feature gates for `polyglot-sql`: `generate`, `transpile`,
  `builder`, `ast-tools`, `semantic`, `openlineage`, `diff`, `planner`, and
  `time`.
- Documented parser-only Rust builds via `default-features = false`.
- Makefile targets for Go SDK builds/tests and Rust feature-gate verification:
  `build-go`, `test-go`, `test-go-integration`, and
  `test-rust-feature-gates`.

### Changed
- The default Rust feature set continues to expose the full existing API, while
  optional capability groups now support smaller parser-only or selected-feature
  builds.
- `polyglot-sql-wasm` now explicitly opts into the full core capability set so
  existing JavaScript/WASM exports remain available with the new core feature
  gates.
- Release automation now verifies the Go SDK against the release-built FFI
  artifact and creates matching nested Go module tags such as
  `packages/go/v0.4.0`.
- Version bump helpers now update the Go SDK runtime version constant.

### Fixed
- SQLite-source transpilation now converts double-quoted column defaults such
  as `DEFAULT "base"`, `DEFAULT "[]"`, and `DEFAULT "{}"` into string
  literals for non-SQLite targets, fixing invalid PostgreSQL output while
  preserving SQLite identity output.

## [0.3.12] - 2026-05-18

### Added
- OpenLineage-compatible lineage payload generation in the Rust core via the
  new `polyglot_sql::openlineage` module. The new helpers produce standalone
  `columnLineage` dataset facets plus inferred input/output datasets, and can
  also build OpenLineage `JobEvent` and `RunEvent` JSON payloads.
- OpenLineage column lineage support for `SELECT`, `INSERT ... SELECT`, and
  `CREATE TABLE AS SELECT`, including dataset namespace/mapping options,
  target-column remapping for `INSERT INTO target(col) SELECT ...`, table alias
  resolution, direct/transformation/aggregation classification, and optional
  schema facet generation from validation schema metadata.
- WASM exports for OpenLineage payload generation:
  `openlineage_column_lineage`, `openlineage_job_event`, and
  `openlineage_run_event`.
- TypeScript SDK helpers and types:
  `openLineageColumnLineage`, `openLineageJobEvent`, `openLineageRunEvent`,
  and the associated OpenLineage option/result interfaces.
- Python binding functions:
  `openlineage_column_lineage`, `openlineage_job_event`, and
  `openlineage_run_event`.
- Documentation for OpenLineage output in the root README, TypeScript SDK
  README, and Python bindings README.
- Focused Rust, WASM, TypeScript SDK, and Python regression coverage for the
  new OpenLineage payload generation paths.

### Notes
- OpenLineage transport/client behavior remains intentionally out of scope.
  Polyglot generates JSON-compatible payloads for callers to inspect, persist,
  or emit through their own infrastructure.
- OpenLineage JSON Schema URLs are emitted as fixed `schemaURL` / `_schemaURL`
  references. Schemas are not downloaded or validated at runtime.

## [0.3.11] - 2026-05-15

### Added
- Regression coverage for issue #201 across Rust, Python, C FFI, WASM, and the
  TypeScript SDK.
- TypeScript/WASM builder `toSql(dialect?)` support for `Expr` and
  `CaseBuilder`, allowing builder-generated SQL to use dialect-specific
  generation rules instead of always using the generic dialect.

### Fixed
- T-SQL `NCHAR`/`NVARCHAR` parsing and generation now preserves national
  character types for T-SQL identity transpilation, including `NVARCHAR(MAX)`.
- Fabric generation now maps unsupported `NCHAR`/`NVARCHAR` casts to
  `CHAR`/`VARCHAR` while preserving lengths, including `(MAX)`.
- Snowflake timestamp variant casts now parse and generate consistently for
  `TIMESTAMP_TZ`, `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`, and their no-underscore
  aliases with optional precision.

## [0.3.10] - 2026-05-14

### Added
- Rust AST table transform options for table qualification and table renaming,
  including generated aliases for unaliased tables/subqueries, configurable
  alias prefixes, set-operation subquery normalization, and optional aliases for
  renamed tables.
- WASM AST transform exports for table qualification and table renaming with
  options: `ast_qualify_tables` and `ast_rename_tables_with_options`.
- TypeScript SDK visitor helpers for `qualifyTables` and enhanced
  `renameTables` options.
- Python binding functions `qualify_tables` and `rename_tables`, accepting both
  single expressions and expression lists.
- C FFI AST transform functions `polyglot_qualify_tables` and
  `polyglot_rename_tables_with_options`.
- Regression coverage for PostgreSQL-to-T-SQL/Fabric transpilation, AST table
  transforms, Python transform bindings, and FFI transform bindings.

### Fixed
- T-SQL and Fabric generation now emulate unsupported `NULLS FIRST` /
  `NULLS LAST` ordering with CASE sort keys when required, while avoiding that
  rewrite for random ordering expressions.
- Fabric `LIMIT`/`OFFSET` generation now uses T-SQL-style `OFFSET ... ROWS` /
  `FETCH NEXT ... ROWS ONLY` syntax where appropriate.
- Table qualification now handles set-operation operands and parser-produced
  passthrough `SELECT * FROM (<set op>)` wrappers more reliably.

## [0.3.9] - 2026-05-11

### Added
- Python binding regression coverage for structured T-SQL `BEGIN TRY` /
  `BEGIN CATCH` parsing, ensuring parsed nodes are exposed as the typed
  `polyglot_sql.TryCatch` subclass.

### Fixed
- Python release builds now include the `TryCatch` expression variant in the
  native subclass dispatcher, fixing the non-exhaustive Rust match introduced
  by structured TRY/CATCH AST support.
- Python package exports and type stubs now expose `TryCatch`, and the stubs
  avoid shadowing `typing.Any` with the SQL expression subclass named `Any`.

## [0.3.8] - 2026-05-11

### Added
- T-SQL regression coverage for structured `BEGIN TRY` / `BEGIN CATCH`
  parsing, traversal, SQL generation, and table extraction from block bodies.
- PostgreSQL-to-Fabric TPC-H regression coverage for all 22 benchmark queries.
- T-SQL regression coverage for multi-statement `DECLARE` batches, including
  table variables followed by `INSERT` and scalar declarations followed by
  `SELECT`.
- AST transform regression coverage for DML statements, including
  `UPDATE ... FROM/JOIN`, PostgreSQL `DELETE ... USING`, and T-SQL
  `OUTPUT ... INTO` clauses.

### Fixed
- T-SQL `BEGIN TRY` / `BEGIN CATCH` blocks now parse into structured AST nodes
  instead of opaque command text, so traversal, lineage, scope analysis, and
  generation can inspect the inner statements.
- T-SQL `DECLARE` parsing now preserves top-level statement boundaries, so
  batches such as `DECLARE @tmp TABLE (...); INSERT INTO @tmp SELECT ...` parse
  as separate statements.
- `transform_recursive`, `rename_tables`, and `replace_by_type` now visit table
  references and expression fields inside `UPDATE` and `DELETE` statements,
  including DML targets, `FROM`/`USING` sources, joins, `OUTPUT`, `RETURNING`,
  `WITH`, `ORDER BY`, and `LIMIT`.

## [0.3.7] - 2026-05-07

### Added
- Full-corpus ClickHouse normalized round-trip verification for
  `make test-rust-clickhouse-coverage`, covering all 85,380 extracted fixture
  statements.

### Changed
- ClickHouse coverage now validates stable same-dialect normalization
  (`parse -> transform/generate -> parse -> transform/generate`) instead of
  exact source identity, while preserving the complete fixture corpus total and
  requiring `85,380/85,380` passing statements.

### Fixed
- ClickHouse parser/generator regressions for terminal backslash-escaped string
  quotes, native `COALESCE`/`IFNULL` spelling, prefix `NOT` precedence, nested
  lambda bodies, single-value `IN` lists, `sum(NULL)`, and same-dialect
  `EXCEPT ALL` identity output.
- ClickHouse normalization stability for malformed corpus probes and dialect
  syntax including partial `WITH` statements, incomplete extracted subqueries,
  `SAMPLE`, TTL `SET`, `Enum8`/`Enum16`, table-function CTAS, quoted dotted
  aliases, and `ALTER TABLE ... UPDATE` mutations.

## [0.3.6] - 2026-05-06

### Added
- BigQuery parser support for alias-first `UNNEST ... WITH OFFSET` table
  expressions, including `UNNEST(arr) AS elem WITH OFFSET AS off` and
  normalization of `WITH OFFSET off` to `WITH OFFSET AS off`.
- Focused round-trip coverage for BigQuery `UNNEST` with offset aliases in
  both `FROM` and `CROSS JOIN` table expressions.

### Changed
- Updated SQLGlot and ClickHouse fixture pins to `v30.7.0` and
  `v26.2.17.31-stable`.
- Cleaned up Makefile target metadata, help output, and `.PHONY` coverage so
  advertised development commands resolve consistently.

### Fixed
- SQLGlot fixture compatibility for updated dialect identity and transpilation
  cases, including MySQL DDL/charset forms, DuckDB FROM-first joins, Redshift
  approximate percentile and interval output, Snowflake string/array/UUID
  rewrites, Oracle `JSON_TABLE ... FORMAT JSON`, Exasol `OPEN`, and BigQuery
  drop-primary-key forms.
- Fabric/T-SQL interval arithmetic rewrites for additional interval expression
  shapes, including cast interval values and colon parameters.
- Broken `make test-rust-functions` target, which now runs the existing
  function-focused library tests.

## [0.3.5] - 2026-04-29

### Fixed
- PostgreSQL and SQLite transpilation regressions around TPCH-style stack-heavy
  queries and dialect-specific expression rewrites.
- Snowflake parser support for dollar-string literals in `PUT`/`FROM` command
  forms and UUID-like paths in stage references.
- Python compatibility tests for Snowflake connector command handling.

## [0.3.4] - 2026-04-27

### Added
- Databricks parser support for additional Delta Lake and Lakeflow command
  forms:
  - `CREATE TABLE ... DEEP CLONE ... VERSION/TIMESTAMP AS OF ...`
  - `REPAIR TABLE` / `MSCK REPAIR TABLE`
  - `APPLY CHANGES INTO`, `AUTO CDC INTO`, and `CREATE FLOW ... AS AUTO CDC`
  - `GENERATE symlink_format_manifest FOR TABLE ...`
  - `CONVERT TO DELTA ...`
- Snowflake scripting block support for `DECLARE ... BEGIN ... END` cursor
  blocks, including cursor `OPEN` and `RETURN TABLE(RESULTSET_FROM_CURSOR(...))`
  command bodies.
- PostgreSQL to T-SQL/Fabric rewrites for scalar array comparisons:
  `= ANY(ARRAY[...])` and `= ANY((...))` now emit `IN (...)`, with empty
  arrays rewritten to an always-false predicate.

### Changed
- Raw command parsing now preserves original source text when available, keeping
  dialect-specific quoted paths and procedural command bodies intact.

### Fixed
- Databricks parsing failures for Delta maintenance, clone, CDC, manifest, and
  conversion commands.
- Snowflake parsing failures for anonymous scripting blocks with cursor
  declarations.
- Regression coverage for MySQL stored procedures using `SIGNAL SQLSTATE` inside
  `BEGIN ... END` blocks.

## [0.3.3] - 2026-04-24

### Added
- Native stack-growth support for deep parser/transform paths via the default
  `stacker` feature, while keeping WASM builds stacker-free.
- Stack-focused regression coverage for deeply nested transpilation and
  ClickHouse/Snowflake parser edge cases.

### Changed
- Refactored recursive parser/transform hot paths to reduce per-level stack
  usage and remove unnecessary manually enlarged test/runtime stacks.
- Updated ClickHouse and sqlglot verification fixtures for the current
  transpilation behavior.
- Documented stack-size behavior, default native safety settings, and WASM/FFI
  integration notes in the README.

### Fixed
- Snowflake Python connector query parsing failures involving empty argument
  lists, optional ODBC call wrappers, and connector-specific SQL forms.
- Deep transpilation/normalization stack handling for large generated queries
  without relying on oversized worker-thread stacks by default.

## [0.3.0] - 2026-04-06

### Added
- `Dialect::transpile` and `Dialect::transpile_with` — the new canonical method
  API for transpilation from a `Dialect` handle. Both accept either a
  `DialectType` enum or a `&Dialect` reference as the target via the new
  `TranspileTarget` trait (implemented for both).
- `TranspileOptions` struct (`#[non_exhaustive]`) carrying transpile
  configuration, with `TranspileOptions::pretty()` helper for pretty-printed
  output. Derives `Serialize`/`Deserialize` (camelCase) for JSON bridges.
- `TranspileTarget` trait — end users don't normally implement this themselves;
  the blanket impls for `DialectType` and `&Dialect` cover built-in and custom
  dialects uniformly.
- `polyglot_sql::transpile_with_by_name` free function — string-keyed variant
  of the new API, used by the C FFI and Python bindings.
- **C FFI**: new `polyglot_transpile_with_options(sql, from, to, options_json)`
  function. `options_json` is a JSON object compatible with `TranspileOptions`,
  e.g. `{"pretty": true}`.

### Changed
- **Breaking (Rust API)**: `polyglot_sql::transpile` and
  `polyglot_sql::transpile_by_name` now apply the full cross-dialect rewrite
  pipeline (`cross_dialect_normalize`) — matching the WASM / FFI / Python
  bindings and the playground. Previously these two entry points bypassed
  source+target-aware normalization, so Rust/Python/C FFI consumers silently
  received under-transformed SQL (including semantically wrong output for some
  DuckDB → Trino patterns such as `CAST(x AS JSON)` and `to_timestamp(x)`).
- `transform_recursive` now recurses into `Expression::CreateView.query`, so
  cross-dialect transforms apply inside view bodies (e.g.
  `CREATE VIEW v AS SELECT to_timestamp(x) FROM t`).

### Removed
- **Breaking (Rust API)**: removed `Dialect::transpile_to`,
  `Dialect::transpile_to_pretty`, `Dialect::transpile_to_dialect`, and
  `Dialect::transpile_to_dialect_pretty` — superseded by the unified
  `Dialect::transpile` / `Dialect::transpile_with` pair.
  - Migration: `dialect.transpile_to(sql, DialectType::X)` →
    `dialect.transpile(sql, DialectType::X)`
  - Migration: `dialect.transpile_to_pretty(sql, DialectType::X)` →
    `dialect.transpile_with(sql, DialectType::X, TranspileOptions::pretty())`
  - Migration: `dialect.transpile_to_dialect(sql, &target)` →
    `dialect.transpile(sql, &target)`

## [0.1.9] - 2026-02-25

### Added
- Rust-side pre-parse formatting guard for deep set-operation chains (`UNION` / `INTERSECT` / `EXCEPT`) with new error code `E_GUARD_SET_OP_CHAIN_EXCEEDED`.
- New FFI API: `polyglot_format_with_options(sql, dialect, options_json)` to override formatting guard limits per call.
- Python formatting per-call guard overrides via keyword-only arguments on `format_sql(...)` / `format(...)`:
  - `max_input_bytes`
  - `max_tokens`
  - `max_ast_nodes`
  - `max_set_op_chain`

### Changed
- Formatting guard defaults now include `maxSetOpChain = 256` to fail fast before deep set-op stack-overflow scenarios in large generated queries.
- ClickHouse `minus(...)` is treated as a function call in set-op guard detection (not as a set operation), avoiding false positives.

### Fixed
- Large/deep set-operation formatting now returns deterministic guard failures instead of process-level stack overflows in affected cases.
- FFI and Python interfaces are now aligned with Rust/WASM/SDK by supporting per-call formatting guard configuration.

## [0.1.8] - 2026-02-24

### Added
- New `polyglot-sql-python` crate with first-party Python bindings (PyO3 + maturin) for parse, transpile, generate, format, validate, diff, lineage, and optimize flows.
- Release CI support for Python package publishing (multi-platform wheel builds + PyPI publish on `v*` tags).
- Dedicated benchmark project in `tools/bench-compare` with isolated `uv` environment management.

### Fixed
- Python local build/test artifacts are now ignored in git (`__pycache__`, `.pytest_cache`, coverage files, and related caches).

## [0.1.7] - 2026-02-24

### Added
- New `polyglot-sql-ffi` crate with a C-compatible API surface for parse, transpile, generate, format, validate, diff, lineage, and optimize flows.
- Generated C header and end-to-end C example (`examples/c`) for native integration.
- Release CI now builds multi-platform FFI artifacts and publishes archives plus checksums to GitHub Releases for `v*` tags.
- Playground capabilities for lineage and schema-aware validation workflows.

### Changed
- SDK/WASM bridge now supports structured value APIs (`transpile_value`, `parse_value`, `generate_value`, `format_sql_value`, `get_dialects_value`) to reduce JSON serialization overhead.
- Parser now rebalances long `AND`/`OR` chains into bounded-depth trees to improve stack behavior on very large predicates.
- CI release pipeline hardening for SDK/WASM/docs/playground version consistency checks against the release tag.
- Build/test flow updates across Makefile and CI to keep Rust, WASM, SDK, docs, playground, and FFI outputs aligned.

### Fixed
- Large-SQL formatting robustness (issue #27 reproduction case) for high condition counts in WASM/SDK usage.
- WASM->TypeScript AST shape compatibility regressions by using JSON-compatible structured serialization.
- Additional parser, optimizer, and validation edge cases discovered during large-query and release-hardening work.

## [0.1.6] - 2026-02-23

### Added
- Schema-aware validation Phase 2 capabilities in `polyglot-sql`:
  - optional type checks (`check_types`)
  - optional reference/FK checks (`check_references`)
  - expanded schema model for keys and references
  - new diagnostics (`E210-E217`, `W210-W216`, `E220`, `E221`, `W220`, `W221`, `W222`)
- WASM and TypeScript SDK wiring for the new schema validation metadata and options.
- Additional validation coverage in Rust, WASM, and SDK tests for type/reference rules.

### Changed
- Canonical SQL generation for `NOT IN` in the WASM builder path to avoid non-canonical output.
- SDK build pipeline now performs WASM extraction/rewrite via a Vite plugin instead of a standalone post-build script.
- WASM release optimization profile no longer uses `--converge` to reduce build-time variance in `wasm-opt`.
- CI release hardening:
  - SDK typecheck in the `sdk-build` job
  - deploy-time docs version assertion against release tag
  - deploy-time playground SDK/WASM version assertion against release tag

### Fixed
- Multiple schema validation edge cases around comparison, arithmetic, assignment, set-ops, and join/reference quality diagnostics.
- Strict/non-strict severity behavior alignment for new validation rule families.
- Generated TypeScript binding diagnostics in the SDK:
  - removed problematic doc-comment patterns that broke generated JSDoc parsing
  - removed `Index.ts` renaming in binding copy flow to avoid case-sensitive import conflicts

[0.4.3]: https://github.com/tobilg/polyglot/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/tobilg/polyglot/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/tobilg/polyglot/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/tobilg/polyglot/compare/v0.3.12...v0.4.0
[0.3.12]: https://github.com/tobilg/polyglot/compare/v0.3.11...v0.3.12
[0.3.11]: https://github.com/tobilg/polyglot/compare/v0.3.10...v0.3.11
[0.3.10]: https://github.com/tobilg/polyglot/compare/v0.3.9...v0.3.10
[0.3.9]: https://github.com/tobilg/polyglot/compare/v0.3.8...v0.3.9
[0.3.8]: https://github.com/tobilg/polyglot/compare/v0.3.7...v0.3.8
[0.3.7]: https://github.com/tobilg/polyglot/compare/v0.3.6...v0.3.7
[0.3.6]: https://github.com/tobilg/polyglot/compare/v0.3.5...v0.3.6
[0.3.5]: https://github.com/tobilg/polyglot/compare/v0.3.4...v0.3.5
[0.3.4]: https://github.com/tobilg/polyglot/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/tobilg/polyglot/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/tobilg/polyglot/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/tobilg/polyglot/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/tobilg/polyglot/compare/v0.1.9...v0.3.0
[0.1.9]: https://github.com/tobilg/polyglot/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/tobilg/polyglot/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/tobilg/polyglot/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/tobilg/polyglot/compare/v0.1.5...v0.1.6
