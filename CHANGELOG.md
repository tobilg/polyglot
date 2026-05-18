# Changelog

All notable changes to this project are documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

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
