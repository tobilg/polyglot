# Changelog

All notable changes to this project are documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

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

[0.3.4]: https://github.com/tobilg/polyglot/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/tobilg/polyglot/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/tobilg/polyglot/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/tobilg/polyglot/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/tobilg/polyglot/compare/v0.1.9...v0.3.0
[0.1.9]: https://github.com/tobilg/polyglot/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/tobilg/polyglot/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/tobilg/polyglot/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/tobilg/polyglot/compare/v0.1.5...v0.1.6
