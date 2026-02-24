# Changelog

All notable changes to this project are documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

## [0.1.7] - Unreleased

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

[Unreleased]: https://github.com/tobilg/polyglot/compare/v0.1.7...HEAD
[0.1.7]: https://github.com/tobilg/polyglot/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/tobilg/polyglot/compare/v0.1.5...v0.1.6
