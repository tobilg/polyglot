.PHONY: help \
        setup-sqlglot setup-clickhouse-tests setup-external \
        extract-fixtures extract-clickhouse-fixtures extract-all-fixtures \
        test-rust test-rust-all test-rust-identity test-rust-dialect \
        test-rust-transpile test-rust-pretty test-rust-roundtrip test-rust-matrix \
        test-rust-compat test-rust-errors test-rust-functions test-rust-custom test-rust-lib test-rust-verify \
        test-rust-transpile-generic test-rust-parser \
        test-rust-clickhouse-parser test-rust-clickhouse-coverage \
        test-ffi \
        test-compare build-wasm clean-fixtures clean-clickhouse-fixtures clean-external clean \
        generate-bindings copy-bindings cargo-build-release \
        build-ffi build-ffi-static generate-ffi-header build-ffi-example clean-ffi \
        develop-python test-python build-python typecheck-python \
        python-docs-build python-docs-preview python-docs-deploy \
        bench-compare bench-rust bench-python bench-parse bench-parse-quick bench-parse-full \
        playground-dev playground-build playground-preview playground-deploy \
        fmt \
        bump-version

# =============================================================================
# Pinned External Project Versions
# =============================================================================

SQLGLOT_REPO := https://github.com/tobymao/sqlglot.git
SQLGLOT_REF := v28.10.1

CLICKHOUSE_REPO := https://github.com/ClickHouse/ClickHouse.git
CLICKHOUSE_REF := v26.1.3.52-stable

# Default target
help:
	@echo "Polyglot Development Commands"
	@echo "=============================="
	@echo ""
	@echo "Fixture Management:"
	@echo "  make setup-external          - Clone external repos (sqlglot, ClickHouse)"
	@echo "  make extract-fixtures        - Extract sqlglot test fixtures"
	@echo "  make extract-clickhouse-fixtures - Extract ClickHouse test fixtures"
	@echo "  make extract-all-fixtures    - Extract all fixtures (sqlglot + ClickHouse)"
	@echo "  make clean-fixtures          - Remove extracted sqlglot fixtures"
	@echo "  make clean-clickhouse-fixtures - Remove ClickHouse fixtures"
	@echo "  make clean-external          - Remove external project clones"
	@echo ""
	@echo "Rust Tests (fast):"
	@echo "  make test-rust           - Run all Rust tests"
	@echo "  make test-rust-all       - Run all sqlglot fixture tests"
	@echo "  make test-rust-lib       - Run lib unit tests (736)"
	@echo "  make test-rust-verify    - Run full Rust verification suite incl. FFI"
	@echo ""
	@echo "  SQLGlot Fixture Tests:"
	@echo "  make test-rust-identity         - Generic identity tests (955)"
	@echo "  make test-rust-dialect          - Dialect identity tests"
	@echo "  make test-rust-transpile        - Transpilation tests"
	@echo "  make test-rust-pretty           - Pretty-printing tests (24)"
	@echo "  make test-rust-transpile-generic - Normalization/transpile tests (test_transpile.py)"
	@echo "  make test-rust-parser           - Parser round-trip/error tests (test_parser.py)"
	@echo ""
	@echo "  Additional Tests:"
	@echo "  make test-rust-roundtrip - Organized roundtrip unit tests"
	@echo "  make test-rust-matrix    - Dialect matrix transpilation tests"
	@echo "  make test-rust-compat    - SQLGlot compatibility tests"
	@echo "  make test-rust-errors    - Error handling tests"
	@echo "  make test-rust-functions - Function normalization tests"
	@echo "  make test-rust-custom   - Custom dialect tests (DataFusion, etc.)"
	@echo "  make test-ffi           - Run C FFI crate tests"
	@echo ""
	@echo "  ClickHouse Tests:"
	@echo "  make test-rust-clickhouse-parser   - ClickHouse parser tests"
	@echo "  make test-rust-clickhouse-coverage - ClickHouse coverage tests (report-only)"
	@echo ""
	@echo "Full Comparison (slow, ~60s):"
	@echo "  make test-compare        - Run JS comparison tool (requires WASM build)"
	@echo ""
	@echo "Benchmarks:"
	@echo "  make bench-compare       - Compare polyglot-sql vs sqlglot performance"
	@echo "  make bench-rust          - Run Rust benchmarks (JSON output)"
	@echo "  make bench-python        - Run Python sqlglot benchmarks (JSON output)"
	@echo "  make bench-parse         - Parse benchmark (core-only: polyglot + sqlglot)"
	@echo "  make bench-parse-quick   - Parse benchmark fast mode (core-only + quick)"
	@echo "  make bench-parse-full    - Parse benchmark (all available parsers)"
	@echo ""
	@echo "Build:"
	@echo "  make generate-bindings   - Generate TypeScript bindings (ts-rs) and copy to SDK"
	@echo "  make copy-bindings       - Copy bindings from Rust crate to TypeScript SDK"
	@echo "  make build-wasm          - Build WASM package"
	@echo "  make build-ffi           - Build C FFI shared/static library"
	@echo "  make generate-ffi-header - Generate C header via cbindgen/build.rs"
	@echo "  make build-ffi-example   - Build and run C example"
	@echo "  make develop-python      - Build/install Python extension in uv-managed env"
	@echo "  make test-python         - Run Python bindings pytest suite"
	@echo "  make build-python        - Build Python wheels (maturin)"
	@echo "  make typecheck-python    - Type-check Python package stubs"
	@echo "  make python-docs-build   - Build Python API docs into packages/python-docs/dist"
	@echo "  make python-docs-deploy  - Deploy Python API docs to Cloudflare Pages"
	@echo "  make build-all           - Build everything"
	@echo "  make fmt                 - Format all code (Rust + TypeScript SDK)"
	@echo ""
	@echo "Playground:"
	@echo "  make playground-dev         - Run playground dev server"
	@echo "  make playground-build       - Build playground for production"
	@echo "  make playground-preview     - Preview production build"
	@echo "  make playground-deploy      - Deploy to Cloudflare Pages"
	@echo ""
	@echo "Release:"
	@echo "  make bump-version V=x.y.z - Bump version in all crates and packages"
	@echo ""
	@echo "Clean:"
	@echo "  make clean               - Remove all build artifacts"
	@echo "  make clean-fixtures      - Remove extracted sqlglot fixtures"
	@echo "  make clean-clickhouse-fixtures - Remove ClickHouse fixtures"
	@echo "  make clean-ffi           - Remove generated FFI header/example artifacts"
	@echo "  make clean-external      - Remove external project clones"

# =============================================================================
# External Project Setup
# =============================================================================

# Clone sqlglot repo at pinned tag
setup-sqlglot:
	@if [ ! -d external-projects/sqlglot/.git ]; then \
		echo "Cloning sqlglot at $(SQLGLOT_REF)..."; \
		mkdir -p external-projects; \
		git clone --depth=1 --branch $(SQLGLOT_REF) $(SQLGLOT_REPO) external-projects/sqlglot; \
		echo "sqlglot cloned."; \
	else \
		echo "sqlglot already present."; \
	fi

# Sparse clone ClickHouse test files
setup-clickhouse-tests:
	@if [ ! -d external-projects/clickhouse/.git ]; then \
		echo "Cloning ClickHouse tests (sparse, $(CLICKHOUSE_REF))..."; \
		mkdir -p external-projects/clickhouse; \
		cd external-projects/clickhouse && \
			git init && \
			git remote add origin $(CLICKHOUSE_REPO) && \
			git sparse-checkout init --cone && \
			git sparse-checkout set tests/queries/0_stateless && \
			git fetch --depth=1 origin $(CLICKHOUSE_REF) && \
			git checkout FETCH_HEAD; \
		echo "ClickHouse test files cloned."; \
	else \
		echo "ClickHouse test files already present."; \
	fi

# Clone all external repos
setup-external: setup-sqlglot setup-clickhouse-tests

# =============================================================================
# Fixture Extraction
# =============================================================================

# Extract sqlglot test fixtures directly to crate test dir
extract-fixtures: setup-sqlglot
	@echo "Extracting fixtures from sqlglot Python tests..."
	@uv run python3 tools/sqlglot-extract/extract-tests.py
	@echo "Done! Fixtures in crates/polyglot-sql/tests/sqlglot_fixtures/"

# Extract ClickHouse SQL tests into custom fixture JSON files
extract-clickhouse-fixtures: setup-clickhouse-tests
	@echo "Extracting ClickHouse test fixtures..."
	@uv run --with sqlglot python3 tools/clickhouse-extract/extract-clickhouse-tests.py
	@echo "Done! Fixtures in crates/polyglot-sql/tests/custom_fixtures/clickhouse/"

# Extract all fixtures (sqlglot + ClickHouse)
extract-all-fixtures: extract-fixtures extract-clickhouse-fixtures

# =============================================================================
# Rust Tests (Fast Iteration)
# =============================================================================

# Run all sqlglot compatibility tests
test-rust:
	cargo test -p polyglot-sql sqlglot -- --nocapture

# Run only generic identity tests (955 tests)
test-rust-identity:
	cargo test -p polyglot-sql sqlglot_identity -- --nocapture

# Run dialect-specific identity tests
test-rust-dialect:
	cargo test -p polyglot-sql sqlglot_dialect -- --nocapture

# Run transpilation tests
test-rust-transpile:
	RUST_MIN_STACK=16777216 cargo test -p polyglot-sql sqlglot_transpilation -- --nocapture

# Run pretty-printing tests (24 tests)
test-rust-pretty:
	cargo test -p polyglot-sql sqlglot_pretty -- --nocapture

# Run lib unit tests (736 tests)
test-rust-lib:
	cargo test --lib -p polyglot-sql

# Run all sqlglot fixture tests
test-rust-all:
	cargo test -p polyglot-sql --test sqlglot_identity --test sqlglot_dialect_identity \
		--test sqlglot_transpilation --test sqlglot_pretty \
		--test sqlglot_transpile --test sqlglot_parser -- --nocapture

# Run lib + fixture suites + custom dialects + clickhouse + FFI tests (full verification)
test-rust-verify:
	@echo "=== Lib unit tests ==="
	@cargo test --lib -p polyglot-sql
	@echo ""
	@echo "=== Generic identity tests ==="
	@cargo test --test sqlglot_identity test_sqlglot_identity_all -p polyglot-sql -- --nocapture
	@echo ""
	@echo "=== Dialect identity tests ==="
	@cargo test --test sqlglot_dialect_identity test_sqlglot_dialect_identity_all -p polyglot-sql -- --nocapture
	@echo ""
	@echo "=== Transpilation tests ==="
	@cargo test --test sqlglot_transpilation test_sqlglot_transpilation_all -p polyglot-sql -- --nocapture
	@echo ""
	@echo "=== Transpile generic tests ==="
	@cargo test --test sqlglot_transpile test_sqlglot_transpile_all -p polyglot-sql -- --nocapture
	@echo ""
	@echo "=== Parser tests ==="
	@cargo test --test sqlglot_parser test_sqlglot_parser_all -p polyglot-sql -- --nocapture
	@echo ""
	@echo "=== Pretty-print tests ==="
	@RUST_MIN_STACK=16777216 cargo test --test sqlglot_pretty test_sqlglot_pretty_all -p polyglot-sql --release -- --nocapture
	@echo ""
	@echo "=== Custom dialect tests ==="
	@cargo test --test custom_dialect_tests -p polyglot-sql -- --nocapture
	@echo ""
	@echo "=== ClickHouse parser tests ==="
	@RUST_MIN_STACK=16777216 cargo test --test custom_clickhouse_parser -p polyglot-sql --release -- --nocapture
	@echo ""
	@echo "=== ClickHouse coverage tests ==="
	@RUST_MIN_STACK=16777216 cargo test --test custom_clickhouse_coverage -p polyglot-sql --release -- --nocapture
	@echo ""
	@echo "=== FFI tests ==="
	@cargo test -p polyglot-sql-ffi -- --nocapture

# Run normalization/transpile tests from test_transpile.py
test-rust-transpile-generic:
	cargo test -p polyglot-sql --test sqlglot_transpile -- --nocapture

# Run parser round-trip/error tests from test_parser.py
test-rust-parser:
	cargo test -p polyglot-sql --test sqlglot_parser -- --nocapture

# -----------------------------------------------------------------------------
# Additional Rust Tests
# -----------------------------------------------------------------------------

# Run organized roundtrip unit tests (131 tests)
test-rust-roundtrip:
	cargo test -p polyglot-sql --test identity_roundtrip -- --nocapture

# Run dialect matrix transpilation tests
test-rust-matrix:
	cargo test -p polyglot-sql --test dialect_matrix -- --nocapture

# Run SQLGlot compatibility tests
test-rust-compat:
	cargo test -p polyglot-sql --test sqlglot_compat -- --nocapture

# Run error handling tests
test-rust-errors:
	cargo test -p polyglot-sql --test error_handling -- --nocapture

# Run function normalization tests
test-rust-functions:
	cargo test -p polyglot-sql --test test_function_normalizations -- --nocapture

# Run custom dialect tests (auto-discovers all dialects in custom_fixtures/)
test-rust-custom:
	cargo test -p polyglot-sql --test custom_dialect_tests -- --nocapture

# Quick check - just compile tests
test-rust-check:
	cargo check -p polyglot-sql --tests

# Run FFI crate tests
test-ffi:
	cargo test -p polyglot-sql-ffi -- --nocapture

# -----------------------------------------------------------------------------
# ClickHouse Tests
# -----------------------------------------------------------------------------

# Run ClickHouse parser tests
test-rust-clickhouse-parser:
	RUST_MIN_STACK=16777216 cargo test --test custom_clickhouse_parser -p polyglot-sql --release -- --nocapture

# Run ClickHouse coverage tests (report-only, failures expected)
test-rust-clickhouse-coverage:
	RUST_MIN_STACK=16777216 cargo test --test custom_clickhouse_coverage -p polyglot-sql --release -- --nocapture

# =============================================================================
# Full Comparison (Reference Implementation)
# =============================================================================

# Run full JS comparison tool (calls Python sqlglot)
test-compare: build-wasm
	cd tools/sqlglot-compare && npm run build && node dist/index.js compare

# =============================================================================
# Benchmarks (Performance Comparison)
# =============================================================================

# Compare polyglot-sql vs sqlglot performance
bench-compare:
	@uv run python3 tools/bench-compare/compare.py

# Run Rust benchmarks (JSON output)
bench-rust:
	@cargo run --example bench_json -p polyglot-sql --release

# Run Python sqlglot benchmarks (JSON output)
bench-python:
	@uv run --with sqlglot[c] python3 tools/bench-compare/bench_sqlglot.py

# Parse benchmark (core): polyglot-sql (Rust/PyO3) vs sqlglot (C tokenizer) via pyperf
bench-parse:
	@uv sync --project tools/bench-compare --reinstall-package polyglot-sql && \
		uv run --project tools/bench-compare python3 tools/bench-compare/bench_parse.py --quiet --core-only

# Parse benchmark (core/quick): faster but less stable timings
bench-parse-quick:
	@uv sync --project tools/bench-compare --reinstall-package polyglot-sql && \
		uv run --project tools/bench-compare python3 tools/bench-compare/bench_parse.py --quiet --core-only --quick

# Parse benchmark (full): include optional third-party parsers when available
bench-parse-full:
	@uv sync --project tools/bench-compare --reinstall-package polyglot-sql && \
		uv run --project tools/bench-compare python3 tools/bench-compare/bench_parse.py --quiet

# =============================================================================
# Build
# =============================================================================

# Generate TypeScript bindings (ts-rs) and copy to SDK
generate-bindings:
	@echo "Generating TypeScript bindings..."
	cargo test -p polyglot-sql --lib --features bindings export_typescript_types
	@echo "Bindings generated in crates/polyglot-sql/bindings/"
	@$(MAKE) copy-bindings

# Copy generated bindings from Rust crate to TypeScript SDK
copy-bindings:
	@echo "Copying bindings to packages/sdk/src/generated/..."
	@mkdir -p packages/sdk/src/generated
	@rm -rf packages/sdk/src/generated/*.ts
	@cp crates/polyglot-sql/bindings/*.ts packages/sdk/src/generated/
	@echo "Copied $$(ls packages/sdk/src/generated/*.ts | wc -l | tr -d ' ') type files."

# Build WASM package (full, all dialects)
build-wasm:
	cd crates/polyglot-sql-wasm && wasm-pack build --target bundler --release --out-dir ../../packages/sdk/wasm
	cd packages/sdk && npm run build

# Priority dialects for per-dialect WASM builds
PRIORITY_DIALECTS := postgresql mysql bigquery snowflake duckdb tsql clickhouse

# Build a single per-dialect WASM binary (usage: make build-wasm-dialect D=clickhouse)
build-wasm-dialect:
ifndef D
	$(error Usage: make build-wasm-dialect D=<dialect>)
endif
	cd crates/polyglot-sql-wasm && wasm-pack build --target bundler --release \
		--out-dir ../../packages/sdk/wasm/$(D) \
		-- --no-default-features --features "console_error_panic_hook,dialect-$(D)"

# Build all priority per-dialect WASM binaries
build-wasm-dialects:
	@for d in $(PRIORITY_DIALECTS); do \
		echo "Building WASM for dialect: $$d"; \
		$(MAKE) build-wasm-dialect D=$$d; \
	done

# Build everything (release-safe order)
build-all:
	@$(MAKE) cargo-build-release
	@$(MAKE) build-ffi
	@$(MAKE) build-python
	@$(MAKE) generate-bindings
	@$(MAKE) build-wasm

# Build core Rust crate in release mode
cargo-build-release:
	cargo build -p polyglot-sql --release

# Build C FFI shared/static libraries with unwind panic strategy
build-ffi:
	cargo build -p polyglot-sql-ffi --profile ffi_release

# Build C FFI static library (same build, staticlib is emitted with cdylib)
build-ffi-static:
	cargo build -p polyglot-sql-ffi --profile ffi_release

# Build Python extension in development mode (uv-managed)
develop-python:
	cd crates/polyglot-sql-python && uv sync --group dev && uv run maturin develop

# Run Python tests
test-python:
	cd crates/polyglot-sql-python && uv sync --group dev && uv run pytest

# Build Python wheels (release)
build-python:
	cd crates/polyglot-sql-python && uv sync --group dev && uv run maturin build --release

# Type-check Python package/stubs
typecheck-python:
	cd crates/polyglot-sql-python && uv sync --group dev && uv run pyright python/polyglot_sql/

# Generate C header via build.rs/cbindgen
generate-ffi-header:
	cargo build -p polyglot-sql-ffi --profile ffi_release
	@echo "Header generated at: crates/polyglot-sql-ffi/polyglot_sql.h"

# Build and run the C example
build-ffi-example: build-ffi
	cd examples/c && \
		cc -o polyglot_example main.c \
			-I../../crates/polyglot-sql-ffi \
			../../target/ffi_release/libpolyglot_sql_ffi.a && \
		./polyglot_example

# =============================================================================
# Development Workflow
# =============================================================================

# Format all code (Rust + TypeScript SDK)
fmt:
	cargo fmt --all
	cd packages/sdk && npm run format

# Quick development cycle: check + test
dev: test-rust-check test-rust

# Full validation before commit
validate: test-rust test-compare
	@echo "All tests passed!"

# =============================================================================
# Documentation
# =============================================================================

# Run documentation dev server
documentation-dev:
	cd packages/documentation && pnpm run dev

# Build documentation for production
documentation-build:
	cd packages/documentation && pnpm run build

# Preview production build
documentation-preview:
	cd packages/documentation && pnpm run preview

# Deploy to Cloudflare Pages
documentation-deploy: documentation-build
	cd packages/documentation && pnpm run deploy

# Build Python API docs to packages/python-docs/dist (overwrite mode)
python-docs-build:
	cd packages/python-docs && pnpm run build

# Preview Python API docs
python-docs-preview: python-docs-build
	cd packages/python-docs && pnpm run preview

# Deploy Python API docs to Cloudflare Pages
python-docs-deploy: python-docs-build
	cd packages/python-docs && pnpm run deploy

# =============================================================================
# Playground
# =============================================================================

# Run playground dev server
playground-dev:
	cd packages/playground && pnpm run dev

# Build playground for production
playground-build:
	cd packages/playground && pnpm run build

# Preview production build
playground-preview:
	cd packages/playground && pnpm run preview

# Deploy to Cloudflare Pages
playground-deploy: playground-build
	cd packages/playground && pnpm run deploy

# =============================================================================
# Release
# =============================================================================

# Bump version in all crates and packages (usage: make bump-version V=0.1.1)
bump-version:
ifndef V
	$(error Usage: make bump-version V=x.y.z)
endif
	@echo "Bumping version to $(V)..."
	cargo set-version $(V)
	pnpm -r exec pnpm version $(V) --no-git-tag-version
	@echo "Version bumped to $(V) in all crates and packages."

# =============================================================================
# Clean
# =============================================================================

# Remove extracted sqlglot fixtures
clean-fixtures:
	rm -rf crates/polyglot-sql/tests/sqlglot_fixtures

# Remove generated ClickHouse fixture files
clean-clickhouse-fixtures:
	rm -rf crates/polyglot-sql/tests/custom_fixtures/clickhouse

# Remove external project clones
clean-external:
	rm -rf external-projects/sqlglot
	rm -rf external-projects/clickhouse

# Remove all build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf crates/polyglot-sql-wasm/pkg
	rm -rf packages/sdk/dist
	rm -rf packages/sdk/node_modules
	rm -rf tools/sqlglot-compare/dist
	rm -rf tools/sqlglot-compare/node_modules
	rm -rf packages/playground/dist
	rm -rf packages/playground/node_modules
	@echo "Clean complete."

# Remove FFI generated artifacts (header and C example binary)
clean-ffi:
	rm -f crates/polyglot-sql-ffi/polyglot_sql.h
	rm -f examples/c/polyglot_example
