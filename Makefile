.PHONY: help extract-fixtures test-rust test-rust-all test-rust-identity test-rust-dialect \
        test-rust-transpile test-rust-pretty test-rust-roundtrip test-rust-matrix \
        test-rust-compat test-rust-errors test-rust-functions test-rust-lib test-rust-verify \
        test-compare build-wasm setup-fixtures clean-fixtures clean generate-bindings copy-bindings \
        bench-compare bench-rust bench-python \
        playground-dev playground-build playground-preview playground-deploy \
        bump-version

# Default target
help:
	@echo "Polyglot Development Commands"
	@echo "=============================="
	@echo ""
	@echo "Fixture Management:"
	@echo "  make extract-fixtures    - Extract tests from Python sqlglot to JSON"
	@echo "  make setup-fixtures      - Create symlink for Rust tests"
	@echo "  make clean-fixtures      - Remove extracted fixtures"
	@echo ""
	@echo "Rust Tests (fast):"
	@echo "  make test-rust           - Run all Rust tests"
	@echo "  make test-rust-all       - Run all sqlglot fixture tests"
	@echo "  make test-rust-lib       - Run lib unit tests (704)"
	@echo "  make test-rust-verify    - Run lib + identity + dialect + transpilation"
	@echo ""
	@echo "  SQLGlot Fixture Tests (8,455 tests):"
	@echo "  make test-rust-identity  - Generic identity tests (955)"
	@echo "  make test-rust-dialect   - Dialect identity tests (3,461)"
	@echo "  make test-rust-transpile - Transpilation tests (4,015)"
	@echo "  make test-rust-pretty    - Pretty-printing tests (24)"
	@echo ""
	@echo "  Additional Tests:"
	@echo "  make test-rust-roundtrip - Organized roundtrip unit tests"
	@echo "  make test-rust-matrix    - Dialect matrix transpilation tests"
	@echo "  make test-rust-compat    - SQLGlot compatibility tests"
	@echo "  make test-rust-errors    - Error handling tests"
	@echo "  make test-rust-functions - Function normalization tests"
	@echo ""
	@echo "Full Comparison (slow, ~60s):"
	@echo "  make test-compare        - Run JS comparison tool (requires WASM build)"
	@echo ""
	@echo "Benchmarks:"
	@echo "  make bench-compare       - Compare polyglot-sql vs sqlglot performance"
	@echo "  make bench-rust          - Run Rust benchmarks (JSON output)"
	@echo "  make bench-python        - Run Python sqlglot benchmarks (JSON output)"
	@echo ""
	@echo "Build:"
	@echo "  make generate-bindings   - Generate TypeScript bindings (ts-rs) and copy to SDK"
	@echo "  make copy-bindings       - Copy bindings from Rust crate to TypeScript SDK"
	@echo "  make build-wasm          - Build WASM package"
	@echo "  make build-all           - Build everything"
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
	@echo "  make clean-fixtures      - Remove extracted fixtures"

# =============================================================================
# Fixture Management
# =============================================================================

# Extract test fixtures from Python sqlglot test files
extract-fixtures:
	@echo "Extracting fixtures from sqlglot Python tests..."
	cd tools/sqlglot-compare && python3 scripts/extract-tests.py
	@echo "Done! Fixtures in tools/sqlglot-compare/fixtures/extracted/"

# Create symlink for Rust tests to access fixtures
setup-fixtures:
	@echo "Setting up fixture symlink for Rust tests..."
	@mkdir -p crates/polyglot-sql/tests
	@if [ ! -L crates/polyglot-sql/tests/fixtures ]; then \
		ln -s ../../../tools/sqlglot-compare/fixtures/extracted \
		      crates/polyglot-sql/tests/fixtures; \
		echo "Symlink created."; \
	else \
		echo "Symlink already exists."; \
	fi

# Remove extracted fixtures
clean-fixtures:
	rm -rf tools/sqlglot-compare/fixtures/extracted
	rm -f crates/polyglot-sql/tests/fixtures

# =============================================================================
# Rust Tests (Fast Iteration)
# =============================================================================

# Run all sqlglot compatibility tests
test-rust: setup-fixtures
	cargo test -p polyglot-sql sqlglot -- --nocapture

# Run only generic identity tests (954 tests)
test-rust-identity: setup-fixtures
	cargo test -p polyglot-sql sqlglot_identity -- --nocapture

# Run dialect-specific identity tests
test-rust-dialect: setup-fixtures
	cargo test -p polyglot-sql sqlglot_dialect -- --nocapture

# Run transpilation tests
test-rust-transpile: setup-fixtures
	cargo test -p polyglot-sql sqlglot_transpilation -- --nocapture

# Run pretty-printing tests (24 tests)
test-rust-pretty: setup-fixtures
	cargo test -p polyglot-sql sqlglot_pretty -- --nocapture

# Run lib unit tests (693 tests)
test-rust-lib:
	cargo test --lib -p polyglot-sql

# Run all sqlglot fixture tests
test-rust-all: setup-fixtures
	cargo test -p polyglot-sql --test sqlglot_identity --test sqlglot_dialect_identity \
		--test sqlglot_transpilation --test sqlglot_pretty -- --nocapture

# Run lib + identity + dialect identity + transpilation (full verification)
test-rust-verify: setup-fixtures
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

# Quick check - just compile tests
test-rust-check:
	cargo check -p polyglot-sql --tests

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
	@uv run --with sqlglot python3 tools/bench-compare/bench_sqlglot.py

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
	@if [ -f packages/sdk/src/generated/Index.ts ]; then \
		mv packages/sdk/src/generated/Index.ts packages/sdk/src/generated/index.ts; \
	fi
	@echo "Copied $$(ls packages/sdk/src/generated/*.ts | wc -l | tr -d ' ') type files."

# Build WASM package
build-wasm:
	cd crates/polyglot-sql-wasm && wasm-pack build --target web
	cd packages/sdk && npm run build

# Build everything
build-all: build-wasm
	cargo build -p polyglot-sql --release

# =============================================================================
# Development Workflow
# =============================================================================

# Quick development cycle: check + test
dev: test-rust-check test-rust

# Full validation before commit
validate: test-rust test-compare
	@echo "All tests passed!"

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
