//! Auto-discovering test runner for custom dialect fixtures.
//!
//! This test runner auto-discovers all dialect subdirectories in `tests/custom_fixtures/`
//! and runs identity and transpilation tests for each. Adding a new custom dialect only
//! requires creating a new subdirectory with JSON fixture files.
//!
//! Run with: cargo test -p polyglot-sql --test custom_dialect_tests -- --nocapture

mod common;

use common::{
    dialect_identity_test, parse_dialect, transpile_test, AllCustomFixtures,
    CustomDialectFixtureFile, CustomDialectFixtures,
};
use once_cell::sync::Lazy;
use std::fs;
use std::path::Path;

const CUSTOM_FIXTURES_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/custom_fixtures");

/// Load all JSON fixture files from a dialect subdirectory.
fn load_dialect_fixtures(dir: &Path) -> Vec<CustomDialectFixtureFile> {
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "json") {
                match fs::read_to_string(&path) {
                    Ok(content) => match serde_json::from_str::<CustomDialectFixtureFile>(&content)
                    {
                        Ok(fixture) => files.push(fixture),
                        Err(e) => eprintln!(
                            "  WARNING: Failed to parse {}: {}",
                            path.display(),
                            e
                        ),
                    },
                    Err(e) => {
                        eprintln!("  WARNING: Failed to read {}: {}", path.display(), e)
                    }
                }
            }
        }
    }
    // Sort by category for deterministic output
    files.sort_by(|a, b| a.category.cmp(&b.category));
    files
}

/// Auto-discover all dialect subdirectories and load their fixtures.
static ALL_CUSTOM_FIXTURES: Lazy<AllCustomFixtures> = Lazy::new(|| {
    let mut dialects = Vec::new();
    if let Ok(entries) = fs::read_dir(CUSTOM_FIXTURES_PATH) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                let dialect_name = entry.file_name().to_string_lossy().to_string();
                let files = load_dialect_fixtures(&entry.path());
                if !files.is_empty() {
                    dialects.push(CustomDialectFixtures {
                        dialect: dialect_name,
                        files,
                    });
                }
            }
        }
    }
    dialects.sort_by(|a, b| a.dialect.cmp(&b.dialect));
    AllCustomFixtures { dialects }
});

/// Collect and run all identity tests for a dialect, returning (passed, failed, total, failures).
fn run_identity_tests(
    fixtures: &CustomDialectFixtures,
) -> (usize, usize, usize, Vec<String>) {
    let dialect_type = match parse_dialect(&fixtures.dialect) {
        Some(dt) => dt,
        None => {
            let msg = format!("Unknown dialect: {}", fixtures.dialect);
            return (0, 1, 1, vec![msg]);
        }
    };

    let mut passed = 0;
    let mut failed = 0;
    let mut failures = Vec::new();

    for file in &fixtures.files {
        for (i, test) in file.identity.iter().enumerate() {
            let expected = test.expected.as_deref();
            match dialect_identity_test(&test.sql, expected, dialect_type) {
                Ok(()) => passed += 1,
                Err(e) => {
                    failed += 1;
                    let desc = if test.description.is_empty() {
                        format!("[{}:{}]", file.category, i)
                    } else {
                        format!("[{}:{}] {}", file.category, i, test.description)
                    };
                    failures.push(format!("  FAIL {}: {}", desc, e));
                }
            }
        }
    }

    (passed, failed, passed + failed, failures)
}

/// Collect and run all transpilation tests for a dialect.
/// Supports sqlglot-compatible `write` (forward) and `read` (reverse) maps.
fn run_transpilation_tests(
    fixtures: &CustomDialectFixtures,
) -> (usize, usize, usize, Vec<String>) {
    let file_dialect = match parse_dialect(&fixtures.dialect) {
        Some(dt) => dt,
        None => {
            let msg = format!("Unknown source dialect: {}", fixtures.dialect);
            return (0, 1, 1, vec![msg]);
        }
    };

    let mut passed = 0;
    let mut failed = 0;
    let mut failures = Vec::new();

    for file in &fixtures.files {
        for (i, test) in file.transpilation.iter().enumerate() {
            let desc_prefix = if test.description.is_empty() {
                format!("[{}:{}]", file.category, i)
            } else {
                format!("[{}:{}] {}", file.category, i, test.description)
            };

            // Forward: parse as file dialect, generate as each target
            for (target_name, expected) in &test.write {
                let target_dialect = match parse_dialect(target_name) {
                    Some(dt) => dt,
                    None => {
                        failed += 1;
                        failures.push(format!(
                            "  FAIL {} (write→{}): Unknown target dialect",
                            desc_prefix, target_name
                        ));
                        continue;
                    }
                };

                match transpile_test(&test.sql, file_dialect, target_dialect, expected) {
                    Ok(()) => passed += 1,
                    Err(e) => {
                        failed += 1;
                        failures.push(format!(
                            "  FAIL {} (write→{}): {}",
                            desc_prefix, target_name, e
                        ));
                    }
                }
            }

            // Reverse: parse as source dialect, generate as file dialect
            for (source_name, source_sql) in &test.read {
                let source_dialect = match parse_dialect(source_name) {
                    Some(dt) => dt,
                    None => {
                        failed += 1;
                        failures.push(format!(
                            "  FAIL {} (read←{}): Unknown source dialect",
                            desc_prefix, source_name
                        ));
                        continue;
                    }
                };

                match transpile_test(source_sql, source_dialect, file_dialect, &test.sql) {
                    Ok(()) => passed += 1,
                    Err(e) => {
                        failed += 1;
                        failures.push(format!(
                            "  FAIL {} (read←{}): {}",
                            desc_prefix, source_name, e
                        ));
                    }
                }
            }
        }
    }

    (passed, failed, passed + failed, failures)
}

#[test]
fn test_custom_dialect_identity_all() {
    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut total_tests = 0;

    for dialect_fixtures in &ALL_CUSTOM_FIXTURES.dialects {
        let (passed, failed, total, failures) = run_identity_tests(dialect_fixtures);
        total_passed += passed;
        total_failed += failed;
        total_tests += total;

        let pass_rate = if total > 0 {
            (passed as f64 / total as f64) * 100.0
        } else {
            100.0
        };

        println!(
            "\n=== {} Identity Tests: {}/{} passed ({:.1}%) ===",
            dialect_fixtures.dialect, passed, total, pass_rate
        );
        for f in &failures {
            println!("{}", f);
        }
    }

    if total_tests > 0 {
        let overall_rate = (total_passed as f64 / total_tests as f64) * 100.0;
        println!(
            "\n=== Custom Dialect Identity Summary: {}/{} passed ({:.1}%) ===",
            total_passed, total_tests, overall_rate
        );
        assert!(
            total_failed == 0,
            "{} identity test(s) failed out of {}",
            total_failed,
            total_tests
        );
    } else {
        println!("\nNo custom dialect identity tests found.");
    }
}

#[test]
fn test_custom_dialect_transpilation_all() {
    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut total_tests = 0;

    for dialect_fixtures in &ALL_CUSTOM_FIXTURES.dialects {
        let (passed, failed, total, failures) = run_transpilation_tests(dialect_fixtures);
        total_passed += passed;
        total_failed += failed;
        total_tests += total;

        let pass_rate = if total > 0 {
            (passed as f64 / total as f64) * 100.0
        } else {
            100.0
        };

        println!(
            "\n=== {} Transpilation Tests: {}/{} passed ({:.1}%) ===",
            dialect_fixtures.dialect, passed, total, pass_rate
        );
        for f in &failures {
            println!("{}", f);
        }
    }

    if total_tests > 0 {
        let overall_rate = (total_passed as f64 / total_tests as f64) * 100.0;
        println!(
            "\n=== Custom Dialect Transpilation Summary: {}/{} passed ({:.1}%) ===",
            total_passed, total_tests, overall_rate
        );
        assert!(
            total_failed == 0,
            "{} transpilation test(s) failed out of {}",
            total_failed,
            total_tests
        );
    } else {
        println!("\nNo custom dialect transpilation tests found.");
    }
}
