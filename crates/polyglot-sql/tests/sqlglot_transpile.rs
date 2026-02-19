//! SQLGlot Transpile Tests (from test_transpile.py)
//!
//! These tests load fixtures from transpile.json and verify:
//! - Normalization: parse generic SQL, generate generic, compare to expected
//! - Transpilation: parse generic SQL, generate with target dialect

mod common;

use common::{normalization_test, parse_dialect, transpile_test, TestResults, TranspileFixtures};
use once_cell::sync::Lazy;
use polyglot_sql::DialectType;
use std::fs;

/// Path to the fixtures directory (created by `make extract-fixtures`)
const FIXTURES_PATH: &str = "tests/sqlglot_fixtures";

/// Lazily load transpile fixtures
static TRANSPILE_FIXTURES: Lazy<Option<TranspileFixtures>> = Lazy::new(|| {
    let path = format!("{}/transpile.json", FIXTURES_PATH);
    let content = fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
});

/// Run normalization tests
fn run_normalization_tests() -> TestResults {
    let fixtures = match TRANSPILE_FIXTURES.as_ref() {
        Some(f) => f,
        None => {
            println!("Skipping normalization tests - fixtures not available");
            return TestResults::default();
        }
    };

    let mut results = TestResults::default();

    for (i, test) in fixtures.normalization.iter().enumerate() {
        let test_id = format!("normalization:{}", i);
        let result = normalization_test(&test.sql, &test.expected);
        results.record_with_sql(&test_id, &test.sql, test.line, result);
    }

    results
}

/// Run transpilation tests (write direction)
fn run_transpilation_tests() -> TestResults {
    let fixtures = match TRANSPILE_FIXTURES.as_ref() {
        Some(f) => f,
        None => {
            println!("Skipping transpilation tests - fixtures not available");
            return TestResults::default();
        }
    };

    let mut results = TestResults::default();

    for (i, test) in fixtures.transpilation.iter().enumerate() {
        if let Some(ref write_dialect) = test.write {
            let target_type = match parse_dialect(write_dialect) {
                Some(d) => d,
                None => continue,
            };

            let test_id = format!("transpile_write:{}:{}", write_dialect, i);
            let result =
                transpile_test(&test.sql, DialectType::Generic, target_type, &test.expected);
            results.record_with_sql(&test_id, &test.sql, test.line, result);
        } else if let Some(ref read_dialect) = test.read {
            let source_type = match parse_dialect(read_dialect) {
                Some(d) => d,
                None => continue,
            };

            let test_id = format!("transpile_read:{}:{}", read_dialect, i);
            let result =
                transpile_test(&test.sql, source_type, DialectType::Generic, &test.expected);
            results.record_with_sql(&test_id, &test.sql, test.line, result);
        }
    }

    results
}

/// Test all normalization fixtures
#[test]
fn test_sqlglot_transpile_normalization() {
    let handle = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let results = run_normalization_tests();
            results.print_summary("SQLGlot Transpile Normalization");
        })
        .unwrap();
    handle.join().unwrap();
}

/// Test all transpilation fixtures from test_transpile.py
#[test]
fn test_sqlglot_transpile_dialect() {
    let handle = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let results = run_transpilation_tests();
            results.print_summary("SQLGlot Transpile Dialect");
        })
        .unwrap();
    handle.join().unwrap();
}

/// Combined test for all transpile.py tests
#[test]
fn test_sqlglot_transpile_all() {
    let handle = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            if TRANSPILE_FIXTURES.is_none() {
                println!("Skipping transpile tests - fixtures not available");
                println!("Run `make setup-fixtures` to set up test fixtures");
                return;
            }

            let norm_results = run_normalization_tests();
            let trans_results = run_transpilation_tests();

            let mut total = TestResults::default();
            total.passed += norm_results.passed;
            total.failed += norm_results.failed;
            total.known_failures += norm_results.known_failures;
            total.new_failures.extend(norm_results.new_failures);
            total.new_passes.extend(norm_results.new_passes);

            total.passed += trans_results.passed;
            total.failed += trans_results.failed;
            total.known_failures += trans_results.known_failures;
            total.new_failures.extend(trans_results.new_failures);
            total.new_passes.extend(trans_results.new_passes);

            total.print_summary("SQLGlot Transpile (All)");
        })
        .unwrap();
    handle.join().unwrap();
}
