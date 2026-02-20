//! SQLGlot Parser Tests (from test_parser.py)
//!
//! These tests load fixtures from parser.json and verify:
//! - Round-trips: parse SQL, generate, compare to expected
//! - Errors: SQL that should fail to parse

mod common;

use common::{
    normalization_test, parse_dialect, parser_error_test, transpile_test, ParserFixtures,
    TestResults,
};
use once_cell::sync::Lazy;
use polyglot_sql::DialectType;
use std::fs;

/// Path to the fixtures directory (created by `make extract-fixtures`)
const FIXTURES_PATH: &str = "tests/sqlglot_fixtures";

/// Lazily load parser fixtures
static PARSER_FIXTURES: Lazy<Option<ParserFixtures>> = Lazy::new(|| {
    let path = format!("{}/parser.json", FIXTURES_PATH);
    let content = fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
});

/// Run round-trip tests
fn run_roundtrip_tests() -> TestResults {
    let fixtures = match PARSER_FIXTURES.as_ref() {
        Some(f) => f,
        None => {
            println!("Skipping parser roundtrip tests - fixtures not available");
            return TestResults::default();
        }
    };

    let mut results = TestResults::default();

    for (i, test) in fixtures.roundtrips.iter().enumerate() {
        let test_id = format!("parser_roundtrip:{}", i);

        let result = if test.read.is_some() || test.write.is_some() {
            // Dialect round-trip: parse with read dialect, generate with write dialect
            let read_dialect = test
                .read
                .as_deref()
                .and_then(parse_dialect)
                .unwrap_or(DialectType::Generic);
            let write_dialect = test
                .write
                .as_deref()
                .and_then(parse_dialect)
                .unwrap_or(DialectType::Generic);

            transpile_test(&test.sql, read_dialect, write_dialect, &test.expected)
        } else {
            // Generic round-trip
            normalization_test(&test.sql, &test.expected)
        };

        results.record_with_sql(&test_id, &test.sql, test.line, result);
    }

    results
}

/// Run error tests
fn run_error_tests() -> TestResults {
    let fixtures = match PARSER_FIXTURES.as_ref() {
        Some(f) => f,
        None => {
            println!("Skipping parser error tests - fixtures not available");
            return TestResults::default();
        }
    };

    let mut results = TestResults::default();

    for (i, test) in fixtures.errors.iter().enumerate() {
        let test_id = format!("parser_error:{}", i);
        let dialect = test.read.as_deref().and_then(parse_dialect);
        let result = parser_error_test(&test.sql, dialect);
        results.record_with_sql(&test_id, &test.sql, test.line, result);
    }

    results
}

/// Test parser round-trips
#[test]
fn test_sqlglot_parser_roundtrips() {
    let handle = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let results = run_roundtrip_tests();
            results.print_summary("SQLGlot Parser Roundtrips");
        })
        .unwrap();
    handle.join().unwrap();
}

/// Test parser errors
#[test]
fn test_sqlglot_parser_errors() {
    let handle = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let results = run_error_tests();
            results.print_summary("SQLGlot Parser Errors");
        })
        .unwrap();
    handle.join().unwrap();
}

/// Combined test for all parser tests
#[test]
fn test_sqlglot_parser_all() {
    let handle = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            if PARSER_FIXTURES.is_none() {
                println!("Skipping parser tests - fixtures not available");
                println!("Run `make setup-fixtures` to set up test fixtures");
                return;
            }

            let rt_results = run_roundtrip_tests();
            let err_results = run_error_tests();

            let mut total = TestResults::default();
            total.passed += rt_results.passed;
            total.failed += rt_results.failed;
            total.known_failures += rt_results.known_failures;
            total.new_failures.extend(rt_results.new_failures);
            total.new_passes.extend(rt_results.new_passes);

            total.passed += err_results.passed;
            total.failed += err_results.failed;
            total.known_failures += err_results.known_failures;
            total.new_failures.extend(err_results.new_failures);
            total.new_passes.extend(err_results.new_passes);

            total.print_summary("SQLGlot Parser (All)");

            assert!(
                total.total() == 0 || total.pass_rate() >= 1.0,
                "Pass rate {:.1}% — all parser tests must pass",
                total.pass_rate() * 100.0
            );
        })
        .unwrap();
    handle.join().unwrap();
}
