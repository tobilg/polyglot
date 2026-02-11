//! SQLGlot Pretty-Print Tests
//!
//! These tests load pretty-print test fixtures from the extracted pretty.json file
//! and verify that SQL is formatted correctly with pretty printing.

mod common;

use common::{pretty_test, PrettyFixtures, TestResults};
use once_cell::sync::Lazy;
use std::fs;

/// Path to the fixtures directory (symlink created by `make setup-fixtures`)
const FIXTURES_PATH: &str = "tests/fixtures";

/// Lazily load pretty fixtures from extracted JSON
static PRETTY_FIXTURES: Lazy<Option<PrettyFixtures>> = Lazy::new(|| {
    let path = format!("{}/pretty.json", FIXTURES_PATH);
    match fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str(&content) {
            Ok(fixtures) => Some(fixtures),
            Err(e) => {
                eprintln!("Warning: Failed to parse pretty.json: {}", e);
                None
            }
        },
        Err(e) => {
            eprintln!(
                "Warning: Failed to read pretty.json: {} (run `make setup-fixtures` first)",
                e
            );
            None
        }
    }
});

/// Test all pretty-print fixtures
#[test]
#[cfg_attr(debug_assertions, ignore = "Stack overflow in debug builds due to large stack frames - passes in release mode")]
fn test_sqlglot_pretty_all() {
    let fixtures = match PRETTY_FIXTURES.as_ref() {
        Some(f) => f,
        None => {
            println!("Skipping pretty tests - fixtures not available");
            println!("Run `make extract-fixtures && make setup-fixtures` to set up test fixtures");
            return;
        }
    };

    println!("Found {} pretty test cases", fixtures.tests.len());

    let mut results = TestResults::default();

    for test in &fixtures.tests {
        let test_id = format!("pretty:{}", test.line);
        let result = pretty_test(&test.input, &test.expected);
        results.record_with_sql(&test_id, &test.input, test.line, result);
    }

    results.print_summary("SQLGlot Pretty-Print");

    // Start with a low threshold and increase as implementation improves
    let min_pass_rate = 0.05; // 5% minimum

    assert!(
        results.pass_rate() >= min_pass_rate,
        "Pass rate {:.1}% is below threshold {:.1}%",
        results.pass_rate() * 100.0,
        min_pass_rate * 100.0
    );
}

/// Test a sample of pretty-print fixtures for quick verification
#[test]
#[cfg_attr(debug_assertions, ignore = "Stack overflow in debug builds due to large stack frames - passes in release mode")]
fn test_sqlglot_pretty_sample() {
    let fixtures = match PRETTY_FIXTURES.as_ref() {
        Some(f) => f,
        None => {
            println!("Skipping pretty sample tests - fixtures not available");
            return;
        }
    };

    // Test first 10 fixtures as a quick check
    let sample_size = 10.min(fixtures.tests.len());
    let mut results = TestResults::default();

    for test in fixtures.tests.iter().take(sample_size) {
        let test_id = format!("pretty:{}", test.line);
        let result = pretty_test(&test.input, &test.expected);
        results.record(&test_id, result);
    }

    results.print_summary("SQLGlot Pretty-Print (Sample)");
}

/// Debug test for specific pretty-print SQL
#[test]
fn test_debug_pretty_sql() {
    use polyglot_sql::generator::Generator;
    use polyglot_sql::parser::Parser;

    let test_cases = [
        "SELECT * FROM test;",
        "WITH a AS ((SELECT 1 AS b) UNION ALL (SELECT 2 AS b)) SELECT * FROM a;",
        "SELECT * FROM t WHERE a = 1;",
    ];

    for input in &test_cases {
        println!("\n=== Input ===\n{}", input);
        match Parser::parse_sql(input) {
            Ok(stmts) if !stmts.is_empty() => {
                match Generator::pretty_sql(&stmts[0]) {
                    Ok(output) => println!("=== Output ===\n{}", output),
                    Err(e) => println!("Generate error: {}", e),
                }
            }
            Ok(_) => println!("No statements parsed"),
            Err(e) => println!("Parse error: {}", e),
        }
    }
}
