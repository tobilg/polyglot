//! SQLGlot Identity Tests
//!
//! These tests load identity test fixtures from the extracted JSON files
//! and verify that SQL roundtrips correctly (parse -> generate -> same SQL).

mod common;

use common::{identity_known_failures, identity_test, IdentityFixtures, TestResults};
use once_cell::sync::Lazy;
use std::fs;

/// Path to the fixtures directory (created by `make extract-fixtures`)
const FIXTURES_PATH: &str = "tests/sqlglot_fixtures";

/// Lazily load identity fixtures
static IDENTITY_FIXTURES: Lazy<Option<IdentityFixtures>> = Lazy::new(|| {
    let path = format!("{}/identity.json", FIXTURES_PATH);
    match fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str(&content) {
            Ok(fixtures) => Some(fixtures),
            Err(e) => {
                eprintln!("Warning: Failed to parse identity.json: {}", e);
                None
            }
        },
        Err(e) => {
            eprintln!(
                "Warning: Failed to read identity.json: {} (run `make setup-fixtures` first)",
                e
            );
            None
        }
    }
});

/// Test all identity fixtures
/// This test loads 954 identity tests from identity.json and runs them all,
/// tracking pass/fail rates.
///
/// Note: This test runs in a separate thread with a larger stack size (16MB)
/// to handle deeply nested SQL expressions without stack overflow.
#[test]
fn test_sqlglot_identity_all() {
    // Run in a thread with larger stack to avoid stack overflow on deeply nested expressions
    let result = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024) // 16MB stack
        .spawn(|| {
            let fixtures = match IDENTITY_FIXTURES.as_ref() {
                Some(f) => f,
                None => {
                    println!("Skipping identity tests - fixtures not available");
                    println!("Run `make setup-fixtures` to set up test fixtures");
                    return None;
                }
            };

            let known = identity_known_failures();
            let mut results = TestResults::with_known_failures(&known);

            for test in &fixtures.tests {
                let test_id = format!("identity:{}", test.line);
                let result = identity_test(&test.sql);
                results.record_with_sql(&test_id, &test.sql, test.line, result);
            }

            results.print_summary("SQLGlot Identity");
            Some(results.pass_rate())
        })
        .expect("Failed to spawn test thread")
        .join()
        .expect("Test thread panicked");

    if let Some(pass_rate) = result {
        assert!(
            pass_rate >= 1.0,
            "Pass rate {:.1}% — all identity tests must pass",
            pass_rate * 100.0
        );
    }
}

/// Test a subset of identity fixtures for quick verification
#[test]
fn test_sqlglot_identity_sample() {
    // Use a larger stack size for debug builds where the recursive descent parser
    // can consume significant stack space
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024) // 8MB stack
        .spawn(|| {
            let fixtures = match IDENTITY_FIXTURES.as_ref() {
                Some(f) => f,
                None => {
                    println!("Skipping identity sample tests - fixtures not available");
                    return;
                }
            };

            // Test first 100 fixtures as a quick check
            let sample_size = 100.min(fixtures.tests.len());
            let mut results = TestResults::default();

            for test in fixtures.tests.iter().take(sample_size) {
                let test_id = format!("identity:{}", test.line);
                let result = identity_test(&test.sql);
                results.record(&test_id, result);
            }

            results.print_summary("SQLGlot Identity (Sample)");
        })
        .expect("Failed to spawn test thread")
        .join()
        .expect("Test thread panicked");
}

/// Tests organized by complexity category
mod by_category {
    use super::*;

    /// Test simple expressions (numbers, strings, identifiers)
    #[test]
    fn test_identity_simple_expressions() {
        let test_cases = ["1", "(1)", "1.0", "'x'", "''", "a", "a.b"];

        let mut results = TestResults::default();

        for sql in &test_cases {
            let result = identity_test(sql);
            results.record(sql, result);
        }

        results.print_summary("Simple Expressions");
        assert!(
            results.pass_rate() >= 0.5,
            "Simple expressions should mostly pass"
        );
    }

    /// Test arithmetic operations
    #[test]
    fn test_identity_arithmetic() {
        let test_cases = ["1 + 2", "1 - 2", "1 * 2", "1 / 2", "(1 + 2) * 3"];

        let mut results = TestResults::default();

        for sql in &test_cases {
            let result = identity_test(sql);
            results.record(sql, result);
        }

        results.print_summary("Arithmetic Operations");
    }

    /// Test SELECT statements
    #[test]
    fn test_identity_select() {
        let test_cases = [
            "SELECT 1",
            "SELECT a",
            "SELECT a, b",
            "SELECT * FROM t",
            "SELECT a FROM t WHERE b = 1",
        ];

        let mut results = TestResults::default();

        for sql in &test_cases {
            let result = identity_test(sql);
            results.record(sql, result);
        }

        results.print_summary("SELECT Statements");
    }

    /// Test function calls
    #[test]
    fn test_identity_functions() {
        let test_cases = ["SUM(1)", "COUNT(*)", "MAX(a)", "MIN(b)", "COALESCE(a, b)"];

        let mut results = TestResults::default();

        for sql in &test_cases {
            let result = identity_test(sql);
            results.record(sql, result);
        }

        results.print_summary("Function Calls");
    }

    /// Debug test for specific SQL
    #[test]
    fn test_debug_sql() {
        let test_cases = [
            // Remaining failures from test suite:
            // Line 36: Triple-quoted string
            r#""""x""""#,
            // Line 204: @ identifier
            r#"@"x""#,
            // Line 448: Nested UNION
            "SELECT * FROM ((SELECT 1) UNION (SELECT 2) UNION (SELECT 3))",
            // Line 482: Complex nested UNION
            "SELECT * FROM (((SELECT 1) UNION SELECT 2) ORDER BY x LIMIT 1 OFFSET 1)",
            // Line 483: CROSS JOIN with parens
            "SELECT * FROM ((SELECT 1 AS x) CROSS JOIN (SELECT 2 AS y)) AS z",
            // Remaining failures
            "''''",                               // Line 19: Four single quotes
            "SELECT * FROM ((SELECT 1))",         // Line 440: Double-parenthesized subquery
            "SELECT * FROM ((SELECT 1) AS a(b))", // Line 447: Subquery alias with column aliases
            "SELECT a FROM test PIVOT(SUM(x) FOR y IN ('z', 'q')) UNPIVOT(x FOR y IN (z, q)) AS x", // Line 355: PIVOT...UNPIVOT with alias
            "SELECT * FROM ((SELECT 1) AS a UNION ALL (SELECT 2) AS b)", // Line 446: Aliased subqueries in UNION
        ];
        for sql in &test_cases {
            println!("\nInput:  {}", sql);
            let result = identity_test(sql);
            match result {
                Ok(_) => println!("✓ PASS"),
                Err(e) => println!("{}", e),
            }
        }
    }
}
