//! SQLGlot Pretty-Print Tests
//!
//! These tests load pretty-print test fixtures from the extracted pretty.json file
//! and verify that SQL is formatted correctly with pretty printing.

mod common;

use common::{pretty_test, PrettyFixtures, TestResults};
use once_cell::sync::Lazy;
use std::fs;

/// Path to the fixtures directory (created by `make extract-fixtures`)
const FIXTURES_PATH: &str = "tests/sqlglot_fixtures";

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
fn test_sqlglot_pretty_all() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let fixtures = match PRETTY_FIXTURES.as_ref() {
                Some(f) => f,
                None => {
                    println!("Skipping pretty tests - fixtures not available");
                    println!(
                        "Run `make extract-fixtures && make setup-fixtures` to set up test fixtures"
                    );
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

            let min_pass_rate = 1.0; // 100% required

            assert!(
                results.pass_rate() >= min_pass_rate,
                "Pass rate {:.1}% is below threshold {:.1}%",
                results.pass_rate() * 100.0,
                min_pass_rate * 100.0
            );
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Test a sample of pretty-print fixtures for quick verification
#[test]
fn test_sqlglot_pretty_sample() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
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
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Debug test for the 3 remaining failing pretty-print cases
#[test]
fn test_debug_pretty_sql() {
    use polyglot_sql::generator::Generator;
    use polyglot_sql::parser::Parser;

    let cases: Vec<(&str, &str, usize)> = vec![
        (
            "SELECT\n    id,\n    -- SUM(total) as all_that,\n    ARRAY_AGG(foo)[0][0] AS first_foo,\nFROM facts\nGROUP BY all;",
            "SELECT\n  id,\n  ARRAY_AGG(foo)[0][0] AS first_foo /* SUM(total) as all_that, */\nFROM facts\nGROUP BY ALL;",
            441,
        ),
        (
            "SELECT\n    *\nFROM\n    a\nWHERE\n    /*111*/\n    b = 1\n    /*222*/\nORDER BY\n    c;",
            "SELECT\n  *\nFROM a\nWHERE\n  b /* 111 */ = 1\n/* 222 */\nORDER BY\n  c;",
            457,
        ),
        (
            "SELECT 1\nFROM foo\nWHERE 1=1\nAND -- first comment\n    -- second comment\n    foo.a = 1;",
            "SELECT\n  1\nFROM foo\nWHERE\n  1 = 1 AND /* first comment */ foo.a /* second comment */ = 1;",
            423,
        ),
        (
            "SELECT *\nFROM foo\nwHERE 1=1\n    AND\n        -- my comment\n        EXISTS (\n            SELECT 1\n            FROM bar\n        );",
            "SELECT\n  *\nFROM foo\nWHERE\n  1 = 1 AND EXISTS(\n    SELECT\n      1\n    FROM bar\n  ) /* my comment */;",
            405,
        ),
    ];

    for (input, expected, line) in &cases {
        let stmts =
            Parser::parse_sql(input).unwrap_or_else(|e| panic!("line {} parse error: {}", line, e));
        assert!(!stmts.is_empty(), "line {} parsed no statements", line);

        let output = Generator::pretty_sql(&stmts[0])
            .unwrap_or_else(|e| panic!("line {} generate error: {}", line, e));

        assert_eq!(output, *expected, "line {} pretty output mismatch", line);
    }
}
