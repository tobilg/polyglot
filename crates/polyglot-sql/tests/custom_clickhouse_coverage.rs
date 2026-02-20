//! ClickHouse coverage test runner.
//!
//! Runs all ClickHouse identity tests extracted from the official ClickHouse test suite.
//! Reports pass/fail statistics without asserting — failures are expected and represent
//! areas for future parser/generator improvement.
//!
//! Run with: cargo test -p polyglot-sql --test clickhouse_coverage_tests -- --nocapture

mod common;

use common::test_data::CustomDialectFixtureFile;
use common::test_runner::{dialect_identity_test, parse_dialect};
use once_cell::sync::Lazy;
use std::fs;

const CLICKHOUSE_FIXTURES_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/custom_fixtures/clickhouse"
);

/// Load all JSON fixture files from the ClickHouse fixtures directory.
static CLICKHOUSE_FIXTURES: Lazy<Vec<CustomDialectFixtureFile>> = Lazy::new(|| {
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(CLICKHOUSE_FIXTURES_PATH) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "json") {
                match fs::read_to_string(&path) {
                    Ok(content) => {
                        match serde_json::from_str::<CustomDialectFixtureFile>(&content) {
                            Ok(fixture) => files.push(fixture),
                            Err(e) => {
                                eprintln!("  WARNING: Failed to parse {}: {}", path.display(), e)
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("  WARNING: Failed to read {}: {}", path.display(), e)
                    }
                }
            }
        }
    }
    files.sort_by(|a, b| a.category.cmp(&b.category));
    files
});

#[test]
fn test_clickhouse_coverage_identity() {
    let dialect_type = parse_dialect("clickhouse").expect("ClickHouse dialect must exist");

    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut category_stats: Vec<(String, usize, usize)> = Vec::new();

    for file in CLICKHOUSE_FIXTURES.iter() {
        let mut passed = 0;
        let mut failed = 0;

        for test in &file.identity {
            let expected = test.expected.as_deref();
            match dialect_identity_test(&test.sql, expected, dialect_type) {
                Ok(()) => passed += 1,
                Err(_) => failed += 1,
            }
        }

        total_passed += passed;
        total_failed += failed;
        category_stats.push((file.category.clone(), passed, passed + failed));
    }

    let total = total_passed + total_failed;
    let pass_rate = if total > 0 {
        (total_passed as f64 / total as f64) * 100.0
    } else {
        100.0
    };

    println!("\n=== ClickHouse Coverage Identity Tests ===");
    for (cat, passed, total) in &category_stats {
        let rate = if *total > 0 {
            (*passed as f64 / *total as f64) * 100.0
        } else {
            100.0
        };
        println!("  {}: {}/{} ({:.1}%)", cat, passed, total, rate);
    }
    println!(
        "\n  TOTAL: {}/{} passed ({:.1}%)",
        total_passed, total, pass_rate
    );

    // Report only — do not assert. Failures are expected and tracked for improvement.
}
