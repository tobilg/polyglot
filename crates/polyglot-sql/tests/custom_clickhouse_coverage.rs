//! ClickHouse coverage test runner.
//!
//! Runs all ClickHouse tests extracted from the official ClickHouse test suite
//! through a normalized round-trip:
//! parse ClickHouse SQL -> transform/generate normalized ClickHouse SQL ->
//! parse generated SQL -> transform/generate again.
//!
//! Run with: cargo test -p polyglot-sql --test clickhouse_coverage_tests -- --nocapture

mod common;

use common::test_data::CustomDialectFixtureFile;
use once_cell::sync::Lazy;
use polyglot_sql::dialects::{Dialect, DialectType};
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

fn normalized_clickhouse_sql(sql: &str, dialect: &Dialect) -> Result<String, String> {
    let statements = dialect
        .parse(sql)
        .map_err(|e| format!("Parse error: {e}"))?;

    let mut outputs = Vec::with_capacity(statements.len());
    for statement in statements {
        let transformed = dialect
            .transform(statement)
            .map_err(|e| format!("Transform error: {e}"))?;
        outputs.push(
            dialect
                .generate_with_source(&transformed, DialectType::ClickHouse)
                .map_err(|e| format!("Generate error: {e}"))?,
        );
    }

    Ok(outputs.join("; "))
}

fn normalized_roundtrip_test(sql: &str) -> Result<(), String> {
    let dialect = Dialect::get(DialectType::ClickHouse);
    let first = normalized_clickhouse_sql(sql, &dialect)?;
    let second = normalized_clickhouse_sql(&first, &dialect)
        .map_err(|e| format!("{e}\n  generated: {first}"))?;

    if first != second {
        return Err(format!(
            "Normalized output is unstable:\n  input:     {sql}\n  first:     {first}\n  second:    {second}"
        ));
    }

    Ok(())
}

fn is_out_of_scope_clickhouse_fixture(category: &str, sql: &str) -> bool {
    if !matches!(category, "other_01" | "other_02") {
        return false;
    }

    let trimmed = sql.trim();
    trimmed.starts_with("q=75 ") || trimmed.starts_with(". number%7 ") || trimmed.contains(" | ")
}

#[test]
fn test_clickhouse_normalized_roundtrip_coverage() {
    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut total_skipped = 0;
    let mut category_stats: Vec<(String, usize, usize)> = Vec::new();

    for file in CLICKHOUSE_FIXTURES.iter() {
        let mut passed = 0;
        let mut failed = 0;
        let mut skipped = 0;

        for test in &file.identity {
            if is_out_of_scope_clickhouse_fixture(&file.category, &test.sql) {
                skipped += 1;
                continue;
            }

            match normalized_roundtrip_test(&test.sql) {
                Ok(()) => passed += 1,
                Err(error) => {
                    failed += 1;
                    println!(
                        "FAIL [{}] {}\n  {}\n{}",
                        file.category, test.description, test.sql, error
                    );
                }
            }
        }

        total_passed += passed;
        total_failed += failed;
        total_skipped += skipped;
        category_stats.push((file.category.clone(), passed, passed + failed));
    }

    let total = total_passed + total_failed;
    let pass_rate = if total > 0 {
        (total_passed as f64 / total as f64) * 100.0
    } else {
        100.0
    };

    println!("\n=== ClickHouse Normalized Round-Trip Coverage ===");
    for (cat, passed, total) in &category_stats {
        let rate = if *total > 0 {
            (*passed as f64 / *total as f64) * 100.0
        } else {
            100.0
        };
        println!("  {}: {}/{} ({:.1}%)", cat, passed, total, rate);
    }
    println!(
        "\n  TOTAL: {}/{} normalized round-trips passed ({:.1}%)",
        total_passed, total, pass_rate
    );
    println!("  Skipped out-of-scope KQL/non-SQL fixtures: {total_skipped}");

    assert_eq!(
        total_failed, 0,
        "ClickHouse normalized round-trip coverage has {total_failed} failing cases"
    );
}

#[test]
#[ignore = "diagnostic helper for grouping ClickHouse coverage failures"]
fn debug_clickhouse_coverage_failures() {
    let category_filter = std::env::var("CLICKHOUSE_COVERAGE_CATEGORY").ok();
    let mut printed = 0;

    for file in CLICKHOUSE_FIXTURES.iter() {
        if category_filter
            .as_deref()
            .is_some_and(|category| category != file.category)
        {
            continue;
        }

        let mut category_printed = 0;
        for test in &file.identity {
            if is_out_of_scope_clickhouse_fixture(&file.category, &test.sql) {
                continue;
            }

            if let Err(err) = normalized_roundtrip_test(&test.sql) {
                if category_printed == 0 {
                    println!("\n=== {} ===", file.category);
                }
                println!("SQL: {}", test.sql);
                println!("ERR: {}\n", err);
                category_printed += 1;
                printed += 1;

                if category_printed >= 5 || printed >= 120 {
                    break;
                }
            }
        }

        if printed >= 120 {
            break;
        }
    }
}
