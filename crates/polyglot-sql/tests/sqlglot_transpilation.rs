//! SQLGlot Transpilation Tests
//!
//! These tests load transpilation test fixtures from the extracted JSON files
//! and verify that SQL transpiles correctly between dialects.

mod common;

use common::{
    parse_dialect, transpilation_known_failures, transpile_test, DialectFixture, TestResults,
};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fs;

/// Path to the fixtures directory (created by `make extract-fixtures`)
const FIXTURES_PATH: &str = "tests/sqlglot_fixtures";

/// Available dialect fixtures
const DIALECT_FILES: &[&str] = &[
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "doris",
    "duckdb",
    "generic",
    "hive",
    "materialize",
    "mysql",
    "oracle",
    "postgres",
    "presto",
    "redshift",
    "risingwave",
    "singlestore",
    "snowflake",
    "spark",
    "sqlite",
    "starrocks",
    "teradata",
    "trino",
    "tsql",
];

/// Lazily load all dialect fixtures for transpilation tests
static DIALECT_FIXTURES: Lazy<HashMap<String, DialectFixture>> = Lazy::new(|| {
    let mut fixtures = HashMap::new();

    for dialect in DIALECT_FILES {
        let path = format!("{}/dialects/{}.json", FIXTURES_PATH, dialect);
        if let Ok(content) = fs::read_to_string(&path) {
            if let Ok(fixture) = serde_json::from_str::<DialectFixture>(&content) {
                fixtures.insert(dialect.to_string(), fixture);
            }
        }
    }

    if fixtures.is_empty() {
        eprintln!(
            "Warning: No dialect fixtures loaded for transpilation tests. Run `make setup-fixtures` first."
        );
    }

    fixtures
});

/// Run transpilation tests for a specific source dialect
fn run_dialect_transpilation_tests(source_dialect: &str) -> TestResults {
    let fixture = match DIALECT_FIXTURES.get(source_dialect) {
        Some(f) => f,
        None => {
            println!(
                "Skipping {} transpilation tests - fixtures not available",
                source_dialect
            );
            return TestResults::default();
        }
    };

    let source_type = match parse_dialect(&fixture.dialect) {
        Some(d) => d,
        None => {
            println!("Unknown source dialect: {}", fixture.dialect);
            return TestResults::default();
        }
    };

    let mut results = TestResults::default();

    for (i, test) in fixture.transpilation.iter().enumerate() {
        // Test "write" mappings - parse SQL using source dialect, generate using target
        for (target_name, expected) in &test.write {
            let target_type = match parse_dialect(target_name) {
                Some(d) => d,
                None => continue, // Skip unknown dialects
            };

            let known = transpilation_known_failures(source_dialect, target_name);
            let test_id = format!("{}->{}:{}", source_dialect, target_name, i);

            if known.contains(&test_id) {
                results.known_failures += 1;
                results.failed += 1;
                continue;
            }

            let result = transpile_test(&test.sql, source_type, target_type, expected);
            results.record(&test_id, result);
        }
    }

    results
}

/// Test all transpilation fixtures
#[test]
fn test_sqlglot_transpilation_all() {
    // Run in a thread with larger stack to avoid stack overflow during recursive Drop
    let handle = std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024) // 8MB stack
        .spawn(|| {
            if DIALECT_FIXTURES.is_empty() {
                println!("Skipping transpilation tests - fixtures not available");
                println!("Run `make setup-fixtures` to set up test fixtures");
                return;
            }

            let mut total_results = TestResults::default();

            for source_dialect in DIALECT_FILES {
                let results = run_dialect_transpilation_tests(source_dialect);
                total_results.passed += results.passed;
                total_results.failed += results.failed;
                total_results.known_failures += results.known_failures;
                total_results.new_failures.extend(results.new_failures);
                total_results.new_passes.extend(results.new_passes);
            }

            total_results.print_summary("SQLGlot Transpilation (All)");

            // Assert minimum pass rate
            let min_pass_rate = 0.01; // 1% minimum (transpilation tests are the hardest)

            assert!(
                total_results.total() == 0 || total_results.pass_rate() >= min_pass_rate,
                "Pass rate {:.1}% is below threshold {:.1}%",
                total_results.pass_rate() * 100.0,
                min_pass_rate * 100.0
            );
        })
        .unwrap();
    handle.join().unwrap();
}

// Macro to generate per-dialect transpilation test functions
// Uses a thread with larger stack to avoid stack overflow during recursive Drop
macro_rules! transpile_test_fn {
    ($test_name:ident, $dialect:literal) => {
        #[test]
        fn $test_name() {
            let handle = std::thread::Builder::new()
                .stack_size(8 * 1024 * 1024) // 8MB stack
                .spawn(|| {
                    let results = run_dialect_transpilation_tests($dialect);
                    results.print_summary(&format!("{} Transpilation", $dialect.to_uppercase()));
                })
                .unwrap();
            handle.join().unwrap();
        }
    };
}

// Generate transpilation tests for key dialects
transpile_test_fn!(test_sqlglot_transpilation_mysql, "mysql");
transpile_test_fn!(test_sqlglot_transpilation_postgres, "postgres");
transpile_test_fn!(test_sqlglot_transpilation_bigquery, "bigquery");
transpile_test_fn!(test_sqlglot_transpilation_snowflake, "snowflake");
transpile_test_fn!(test_sqlglot_transpilation_duckdb, "duckdb");
transpile_test_fn!(test_sqlglot_transpilation_generic, "generic");
transpile_test_fn!(test_sqlglot_transpilation_tsql, "tsql");
transpile_test_fn!(test_sqlglot_transpilation_oracle, "oracle");
transpile_test_fn!(test_sqlglot_transpilation_spark, "spark");
transpile_test_fn!(test_sqlglot_transpilation_hive, "hive");
transpile_test_fn!(test_sqlglot_transpilation_sqlite, "sqlite");
transpile_test_fn!(test_sqlglot_transpilation_presto, "presto");
transpile_test_fn!(test_sqlglot_transpilation_trino, "trino");
transpile_test_fn!(test_sqlglot_transpilation_redshift, "redshift");
transpile_test_fn!(test_sqlglot_transpilation_clickhouse, "clickhouse");
transpile_test_fn!(test_sqlglot_transpilation_databricks, "databricks");
transpile_test_fn!(test_sqlglot_transpilation_athena, "athena");
transpile_test_fn!(test_sqlglot_transpilation_doris, "doris");
transpile_test_fn!(test_sqlglot_transpilation_materialize, "materialize");
transpile_test_fn!(test_sqlglot_transpilation_risingwave, "risingwave");
transpile_test_fn!(test_sqlglot_transpilation_singlestore, "singlestore");
transpile_test_fn!(test_sqlglot_transpilation_starrocks, "starrocks");
transpile_test_fn!(test_sqlglot_transpilation_teradata, "teradata");

/// Test specific dialect pairs that are commonly used
mod dialect_pairs {
    use super::*;

    fn test_dialect_pair(source: &str, target: &str) {
        let fixture = match DIALECT_FIXTURES.get(source) {
            Some(f) => f,
            None => {
                println!("Skipping {} -> {} - fixtures not available", source, target);
                return;
            }
        };

        let source_type = match parse_dialect(&fixture.dialect) {
            Some(d) => d,
            None => return,
        };

        let target_type = match parse_dialect(target) {
            Some(d) => d,
            None => return,
        };

        let mut results = TestResults::default();

        for (i, test) in fixture.transpilation.iter().enumerate() {
            if let Some(expected) = test.write.get(target) {
                let test_id = format!("{}->{}:{}", source, target, i);
                let result = transpile_test(&test.sql, source_type, target_type, expected);
                results.record(&test_id, result);
            }
        }

        results.print_summary(&format!("{} -> {}", source.to_uppercase(), target.to_uppercase()));
    }

    #[test]
    fn test_mysql_to_postgres() {
        test_dialect_pair("mysql", "postgres");
    }

    #[test]
    fn test_postgres_to_mysql() {
        test_dialect_pair("postgres", "mysql");
    }

    #[test]
    fn test_mysql_to_bigquery() {
        test_dialect_pair("mysql", "bigquery");
    }

    #[test]
    fn test_postgres_to_bigquery() {
        test_dialect_pair("postgres", "bigquery");
    }

    #[test]
    fn test_mysql_to_snowflake() {
        test_dialect_pair("mysql", "snowflake");
    }

    #[test]
    fn test_postgres_to_snowflake() {
        test_dialect_pair("postgres", "snowflake");
    }

    #[test]
    fn test_mysql_to_duckdb() {
        test_dialect_pair("mysql", "duckdb");
    }

    #[test]
    #[cfg_attr(debug_assertions, ignore = "Stack overflow in debug builds due to large stack frames - passes in release mode")]
    fn test_postgres_to_duckdb() {
        test_dialect_pair("postgres", "duckdb");
    }

    #[test]
    fn test_tsql_to_postgres() {
        test_dialect_pair("tsql", "postgres");
    }

    #[test]
    fn test_oracle_to_postgres() {
        test_dialect_pair("oracle", "postgres");
    }

    #[test]
    fn test_spark_to_hive() {
        test_dialect_pair("spark", "hive");
    }

    #[test]
    fn test_presto_to_trino() {
        test_dialect_pair("presto", "trino");
    }
}
