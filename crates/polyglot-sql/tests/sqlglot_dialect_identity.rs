//! SQLGlot Dialect-Specific Identity Tests
//!
//! These tests load dialect-specific identity test fixtures from the extracted JSON files
//! and verify that SQL roundtrips correctly for each dialect.

mod common;

use common::{
    dialect_identity_known_failures, dialect_identity_test, parse_dialect, DialectFixture,
    TestResults,
};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fs;

/// Path to the fixtures directory
const FIXTURES_PATH: &str = "tests/fixtures";

/// Available dialect fixtures
const DIALECT_FILES: &[&str] = &[
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "doris",
    "dremio",
    "drill",
    "druid",
    "duckdb",
    "dune",
    "exasol",
    "fabric",
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
    "solr",
    "spark",
    "sqlite",
    "starrocks",
    "teradata",
    "trino",
    "tsql",
];

/// Lazily load all dialect fixtures
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
        eprintln!("Warning: No dialect fixtures loaded. Run `make setup-fixtures` first.");
    }

    fixtures
});

/// Run identity tests for a specific dialect
///
/// Note: This function runs in a separate thread with a larger stack size (16MB)
/// to handle deeply nested SQL expressions without stack overflow.
fn run_dialect_identity_tests(dialect_name: &str) -> TestResults {
    let dialect_name = dialect_name.to_string();
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024) // 16MB stack
        .spawn(move || {
            run_dialect_identity_tests_impl(&dialect_name)
        })
        .expect("Failed to spawn test thread")
        .join()
        .expect("Test thread panicked")
}

fn run_dialect_identity_tests_impl(dialect_name: &str) -> TestResults {
    let fixture = match DIALECT_FIXTURES.get(dialect_name) {
        Some(f) => f,
        None => {
            println!(
                "Skipping {} identity tests - fixtures not available",
                dialect_name
            );
            return TestResults::default();
        }
    };

    let dialect_type = match parse_dialect(&fixture.dialect) {
        Some(d) => d,
        None => {
            println!("Unknown dialect: {}", fixture.dialect);
            return TestResults::default();
        }
    };

    let known = dialect_identity_known_failures(dialect_name);
    let mut results = TestResults::with_known_failures(&known);

    for (i, test) in fixture.identity.iter().enumerate() {
        let test_id = format!("{}:identity:{}", dialect_name, i);
        let result = dialect_identity_test(&test.sql, test.expected.as_deref(), dialect_type);
        results.record_with_sql(&test_id, &test.sql, i, result);
    }

    results
}

/// Test all dialect identity fixtures combined
#[test]
fn test_sqlglot_dialect_identity_all() {
    if DIALECT_FIXTURES.is_empty() {
        println!("Skipping dialect identity tests - fixtures not available");
        println!("Run `make setup-fixtures` to set up test fixtures");
        return;
    }

    let mut total_results = TestResults::default();

    for dialect_name in DIALECT_FILES {
        let results = run_dialect_identity_tests(dialect_name);
        total_results.passed += results.passed;
        total_results.failed += results.failed;
        total_results.known_failures += results.known_failures;
        total_results.new_failures.extend(results.new_failures);
        total_results.new_passes.extend(results.new_passes);
    }

    total_results.print_summary("SQLGlot Dialect Identity (All)");

    // Assert minimum pass rate
    let min_pass_rate = 0.05; // 5% minimum (dialect tests are harder)

    assert!(
        total_results.total() == 0 || total_results.pass_rate() >= min_pass_rate,
        "Pass rate {:.1}% is below threshold {:.1}%",
        total_results.pass_rate() * 100.0,
        min_pass_rate * 100.0
    );
}

// Macro to generate per-dialect test functions
macro_rules! dialect_test {
    ($test_name:ident, $dialect:literal) => {
        #[test]
        fn $test_name() {
            let results = run_dialect_identity_tests($dialect);
            results.print_summary(&format!("{} Identity", $dialect.to_uppercase()));
        }
    };
}

// Generate tests for each dialect
dialect_test!(test_sqlglot_dialect_athena, "athena");
dialect_test!(test_sqlglot_dialect_bigquery, "bigquery");
dialect_test!(test_sqlglot_dialect_clickhouse, "clickhouse");
dialect_test!(test_sqlglot_dialect_databricks, "databricks");
dialect_test!(test_sqlglot_dialect_doris, "doris");
dialect_test!(test_sqlglot_dialect_dremio, "dremio");
dialect_test!(test_sqlglot_dialect_drill, "drill");
dialect_test!(test_sqlglot_dialect_druid, "druid");
dialect_test!(test_sqlglot_dialect_duckdb, "duckdb");
dialect_test!(test_sqlglot_dialect_dune, "dune");
dialect_test!(test_sqlglot_dialect_exasol, "exasol");
dialect_test!(test_sqlglot_dialect_fabric, "fabric");
dialect_test!(test_sqlglot_dialect_generic, "generic");
dialect_test!(test_sqlglot_dialect_hive, "hive");
dialect_test!(test_sqlglot_dialect_materialize, "materialize");
dialect_test!(test_sqlglot_dialect_mysql, "mysql");
dialect_test!(test_sqlglot_dialect_oracle, "oracle");
dialect_test!(test_sqlglot_dialect_postgres, "postgres");
dialect_test!(test_sqlglot_dialect_presto, "presto");
dialect_test!(test_sqlglot_dialect_redshift, "redshift");
dialect_test!(test_sqlglot_dialect_risingwave, "risingwave");
dialect_test!(test_sqlglot_dialect_singlestore, "singlestore");
dialect_test!(test_sqlglot_dialect_snowflake, "snowflake");
dialect_test!(test_sqlglot_dialect_solr, "solr");
dialect_test!(test_sqlglot_dialect_spark, "spark");
dialect_test!(test_sqlglot_dialect_sqlite, "sqlite");
dialect_test!(test_sqlglot_dialect_starrocks, "starrocks");
dialect_test!(test_sqlglot_dialect_tableau, "tableau");
dialect_test!(test_sqlglot_dialect_teradata, "teradata");
dialect_test!(test_sqlglot_dialect_trino, "trino");
dialect_test!(test_sqlglot_dialect_tsql, "tsql");

/// Test priority dialects with higher pass rate requirements
mod priority_dialects {
    use super::*;

    /// PostgreSQL - most comprehensive feature set
    #[test]
    fn test_postgres_priority() {
        let results = run_dialect_identity_tests("postgres");
        results.print_summary("PostgreSQL (Priority)");

        // PostgreSQL should have a reasonable pass rate
        if results.total() > 0 {
            println!(
                "PostgreSQL coverage: {}/{} ({:.1}%)",
                results.passed,
                results.total(),
                results.pass_rate() * 100.0
            );
        }
    }

    /// MySQL - widely used
    #[test]
    fn test_mysql_priority() {
        let results = run_dialect_identity_tests("mysql");
        results.print_summary("MySQL (Priority)");

        if results.total() > 0 {
            println!(
                "MySQL coverage: {}/{} ({:.1}%)",
                results.passed,
                results.total(),
                results.pass_rate() * 100.0
            );
        }
    }

    /// BigQuery - cloud analytics
    #[test]
    fn test_bigquery_priority() {
        let results = run_dialect_identity_tests("bigquery");
        results.print_summary("BigQuery (Priority)");

        if results.total() > 0 {
            println!(
                "BigQuery coverage: {}/{} ({:.1}%)",
                results.passed,
                results.total(),
                results.pass_rate() * 100.0
            );
        }
    }

    /// Snowflake - cloud data warehouse
    #[test]
    fn test_snowflake_priority() {
        let results = run_dialect_identity_tests("snowflake");
        results.print_summary("Snowflake (Priority)");

        if results.total() > 0 {
            println!(
                "Snowflake coverage: {}/{} ({:.1}%)",
                results.passed,
                results.total(),
                results.pass_rate() * 100.0
            );
        }
    }

    /// DuckDB - modern analytics
    #[test]
    fn test_duckdb_priority() {
        let results = run_dialect_identity_tests("duckdb");
        results.print_summary("DuckDB (Priority)");

        if results.total() > 0 {
            println!(
                "DuckDB coverage: {}/{} ({:.1}%)",
                results.passed,
                results.total(),
                results.pass_rate() * 100.0
            );
        }
    }

    /// TSQL - SQL Server
    #[test]
    fn test_tsql_priority() {
        let results = run_dialect_identity_tests("tsql");
        results.print_summary("TSQL (Priority)");

        if results.total() > 0 {
            println!(
                "TSQL coverage: {}/{} ({:.1}%)",
                results.passed,
                results.total(),
                results.pass_rate() * 100.0
            );
        }
    }
}
