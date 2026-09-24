#![allow(dead_code)]
//! Known failure tracking for SQLGlot compatibility tests

use std::collections::{HashMap, HashSet};

/// A single test failure with all relevant information
#[derive(Debug, Clone)]
pub struct FailureInfo {
    pub test_id: String,
    pub line: usize,
    pub sql: String,
    pub error: String,
    pub category: String,
}

/// Test result tracking
pub struct TestResults {
    pub passed: usize,
    pub failed: usize,
    pub known_failures: usize,
    pub new_failures: Vec<FailureInfo>,
    pub new_passes: Vec<String>,
    known_failures_set: HashSet<String>,
}

impl Default for TestResults {
    fn default() -> Self {
        Self {
            passed: 0,
            failed: 0,
            known_failures: 0,
            new_failures: Vec::new(),
            new_passes: Vec::new(),
            known_failures_set: HashSet::new(),
        }
    }
}

impl TestResults {
    /// Create new test results with known failures
    pub fn with_known_failures(known: &HashSet<String>) -> Self {
        Self {
            known_failures_set: known.clone(),
            ..Default::default()
        }
    }

    /// Record a test result with SQL
    pub fn record_with_sql(
        &mut self,
        test_id: &str,
        sql: &str,
        line: usize,
        result: Result<(), String>,
    ) {
        let is_known_failure = self.known_failures_set.contains(test_id);

        match result {
            Ok(()) => {
                self.passed += 1;
                if is_known_failure {
                    self.new_passes.push(test_id.to_string());
                }
            }
            Err(msg) => {
                self.failed += 1;
                if is_known_failure {
                    self.known_failures += 1;
                } else {
                    let category = Self::categorize_error(&msg);
                    self.new_failures.push(FailureInfo {
                        test_id: test_id.to_string(),
                        line,
                        sql: sql.to_string(),
                        error: msg,
                        category,
                    });
                }
            }
        }
    }

    /// Record a test result (legacy method for backwards compatibility)
    pub fn record(&mut self, test_id: &str, result: Result<(), String>) {
        // Extract line from test_id (format: "identity:123")
        let line = test_id
            .split(':')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        self.record_with_sql(test_id, "", line, result);
    }

    /// Get total number of tests
    pub fn total(&self) -> usize {
        self.passed + self.failed
    }

    /// Get pass rate (0.0 to 1.0)
    pub fn pass_rate(&self) -> f64 {
        if self.total() == 0 {
            return 0.0;
        }
        self.passed as f64 / self.total() as f64
    }

    /// Categorize an error message
    fn categorize_error(error: &str) -> String {
        if error.contains("Mismatch") {
            "Mismatch".to_string()
        } else if error.contains("Unexpected token:") {
            if let Some(pos) = error.find("Unexpected token:") {
                let after = &error[pos + 17..];
                let token: String = after
                    .trim()
                    .split_whitespace()
                    .next()
                    .unwrap_or("Unknown")
                    .to_string();
                format!("Unexpected token: {}", token)
            } else {
                "Unexpected token".to_string()
            }
        } else if error.contains("Expected") && error.contains("got") {
            "Syntax error (expected token)".to_string()
        } else if error.contains("Unknown interval unit") {
            "Unknown interval unit".to_string()
        } else if error.contains("Tokenization error") {
            if error.contains("Unterminated string") {
                "Tokenization: Unterminated string".to_string()
            } else {
                "Tokenization error".to_string()
            }
        } else if error.contains("Generate error") {
            "Generate error".to_string()
        } else if error.contains("No statements parsed") {
            "Empty parse".to_string()
        } else {
            "Other error".to_string()
        }
    }

    /// Print summary to stdout
    pub fn print_summary(&self, category: &str) {
        println!("\n=== {} Results ===", category);
        println!(
            "Passed: {}/{} ({:.1}%)",
            self.passed,
            self.total(),
            self.pass_rate() * 100.0
        );

        if self.known_failures > 0 {
            println!("Known failures: {}", self.known_failures);
        }

        // Group failures by category
        let mut categories: HashMap<String, Vec<&FailureInfo>> = HashMap::new();
        for failure in &self.new_failures {
            categories
                .entry(failure.category.clone())
                .or_default()
                .push(failure);
        }

        // Sort by count descending
        let mut sorted: Vec<_> = categories.iter().collect();
        sorted.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

        if !self.new_failures.is_empty() {
            println!("\n### Failure Categories ###");
            println!("| Category | Count |");
            println!("|----------|-------|");
            for (cat, failures) in &sorted {
                println!("| {} | {} |", cat, failures.len());
            }

            println!("\n### Detailed Failures by Category ###");
            for (cat, failures) in &sorted {
                println!("\n#### {} ({} failures)", cat, failures.len());
                for failure in failures.iter() {
                    println!("- Line {}: {}", failure.line, failure.error);
                }
            }
        }

        if !self.new_passes.is_empty() {
            println!("\nNew passes ({}):", self.new_passes.len());
            for (i, pass) in self.new_passes.iter().take(5).enumerate() {
                println!("  {}. {}", i + 1, pass);
            }
            if self.new_passes.len() > 5 {
                println!("  ... and {} more", self.new_passes.len() - 5);
            }
        }
    }
}

/// Load identity test known failures
/// These are tests that we know fail and are working on fixing
pub fn identity_known_failures() -> HashSet<String> {
    // Start with an empty set - failures will be added as we identify them
    HashSet::new()
}

/// Load dialect identity tests that should be skipped for now.
pub fn dialect_identity_known_failures(_dialect: &str) -> HashSet<String> {
    HashSet::new()
}

/// Load transpilation tests that should be skipped for now.
pub fn transpilation_known_failures(source: &str, target: &str) -> HashSet<String> {
    let mut failures = HashSet::new();

    if source == "bigquery" && target == "duckdb" {
        // Issue #479: SQLGlot 30.14.0 emits bare DECIMAL for BigQuery NUMERIC,
        // narrowing (38, 9) to DuckDB's (18, 3). Verified in DuckDB 1.5.5:
        // CAST('0.1249' AS DECIMAL) yields 0.125; ROUND(..., 2) yields 0.13.
        // The two ROUND fixtures use 2.25, which hides the value error, but their
        // expected CAST still has the wrong precision/scale. ROUND_EVEN also
        // changes 0.1349 to 0.14 instead of 0.13 after that premature rounding.
        // Keep other targets active; existing DuckDB unit tests cover both modes.
        for index in [86, 253, 254] {
            failures.insert(format!("bigquery->duckdb:{index}"));
        }
    }

    if source == "presto" && target == "duckdb" {
        // Issue #463: SQLGlot 30.14.0 maps two-argument MAX_BY to ARG_MAX,
        // which drops NULL winning values. Verified by executing its output
        // in DuckDB; Presto/Trino semantics require ARG_MAX_NULL instead.
        failures.insert("presto->duckdb:75".to_string());
        // Issue #464: SQLGlot 30.14.0 passes REGEXP_EXTRACT through, but
        // DuckDB returns an empty string for no match where Presto returns
        // NULL. Extracting the first REGEXP_EXTRACT_ALL result preserves both
        // absent and empty matches; verified against Trino and DuckDB.
        failures.insert("presto->duckdb:114".to_string());
    }

    // Issue #457 (approved exclusions): SQLGlot removes explicit NULL ordering
    // that matches Snowflake/DuckDB's factory defaults, but both are configurable.
    // Verified against SQLGlot 30.14.0. Exclude only these source/target/index
    // expectations, not other destinations or whole ORDER BY fixture groups.
    // All but duckdb:13 use the shared fname/lname/age ORDER BY example;
    // duckdb:13 is SELECT * FROM x ORDER BY 1 NULLS LAST.
    let null_ordering_cases: &[usize] = match (source, target) {
        ("generic", "duckdb") => &[99],
        ("hive", "duckdb") => &[45],
        ("spark", "duckdb" | "snowflake") => &[82],
        ("snowflake", "duckdb" | "snowflake") => &[143],
        ("duckdb", "duckdb") => &[13, 112],
        _ => &[],
    };
    for index in null_ordering_cases {
        failures.insert(format!("{source}->{target}:{index}"));
    }

    if source == "generic" && target == "tsql" {
        // SQLGlot currently keeps the unsupported T-SQL aggregate FILTER syntax
        // for COUNT_IF(...). Polyglot rewrites remaining filters into CASE
        // expressions for T-SQL/Fabric targets.
        failures.insert("generic->tsql:201".to_string());
    }

    if source == "tsql" && target == "tsql" {
        // SQLGlot renders `a IS NOT FALSE` as `NOT a = 0`, which drops NULL
        // rows in predicate contexts. Polyglot intentionally emits the
        // NULL-preserving T-SQL truth table instead.
        failures.insert("tsql->tsql:80".to_string());
    }

    if source == "tsql" && target == "spark" {
        // SQLGlot treats bare T-SQL FLOAT as single precision, but SQL Server defaults it to
        // FLOAT(53), which is double precision.
        failures.insert("tsql->spark:51".to_string());
    }

    if source == "postgres" && target == "clickhouse" {
        // SQLGlot maps JSON_EXTRACT_PATH_TEXT to JSONExtractString, which
        // collapses a missing path or JSON null into an empty string. Polyglot
        // intentionally preserves PostgreSQL's NULL semantics.
        failures.insert("postgres->clickhouse:14".to_string());
    }

    if source == "tsql" && target == "hive" {
        // SQLGlot treats T-SQL FLOAT(32) as single precision, but T-SQL normalizes every
        // precision from 25 through 53 to double precision.
        failures.insert("tsql->hive:52".to_string());
    }

    failures
}
