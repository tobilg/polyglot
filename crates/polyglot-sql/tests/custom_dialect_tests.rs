//! Auto-discovering test runner for custom dialect fixtures.
//!
//! This test runner auto-discovers all dialect subdirectories in `tests/custom_fixtures/`
//! and runs identity and transpilation tests for each. Adding a new custom dialect only
//! requires creating a new subdirectory with JSON fixture files.
//!
//! Run with: cargo test -p polyglot-sql --test custom_dialect_tests -- --nocapture

mod common;

use common::{
    dialect_identity_test, parse_dialect, transpile_test, AllCustomFixtures,
    CustomDialectFixtureFile, CustomDialectFixtures,
};
use once_cell::sync::Lazy;
use std::fs;
use std::path::Path;

const CUSTOM_FIXTURES_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/custom_fixtures");

/// Load all JSON fixture files from a dialect subdirectory.
fn load_dialect_fixtures(dir: &Path) -> Vec<CustomDialectFixtureFile> {
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "json") {
                match fs::read_to_string(&path) {
                    Ok(content) => match serde_json::from_str::<CustomDialectFixtureFile>(&content)
                    {
                        Ok(fixture) => files.push(fixture),
                        Err(e) => eprintln!("  WARNING: Failed to parse {}: {}", path.display(), e),
                    },
                    Err(e) => {
                        eprintln!("  WARNING: Failed to read {}: {}", path.display(), e)
                    }
                }
            }
        }
    }
    // Sort by category for deterministic output
    files.sort_by(|a, b| a.category.cmp(&b.category));
    files
}

/// Dialects with separate test runners (excluded from this auto-discovery).
const EXCLUDED_DIALECTS: &[&str] = &["clickhouse"];

#[cfg(all(feature = "dialect-hana", feature = "dialect-vertica"))]
#[test]
fn hana_and_vertica_preserve_each_others_source_semantics() {
    use polyglot_sql::{transpile_with_by_name, TranspileOptions, UnsupportedLevel};

    for (source, target) in [("hana", "vertica"), ("vertica", "hana")] {
        assert_eq!(
            transpile_with_by_name(
                "SELECT id FROM t",
                source,
                target,
                &TranspileOptions::strict(),
            )
            .unwrap(),
            ["SELECT id FROM t"],
        );
    }
    // A qualified UDF must not become Vertica's postfix factorial operator.
    assert_eq!(
        transpile_with_by_name(
            "SELECT demo.FACTORIAL(3) FROM t",
            "hana",
            "vertica",
            &TranspileOptions::strict(),
        )
        .unwrap(),
        ["SELECT demo.FACTORIAL(3) FROM t"],
    );
    for level in [
        UnsupportedLevel::Ignore,
        UnsupportedLevel::Warn,
        UnsupportedLevel::Raise,
        UnsupportedLevel::Immediate,
    ] {
        for (sql, source, target) in [
            ("SELECT * FROM t FOR JSON", "hana", "vertica"),
            (
                "ALTER TABLE t ADD (x SMALLDECIMAL ARRAY)",
                "hana",
                "vertica",
            ),
            ("SELECT x::!INT FROM t", "vertica", "hana"),
            ("SELECT LISTAGG(x) FROM t", "vertica", "hana"),
            ("SELECT CAST(12.349 AS DECIMAL(5, 2))", "vertica", "hana"),
        ] {
            assert!(
                transpile_with_by_name(
                    sql,
                    source,
                    target,
                    &TranspileOptions::default().with_unsupported_level(level),
                )
                .is_err(),
                "{source} -> {target}: {sql} ({level:?})",
            );
        }
    }
}

#[test]
fn vertica_semantic_errors_are_independent_of_diagnostic_level() {
    use polyglot_sql::{transpile_with_by_name, TranspileOptions, UnsupportedLevel};
    let cases = [
        (
            "SELECT LISTAGG(x) FROM t",
            "vertica",
            "postgresql",
            "byte limit",
        ),
        (
            "SELECT LISTAGG(x USING PARAMETERS max_length=3, on_overflow='TRUNCATE') FROM t",
            "vertica",
            "mysql",
            "byte limit",
        ),
        (
            "SELECT STRING_AGG(x, ',') FROM t",
            "postgresql",
            "vertica",
            "byte limit",
        ),
        (
            "SELECT TRY_CAST(x AS INT) FROM t",
            "snowflake",
            "vertica",
            "constant cast",
        ),
        (
            "SELECT SAFE_CAST(x AS INT64) FROM t",
            "bigquery",
            "vertica",
            "constant cast",
        ),
    ];
    for level in [
        UnsupportedLevel::Ignore,
        UnsupportedLevel::Warn,
        UnsupportedLevel::Raise,
        UnsupportedLevel::Immediate,
    ] {
        let mut options = TranspileOptions::default();
        options.unsupported_level = level;
        for (sql, source, target, message) in cases {
            let error = transpile_with_by_name(sql, source, target, &options).expect_err(sql);
            assert!(error.to_string().contains(message), "{sql}: {error}");
        }
    }
}

#[test]
fn vertica_filtered_approximate_count_preserves_selected_rows() {
    let output = polyglot_sql::transpile_with_by_name(
        "SELECT APPROX_COUNT_DISTINCT(x) FILTER (WHERE keep) FROM t",
        "duckdb",
        "vertica",
        &polyglot_sql::TranspileOptions::strict(),
    )
    .unwrap();
    assert_eq!(
        output,
        ["SELECT APPROXIMATE_COUNT_DISTINCT(CASE WHEN keep THEN x END) FROM t"]
    );
}

#[test]
fn vertica_parameter_expressions_and_nested_factorials_roundtrip() {
    for sql in [
        "SELECT LISTAGG(x USING PARAMETERS max_length=1024*2) FROM t",
        "SELECT !! !! 3",
        "SELECT !! (1 + 2)",
    ] {
        let once = polyglot_sql::transpile_with_by_name(
            sql,
            "vertica",
            "vertica",
            &polyglot_sql::TranspileOptions::strict(),
        )
        .unwrap();
        let twice = polyglot_sql::transpile_with_by_name(
            &once[0],
            "vertica",
            "vertica",
            &polyglot_sql::TranspileOptions::strict(),
        )
        .unwrap();
        assert_eq!(once, twice, "{sql}");
    }
}

/// Auto-discover all dialect subdirectories and load their fixtures.
static ALL_CUSTOM_FIXTURES: Lazy<AllCustomFixtures> = Lazy::new(|| {
    let mut dialects = Vec::new();
    if let Ok(entries) = fs::read_dir(CUSTOM_FIXTURES_PATH) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                let dialect_name = entry.file_name().to_string_lossy().to_string();
                if EXCLUDED_DIALECTS.contains(&dialect_name.as_str()) {
                    continue;
                }
                let files = load_dialect_fixtures(&entry.path());
                if !files.is_empty() {
                    dialects.push(CustomDialectFixtures {
                        dialect: dialect_name,
                        files,
                    });
                }
            }
        }
    }
    dialects.sort_by(|a, b| a.dialect.cmp(&b.dialect));
    AllCustomFixtures { dialects }
});

/// Collect and run all identity tests for a dialect, returning (passed, failed, total, failures).
fn run_identity_tests(fixtures: &CustomDialectFixtures) -> (usize, usize, usize, Vec<String>) {
    let dialect_type = match parse_dialect(&fixtures.dialect) {
        Some(dt) => dt,
        None => {
            let msg = format!("Unknown dialect: {}", fixtures.dialect);
            return (0, 1, 1, vec![msg]);
        }
    };

    let mut passed = 0;
    let mut failed = 0;
    let mut failures = Vec::new();

    for file in &fixtures.files {
        for (i, test) in file.identity.iter().enumerate() {
            let expected = test.expected.as_deref();
            match dialect_identity_test(&test.sql, expected, dialect_type) {
                Ok(()) => passed += 1,
                Err(e) => {
                    failed += 1;
                    let desc = if test.description.is_empty() {
                        format!("[{}:{}]", file.category, i)
                    } else {
                        format!("[{}:{}] {}", file.category, i, test.description)
                    };
                    failures.push(format!("  FAIL {}: {}", desc, e));
                }
            }
        }
    }

    (passed, failed, passed + failed, failures)
}

/// Collect and run all transpilation tests for a dialect.
/// Supports sqlglot-compatible `write` (forward) and `read` (reverse) maps.
fn run_transpilation_tests(fixtures: &CustomDialectFixtures) -> (usize, usize, usize, Vec<String>) {
    let file_dialect = match parse_dialect(&fixtures.dialect) {
        Some(dt) => dt,
        None => {
            let msg = format!("Unknown source dialect: {}", fixtures.dialect);
            return (0, 1, 1, vec![msg]);
        }
    };

    let mut passed = 0;
    let mut failed = 0;
    let mut failures = Vec::new();

    for file in &fixtures.files {
        for (i, test) in file.transpilation.iter().enumerate() {
            let desc_prefix = if test.description.is_empty() {
                format!("[{}:{}]", file.category, i)
            } else {
                format!("[{}:{}] {}", file.category, i, test.description)
            };

            // Forward: parse as file dialect, generate as each target
            for (target_name, expected) in &test.write {
                let target_dialect = match parse_dialect(target_name) {
                    Some(dt) => dt,
                    None => {
                        failed += 1;
                        failures.push(format!(
                            "  FAIL {} (write→{}): Unknown target dialect",
                            desc_prefix, target_name
                        ));
                        continue;
                    }
                };

                match transpile_test(&test.sql, file_dialect, target_dialect, expected) {
                    Ok(()) => passed += 1,
                    Err(e) => {
                        failed += 1;
                        failures.push(format!(
                            "  FAIL {} (write→{}): {}",
                            desc_prefix, target_name, e
                        ));
                    }
                }
            }

            // Reverse: parse as source dialect, generate as file dialect
            for (source_name, source_sql) in &test.read {
                let source_dialect = match parse_dialect(source_name) {
                    Some(dt) => dt,
                    None => {
                        failed += 1;
                        failures.push(format!(
                            "  FAIL {} (read←{}): Unknown source dialect",
                            desc_prefix, source_name
                        ));
                        continue;
                    }
                };

                match transpile_test(source_sql, source_dialect, file_dialect, &test.sql) {
                    Ok(()) => passed += 1,
                    Err(e) => {
                        failed += 1;
                        failures.push(format!(
                            "  FAIL {} (read←{}): {}",
                            desc_prefix, source_name, e
                        ));
                    }
                }
            }
        }
    }

    (passed, failed, passed + failed, failures)
}

#[test]
fn test_custom_dialect_identity_all() {
    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut total_tests = 0;

    for dialect_fixtures in &ALL_CUSTOM_FIXTURES.dialects {
        let (passed, failed, total, failures) = run_identity_tests(dialect_fixtures);
        total_passed += passed;
        total_failed += failed;
        total_tests += total;

        let pass_rate = if total > 0 {
            (passed as f64 / total as f64) * 100.0
        } else {
            100.0
        };

        println!(
            "\n=== {} Identity Tests: {}/{} passed ({:.1}%) ===",
            dialect_fixtures.dialect, passed, total, pass_rate
        );
        for f in &failures {
            println!("{}", f);
        }
    }

    if total_tests > 0 {
        let overall_rate = (total_passed as f64 / total_tests as f64) * 100.0;
        println!(
            "\n=== Custom Dialect Identity Summary: {}/{} passed ({:.1}%) ===",
            total_passed, total_tests, overall_rate
        );
        assert!(
            total_failed == 0,
            "{} identity test(s) failed out of {}",
            total_failed,
            total_tests
        );
    } else {
        println!("\nNo custom dialect identity tests found.");
    }
}

#[test]
fn test_custom_dialect_transpilation_all() {
    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut total_tests = 0;

    for dialect_fixtures in &ALL_CUSTOM_FIXTURES.dialects {
        let (passed, failed, total, failures) = run_transpilation_tests(dialect_fixtures);
        total_passed += passed;
        total_failed += failed;
        total_tests += total;

        let pass_rate = if total > 0 {
            (passed as f64 / total as f64) * 100.0
        } else {
            100.0
        };

        println!(
            "\n=== {} Transpilation Tests: {}/{} passed ({:.1}%) ===",
            dialect_fixtures.dialect, passed, total, pass_rate
        );
        for f in &failures {
            println!("{}", f);
        }
    }

    if total_tests > 0 {
        let overall_rate = (total_passed as f64 / total_tests as f64) * 100.0;
        println!(
            "\n=== Custom Dialect Transpilation Summary: {}/{} passed ({:.1}%) ===",
            total_passed, total_tests, overall_rate
        );
        assert!(
            total_failed == 0,
            "{} transpilation test(s) failed out of {}",
            total_failed,
            total_tests
        );
    } else {
        println!("\nNo custom dialect transpilation tests found.");
    }
}

#[test]
fn vertica_structured_native_roundtrips() {
    use polyglot_sql::{transpile_with_by_name, TranspileOptions};
    for sql in [
        "SELECT value::!INT FROM t",
        "SELECT (value + 1)::!INT FROM t",
        "SELECT INTERVAL(3) '1.2345 SECOND', INTERVALYM '2 YEARS'",
        "CREATE TABLE t (a ARRAY[INT, 10], b ARRAY[VARCHAR(50)](32000), s SET[INT], r ROW(name VARCHAR, age INT), binary_value LONG VARBINARY(1000))",
        "SELECT ARRAY['1', '2']::ARRAY[INT], ARRAY[2, 1, 2]::SET[INT]",
        "SELECT SET[1, 2, 2]",
        "SELECT EXPLODE(a) OVER() FROM t",
        "SELECT EXPLODE(a) OVER(PARTITION BEST) FROM t",
        "SELECT EXPLODE(a USING PARAMETERS skip_partitioning=true) FROM t",
        "SELECT APPROXIMATE_PERCENTILE(x USING PARAMETERS percentiles='0.5,0.9') FROM t",
        "SELECT ROW_NUMBER() OVER(ORDER BY x NULLS AUTO) FROM t",
        "SELECT LISTAGG(x) WITHIN GROUP(ORDER BY y NULLS AUTO) FROM t",
        "AT EPOCH LATEST SELECT * FROM t",
        "AT EPOCH 42 WITH q AS (SELECT id FROM t) SELECT * FROM q",
        "AT TIME '2026-01-01 00:00:00' SELECT * FROM t",
        "SELECT id FROM t FOR UPDATE OF t",
        "SELECT /*+LABEL('review')*/ id FROM t",
    ] {
        let first = transpile_with_by_name(sql, "vertica", "vertica", &TranspileOptions::strict()).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let second = transpile_with_by_name(&first[0], "vertica", "vertica", &TranspileOptions::strict()).unwrap_or_else(|e| panic!("{}: {e}", first[0]));
        assert_eq!(first, second, "{sql}");
    }
}

#[test]
fn vertica_reviewed_native_surface() {
    use polyglot_sql::traversal::ExpressionWalk;
    use polyglot_sql::{transpile_with_by_name, Dialect, DialectType, TranspileOptions};
    let mut errors = Vec::new();
    for (label, sql) in [
        ("plain_string", "SELECT 'a\\nb'"),
        ("escape_string", "SELECT E'a\\nb'"),
        ("dollar_string", "SELECT $tag$a'b$tag$"),
        ("unicode_string", "SELECT U&'m\\00fcde'"),
        ("unicode_escape", "SELECT U&'m!00fcde' UESCAPE '!'"),
        ("hex_string", "SELECT X'abcd'"),
        ("binary_string", "SELECT B'101100'"),
        ("safe_cast_native", "SELECT value::!INT FROM t"),
        ("array_literal", "SELECT ARRAY[10, 20]"),
        ("array_index", "SELECT (ARRAY[10, 20])[0]"),
        ("array_column_type", "CREATE TABLE t (a ARRAY[INT])"),
        ("array_bound", "CREATE TABLE t (a ARRAY[INT, 10])"),
        ("array_size", "CREATE TABLE t (a ARRAY[VARCHAR(50)](32000))"),
        ("set_literal", "SELECT SET[1, 2, 2]"),
        ("set_column_type", "CREATE TABLE t (id INT, s SET[INT])"),
        ("set_cast", "SELECT ARRAY[2, 1, 2]::SET[INT]"),
        ("row_named", "SELECT ROW('Amy' AS name, 2 AS id)"),
        ("row_alias_names", "SELECT ROW('Amy', 2) AS student(name, id)"),
        ("row_column_type", "CREATE TABLE t (id INT, r ROW(name VARCHAR, age INT))"),
        ("percentile_params", "SELECT APPROXIMATE_PERCENTILE(x USING PARAMETERS percentiles='0.5,0.9') FROM t"),
        ("explode_window", "SELECT EXPLODE(a) OVER() FROM t"),
        ("explode_params", "SELECT EXPLODE(a USING PARAMETERS skip_partitioning=true) FROM t"),
        ("explode_partition_best", "SELECT EXPLODE(a) OVER(PARTITION BEST) FROM t"),
        ("nulls_auto_window", "SELECT ROW_NUMBER() OVER(ORDER BY x NULLS AUTO) FROM t"),
        ("nulls_auto_aggregate", "SELECT LISTAGG(x) WITHIN GROUP(ORDER BY y NULLS AUTO) FROM t"),
        ("epoch_latest", "AT EPOCH LATEST SELECT * FROM t"),
        ("epoch_number", "AT EPOCH 42 SELECT * FROM t"),
        ("epoch_time", "AT TIME '2026-01-01 00:00:00' SELECT * FROM t"),
        ("for_update", "SELECT id FROM t FOR UPDATE"),
        ("for_update_of", "SELECT id FROM t FOR UPDATE OF t"),
        ("limit_partition", "SELECT k, v FROM t LIMIT 2 OVER(PARTITION BY k ORDER BY v DESC)"),
        ("timeseries", "SELECT slice_time, TS_FIRST_VALUE(v) FROM t TIMESERIES slice_time AS '5 seconds' OVER(ORDER BY ts)"),
        ("match_events", "SELECT * FROM t MATCH (PARTITION BY k ORDER BY ts DEFINE A AS v > 0 PATTERN P AS (A+))"),
        ("interpolate", "SELECT t.ts FROM t LEFT JOIN u ON t.ts INTERPOLATE PREVIOUS VALUE u.ts"),
        ("select_label_hint", "SELECT /*+LABEL('coverage_review')*/ id FROM t"),
        ("recursive_cte", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r"),
        ("grouping_sets", "SELECT a, b, COUNT(*) FROM t GROUP BY GROUPING SETS ((a), (b), ())"),
        ("match_columns", "SELECT MATCH_COLUMNS('^a') FROM t"),
        ("projection", "CREATE PROJECTION p AS SELECT id FROM t ORDER BY id UNSEGMENTED ALL NODES"),
        ("table_segmentation", "CREATE TABLE t (id INT) ORDER BY id SEGMENTED BY HASH(id) ALL NODES"),
        ("table_encoding", "CREATE TABLE t (id INT ENCODING RLE)"),
        ("flex_table", "CREATE FLEX TABLE t()"),
        ("copy_local", "COPY t FROM LOCAL '/tmp/data.csv' DELIMITER ','"),
        ("copy_parser", "COPY t FROM '/tmp/data.json' PARSER FJSONPARSER()"),
        ("export_parquet", "EXPORT TO PARQUET(directory='/tmp/out') AS SELECT * FROM t"),
        ("conditional_event", "SELECT CONDITIONAL_TRUE_EVENT(x > 0) OVER(ORDER BY ts) FROM t"),
        ("nullifzero", "SELECT NULLIFZERO(x) FROM t"),
        ("regexp_extract", "SELECT REGEXP_SUBSTR(x, '(a)', 1, 1, '', 1) FROM t"),
        ("time_slice", "SELECT TIME_SLICE(ts, 5, 'MINUTE', 'START') FROM t"),
        ("array_column_index", "SELECT a[0] FROM t"),
        ("array_slice", "SELECT (ARRAY[10, 20, 30])[0:2]"),
        ("copy_parser_parameter", "COPY t FROM '/tmp/data.json' PARSER FJSONPARSER(flatten_maps=true)"),
        ("array_cast", "SELECT ARRAY['1', '2']::ARRAY[INT]")
    ] {
        let check = || -> Result<(), String> {
            let ast = Dialect::get(DialectType::Vertica).parse(sql).map_err(|e| e.to_string())?;
            for root in &ast {
                if root.dfs().any(|e| matches!(e, polyglot_sql::expressions::Expression::Raw(_) | polyglot_sql::expressions::Expression::Command(_))) { return Err("unstructured AST".into()); }
                let json = serde_json::to_string(root).unwrap();
                let restored: polyglot_sql::expressions::Expression = serde_json::from_str(&json).map_err(|e| e.to_string())?;
                assert_eq!(root, &restored);
            }
            let output = transpile_with_by_name(sql, "vertica", "vertica", &TranspileOptions::strict()).map_err(|e| e.to_string())?;
            let again = transpile_with_by_name(&output[0], "vertica", "vertica", &TranspileOptions::strict()).map_err(|e| format!("{}: {e}", output[0]))?;
            if output != again { return Err(format!("unstable: {output:?} -> {again:?}")); }
            Ok(())
        };
        if let Err(error) = check() { errors.push(format!("{label}: {error}")); }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn vertica_unsafe_foreign_conversions_fail_in_every_mode() {
    use polyglot_sql::{transpile_with_by_name, TranspileOptions, UnsupportedLevel};
    for sql in [
        "SELECT x::!INT FROM t",
        "SELECT SET[1, 2]",
        "SELECT ROW(1, 2)",
        "SELECT INTERVAL(3) '1.2345 SECOND'",
        "SELECT EXPLODE(a) OVER() FROM t",
        "SELECT ROW_NUMBER() OVER(ORDER BY x NULLS AUTO) FROM t",
        "SELECT APPROXIMATE_PERCENTILE(x USING PARAMETERS percentiles='0.5') FROM t",
        "SELECT x FROM t ORDER BY x",
        "SELECT DATEDIFF(day, a, b) FROM t",
        "SELECT DATEDIFF(day, a::TIMESTAMPTZ, b::TIMESTAMPTZ) FROM t",
        "SELECT a[lo:hi] FROM t",
        "SELECT id FROM t FOR UPDATE",
        "SELECT /*+LABEL('review')*/ id FROM t",
        "AT EPOCH LATEST SELECT * FROM t",
        "SELECT slice_time FROM t TIMESERIES slice_time AS '5 seconds' OVER(ORDER BY ts)",
        "SELECT * FROM t MATCH(ORDER BY ts DEFINE A AS v > 0 PATTERN P AS(A+))",
        "SELECT t.ts FROM t LEFT JOIN u ON t.ts INTERPOLATE NEXT VALUE u.ts",
        "CREATE PROJECTION p AS SELECT id FROM t UNSEGMENTED ALL NODES",
        "CREATE TABLE t(id INT ENCODING RLE)",
        "CREATE TABLE t(a ARRAY[INT, 10])",
        "CREATE TABLE t(b LONG VARBINARY(1000))",
        "CREATE FLEX TABLE t()",
        "COPY t FROM LOCAL '/tmp/file'",
        "EXPORT TO PARQUET(directory='/tmp/out') AS SELECT * FROM t",
        "SELECT * FROM t LIMIT 1 OVER(PARTITION BY k ORDER BY v)",
        "SELECT TIME_SLICE(ts, 5, 'MINUTE', 'START') FROM t",
        "SELECT MATCH_COLUMNS('^a') FROM t",
        "SELECT REGEXP_SUBSTR(x, '(a)', 1, 1, '', 1) FROM t",
    ] {
        for target in ["postgresql", "duckdb"] {
            for level in [
                UnsupportedLevel::Ignore,
                UnsupportedLevel::Warn,
                UnsupportedLevel::Raise,
                UnsupportedLevel::Immediate,
            ] {
                let mut options = TranspileOptions::default();
                options.unsupported_level = level;
                assert!(
                    transpile_with_by_name(sql, "vertica", target, &options).is_err(),
                    "{sql} -> {target}, {level:?}"
                );
            }
        }
    }
}

#[test]
fn vertica_foreign_values_and_types() {
    use polyglot_sql::{transpile_with_by_name, Dialect, DialectType, TranspileOptions};
    // Set POLYGLOT_DUCKDB to a DuckDB CLI to execute the generated expressions too.
    let engine = std::env::var("POLYGLOT_DUCKDB").ok();
    for (sql, expected) in [
        ("SELECT (ARRAY[10, 20])[0] AS result", "10"),
        ("SELECT (ARRAY[10, 20])[-1] AS result", "NULL"),
        ("SELECT (ARRAY[10, 20])[99] AS result", "NULL"),
        ("SELECT (ARRAY[10, 20])[NULL] AS result", "NULL"),
        ("SELECT (ARRAY[10, 20])[i] AS result FROM (SELECT 1 AS i) t", "20"),
        ("SELECT (ARRAY[ARRAY[1, 2], ARRAY[3, 4]])[1][0] AS result", "3"),
        ("SELECT (ARRAY[10, 20, 30])[0:2] AS result", "[10, 20]"),
        ("SELECT (ARRAY[10, 20, 30])[2:1] AS result", "[]"),
        ("SELECT (ARRAY[10, 20, 30])[:2] AS result", "[10, 20]"),
        ("SELECT (ARRAY[10, 20, 30])[1:] AS result", "[20, 30]"),
        ("SELECT NULLIFZERO(0) AS result", "NULL"),
        ("SELECT NULLIFZERO(2) AS result", "2"),
        ("SELECT DATEDIFF(day, TIMESTAMP '2026-01-01 23:59:00', TIMESTAMP '2026-01-02 00:01:00') AS result", "1"),
        ("SELECT DATEDIFF(day, TIMESTAMP '2026-01-02 00:01:00', TIMESTAMP '2026-01-01 23:59:00') AS result", "-1"),
        ("SELECT DATEDIFF(year, DATE '2025-12-31', DATE '2026-01-01') AS result", "1"),
        ("SELECT DATEDIFF(quarter, DATE '2026-03-31', DATE '2026-04-01') AS result", "1"),
        ("SELECT DATEDIFF(month, DATE '2026-01-31', DATE '2026-02-01') AS result", "1"),
        ("SELECT DATEDIFF(hour, TIMESTAMP '2026-01-01 00:59:59', TIMESTAMP '2026-01-01 01:00:00') AS result", "1"),
        ("SELECT DATEDIFF(minute, TIMESTAMP '2026-01-01 00:00:59', TIMESTAMP '2026-01-01 00:01:00') AS result", "1"),
        ("SELECT DATEDIFF(second, TIMESTAMP '2026-01-01 00:00:00.999999', TIMESTAMP '2026-01-01 00:00:01') AS result", "1"),
        ("SELECT DATEDIFF(millisecond, TIMESTAMP '2026-01-01 00:00:00.000999', TIMESTAMP '2026-01-01 00:00:00.001') AS result", "1"),
        ("SELECT DATEDIFF(microsecond, TIMESTAMP '2026-01-01 00:00:00.000001', TIMESTAMP '2026-01-01 00:00:00.000002') AS result", "1"),
    ] {
        let output = transpile_with_by_name(sql, "vertica", "duckdb", &TranspileOptions::strict()).unwrap_or_else(|e| panic!("{sql}: {e}"));
        Dialect::get(DialectType::DuckDB).parse(&output[0]).unwrap_or_else(|e| panic!("{}: {e}", output[0]));
        let postgres = transpile_with_by_name(sql, "vertica", "postgresql", &TranspileOptions::strict()).unwrap_or_else(|e| panic!("{sql}: {e}"));
        Dialect::get(DialectType::PostgreSQL).parse(&postgres[0]).unwrap_or_else(|e| panic!("{}: {e}", postgres[0]));
        if let Some(engine) = &engine {
            let query = format!("SELECT COALESCE(CAST(result AS VARCHAR), 'NULL') FROM ({}) q", output[0]);
            let result = std::process::Command::new(engine).args(["-init", "/dev/null", "-noheader", "-list", ":memory:", &query]).output().unwrap();
            assert!(result.status.success(), "{}: {}", output[0], String::from_utf8_lossy(&result.stderr));
            assert_eq!(String::from_utf8_lossy(&result.stdout).trim(), expected, "{sql}\n{}", output[0]);
        }
    }
    for (source, hex) in [
        ("B'101100'", "2c"),
        ("B'000000001'", "0001"),
        ("X'abc'", "0abc"),
        ("B''", ""),
    ] {
        let sql = format!("SELECT {source}");
        let pg = transpile_with_by_name(&sql, "vertica", "postgresql", &TranspileOptions::strict())
            .unwrap();
        assert_eq!(pg[0], format!("SELECT DECODE('{hex}', 'hex')"));
        let duck =
            transpile_with_by_name(&sql, "vertica", "duckdb", &TranspileOptions::strict()).unwrap();
        assert_eq!(duck[0], format!("SELECT UNHEX('{hex}')"));
    }
}

#[test]
fn vertica_partitioned_limit_preserves_outputs_and_scopes() {
    use polyglot_sql::expressions::Expression;
    use polyglot_sql::{transpile_with_by_name, Dialect, DialectType, TranspileOptions};
    let sql = "SELECT k, v AS value FROM t LIMIT 2 OVER(PARTITION BY k ORDER BY v DESC)";
    let ast = Dialect::get(DialectType::Vertica).parse(sql).unwrap();
    let Expression::Select(select) = &ast[0] else {
        panic!("expected SELECT")
    };
    assert!(select.vertica.as_ref().unwrap().limit_over.is_some());
    for target in ["postgresql", "duckdb"] {
        let result =
            transpile_with_by_name(sql, "vertica", target, &TranspileOptions::strict()).unwrap();
        assert!(result[0].contains("ROW_NUMBER() OVER"), "{}", result[0]);
        if target == "duckdb" {
            assert!(result[0].contains("NULLS FIRST"), "{}", result[0]);
        }
        let ast = Dialect::get(if target == "duckdb" {
            DialectType::DuckDB
        } else {
            DialectType::PostgreSQL
        })
        .parse(&result[0])
        .unwrap();
        let Expression::Select(select) = &ast[0] else {
            panic!("expected SELECT")
        };
        assert_eq!(select.expressions.len(), 2);
        assert!(matches!(&select.expressions[1], Expression::Alias(a) if a.alias.name == "value"));
        if target == "duckdb" {
            if let Ok(engine) = std::env::var("POLYGLOT_DUCKDB") {
                let sql = format!("CREATE TABLE t(k INT, v INT); INSERT INTO t VALUES (1,1),(1,2),(1,3),(2,NULL),(2,4),(2,5); SELECT k, COALESCE(CAST(value AS VARCHAR), 'NULL') FROM ({}) q ORDER BY k, value NULLS FIRST", result[0]);
                let output = std::process::Command::new(engine)
                    .args(["-init", "/dev/null", "-noheader", "-list", ":memory:", &sql])
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert_eq!(
                    String::from_utf8_lossy(&output.stdout).trim(),
                    "1|2\n1|3\n2|NULL\n2|5"
                );
            }
        }
    }
}

#[test]
fn vertica_structured_fields_and_invalid_native_syntax() {
    use polyglot_sql::expressions::{Expression, VerticaExpression, VerticaKsafe};
    use polyglot_sql::traversal::{is_aggregate, ExpressionWalk};
    use polyglot_sql::{Dialect, DialectType};
    let dialect = Dialect::get(DialectType::Vertica);
    let parsed = dialect.parse("CREATE PROJECTION p(id ENCODING RLE) AS SELECT id FROM t ORDER BY id SEGMENTED BY HASH(id) ALL NODES KSAFE 1").unwrap();
    let Expression::Vertica(node) = &parsed[0] else {
        panic!("expected native projection")
    };
    let VerticaExpression::Projection {
        physical, columns, ..
    } = node.as_ref()
    else {
        panic!("expected projection")
    };
    assert_eq!(physical.order_by.len(), 1);
    assert_eq!(physical.ksafe, Some(VerticaKsafe::Level(1)));
    assert_eq!(columns[0].encoding.as_ref().unwrap().name, "RLE");
    assert!(parsed[0]
        .dfs()
        .any(|e| matches!(e, Expression::Column(c) if c.name.name == "id")));
    assert!(Dialect::get(DialectType::PostgreSQL)
        .generate(&parsed[0])
        .is_err());
    let aggregate = dialect
        .parse("SELECT APPROXIMATE_PERCENTILE(x USING PARAMETERS percentiles='0.5') FROM t")
        .unwrap();
    assert!(aggregate[0].dfs().any(is_aggregate));
    for sql in [
        "CREATE TABLE t(r ROW(\"odd name\" INT, \"a\"\"b\" VARCHAR))",
        "CREATE PROJECTION p AS SELECT id FROM t UNSEGMENTED ALL NODES KSAFE",
        "SELECT k, v FROM t LIMIT 1 OVER(PARTITION BY k ORDER BY v) OFFSET 2",
    ] {
        let ast = dialect.parse(sql).unwrap();
        let restored: Vec<Expression> =
            serde_json::from_str(&serde_json::to_string(&ast).unwrap()).unwrap();
        assert_eq!(ast, restored, "{sql}");
        let output = dialect.generate(&ast[0]).unwrap();
        let again = dialect.parse(&output).unwrap();
        assert_eq!(output, dialect.generate(&again[0]).unwrap());
    }
    for sql in [
        "CREATE TABLE t(a ARRAY[INT, 0])",
        "CREATE TABLE t(a ARRAY[INT, 2](100))",
        "SELECT INTERVAL(7) '1 SECOND'",
        "SELECT EXPLODE(a USING PARAMETERS x=1, X=2) FROM t",
        "SELECT * FROM t MATCH(ORDER BY ts DEFINE A AS SUM(v) > 0 PATTERN P AS(A+))",
        "SELECT DISTINCT v FROM t MATCH(ORDER BY ts DEFINE A AS v > 0 PATTERN P AS(A+))",
        "SELECT * FROM t MATCH(ORDER BY ts DEFINE A AS v > 0 PATTERN P AS(B+))",
    ] {
        assert!(dialect.parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn vertica_ordering_contexts_preserve_null_placement() {
    use polyglot_sql::{transpile_with_by_name, TranspileOptions};
    for (sql, expected) in [
        (
            "SELECT ROW_NUMBER() OVER(ORDER BY x DESC) FROM t",
            "x DESC NULLS FIRST",
        ),
        ("SELECT x::INT AS i FROM t ORDER BY i", "i NULLS FIRST"),
        ("SELECT 1 AS i ORDER BY i", "i NULLS FIRST"),
    ] {
        let output =
            transpile_with_by_name(sql, "vertica", "duckdb", &TranspileOptions::strict()).unwrap();
        assert!(output[0].contains(expected), "{}", output[0]);
    }
    assert!(transpile_with_by_name(
        "SELECT RANDOM() AS x ORDER BY x NULLS FIRST",
        "postgresql",
        "vertica",
        &TranspileOptions::default()
    )
    .is_err());
}

#[test]
fn vertica_direct_generation_preserves_filters_and_safe_cast_failures() {
    use polyglot_sql::{Dialect, DialectType};
    let source = Dialect::get(DialectType::DuckDB);
    let target = Dialect::get(DialectType::Vertica);
    let ast = source
        .parse("SELECT APPROX_COUNT_DISTINCT(x) FILTER(WHERE keep) FROM t")
        .unwrap();
    let sql = target.generate(&ast[0]).unwrap();
    assert!(
        sql.contains("APPROXIMATE_COUNT_DISTINCT(CASE WHEN keep THEN x END)"),
        "{sql}"
    );
    let ast = source.parse("SELECT TRY_CAST(x AS INT) FROM t").unwrap();
    assert!(target.generate(&ast[0]).is_err());
}

fn execute_vertica_target(setup: &str, sql: &str) -> Option<String> {
    let engine = std::env::var("POLYGLOT_DUCKDB").ok()?;
    let query = format!("{setup}; {sql}");
    let result = std::process::Command::new(engine)
        .args([
            "-init",
            "/dev/null",
            "-noheader",
            "-list",
            "-nullvalue",
            "NULL",
            ":memory:",
            &query,
        ])
        .output()
        .expect("run DuckDB");
    assert!(
        result.status.success(),
        "{query}\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    Some(String::from_utf8_lossy(&result.stdout).trim().to_string())
}

fn vertica_translate(sql: &str, read: &str, write: &str) -> String {
    polyglot_sql::transpile_with_by_name(
        sql,
        read,
        write,
        &polyglot_sql::TranspileOptions::strict(),
    )
    .unwrap_or_else(|e| panic!("{read} -> {write}: {sql}: {e}"))[0]
        .clone()
}

#[test]
fn vertica_array_access_uses_native_subscripts_without_name_capture() {
    let setup = "CREATE TABLE t(a INT[], arr INT[], i0 BIGINT); INSERT INTO t VALUES ([10,20],[10,20],0),([10,20],[10,20],1),([10,20],[10,20],-1),([10,20],[10,20],NULL),([10,20],[10,20],9223372036854775807)";
    for (sql, expected) in [
        ("SELECT a[0] FROM t", "10\n10\n10\n10\n10"),
        ("SELECT arr[i0] FROM t", "10\n20\nNULL\nNULL\nNULL"),
        ("SELECT arr[i0 + 0] FROM t", "10\n20\nNULL\nNULL\nNULL"),
        (
            "SELECT arr[COALESCE(i0, NULL)] FROM t",
            "10\n20\nNULL\nNULL\nNULL",
        ),
        (
            "SELECT arr[2147483647] FROM t",
            "NULL\nNULL\nNULL\nNULL\nNULL",
        ),
    ] {
        for target in ["duckdb", "postgresql"] {
            let output = vertica_translate(sql, "vertica", target);
            assert!(!output.contains("(SELECT"), "{output}");
            polyglot_sql::Dialect::get(target.parse().unwrap())
                .parse(&output)
                .unwrap();
            if target == "duckdb" {
                if let Some(actual) = execute_vertica_target(setup, &output) {
                    assert_eq!(actual, expected, "{output}");
                }
            }
        }
    }
    assert_eq!(
        vertica_translate("SELECT SUM(arr[0]) FROM t", "vertica", "duckdb"),
        "SELECT SUM(arr[1]) FROM t"
    );
    let bounds = "CREATE TABLE t(arr INT[], i BIGINT); INSERT INTO t VALUES ([10],-9223372036854775808),([10],2147483646),([10],2147483647),([10],NULL)";
    for index in ["i", "i + 0", "COALESCE(i, NULL)", "-9223372036854775808"] {
        let sql = vertica_translate(&format!("SELECT arr[{index}] FROM t"), "vertica", "duckdb");
        if let Some(actual) = execute_vertica_target(bounds, &sql) {
            assert_eq!(actual, "NULL\nNULL\nNULL\nNULL", "{sql}");
        }
    }
    let sql = vertica_translate("SELECT arr[NEXTVAL('s')] FROM t", "vertica", "duckdb");
    assert_eq!(sql.matches("NEXTVAL").count(), 1, "{sql}");
    let setup = "CREATE SEQUENCE s MINVALUE 0 START 0; CREATE TABLE t(arr INT[]); INSERT INTO t VALUES ([10,20,30]),([10,20,30]),([10,20,30])";
    if let Some(actual) = execute_vertica_target(setup, &sql) {
        assert_eq!(actual, "10\n20\n30");
    }
    if let Some(plan) = execute_vertica_target(
        "CREATE TABLE t(arr INT[])",
        "EXPLAIN SELECT SUM(arr[1]) FROM t",
    ) {
        assert!(!plan.contains("JOIN"), "{plan}");
    }
}

#[test]
fn vertica_partitioned_limit_preserves_constants_and_base_aliases() {
    let setup = "CREATE TABLE t(k INT, v INT); INSERT INTO t VALUES (1,1),(1,2),(2,3)";
    for (sql, expected) in [
        ("SELECT k, v FROM t LIMIT 1 OVER(PARTITION BY 1 ORDER BY v DESC)", "2|3"),
        ("SELECT k AS d, SUM(v) AS s FROM t GROUP BY d LIMIT 1 OVER(PARTITION BY 1 ORDER BY d DESC)", "2|3"),
        ("SELECT k AS d, v FROM t WHERE d > 1 LIMIT 1 OVER(PARTITION BY d ORDER BY v)", "2|3"),
        ("SELECT k AS d, SUM(v) AS s FROM t GROUP BY k HAVING s > 1 LIMIT 1 OVER(PARTITION BY 1 ORDER BY d DESC)", "2|3"),
        ("SELECT k AS \"odd name\", v FROM t LIMIT 1 OVER(PARTITION BY 1 ORDER BY v DESC)", "2|3"),
    ] {
        let output = vertica_translate(sql, "vertica", "duckdb");
        if let Some(actual) = execute_vertica_target(setup, &output) { assert_eq!(actual, expected, "{sql}\n{output}"); }
    }
    let sql = vertica_translate(
        "SELECT k AS d, SUM(v) AS s FROM t GROUP BY d LIMIT 1 OVER(PARTITION BY d ORDER BY s)",
        "vertica",
        "postgresql",
    );
    assert!(
        sql.contains("k AS d") && sql.contains("GROUP BY d"),
        "{sql}"
    );
    let sql = vertica_translate(
        "SELECT k, v FROM t LIMIT 1 OVER(PARTITION BY k ORDER BY 1)",
        "vertica",
        "duckdb",
    );
    assert!(
        sql.contains("ORDER BY 1"),
        "window constants are not ordinals: {sql}"
    );
    let sql = vertica_translate(
        "SELECT k, v FROM t ORDER BY 2 NULLS LAST LIMIT 1 OVER(PARTITION BY 1 ORDER BY v DESC)",
        "vertica",
        "duckdb",
    );
    if let Some(actual) = execute_vertica_target(setup, &sql) {
        assert_eq!(actual, "2|3");
    }
    let sql = vertica_translate(
        "SELECT k FROM t LIMIT 1 OVER(PARTITION BY 1 ORDER BY _vertica_hidden DESC)",
        "vertica",
        "duckdb",
    );
    if let Some(actual) = execute_vertica_target(
        "CREATE TABLE t(k INT, _vertica_hidden INT); INSERT INTO t VALUES (1,1),(2,2)",
        &sql,
    ) {
        assert_eq!(actual, "2", "{sql}");
    }
}

#[test]
fn vertica_set_operation_ordering_preserves_nulls_and_limits() {
    for operation in ["UNION ALL", "INTERSECT", "EXCEPT"] {
        let branches = if operation == "EXCEPT" {
            "SELECT CAST(1 AS INT) AS x UNION ALL SELECT CAST(NULL AS INT) AS x EXCEPT SELECT CAST(2 AS INT) AS x".to_string()
        } else if operation == "INTERSECT" {
            "(SELECT CAST(1 AS INT) AS x UNION ALL SELECT CAST(NULL AS INT) AS x) INTERSECT (SELECT CAST(1 AS INT) AS x UNION ALL SELECT CAST(NULL AS INT) AS x)".to_string()
        } else {
            "SELECT CAST(1 AS INT) AS x UNION ALL SELECT CAST(NULL AS INT) AS x".to_string()
        };
        for key in ["x", "1"] {
            let sql = format!("{branches} ORDER BY {key} LIMIT 1");
            let output = vertica_translate(&sql, "vertica", "duckdb");
            assert!(output.contains("NULLS FIRST"), "{output}");
            if let Some(actual) = execute_vertica_target("", &output) {
                assert_eq!(actual, "NULL", "{sql}");
            }
        }
    }
    for operation in ["UNION ALL", "INTERSECT", "EXCEPT"] {
        let sql =
            format!("SELECT x FROM t {operation} SELECT x FROM u ORDER BY x NULLS FIRST LIMIT 1");
        let output = vertica_translate(&sql, "postgresql", "vertica");
        assert!(
            output.contains("CASE WHEN") && !output.contains("NULLS FIRST"),
            "{output}"
        );
        assert!(output.ends_with("LIMIT 1"), "{output}");
        polyglot_sql::Dialect::get(polyglot_sql::DialectType::Vertica)
            .parse(&output)
            .unwrap();
        let setup = "CREATE TABLE t(x INT); INSERT INTO t VALUES (NULL),(1),(2); CREATE TABLE u(x INT); INSERT INTO u VALUES (NULL),(3)";
        if let Some(actual) = execute_vertica_target(setup, &output) {
            assert_eq!(
                actual,
                if operation == "EXCEPT" { "1" } else { "NULL" },
                "{output}"
            );
        }
    }
}

#[test]
fn vertica_unverified_ordering_and_clocks_fail_in_every_mode() {
    use polyglot_sql::{transpile_with_by_name, TranspileOptions, UnsupportedLevel};
    for (sql, target) in [
        ("SELECT CAST(x AS VARCHAR) AS x FROM t ORDER BY t.x", "duckdb"),
        ("SELECT x FROM t UNION ALL SELECT x FROM u ORDER BY x", "duckdb"),
        ("SELECT CAST(1 AS INT) AS x UNION ALL SELECT CAST(2 AS FLOAT) AS x ORDER BY x", "duckdb"),
        ("SELECT GETDATE()", "duckdb"),
        ("SELECT GETUTCDATE()", "duckdb"),
        ("SELECT SYSDATE", "duckdb"),
        ("SELECT k AS d, v FROM t WHERE d > 1 LIMIT 1 OVER(PARTITION BY d ORDER BY v)", "postgresql"),
        ("SELECT k, SUM(v) AS s FROM t GROUP BY k HAVING s > 1 LIMIT 1 OVER(PARTITION BY k ORDER BY s)", "postgresql"),
    ] {
        for level in [UnsupportedLevel::Ignore, UnsupportedLevel::Warn, UnsupportedLevel::Raise, UnsupportedLevel::Immediate] {
            let mut options = TranspileOptions::default();
            options.unsupported_level = level;
            assert!(transpile_with_by_name(sql, "vertica", target, &options).is_err(), "{sql}, {level:?}");
        }
    }
    let output = vertica_translate("SELECT GETDATE(), GETUTCDATE()", "vertica", "postgresql");
    assert!(output.contains("STATEMENT_TIMESTAMP()"));
}

#[test]
fn vertica_bigquery_countif_preserves_empty_inputs_and_frames() {
    let setup = "CREATE TABLE t(x INT); INSERT INTO t VALUES (1),(2)";
    for (sql, expected) in [
        ("SELECT COUNTIF(x > 0) FROM t WHERE FALSE", "0"),
        ("SELECT COUNTIF(x > 10) FROM t", "0"),
        ("SELECT COUNTIF(x > 0) FROM t", "2"),
        ("SELECT COUNTIF(x > 0) OVER(ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) FROM t", "0\n1"),
    ] {
        let output = vertica_translate(sql, "bigquery", "vertica");
        assert!(output.contains("COUNT(CASE"), "{output}");
        if let Some(actual) = execute_vertica_target(setup, &output) { assert_eq!(actual, expected, "{output}"); }
    }
}

#[test]
fn vertica_distinct_ordering_sorts_outside_deduplication() {
    for (sql, expected) in [
        (
            "SELECT DISTINCT x FROM t ORDER BY x NULLS FIRST LIMIT 2",
            "NULL\n1",
        ),
        (
            "SELECT DISTINCT x AS value FROM t ORDER BY 1 NULLS FIRST LIMIT 2",
            "NULL\n1",
        ),
        (
            "SELECT DISTINCT x AS \"odd name\" FROM t ORDER BY \"odd name\" NULLS FIRST LIMIT 2",
            "NULL\n1",
        ),
        ("SELECT DISTINCT x FROM t ORDER BY x LIMIT 2", "1\n2"),
    ] {
        let output = vertica_translate(sql, "postgresql", "vertica");
        let ast = polyglot_sql::Dialect::get(polyglot_sql::DialectType::Vertica)
            .parse(&output)
            .unwrap();
        let polyglot_sql::expressions::Expression::Select(outer) = &ast[0] else {
            panic!("expected SELECT")
        };
        assert!(!outer.distinct && outer.expressions.len() == 1, "{output}");
        assert!(
            output.contains("SELECT DISTINCT") && output.contains("CASE WHEN"),
            "{output}"
        );
        let setup = "CREATE TABLE t(x INT); INSERT INTO t VALUES (1),(1),(NULL),(2)";
        if let Some(actual) = execute_vertica_target(setup, &output) {
            assert_eq!(actual, expected, "{output}");
        }
    }
}

#[test]
fn vertica_parenthesized_query_ordering_preserves_nulls_and_aliases() {
    for sql in [
        "(SELECT x FROM t) ORDER BY x NULLS FIRST LIMIT 1",
        "SELECT y FROM ((SELECT x FROM t UNION ALL SELECT x FROM t) ORDER BY x NULLS FIRST LIMIT 1) AS s(y)",
    ] {
        let output = vertica_translate(sql, "postgresql", "vertica");
        assert!(output.contains("CASE WHEN") && !output.contains("NULLS FIRST"), "{output}");
        if let Some(actual) = execute_vertica_target(
            "CREATE TABLE t(x INT); INSERT INTO t VALUES (1),(NULL)",
            &output,
        ) {
            assert_eq!(actual, "NULL", "{output}");
        }
    }
    let sql =
        "(SELECT CAST(1 AS INT) AS x UNION ALL SELECT CAST(NULL AS INT) AS x) ORDER BY x LIMIT 1";
    let output = vertica_translate(sql, "vertica", "duckdb");
    if let Some(actual) = execute_vertica_target("", &output) {
        assert_eq!(actual, "NULL", "{output}");
    }
}

#[test]
#[ignore = "manual DuckDB execution benchmark; set POLYGLOT_DUCKDB and run with --ignored --nocapture"]
fn vertica_array_execution_benchmark() {
    std::env::var("POLYGLOT_DUCKDB").expect("POLYGLOT_DUCKDB must name a DuckDB CLI");
    let previous = "SELECT SUM((SELECT _polyglot_v.a[CASE WHEN _polyglot_v.i0 < 0 OR _polyglot_v.i0 >= 2147483647 THEN NULL ELSE _polyglot_v.i0 + 1 END] FROM (SELECT arr AS a, 0 AS i0) AS _polyglot_v)) FROM t";
    let setup = "SET threads=1; CREATE TABLE t AS SELECT [i,i+1] AS arr, i%2 AS idx FROM range(1000000) r(i); CREATE SEQUENCE s MINVALUE 0 START 0";
    for (label, index) in [
        ("constant", "0"),
        ("column", "idx"),
        ("computed", "idx + 0"),
        ("volatile", "NEXTVAL('s') % 2"),
    ] {
        let generated = vertica_translate(
            &format!("SELECT SUM(arr[{index}]) FROM t"),
            "vertica",
            "duckdb",
        );
        let native = if index == "0" {
            let direct = "SELECT SUM(arr[1]) FROM t";
            assert_eq!(generated, direct);
            direct.to_string()
        } else {
            format!("SELECT SUM(arr[({index}) + 1]) FROM t")
        };
        let mut queries = vec![("native", native)];
        if index == "0" {
            queries.push(("previous", previous.to_string()));
        }
        queries.push(("generated", generated));
        let mut sql = String::new();
        for _ in 0..7 {
            for (_, query) in &queries {
                sql.push_str(&format!("EXPLAIN ANALYZE {query};\n"));
            }
        }
        let output = execute_vertica_target(setup, &sql).unwrap();
        let times = output
            .lines()
            .filter_map(|line| {
                line.split_once("Total Time: ")
                    .and_then(|(_, t)| t.split_once('s'))
                    .and_then(|(t, _)| t.parse::<f64>().ok())
            })
            .collect::<Vec<_>>();
        assert_eq!(times.len(), 7 * queries.len(), "{output}");
        for (index, (name, query)) in queries.iter().enumerate() {
            let mut warm = times
                .iter()
                .skip(queries.len() + index)
                .step_by(queries.len())
                .copied()
                .collect::<Vec<_>>();
            warm.sort_by(f64::total_cmp);
            let median = (warm[2] + warm[3]) / 2.0;
            println!("{label}/{name}: median={median:.6}s, warm_samples={warm:?}");
            println!("SQL: {query}");
        }
    }
}
