//! SQLite and PostgreSQL truncate the division of two integers, so their `/`
//! is kept as is; a cast is only needed from dialects that divide as floats.
use polyglot_sql::{transpile, DialectType};

#[test]
fn integer_division_is_kept_from_truncating_dialects() {
    for (read, write) in [
        (DialectType::SQLite, DialectType::SQLite),
        (DialectType::PostgreSQL, DialectType::SQLite),
        (DialectType::DataFusion, DialectType::SQLite),
        (DialectType::SQLite, DialectType::PostgreSQL),
    ] {
        let out = transpile("SELECT 7 / 2, a / b FROM t", read, write).unwrap();
        assert_eq!(
            out,
            vec!["SELECT 7 / 2, a / b FROM t"],
            "{read:?} -> {write:?}"
        );
    }
}

#[test]
fn fractional_division_to_sqlite_casts() {
    for read in [
        DialectType::Generic,
        DialectType::BigQuery,
        DialectType::Snowflake,
        DialectType::MySQL,
        DialectType::DuckDB,
    ] {
        let out = transpile("SELECT 7 / 2, a / b FROM t", read, DialectType::SQLite).unwrap();
        assert_eq!(
            out,
            vec!["SELECT CAST(7 AS REAL) / 2, CAST(a AS REAL) / b FROM t"],
            "{read:?}"
        );
    }
}

#[test]
fn bigquery_safe_divide_to_sqlite_casts() {
    let out = transpile(
        "SELECT SAFE_DIVIDE(7, 2)",
        DialectType::BigQuery,
        DialectType::SQLite,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT IIF(2 <> 0, CAST(7 AS REAL) / 2, NULL)"]);
}

#[test]
fn existing_nullif_denominator_still_casts() {
    for read in [DialectType::MySQL, DialectType::DuckDB] {
        let out = transpile("SELECT 7 / NULLIF(2, 0)", read, DialectType::SQLite).unwrap();
        assert_eq!(
            out,
            vec!["SELECT CAST(7 AS REAL) / NULLIF(2, 0)"],
            "{read:?}"
        );
        let out = transpile("SELECT 7 / NULLIF(2, 0)", read, DialectType::PostgreSQL).unwrap();
        assert_eq!(
            out,
            vec!["SELECT CAST(7 AS DOUBLE PRECISION) / NULLIF(2, 0)"],
            "{read:?}"
        );
    }
}

#[test]
fn decimal_division_from_truncating_dialects_casts() {
    for (sql, want) in [
        (
            "SELECT CAST(7 AS NUMERIC) / 2",
            "SELECT CAST(CAST(7 AS DECIMAL) AS REAL) / 2",
        ),
        (
            "SELECT 7 / CAST(2 AS DECIMAL(10, 2))",
            "SELECT CAST(7 AS REAL) / CAST(2 AS DECIMAL(10, 2))",
        ),
        (
            "SELECT (CAST(7 AS NUMERIC)) / 2",
            "SELECT CAST((CAST(7 AS DECIMAL)) AS REAL) / 2",
        ),
        (
            "SELECT 7 / NULLIF(2, 0.0)",
            "SELECT CAST(7 AS REAL) / NULLIF(2, 0.0)",
        ),
        (
            "SELECT 7 / NULLIF(CAST(2 AS NUMERIC), 0)",
            "SELECT CAST(7 AS REAL) / NULLIF(CAST(2 AS DECIMAL), 0)",
        ),
    ] {
        let out = transpile(sql, DialectType::PostgreSQL, DialectType::SQLite).unwrap();
        assert_eq!(out, vec![want], "{sql}");
    }
}

#[test]
fn projected_decimal_division() {
    for (sql, quotient) in [
        (
            "WITH x AS (SELECT CAST(7 AS NUMERIC) AS n) SELECT n / 2 FROM x",
            "CAST(n AS REAL) / 2",
        ),
        (
            "SELECT q.n / 2 FROM (SELECT CAST(7 AS NUMERIC) AS n) AS q",
            "CAST(q.n AS REAL) / 2",
        ),
        (
            "WITH x AS (SELECT 7 AS n) SELECT n / 2 FROM x",
            "SELECT n / 2 FROM x",
        ),
    ] {
        let out = transpile(sql, DialectType::PostgreSQL, DialectType::SQLite).unwrap();
        assert!(out[0].contains(quotient), "{}", out[0]);
    }
}

#[test]
fn tsql_division_uses_source_result_type() {
    for (sql, want) in [
        ("SELECT 7 / NULLIF(2, 0.0)", "SELECT 7 / NULLIF(2, 0.0)"),
        ("SELECT AVG(7) / 2", "SELECT CAST(AVG(7) AS INTEGER) / 2"),
        (
            "WITH q AS (SELECT AVG(7) AS n) SELECT n / 2 FROM q",
            "WITH q AS (SELECT CAST(AVG(7) AS INTEGER) AS n) SELECT n / 2 FROM q",
        ),
    ] {
        assert_eq!(
            transpile(sql, DialectType::TSQL, DialectType::SQLite).unwrap(),
            [want]
        );
    }
}

#[test]
fn sqlite_to_duckdb_uses_operand_sensitive_division() {
    let out = transpile(
        "SELECT 7 / 2, 7.0 / 2, 7 / 0",
        DialectType::SQLite,
        DialectType::DuckDB,
    )
    .unwrap();
    assert_eq!(
        out,
        ["SELECT 7 // NULLIF(2, 0), 7.0 // NULLIF(2, 0), 7 // NULLIF(0, 0)"]
    );
}
