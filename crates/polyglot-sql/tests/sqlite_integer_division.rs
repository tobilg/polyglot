//! SQLite and PostgreSQL truncate the division of two integers, so their `/`
//! is kept as is; a cast is only needed from dialects that divide as floats.
use polyglot_sql::{transpile, DialectType};

#[test]
fn integer_division_is_kept_from_truncating_dialects() {
    for (read, write) in [
        (DialectType::SQLite, DialectType::SQLite),
        (DialectType::PostgreSQL, DialectType::SQLite),
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
    ] {
        let out = transpile(sql, DialectType::PostgreSQL, DialectType::SQLite).unwrap();
        assert_eq!(out, vec![want], "{sql}");
    }
}
