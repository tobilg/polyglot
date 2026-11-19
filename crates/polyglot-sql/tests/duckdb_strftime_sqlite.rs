//! DuckDB accepts STRFTIME(value, format) and STRFTIME(format, value); SQLite takes
//! STRFTIME(format, value).
use polyglot_sql::{transpile, DialectType};

fn duckdb_to_sqlite(sql: &str) -> String {
    transpile(sql, DialectType::DuckDB, DialectType::SQLite)
        .unwrap()
        .remove(0)
}

#[test]
fn duckdb_strftime_value_first_to_sqlite() {
    assert_eq!(
        duckdb_to_sqlite("SELECT STRFTIME(d, '%Y-%m') FROM t"),
        "SELECT STRFTIME('%Y-%m', d) FROM t"
    );
    assert_eq!(
        duckdb_to_sqlite("SELECT STRFTIME(DATE '2024-01-15', '%Y-%m')"),
        "SELECT STRFTIME('%Y-%m', '2024-01-15')"
    );
    assert_eq!(
        duckdb_to_sqlite("SELECT STRFTIME(ts, '%Y' || '-%m') FROM t"),
        "SELECT STRFTIME('%Y' || '-%m', ts) FROM t"
    );
}

#[test]
fn duckdb_strftime_format_first_to_sqlite() {
    assert_eq!(
        duckdb_to_sqlite("SELECT STRFTIME('%Y-%m', d) FROM t"),
        "SELECT STRFTIME('%Y-%m', d) FROM t"
    );
    assert_eq!(
        duckdb_to_sqlite("SELECT STRFTIME('%Y-%m', DATE '2024-01-15')"),
        "SELECT STRFTIME('%Y-%m', '2024-01-15')"
    );
    assert_eq!(
        duckdb_to_sqlite("SELECT STRFTIME('%Y' || '-%m', ts) FROM t"),
        "SELECT STRFTIME('%Y' || '-%m', ts) FROM t"
    );
    assert_eq!(
        duckdb_to_sqlite("SELECT STRFTIME(CONCAT('%Y', '-%m'), d) FROM t"),
        "SELECT STRFTIME(CONCAT('%Y', '-%m'), d) FROM t"
    );
}

#[test]
fn sqlite_strftime_round_trips() {
    let out = transpile(
        "SELECT STRFTIME('%Y-%m', d) FROM t",
        DialectType::SQLite,
        DialectType::SQLite,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT STRFTIME('%Y-%m', d) FROM t"]);
}
