//! DuckDB's STRFTIME(value, format) becomes SQLite's STRFTIME(format, value).
use polyglot_sql::{transpile, DialectType};

#[test]
fn duckdb_strftime_swaps_arguments_for_sqlite() {
    let out = transpile(
        "SELECT STRFTIME(d, '%Y-%m') FROM t",
        DialectType::DuckDB,
        DialectType::SQLite,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT STRFTIME('%Y-%m', d) FROM t"]);
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
