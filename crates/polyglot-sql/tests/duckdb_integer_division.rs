//! DuckDB `//` integer division parses to `IntDiv` and round-trips.
use polyglot_sql::{transpile, DialectType};

#[test]
fn duckdb_int_div_round_trips() {
    let out = transpile(
        "SELECT 7 // 2 AS v",
        DialectType::DuckDB,
        DialectType::DuckDB,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 7 // 2 AS v"]);
}

#[test]
fn duckdb_int_div_transpiles_like_other_integer_division() {
    // Same lowering the existing MySQL `DIV` operator gets for these targets.
    for target in [DialectType::PostgreSQL, DialectType::BigQuery] {
        let out = transpile("SELECT 7 // 2 AS v", DialectType::DuckDB, target).unwrap();
        assert_eq!(out, vec!["SELECT DIV(7, 2) AS v"], "target {target:?}");
    }
}

#[test]
fn duckdb_int_div_binds_tighter_than_addition() {
    let out = transpile(
        "SELECT 1 + 7 // 2 AS v",
        DialectType::DuckDB,
        DialectType::DuckDB,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 1 + 7 // 2 AS v"]);
}

#[test]
fn plain_division_is_unchanged_in_duckdb_and_elsewhere() {
    let out = transpile(
        "SELECT 7 / 2 AS v",
        DialectType::DuckDB,
        DialectType::DuckDB,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 7 / 2 AS v"]);
    let out = transpile(
        "SELECT 7 / 2 AS v",
        DialectType::PostgreSQL,
        DialectType::PostgreSQL,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 7 / 2 AS v"]);
}
