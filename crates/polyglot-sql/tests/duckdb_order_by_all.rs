//! DuckDB `ORDER BY ALL`: round-trips unquoted for DuckDB, expands to
//! positional `ORDER BY 1..n` for targets without it, and is rejected when the
//! projection is `*` (the column count is unknown).
use polyglot_sql::{transpile, DialectType};

#[test]
fn order_by_all_round_trips_unquoted_in_duckdb() {
    let out = transpile(
        "SELECT a, b FROM t ORDER BY ALL",
        DialectType::DuckDB,
        DialectType::DuckDB,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT a, b FROM t ORDER BY ALL"]);
}

#[test]
fn order_by_all_expands_to_positional_for_other_targets() {
    for target in [
        DialectType::PostgreSQL,
        DialectType::Snowflake,
        DialectType::DataFusion,
    ] {
        let out = transpile(
            "SELECT a, b FROM t ORDER BY ALL",
            DialectType::DuckDB,
            target,
        )
        .unwrap();
        assert_eq!(
            out,
            vec!["SELECT a, b FROM t ORDER BY 1, 2"],
            "target {target:?}"
        );
    }
}

#[test]
fn order_by_all_expansion_keeps_the_direction() {
    // DuckDB sorts NULLs last for DESC too, whereas PostgreSQL defaults DESC to
    // NULLS FIRST, so the expansion carries the explicit null order across.
    let out = transpile(
        "SELECT a, b FROM t ORDER BY ALL DESC",
        DialectType::DuckDB,
        DialectType::PostgreSQL,
    )
    .unwrap();
    assert_eq!(
        out,
        vec!["SELECT a, b FROM t ORDER BY 1 DESC NULLS LAST, 2 DESC NULLS LAST"]
    );
}

#[test]
fn order_by_all_expands_inside_subqueries() {
    let out = transpile(
        "SELECT a FROM t WHERE a IN (SELECT x, y FROM u ORDER BY ALL LIMIT 1)",
        DialectType::DuckDB,
        DialectType::PostgreSQL,
    )
    .unwrap();
    assert_eq!(
        out,
        vec!["SELECT a FROM t WHERE a IN (SELECT x, y FROM u ORDER BY 1, 2 LIMIT 1)"]
    );
}

#[test]
fn order_by_all_over_star_is_unsupported_for_other_targets() {
    let err = transpile(
        "SELECT * FROM t ORDER BY ALL",
        DialectType::DuckDB,
        DialectType::PostgreSQL,
    )
    .unwrap_err();
    assert!(err.to_string().contains("ORDER BY ALL"), "got: {err}");
}

#[test]
fn a_quoted_column_named_all_is_not_expanded() {
    let out = transpile(
        "SELECT a, b FROM t ORDER BY \"all\"",
        DialectType::DuckDB,
        DialectType::PostgreSQL,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT a, b FROM t ORDER BY \"all\""]);
}
