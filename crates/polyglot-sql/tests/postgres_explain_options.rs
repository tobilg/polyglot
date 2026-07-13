//! Regression tests for PostgreSQL's parenthesized `EXPLAIN ( option [, ...] )` form.
//!
//! PostgreSQL (and the PostgreSQL-compatible dialects CockroachDB, RisingWave and
//! Materialize) accept `EXPLAIN ( VERBOSE, COSTS OFF, FORMAT JSON, ... ) <statement>`.
//! These options are planner/output directives with no cross-dialect meaning, so they
//! are parsed and discarded - consistent with the way `EXPLAIN` is normalized to
//! `DESCRIBE`. The `ANALYZE` option is folded into the `DESCRIBE` style so that
//! `EXPLAIN (ANALYZE) ...` matches bare `EXPLAIN ANALYZE ...`.

use polyglot_sql::{transpile, DialectType};

fn pg(sql: &str) -> String {
    transpile(sql, DialectType::PostgreSQL, DialectType::PostgreSQL)
        .expect("PostgreSQL EXPLAIN should transpile")
        .join("; ")
}

#[test]
fn parenthesized_options_are_parsed_and_discarded() {
    let cases = [
        ("EXPLAIN (VERBOSE, COSTS OFF) SELECT 1", "DESCRIBE SELECT 1"),
        ("EXPLAIN (FORMAT json) SELECT 1", "DESCRIBE SELECT 1"),
        (
            "EXPLAIN (VERBOSE, COSTS OFF) SELECT max(unique1) FROM tenk1",
            "DESCRIBE SELECT MAX(unique1) FROM tenk1",
        ),
        (
            "EXPLAIN (BUFFERS, SETTINGS, WAL, TIMING OFF, SUMMARY, GENERIC_PLAN) SELECT 1",
            "DESCRIBE SELECT 1",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(pg(input), expected, "input: {input}");
    }
}

#[test]
fn analyze_option_folds_into_describe_style() {
    // ANALYZE (with or without a truthy argument, in any position) behaves like bare
    // `EXPLAIN ANALYZE`.
    let cases = [
        ("EXPLAIN (ANALYZE) SELECT 1", "DESCRIBE ANALYZE SELECT 1"),
        (
            "EXPLAIN (ANALYZE true) SELECT 1",
            "DESCRIBE ANALYZE SELECT 1",
        ),
        ("EXPLAIN (ANALYZE ON) SELECT 1", "DESCRIBE ANALYZE SELECT 1"),
        (
            "EXPLAIN (COSTS OFF, ANALYZE) SELECT 1",
            "DESCRIBE ANALYZE SELECT 1",
        ),
        (
            "EXPLAIN (ANALYZE true, BUFFERS) SELECT max(a) FROM t",
            "DESCRIBE ANALYZE SELECT MAX(a) FROM t",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(pg(input), expected, "input: {input}");
    }
}

#[test]
fn analyze_false_is_not_folded() {
    assert_eq!(pg("EXPLAIN (ANALYZE false) SELECT 1"), "DESCRIBE SELECT 1");
    assert_eq!(pg("EXPLAIN (ANALYZE off) SELECT 1"), "DESCRIBE SELECT 1");
}

#[test]
fn bare_explain_is_unchanged() {
    assert_eq!(pg("EXPLAIN SELECT 1"), "DESCRIBE SELECT 1");
    assert_eq!(pg("EXPLAIN ANALYZE SELECT 1"), "DESCRIBE ANALYZE SELECT 1");
}

#[test]
fn parenthesized_subquery_is_not_treated_as_options() {
    // `EXPLAIN (SELECT ...)` must still be parsed as a parenthesized subquery target,
    // not as an option list.
    assert_eq!(pg("EXPLAIN (SELECT 1)"), "DESCRIBE (SELECT 1)");
}

#[test]
fn options_transpile_to_clickhouse_without_error() {
    // The primary motivation: PostgreSQL EXPLAIN statements must reach a target dialect
    // (here ClickHouse) instead of failing at parse time.
    let out = transpile(
        "EXPLAIN (VERBOSE, COSTS OFF) SELECT max(unique1) FROM tenk1",
        DialectType::PostgreSQL,
        DialectType::ClickHouse,
    )
    .expect("PostgreSQL EXPLAIN (options) should transpile to ClickHouse");
    assert_eq!(out, vec!["DESCRIBE SELECT max(unique1) FROM tenk1"]);
}

#[test]
fn options_accepted_for_postgres_compatible_dialects() {
    // CockroachDB / RisingWave / Materialize share the parenthesized EXPLAIN syntax.
    for read in [
        DialectType::CockroachDB,
        DialectType::RisingWave,
        DialectType::Materialize,
    ] {
        let out = transpile("EXPLAIN (VERBOSE) SELECT 1", read, read)
            .unwrap_or_else(|e| panic!("{read:?} EXPLAIN (options) should transpile: {e}"));
        assert_eq!(out.len(), 1, "{read:?}");
    }
}
