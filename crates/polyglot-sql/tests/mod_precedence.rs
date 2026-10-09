//! `MOD(a, b)` lowered to the infix `%` must keep the call's grouping when it
//! is an operand of another operator.
use polyglot_sql::{transpile, DialectType};

#[test]
fn mod_lowered_to_percent_keeps_its_grouping_under_multiplication() {
    for target in [
        DialectType::PostgreSQL,
        DialectType::DuckDB,
        DialectType::DataFusion,
        DialectType::MySQL,
    ] {
        let out = transpile("SELECT 2 * MOD(5, 3) AS v", DialectType::Snowflake, target).unwrap();
        assert_eq!(out, vec!["SELECT 2 * (5 % 3) AS v"], "target {target:?}");
    }
}

#[test]
fn mod_lowered_to_percent_as_a_left_operand_needs_no_parentheses() {
    // `*` and `%` share precedence and associate left, so `7 % 4 * 2` already
    // groups as `(7 % 4) * 2`.
    let out = transpile(
        "SELECT MOD(7, 4) * 2 AS v",
        DialectType::Snowflake,
        DialectType::PostgreSQL,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 7 % 4 * 2 AS v"]);
}

#[test]
fn top_level_mod_is_not_parenthesized() {
    let out = transpile(
        "SELECT MOD(a, 7) AS m FROM t",
        DialectType::PostgreSQL,
        DialectType::TSQL,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT a % 7 AS m FROM t"]);
}

#[test]
fn dialects_that_keep_the_function_form_are_unchanged() {
    let out = transpile(
        "SELECT 2 * MOD(5, 3) AS v",
        DialectType::Snowflake,
        DialectType::Oracle,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 2 * MOD(5, 3) AS v"]);
}

#[test]
fn datafusion_renders_mod_as_percent_with_grouping() {
    // DataFusion has no MOD function, only `%`, so both the generator path
    // (a parsed ModFunc) and the normalization path (a generic MOD call from
    // BigQuery) must render the operator, keeping the call's grouping.
    for (source, sql, expected) in [
        (
            DialectType::DuckDB,
            "SELECT 2 * MOD(5, 3) AS v",
            "SELECT 2 * (5 % 3) AS v",
        ),
        (
            DialectType::BigQuery,
            "SELECT 2 * MOD(5 + 1, 4) AS v",
            "SELECT 2 * ((5 + 1) % 4) AS v",
        ),
        (
            DialectType::BigQuery,
            "SELECT MOD(7, 4) * 2 AS v",
            "SELECT 7 % 4 * 2 AS v",
        ),
    ] {
        assert_eq!(
            transpile(sql, source, DialectType::DataFusion).unwrap(),
            vec![expected],
            "{source:?}: {sql}"
        );
    }
}

#[test]
fn mod_preserves_unary_and_argument_grouping_through_both_lowering_paths() {
    for source in [DialectType::DuckDB, DialectType::BigQuery] {
        for target in [DialectType::DuckDB, DialectType::DataFusion] {
            for (sql, expected) in [
                ("SELECT 2 * -MOD(5, 3)", "SELECT 2 * -(5 % 3)"),
                ("SELECT 80.0 / -MOD(7, 4)", "SELECT 80.0 / -(7 % 4)"),
                ("SELECT -MOD(-7, 4)", "SELECT -(-7 % 4)"),
                ("SELECT MOD(7, -MOD(9, 4))", "SELECT 7 % -(9 % 4)"),
                ("SELECT MOD(7 | 8, 4)", "SELECT (7 | 8) % 4"),
                ("SELECT MOD(7, 4 | 8)", "SELECT 7 % (4 | 8)"),
                ("SELECT MOD(7 & 3, 2)", "SELECT (7 & 3) % 2"),
                ("SELECT MOD(7, 1 << 2)", "SELECT 7 % (1 << 2)"),
                ("SELECT MOD(7, 8 >> 2)", "SELECT 7 % (8 >> 2)"),
            ] {
                assert_eq!(
                    transpile(sql, source, target).unwrap(),
                    vec![expected],
                    "{source:?} -> {target:?}: {sql}",
                );
            }
        }
    }
}

#[test]
fn mod_under_unary_operators_keeps_function_form_when_supported() {
    assert_eq!(
        transpile(
            "SELECT 2 * -MOD(5, 3)",
            DialectType::DuckDB,
            DialectType::Oracle,
        )
        .unwrap(),
        vec!["SELECT 2 * -MOD(5, 3)"],
    );
}

#[test]
fn mod_keeps_grouping_under_bitwise_not() {
    assert_eq!(
        transpile(
            "SELECT ~MOD(7, 4)",
            DialectType::DuckDB,
            DialectType::DuckDB,
        )
        .unwrap(),
        vec!["SELECT ~(7 % 4)"],
    );
}

#[test]
fn native_integer_division_groups_mod_operands() {
    for (sql, expected) in [
        ("SELECT 100 // MOD(7, 4)", "SELECT 100 // (7 % 4)"),
        ("SELECT 100 // -MOD(7, 4)", "SELECT 100 // -(7 % 4)"),
        ("SELECT MOD(7, 4) // 2", "SELECT 7 % 4 // 2"),
        ("SELECT MOD(7, 9 // 4)", "SELECT 7 % (9 // 4)"),
        ("SELECT MOD(9 // 4, 2)", "SELECT (9 // 4) % 2"),
    ] {
        assert_eq!(
            transpile(sql, DialectType::DuckDB, DialectType::DuckDB).unwrap(),
            vec![expected],
            "{sql}",
        );
    }
}

#[test]
fn integer_division_functions_lowered_to_operators_keep_grouping() {
    for (target, operator) in [
        (DialectType::DuckDB, "//"),
        (DialectType::Vertica, "//"),
        (DialectType::Hive, "DIV"),
        (DialectType::Spark, "DIV"),
        (DialectType::Databricks, "DIV"),
    ] {
        assert_eq!(
            transpile("SELECT 2 * DIV(7, 3)", DialectType::BigQuery, target).unwrap(),
            vec![format!("SELECT 2 * (7 {operator} 3)")],
            "{target:?}",
        );
        let divisor = if target == DialectType::Vertica {
            "MOD(7, 4)"
        } else {
            "(7 % 4)"
        };
        assert_eq!(
            transpile("SELECT DIV(100, MOD(7, 4))", DialectType::BigQuery, target).unwrap(),
            vec![format!("SELECT 100 {operator} {divisor}")],
            "{target:?}",
        );
    }
}
