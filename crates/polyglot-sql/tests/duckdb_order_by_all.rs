//! DuckDB `ORDER BY ALL`: round-trips unquoted for dialects with native
//! support, expands to positional `ORDER BY 1..n` for dialects without it,
//! and is rejected when the column count can't be known statically (`*` or
//! `COLUMNS(...)`). Also covers that the bare `ALL` keyword never leaks into
//! or out of ordinary identifier handling, that the positional expansion
//! survives the `DISTINCT ON` window-function rewrite, and that NULL
//! ordering is preserved.
use polyglot_sql::{generate, parse_one, rename_columns, DialectType};

fn transpile(sql: &str, source: DialectType, target: DialectType) -> String {
    polyglot_sql::transpile(sql, source, target)
        .unwrap()
        .remove(0)
}

#[test]
fn order_by_all_round_trips_unquoted_in_duckdb() {
    assert_eq!(
        transpile(
            "SELECT a, b FROM t ORDER BY ALL",
            DialectType::DuckDB,
            DialectType::DuckDB
        ),
        "SELECT a, b FROM t ORDER BY ALL"
    );
}

#[test]
fn order_by_all_passes_through_for_native_targets() {
    // Snowflake, ClickHouse, and Databricks natively support ORDER BY ALL, so
    // the keyword is kept rather than expanded to positions -- including over
    // `*`, where expansion would have been impossible. DuckDB's ALL sorts
    // NULLs last in both directions; the NULLS clause is spelled out only
    // where the target's own default differs (Databricks ASC defaults to
    // NULLs first; Snowflake DESC does too; ClickHouse already matches).
    let cases = [
        (DialectType::Snowflake, "ALL", "ALL DESC NULLS LAST"),
        (DialectType::ClickHouse, "ALL", "ALL DESC"),
        (DialectType::Databricks, "ALL NULLS LAST", "ALL DESC"),
        (DialectType::Spark, "ALL NULLS LAST", "ALL DESC"),
    ];
    for (target, asc, desc) in cases {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t ORDER BY ALL",
                DialectType::DuckDB,
                target
            ),
            format!("SELECT a, b FROM t ORDER BY {asc}"),
            "target {target:?}"
        );
        assert_eq!(
            transpile(
                "SELECT a, b FROM t ORDER BY ALL DESC",
                DialectType::DuckDB,
                target
            ),
            format!("SELECT a, b FROM t ORDER BY {desc}"),
            "target {target:?}"
        );
        assert_eq!(
            transpile("SELECT * FROM t ORDER BY ALL", DialectType::DuckDB, target),
            format!("SELECT * FROM t ORDER BY {asc}"),
            "target {target:?}"
        );
    }
}

#[test]
fn order_by_all_expands_from_any_native_source() {
    // The marker must be expanded whenever the *source* has native ORDER BY
    // ALL and the target doesn't -- not only for DuckDB -- otherwise the bare
    // keyword leaks out as a nonexistent `"ALL"` column. Each source's own
    // NULL default carries over (Databricks sorts NULLs first for ASC, which
    // PostgreSQL doesn't).
    let cases = [
        (DialectType::Snowflake, "1, 2"),
        (DialectType::ClickHouse, "1, 2"),
        (DialectType::Databricks, "1 NULLS FIRST, 2 NULLS FIRST"),
        (DialectType::Spark, "1 NULLS FIRST, 2 NULLS FIRST"),
    ];
    for (source, expected) in cases {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t ORDER BY ALL",
                source,
                DialectType::PostgreSQL
            ),
            format!("SELECT a, b FROM t ORDER BY {expected}"),
            "source {source:?}"
        );
    }
}

#[test]
fn order_by_all_expands_to_positional_for_targets_without_native_support() {
    // PostgreSQL's own ASC default is also NULLS LAST, so (like DataFusion,
    // covered separately below) the explicit ordering DuckDB's ALL implies
    // is elided rather than printed redundantly.
    assert_eq!(
        transpile(
            "SELECT a, b FROM t ORDER BY ALL",
            DialectType::DuckDB,
            DialectType::PostgreSQL
        ),
        "SELECT a, b FROM t ORDER BY 1, 2"
    );
}

#[test]
fn order_by_all_expansion_keeps_the_direction() {
    // DuckDB sorts NULLs last for DESC too, whereas PostgreSQL defaults DESC to
    // NULLS FIRST, so the expansion carries the explicit null order across.
    assert_eq!(
        transpile(
            "SELECT a, b FROM t ORDER BY ALL DESC",
            DialectType::DuckDB,
            DialectType::PostgreSQL
        ),
        "SELECT a, b FROM t ORDER BY 1 DESC NULLS LAST, 2 DESC NULLS LAST"
    );
}

#[test]
fn order_by_all_reports_unsupported_when_mysql_cant_preserve_null_order() {
    // DuckDB's implicit default is NULLs last in both directions. MySQL has
    // no NULLS FIRST/LAST syntax, and its own ASC default is NULLs *first*
    // (NULL sorts as the smallest value) -- the opposite of what DuckDB's
    // ORDER BY ALL needs here. Silently emitting plain `ORDER BY 1` would
    // change which rows a LIMIT selects, so this must be reported as
    // unsupported rather than transpiled incorrectly.
    let err = polyglot_sql::transpile(
        "SELECT a FROM t ORDER BY ALL",
        DialectType::DuckDB,
        DialectType::MySQL,
    )
    .unwrap_err();
    assert!(err.to_string().contains("NULL"), "got: {err}");
}

#[test]
fn order_by_all_expansion_omits_nulls_clause_for_datafusion() {
    // The cross-dialect NULL-ordering pass models DataFusion, like DuckDB, as
    // sorting NULLs last in both directions, so no clause is needed.
    assert_eq!(
        transpile(
            "SELECT a, b FROM t ORDER BY ALL",
            DialectType::DuckDB,
            DialectType::DataFusion
        ),
        "SELECT a, b FROM t ORDER BY 1, 2"
    );
}

#[test]
fn order_by_all_expands_inside_subqueries() {
    assert_eq!(
        transpile(
            "SELECT a FROM t WHERE a IN (SELECT x, y FROM u ORDER BY ALL LIMIT 1)",
            DialectType::DuckDB,
            DialectType::PostgreSQL
        ),
        "SELECT a FROM t WHERE a IN (SELECT x, y FROM u ORDER BY 1, 2 LIMIT 1)"
    );
}

#[test]
fn order_by_all_over_star_is_unsupported_for_other_targets() {
    let err = polyglot_sql::transpile(
        "SELECT * FROM t ORDER BY ALL",
        DialectType::DuckDB,
        DialectType::PostgreSQL,
    )
    .unwrap_err();
    assert!(err.to_string().contains("ORDER BY ALL"), "got: {err}");
}

#[test]
fn order_by_all_over_columns_macro_is_unsupported_for_other_targets() {
    // COLUMNS(...) can expand to any number of output columns, so (like `*`)
    // the positional count can't be derived statically.
    let err = polyglot_sql::transpile(
        "SELECT COLUMNS('a|b') FROM t ORDER BY ALL",
        DialectType::DuckDB,
        DialectType::PostgreSQL,
    )
    .unwrap_err();
    assert!(err.to_string().contains("ORDER BY ALL"), "got: {err}");
}

#[test]
fn a_quoted_column_named_all_is_not_expanded() {
    assert_eq!(
        transpile(
            "SELECT a, b FROM t ORDER BY \"all\"",
            DialectType::DuckDB,
            DialectType::PostgreSQL
        ),
        "SELECT a, b FROM t ORDER BY \"all\""
    );
}

#[test]
fn an_ordinary_column_renamed_to_all_is_still_quoted() {
    // Renaming a column to the literal name "all" must still round-trip as a
    // properly quoted identifier in DuckDB output — it must not be emitted
    // bare, which DuckDB would then parse back as the ORDER BY ALL keyword.
    use std::collections::HashMap;
    let expr = parse_one("SELECT x FROM t", DialectType::DuckDB).unwrap();
    let mapping = HashMap::from([("x".to_string(), "all".to_string())]);
    let renamed = rename_columns(expr, &mapping);
    let out = generate(&renamed, DialectType::DuckDB).unwrap();
    assert_eq!(out, "SELECT \"all\" FROM t");
}

#[test]
fn order_by_all_on_union_expands_using_left_side_width() {
    assert_eq!(
        transpile(
            "SELECT a, b FROM t UNION ALL SELECT c, d FROM u ORDER BY ALL",
            DialectType::DuckDB,
            DialectType::PostgreSQL
        ),
        "SELECT a, b FROM t UNION ALL SELECT c, d FROM u ORDER BY 1, 2"
    );
}

#[test]
fn order_by_all_on_parenthesized_union_operands_expands() {
    // Parenthesized set-operation operands parse as subqueries; the width
    // lookup must see through them rather than treat the count as unknown.
    assert_eq!(
        transpile(
            "(SELECT a, b FROM t) UNION (SELECT c, d FROM u) ORDER BY ALL",
            DialectType::DuckDB,
            DialectType::PostgreSQL
        ),
        "(SELECT a, b FROM t) UNION (SELECT c, d FROM u) ORDER BY 1, 2"
    );
}

#[test]
fn order_by_all_on_union_passes_through_for_native_targets() {
    assert_eq!(
        transpile(
            "SELECT a, b FROM t UNION ALL SELECT c, d FROM u ORDER BY ALL",
            DialectType::DuckDB,
            DialectType::Snowflake
        ),
        "SELECT a, b FROM t UNION ALL SELECT c, d FROM u ORDER BY ALL"
    );
}

#[test]
fn order_by_all_on_union_over_star_is_unsupported() {
    let err = polyglot_sql::transpile(
        "SELECT * FROM t UNION SELECT * FROM u ORDER BY ALL",
        DialectType::DuckDB,
        DialectType::PostgreSQL,
    )
    .unwrap_err();
    assert!(err.to_string().contains("ORDER BY ALL"), "got: {err}");
}

#[test]
fn order_by_all_survives_distinct_on_rewrite() {
    // SQLite has no DISTINCT ON, so it's emulated with a ROW_NUMBER() window.
    // The positional ORDER BY produced by expanding ALL must be resolved to
    // the actual projected columns before landing inside that window's own
    // ORDER BY, where a bare "1"/"2" would mean a literal constant, not a
    // positional column reference, and silently change the result set.
    let out = transpile(
        "SELECT DISTINCT ON (a) a, b FROM t ORDER BY ALL",
        DialectType::DuckDB,
        DialectType::SQLite,
    );
    assert!(!out.contains("ORDER BY 1"), "got: {out}");
    assert!(out.contains("PARTITION BY a"), "got: {out}");
    assert!(out.contains("ORDER BY a"), "got: {out}");
}

fn strict_transpile(
    sql: &str,
    source: DialectType,
    target: DialectType,
) -> polyglot_sql::Result<Vec<String>> {
    polyglot_sql::Dialect::get(source).transpile_with(
        sql,
        &polyglot_sql::Dialect::get(target),
        polyglot_sql::TranspileOptions::strict(),
    )
}

#[test]
fn renaming_an_ordered_column_to_all_does_not_change_the_sort_keys() {
    use std::collections::HashMap;
    let mapping = HashMap::from([("x".to_string(), "all".to_string())]);
    for sql in [
        "SELECT b, x FROM t ORDER BY x",
        "SELECT b, x, ROW_NUMBER() OVER (ORDER BY x) AS n FROM t ORDER BY x",
    ] {
        let expr = parse_one(sql, DialectType::DuckDB).unwrap();
        let renamed = rename_columns(expr, &mapping);
        let output = generate(&renamed, DialectType::DuckDB).unwrap();
        assert!(output.contains("ORDER BY \"all\""), "{output}");
        assert!(!output.contains("ORDER BY ALL"), "{output}");
    }
}

#[test]
fn keyword_survives_serialization_renaming_and_identifier_quoting() {
    use std::collections::HashMap;
    let expr = parse_one("SELECT a, b FROM t ORDER BY ALL", DialectType::DuckDB).unwrap();
    let json = serde_json::to_string(&expr).unwrap();
    let expr = serde_json::from_str(&json).unwrap();
    let mapping = HashMap::from([("ALL".to_string(), "different".to_string())]);
    let renamed = rename_columns(expr, &mapping);
    let out = polyglot_sql::Dialect::get(DialectType::DuckDB)
        .generate_with_identify(&renamed)
        .unwrap();
    assert_eq!(out, "SELECT \"a\", \"b\" FROM \"t\" ORDER BY ALL");
}

#[test]
fn qualified_and_quoted_all_columns_remain_columns() {
    for column in ["t.all", "\"all\""] {
        let sql = format!("SELECT a, b FROM t ORDER BY {column}");
        for target in [
            DialectType::DuckDB,
            DialectType::PostgreSQL,
            DialectType::Snowflake,
        ] {
            let out = strict_transpile(&sql, DialectType::DuckDB, target)
                .unwrap()
                .remove(0);
            assert!(!out.contains("ORDER BY ALL"), "{out}");
            assert!(!out.contains("ORDER BY 1"), "{out}");
        }
    }
}

#[test]
fn mysql_null_ordering_is_checked_for_every_native_source() {
    let modifiers = [
        "",
        "DESC",
        "NULLS FIRST",
        "NULLS LAST",
        "DESC NULLS FIRST",
        "DESC NULLS LAST",
    ];
    for source in [
        DialectType::DuckDB,
        DialectType::ClickHouse,
        DialectType::Snowflake,
        DialectType::Spark,
        DialectType::Databricks,
    ] {
        for modifier in modifiers {
            let desc = modifier.starts_with("DESC");
            let nulls_first = if modifier.contains("NULLS FIRST") {
                true
            } else if modifier.contains("NULLS LAST") {
                false
            } else {
                match source {
                    DialectType::Snowflake => desc,
                    DialectType::Spark | DialectType::Databricks => !desc,
                    _ => false,
                }
            };
            let sql = format!("SELECT a FROM t ORDER BY ALL {modifier} LIMIT 1");
            let result = strict_transpile(&sql, source, DialectType::MySQL);
            if nulls_first == !desc {
                let out = result.unwrap().remove(0);
                let direction = if desc { " DESC" } else { "" };
                assert_eq!(
                    out,
                    format!("SELECT a FROM t ORDER BY 1{direction} LIMIT 1"),
                    "source {source:?}, modifier {modifier}"
                );
            } else {
                let err = result.unwrap_err().to_string();
                assert!(err.contains("NULL"), "{source:?} {modifier}: {err}");
            }
        }
    }
}

#[test]
fn nested_column_expansions_are_rejected() {
    for projection in [
        "COLUMNS('a|b') + 1",
        "ABS(COLUMNS('a|b'))",
        "CAST(COLUMNS('a|b') AS INT)",
        "MIN(COLUMNS('a|b'))",
        "(COLUMNS('a|b')) AS n",
        "t.*",
    ] {
        let sql = format!("SELECT {projection} FROM t ORDER BY ALL");
        let err = strict_transpile(&sql, DialectType::DuckDB, DialectType::PostgreSQL)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown column count"), "{sql}: {err}");
    }
}

#[test]
fn count_star_and_scalar_subqueries_have_known_width() {
    for sql in [
        "SELECT COUNT(*) FROM t ORDER BY ALL",
        "SELECT (SELECT COUNT(*) FROM u) AS n, a FROM t ORDER BY ALL",
    ] {
        let out = strict_transpile(sql, DialectType::DuckDB, DialectType::PostgreSQL)
            .unwrap()
            .remove(0);
        assert!(out.contains("ORDER BY 1"), "{out}");
    }
}

#[test]
fn outer_parenthesized_query_ordering_is_expanded() {
    for query in [
        "(SELECT a, b FROM t)",
        "(SELECT a, b FROM t UNION ALL SELECT a, b FROM u)",
    ] {
        let sql = format!("{query} ORDER BY ALL DESC LIMIT 1");
        let out = strict_transpile(&sql, DialectType::DuckDB, DialectType::PostgreSQL)
            .unwrap()
            .remove(0);
        assert_eq!(
            out,
            format!("{query} ORDER BY 1 DESC NULLS LAST, 2 DESC NULLS LAST LIMIT 1")
        );
    }
}

#[test]
fn outer_parenthesized_star_and_union_by_name_are_rejected() {
    for sql in [
        "(SELECT * FROM t) ORDER BY ALL",
        "SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS b ORDER BY ALL",
        "(SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS b) ORDER BY ALL",
    ] {
        let err = strict_transpile(sql, DialectType::DuckDB, DialectType::PostgreSQL)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown column count"), "{sql}: {err}");
    }
}

#[test]
fn snowflake_aggregate_projections_require_expansion() {
    for projection in ["SUM(b)", "SUM(b) + 1", "COUNT(*)"] {
        let sql = format!("SELECT a, {projection} FROM t GROUP BY a ORDER BY ALL");
        let out = strict_transpile(&sql, DialectType::DuckDB, DialectType::Snowflake)
            .unwrap()
            .remove(0);
        assert_eq!(
            out,
            format!("SELECT a, {projection} FROM t GROUP BY a ORDER BY 1, 2")
        );
    }
}

#[test]
fn native_all_targets_resolve_sort_keys_before_distinct_on_emulation() {
    let sql = "SELECT DISTINCT ON (a) a, b FROM t ORDER BY ALL";
    for target in [
        DialectType::Snowflake,
        DialectType::Databricks,
        DialectType::Spark,
        DialectType::ClickHouse,
    ] {
        let out = strict_transpile(sql, DialectType::DuckDB, target)
            .unwrap()
            .remove(0);
        assert!(!out.contains("ORDER BY ALL"), "{target:?}: {out}");
        assert!(!out.contains("ORDER BY 1"), "{target:?}: {out}");
        assert!(
            out.contains("PARTITION BY a ORDER BY a"),
            "{target:?}: {out}"
        );
    }
}

#[test]
fn distinct_on_window_sort_keys_fail_explicitly_instead_of_nesting_windows() {
    for order in ["ALL", "1, 2"] {
        let sql = format!(
            "SELECT DISTINCT ON (a) a, ROW_NUMBER() OVER (ORDER BY b) AS n FROM t ORDER BY {order}"
        );
        for target in [
            DialectType::SQLite,
            DialectType::Snowflake,
            DialectType::Spark,
        ] {
            let err = strict_transpile(&sql, DialectType::DuckDB, target)
                .unwrap_err()
                .to_string();
            assert!(err.contains("window expression"), "{target:?}: {err}");
        }
        assert!(strict_transpile(&sql, DialectType::DuckDB, DialectType::DuckDB).is_ok());
    }
}

#[test]
fn expansion_preserves_explicit_ascending_direction() {
    assert_eq!(
        transpile(
            "SELECT a, b FROM t ORDER BY ALL ASC NULLS LAST",
            DialectType::DuckDB,
            DialectType::PostgreSQL
        ),
        "SELECT a, b FROM t ORDER BY 1 ASC, 2 ASC"
    );
}
