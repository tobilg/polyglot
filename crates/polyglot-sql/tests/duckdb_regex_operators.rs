use polyglot_sql::dialects::Dialect;
use polyglot_sql::error::Error;
use polyglot_sql::{
    generate, parse_one, transpile, DialectType, Expression, ExpressionWalk, TranspileOptions,
};

const FOREIGN_FULL_MATCH_TARGETS: [DialectType; 7] = [
    DialectType::PostgreSQL,
    DialectType::MySQL,
    DialectType::ClickHouse,
    DialectType::Snowflake,
    DialectType::BigQuery,
    DialectType::TSQL,
    DialectType::Fabric,
];

#[test]
fn duckdb_tilde_preserves_partial_match_for_direct_generation() {
    for (sql, expected) in [
        ("SELECT 'abc' ~ 'b' AS matched", "SELECT REGEXP_MATCHES('abc', 'b') AS matched"),
        ("SELECT 'abc' ~ 'abc' AS matched", "SELECT REGEXP_MATCHES('abc', 'abc') AS matched"),
        ("SELECT 'abc' ~ 'a.*' AS matched", "SELECT REGEXP_MATCHES('abc', 'a.*') AS matched"),
        ("SELECT 'abC' ~ 'abc' AS matched", "SELECT REGEXP_MATCHES('abC', 'abc') AS matched"),
        ("SELECT NULL ~ 'x' AS matched", "SELECT REGEXP_MATCHES(NULL, 'x') AS matched"),
        ("SELECT id FROM samples WHERE (value ~ '^[0-9]') AND id > 0", "SELECT id FROM samples WHERE (REGEXP_MATCHES(value, '^[0-9]')) AND id > 0"),
        ("SELECT id FROM samples WHERE json_extract_string(payload, '$.rating') ~ '^[0-9]'", "SELECT id FROM samples WHERE REGEXP_MATCHES(JSON_EXTRACT_STRING(payload, '$.rating'), '^[0-9]')"),
        ("SELECT id FROM samples WHERE value ~ pattern", "SELECT id FROM samples WHERE REGEXP_MATCHES(value, pattern)"),
    ] {
        let expression = parse_one(sql, DialectType::DuckDB).unwrap();
        assert_eq!(expression.count(|node| matches!(node, Expression::RegexpLike(_))), 1, "{sql}");
        let generated = generate(&expression, DialectType::DuckDB).unwrap();
        assert_eq!(generated, expected, "{sql}");
        parse_one(&generated, DialectType::DuckDB).unwrap();
    }
}

#[test]
fn duckdb_negation_wraps_the_partial_match_expression() {
    for (sql, expected) in [
        (
            "SELECT 'abc' !~ 'b'",
            "SELECT NOT REGEXP_MATCHES('abc', 'b')",
        ),
        (
            "SELECT NOT ('abc' ~ 'b')",
            "SELECT NOT (REGEXP_MATCHES('abc', 'b'))",
        ),
        (
            "SELECT NOT (NULL ~ 'x')",
            "SELECT NOT (REGEXP_MATCHES(NULL, 'x'))",
        ),
    ] {
        let expression = parse_one(sql, DialectType::DuckDB).unwrap();
        assert_eq!(
            generate(&expression, DialectType::DuckDB).unwrap(),
            expected,
            "{sql}"
        );
    }
}

#[test]
fn postgres_tilde_keeps_its_partial_match_ast() {
    for (sql, expected) in [
        ("SELECT 'abc' ~ 'b'", "SELECT 'abc' ~ 'b'"),
        ("SELECT 'abc' !~ 'b'", "SELECT NOT 'abc' ~ 'b'"),
    ] {
        let expression = parse_one(sql, DialectType::PostgreSQL).unwrap();
        assert_eq!(
            expression.count(|node| matches!(node, Expression::RegexpLike(_))),
            1,
            "{sql}"
        );
        assert_eq!(
            generate(&expression, DialectType::PostgreSQL).unwrap(),
            expected,
            "{sql}"
        );
    }
}

#[test]
fn non_tilde_regex_tokens_keep_their_existing_ast() {
    for sql in [
        "SELECT value RLIKE pattern FROM samples",
        "SELECT value REGEXP pattern FROM samples",
    ] {
        let expression = parse_one(sql, DialectType::DuckDB).unwrap();
        assert_eq!(
            expression.count(|node| matches!(node, Expression::RegexpLike(_))),
            1
        );
        assert_eq!(
            expression.count(|node| matches!(node, Expression::RegexpFullMatch(_))),
            0
        );
    }
}

#[test]
fn explicit_regex_functions_keep_their_match_semantics() {
    for (sql, expected) in [
        (
            "SELECT regexp_matches('abc', 'b')",
            "SELECT REGEXP_MATCHES('abc', 'b')",
        ),
        (
            "SELECT regexp_full_match('abc', 'b')",
            "SELECT REGEXP_FULL_MATCH('abc', 'b')",
        ),
        (
            "SELECT regexp_full_match('AbC', 'b', 'i')",
            "SELECT REGEXP_FULL_MATCH('AbC', 'b', 'i')",
        ),
        (
            "SELECT NOT regexp_full_match(value, pattern, options) FROM samples",
            "SELECT NOT REGEXP_FULL_MATCH(value, pattern, options) FROM samples",
        ),
    ] {
        let expression = parse_one(sql, DialectType::DuckDB).unwrap();
        assert_eq!(
            generate(&expression, DialectType::DuckDB).unwrap(),
            expected,
            "{sql}"
        );
        assert_eq!(
            transpile(sql, DialectType::DuckDB, DialectType::DuckDB).unwrap(),
            vec![expected],
            "{sql}"
        );
    }
}

#[test]
fn identity_transpile_preserves_partial_match_semantics() {
    for (sql, expected) in [
        ("SELECT 'abc' ~ 'b'", "SELECT REGEXP_MATCHES('abc', 'b')"),
        (
            "SELECT 'abc' !~ 'b'",
            "SELECT NOT REGEXP_MATCHES('abc', 'b')",
        ),
    ] {
        assert_eq!(
            transpile(sql, DialectType::DuckDB, DialectType::DuckDB).unwrap(),
            vec![expected],
            "{sql}"
        );
    }
}

#[test]
fn partial_match_lineage_keeps_both_operands() {
    for sql in [
        "SELECT value ~ pattern AS matched FROM samples",
        "SELECT value !~ pattern AS matched FROM samples",
    ] {
        let expression = parse_one(sql, DialectType::DuckDB).unwrap();
        let node = polyglot_sql::lineage::lineage(
            "matched",
            &expression,
            Some(DialectType::DuckDB),
            false,
        )
        .unwrap();
        let mut names: Vec<_> = node.walk().map(|node| node.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            ["matched", "samples.pattern", "samples.value"],
            "{sql}"
        );
    }
}

#[test]
fn full_match_has_no_unverified_foreign_dialect_lowering() {
    use polyglot_sql::expressions::RegexpFullMatch;
    let expression = Expression::RegexpFullMatch(Box::new(RegexpFullMatch {
        this: Box::new(Expression::column("value")),
        expression: Box::new(Expression::column("pattern")),
        options: Vec::new(),
    }));
    for target in FOREIGN_FULL_MATCH_TARGETS {
        let error = generate(&expression, target)
            .expect_err("unverified full-match lowering must be explicit");
        assert!(
            error.to_string().contains("full-match regular expression"),
            "{target:?}: {error}"
        );
    }
}

#[test]
fn parsed_full_match_calls_reject_unverified_foreign_targets() {
    for sql in [
        "SELECT regexp_full_match('abc', 'b')",
        "SELECT regexp_full_match('abc', '^abc$')",
        "SELECT regexp_full_match(NULL, 'b')",
        "SELECT regexp_full_match(value, pattern) FROM samples",
        "SELECT regexp_full_match('AbC', 'b', 'i')",
        "SELECT NOT regexp_full_match('abc', 'b')",
        "SELECT NOT regexp_full_match(value, pattern, options) FROM samples",
        "SELECT \"regexp_full_match\"('abc', 'b')",
    ] {
        let expression = parse_one(sql, DialectType::DuckDB).unwrap();
        for target in FOREIGN_FULL_MATCH_TARGETS {
            for (operation, result) in [
                (
                    "generate",
                    generate(&expression, target).map(|sql| vec![sql]),
                ),
                ("transpile", transpile(sql, DialectType::DuckDB, target)),
                (
                    "strict",
                    Dialect::get(DialectType::DuckDB).transpile_with(
                        sql,
                        target,
                        TranspileOptions::strict(),
                    ),
                ),
            ] {
                let error = result.expect_err("full matching must not become partial matching");
                assert!(
                    matches!(error, Error::Unsupported { .. }),
                    "{operation} {target:?}: {sql}: {error}"
                );
            }
        }
    }
}

#[test]
fn qualified_full_match_names_remain_user_defined_calls() {
    let sql = "SELECT custom.regexp_full_match(value, pattern) FROM samples";
    let expression = parse_one(sql, DialectType::DuckDB).unwrap();
    for target in FOREIGN_FULL_MATCH_TARGETS {
        assert_eq!(generate(&expression, target).unwrap(), sql, "{target:?}");
        assert_eq!(
            transpile(sql, DialectType::DuckDB, target).unwrap(),
            vec![sql],
            "{target:?}"
        );
        assert_eq!(
            Dialect::get(DialectType::DuckDB)
                .transpile_with(sql, target, TranspileOptions::strict())
                .unwrap(),
            vec![sql],
            "{target:?}"
        );
    }
}

#[test]
fn strict_transpile_recognizes_full_match_as_a_regex_predicate() {
    for target in [DialectType::TSQL, DialectType::Fabric] {
        for sql in [
            "SELECT regexp_full_match(value, pattern) FROM samples",
            "SELECT NOT regexp_full_match(value, pattern) FROM samples",
        ] {
            let error = Dialect::get(DialectType::DuckDB)
                .transpile_with(sql, target, TranspileOptions::strict())
                .expect_err("strict mode must recognize the full-match predicate");
            assert!(
                error.to_string().contains("regular expression predicates"),
                "{target:?}: {error}"
            );
        }
    }
}

#[test]
fn full_match_generation_preserves_expression_options() {
    use polyglot_sql::expressions::RegexpFullMatch;
    let expression = Expression::RegexpFullMatch(Box::new(RegexpFullMatch {
        this: Box::new(Expression::column("value")),
        expression: Box::new(Expression::column("pattern")),
        options: vec![Expression::column("options")],
    }));
    assert_eq!(
        generate(&expression, DialectType::DuckDB).unwrap(),
        "REGEXP_FULL_MATCH(value, pattern, options)"
    );
}

#[test]
fn full_match_lineage_visits_expression_options() {
    use polyglot_sql::expressions::RegexpFullMatch;
    let mut expression =
        parse_one("SELECT value AS matched FROM samples", DialectType::DuckDB).unwrap();
    let Expression::Select(select) = &mut expression else {
        panic!("expected SELECT")
    };
    let Expression::Alias(alias) = &mut select.expressions[0] else {
        panic!("expected alias")
    };
    alias.this = Expression::RegexpFullMatch(Box::new(RegexpFullMatch {
        this: Box::new(Expression::column("value")),
        expression: Box::new(Expression::column("pattern")),
        options: vec![Expression::column("options")],
    }));
    let node =
        polyglot_sql::lineage::lineage("matched", &expression, Some(DialectType::DuckDB), false)
            .unwrap();
    let mut names: Vec<_> = node.walk().map(|node| node.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "matched",
            "samples.options",
            "samples.pattern",
            "samples.value"
        ]
    );
}

#[test]
fn direct_partial_match_generation_preserves_flags() {
    use polyglot_sql::expressions::RegexpFunc;
    let expression = Expression::RegexpLike(Box::new(RegexpFunc {
        this: Expression::column("value"),
        pattern: Expression::column("pattern"),
        options: None,
        flags: Some(Expression::column("options")),
    }));
    assert_eq!(
        generate(&expression, DialectType::DuckDB).unwrap(),
        "REGEXP_MATCHES(value, pattern, options)"
    );
}
