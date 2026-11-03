use polyglot_sql::{generate, parse_one, DialectType, Expression, ExpressionWalk};

#[test]
fn duckdb_tilde_parses_as_full_match_for_direct_generation() {
    for sql in [
        "SELECT 'abc' ~ 'b' AS matched",
        "SELECT 'abc' ~ 'abc' AS matched",
        "SELECT 'abc' ~ 'a.*' AS matched",
        "SELECT 'abC' ~ 'abc' AS matched",
        "SELECT NULL ~ 'x' AS matched",
        "SELECT id FROM samples WHERE (value ~ '^[0-9]') AND id > 0",
        "SELECT id FROM samples WHERE json_extract_string(payload, '$.rating') ~ '^[0-9]'",
        "SELECT id FROM samples WHERE value ~ pattern",
    ] {
        let expression = parse_one(sql, DialectType::DuckDB).unwrap();
        assert_eq!(
            expression.count(|node| matches!(node, Expression::RegexpFullMatch(_))),
            1,
            "{sql}"
        );
        assert_eq!(
            expression.count(|node| matches!(node, Expression::RegexpLike(_))),
            0,
            "{sql}"
        );
        let generated = generate(&expression, DialectType::DuckDB).unwrap();
        let upper = generated.to_uppercase();
        assert!(upper.contains("REGEXP_FULL_MATCH("), "{generated}");
        assert!(!upper.contains("REGEXP_LIKE("), "{generated}");
        assert!(!upper.contains("REGEXP_MATCHES("), "{generated}");
        parse_one(&generated, DialectType::DuckDB).unwrap();
    }
}

#[test]
fn duckdb_negation_wraps_the_full_match_expression() {
    for sql in [
        "SELECT 'abc' !~ 'b'",
        "SELECT NOT ('abc' ~ 'b')",
        "SELECT NOT (NULL ~ 'x')",
    ] {
        let expression = parse_one(sql, DialectType::DuckDB).unwrap();
        assert_eq!(
            expression.count(|node| matches!(node, Expression::RegexpFullMatch(_))),
            1
        );
        assert_eq!(
            expression.count(|node| matches!(node, Expression::Not(_))),
            1
        );
        let output = generate(&expression, DialectType::DuckDB).unwrap();
        assert!(output.to_uppercase().contains("NOT"), "{output}");
        assert!(
            output.to_uppercase().contains("REGEXP_FULL_MATCH("),
            "{output}"
        );
    }
}

#[test]
fn postgres_tilde_keeps_its_partial_match_ast() {
    for sql in ["SELECT 'abc' ~ 'b'", "SELECT 'abc' !~ 'b'"] {
        let expression = parse_one(sql, DialectType::PostgreSQL).unwrap();
        assert_eq!(
            expression.count(|node| matches!(node, Expression::RegexpLike(_))),
            1
        );
        assert_eq!(
            expression.count(|node| matches!(node, Expression::RegexpFullMatch(_))),
            0
        );
        let output = generate(&expression, DialectType::PostgreSQL).unwrap();
        assert!(output.contains(" ~ "), "{output}");
        assert!(
            !output.to_uppercase().contains("REGEXP_FULL_MATCH"),
            "{output}"
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
fn explicit_partial_regex_function_stays_partial() {
    let expression = parse_one("SELECT regexp_matches('abc', 'b')", DialectType::DuckDB).unwrap();
    let output = generate(&expression, DialectType::DuckDB)
        .unwrap()
        .to_uppercase();
    assert!(output.contains("REGEXP_MATCHES("), "{output}");
    assert!(!output.contains("REGEXP_FULL_MATCH("), "{output}");
}

#[test]
fn explicit_full_regex_function_stays_full() {
    let expression =
        parse_one("SELECT regexp_full_match('abc', 'b')", DialectType::DuckDB).unwrap();
    let output = generate(&expression, DialectType::DuckDB)
        .unwrap()
        .to_uppercase();
    assert!(output.contains("REGEXP_FULL_MATCH("), "{output}");
    assert!(!output.contains("REGEXP_MATCHES("), "{output}");
}

#[test]
fn identity_transpile_preserves_full_match_semantics() {
    for sql in ["SELECT 'abc' ~ 'b'", "SELECT 'abc' !~ 'b'"] {
        let output = polyglot_sql::transpile(sql, DialectType::DuckDB, DialectType::DuckDB)
            .unwrap()
            .remove(0)
            .to_uppercase();
        assert!(output.contains("REGEXP_FULL_MATCH("), "{output}");
        assert!(!output.contains("REGEXP_MATCHES("), "{output}");
    }
}

#[test]
fn full_match_lineage_keeps_both_operands() {
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
    for sql in [
        "SELECT 'abc' ~ '^abc$'",
        "SELECT value ~ pattern FROM samples",
        "SELECT value !~ pattern FROM samples",
    ] {
        let expression = parse_one(sql, DialectType::DuckDB).unwrap();
        for target in [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::ClickHouse,
            DialectType::Snowflake,
            DialectType::BigQuery,
            DialectType::TSQL,
            DialectType::Fabric,
        ] {
            let error = generate(&expression, target)
                .expect_err("unverified full-match lowering must be explicit");
            assert!(
                error.to_string().contains("full-match regular expression"),
                "{target:?}: {error}"
            );
            let error = polyglot_sql::transpile(sql, DialectType::DuckDB, target)
                .expect_err("transpilation must not emit an unverified full-match function");
            assert!(
                error.to_string().contains("full-match regular expression"),
                "{target:?}: {error}"
            );
        }
    }
}

#[test]
fn strict_transpile_recognizes_full_match_as_a_regex_predicate() {
    use polyglot_sql::dialects::Dialect;
    use polyglot_sql::TranspileOptions;
    for target in [DialectType::TSQL, DialectType::Fabric] {
        for sql in [
            "SELECT value ~ pattern FROM samples",
            "SELECT value !~ pattern FROM samples",
        ] {
            let error = Dialect::get(DialectType::DuckDB)
                .transpile_with(sql, target, TranspileOptions::strict())
                .expect_err("strict mode must recognize the full-match AST");
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
