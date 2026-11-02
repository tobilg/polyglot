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
