use polyglot_sql::dialects::{Dialect, DialectType};

fn transpile(sql: &str, from: DialectType, to: DialectType) -> String {
    let source_dialect = Dialect::get(from);
    let result = source_dialect.transpile(sql, to).expect("transpile");
    result[0].clone()
}

#[test]
fn test_pg_to_spark_qualified_column_refs() {
    let result = transpile(
        "SELECT t.elem, t.ord FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord)",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("t.ord + 1"), "t.ord must be rewritten to t.ord + 1, got: {}", result);
    assert!(result.contains("AS t(ord, elem)"), "should alias correctly, got: {}", result);
}

#[test]
fn test_pg_to_snowflake_qualified_column_refs() {
    let result = transpile(
        "SELECT t.elem, t.ord FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord)",
        DialectType::PostgreSQL,
        DialectType::Snowflake,
    );
    assert!(result.contains("t.INDEX + 1") || result.contains("t.ord + 1"), "t.ord must be rewritten to t.INDEX + 1 or shifted, got: {}", result);
    assert!(result.contains("VALUE") || result.contains("t.elem"), "t.elem must be preserved or rewritten to VALUE, got: {}", result);
}

#[test]
fn test_pg_to_spark_mixed_qualified_unqualified() {
    let result = transpile(
        "SELECT t.elem, ord, t.ord + 10 AS adjusted FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord)",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("ord + 1"), "ord in expression must be rewritten to ord + 1, got: {}", result);
    assert!(result.contains("t.ord + 1"), "t.ord in expression must be rewritten to t.ord + 1, got: {}", result);
}

#[test]
fn test_pg_to_spark_ordinality_in_where() {
    let result = transpile(
        "SELECT elem FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord) WHERE ord <= 2",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("WHERE ord + 1 <= 2"), "WHERE clause must rewrite ord to ord + 1, got: {}", result);
}

#[test]
fn test_pg_to_snowflake_ordinality_in_where() {
    let result = transpile(
        "SELECT elem FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord) WHERE ord <= 2",
        DialectType::PostgreSQL,
        DialectType::Snowflake,
    );
    assert!(result.contains("ord + 1") || result.contains("INDEX + 1"), "WHERE clause must reference INDEX + 1 or shifted alias, got: {}", result);
}

#[test]
fn test_pg_to_spark_ordinality_in_expression() {
    let result = transpile(
        "SELECT elem, ord * 2 AS double_pos FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord)",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("ord + 1"), "ord inside expression must be rewritten to ord + 1, got: {}", result);
}

#[test]
fn test_pg_to_spark_lateral_unnest_with_ordinality() {
    let result = transpile(
        "SELECT t.id, u.val, u.pos FROM my_table t CROSS JOIN LATERAL UNNEST(t.tags) WITH ORDINALITY AS u(val, pos)",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("POSEXPLODE"), "LATERAL UNNEST WITH ORDINALITY must use POSEXPLODE, got: {}", result);
    assert!(!result.contains("WITH ORDINALITY"), "WITH ORDINALITY must be stripped, got: {}", result);
}

#[test]
fn test_pg_to_snowflake_lateral_unnest_with_ordinality() {
    let result = transpile(
        "SELECT t.id, u.val, u.pos FROM my_table t CROSS JOIN LATERAL UNNEST(t.tags) WITH ORDINALITY AS u(val, pos)",
        DialectType::PostgreSQL,
        DialectType::Snowflake,
    );
    assert!(!result.contains("WITH ORDINALITY"), "WITH ORDINALITY must be stripped from FLATTEN output, got: {}", result);
    assert!(result.contains("FLATTEN"), "Should use FLATTEN, got: {}", result);
}

#[test]
fn test_pg_to_clickhouse_lateral_unnest_with_ordinality() {
    let result = transpile(
        "SELECT t.id, u.val, u.pos FROM my_table t CROSS JOIN LATERAL UNNEST(t.tags) WITH ORDINALITY AS u(val, pos)",
        DialectType::PostgreSQL,
        DialectType::ClickHouse,
    );
    assert!(!result.contains("WITH ORDINALITY"), "WITH ORDINALITY must be stripped, got: {}", result);
    assert!(result.contains("ARRAY JOIN") || result.contains("arrayEnumerate"), "Should use ARRAY JOIN pattern, got: {}", result);
}

#[test]
fn test_pg_to_snowflake_select_star_column_exposure() {
    let result = transpile(
        "SELECT * FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord)",
        DialectType::PostgreSQL,
        DialectType::Snowflake,
    );
    assert!(!result.contains("seq, key, path") || !result.contains("SELECT *"), "SELECT * must be expanded to avoid FLATTEN internal columns, got: {}", result);
}

#[test]
fn test_pg_to_spark_select_star_column_names() {
    let result = transpile(
        "SELECT * FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord)",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("AS t(ord, elem)"), "SELECT * should have aliases preserved but swapped, got: {}", result);
}

#[test]
fn test_pg_to_spark_ordinality_in_join_on() {
    let result = transpile(
        "SELECT u.elem, u.ord, o.name FROM UNNEST(ARRAY[1,2,3]) WITH ORDINALITY AS u(elem, ord) JOIN other_table o ON o.id = u.ord",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("u.ord + 1"), "u.ord in JOIN ON must be rewritten to u.ord + 1, got: {}", result);
}

#[test]
fn test_pg_to_spark_multiple_unnest_with_ordinality() {
    let result = transpile(
        "SELECT a.v, a.p, b.v AS v2, b.p AS p2 FROM UNNEST(ARRAY[10, 20]) WITH ORDINALITY AS a(v, p), UNNEST(ARRAY[30, 40]) WITH ORDINALITY AS b(v, p)",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(!result.contains("a.v, a.p") || result.contains("AS v") && result.contains("AS p"), "Multiple UNNEST aliases must not collide, got: {}", result);
}

#[test]
fn test_pg_to_spark_ordinality_in_case() {
    let result = transpile(
        "SELECT CASE WHEN ord > 1 THEN elem ELSE 0 END FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord)",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("ord + 1"), "ord inside CASE must be rewritten to ord + 1, got: {}", result);
}

#[test]
fn test_pg_to_spark_ordinality_in_in() {
    let result = transpile(
        "SELECT elem FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord) WHERE ord IN (1, 2)",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("ord + 1 IN"), "ord inside IN must be rewritten to ord + 1, got: {}", result);
}

#[test]
fn test_pg_to_spark_ordinality_deep_nesting() {
    let result = transpile(
        "SELECT COALESCE(NULLIF(CAST(CASE WHEN ord % 2 = 0 THEN ord ELSE elem END AS VARCHAR), '0'), 'none') FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord)",
        DialectType::PostgreSQL,
        DialectType::Spark,
    );
    assert!(result.contains("ord + 1 % 2 = 0") || result.contains("(ord + 1) % 2 = 0") || result.contains("ord + 1"), "deeply nested ord must be rewritten to ord + 1, got: {}", result);
}

#[test]
fn test_pg_to_bq_ordinality_base() {
    let result = transpile(
        "SELECT elem, ord FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(elem, ord)",
        DialectType::PostgreSQL,
        DialectType::BigQuery,
    );
    assert!(result.contains("ord + 1"), "BigQuery 0-based OFFSET must be shifted to + 1, got: {}", result);
}
