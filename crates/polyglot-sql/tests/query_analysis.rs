use polyglot_sql::{
    analyze_query, scope::SourceKind, AnalyzeQueryOptions, DialectType, ProjectionNullability,
    QueryShape, ReferenceConfidence, SetOperationBranchRole, TransformKind, ValidationSchema,
};
use serde_json::json;

#[test]
fn analyze_query_non_recursive_cte_table_shadowing_473() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{"name": "orders", "columns": [{"name": "id", "type": "BIGINT"}]}]
    }))
    .unwrap();
    for dialect in [DialectType::DuckDB, DialectType::PostgreSQL] {
        for (sql, expected) in [
            ("WITH orders AS (SELECT CAST(1 AS INT) AS id UNION ALL SELECT id FROM orders) SELECT id FROM orders", "BIGINT"),
            ("WITH orders(out_id) AS (SELECT CAST(1 AS INT) AS id UNION ALL SELECT id FROM orders) SELECT out_id FROM orders", "BIGINT"),
            ("WITH RECURSIVE orders(id) AS (SELECT CAST(1 AS INT) AS id UNION ALL SELECT id + 1 FROM orders WHERE id < 3) SELECT id FROM orders", "INT"),
        ] {
            let analysis = analyze_query(sql, AnalyzeQueryOptions {
                dialect, schema: Some(schema.clone()), ..Default::default()
            }).unwrap();
            assert_eq!(analysis.projections[0].type_hint.as_deref(), Some(expected), "{dialect}: {sql}");
        }
    }
}

#[test]
fn analyze_query_set_operation_types_454() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{"name": "orders", "columns": [{"name": "amount", "type": "VARCHAR", "nullable": true}]}]
    })).unwrap();
    for operator in [
        "UNION ALL",
        "UNION",
        "UNION ALL BY NAME",
        "INTERSECT",
        "EXCEPT",
    ] {
        for (left, right) in [("INTEGER", "FLOAT"), ("FLOAT", "INTEGER")] {
            let union = format!("SELECT CAST(amount AS {left}) AS amount FROM orders {operator} SELECT CAST(amount AS {right}) AS amount FROM orders");
            for with_schema in [false, true] {
                for sql in [
                    union.clone(),
                    format!("SELECT t.amount FROM ({union}) AS t"),
                    format!(
                        "WITH t AS ({union}), u AS (SELECT amount FROM t) SELECT amount FROM u"
                    ),
                    format!("WITH t(n) AS ({union}) SELECT n FROM t"),
                ] {
                    let analysis = analyze_query(
                        &sql,
                        AnalyzeQueryOptions {
                            dialect: DialectType::Snowflake,
                            schema: with_schema.then(|| schema.clone()),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                    let projection = &analysis.projections[0];
                    assert_eq!(projection.type_hint.as_deref(), Some("FLOAT"), "{sql}");
                    assert_eq!(projection.cast_type, None, "{sql}");
                }
            }
            let analysis = analyze_query(
                &union,
                AnalyzeQueryOptions {
                    dialect: DialectType::Snowflake,
                    ..Default::default()
                },
            )
            .unwrap();
            let branches = &analysis.set_operations[0].branches;
            assert_eq!(
                branches[0].projections[0].cast_type.as_deref(),
                Some(if left == "INTEGER" { "INT" } else { "FLOAT" })
            );
            assert_eq!(
                branches[1].projections[0].cast_type.as_deref(),
                Some(if right == "INTEGER" { "INT" } else { "FLOAT" })
            );
        }
    }
}

#[test]
fn analyze_query_set_operation_consumers_and_parameters_454() {
    let mut failures = Vec::new();
    for (dialect, sql, expected) in [
        (DialectType::Snowflake, "SELECT (SELECT CAST(1 AS INT) UNION ALL SELECT CAST(2 AS FLOAT) LIMIT 1) AS a", Some("FLOAT")),
        (DialectType::Snowflake, "SELECT CAST(1 AS INT) AS a UNION ALL (SELECT CAST(2 AS INT) AS b UNION ALL SELECT CAST(3 AS FLOAT) AS b)", Some("FLOAT")),
        (DialectType::DuckDB, "SELECT CAST(1 AS DECIMAL(10,2)) AS a UNION ALL SELECT CAST(2 AS DECIMAL(8,4)) AS a", Some("DECIMAL(12, 4)")),
        (DialectType::TSQL, "SELECT CAST(1 AS DECIMAL(10,2)) AS a UNION ALL SELECT CAST(2 AS DECIMAL(8,4)) AS a", Some("DECIMAL(12, 4)")),
        (DialectType::TSQL, "SELECT CAST('a' AS VARCHAR(10)) AS a UNION ALL SELECT CAST('b' AS VARCHAR(30)) AS a", Some("VARCHAR(30)")),
        (DialectType::TSQL, "SELECT CAST(0x01 AS BINARY(2)) AS a UNION ALL SELECT CAST(0x02 AS BINARY(5)) AS a", Some("BINARY(5)")),
        (DialectType::DuckDB, "SELECT t.a FROM (SELECT CAST([1] AS INT[]) AS a UNION ALL SELECT CAST([2.5] AS DOUBLE[]) AS a) t", Some("DOUBLE[]")),
        (DialectType::BigQuery, "SELECT ARRAY(SELECT 1 UNION ALL SELECT 2.5) AS a", Some("ARRAY<FLOAT64>")),
        (DialectType::BigQuery, "SELECT ARRAY(SELECT 1 AS a UNION ALL (SELECT 2 AS b UNION ALL SELECT 2.5 AS b)) AS a", Some("ARRAY<FLOAT64>")),
        (DialectType::Snowflake, "SELECT CAST(1 AS DECIMAL(10,2)) AS a UNION ALL SELECT unknown_fn() AS a", None),
        (DialectType::Snowflake, "SELECT unknown_fn() AS a UNION ALL SELECT CAST(1 AS DECIMAL(10,2)) AS a", None),
        (DialectType::Snowflake, "SELECT unknown_fn(1) AS a UNION ALL SELECT CAST(1 AS DECIMAL(10,2)) AS a", None),
        (DialectType::Snowflake, "WITH t AS (SELECT unknown_fn(1) AS a) SELECT a FROM t UNION ALL SELECT CAST(1 AS DECIMAL(10,2)) AS a", None),
        (DialectType::Snowflake, "SELECT t.a FROM (SELECT CAST(1 AS INT) AS a UNION ALL SELECT unknown_fn() AS a) t", None),
        (DialectType::PostgreSQL, "SELECT NULL AS a UNION ALL SELECT NULL AS a UNION ALL SELECT 1 AS a", None),
        (DialectType::BigQuery, "SELECT NULL AS a UNION ALL SELECT NULL AS a", Some("INT64")),
        (DialectType::BigQuery, "SELECT CAST(1 AS INT64) AS a UNION ALL SELECT CAST(2 AS FLOAT64) AS a", Some("FLOAT64")),
        (DialectType::SQLite, "SELECT CAST(1 AS INTEGER) AS a UNION ALL SELECT CAST(2 AS REAL) AS a", None),
        (DialectType::Teradata, "SELECT CAST(1 AS INTEGER) AS a UNION ALL SELECT CAST(2 AS FLOAT) AS a", Some("INT")),
        (DialectType::PostgreSQL, "SELECT CAST(1 AS REAL) AS a UNION ALL SELECT CAST(2 AS FLOAT) AS a", Some("DOUBLE PRECISION")),
        (DialectType::TSQL, "SELECT CAST(1 AS REAL) AS a UNION ALL SELECT CAST(2 AS FLOAT) AS a", Some("FLOAT")),
        (DialectType::ClickHouse, "SELECT CAST(1 AS Int32) AS a UNION ALL SELECT CAST(2 AS Float32) AS a", Some("Float64")),
        (DialectType::ClickHouse, "SELECT CAST(1 AS Int64) AS a UNION ALL SELECT CAST(2 AS Float64) AS a", None),
        (DialectType::ClickHouse, "SELECT CAST(1 AS Int32) AS a UNION ALL SELECT NULL AS a", Some("Nullable(Int32)")),
        (DialectType::DataFusion, "SELECT CAST(1 AS INT) AS a UNION ALL SELECT CAST(2 AS FLOAT) AS a", Some("FLOAT")),
        (DialectType::Databricks, "SELECT CAST(1 AS INT) AS a UNION ALL SELECT CAST(2 AS FLOAT) AS a", Some("DOUBLE")),
        (DialectType::Spark, "SELECT CAST(1 AS INT) AS a UNION ALL SELECT CAST(2 AS FLOAT) AS a", None),
        (DialectType::Trino, "SELECT CAST('1' AS VARCHAR) AS a UNION ALL SELECT CAST(2 AS INT) AS a", None),
        (DialectType::Hive, "SELECT CAST('1' AS STRING) AS a UNION ALL SELECT CAST(2 AS INT) AS a", None),
        (DialectType::BigQuery, "SELECT '2024-02-29' AS a UNION ALL SELECT CAST('2024-01-01' AS DATE) AS a", Some("DATE")),
        (DialectType::BigQuery, "SELECT '2024-02-30' AS a UNION ALL SELECT CAST('2024-01-01' AS DATE) AS a", None),
        (DialectType::BigQuery, "SELECT CAST('2024-02-29' AS STRING) AS a UNION ALL SELECT CAST('2024-01-01' AS DATE) AS a", None),
        (DialectType::Snowflake, "SELECT (SELECT 1 AS a, 2 AS b UNION ALL SELECT 3 AS a, 4 AS b) AS x", None),
        (DialectType::DuckDB, "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t", Some("INT")),
        (DialectType::PostgreSQL, "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t", Some("INT")),
    ] {
        let analysis = analyze_query(sql, AnalyzeQueryOptions { dialect, ..Default::default() }).unwrap();
        let actual = analysis.projections[0].type_hint.as_deref();
        if actual != expected {
            failures.push(format!("{dialect:?}: {sql}: expected {expected:?}, got {actual:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn analyze_query_set_operation_name_alignment_and_cast_consensus_454() {
    let named = analyze_query(
        "SELECT CAST(1 AS INT) AS a UNION ALL BY NAME (SELECT CAST(2 AS INT) AS a UNION ALL BY NAME SELECT CAST(3 AS FLOAT) AS a, CAST('x' AS VARCHAR) AS b)",
        AnalyzeQueryOptions { dialect: DialectType::Snowflake, ..Default::default() },
    ).unwrap();
    let nested_branch = &named.set_operations[0].branches[1].projections;
    assert_eq!(nested_branch.len(), 2);
    assert_eq!(nested_branch[0].type_hint.as_deref(), Some("FLOAT"));
    assert_eq!(nested_branch[1].type_hint.as_deref(), Some("VARCHAR"));
    assert!(nested_branch.iter().all(|p| p.cast_type.is_none()));
    let nested = analyze_query(
        "SELECT CAST(1 AS INT) AS a UNION ALL (SELECT CAST(2 AS INT) AS b UNION ALL SELECT CAST(3 AS FLOAT) AS b)",
        AnalyzeQueryOptions { dialect: DialectType::Snowflake, ..Default::default() },
    ).unwrap();
    assert_eq!(
        nested.set_operations[0].branches[1].projections[0]
            .type_hint
            .as_deref(),
        Some("FLOAT")
    );
    assert_eq!(
        nested.set_operations[0].branches[1].projections[0].cast_type,
        None
    );
    for (sql, expected_types, expected_casts) in [
        ("SELECT CAST(1 AS INT) AS a, CAST(2 AS FLOAT) AS b UNION ALL BY NAME SELECT CAST(3 AS INT) AS b, CAST(4 AS FLOAT) AS a",
            vec![Some("FLOAT"), Some("FLOAT")], vec![None, None]),
        ("SELECT CAST(1 AS INT) AS a UNION ALL BY NAME SELECT CAST(2 AS FLOAT) AS a, CAST('x' AS VARCHAR) AS b",
            vec![Some("FLOAT"), Some("VARCHAR")], vec![None, None]),
        ("SELECT CAST(1 AS INT) AS a UNION ALL SELECT CAST(2 AS INTEGER) AS a",
            vec![Some("INT")], vec![Some("INT")]),
        ("SELECT CAST(1 AS INT) AS a UNION ALL SELECT 2 AS a",
            vec![Some("INT")], vec![None]),
        ("SELECT CAST(1 AS INT) AS a UNION ALL SELECT NULL AS a",
            vec![Some("INT")], vec![None]),
        ("SELECT CAST(1 AS INT) AS \"a\" UNION ALL BY NAME SELECT CAST(2 AS FLOAT) AS a",
            vec![Some("INT"), Some("FLOAT")], vec![None, None]),
    ] {
        let analysis = analyze_query(sql, AnalyzeQueryOptions { dialect: DialectType::Snowflake, ..Default::default() }).unwrap();
        assert_eq!(analysis.projections.iter().map(|p| p.type_hint.as_deref()).collect::<Vec<_>>(), expected_types, "{sql}");
        assert_eq!(analysis.projections.iter().map(|p| p.cast_type.as_deref()).collect::<Vec<_>>(), expected_casts, "{sql}");
    }
}

#[test]
fn analysis_review_lambda_dependencies_and_result_type() {
    let options: AnalyzeQueryOptions = serde_json::from_value(json!({"dialect":"snowflake", "schema":{"tables":[{"name":"items","columns":[{"name":"quantity","type":"INT"}]}]}})).unwrap();
    for sql in [
        "SELECT TRANSFORM(ARRAY_CONSTRUCT(quantity), x -> x + 1) AS xs FROM items",
        "SELECT TRANSFORM(ARRAY_CONSTRUCT(quantity), x INT -> x + quantity) AS xs FROM items",
        "SELECT TRANSFORM(ARRAY_CONSTRUCT(quantity), x -> TRANSFORM(ARRAY_CONSTRUCT(quantity), x -> x + quantity)) AS xs FROM items",
    ] {
        let result = analyze_query(sql, options.clone()).unwrap();
        let projection = &result.projections[0];
        assert!(projection.type_hint.as_deref().is_some_and(|t| t.starts_with("ARRAY")), "{sql}: {:?}", projection.type_hint);
        assert_eq!(projection.upstream.len(), 1, "{sql}: {:?}", projection.upstream);
        assert!(projection.upstream[0].column.eq_ignore_ascii_case("quantity"));
    }
    let result = analyze_query("SELECT quantity FROM items WHERE ARRAY_SIZE(FILTER(ARRAY_CONSTRUCT(quantity), x -> x > 0)) > 0", options).unwrap();
    assert!(result
        .column_uses
        .iter()
        .flat_map(|fact| &fact.references)
        .all(|r| r.reference.column.eq_ignore_ascii_case("quantity")));
}

#[test]
fn analysis_review_projection_and_clause_confidence() {
    let options: AnalyzeQueryOptions = serde_json::from_value(json!({"schema":{"tables":[{"name":"items","columns":[{"name":"quantity","type":"INT"}]}]}})).unwrap();
    for sql in [
        "SELECT missing FROM items WHERE missing > 0",
        "SELECT quantity FROM items JOIN unknown_source ON TRUE WHERE quantity > 0",
    ] {
        let result = analyze_query(sql, options.clone()).unwrap();
        assert!(
            result.projections[0]
                .upstream
                .iter()
                .all(|r| r.confidence != ReferenceConfidence::Resolved),
            "{sql}: {:?}",
            result.projections[0].upstream
        );
        let filter = result
            .column_uses
            .iter()
            .find(|f| f.context == polyglot_sql::ColumnUseContext::Filter)
            .unwrap();
        assert!(filter
            .references
            .iter()
            .all(|r| r.reference.confidence != ReferenceConfidence::Resolved));
    }
}

#[test]
fn analysis_honors_complexity_guards_in_parsing_and_fact_rendering() {
    let expression = format!("{}value{}", "COALESCE(".repeat(65), ", 0)".repeat(65));
    let sql = format!("WITH c AS (SELECT {expression} AS value FROM records) SELECT value FROM c WHERE {expression} > 0");
    assert!(analyze_query(&sql, AnalyzeQueryOptions::default())
        .unwrap_err()
        .to_string()
        .contains("E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED"));
    for limit in [Some(128), None] {
        let options: AnalyzeQueryOptions = serde_json::from_value(json!({
            "dialect": "snowflake", "complexityGuard": {"maxFunctionCallDepth": limit}
        }))
        .unwrap();
        let result = analyze_query(&sql, options).unwrap();
        assert!(result.cte_facts[0].body_sql.contains("COALESCE"));
        assert!(!result.column_uses[0].expression_sql.is_empty());
        assert_eq!(result.projections.len(), 1);
    }
    // Rendering a permitted deep AST must not revert to the default depth of 512.
    let expression = format!("value{}", " + 1".repeat(520));
    let sql = format!("WITH c AS (SELECT {expression} AS value FROM records) SELECT value FROM c WHERE {expression} > 0");
    let options: AnalyzeQueryOptions =
        serde_json::from_value(json!({"complexityGuard": {"maxAstDepth": 1024}})).unwrap();
    let result = analyze_query(&sql, options).unwrap();
    assert_eq!(result.cte_facts[0].body_sql.matches(" + ").count(), 520);
    assert!(result
        .column_uses
        .iter()
        .any(|fact| fact.expression_sql.matches(" + ").count() == 520));
}

#[test]
fn analyze_query_preserves_cast_types_through_cte_chains() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{"name": "raw_orders", "columns": [{"name": "amount", "type": "VARCHAR"}]}]
    }))
    .unwrap();
    for (dialect, expected) in [
        (DialectType::Snowflake, "INT"),
        (DialectType::DuckDB, "INT"),
        (DialectType::PostgreSQL, "INT"),
        (DialectType::BigQuery, "INT64"),
    ] {
        for sql in [
            "WITH transformed AS (SELECT CAST(amount AS INTEGER) AS amount FROM raw_orders), final AS (SELECT amount FROM transformed) SELECT amount FROM final",
            "WITH transformed AS (SELECT CAST(amount AS INTEGER) AS n FROM raw_orders), final AS (SELECT n FROM transformed) SELECT n FROM final",
            "WITH transformed AS (SELECT CAST(amount AS INTEGER) AS amount FROM raw_orders), final AS (SELECT transformed.amount FROM transformed) SELECT final.amount FROM final",
            "WITH transformed(n) AS (SELECT CAST(amount AS INTEGER) FROM raw_orders), final(m) AS (SELECT n FROM transformed) SELECT m FROM final",
            "WITH transformed AS (SELECT CAST(amount AS INTEGER) AS amount FROM raw_orders), final AS (SELECT * FROM transformed) SELECT * FROM final",
            "WITH transformed AS (SELECT CAST(amount AS INTEGER) AS amount FROM raw_orders) SELECT amount FROM (SELECT amount FROM transformed) AS final",
            "WITH unused AS (SELECT CAST(amount AS DATE) AS amount FROM raw_orders), transformed AS (SELECT CAST(amount AS INTEGER) AS amount FROM raw_orders) SELECT amount FROM transformed",
        ] {
            for with_schema in [false, true] {
                let analysis = analyze_query(sql, AnalyzeQueryOptions {
complexity_guard: None,
                    dialect,
                    schema: with_schema.then(|| schema.clone()),
                }).unwrap();
                let fact = &analysis.projections[0];
                assert_eq!(fact.type_hint.as_deref(), Some(expected), "{dialect:?}, schema={with_schema}: {sql}");
                assert_eq!(fact.transform_kind, TransformKind::Direct, "{sql}");
                assert_eq!(fact.cast_type, None, "{sql}");
                assert!(fact.upstream.iter().any(|reference| {
                    reference.table.as_deref().is_some_and(|table| table.eq_ignore_ascii_case("raw_orders"))
                        && reference.column.eq_ignore_ascii_case("amount")
                }), "{sql}: {:?}", fact.upstream);
            }
        }
    }
}

#[test]
fn analyze_query_keeps_scalar_subquery_types_in_their_own_scope() {
    let schema: ValidationSchema = serde_json::from_value(json!({"tables": [
        {"name":"t1", "columns":[{"name":"id","type":"INT"}]},
        {"name":"t2", "columns":[{"name":"id","type":"INT"},{"name":"val","type":"DOUBLE"}]}
    ]}))
    .unwrap();
    for (sql, expected) in [
        ("SELECT (SELECT val FROM t2 WHERE t2.id = t1.id) AS v FROM t1", "DOUBLE"),
        ("SELECT (SELECT id) AS v FROM t1", "INT"),
        ("SELECT (SELECT (SELECT id)) AS v FROM t1", "INT"),
        ("SELECT (WITH c AS (SELECT id AS n) SELECT n FROM c) AS v FROM t1", "INT"),
        ("SELECT (WITH c AS (SELECT CAST(val AS VARCHAR) AS n FROM t2) SELECT n FROM c) AS v FROM t1", "TEXT"),
        ("SELECT b.val FROM t1 JOIN LATERAL (SELECT val FROM t2 WHERE t2.id = t1.id) AS b ON TRUE", "DOUBLE"),
        ("SELECT b.id FROM t1, LATERAL (SELECT id) AS b", "INT"),
        ("WITH unused AS (SELECT 1 AS n) SELECT (SELECT n) AS v FROM t1", "UNKNOWN"),
    ] {
        let analysis = analyze_query(sql, AnalyzeQueryOptions {
complexity_guard: None,
            dialect: DialectType::DuckDB, schema: Some(schema.clone()),
        }).unwrap();
        // Unresolved types are omitted instead of exposing the internal UNKNOWN sentinel.
        assert_eq!(analysis.projections[0].type_hint.as_deref(), (expected != "UNKNOWN").then_some(expected), "{sql}");
    }
}

#[test]
fn analyze_query_infers_the_selected_expression_not_an_arbitrary_upstream_cast() {
    for (sql, expected) in [
        ("WITH a AS (SELECT CAST('1' AS INT) AS n), b AS (SELECT CAST(n AS VARCHAR) AS n FROM a) SELECT n FROM b", "TEXT"),
        ("WITH a AS (SELECT CAST('1' AS INT) AS n), b AS (SELECT n + 0.5 AS n FROM a) SELECT n FROM b", "DOUBLE"),
        ("WITH a AS (SELECT CAST('1' AS INT) AS n), b AS (WITH a AS (SELECT CAST('2024-01-01' AS DATE) AS n) SELECT n FROM a) SELECT n FROM b", "DATE"),
        ("WITH a AS (SELECT CAST('1' AS INT) AS n), b AS (SELECT n FROM a UNION ALL SELECT n FROM a) SELECT n FROM b", "INT"),
    ] {
        let analysis = analyze_query(sql, AnalyzeQueryOptions {
            dialect: DialectType::DuckDB,
            ..Default::default()
        }).unwrap();
        assert_eq!(analysis.projections[0].type_hint.as_deref(), Some(expected), "{sql}");
    }
}

#[test]
fn schema_aware_lineage_preserves_cte_cast_nodes() {
    use polyglot_sql::traversal::ExpressionWalk;
    let sql = "WITH transformed AS (SELECT CAST(amount AS INTEGER) AS amount FROM raw_orders), final AS (SELECT amount FROM transformed) SELECT amount FROM final";
    let expression = polyglot_sql::parse_one(sql, DialectType::Snowflake).unwrap();
    let schema = polyglot_sql::mapping_schema_from_validation_schema_with_dialect(
        &serde_json::from_value(json!({"tables": [{"name": "raw_orders", "columns": [{"name": "amount", "type": "VARCHAR"}]}]})).unwrap(),
        DialectType::Snowflake,
    );
    let node = polyglot_sql::lineage::lineage_with_schema(
        "amount",
        &expression,
        Some(&schema),
        Some(DialectType::Snowflake),
        false,
    )
    .unwrap();
    let nodes: Vec<_> = node.walk().collect();
    assert_eq!(nodes.len(), 4);
    assert!(nodes.iter().any(|node| node
        .expression
        .contains(|e| matches!(e, polyglot_sql::Expression::Cast(_)))));
    assert_eq!(nodes[1].source_kind, SourceKind::Cte);
    assert_eq!(nodes[2].source_kind, SourceKind::Cte);
    assert_eq!(nodes[3].source_kind, SourceKind::Table);
}

fn column_use_analysis(sql: &str, with_schema: bool) -> polyglot_sql::QueryAnalysis {
    analyze_query(
        sql,
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: with_schema.then(schema),
        },
    )
    .unwrap()
}

#[test]
fn analyze_query_reports_filter_uses_without_changing_projection_lineage() {
    use polyglot_sql::ColumnUseContext;
    for with_schema in [false, true] {
        let sql = "SELECT o.id FROM orders AS o WHERE o.amount > 10";
        let analysis = column_use_analysis(sql, with_schema);
        assert_eq!(analysis.projections[0].upstream.len(), 1);
        assert_eq!(analysis.projections[0].upstream[0].column, "id");
        assert_eq!(analysis.column_uses.len(), 1);
        let fact = &analysis.column_uses[0];
        assert_eq!(fact.context, ColumnUseContext::Filter);
        assert_eq!(fact.scope_path, "root");
        assert_eq!(fact.expression_path, "where_clause.this");
        assert_eq!(fact.expression_sql, "o.amount > 10");
        assert!(fact.span.is_none());
        let reference = &fact.references[0];
        assert_eq!(reference.reference.source_name.as_deref(), Some("orders"));
        assert_eq!(reference.reference.source_alias.as_deref(), Some("o"));
        assert_eq!(reference.reference.column, "amount");
        assert_eq!(
            reference.reference.confidence,
            ReferenceConfidence::Resolved
        );
        let span = reference.span.unwrap();
        assert_eq!(
            sql.chars()
                .skip(span.start)
                .take(span.end - span.start)
                .collect::<String>(),
            "o.amount"
        );
    }
    assert!(column_use_analysis("SELECT 1", false)
        .column_uses
        .is_empty());
}

#[test]
fn analyze_query_column_uses_group_conditions_and_keep_occurrence_spans() {
    use polyglot_sql::ColumnUseContext;
    let sql = "SELECT '😀', o.id FROM orders o JOIN customers c ON o.customer_id = c.id AND o.amount > 0 WHERE o.amount > 1 OR o.amount < 0";
    let analysis = column_use_analysis(sql, true);
    let join = analysis
        .column_uses
        .iter()
        .find(|fact| fact.context == ColumnUseContext::Join)
        .unwrap();
    assert_eq!(join.expression_path, "joins[0].on");
    assert_eq!(join.references.len(), 3);
    let filter = analysis
        .column_uses
        .iter()
        .find(|fact| fact.context == ColumnUseContext::Filter)
        .unwrap();
    assert_eq!(filter.references.len(), 2);
    assert_ne!(filter.references[0].span, filter.references[1].span);
    for reference in &filter.references {
        let span = reference.span.unwrap();
        assert_eq!(
            sql.chars()
                .skip(span.start)
                .take(span.end - span.start)
                .collect::<String>(),
            "o.amount"
        );
    }
}

#[test]
fn analyze_query_column_uses_cover_group_having_order_aliases_and_ordinals() {
    use polyglot_sql::ColumnUseContext;
    let analysis = column_use_analysis("SELECT o.customer_id AS customer, SUM(o.amount) AS amount_sum FROM orders o GROUP BY 1 HAVING amount_sum > 10 ORDER BY amount_sum, 1", true);
    for (context, expected) in [
        (ColumnUseContext::Group, "customer_id"),
        (ColumnUseContext::Having, "amount"),
    ] {
        let fact = analysis
            .column_uses
            .iter()
            .find(|fact| fact.context == context)
            .unwrap();
        assert_eq!(fact.references[0].reference.column, expected, "{fact:?}");
    }
    let order: Vec<_> = analysis
        .column_uses
        .iter()
        .filter(|fact| fact.context == ColumnUseContext::Order)
        .collect();
    assert_eq!(order.len(), 2);
    assert_eq!(order[0].references[0].reference.column, "amount");
    assert_eq!(order[1].references[0].reference.column, "customer_id");
    assert!(order[1].references[0].span.is_none());
}

#[test]
fn analyze_query_column_uses_cover_windows_qualify_and_aggregate_filters() {
    use polyglot_sql::ColumnUseContext;
    let analysis = column_use_analysis("SELECT o.id, ROW_NUMBER() OVER w AS rn, SUM(o.amount) FILTER (WHERE o.amount > 0) OVER (PARTITION BY o.customer_id ORDER BY o.id) AS total FROM orders o WINDOW w AS (PARTITION BY o.customer_id ORDER BY o.id) QUALIFY rn = 1 ORDER BY o.id", true);
    for context in [
        ColumnUseContext::WindowPartition,
        ColumnUseContext::WindowOrder,
        ColumnUseContext::Qualify,
        ColumnUseContext::Filter,
        ColumnUseContext::Order,
    ] {
        let facts: Vec<_> = analysis
            .column_uses
            .iter()
            .filter(|fact| fact.context == context)
            .collect();
        assert!(
            !facts.is_empty(),
            "missing {context:?}: {:?}",
            analysis.column_uses
        );
        assert!(
            facts.iter().all(|fact| !fact.references.is_empty()),
            "{facts:?}"
        );
    }
}

#[test]
fn analyze_query_column_uses_resolve_chained_ctes_and_correlated_scopes() {
    use polyglot_sql::ColumnUseContext;
    let sql = "WITH base AS (SELECT id, amount FROM orders), paid AS (SELECT id, amount * 2 AS amount FROM base WHERE amount > 0) SELECT p.id FROM paid p WHERE p.amount > 10 AND EXISTS (SELECT 1 FROM customers c WHERE c.id = p.id)";
    for with_schema in [false, true] {
        let analysis = column_use_analysis(sql, with_schema);
        let root = analysis
            .column_uses
            .iter()
            .find(|fact| fact.scope_path == "root" && fact.context == ColumnUseContext::Filter)
            .unwrap();
        assert_eq!(
            root.references.len(),
            1,
            "subquery references belong to their own scope"
        );
        assert_eq!(
            root.references[0].reference.table.as_deref(),
            Some("orders"),
            "{root:?}"
        );
        assert_eq!(root.references[0].reference.column, "amount");
        let correlated = analysis
            .column_uses
            .iter()
            .find(|fact| fact.scope_path == "root.subqueries[0]")
            .unwrap();
        assert_eq!(correlated.references.len(), 2);
        assert_eq!(
            correlated.references[1].reference.table.as_deref(),
            Some("orders"),
            "{correlated:?}"
        );
        assert!(analysis
            .column_uses
            .iter()
            .any(|fact| fact.scope_path == "root.ctes[1]"));
    }
}

#[test]
fn analyze_query_column_uses_do_not_leak_ctes_or_non_lateral_sources() {
    let analysis = column_use_analysis("SELECT o.id FROM orders o WHERE EXISTS (WITH local AS (SELECT id FROM customers) SELECT 1 FROM local WHERE local.id = o.id) AND EXISTS (SELECT 1 FROM local WHERE local.id = 1)", true);
    let leaked = analysis
        .column_uses
        .iter()
        .find(|fact| fact.scope_path == "root.subqueries[1]")
        .unwrap();
    assert_ne!(
        leaked.references[0].reference.table.as_deref(),
        Some("customers")
    );
    let analysis = column_use_analysis("SELECT o.id FROM orders o JOIN (SELECT id FROM customers WHERE id = o.id) c ON o.id = c.id", false);
    let derived = analysis
        .column_uses
        .iter()
        .find(|fact| fact.scope_path == "root.derived[0]")
        .unwrap();
    assert_eq!(
        derived.references[1].reference.confidence,
        ReferenceConfidence::Unknown
    );
}

#[test]
fn analyze_query_column_uses_conservatively_resolve_partial_and_ambiguous_schemas() {
    for (sql, with_schema, expected) in [
        (
            "SELECT 1 FROM orders o JOIN customers c ON TRUE WHERE id > 0",
            true,
            ReferenceConfidence::Ambiguous,
        ),
        (
            "SELECT 1 FROM orders o JOIN missing m ON TRUE WHERE id > 0",
            true,
            ReferenceConfidence::Unknown,
        ),
        (
            "SELECT 1 FROM orders o JOIN customers c ON TRUE WHERE id > 0",
            false,
            ReferenceConfidence::Unknown,
        ),
        (
            "SELECT 1 FROM orders o WHERE bogus.id > 0",
            true,
            ReferenceConfidence::Unknown,
        ),
        (
            "SELECT 1 FROM orders o WHERE o.missing > 0",
            true,
            ReferenceConfidence::Unknown,
        ),
    ] {
        let analysis = column_use_analysis(sql, with_schema);
        let fact = analysis
            .column_uses
            .iter()
            .find(|fact| fact.context == polyglot_sql::ColumnUseContext::Filter)
            .unwrap();
        assert_eq!(
            fact.references[0].reference.confidence, expected,
            "{sql}: {fact:?}"
        );
    }
}

#[test]
fn analyze_query_column_uses_cover_using_and_natural_joins() {
    for sql in [
        "SELECT 1 FROM orders JOIN customers USING (id)",
        "SELECT 1 FROM orders NATURAL JOIN customers",
    ] {
        let analysis = column_use_analysis(sql, true);
        let fact = analysis
            .column_uses
            .iter()
            .find(|fact| fact.context == polyglot_sql::ColumnUseContext::Join)
            .unwrap();
        assert_eq!(fact.references.len(), 2, "{fact:?}");
        assert!(fact
            .references
            .iter()
            .all(|reference| reference.reference.column == "id"
                && reference.reference.confidence == ReferenceConfidence::Resolved));
    }
    let unknown = column_use_analysis("SELECT 1 FROM orders NATURAL JOIN customers", false);
    assert_eq!(
        unknown.column_uses[0].references[0].reference.confidence,
        ReferenceConfidence::Unknown
    );
}

#[test]
fn analyze_query_column_uses_identify_nested_filter_branches_and_set_order() {
    use polyglot_sql::ColumnUseContext;
    let sql = "SELECT id FROM orders EXCEPT (SELECT id FROM customers WHERE id > 0 UNION ALL SELECT id FROM users) ORDER BY id";
    let analysis = column_use_analysis(sql, true);
    let filter_branches: Vec<_> = analysis
        .column_uses
        .iter()
        .filter(|fact| fact.context == ColumnUseContext::SetOperationFilter)
        .collect();
    assert_eq!(filter_branches.len(), 2, "{:?}", analysis.column_uses);
    assert!(filter_branches
        .iter()
        .all(|fact| fact.scope_path.starts_with("root.branches[1].branches[")));
    let order = analysis
        .column_uses
        .iter()
        .find(|fact| fact.context == ColumnUseContext::Order)
        .unwrap();
    assert!(!order.references.is_empty());
    assert!(
        analysis.projections[0]
            .upstream
            .iter()
            .any(|reference| reference.table.as_deref() == Some("customers")),
        "existing lineage must remain unchanged"
    );
}

#[test]
fn analyze_query_column_uses_are_json_additive_and_deterministic() {
    let sql =
        "SELECT o.id FROM orders o JOIN customers c USING(id) WHERE o.amount > 0 ORDER BY o.id";
    let value = serde_json::to_value(column_use_analysis(sql, true)).unwrap();
    assert_eq!(
        value,
        serde_json::to_value(column_use_analysis(sql, true)).unwrap()
    );
    assert!(value["columnUses"][0]["references"][0]
        .get("reference")
        .is_none());
    let mut legacy = value;
    legacy.as_object_mut().unwrap().remove("columnUses");
    let decoded: polyglot_sql::QueryAnalysis = serde_json::from_value(legacy).unwrap();
    assert!(decoded.column_uses.is_empty());
}

#[test]
fn analyze_query_column_uses_include_scalar_predicate_inputs_but_not_exists_outputs() {
    for predicate in [
        "o.id = (SELECT MAX(c.id) FROM customers c)",
        "o.id IN (SELECT c.id FROM customers c)",
    ] {
        let analysis =
            column_use_analysis(&format!("SELECT 1 FROM orders o WHERE {predicate}"), true);
        let root = &analysis.column_uses[0];
        assert_eq!(root.references.len(), 2, "{root:?}");
        assert!(root
            .references
            .iter()
            .any(|usage| usage.reference.table.as_deref() == Some("customers")));
    }
    let analysis = column_use_analysis(
        "SELECT 1 FROM orders o WHERE EXISTS (SELECT c.name FROM customers c WHERE c.id = o.id)",
        true,
    );
    assert!(analysis.column_uses[0].references.is_empty());
    assert_eq!(analysis.column_uses[1].references.len(), 2);
    assert!(analysis
        .column_uses
        .iter()
        .flat_map(|fact| &fact.references)
        .all(|usage| usage.reference.column != "name"));
}

#[test]
fn analyze_query_column_uses_handle_cte_alias_columns_and_unknown_transitive_references() {
    let analysis = column_use_analysis(
        "WITH renamed(key) AS (SELECT id FROM orders) SELECT 1 FROM renamed WHERE key > 0",
        true,
    );
    assert_eq!(analysis.column_uses[0].references[0].reference.column, "id");
    assert_eq!(
        analysis.column_uses[0].references[0]
            .reference
            .table
            .as_deref(),
        Some("orders")
    );
    for sql in [
        "WITH invalid AS (SELECT bogus.id AS key FROM orders) SELECT 1 FROM invalid WHERE key > 0",
        "WITH invalid AS (SELECT o.missing AS key FROM orders o) SELECT 1 FROM invalid WHERE key > 0",
    ] {
        let analysis = column_use_analysis(sql, true);
        assert_eq!(analysis.column_uses[0].references[0].reference.confidence, ReferenceConfidence::Unknown, "{analysis:?}");
    }
}

#[test]
fn analyze_query_column_uses_expand_star_filter_inputs_with_schema() {
    let analysis = column_use_analysis("SELECT * FROM customers EXCEPT SELECT * FROM users", true);
    let fact = analysis
        .column_uses
        .iter()
        .find(|fact| fact.context == polyglot_sql::ColumnUseContext::SetOperationFilter)
        .unwrap();
    assert_eq!(fact.references.len(), 2, "{fact:?}");
    assert_eq!(fact.references[0].reference.column, "id");
    assert_eq!(fact.references[1].reference.column, "name");
}

#[test]
fn analyze_query_column_uses_resolve_output_aliases_after_star_expansion() {
    let analysis = column_use_analysis(
        "SELECT o.*, o.amount * 2 AS doubled FROM orders o ORDER BY doubled",
        true,
    );
    let reference = &analysis.column_uses[0].references[0].reference;
    assert_eq!(reference.column, "amount");
    assert_eq!(reference.table.as_deref(), Some("orders"));
}

#[test]
fn analyze_query_column_uses_reuse_struct_field_resolution() {
    let schema: ValidationSchema = serde_json::from_value(json!({"tables":[{"name":"events","columns":[{"name":"payload","type":"STRUCT(active BOOLEAN)"}]}]})).unwrap();
    let analysis = analyze_query(
        "SELECT 1 FROM events WHERE payload.active = TRUE",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(schema),
        },
    )
    .unwrap();
    let reference = &analysis.column_uses[0].references[0].reference;
    assert_eq!(reference.column, "payload");
    assert_eq!(reference.table.as_deref(), Some("events"));
    assert_eq!(reference.confidence, ReferenceConfidence::Resolved);
}

#[test]
fn analyze_query_column_uses_preserve_transitive_partial_schema_uncertainty() {
    let analysis = column_use_analysis(
        "WITH c AS (SELECT id FROM orders o JOIN missing m ON TRUE) SELECT 1 FROM c WHERE c.id > 0",
        true,
    );
    let fact = analysis
        .column_uses
        .iter()
        .find(|fact| fact.context == polyglot_sql::ColumnUseContext::Filter)
        .unwrap();
    assert!(
        fact.references
            .iter()
            .all(|usage| usage.reference.confidence == ReferenceConfidence::Unknown),
        "{fact:?}"
    );
}

#[test]
fn analyze_query_column_uses_cover_supported_qualify_dialects_and_filter_operators() {
    for dialect in [
        DialectType::DuckDB,
        DialectType::BigQuery,
        DialectType::Snowflake,
    ] {
        let analysis = analyze_query("SELECT o.id, ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.id) AS rn FROM orders o QUALIFY rn = 1", AnalyzeQueryOptions {
complexity_guard: None, dialect, schema: Some(schema()) }).unwrap();
        let fact = analysis
            .column_uses
            .iter()
            .find(|fact| fact.context == polyglot_sql::ColumnUseContext::Qualify)
            .unwrap();
        assert!(!fact.references.is_empty(), "{dialect:?}: {fact:?}");
        assert!(
            fact.references
                .iter()
                .all(|usage| usage.reference.confidence == ReferenceConfidence::Resolved),
            "{dialect:?}: {fact:?}"
        );
    }
    for operator in ["EXCEPT", "INTERSECT"] {
        let analysis = column_use_analysis(
            &format!("SELECT id FROM orders {operator} SELECT id FROM customers"),
            true,
        );
        let fact = analysis
            .column_uses
            .iter()
            .find(|fact| fact.context == polyglot_sql::ColumnUseContext::SetOperationFilter)
            .unwrap();
        assert_eq!(fact.scope_path, "root.branches[1]");
        assert_eq!(
            fact.references[0].reference.table.as_deref(),
            Some("customers")
        );
    }
}

#[test]
fn analyze_query_column_uses_cover_aggregate_order_and_window_frame_inputs() {
    let analysis = column_use_analysis("SELECT ARRAY_AGG(o.amount ORDER BY o.id), SUM(o.amount) OVER (ORDER BY o.id ROWS BETWEEN o.customer_id PRECEDING AND CURRENT ROW) FROM orders o", false);
    for (context, column) in [
        (polyglot_sql::ColumnUseContext::AggregateOrder, "id"),
        (polyglot_sql::ColumnUseContext::WindowFrame, "customer_id"),
    ] {
        let fact = analysis
            .column_uses
            .iter()
            .find(|fact| fact.context == context)
            .unwrap_or_else(|| panic!("missing {context:?}: {:?}", analysis.column_uses));
        assert_eq!(fact.references[0].reference.column, column);
    }
}

#[test]
fn analyze_query_column_uses_do_not_resolve_forward_join_aliases() {
    let analysis = column_use_analysis(
        "SELECT o.id FROM orders o JOIN customers c ON c.id = u.id JOIN users u ON u.id = o.id",
        false,
    );
    let joins: Vec<_> = analysis
        .column_uses
        .iter()
        .filter(|fact| fact.context == polyglot_sql::ColumnUseContext::Join)
        .collect();
    assert_eq!(
        joins[0].references[1].reference.confidence,
        ReferenceConfidence::Unknown
    );
    assert_eq!(
        joins[1].references[0].reference.confidence,
        ReferenceConfidence::Resolved
    );
}

fn schema() -> ValidationSchema {
    serde_json::from_value(json!({
        "tables": [
            {
                "name": "users",
                "columns": [
                    {"name": "id", "type": "INT", "primaryKey": true},
                    {"name": "name", "type": "TEXT", "nullable": false}
                ]
            },
            {
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "INT", "nullable": false},
                    {"name": "user_id", "type": "INT"},
                    {"name": "customer_id", "type": "INT"},
                    {"name": "amount", "type": "DECIMAL(10,2)", "nullable": true},
                    {"name": "total", "type": "FLOAT"}
                ]
            },
            {
                "name": "customers",
                "columns": [
                    {"name": "id", "type": "INT", "nullable": false},
                    {"name": "name", "type": "TEXT", "nullable": false}
                ]
            },
            {
                "name": "x",
                "columns": [{"name": "a", "type": "INT"}]
            },
            {
                "name": "y",
                "columns": [{"name": "b", "type": "INT"}]
            }
        ]
    }))
    .unwrap()
}

#[test]
fn analyze_query_reports_projection_relations_and_types() {
    let analysis = analyze_query(
        "SELECT u.id, CAST(o.total AS TEXT) AS total_text, 1 AS one \
         FROM users AS u JOIN orders AS o ON u.id = o.user_id",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    assert_eq!(analysis.shape, QueryShape::Select);
    assert_eq!(analysis.projections.len(), 3);
    assert_eq!(analysis.projections[0].name.as_deref(), Some("id"));
    assert_eq!(
        analysis.projections[0].transform_kind,
        TransformKind::Direct
    );
    assert_eq!(analysis.projections[1].transform_kind, TransformKind::Cast);
    assert_eq!(analysis.projections[1].cast_type.as_deref(), Some("TEXT"));
    assert_eq!(
        analysis.projections[2].transform_kind,
        TransformKind::Constant
    );

    assert!(analysis
        .relations
        .iter()
        .any(|relation| relation.name == "users" && relation.alias.as_deref() == Some("u")));
    assert!(analysis
        .relations
        .iter()
        .any(|relation| relation.name == "orders" && relation.columns.contains(&"total".into())));
    assert!(analysis
        .base_tables
        .iter()
        .any(|relation| relation.name == "orders"));
    assert!(analysis
        .base_tables
        .iter()
        .any(|relation| relation.name == "users"));

    let total = &analysis.projections[1].upstream;
    assert!(total.iter().any(|reference| {
        reference.table.as_deref() == Some("orders")
            && reference.column == "total"
            && reference.confidence == ReferenceConfidence::Resolved
    }));
}

#[test]
fn analyze_query_duckdb_extract_date_part_types() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{
            "name": "events",
            "columns": [{"name": "created_at", "type": "TIMESTAMP"}]
        }]
    }))
    .unwrap();

    for (date_part, expected_type) in [
        ("'year'", "BIGINT"),
        ("'month'", "BIGINT"),
        ("'day'", "BIGINT"),
        ("YEAR", "BIGINT"),
        ("'second'", "BIGINT"),
        ("'milliseconds'", "BIGINT"),
        ("'microseconds'", "BIGINT"),
        ("'yearweek'", "BIGINT"),
        ("'epoch'", "DOUBLE"),
        ("EPOCH", "DOUBLE"),
        ("'EpOcH'", "DOUBLE"),
        ("'julian'", "DOUBLE"),
        ("JULIAN", "DOUBLE"),
        ("'JuLiAn'", "DOUBLE"),
    ] {
        let sql = format!("SELECT EXTRACT({date_part} FROM created_at) AS extracted FROM events");
        let analysis = analyze_query(
            &sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::DuckDB,
                schema: Some(schema.clone()),
            },
        )
        .unwrap_or_else(|error| panic!("analyze_query failed for {sql:?}: {error}"));

        assert_eq!(analysis.projections[0].name.as_deref(), Some("extracted"));
        assert_eq!(
            analysis.projections[0].type_hint.as_deref(),
            Some(expected_type),
            "unexpected output type for {sql:?}"
        );
        assert!(analysis.projections[0].upstream.iter().any(|reference| {
            reference.table.as_deref() == Some("events") && reference.column == "created_at"
        }));
    }
}

#[test]
fn analyze_query_duckdb_trim_types_and_lineage() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{
            "name": "records",
            "columns": [
                {"name": "name", "type": "VARCHAR"},
                {"name": "chars", "type": "VARCHAR"},
                {"name": "values_json", "type": "JSON"}
            ]
        }]
    }))
    .unwrap();

    for (function, columns) in [
        ("TRIM(name)", vec!["name"]),
        (
            r#"TRIM(BOTH '"' FROM values_json->>0)"#,
            vec!["values_json"],
        ),
        (
            "TRIM(LOWER(TRIM(name)), UPPER(chars))",
            vec!["name", "chars"],
        ),
    ] {
        let sql = format!("SELECT {function} AS normalized FROM records");
        for schema in [None, Some(schema.clone())] {
            let has_schema = schema.is_some();
            let analysis = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    complexity_guard: None,
                    dialect: DialectType::DuckDB,
                    schema,
                },
            )
            .unwrap();
            assert_eq!(analysis.projections.len(), 1, "{sql}");
            let projection = &analysis.projections[0];
            assert_eq!(projection.name.as_deref(), Some("normalized"), "{sql}");
            assert_eq!(projection.type_hint.as_deref(), Some("TEXT"), "{sql}");
            if has_schema {
                assert_eq!(projection.upstream.len(), columns.len(), "{sql}");
                for column in &columns {
                    assert!(
                        projection.upstream.iter().any(|reference| {
                            reference.table.as_deref() == Some("records")
                                && reference.column == *column
                                && reference.confidence == ReferenceConfidence::Resolved
                        }),
                        "{sql}: {column}"
                    );
                }
            }
        }
    }
}

#[test]
fn analyze_query_duckdb_regexp_extract_all_types() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{
            "name": "documents",
            "columns": [{"name": "body", "type": "VARCHAR"}]
        }]
    }))
    .unwrap();
    for (function, type_hint) in [
        ("REGEXP_EXTRACT_ALL(body, '[0-9]+')", "TEXT[]"),
        (
            "REGEXP_EXTRACT_ALL(body, '([a-z])([0-9]+)', 2, 'i')",
            "TEXT[]",
        ),
        (
            "REGEXP_EXTRACT_ALL(body, '([a-z])([0-9]+)', ['letter', 'number'])",
            "STRUCT(letter TEXT, number TEXT)[]",
        ),
        ("UNNEST(REGEXP_EXTRACT_ALL(body, '[0-9]+'))", "TEXT"),
        (
            "UNNEST(REGEXP_EXTRACT_ALL(body, '([a-z])([0-9]+)', ['letter', 'number'], 'i'))",
            "STRUCT(letter TEXT, number TEXT)",
        ),
    ] {
        let sql = format!("SELECT {function} AS matches FROM documents");
        for schema in [None, Some(schema.clone())] {
            let has_schema = schema.is_some();
            let analysis = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    complexity_guard: None,
                    dialect: DialectType::DuckDB,
                    schema,
                },
            )
            .unwrap();
            assert_eq!(analysis.projections.len(), 1, "{sql}");
            let projection = &analysis.projections[0];
            assert_eq!(projection.name.as_deref(), Some("matches"), "{sql}");
            assert_eq!(projection.type_hint.as_deref(), Some(type_hint), "{sql}");
            if has_schema {
                assert!(
                    projection.upstream.iter().any(|reference| {
                        reference.table.as_deref() == Some("documents")
                            && reference.column == "body"
                            && reference.confidence == ReferenceConfidence::Resolved
                    }),
                    "{sql}"
                );
            }
        }
    }
}

#[test]
fn analyze_query_duckdb_date_name_types() {
    for input_type in ["DATE", "TIMESTAMP", "TIMESTAMPTZ"] {
        let schema: ValidationSchema = serde_json::from_value(json!({
            "tables": [{
                "name": "events",
                "columns": [{"name": "created_at", "type": input_type}]
            }]
        }))
        .unwrap();
        for function in ["MONTHNAME", "DAYNAME"] {
            let sql = format!("SELECT {function}(created_at) AS label FROM events");
            for schema in [None, Some(schema.clone())] {
                let has_schema = schema.is_some();
                let analysis = analyze_query(
                    &sql,
                    AnalyzeQueryOptions {
                        complexity_guard: None,
                        dialect: DialectType::DuckDB,
                        schema,
                    },
                )
                .unwrap();
                assert_eq!(analysis.projections.len(), 1, "{sql}");
                let projection = &analysis.projections[0];
                assert_eq!(projection.name.as_deref(), Some("label"), "{sql}");
                assert_eq!(projection.type_hint.as_deref(), Some("TEXT"), "{sql}");
                assert!(
                    projection.upstream.iter().any(|reference| {
                        reference.column == "created_at"
                            && (!has_schema
                                || (reference.table.as_deref() == Some("events")
                                    && reference.confidence == ReferenceConfidence::Resolved))
                    }),
                    "{sql}"
                );
            }
        }
    }
}

#[test]
fn analyze_query_duckdb_array_to_string_types() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{
            "name": "events",
            "columns": [
                {"name": "labels", "type": "VARCHAR[]"},
                {"name": "label", "type": "VARCHAR"}
            ]
        }]
    }))
    .unwrap();

    for (function, column) in [
        ("ARRAY_TO_STRING(labels, ', ')", "labels"),
        ("ARRAY_TO_STRING(ARRAY_AGG(label), ', ')", "label"),
        ("ARRAY_TO_STRING_COMMA_DEFAULT(labels)", "labels"),
        ("ARRAY_TO_STRING_COMMA_DEFAULT(ARRAY_AGG(label))", "label"),
    ] {
        let sql = format!("SELECT {function} AS label_text FROM events");
        for schema in [None, Some(schema.clone())] {
            let has_schema = schema.is_some();
            let analysis = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    complexity_guard: None,
                    dialect: DialectType::DuckDB,
                    schema,
                },
            )
            .unwrap_or_else(|error| panic!("analyze_query failed for {sql:?}: {error}"));
            assert_eq!(analysis.projections.len(), 1, "{sql}");
            let projection = &analysis.projections[0];
            assert_eq!(projection.name.as_deref(), Some("label_text"), "{sql}");
            assert_eq!(projection.type_hint.as_deref(), Some("TEXT"), "{sql}");
            if has_schema {
                assert!(
                    projection.upstream.iter().any(|reference| {
                        reference.table.as_deref() == Some("events")
                            && reference.column == column
                            && reference.confidence == ReferenceConfidence::Resolved
                    }),
                    "{sql}"
                );
            }
        }
    }
}

#[test]
fn analyze_query_reports_function_projection_arguments() {
    let analysis = analyze_query(
        "SELECT DATE_TRUNC('month', created_at) AS bucket FROM events",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(
                serde_json::from_value(json!({
                    "tables": [
                        {
                            "name": "events",
                            "columns": [
                                {"name": "created_at", "type": "TIMESTAMP"}
                            ]
                        }
                    ]
                }))
                .unwrap(),
            ),
        },
    )
    .unwrap();

    let transform_function = analysis.projections[0]
        .transform_function
        .as_ref()
        .expect("expected transform function fact");
    assert_eq!(transform_function.name, "DATE_TRUNC");
    assert_eq!(transform_function.literal_args, vec!["month"]);
    assert_eq!(transform_function.column_args.len(), 1);
    assert_eq!(
        transform_function.column_args[0].table.as_deref(),
        Some("events")
    );
    assert_eq!(transform_function.column_args[0].column, "created_at");
    assert_eq!(
        analysis.projections[0].type_hint.as_deref(),
        Some("TIMESTAMP")
    );
}

#[test]
fn analyze_query_follows_cte_lineage() {
    let analysis = analyze_query(
        "WITH base AS (SELECT id FROM users) SELECT id FROM base",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    assert_eq!(analysis.ctes, vec!["base"]);
    assert!(analysis.projections[0].upstream.iter().any(|reference| {
        reference.table.as_deref() == Some("users") && reference.column == "id"
    }));
    assert_eq!(analysis.base_tables.len(), 1);
    assert_eq!(analysis.base_tables[0].name, "users");
}

#[test]
fn analyze_query_reports_top_level_cte_facts() {
    let analysis = analyze_query(
        "WITH base(order_id, amount) AS (SELECT id, amount FROM orders), \
         nested AS (WITH inner_cte AS (SELECT id FROM users) SELECT id FROM inner_cte) \
         SELECT order_id FROM base",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    assert_eq!(analysis.cte_facts.len(), 2);
    assert_eq!(analysis.cte_facts[0].name, "base");
    assert_eq!(analysis.cte_facts[0].columns, vec!["order_id", "amount"]);
    assert!(analysis.cte_facts[0]
        .body_sql
        .contains("SELECT id, amount FROM orders"));
    assert_eq!(analysis.cte_facts[0].output_columns, vec!["id", "amount"]);

    assert_eq!(analysis.cte_facts[1].name, "nested");
    assert!(analysis.cte_facts.iter().all(|cte| cte.name != "inner_cte"));
}

#[test]
fn analyze_query_reports_original_cte_body_sql_before_schema_rewrites() {
    let analysis = analyze_query(
        "WITH base AS (SELECT amount FROM orders) SELECT amount FROM base",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    assert_eq!(analysis.cte_facts.len(), 1);
    assert_eq!(analysis.cte_facts[0].body_sql, "SELECT amount FROM orders");
}

#[test]
fn analyze_query_reports_set_operations() {
    let analysis = analyze_query(
        "SELECT a FROM x UNION ALL SELECT b FROM y",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    assert_eq!(analysis.shape, QueryShape::SetOperation);
    assert_eq!(analysis.set_operations.len(), 1);
    assert_eq!(analysis.set_operations[0].kind, "union");
    assert!(analysis.set_operations[0].all);
    assert_eq!(analysis.set_operations[0].output_columns, vec!["a"]);
    assert_eq!(analysis.set_operations[0].branches.len(), 2);
    assert_eq!(
        analysis.set_operations[0]
            .branches
            .iter()
            .map(|branch| branch.role)
            .collect::<Vec<_>>(),
        vec![SetOperationBranchRole::Value, SetOperationBranchRole::Value]
    );
    assert!(analysis
        .relations
        .iter()
        .any(|relation| relation.name == "x"));
    assert!(analysis
        .relations
        .iter()
        .any(|relation| relation.name == "y"));
    assert!(analysis
        .base_tables
        .iter()
        .any(|relation| relation.name == "x"));
    assert!(analysis
        .base_tables
        .iter()
        .any(|relation| relation.name == "y"));
    assert_eq!(
        analysis.set_operations[0].branches[0].projections[0]
            .name
            .as_deref(),
        Some("a")
    );

    for operator in ["EXCEPT", "INTERSECT"] {
        let analysis = analyze_query(
            &format!("SELECT a FROM x {operator} SELECT b FROM y"),
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::Generic,
                schema: Some(schema()),
            },
        )
        .unwrap();
        assert_eq!(
            analysis.set_operations[0]
                .branches
                .iter()
                .map(|branch| branch.role)
                .collect::<Vec<_>>(),
            vec![
                SetOperationBranchRole::Value,
                SetOperationBranchRole::Filter
            ]
        );
    }
}

#[test]
fn analyze_query_reports_name_aligned_set_operation_outputs() {
    let sql = "SELECT 1 AS left_value UNION ALL BY NAME SELECT 2 AS right_value";
    for dialect in [DialectType::DuckDB, DialectType::Snowflake] {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect,
                schema: None,
            },
        )
        .unwrap_or_else(|error| panic!("analysis failed for {dialect:?}: {error}"));

        assert_eq!(
            analysis.set_operations[0].output_columns,
            vec!["left_value", "right_value"]
        );
        assert_eq!(
            analysis
                .projections
                .iter()
                .map(|projection| projection.name.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("left_value"), Some("right_value")]
        );
        assert!(analysis
            .projections
            .iter()
            .all(|projection| projection.nullability == ProjectionNullability::Nullable));
    }
}

#[test]
fn analyze_query_reports_bigquery_name_alignment_modes() {
    for (sql, expected_names, expected_nullability) in [
        (
            "SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a",
            vec!["a", "b"],
            vec![
                ProjectionNullability::NonNull,
                ProjectionNullability::NonNull,
            ],
        ),
        (
            "SELECT 1 AS a, 2 AS b INNER UNION ALL BY NAME SELECT 3 AS b, 4 AS c",
            vec!["b"],
            vec![ProjectionNullability::NonNull],
        ),
        (
            "SELECT 1 AS a, 2 AS b FULL OUTER UNION ALL BY NAME SELECT 3 AS b, 4 AS c",
            vec!["a", "b", "c"],
            vec![
                ProjectionNullability::Nullable,
                ProjectionNullability::NonNull,
                ProjectionNullability::Nullable,
            ],
        ),
        (
            "SELECT 1 AS a, 2 AS b FULL OUTER UNION ALL BY NAME ON (c, a) SELECT 3 AS b, 4 AS c",
            vec!["c", "a"],
            vec![
                ProjectionNullability::Nullable,
                ProjectionNullability::Nullable,
            ],
        ),
    ] {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::BigQuery,
                schema: None,
            },
        )
        .unwrap_or_else(|error| panic!("analysis failed for {sql:?}: {error}"));

        assert_eq!(analysis.set_operations[0].output_columns, expected_names);
        assert!(
            analysis
                .projections
                .iter()
                .all(|p| p.type_hint.as_deref() == Some("INT64")),
            "{sql}"
        );
        assert_eq!(
            analysis
                .projections
                .iter()
                .map(|projection| projection.nullability)
                .collect::<Vec<_>>(),
            expected_nullability
        );
    }
}

#[test]
fn analyze_query_rejects_non_query_statements() {
    let err = analyze_query(
        "CREATE TABLE t (a INT)",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: None,
        },
    )
    .unwrap_err();

    assert!(err.to_string().contains("requires a SELECT"));
}

#[test]
fn analyze_query_preserves_physical_table_aliases_in_lineage() {
    let analysis = analyze_query(
        "SELECT o.id FROM orders AS o",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    let reference = analysis.projections[0]
        .upstream
        .iter()
        .find(|reference| reference.column == "id")
        .unwrap();
    assert_eq!(reference.source_name.as_deref(), Some("orders"));
    assert_eq!(reference.source_alias.as_deref(), Some("o"));
    assert_eq!(reference.table.as_deref(), Some("orders"));
}

#[test]
fn analyze_query_limits_qualified_star_to_matching_source() {
    let analysis = analyze_query(
        "SELECT o.* FROM orders AS o JOIN customers AS c ON o.customer_id = c.id",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    assert_eq!(analysis.projections.len(), 5);
    let mut projection_names: Vec<_> = analysis
        .projections
        .iter()
        .filter_map(|projection| projection.name.as_deref())
        .collect();
    projection_names.sort_unstable();
    assert_eq!(
        projection_names,
        vec!["amount", "customer_id", "id", "total", "user_id"]
    );
    assert!(analysis
        .projections
        .iter()
        .all(|projection| !projection.is_star));
    assert_eq!(analysis.star_projections.len(), 1);
    assert_eq!(analysis.star_projections[0].index, 0);
    assert_eq!(analysis.star_projections[0].table.as_deref(), Some("o"));
    let mut expanded = analysis.star_projections[0].expanded_columns.clone();
    expanded.sort();
    assert_eq!(
        expanded,
        vec!["amount", "customer_id", "id", "total", "user_id"]
    );
    assert!(analysis.projections.iter().all(|projection| {
        projection
            .upstream
            .iter()
            .all(|reference| reference.table.as_deref() == Some("orders"))
    }));
}

#[test]
fn analyze_query_resolves_unique_unqualified_columns_with_alias_schema() {
    let analysis = analyze_query(
        "SELECT amount FROM orders AS o JOIN customers AS c ON o.customer_id = c.id",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    let reference = analysis.projections[0]
        .upstream
        .iter()
        .find(|reference| reference.column == "amount")
        .unwrap();
    assert_eq!(reference.table.as_deref(), Some("orders"));
    assert_eq!(reference.source_alias.as_deref(), Some("o"));
    assert_eq!(reference.confidence, ReferenceConfidence::Resolved);
}

#[test]
fn analyze_query_resolves_natural_join_merged_column_with_schema() {
    let natural_join_schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [
            {
                "name": "source_table",
                "columns": [
                    {"name": "shared_key", "type": "VARCHAR"}
                ]
            }
        ]
    }))
    .unwrap();
    let analysis = analyze_query(
        "SELECT shared_key AS output_key FROM source_table \
         NATURAL JOIN (SELECT shared_key FROM source_table) derived",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(natural_join_schema),
        },
    )
    .unwrap();

    let projection = &analysis.projections[0];
    assert_eq!(projection.name.as_deref(), Some("output_key"));
    assert_eq!(projection.type_hint.as_deref(), Some("TEXT"));
    assert!(
        !projection.upstream.is_empty(),
        "merged column should retain physical upstreams"
    );
    assert!(projection.upstream.iter().all(|reference| {
        reference.column == "shared_key"
            && reference.source_name.as_deref() == Some("source_table")
            && reference.table.as_deref() == Some("source_table")
            && reference.source_kind == SourceKind::Table
            && !reference.unqualified
            && reference.confidence == ReferenceConfidence::Resolved
    }));
}

#[test]
fn analyze_query_propagates_schema_type_through_anonymous_derived_table() {
    let derived_table_schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [
            {
                "name": "source_table",
                "columns": [
                    {"name": "source_value", "type": "VARCHAR"}
                ]
            }
        ]
    }))
    .unwrap();

    for sql in [
        "SELECT source_value FROM (SELECT source_value FROM source_table)",
        "SELECT derived.source_value FROM (SELECT source_value FROM source_table) AS derived",
    ] {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::DuckDB,
                schema: Some(derived_table_schema.clone()),
            },
        )
        .unwrap();

        let projection = &analysis.projections[0];
        assert_eq!(projection.type_hint.as_deref(), Some("TEXT"), "{sql}");
        assert!(
            projection.upstream.iter().any(|reference| {
                reference.column == "source_value"
                    && reference.source_name.as_deref() == Some("source_table")
                    && reference.table.as_deref() == Some("source_table")
                    && reference.source_kind == SourceKind::Table
                    && reference.confidence == ReferenceConfidence::Resolved
            }),
            "{sql}: {:#?}",
            projection.upstream
        );
    }
}

#[test]
fn analyze_query_preserves_precise_schema_type_hints() {
    let analysis = analyze_query(
        "SELECT amount FROM orders",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    assert_eq!(
        analysis.projections[0].type_hint.as_deref(),
        Some("DECIMAL(10, 2)")
    );
}

#[test]
fn analyze_query_expands_unqualified_star_with_schema() {
    let analysis = analyze_query(
        "SELECT * FROM orders",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    let mut names: Vec<_> = analysis
        .projections
        .iter()
        .filter_map(|projection| projection.name.as_deref())
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["amount", "customer_id", "id", "total", "user_id"]
    );
    assert!(analysis
        .projections
        .iter()
        .all(|projection| !projection.is_star));
    assert_eq!(analysis.star_projections.len(), 1);
    assert_eq!(analysis.star_projections[0].index, 0);
    assert_eq!(analysis.star_projections[0].table, None);
    let mut expanded = analysis.star_projections[0].expanded_columns.clone();
    expanded.sort();
    assert_eq!(
        expanded,
        vec!["amount", "customer_id", "id", "total", "user_id"]
    );
}

#[test]
fn analyze_query_classifies_typed_aggregates() {
    let analysis = analyze_query(
        "SELECT COUNT(*) AS rows, SUM(amount) AS amount_sum FROM orders",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    assert_eq!(
        analysis.projections[0].transform_kind,
        TransformKind::Aggregation
    );
    assert_eq!(
        analysis.projections[1].transform_kind,
        TransformKind::Aggregation
    );
    assert_eq!(
        analysis.projections[0].nullability,
        ProjectionNullability::NonNull
    );

    let duckdb_analysis = analyze_query(
        "SELECT COUNT_IF(numeric_value > 0), MEDIAN(numeric_value), FIRST(numeric_value), ARG_MAX_NULL(label, numeric_value), ARG_MIN_NULL(label, numeric_value) FROM source_table",
        AnalyzeQueryOptions {
complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: None,
        },
    )
    .unwrap();

    assert_eq!(duckdb_analysis.projections.len(), 5);
    assert!(duckdb_analysis
        .projections
        .iter()
        .all(|projection| projection.transform_kind == TransformKind::Aggregation));
}

#[test]
fn analyze_query_infers_duckdb_median_type_issue_425() {
    let median_schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{
            "name": "values_table",
            "columns": [{"name": "x", "type": "DOUBLE"}]
        }]
    }))
    .unwrap();

    let analysis = analyze_query(
        "SELECT MEDIAN(x) AS median_x FROM values_table",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(median_schema),
        },
    )
    .unwrap();

    assert_eq!(analysis.projections.len(), 1);
    assert_eq!(
        analysis.projections[0].transform_kind,
        TransformKind::Aggregation
    );
    assert_eq!(analysis.projections[0].type_hint.as_deref(), Some("DOUBLE"));
}

#[test]
fn analyze_query_preserves_nullability_through_query_scopes() {
    for dialect in [
        DialectType::Snowflake,
        DialectType::DuckDB,
        DialectType::PostgreSQL,
        DialectType::BigQuery,
    ] {
        for (nullable, expected) in [
            (Some(false), ProjectionNullability::NonNull),
            (Some(true), ProjectionNullability::Nullable),
            (None, ProjectionNullability::Unknown),
        ] {
            let schema: ValidationSchema = serde_json::from_value(json!({"tables": [{"name": "orders", "columns": [{"name": "amount", "type": "INTEGER", "nullable": nullable}]}]})).unwrap();
            for sql in [
                "SELECT amount FROM orders",
                "WITH typed AS (SELECT amount FROM orders) SELECT amount FROM typed",
                "WITH a AS (SELECT amount FROM orders), b AS (SELECT amount FROM a) SELECT amount FROM b",
                "WITH typed AS (SELECT amount AS value FROM orders) SELECT value FROM typed",
                "WITH typed(value) AS (SELECT amount FROM orders) SELECT value FROM typed",
                "WITH a(n) AS (SELECT amount FROM orders), b(m) AS (SELECT n FROM a) SELECT m FROM b",
                "WITH typed AS (SELECT amount FROM orders) SELECT t.amount FROM typed AS t",
                "SELECT amount FROM (SELECT amount FROM orders) AS typed",
                "SELECT value FROM (SELECT amount FROM orders) AS typed(value)",
                "WITH typed AS (SELECT * FROM orders) SELECT amount FROM typed",
                "WITH typed AS (SELECT amount FROM orders) SELECT amount FROM (SELECT * FROM typed) AS d",
                "WITH unused AS (SELECT NULL AS amount), typed AS (SELECT amount FROM orders) SELECT amount FROM typed",
            ] {
                let analysis = analyze_query(sql, AnalyzeQueryOptions {
complexity_guard: None, dialect, schema: Some(schema.clone()) }).unwrap();
                assert_eq!(analysis.projections[0].nullability, expected, "{dialect:?}, {nullable:?}: {sql}");
            }
        }
    }
}

#[test]
fn analyze_query_propagates_expression_nullability_without_schema() {
    for (body, expected) in [
        ("1", ProjectionNullability::NonNull),
        ("NULL", ProjectionNullability::Nullable),
        ("COUNT(*)", ProjectionNullability::NonNull),
        ("CAST(1 AS INT)", ProjectionNullability::NonNull),
        ("CAST(NULL AS INT)", ProjectionNullability::Nullable),
        ("COALESCE(NULL, 0)", ProjectionNullability::NonNull),
        ("COALESCE(NULL, NULL)", ProjectionNullability::Nullable),
        ("TRY_CAST('bad' AS INT)", ProjectionNullability::Unknown),
        ("unknown_function(1)", ProjectionNullability::Unknown),
    ] {
        for sql in [
            format!("WITH a AS (SELECT {body} AS x), b AS (SELECT x FROM a) SELECT x FROM b"),
            format!("SELECT x FROM (SELECT {body} AS x) AS d"),
        ] {
            let analysis = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    complexity_guard: None,
                    dialect: DialectType::DuckDB,
                    schema: None,
                },
            )
            .unwrap();
            assert_eq!(analysis.projections[0].nullability, expected, "{sql}");
        }
    }
}

#[test]
fn analyze_query_nullability_respects_cte_shadowing_and_output_positions() {
    for (sql, expected) in [
        ("WITH orders AS (SELECT NULL AS id) SELECT id FROM orders", ProjectionNullability::Nullable),
        ("WITH orders AS (SELECT id FROM orders) SELECT id FROM orders", ProjectionNullability::NonNull),
        ("WITH a AS (SELECT 1 AS x), b AS (WITH a AS (SELECT NULL AS x) SELECT x FROM a) SELECT x FROM b", ProjectionNullability::Nullable),
        ("WITH a AS (SELECT NULL AS x), b AS (WITH a AS (SELECT 1 AS x) SELECT x FROM a) SELECT x FROM b", ProjectionNullability::NonNull),
        ("WITH a AS (SELECT NULL AS x), b AS (SELECT x FROM a), c AS (WITH a AS (SELECT 1 AS x) SELECT x FROM b) SELECT x FROM c", ProjectionNullability::Nullable),
        ("WITH a(x,y) AS (SELECT id, amount FROM orders) SELECT y FROM a", ProjectionNullability::Nullable),
        ("WITH a(x,y) AS (SELECT id, amount FROM orders) SELECT x FROM a", ProjectionNullability::NonNull),
        ("WITH a AS (SELECT NULL AS x), b AS (SELECT 1 AS x) SELECT x FROM b", ProjectionNullability::NonNull),
    ] {
        let analysis = analyze_query(sql, AnalyzeQueryOptions {
complexity_guard: None, dialect: DialectType::DuckDB, schema: Some(schema()) }).unwrap();
        assert_eq!(analysis.projections[0].nullability, expected, "{sql}");
    }
    let analysis = analyze_query(
        "WITH a AS (SELECT 1 AS \"x\", NULL AS \"X\") SELECT \"x\", \"X\" FROM a",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Snowflake,
            schema: None,
        },
    )
    .unwrap();
    assert_eq!(
        analysis
            .projections
            .iter()
            .map(|p| p.nullability)
            .collect::<Vec<_>>(),
        vec![
            ProjectionNullability::NonNull,
            ProjectionNullability::Nullable
        ]
    );
}

#[test]
fn analyze_query_nullability_resolves_quoted_source_names() {
    for (sql, expected) in [
        ("WITH d AS (SELECT 1 AS x) SELECT \"D\".x FROM d", ProjectionNullability::NonNull),
        ("SELECT a.x FROM (SELECT NULL AS x) AS \"a\" CROSS JOIN (SELECT 1 AS x) AS \"A\"", ProjectionNullability::NonNull),
        ("SELECT \"a\".x FROM (SELECT NULL AS x) AS \"a\" CROSS JOIN (SELECT 1 AS x) AS \"A\"", ProjectionNullability::Nullable),
        ("SELECT \"A\".x FROM (SELECT 1 AS x) AS \"A\" LEFT JOIN (SELECT 2 AS x) AS \"a\" ON TRUE", ProjectionNullability::NonNull),
        ("SELECT \"a\".x FROM (SELECT 1 AS x) AS \"A\" LEFT JOIN (SELECT 2 AS x) AS \"a\" ON TRUE", ProjectionNullability::Nullable),
        ("WITH \"a\" AS (SELECT NULL AS x), \"A\" AS (SELECT 1 AS x) SELECT x FROM a", ProjectionNullability::NonNull),
        ("WITH \"a\" AS (SELECT NULL AS x), \"A\" AS (SELECT 1 AS x) SELECT x FROM \"a\"", ProjectionNullability::Nullable),
    ] {
        let analysis = analyze_query(sql, AnalyzeQueryOptions {
complexity_guard: None, dialect: DialectType::Snowflake, schema: None }).unwrap();
        assert_eq!(analysis.projections[0].nullability, expected, "{sql}");
    }
}

#[test]
fn analyze_query_nullability_preserves_nested_outer_join_effects() {
    for (sql, expected) in [
        ("WITH a AS (SELECT o.id FROM users u LEFT JOIN orders o ON TRUE) SELECT id FROM a", ProjectionNullability::Nullable),
        ("WITH a AS (SELECT u.id FROM users u RIGHT JOIN orders o ON TRUE) SELECT id FROM a", ProjectionNullability::Nullable),
        ("WITH a AS (SELECT o.id FROM users u FULL JOIN orders o ON TRUE) SELECT id FROM a", ProjectionNullability::Nullable),
        ("WITH a AS (SELECT o.id FROM users u INNER JOIN orders o ON TRUE) SELECT id FROM a", ProjectionNullability::NonNull),
        ("WITH a AS (SELECT id FROM orders) SELECT a.id FROM users u LEFT JOIN a ON TRUE", ProjectionNullability::Nullable),
        ("WITH a AS (SELECT COALESCE(o.id, 0) AS id FROM users u LEFT JOIN orders o ON TRUE) SELECT id FROM a", ProjectionNullability::NonNull),
        ("WITH a AS (SELECT COALESCE(amount, 0) AS id FROM orders) SELECT a.id FROM users u LEFT JOIN a ON TRUE", ProjectionNullability::Nullable),
        ("WITH a AS (SELECT o.id FROM users u LEFT JOIN orders o ON TRUE) SELECT COALESCE(id, 0) FROM a", ProjectionNullability::NonNull),
    ] {
        let analysis = analyze_query(sql, AnalyzeQueryOptions {
complexity_guard: None, dialect: DialectType::DuckDB, schema: Some(schema()) }).unwrap();
        assert_eq!(analysis.projections[0].nullability, expected, "{sql}");
    }
}

#[test]
fn analyze_query_nullability_keeps_uncertain_sources_conservative() {
    for sql in [
        "WITH a AS (SELECT missing FROM orders) SELECT missing FROM a",
        "WITH a AS (SELECT id FROM missing_table) SELECT id FROM a",
        "WITH a AS (SELECT id FROM orders o JOIN users u ON TRUE) SELECT id FROM a",
        "WITH a AS (SELECT id FROM orders o JOIN missing_table m ON TRUE) SELECT id FROM a",
        "WITH a AS (SELECT 1 AS x, NULL AS x) SELECT x FROM a",
        "WITH RECURSIVE a(x) AS (SELECT 1 UNION ALL SELECT x FROM a) SELECT x FROM a",
        "WITH a AS (SELECT (SELECT id FROM orders) AS id) SELECT id FROM a",
    ] {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::DuckDB,
                schema: Some(schema()),
            },
        )
        .unwrap();
        assert_eq!(
            analysis.projections[0].nullability,
            ProjectionNullability::Unknown,
            "{sql}"
        );
    }
}

#[test]
fn analyze_query_nullability_accounts_for_all_set_operation_branches() {
    for (body, expected) in [
        (
            "SELECT id AS x FROM orders UNION ALL SELECT NULL AS x",
            ProjectionNullability::Nullable,
        ),
        (
            "SELECT NULL AS x UNION ALL SELECT id AS x FROM orders",
            ProjectionNullability::Nullable,
        ),
        (
            "SELECT id AS x FROM orders UNION ALL SELECT id AS x FROM users",
            ProjectionNullability::NonNull,
        ),
        (
            "SELECT id AS x FROM orders UNION ALL SELECT total AS x FROM orders",
            ProjectionNullability::Unknown,
        ),
        (
            "SELECT 1 AS x UNION ALL BY NAME SELECT 2 AS y",
            ProjectionNullability::Nullable,
        ),
        (
            "SELECT 1 AS x, NULL AS y UNION ALL BY NAME SELECT 2 AS y, NULL AS x",
            ProjectionNullability::Nullable,
        ),
        (
            "SELECT id AS x FROM orders EXCEPT SELECT NULL AS x",
            ProjectionNullability::NonNull,
        ),
        (
            "SELECT id AS x FROM orders INTERSECT SELECT NULL AS x",
            ProjectionNullability::NonNull,
        ),
    ] {
        for sql in [
            body.to_string(),
            format!("WITH a AS ({body}) SELECT x FROM a"),
        ] {
            let analysis = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    complexity_guard: None,
                    dialect: DialectType::DuckDB,
                    schema: Some(schema()),
                },
            )
            .unwrap();
            assert_eq!(analysis.projections[0].nullability, expected, "{sql}");
        }
    }
    let analysis = analyze_query(
        "WITH a AS (SELECT id FROM orders) SELECT id FROM a UNION ALL SELECT amount FROM orders",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(schema()),
        },
    )
    .unwrap();
    assert_eq!(
        analysis.set_operations[0].branches[0].projections[0].nullability,
        ProjectionNullability::NonNull
    );
    assert_eq!(
        analysis.set_operations[0].branches[1].projections[0].nullability,
        ProjectionNullability::Nullable
    );
}

#[test]
fn analyze_query_nullability_bounds_deep_dependencies_and_reuses_outputs() {
    for (length, expected) in [
        (8, ProjectionNullability::NonNull),
        (140, ProjectionNullability::Unknown),
    ] {
        let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_string()];
        for index in 1..length {
            ctes.push(format!("c{index} AS (SELECT x FROM c{})", index - 1));
        }
        let sql = format!(
            "WITH {} SELECT x, x AS again FROM c{}",
            ctes.join(", "),
            length - 1
        );
        let analysis = analyze_query(&sql, AnalyzeQueryOptions::default()).unwrap();
        assert_eq!(
            analysis
                .projections
                .iter()
                .map(|p| p.nullability)
                .collect::<Vec<_>>(),
            vec![expected, expected]
        );
    }
}

#[test]
fn analyze_query_reports_projection_nullability() {
    let analysis = analyze_query(
        "SELECT \
             COUNT(*) AS rows, \
             1 AS one, \
             NULL AS missing, \
             o.amount, \
             COALESCE(o.amount, 0) AS amount_fallback, \
             c.name AS customer_name \
         FROM orders AS o \
         LEFT JOIN customers AS c ON o.customer_id = c.id",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    let nullability: Vec<_> = analysis
        .projections
        .iter()
        .map(|projection| projection.nullability)
        .collect();

    assert_eq!(
        nullability,
        vec![
            ProjectionNullability::NonNull,
            ProjectionNullability::NonNull,
            ProjectionNullability::Nullable,
            ProjectionNullability::Nullable,
            ProjectionNullability::NonNull,
            ProjectionNullability::Nullable,
        ]
    );
}

#[test]
fn analyze_query_marks_outer_join_source_columns_nullable() {
    let right_join = analyze_query(
        "SELECT u.id FROM users AS u RIGHT JOIN orders AS o ON u.id = o.user_id",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();
    assert_eq!(
        right_join.projections[0].nullability,
        ProjectionNullability::Nullable
    );

    let full_join = analyze_query(
        "SELECT u.id, o.id FROM users AS u FULL JOIN orders AS o ON u.id = o.user_id",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();
    assert!(full_join
        .projections
        .iter()
        .all(|projection| projection.nullability == ProjectionNullability::Nullable));
}

#[test]
fn analyze_query_reports_transitive_base_tables() {
    let analysis = analyze_query(
        "WITH paid AS (SELECT customer_id FROM orders) \
         SELECT c.name FROM customers AS c \
         JOIN (SELECT customer_id FROM paid) AS p ON c.id = p.customer_id",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::Generic,
            schema: Some(schema()),
        },
    )
    .unwrap();

    let base_table_names: Vec<_> = analysis
        .base_tables
        .iter()
        .map(|relation| relation.name.as_str())
        .collect();
    assert_eq!(base_table_names, vec!["customers", "orders"]);
    assert!(analysis
        .relations
        .iter()
        .any(|relation| relation.name == "customers"));
    assert!(analysis
        .relations
        .iter()
        .any(|relation| relation.kind == SourceKind::DerivedTable));
}

#[test]
fn analyze_query_reports_structured_physical_table_identity() {
    let analysis = analyze_query(
        r#"SELECT id FROM "my.catalog"."my.schema"."orders.table" AS o"#,
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: None,
        },
    )
    .unwrap();

    let relation = analysis
        .relations
        .iter()
        .find(|relation| relation.kind == SourceKind::Table)
        .unwrap();
    assert_eq!(relation.name, "my.catalog.my.schema.orders.table");
    assert_eq!(relation.catalog.as_deref(), Some("my.catalog"));
    assert_eq!(relation.schema.as_deref(), Some("my.schema"));
    assert_eq!(relation.table.as_deref(), Some("orders.table"));
    assert_eq!(relation.alias.as_deref(), Some("o"));

    assert_eq!(analysis.base_tables.len(), 1);
    assert_eq!(analysis.base_tables[0].name, relation.name);
    assert_eq!(analysis.base_tables[0].catalog, relation.catalog);
    assert_eq!(analysis.base_tables[0].schema, relation.schema);
    assert_eq!(analysis.base_tables[0].table, relation.table);
}

#[test]
fn analyze_query_reports_structured_table_identity_for_qualified_and_derived_sources() {
    let analysis = analyze_query(
        "SELECT x FROM (SELECT id AS x FROM mycatalog.myschema.orders) d",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: None,
        },
    )
    .unwrap();

    let derived = analysis
        .relations
        .iter()
        .find(|relation| relation.name == "d")
        .unwrap();
    assert_eq!(derived.kind, SourceKind::DerivedTable);
    assert_eq!(derived.catalog, None);
    assert_eq!(derived.schema, None);
    assert_eq!(derived.table, None);

    assert_eq!(analysis.base_tables.len(), 1);
    let base_table = &analysis.base_tables[0];
    assert_eq!(base_table.name, "mycatalog.myschema.orders");
    assert_eq!(base_table.catalog.as_deref(), Some("mycatalog"));
    assert_eq!(base_table.schema.as_deref(), Some("myschema"));
    assert_eq!(base_table.table.as_deref(), Some("orders"));
}

#[test]
fn analyze_query_reports_base_tables_inside_derived_table() {
    let analysis = analyze_query(
        "SELECT x FROM (SELECT id AS x FROM orders) d",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: None,
        },
    )
    .unwrap();

    let base_table_names: Vec<_> = analysis
        .base_tables
        .iter()
        .map(|relation| relation.name.as_str())
        .collect();
    assert_eq!(base_table_names, vec!["orders"]);
    assert!(analysis
        .relations
        .iter()
        .any(|relation| relation.name == "d" && relation.kind == SourceKind::DerivedTable));
}

#[test]
fn analyze_query_reports_base_tables_inside_derived_table_set_operation() {
    let analysis = analyze_query(
        "SELECT s FROM (SELECT a AS s FROM orders UNION ALL SELECT a AS s FROM users) u",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: None,
        },
    )
    .unwrap();

    let base_table_names: Vec<_> = analysis
        .base_tables
        .iter()
        .map(|relation| relation.name.as_str())
        .collect();
    assert_eq!(base_table_names, vec!["orders", "users"]);
    assert!(analysis
        .relations
        .iter()
        .any(|relation| relation.name == "u" && relation.kind == SourceKind::DerivedTable));
}

fn single_column_schema(table_names: &[&str], column_name: &str) -> ValidationSchema {
    let tables: Vec<_> = table_names
        .iter()
        .map(|name| {
            json!({
                "name": name,
                "columns": [{"name": column_name, "type": "INT"}]
            })
        })
        .collect();

    serde_json::from_value(json!({ "tables": tables })).unwrap()
}

fn unnest_analysis_schema() -> ValidationSchema {
    serde_json::from_value(json!({
        "tables": [
            {
                "name": "t",
                "columns": [{"name": "arr", "type": "INT"}]
            }
        ]
    }))
    .unwrap()
}

fn struct_field_analysis_schema() -> ValidationSchema {
    serde_json::from_value(json!({
        "tables": [{
            "name": "source_table",
            "columns": [
                {"name": "nested_items", "type": "STRUCT(field_value VARCHAR)[]"},
                {"name": "composite_value", "type": "STRUCT(field_value VARCHAR, label VARCHAR)"}
            ]
        }]
    }))
    .unwrap()
}

#[test]
fn analyze_query_resolves_nested_set_operation_inside_derived_table() {
    let analysis = analyze_query(
        "SELECT v FROM ((SELECT v FROM t1 UNION ALL SELECT v FROM t2) \
         UNION ALL SELECT v FROM t3) u",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(single_column_schema(&["t1", "t2", "t3"], "v")),
        },
    )
    .unwrap();

    let upstream_tables: Vec<_> = analysis.projections[0]
        .upstream
        .iter()
        .filter_map(|reference| reference.table.as_deref())
        .collect();
    assert!(upstream_tables.contains(&"t1"));
    assert!(upstream_tables.contains(&"t2"));
    assert!(upstream_tables.contains(&"t3"));

    let base_table_names: Vec<_> = analysis
        .base_tables
        .iter()
        .map(|relation| relation.name.as_str())
        .collect();
    assert_eq!(base_table_names, vec!["t1", "t2", "t3"]);
}

#[test]
fn analyze_query_resolves_mixed_nested_set_operation_arm_inside_derived_table() {
    let analysis = analyze_query(
        "SELECT v FROM (SELECT v FROM t0 UNION ALL \
         (SELECT v FROM t1 UNION ALL SELECT v FROM t2)) u",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(single_column_schema(&["t0", "t1", "t2"], "v")),
        },
    )
    .unwrap();

    let upstream_tables: Vec<_> = analysis.projections[0]
        .upstream
        .iter()
        .filter_map(|reference| reference.table.as_deref())
        .collect();
    assert!(upstream_tables.contains(&"t0"));
    assert!(upstream_tables.contains(&"t1"));
    assert!(upstream_tables.contains(&"t2"));
}

#[test]
fn analyze_query_resolves_nested_set_operation_inside_cte_with_schema() {
    let analysis = analyze_query(
        "WITH c AS (SELECT v FROM ((SELECT v FROM t1 UNION ALL SELECT v FROM t2) \
         UNION ALL SELECT v FROM t3) u) SELECT v FROM c",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(single_column_schema(&["t1", "t2", "t3"], "v")),
        },
    )
    .unwrap();

    let upstream_tables: Vec<_> = analysis.projections[0]
        .upstream
        .iter()
        .filter_map(|reference| reference.table.as_deref())
        .collect();
    assert!(upstream_tables.contains(&"t1"));
    assert!(upstream_tables.contains(&"t2"));
    assert!(upstream_tables.contains(&"t3"));
}

#[test]
fn analyze_query_resolves_unnest_virtual_output_aliases_with_schema() {
    for sql in [
        "SELECT i FROM t, UNNEST(t.arr) AS i",
        "SELECT i FROM t, UNNEST(t.arr) AS u(i)",
        "SELECT u.i FROM t, UNNEST(t.arr) AS u(i)",
    ] {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::DuckDB,
                schema: Some(unnest_analysis_schema()),
            },
        )
        .unwrap_or_else(|error| panic!("analyze_query failed for {sql:?}: {error}"));

        assert!(
            analysis.projections[0]
                .upstream
                .iter()
                .any(|reference| reference.table.as_deref() == Some("t")
                    && reference.column == "arr"),
            "expected t.arr upstream for {sql:?}, got {:?}",
            analysis.projections[0].upstream
        );
    }
}

#[test]
fn analyze_query_resolves_struct_fields_and_types_issue_408() {
    let cases = [
        (
            "SELECT composite_value.field_value AS output_value FROM source_table",
            "composite_value",
        ),
        (
            "SELECT source_table.composite_value.field_value AS output_value FROM source_table",
            "composite_value",
        ),
        (
            "SELECT item.field_value AS output_value FROM source_table s \
             CROSS JOIN UNNEST(s.nested_items) AS expanded(item)",
            "nested_items",
        ),
    ];

    for (sql, expected_column) in cases {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::DuckDB,
                schema: Some(struct_field_analysis_schema()),
            },
        )
        .unwrap_or_else(|error| panic!("analyze_query failed for {sql:?}: {error}"));
        let projection = &analysis.projections[0];

        assert_eq!(projection.type_hint.as_deref(), Some("TEXT"));
        assert!(
            projection.upstream.iter().any(|reference| {
                reference.table.as_deref() == Some("source_table")
                    && reference.column == expected_column
            }),
            "expected source_table.{expected_column} upstream for {sql:?}, got {:?}",
            projection.upstream
        );
    }
}

#[test]
fn analyze_query_propagates_unnest_element_types_through_query_scopes() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{
            "name": "events",
            "columns": [{"name": "tags", "type": "VARCHAR[]"}]
        }]
    }))
    .unwrap();

    for sql in [
        "SELECT UNNEST(e.tags) AS tag FROM events AS e",
        "SELECT u.tag FROM events AS e, UNNEST(e.tags) AS u(tag)",
        "WITH exploded AS (SELECT u.tag AS tag FROM events AS e, \
         UNNEST(e.tags) AS u(tag)) SELECT tag FROM exploded",
        "SELECT d.tag FROM (SELECT u.tag AS tag FROM events AS e, \
         UNNEST(e.tags) AS u(tag)) AS d",
    ] {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::DuckDB,
                schema: Some(schema.clone()),
            },
        )
        .unwrap_or_else(|error| panic!("analyze_query failed for {sql:?}: {error}"));

        assert_eq!(
            analysis.projections[0].type_hint.as_deref(),
            Some("TEXT"),
            "unexpected output type for {sql:?}"
        );
    }
}

#[test]
fn analyze_query_infers_unnest_type_from_case_array_constructor() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{
            "name": "events",
            "columns": [
                {"name": "created_at", "type": "TIMESTAMP"},
                {"name": "closed_at", "type": "TIMESTAMP"}
            ]
        }]
    }))
    .unwrap();
    let analysis = analyze_query(
        "SELECT UNNEST(CASE WHEN closed_at IS NULL THEN ARRAY[created_at] \
         ELSE ARRAY[created_at, closed_at] END) AS event_at FROM events",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(schema),
        },
    )
    .unwrap();

    assert_eq!(analysis.projections[0].name.as_deref(), Some("event_at"));
    assert_eq!(
        analysis.projections[0].type_hint.as_deref(),
        Some("TIMESTAMP")
    );
    assert!(analysis.projections[0].upstream.iter().any(|reference| {
        reference.table.as_deref() == Some("events") && reference.column == "created_at"
    }));
    assert!(analysis.projections[0].upstream.iter().any(|reference| {
        reference.table.as_deref() == Some("events") && reference.column == "closed_at"
    }));
}

#[test]
fn analyze_query_preserves_type_through_parenthesized_case() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{
            "name": "flags",
            "columns": [{"name": "flag", "type": "BOOLEAN"}]
        }]
    }))
    .unwrap();

    for sql in [
        "SELECT CASE WHEN flag THEN 'yes' ELSE 'no' END AS label FROM flags",
        "SELECT (CASE WHEN flag THEN 'yes' ELSE 'no' END) AS label FROM flags",
    ] {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::DuckDB,
                schema: Some(schema.clone()),
            },
        )
        .unwrap_or_else(|error| panic!("analyze_query failed for {sql:?}: {error}"));

        assert_eq!(analysis.projections[0].name.as_deref(), Some("label"));
        assert_eq!(
            analysis.projections[0].type_hint.as_deref(),
            Some("TEXT"),
            "unexpected output type for {sql:?}"
        );
        assert!(analysis.projections[0].upstream.iter().any(|reference| {
            reference.table.as_deref() == Some("flags") && reference.column == "flag"
        }));
    }
}

#[test]
fn analyze_query_keeps_reused_unnest_alias_types_isolated_between_ctes() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [
            {
                "name": "integer_events",
                "columns": [{"name": "items", "type": "INT[]"}]
            },
            {
                "name": "text_events",
                "columns": [{"name": "items", "type": "VARCHAR[]"}]
            }
        ]
    }))
    .unwrap();

    for sql in [
        "WITH ints AS (SELECT u.item FROM integer_events AS e, \
         UNNEST(e.items) AS u(item)), \
         texts AS (SELECT u.item FROM text_events AS e, \
         UNNEST(e.items) AS u(item)) \
         SELECT ints.item AS int_item, texts.item AS text_item FROM ints CROSS JOIN texts",
        "WITH texts AS (SELECT u.item FROM text_events AS e, \
         UNNEST(e.items) AS u(item)), \
         ints AS (SELECT u.item FROM integer_events AS e, \
         UNNEST(e.items) AS u(item)) \
         SELECT ints.item AS int_item, texts.item AS text_item FROM ints CROSS JOIN texts",
    ] {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                complexity_guard: None,
                dialect: DialectType::DuckDB,
                schema: Some(schema.clone()),
            },
        )
        .unwrap_or_else(|error| panic!("analyze_query failed for {sql:?}: {error}"));

        assert_eq!(analysis.projections[0].type_hint.as_deref(), Some("INT"));
        assert_eq!(analysis.projections[1].type_hint.as_deref(), Some("TEXT"));
    }
}

#[test]
fn analyze_query_tolerates_partial_schema_for_unknown_columns() {
    let analysis = analyze_query(
        "SELECT order_id, amount FROM t",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(single_column_schema(&["t"], "amount")),
        },
    )
    .unwrap();

    assert_eq!(analysis.projections.len(), 2);
    assert_eq!(analysis.projections[0].name.as_deref(), Some("order_id"));
    assert_eq!(analysis.projections[1].name.as_deref(), Some("amount"));

    let order_id = &analysis.projections[0].upstream;
    assert!(
        order_id.iter().any(|reference| {
            reference.column == "order_id"
                && reference.confidence == ReferenceConfidence::Unknown
                && reference.source_name.as_deref() == Some("t")
                && reference.table.as_deref() == Some("t")
        }),
        "expected best-effort order_id reference, got {order_id:?}"
    );

    let amount = &analysis.projections[1].upstream;
    assert!(
        amount.iter().any(|reference| {
            reference.column == "amount"
                && reference.confidence == ReferenceConfidence::Resolved
                && reference.table.as_deref() == Some("t")
        }),
        "expected schema-backed amount reference, got {amount:?}"
    );
}

#[test]
fn analyze_query_tolerates_partial_schema_for_qualified_unknown_columns() {
    let analysis = analyze_query(
        "SELECT t.order_id, t.amount FROM t",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(single_column_schema(&["t"], "amount")),
        },
    )
    .unwrap();

    assert!(
        analysis.projections[0].upstream.iter().any(|reference| {
            reference.column == "order_id"
                && reference.confidence == ReferenceConfidence::Unknown
                && reference.table.as_deref() == Some("t")
        }),
        "expected qualified unknown column to stay as best-effort t.order_id, got {:?}",
        analysis.projections[0].upstream
    );
    assert!(
        analysis.projections[1].upstream.iter().any(|reference| {
            reference.column == "amount" && reference.table.as_deref() == Some("t")
        }),
        "expected known t.amount to resolve, got {:?}",
        analysis.projections[1].upstream
    );
}

#[test]
fn analyze_query_tolerates_partial_schema_for_join_conditions() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [
            {
                "name": "t",
                "columns": [{"name": "order_id", "type": "INT"}]
            },
            {
                "name": "u",
                "columns": [{"name": "amount", "type": "INT"}]
            }
        ]
    }))
    .unwrap();

    let analysis = analyze_query(
        "SELECT a.order_id, b.amount FROM t a JOIN u b ON a.id = b.id",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: Some(schema),
        },
    )
    .unwrap();

    assert_eq!(analysis.projections.len(), 2);
    assert!(
        analysis.projections[0].upstream.iter().any(|reference| {
            reference.column == "order_id"
                && reference.source_alias.as_deref() == Some("a")
                && reference.table.as_deref() == Some("t")
        }),
        "expected a.order_id to resolve, got {:?}",
        analysis.projections[0].upstream
    );
    assert!(
        analysis.projections[1].upstream.iter().any(|reference| {
            reference.column == "amount"
                && reference.source_alias.as_deref() == Some("b")
                && reference.table.as_deref() == Some("u")
        }),
        "expected b.amount to resolve, got {:?}",
        analysis.projections[1].upstream
    );
}

#[test]
fn analyze_query_resolves_same_select_alias_reference() {
    let analysis = analyze_query(
        "WITH c AS (SELECT x FROM t) SELECT c.x AS a, a + 1 AS b FROM c",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: None,
        },
    )
    .unwrap();

    let projection = analysis
        .projections
        .iter()
        .find(|projection| projection.name.as_deref() == Some("b"))
        .unwrap();
    assert!(projection
        .upstream
        .iter()
        .any(|reference| reference.table.as_deref() == Some("t") && reference.column == "x"));
}

#[test]
fn analyze_query_resolves_pivot_alias_columns_and_generated_outputs() {
    let analysis = analyze_query(
        "SELECT region2, p1 FROM (SELECT region, q, amt FROM sales) \
         PIVOT(SUM(amt) FOR q IN ('Q1')) AS p(region2, p1)",
        AnalyzeQueryOptions {
            complexity_guard: None,
            dialect: DialectType::DuckDB,
            schema: None,
        },
    )
    .unwrap();

    let region = analysis
        .projections
        .iter()
        .find(|projection| projection.name.as_deref() == Some("region2"))
        .unwrap();
    assert!(region.upstream.iter().any(|reference| {
        reference.table.as_deref() == Some("sales") && reference.column == "region"
    }));

    let pivot_value = analysis
        .projections
        .iter()
        .find(|projection| projection.name.as_deref() == Some("p1"))
        .unwrap();
    assert!(pivot_value.upstream.iter().any(|reference| {
        reference.table.as_deref() == Some("sales") && reference.column == "amt"
    }));
}

#[test]
fn analyze_query_pivot_cte_does_not_reenter_its_source_470() {
    let schema: ValidationSchema = serde_json::from_value(json!({
        "tables": [{"name": "staged_orders", "columns": [
            {"name": "customer_id", "type": "INT"},
            {"name": "category", "type": "VARCHAR"},
            {"name": "amount", "type": "INT"}
        ]}]
    }))
    .unwrap();
    for pivot_values in ["'books', 'games'", "ANY ORDER BY category"] {
        for (source, output) in [
            ("pivot_input", "customer_id"),
            ("pivot_input AS i", "i.customer_id"),
            ("pivot_input", "p.customer_id"),
        ] {
            let alias = if output.starts_with("p.") {
                " AS p"
            } else {
                ""
            };
            for projection in ["*", output] {
                let sql = format!("WITH pivot_input AS (SELECT customer_id, category, amount FROM staged_orders) SELECT {projection} FROM {source} PIVOT(MAX(amount) FOR category IN ({pivot_values})){alias}");
                for with_schema in [false, true] {
                    let analysis = analyze_query(
                        &sql,
                        AnalyzeQueryOptions {
                            dialect: DialectType::Snowflake,
                            schema: with_schema.then(|| schema.clone()),
                            ..Default::default()
                        },
                    )
                    .unwrap_or_else(|error| panic!("{sql}: {error}"));
                    assert!(!analysis.projections.is_empty(), "{sql}");
                    if projection != "*" {
                        assert!(
                            analysis.projections[0]
                                .upstream
                                .iter()
                                .any(|reference| reference.table.as_deref().is_some_and(|table| {
                                    table.eq_ignore_ascii_case("staged_orders")
                                }) && reference
                                    .column
                                    .eq_ignore_ascii_case("customer_id")),
                            "{sql}: {:?}",
                            analysis.projections[0].upstream
                        );
                    } else {
                        assert!(
                            !analysis.projections.iter().any(|projection| projection
                                .name
                                .as_deref()
                                .is_some_and(|name| name.eq_ignore_ascii_case("category")
                                    || name.eq_ignore_ascii_case("amount"))),
                            "pivot input columns are not pivot output columns: {sql}"
                        );
                    }
                }
            }
        }
    }
}
