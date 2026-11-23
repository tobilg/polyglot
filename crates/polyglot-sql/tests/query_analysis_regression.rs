use polyglot_sql::lineage::{get_source_tables, lineage};
use polyglot_sql::traversal::get_all_tables;
use polyglot_sql::{
    analyze_query, generate, parse, AnalyzeQueryOptions, DialectType, Expression, TransformKind,
};

fn first_projection(sql: &str) -> polyglot_sql::ProjectionFact {
    let analysis = analyze_query(
        sql,
        AnalyzeQueryOptions {
            dialect: DialectType::DuckDB,
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| panic!("analyze_query failed for {sql:?}: {error}"));

    analysis
        .projections
        .into_iter()
        .next()
        .expect("expected one projection")
}

fn parse_one_statement(sql: &str, dialect: DialectType) -> Expression {
    let mut expressions = parse(sql, dialect).expect("statement should parse");
    assert_eq!(expressions.len(), 1);
    expressions.remove(0)
}

#[test]
fn clickhouse_negated_like_retains_filter_column_references() {
    use polyglot_sql::{ColumnUseContext, ReferenceConfidence};

    for operator in ["LIKE", "ILIKE"] {
        for (identifier, column) in [("a", "a"), ("`a b`", "a b")] {
            let sql = format!("SELECT id FROM t WHERE NOT {identifier} {operator} '%x%'");
            let analysis = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    dialect: DialectType::ClickHouse,
                    ..Default::default()
                },
            )
            .unwrap();
            let filters: Vec<_> = analysis
                .column_uses
                .iter()
                .filter(|fact| fact.context == ColumnUseContext::Filter)
                .collect();
            assert_eq!(filters.len(), 1, "{sql}");
            assert_eq!(filters[0].references.len(), 1, "{sql}");
            let reference = &filters[0].references[0].reference;
            assert_eq!(reference.column, column, "{sql}");
            assert_eq!(reference.table.as_deref(), Some("t"), "{sql}");
            assert_eq!(reference.confidence, ReferenceConfidence::Resolved, "{sql}");
        }
    }
}

fn assert_postgres_command(sql: &str, expected: &str) {
    let expr = parse_one_statement(sql, DialectType::PostgreSQL);
    let Expression::Command(command) = &expr else {
        panic!("expected command expression, got {}", expr.variant_name());
    };

    assert_eq!(command.this, expected);
    assert_eq!(
        generate(&expr, DialectType::PostgreSQL).expect("command should generate"),
        expected
    );
}

#[test]
fn analyze_query_reports_top_level_transform_function() {
    let projection = first_projection("SELECT DATE_TRUNC('month', created_at) AS m FROM orders");

    let transform = projection
        .transform_function
        .expect("DATE_TRUNC should be reported");
    assert_eq!(projection.transform_kind, TransformKind::Expression);
    assert_eq!(transform.name, "DATE_TRUNC");
    assert_eq!(transform.literal_args, vec!["month"]);
    assert_eq!(transform.column_args.len(), 1);
    assert_eq!(transform.column_args[0].table.as_deref(), Some("orders"));
    assert_eq!(transform.column_args[0].column, "created_at");
}

#[test]
fn analyze_query_reports_transform_function_wrapped_in_coalesce() {
    let projection = first_projection(
        "SELECT COALESCE(DATE_TRUNC('month', created_at), DATE '1970-01-01') AS m FROM orders",
    );

    let transform = projection
        .transform_function
        .expect("nested DATE_TRUNC should be reported");
    assert_eq!(projection.transform_kind, TransformKind::Expression);
    assert_eq!(transform.name, "DATE_TRUNC");
    assert_eq!(transform.literal_args, vec!["month"]);
    assert_eq!(transform.column_args.len(), 1);
    assert_eq!(transform.column_args[0].table.as_deref(), Some("orders"));
    assert_eq!(transform.column_args[0].column, "created_at");
}

#[test]
fn analyze_query_reports_transform_function_wrapped_in_cast() {
    let projection =
        first_projection("SELECT CAST(DATE_TRUNC('day', created_at) AS DATE) AS d FROM orders");

    let transform = projection
        .transform_function
        .expect("nested DATE_TRUNC should be reported");
    assert_eq!(projection.transform_kind, TransformKind::Cast);
    assert_eq!(projection.cast_type.as_deref(), Some("DATE"));
    assert_eq!(transform.name, "DATE_TRUNC");
    assert_eq!(transform.literal_args, vec!["day"]);
    assert_eq!(transform.column_args.len(), 1);
    assert_eq!(transform.column_args[0].table.as_deref(), Some("orders"));
    assert_eq!(transform.column_args[0].column, "created_at");
}

#[test]
fn analyze_query_omits_ambiguous_nested_transform_functions() {
    let projection = first_projection(
        "SELECT COALESCE(DATE_TRUNC('month', created_at), DATE_TRUNC('day', updated_at)) AS m FROM orders",
    );

    assert!(
        projection.transform_function.is_none(),
        "multiple transform function candidates should remain ambiguous"
    );
}

#[test]
fn postgres_prepare_is_structured_and_traversable() {
    let expr = parse_one_statement(
        "PREPARE leak AS SELECT id FROM sensitive_table WHERE id = $1",
        DialectType::PostgreSQL,
    );

    let Expression::Prepare(prepare) = &expr else {
        panic!("expected prepare expression, got {}", expr.variant_name());
    };
    assert_eq!(prepare.name.name, "leak");
    assert!(prepare.parameter_types.is_empty());
    assert!(matches!(prepare.statement, Expression::Select(_)));

    let tables = get_all_tables(&expr);
    assert!(tables.iter().any(|table| match table {
        Expression::Table(table) => table.name.name == "sensitive_table",
        _ => false,
    }));

    let node = lineage("id", &expr, Some(DialectType::PostgreSQL), false)
        .expect("lineage should analyze prepared statement body");
    let source_tables = get_source_tables(&node);
    assert!(source_tables.contains("sensitive_table"));
}

#[test]
fn postgres_prepare_with_parameter_types_roundtrips() {
    let expr = parse_one_statement(
        r#"PREPARE leak (int) AS SELECT * FROM "Employee" WHERE "EmployeeId" = $1"#,
        DialectType::PostgreSQL,
    );

    let Expression::Prepare(prepare) = &expr else {
        panic!("expected prepare expression, got {}", expr.variant_name());
    };
    assert_eq!(prepare.name.name, "leak");
    assert_eq!(prepare.parameter_types.len(), 1);

    let sql = generate(&expr, DialectType::PostgreSQL).expect("prepare should generate");
    assert!(sql.starts_with("PREPARE leak (INT) AS SELECT"));
    assert!(sql.contains(r#""Employee""#));
}

#[test]
fn postgres_execute_prepared_statement_with_arguments_roundtrips() {
    let expr = parse_one_statement("EXECUTE leak(1)", DialectType::PostgreSQL);

    let Expression::Execute(execute) = &expr else {
        panic!("expected execute expression, got {}", expr.variant_name());
    };
    assert!(execute.prepared);
    assert_eq!(execute.arguments.len(), 1);
    assert!(execute.parameters.is_empty());

    let sql = generate(&expr, DialectType::PostgreSQL).expect("execute should generate");
    assert_eq!(sql, "EXECUTE leak(1)");
}

#[test]
fn generic_prepare_and_execute_parse_without_command_fallback() {
    let prepare = parse_one_statement(
        "PREPARE leak AS SELECT id FROM sensitive_table WHERE id = $1",
        DialectType::Generic,
    );
    assert!(matches!(prepare, Expression::Prepare(_)));

    let execute = parse_one_statement("EXECUTE leak(1)", DialectType::Generic);
    assert!(matches!(execute, Expression::Execute(_)));
}

#[test]
fn postgres_create_replication_slot_parses_as_command() {
    assert_postgres_command(
        r#"CREATE_REPLICATION_SLOT "sdp" LOGICAL pgoutput (SNAPSHOT 'nothing')"#,
        r#"CREATE_REPLICATION_SLOT "sdp" LOGICAL pgoutput(SNAPSHOT 'nothing')"#,
    );
}

#[test]
fn postgres_replication_protocol_commands_parse_as_commands() {
    for (sql, expected) in [
        (
            "BASE_BACKUP (LABEL 'polyglot')",
            "BASE_BACKUP(LABEL 'polyglot')",
        ),
        ("DROP_REPLICATION_SLOT sdp", "DROP_REPLICATION_SLOT sdp"),
        ("IDENTIFY_SYSTEM", "IDENTIFY_SYSTEM"),
        ("READ_REPLICATION_SLOT sdp", "READ_REPLICATION_SLOT sdp"),
        (
            "START_REPLICATION SLOT sdp LOGICAL 0/0",
            "START_REPLICATION SLOT sdp LOGICAL 0/0",
        ),
        ("TIMELINE_HISTORY 1", "TIMELINE_HISTORY 1"),
    ] {
        assert_postgres_command(sql, expected);
    }
}

#[test]
fn base_tables_include_every_parenthesized_join_member() {
    for (sql, expected) in [
        ("SELECT a.id FROM alpha a JOIN (beta b JOIN gamma c ON b.id = c.id) ON a.id = b.id", vec!["alpha", "beta", "gamma"]),
        ("SELECT a.id FROM alpha a JOIN ((beta b JOIN gamma c ON b.id = c.id) LEFT JOIN delta d ON c.id = d.id) ON a.id = b.id", vec!["alpha", "beta", "delta", "gamma"]),
        ("WITH chosen AS (SELECT id FROM gamma) SELECT a.id FROM alpha a JOIN (beta b JOIN chosen c ON b.id = c.id) ON a.id = b.id", vec!["alpha", "beta", "gamma"]),
        ("SELECT a.id FROM alpha a JOIN (beta b JOIN gamma c ON b.id = c.id) AS grouped ON a.id = grouped.id", vec!["alpha", "beta", "gamma"]),
        ("SELECT a.id FROM alpha a JOIN (beta b JOIN gamma c ON b.id = c.id) AS grouped(x, y) ON a.id = grouped.x", vec!["alpha", "beta", "gamma"]),
        ("SELECT a.id FROM alpha a JOIN (beta b JOIN gamma c ON c.id IN (SELECT id FROM delta)) ON a.id = b.id", vec!["alpha", "beta", "delta", "gamma"]),
        ("SELECT a.id FROM alpha a JOIN (beta b JOIN gamma c ON c.id IN (SELECT id FROM delta)) AS grouped(x, y) ON a.id = grouped.x", vec!["alpha", "beta", "delta", "gamma"]),
    ] {
        let analysis = analyze_query(
            sql,
            AnalyzeQueryOptions {
                dialect: DialectType::DuckDB,
                ..Default::default()
            },
        )
        .expect("parenthesized join should analyze");
        let names: Vec<_> = analysis
            .base_tables
            .iter()
            .map(|table| table.name.as_str())
            .collect();
        assert_eq!(names, expected, "missing join member for {sql}");
    }
}

#[test]
fn aliased_join_groups_own_their_predicate_facts() {
    use polyglot_sql::{ColumnUseContext, ReferenceConfidence};
    for group in [
        "(beta b JOIN gamma c ON b.id = c.id)",
        "((beta b JOIN gamma c ON b.id = c.id))",
        "(((beta b JOIN gamma c ON b.id = c.id)))",
    ] {
        for alias in ["", " AS g", " AS g(x, y)"] {
            let sql = format!("SELECT 1 FROM {group}{alias}");
            let analysis = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    dialect: DialectType::DuckDB,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(
                analysis
                    .base_tables
                    .iter()
                    .map(|table| table.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["beta", "gamma"],
                "{sql}"
            );
            let joins: Vec<_> = analysis
                .column_uses
                .iter()
                .filter(|fact| fact.context == ColumnUseContext::Join)
                .collect();
            assert_eq!(joins.len(), 1, "{sql}: {joins:?}");
            assert_eq!(
                joins[0].scope_path,
                if alias.is_empty() {
                    "root"
                } else {
                    "root.derived[0]"
                },
                "{sql}"
            );
            assert_eq!(joins[0].references.len(), 2, "{sql}");
            for (reference, table) in joins[0].references.iter().zip(["beta", "gamma"]) {
                assert_eq!(reference.reference.table.as_deref(), Some(table), "{sql}");
                assert_eq!(
                    reference.reference.confidence,
                    ReferenceConfidence::Resolved,
                    "{sql}"
                );
            }
        }
    }
}

#[test]
fn wrapped_join_aliases_keep_nested_dependencies_and_alias_isolation() {
    use polyglot_sql::scope::build_scope;
    for alias in ["g", "g(x, y)"] {
        let sql = format!("SELECT 1 FROM (((beta b JOIN gamma c ON c.id IN (SELECT id FROM delta)) JOIN epsilon e ON b.id = e.id)) AS {alias}");
        let expression = parse_one_statement(&sql, DialectType::DuckDB);
        let scope = build_scope(&expression);
        assert_eq!(scope.sources.len(), 1);
        assert!(scope.sources.contains_key("g"));
        assert_eq!(scope.derived_table_scopes.len(), 1);
        assert_eq!(
            scope.derived_table_scopes[0].outer_columns,
            if alias.contains('(') {
                vec!["x", "y"]
            } else {
                vec![]
            }
        );
        let analysis = analyze_query(
            &sql,
            AnalyzeQueryOptions {
                dialect: DialectType::DuckDB,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            analysis
                .base_tables
                .iter()
                .map(|table| table.name.as_str())
                .collect::<Vec<_>>(),
            vec!["beta", "delta", "epsilon", "gamma"],
            "{sql}"
        );
    }
}

#[test]
fn re_aliased_join_groups_preserve_dependencies_and_scope_boundaries() {
    use polyglot_sql::scope::build_scope;
    use polyglot_sql::{ColumnUseContext, ReferenceConfidence};

    for group in [
        "((beta b JOIN gamma c ON b.id = c.id) AS inner_g(x, y))",
        "((((beta b JOIN gamma c ON b.id = c.id) AS inner_g(x, y))))",
    ] {
        for alias in ["outer_g", "outer_g(u, v)"] {
            let sql = format!("SELECT 1 FROM {group} AS {alias}");
            let expression = parse_one_statement(&sql, DialectType::DuckDB);
            let scope = build_scope(&expression);
            assert_eq!(scope.sources.len(), 1, "{sql}");
            assert!(scope.sources.contains_key("outer_g"), "{sql}");
            assert_eq!(scope.derived_table_scopes.len(), 1, "{sql}");
            let child = &scope.derived_table_scopes[0];
            assert_eq!(child.sources.len(), 2, "{sql}");
            assert!(child.sources.contains_key("b"), "{sql}");
            assert!(child.sources.contains_key("c"), "{sql}");
            assert_eq!(
                child.outer_columns,
                if alias.contains('(') {
                    vec!["u", "v"]
                } else {
                    vec![]
                },
                "{sql}"
            );
            let analysis = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    dialect: DialectType::DuckDB,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(
                analysis
                    .base_tables
                    .iter()
                    .map(|table| table.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["beta", "gamma"],
                "{sql}"
            );
            let joins: Vec<_> = analysis
                .column_uses
                .iter()
                .filter(|fact| fact.context == ColumnUseContext::Join)
                .collect();
            assert_eq!(joins.len(), 1, "{sql}");
            assert_eq!(joins[0].scope_path, "root.derived[0]", "{sql}");
            assert_eq!(joins[0].references.len(), 2, "{sql}");
            for (reference, table) in joins[0].references.iter().zip(["beta", "gamma"]) {
                assert_eq!(reference.reference.table.as_deref(), Some(table), "{sql}");
                assert_eq!(
                    reference.reference.confidence,
                    ReferenceConfidence::Resolved,
                    "{sql}"
                );
            }
        }
    }
}

#[test]
fn snowflake_function_stars_read_source_columns() {
    let analysis = analyze_query(
        "SELECT OBJECT_CONSTRUCT(*) AS obj, HASH(*) AS h, OBJECT_CONSTRUCT(t.*) AS q, COUNT(*) AS n FROM s.t",
        AnalyzeQueryOptions {
            dialect: DialectType::Snowflake,
            schema: Some(serde_json::from_value(serde_json::json!({"tables": [
                {"name":"t", "schema":"s", "columns":[{"name":"k","type":"INT"},{"name":"v","type":"INT"}]}
            ]})).unwrap()),
            ..Default::default()
        },
    ).unwrap();
    for projection in &analysis.projections[..3] {
        let mut names = projection
            .upstream
            .iter()
            .map(|r| {
                assert_eq!(r.source_name.as_deref(), Some("s.t"));
                assert_eq!(r.path_transform_kind, Some(TransformKind::Expression));
                r.column.as_str()
            })
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, ["K", "V"], "{:?}", projection.name);
    }
    assert!(analysis.projections[3].upstream.is_empty());
}

#[test]
fn snowflake_analysis_preserves_distinct_column_identifiers() {
    let schema = serde_json::from_value(serde_json::json!({"tables": [
        {"name":"T","schema":"S","columns":[{"name":"ID","type":"INT"},{"name":"\"id\"","type":"INT"}]}
    ]})).unwrap();
    for schema in [Some(schema), None] {
        let nested = analyze_query(
            r#"WITH c AS (SELECT id, "id" FROM s.t) SELECT c.id, c."id" FROM c"#,
            AnalyzeQueryOptions {
                dialect: DialectType::Snowflake,
                schema: schema.clone(),
                ..Default::default()
            },
        )
        .unwrap();
        for (p, expected) in nested.projections.iter().zip(["ID", "id"]) {
            assert_eq!(p.upstream.len(), 1);
            assert_eq!(p.upstream[0].source_name.as_deref(), Some("s.t"));
            assert_eq!(p.upstream[0].column, expected);
        }
        let analysis = analyze_query(
            r#"SELECT id, "id", id AS total, "id" AS "Total", id + "id" AS both_ids FROM s.t WHERE id > "id""#,
            AnalyzeQueryOptions {dialect: DialectType::Snowflake, schema, ..Default::default()},
        ).unwrap();
        for (p, (output, input)) in analysis.projections.iter().zip([
            ("ID", "ID"),
            ("id", "id"),
            ("TOTAL", "ID"),
            ("Total", "id"),
        ]) {
            assert_eq!(p.name.as_deref(), Some(output));
            assert_eq!(p.upstream.len(), 1);
            assert_eq!(p.upstream[0].column, input, "{output}");
        }
        let mut columns = analysis.projections[4]
            .upstream
            .iter()
            .map(|r| r.column.as_str())
            .collect::<Vec<_>>();
        columns.sort_unstable();
        assert_eq!(columns, ["ID", "id"]);
        let mut columns = analysis.column_uses[0]
            .references
            .iter()
            .map(|r| r.reference.column.as_str())
            .collect::<Vec<_>>();
        columns.sort_unstable();
        assert_eq!(columns, ["ID", "id"]);
    }
}

#[test]
fn pivot_reports_implicit_group_keys() {
    use polyglot_sql::ColumnUseContext;
    let schema: polyglot_sql::ValidationSchema = serde_json::from_value(serde_json::json!({"tables": [
        {"name":"t","schema":"s","columns":[{"name":"k","type":"INT"},{"name":"status","type":"VARCHAR"},{"name":"amount","type":"INT"},{"name":"c","type":"INT"}]}
    ]})).unwrap();
    for (sql, expected) in [
        ("SELECT * FROM (SELECT k, status, amount FROM s.t) PIVOT(SUM(amount) FOR status IN ('placed', 'shipped'))", vec!["K"]),
        ("WITH q AS (SELECT k AS key_alias, status, amount FROM s.t) SELECT 1 FROM q PIVOT(SUM(amount) FOR status IN (ANY))", vec!["K"]),
        ("SELECT 1 FROM s.t PIVOT(SUM(amount) FOR status IN ('placed'))", vec!["C","K"]),
        ("SELECT * FROM (SELECT status, amount FROM s.t) PIVOT(SUM(amount) FOR status IN ('placed'))", vec![]),
    ] {
        let result = analyze_query(sql, AnalyzeQueryOptions { dialect: DialectType::Snowflake, schema: Some(schema.clone()), ..Default::default() }).unwrap();
        let mut columns = Vec::new();
        for fact in result.column_uses.iter().filter(|f| f.context == ColumnUseContext::Group) {
            assert!(fact.span.is_none());
            assert_eq!(fact.references.len(), 1, "{sql}: {fact:?}");
            let r = &fact.references[0].reference;
            assert_eq!(r.source_name.as_deref(), Some("s.t"), "{sql}");
            columns.push(r.column.as_str());
        }
        columns.sort_unstable();
        assert_eq!(columns, expected, "{sql}");
    }
}

#[test]
fn unpivot_labels_and_null_filter_have_separate_dependencies() {
    use polyglot_sql::ColumnUseContext;
    for dialect in [DialectType::Snowflake, DialectType::DuckDB] {
        for mode in ["", "EXCLUDE NULLS", "INCLUDE NULLS"] {
            let sql = format!("SELECT k, metric, val FROM (SELECT k, amount, c FROM s.t) UNPIVOT {mode} (val FOR metric IN (amount AS revenue, c))");
            let result = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    dialect,
                    ..Default::default()
                },
            )
            .unwrap();
            assert!(result.projections[1].upstream.is_empty(), "{sql}");
            let mut values: Vec<_> = result.projections[2]
                .upstream
                .iter()
                .map(|r| r.column.to_ascii_lowercase())
                .collect();
            values.sort();
            assert_eq!(values, ["amount", "c"]);
            let filters: Vec<_> = result
                .column_uses
                .iter()
                .filter(|u| u.context == ColumnUseContext::Filter)
                .collect();
            if mode == "INCLUDE NULLS" {
                assert!(filters.is_empty());
            } else {
                let mut columns: Vec<_> = filters
                    .iter()
                    .flat_map(|f| &f.references)
                    .map(|r| {
                        assert_eq!(r.reference.source_name.as_deref(), Some("s.t"));
                        r.reference.column.to_ascii_lowercase()
                    })
                    .collect();
                columns.sort();
                assert_eq!(columns, values, "{sql}");
            }
        }
    }
}

#[test]
fn snowflake_star_ilike_filters_before_other_modifiers() {
    let dialect = DialectType::Snowflake;
    let schema: polyglot_sql::ValidationSchema = serde_json::from_value(serde_json::json!({"tables": [
        {"name":"t","columns":[{"name":"a1","type":"INT"},{"name":"a2","type":"INT"},{"name":"abc","type":"INT"},{"name":"b","type":"INT"}]}
    ]})).unwrap();
    for (sql, expected) in [
        ("SELECT * ILIKE '%A%' FROM t", vec!["A1", "A2", "ABC"]),
        (
            "SELECT t.* ILIKE 'A_' RENAME (a1 AS x) FROM t",
            vec!["X", "A2"],
        ),
        (
            "SELECT * ILIKE 'a_' REPLACE (b AS a1) FROM t",
            vec!["A1", "A2"],
        ),
    ] {
        let expr = parse_one_statement(sql, dialect);
        let emitted = generate(&expr, dialect).unwrap();
        assert!(emitted.contains(" ILIKE "), "{emitted}");
        for sql in [sql, emitted.as_str()] {
            let analysis = analyze_query(
                sql,
                AnalyzeQueryOptions {
                    dialect,
                    schema: Some(schema.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(
                analysis.star_projections[0].expanded_columns, expected,
                "{sql}"
            );
            assert_eq!(
                analysis
                    .projections
                    .iter()
                    .map(|p| p.name.as_deref().unwrap())
                    .collect::<Vec<_>>(),
                expected,
                "{sql}"
            );
            if sql.contains("REPLACE") {
                assert_eq!(analysis.projections[0].upstream[0].column, "B");
            }
        }
    }
    for sql in [
        "SELECT * ILIKE '%a%' EXCLUDE b FROM t",
        "SELECT t.* EXCLUDE b ILIKE '%a%' FROM t",
        "SELECT * ILIKE b FROM t",
    ] {
        assert!(parse(sql, dialect).is_err(), "{sql}");
    }
}

#[test]
fn snowflake_output_headings_for_expressions_and_pivot_values() {
    let schema = serde_json::from_value(serde_json::json!({"tables":[{"name":"t","columns":[
        {"name":"k","type":"INT"},{"name":"a","type":"INT"},{"name":"b","type":"VARCHAR"}]}]}))
    .unwrap();
    let options = AnalyzeQueryOptions {
        dialect: DialectType::Snowflake,
        schema: Some(schema),
        ..Default::default()
    };
    for (sql, names) in [
        (
            "SELECT a * 2, UPPER(b), a, a FROM t",
            vec!["A * 2", "UPPER(B)", "A", "A"],
        ),
        (
            "SELECT * FROM (SELECT k,b,a FROM t) PIVOT (SUM(a) FOR b IN ('x', 'y'))",
            vec!["K", "'x'", "'y'"],
        ),
    ] {
        let analysis = analyze_query(sql, options.clone()).unwrap();
        assert_eq!(
            analysis
                .projections
                .iter()
                .map(|p| p.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            names,
            "{sql}"
        );
    }
    let analysis = analyze_query(
        r#"WITH c AS (SELECT a * 2 FROM t) SELECT "A * 2" FROM c"#,
        options,
    )
    .unwrap();
    assert_eq!(analysis.projections[0].upstream[0].column, "A");
}

#[test]
fn snowflake_table_function_stars_expand_in_declared_order() {
    let schema = serde_json::from_value(serde_json::json!({"tables":[{"name":"t","columns":[
        {"name":"items","type":"VARIANT"},{"name":"b","type":"VARCHAR"}]}]}))
    .unwrap();
    for schema in [Some(schema), None] {
        for (source, names) in [
            (
                "t, LATERAL FLATTEN(input => t.items) f",
                vec!["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"],
            ),
            (
                "t, LATERAL SPLIT_TO_TABLE(t.b, ',') f",
                vec!["SEQ", "INDEX", "VALUE"],
            ),
            (
                "TABLE(SPLIT_TO_TABLE('a,b', ',')) f",
                vec!["SEQ", "INDEX", "VALUE"],
            ),
        ] {
            let sql = format!("SELECT f.* FROM {source}");
            let analysis = analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    dialect: DialectType::Snowflake,
                    schema: schema.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(
                analysis
                    .projections
                    .iter()
                    .map(|p| p.name.as_deref().unwrap())
                    .collect::<Vec<_>>(),
                names,
                "{sql}"
            );
        }
    }
}

#[test]
fn ctes_that_read_the_previous_cte_twice_stay_linear() {
    use polyglot_sql::scope::SourceKind;
    use polyglot_sql::{ColumnUseContext, ReferenceConfidence};

    // Each CTE reads the previous CTE in two branches. A separate trace per
    // reference doubled the lineage at each level, so depth 24 did not finish.
    const DEPTH: usize = 24;
    for (branch, b_kind, b_functions) in [
        (
            "SELECT * FROM {cte}",
            TransformKind::Direct,
            Vec::<&str>::new(),
        ),
        (
            "SELECT a, UPPER(b) AS b, c FROM {cte} WHERE a > 0",
            TransformKind::Expression,
            vec!["UPPER"],
        ),
    ] {
        let mut ctes = vec!["c0 AS (SELECT a, b, c FROM t)".to_string()];
        for level in 1..=DEPTH {
            let previous = format!("c{}", level - 1);
            let second = branch.replace("{cte}", &previous);
            ctes.push(format!(
                "c{level} AS (SELECT * FROM {previous} UNION ALL {second})"
            ));
        }
        let sql = format!("WITH {} SELECT * FROM c{DEPTH}", ctes.join(", "));
        let analysis = analyze_query(
            &sql,
            AnalyzeQueryOptions {
                dialect: DialectType::Snowflake,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.ctes.len(), DEPTH + 1, "{branch}");
        for (projection, column) in analysis.projections.iter().zip(["A", "B", "C"]) {
            assert_eq!(projection.name.as_deref(), Some(column), "{branch}");
            assert_eq!(projection.upstream.len(), 1, "{branch}");
            let reference = &projection.upstream[0];
            assert_eq!(reference.table.as_deref(), Some("t"), "{branch}");
            assert_eq!(reference.column, column, "{branch}");
            assert_eq!(reference.source_kind, SourceKind::Table, "{branch}");
            assert_eq!(
                reference.confidence,
                ReferenceConfidence::Resolved,
                "{branch}"
            );
        }
        assert_eq!(analysis.projections.len(), 3, "{branch}");

        // Paths through both branches merge into one reference.
        let b = &analysis.projections[1].upstream[0];
        assert_eq!(b.path_transform_kind, Some(b_kind), "{branch}");
        assert_eq!(b.path_functions, b_functions, "{branch}");

        let filters: Vec<_> = analysis
            .column_uses
            .iter()
            .filter(|fact| fact.context == ColumnUseContext::Filter)
            .collect();
        let expected_filters = if branch.contains("WHERE") { DEPTH } else { 0 };
        assert_eq!(filters.len(), expected_filters, "{branch}");
        for filter in filters {
            assert_eq!(filter.references.len(), 1, "{branch}");
            let reference = &filter.references[0].reference;
            assert_eq!(reference.table.as_deref(), Some("t"), "{branch}");
            assert_eq!(reference.column, "A", "{branch}");
        }
    }
}

#[test]
fn a_union_of_many_filtered_ctes_stays_small() {
    use polyglot_sql::scope::SourceKind;
    use polyglot_sql::ColumnUseContext;

    // A UNION ALL of N CTEs is N nested scopes. A copy of the visible CTE
    // scopes and of the union operands at each level grew as N^3.
    // 48 stays below MAX_LINEAGE_DEPTH.
    const BRANCHES: usize = 48;
    let mut ctes = vec!["src AS (SELECT a, b, c FROM t)".to_string()];
    for i in 0..BRANCHES {
        ctes.push(format!(
            "b{i} AS (SELECT a, UPPER(b) AS b, c FROM src WHERE a = {i})"
        ));
    }
    let branches: Vec<_> = (0..BRANCHES)
        .map(|i| format!("SELECT * FROM b{i}"))
        .collect();
    ctes.push(format!("u AS ({})", branches.join(" UNION ALL ")));
    let sql = format!("WITH {} SELECT * FROM u", ctes.join(", "));
    let analysis = analyze_query(
        &sql,
        AnalyzeQueryOptions {
            dialect: DialectType::Snowflake,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(analysis.ctes.len(), BRANCHES + 2);
    assert_eq!(analysis.projections.len(), 3);
    for (projection, column) in analysis.projections.iter().zip(["A", "B", "C"]) {
        assert_eq!(projection.name.as_deref(), Some(column));
        assert_eq!(projection.upstream.len(), 1);
        let reference = &projection.upstream[0];
        assert_eq!(reference.table.as_deref(), Some("t"));
        assert_eq!(reference.column, column);
        assert_eq!(reference.source_kind, SourceKind::Table);
    }
    let b = &analysis.projections[1].upstream[0];
    assert_eq!(b.path_transform_kind, Some(TransformKind::Expression));
    assert_eq!(b.path_functions, vec!["UPPER"]);

    let filters: Vec<_> = analysis
        .column_uses
        .iter()
        .filter(|fact| fact.context == ColumnUseContext::Filter)
        .collect();
    assert_eq!(filters.len(), BRANCHES);
    for filter in filters {
        assert_eq!(filter.references.len(), 1);
        let reference = &filter.references[0].reference;
        assert_eq!(reference.table.as_deref(), Some("t"));
        assert_eq!(reference.column, "A");
    }
}

#[test]
fn public_lineage_node_supports_struct_update() {
    use polyglot_sql::{expressions::Expression, lineage::LineageNode};
    let node = LineageNode::new("x", Expression::null(), Expression::null());
    let renamed = LineageNode {
        name: "y".into(),
        ..node
    };
    assert_eq!(renamed.walk().count(), 1);
}

#[test]
fn shared_cte_traces_preserve_function_order() {
    let analysis = analyze_query(
        "WITH x AS (SELECT UPPER(b) AS b FROM t), y AS (SELECT b FROM x UNION ALL SELECT LOWER(b) AS b FROM x) SELECT b FROM y",
        AnalyzeQueryOptions {dialect: DialectType::Snowflake, ..Default::default()},
    ).unwrap();
    let upstream = &analysis.projections[0].upstream;
    assert_eq!(upstream.len(), 1);
    assert_eq!(upstream[0].path_functions, ["UPPER", "LOWER"]);
}
