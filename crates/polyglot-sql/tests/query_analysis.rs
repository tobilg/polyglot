use polyglot_sql::{
    analyze_query, scope::SourceKind, AnalyzeQueryOptions, DialectType, ProjectionNullability,
    QueryShape, ReferenceConfidence, TransformKind, ValidationSchema,
};
use serde_json::json;

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
fn analyze_query_reports_function_projection_arguments() {
    let analysis = analyze_query(
        "SELECT DATE_TRUNC('month', created_at) AS bucket FROM events",
        AnalyzeQueryOptions {
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
}

#[test]
fn analyze_query_follows_cte_lineage() {
    let analysis = analyze_query(
        "WITH base AS (SELECT id FROM users) SELECT id FROM base",
        AnalyzeQueryOptions {
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
}

#[test]
fn analyze_query_rejects_non_query_statements() {
    let err = analyze_query(
        "CREATE TABLE t (a INT)",
        AnalyzeQueryOptions {
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
fn analyze_query_preserves_precise_schema_type_hints() {
    let analysis = analyze_query(
        "SELECT amount FROM orders",
        AnalyzeQueryOptions {
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
