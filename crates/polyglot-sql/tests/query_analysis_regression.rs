use polyglot_sql::{analyze_query, AnalyzeQueryOptions, DialectType, TransformKind};

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
