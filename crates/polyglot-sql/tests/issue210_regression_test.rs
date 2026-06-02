use polyglot_sql::lineage::{get_source_tables, lineage};
use polyglot_sql::traversal::get_all_tables;
use polyglot_sql::{generate, parse, DialectType, Expression};

fn parse_one(sql: &str, dialect: DialectType) -> Expression {
    let mut expressions = parse(sql, dialect).expect("statement should parse");
    assert_eq!(expressions.len(), 1);
    expressions.remove(0)
}

#[test]
fn postgres_prepare_is_structured_and_traversable() {
    let expr = parse_one(
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
    let expr = parse_one(
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
    let expr = parse_one("EXECUTE leak(1)", DialectType::PostgreSQL);

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
    let prepare = parse_one(
        "PREPARE leak AS SELECT id FROM sensitive_table WHERE id = $1",
        DialectType::Generic,
    );
    assert!(matches!(prepare, Expression::Prepare(_)));

    let execute = parse_one("EXECUTE leak(1)", DialectType::Generic);
    assert!(matches!(execute, Expression::Execute(_)));
}
