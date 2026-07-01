use polyglot_sql::expressions::FunctionSecurity;
use polyglot_sql::{generate, parse, DialectType, Expression};

fn assert_create_view(
    sql: &str,
    dialect: DialectType,
    expected_security: Option<FunctionSecurity>,
    expected_comment: Option<&str>,
) {
    let ast = parse(sql, dialect).unwrap_or_else(|error| {
        panic!("{dialect:?} CREATE VIEW should parse for {sql:?}: {error}")
    });
    assert_eq!(ast.len(), 1);

    let Expression::CreateView(view) = &ast[0] else {
        panic!(
            "expected CreateView for {sql:?}, got {}",
            ast[0].variant_name()
        );
    };

    assert_eq!(
        view.security, expected_security,
        "security mismatch for {sql}"
    );
    assert_eq!(
        view.comment.as_deref(),
        expected_comment,
        "comment mismatch for {sql}"
    );

    let generated = generate(&ast[0], dialect).unwrap_or_else(|error| {
        panic!("{dialect:?} CREATE VIEW should generate for {sql:?}: {error}")
    });
    parse(&generated, dialect).unwrap_or_else(|error| {
        panic!("{dialect:?} generated CREATE VIEW should reparse for {generated:?}: {error}")
    });
}

#[test]
fn trino_create_view_accepts_comment_before_security() {
    assert_create_view(
        "CREATE VIEW v COMMENT 'c' SECURITY DEFINER AS SELECT 1 AS a",
        DialectType::Trino,
        Some(FunctionSecurity::Definer),
        Some("c"),
    );
    assert_create_view(
        "CREATE VIEW v COMMENT 'c' SECURITY INVOKER AS SELECT 1 AS a",
        DialectType::Trino,
        Some(FunctionSecurity::Invoker),
        Some("c"),
    );
}

#[test]
fn trino_create_view_existing_security_and_comment_orders_still_parse() {
    assert_create_view(
        "CREATE VIEW v SECURITY DEFINER AS SELECT 1 AS a",
        DialectType::Trino,
        Some(FunctionSecurity::Definer),
        None,
    );
    assert_create_view(
        "CREATE VIEW v COMMENT 'c' AS SELECT 1 AS a",
        DialectType::Trino,
        None,
        Some("c"),
    );
    assert_create_view(
        "CREATE VIEW v SECURITY DEFINER COMMENT 'c' AS SELECT 1 AS a",
        DialectType::Trino,
        Some(FunctionSecurity::Definer),
        Some("c"),
    );
}

#[test]
fn presto_create_view_accepts_comment_before_security() {
    assert_create_view(
        "CREATE VIEW v COMMENT 'c' SECURITY DEFINER AS SELECT 1 AS a",
        DialectType::Presto,
        Some(FunctionSecurity::Definer),
        Some("c"),
    );
    assert_create_view(
        "CREATE VIEW v COMMENT 'c' SECURITY INVOKER AS SELECT 1 AS a",
        DialectType::Presto,
        Some(FunctionSecurity::Invoker),
        Some("c"),
    );
}

#[test]
fn create_view_accepts_sql_security_after_comment() {
    assert_create_view(
        "CREATE VIEW v COMMENT 'c' SQL SECURITY DEFINER AS SELECT 1 AS a",
        DialectType::MySQL,
        Some(FunctionSecurity::Definer),
        Some("c"),
    );
}
