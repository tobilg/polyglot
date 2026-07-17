use std::{cell::RefCell, collections::HashMap};

use polyglot_sql::dialects::transform_recursive;
use polyglot_sql::expressions::{
    Cast, DataType, Expression, JoinKind, LikeOp, Literal, StructField,
};
use polyglot_sql::generator::{Generator, GeneratorConfig};
use polyglot_sql::{
    parse, rename_tables, replace_by_type, transform, transform_map, DialectType, ExpressionWalk,
    Parser,
};

fn parse_one(sql: &str) -> Expression {
    Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("expected one statement")
}

fn parse_one_dialect(sql: &str, dialect: DialectType) -> Expression {
    parse(sql, dialect)
        .unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("expected one statement")
}

fn generate_with_dialect(expr: &Expression, dialect: DialectType) -> String {
    let config = GeneratorConfig {
        dialect: Some(dialect),
        ..Default::default()
    };
    let mut generator = Generator::with_config(config);
    generator
        .generate(expr)
        .unwrap_or_else(|e| panic!("failed to generate {dialect:?} SQL: {e}"))
}

fn first_index(order: &[String], target: &str) -> usize {
    order
        .iter()
        .position(|name| name == target)
        .unwrap_or_else(|| panic!("missing {target} in visit order: {order:?}"))
}

fn rename_predicate_children(node: Expression) -> Expression {
    match node {
        Expression::Table(mut table) => {
            if table
                .schema
                .as_ref()
                .is_some_and(|schema| schema.name == "src")
            {
                table.schema = Some(polyglot_sql::expressions::Identifier::quoted("dst"));
            }
            Expression::Table(table)
        }
        Expression::Column(mut column) => {
            column.name.name = match column.name.name.as_str() {
                "x" => "lhs".to_string(),
                "y" => "rhs".to_string(),
                _ => column.name.name,
            };
            Expression::Column(column)
        }
        other => other,
    }
}

fn assert_predicate_children_renamed(sql: &str) {
    let expression = parse_one_dialect(sql, DialectType::PostgreSQL);
    let transformed = transform_map(expression, &|node| Ok(rename_predicate_children(node)))
        .expect("predicate child transform should succeed");
    let sql = generate_with_dialect(&transformed, DialectType::PostgreSQL);

    assert!(sql.contains("\"dst\".a"), "{sql}");
    assert!(sql.contains("lhs"), "{sql}");
    assert!(sql.contains("rhs"), "{sql}");
    assert!(!sql.contains("src."), "{sql}");
    if sql.contains("FROM") && sql.matches("FROM").count() > 1 {
        assert!(sql.contains("\"dst\".b"), "{sql}");
    }
}

#[test]
fn transform_recursive_visits_children_before_parents() {
    let expr = parse_one("SELECT a + 1 AS x");
    let order = RefCell::new(Vec::new());

    let transformed = transform_recursive(expr, &|node| {
        order.borrow_mut().push(node.variant_name().to_string());
        Ok(node)
    })
    .expect("transform should succeed");

    assert!(matches!(transformed, Expression::Select(_)));

    let order = order.into_inner();
    assert!(first_index(&order, "column") < first_index(&order, "add"));
    assert!(first_index(&order, "literal") < first_index(&order, "add"));
    assert!(first_index(&order, "add") < first_index(&order, "alias"));
    assert!(first_index(&order, "alias") < first_index(&order, "select"));
}

#[test]
fn transform_map_visits_quantified_comparison_children() {
    for sql in [
        "SELECT * FROM src.a WHERE x = ANY (SELECT y FROM src.b)",
        "SELECT * FROM src.a WHERE x <> ALL (SELECT y FROM src.b)",
        "SELECT * FROM src.a WHERE x < ANY (SELECT y FROM src.b)",
        "SELECT * FROM src.a WHERE x <= ALL (SELECT y FROM src.b)",
        "SELECT * FROM src.a WHERE x > ANY (SELECT y FROM src.b)",
        "SELECT * FROM src.a WHERE x >= ALL (SELECT y FROM src.b)",
        "SELECT * FROM src.a WHERE x = SOME (SELECT y FROM src.b)",
        "SELECT x = ANY(ARRAY[y]) FROM src.a",
    ] {
        assert_predicate_children_renamed(sql);
    }
}

#[test]
fn transform_map_visits_null_safe_comparison_children() {
    for sql in [
        "SELECT * FROM src.a WHERE x IS DISTINCT FROM (SELECT y FROM src.b)",
        "SELECT * FROM src.a WHERE x IS NOT DISTINCT FROM (SELECT y FROM src.b)",
    ] {
        assert_predicate_children_renamed(sql);
    }
}

#[test]
fn affected_predicates_preserve_bottom_up_transform_order() {
    for (sql, parent) in [
        (
            "SELECT * FROM src.a WHERE x = ANY (SELECT y FROM src.b)",
            "any",
        ),
        (
            "SELECT * FROM src.a WHERE x = ALL (SELECT y FROM src.b)",
            "all",
        ),
        (
            "SELECT * FROM src.a WHERE x IS NOT DISTINCT FROM (SELECT y FROM src.b)",
            "null_safe_eq",
        ),
        (
            "SELECT * FROM src.a WHERE x IS DISTINCT FROM (SELECT y FROM src.b)",
            "null_safe_neq",
        ),
    ] {
        let expression = parse_one_dialect(sql, DialectType::PostgreSQL);
        let order = RefCell::new(Vec::new());
        transform_recursive(expression, &|node| {
            let label = match &node {
                Expression::Column(column) => format!("column:{}", column.name.name),
                Expression::Table(table) => format!("table:{}", table.name.name),
                _ => node.variant_name().to_string(),
            };
            order.borrow_mut().push(label);
            Ok(node)
        })
        .expect("transform should succeed");

        let order = order.into_inner();
        assert!(first_index(&order, "column:x") < first_index(&order, parent));
        assert!(first_index(&order, "column:y") < first_index(&order, parent));
        assert!(first_index(&order, "table:b") < first_index(&order, parent));
    }
}

#[test]
fn delegated_transform_entry_points_visit_quantified_subqueries() {
    let sql = "SELECT * FROM a WHERE x > ALL (SELECT y FROM b)";
    let rename_nested_table = |node: Expression| match node {
        Expression::Table(mut table) if table.name.name == "b" => {
            table.name.name = "renamed_b".to_string();
            Expression::Table(table)
        }
        other => other,
    };

    let transformed = transform(parse_one_dialect(sql, DialectType::PostgreSQL), &|node| {
        Ok(Some(rename_nested_table(node)))
    })
    .expect("optional transform should succeed");
    assert!(generate_with_dialect(&transformed, DialectType::PostgreSQL).contains("FROM renamed_b"));

    let transformed = parse_one_dialect(sql, DialectType::PostgreSQL)
        .transform_owned(|node| Ok(Some(rename_nested_table(node))))
        .expect("owned transform should succeed");
    assert!(generate_with_dialect(&transformed, DialectType::PostgreSQL).contains("FROM renamed_b"));

    let mapping = HashMap::from([("b".to_string(), "renamed_b".to_string())]);
    let transformed = rename_tables(parse_one_dialect(sql, DialectType::PostgreSQL), &mapping);
    assert!(generate_with_dialect(&transformed, DialectType::PostgreSQL).contains("FROM renamed_b"));
}

#[test]
fn transform_recursive_applies_join_wrapper_transform() {
    let expr = parse_one("SELECT * FROM a JOIN b ON a.id = b.id");

    let transformed = transform_recursive(expr, &|node| match node {
        Expression::Join(mut join) => {
            join.kind = JoinKind::Left;
            Ok(Expression::Join(join))
        }
        other => Ok(other),
    })
    .expect("transform should succeed");

    let Expression::Select(select) = transformed else {
        panic!("expected select");
    };
    assert_eq!(select.joins.len(), 1);
    assert_eq!(select.joins[0].kind, JoinKind::Left);
}

#[test]
fn transform_recursive_rejects_non_join_from_join_wrapper() {
    let expr = parse_one("SELECT * FROM a JOIN b ON a.id = b.id");

    let err = transform_recursive(expr, &|node| match node {
        Expression::Join(_) => Ok(Expression::identifier("not_a_join")),
        other => Ok(other),
    })
    .expect_err("join wrapper should reject non-join result");

    let message = err.to_string();
    assert!(
        message.contains("non-join expression"),
        "unexpected error: {message}"
    );
}

#[test]
fn transform_recursive_applies_ordered_wrapper_transform() {
    let expr = parse_one("SELECT * FROM a ORDER BY x NULLS LAST");

    let transformed = transform_recursive(expr, &|node| match node {
        Expression::Ordered(mut ordered) => {
            ordered.desc = true;
            ordered.nulls_first = Some(true);
            Ok(Expression::Ordered(ordered))
        }
        other => Ok(other),
    })
    .expect("transform should succeed");

    let Expression::Select(select) = transformed else {
        panic!("expected select");
    };
    let order_by = select.order_by.expect("expected order by");
    assert_eq!(order_by.expressions.len(), 1);
    assert!(order_by.expressions[0].desc);
    assert_eq!(order_by.expressions[0].nulls_first, Some(true));
}

#[test]
fn transform_recursive_preserves_ordered_original_when_wrapper_transform_errors() {
    let expr = parse_one("SELECT * FROM a ORDER BY x NULLS LAST");
    let original_sql = expr.sql();

    let transformed = transform_recursive(expr, &|node| match node {
        Expression::Ordered(_) => Err(polyglot_sql::Error::Parse {
            message: "ordered wrapper failure".to_string(),
            line: 0,
            column: 0,
            start: 0,
            end: 0,
        }),
        other => Ok(other),
    })
    .expect("ordered wrapper failure should fall back to original");

    assert_eq!(transformed.sql(), original_sql);
}

#[test]
fn transform_recursive_preserves_cte_body_when_child_transform_errors() {
    let expr = parse_one("WITH cte AS (SELECT 1) SELECT * FROM cte");
    let original_sql = expr.sql();

    let transformed = transform_recursive(expr, &|node| match node {
        Expression::Literal(_) => Err(polyglot_sql::Error::Parse {
            message: "literal transform failure".to_string(),
            line: 0,
            column: 0,
            start: 0,
            end: 0,
        }),
        other => Ok(other),
    })
    .expect("cte child failure should fall back to original body");

    assert_eq!(transformed.sql(), original_sql);
}

#[test]
fn transform_recursive_renames_update_target_from_and_join_tables() {
    let expr = parse_one_dialect(
        "UPDATE employees e \
         SET salary = s.new_salary \
         FROM salary_updates s \
         JOIN department_updates d ON d.id = s.department_id \
         WHERE e.id = s.employee_id \
         RETURNING e.id",
        DialectType::PostgreSQL,
    );
    let mapping = HashMap::from([
        ("employees".to_string(), "table_1".to_string()),
        ("salary_updates".to_string(), "table_2".to_string()),
        ("department_updates".to_string(), "table_3".to_string()),
    ]);

    let transformed = rename_tables(expr, &mapping);
    let sql = generate_with_dialect(&transformed, DialectType::PostgreSQL);

    assert!(sql.contains("UPDATE table_1 AS e"), "{sql}");
    assert!(sql.contains("FROM table_2 AS s"), "{sql}");
    assert!(sql.contains("JOIN table_3 AS d"), "{sql}");
    assert!(!sql.contains("employees"), "{sql}");
    assert!(!sql.contains("salary_updates"), "{sql}");
    assert!(!sql.contains("department_updates"), "{sql}");
}

#[test]
fn replace_by_type_visits_delete_using_and_returning_fields() {
    let expr = parse_one_dialect(
        "DELETE FROM employees e \
         USING salary_updates s \
         WHERE e.id = s.employee_id \
         RETURNING e.id",
        DialectType::PostgreSQL,
    );
    let mapping = HashMap::from([
        ("employees".to_string(), "table_1".to_string()),
        ("salary_updates".to_string(), "table_2".to_string()),
    ]);

    let transformed = replace_by_type(
        expr,
        |node| {
            matches!(node, Expression::Table(table) if mapping.contains_key(&table.name.name))
                || matches!(node, Expression::Column(column) if column.name.name == "id")
        },
        |node| match node {
            Expression::Table(mut table) => {
                table.name.name = mapping[&table.name.name].clone();
                Expression::Table(table)
            }
            Expression::Column(mut column) => {
                column.name.name = "employee_id".to_string();
                Expression::Column(column)
            }
            other => other,
        },
    );
    let sql = generate_with_dialect(&transformed, DialectType::PostgreSQL);

    assert!(sql.contains("DELETE FROM table_1 e"), "{sql}");
    assert!(sql.contains("USING table_2 AS s"), "{sql}");
    assert!(sql.contains("RETURNING e.employee_id"), "{sql}");
    assert!(!sql.contains("employees"), "{sql}");
    assert!(!sql.contains("salary_updates"), "{sql}");
}

#[test]
fn replace_by_type_visits_update_output_clause() {
    let expr = parse_one_dialect(
        "UPDATE employees \
         SET salary = 1 \
         OUTPUT INSERTED.id INTO audit \
         WHERE id = 1",
        DialectType::TSQL,
    );

    let transformed = replace_by_type(
        expr,
        |node| {
            matches!(node, Expression::Table(table) if table.name.name == "employees")
                || matches!(node, Expression::Column(column) if column.name.name == "audit")
        },
        |node| match node {
            Expression::Table(mut table) => {
                table.name.name = "table_1".to_string();
                Expression::Table(table)
            }
            Expression::Column(mut column) => {
                column.name.name = "audit_redacted".to_string();
                Expression::Column(column)
            }
            other => other,
        },
    );
    let sql = generate_with_dialect(&transformed, DialectType::TSQL);

    assert!(sql.contains("UPDATE table_1"), "{sql}");
    assert!(
        sql.contains("OUTPUT INSERTED.id INTO audit_redacted"),
        "{sql}"
    );
    assert!(!sql.contains("employees"), "{sql}");
    assert!(!sql.contains("INTO audit "), "{sql}");
}

#[test]
fn transform_recursive_rewrites_nested_cast_data_types() {
    let expr = Expression::Cast(Box::new(Cast {
        this: Expression::column("value"),
        to: DataType::Array {
            element_type: Box::new(DataType::Struct {
                fields: vec![
                    StructField::new(
                        "a".to_string(),
                        DataType::Int {
                            length: None,
                            integer_spelling: false,
                        },
                    ),
                    StructField::new(
                        "b".to_string(),
                        DataType::Array {
                            element_type: Box::new(DataType::Int {
                                length: None,
                                integer_spelling: false,
                            }),
                            dimension: None,
                        },
                    ),
                ],
                nested: false,
            }),
            dimension: None,
        },
        trailing_comments: Vec::new(),
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }));

    let transformed = transform_recursive(expr, &|node| match node {
        Expression::DataType(DataType::Int { .. }) => {
            Ok(Expression::DataType(DataType::BigInt { length: None }))
        }
        other => Ok(other),
    })
    .expect("transform should succeed");

    let Expression::Cast(cast) = transformed else {
        panic!("expected cast");
    };
    let DataType::Array { element_type, .. } = cast.to else {
        panic!("expected array type");
    };
    let DataType::Struct { fields, .. } = *element_type else {
        panic!("expected struct type");
    };
    assert!(matches!(fields[0].data_type, DataType::BigInt { .. }));
    let DataType::Array { element_type, .. } = &fields[1].data_type else {
        panic!("expected nested array");
    };
    assert!(matches!(element_type.as_ref(), DataType::BigInt { .. }));
}

#[test]
fn transform_recursive_visits_generated_child_metadata() {
    let expression = Expression::Like(Box::new(LikeOp {
        left: Expression::column("name"),
        right: Expression::Literal(Box::new(Literal::String("x%".to_string()))),
        escape: Some(Expression::Literal(Box::new(Literal::String(
            "!".to_string(),
        )))),
        quantifier: None,
        inferred_type: None,
    }));

    let transformed = transform_recursive(expression, &|node| match node {
        Expression::Literal(mut literal) if literal.is_string() && literal.value_str() == "!" => {
            *literal = Literal::String("#".to_string());
            Ok(Expression::Literal(literal))
        }
        other => Ok(other),
    })
    .expect("generated child should transform");

    let Expression::Like(like) = transformed else {
        panic!("expected LIKE expression");
    };
    assert!(matches!(
        like.escape,
        Some(Expression::Literal(literal)) if literal.is_string() && literal.value_str() == "#"
    ));
}
