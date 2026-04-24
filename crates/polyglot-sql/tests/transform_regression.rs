use std::cell::RefCell;

use polyglot_sql::dialects::transform_recursive;
use polyglot_sql::expressions::{Cast, DataType, Expression, JoinKind, StructField};
use polyglot_sql::Parser;

fn parse_one(sql: &str) -> Expression {
    Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("expected one statement")
}

fn first_index(order: &[String], target: &str) -> usize {
    order
        .iter()
        .position(|name| name == target)
        .unwrap_or_else(|| panic!("missing {target} in visit order: {order:?}"))
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
