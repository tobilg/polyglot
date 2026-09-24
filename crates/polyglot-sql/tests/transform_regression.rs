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

// Issue #475: complete AST rewrites must reach typed function arguments.
#[test]
fn complete_transforms_visit_typed_function_children() {
    use polyglot_sql::{qualify_columns, rename_columns, replace_nodes, transform_all};
    for (dialect, sql) in [
        (DialectType::Oracle, "SELECT UPPER(t.c) FROM t"),
        (DialectType::Oracle, "SELECT INSTR(t.c, 'x') FROM t"),
        (DialectType::Oracle, "SELECT NVL2(t.c, 1, 0) FROM t"),
        (DialectType::Oracle, "SELECT LAST_DAY(t.c) FROM t"),
        (DialectType::Oracle, "SELECT NEXT_DAY(t.c, 'MONDAY') FROM t"),
        (DialectType::Oracle, "SELECT ADD_MONTHS(t.c, 1) FROM t"),
        (
            DialectType::Oracle,
            "SELECT MONTHS_BETWEEN(t.c, t.d) FROM t",
        ),
        (DialectType::Oracle, "SELECT TO_NUMBER(t.c) FROM t"),
        (DialectType::Oracle, "SELECT INITCAP(t.c) FROM t"),
        (
            DialectType::Oracle,
            "SELECT FIRST_VALUE(t.c) OVER (ORDER BY t.d) FROM t",
        ),
        (DialectType::Oracle, "SELECT JSON_VALUE(t.c, '$.a') FROM t"),
        (
            DialectType::Snowflake,
            "SELECT YEAR(t.c), CONTAINS(t.c, 'x'), ENDSWITH(t.c, 'x') FROM t",
        ),
        (
            DialectType::BigQuery,
            "SELECT STARTS_WITH(t.c, 'a'), ENDS_WITH(t.c, 'z'), ARRAY_LENGTH(t.c) FROM t",
        ),
        (
            DialectType::PostgreSQL,
            "SELECT t.c ~ 'x', t.c IS DISTINCT FROM t.d FROM t",
        ),
        (DialectType::ClickHouse, "SELECT quantile(0.5)(t.c) FROM t"),
        (
            DialectType::Oracle,
            "SELECT NVL2(INSTR(t.c, t.d), LAST_DAY(t.c), NEXT_DAY(t.d, 'MONDAY')) FROM t",
        ),
    ] {
        let expression = parse_one_dialect(sql, dialect);
        let columns = expression
            .find_all(|node| matches!(node, Expression::Column(_)))
            .len();
        assert!(columns > 0, "{sql}");
        let expected_nodes = expression.dfs().fold(HashMap::new(), |mut counts, node| {
            *counts.entry(node.variant_name()).or_insert(0) += 1;
            counts
        });
        let visited = RefCell::new(HashMap::new());
        let identity = transform_all(expression.clone(), &|node| {
            *visited.borrow_mut().entry(node.variant_name()).or_insert(0) += 1;
            Ok(node)
        })
        .unwrap();
        assert_eq!(visited.into_inner(), expected_nodes, "{sql}");
        assert_eq!(identity, expression, "identity rewrite changed {sql}");

        // A second visit would rewrite renamed to twice; assert exact-once behavior.
        let mapping = HashMap::from([
            ("c".to_string(), "renamed".to_string()),
            ("d".to_string(), "renamed".to_string()),
            ("renamed".to_string(), "twice".to_string()),
        ]);
        let renamed = rename_columns(expression.clone(), &mapping);
        assert_eq!(
            renamed
                .find_all(|node| matches!(node, Expression::Column(c) if c.name.name == "renamed"))
                .len(),
            columns,
            "{sql}"
        );
        assert!(
            generate_with_dialect(&renamed, dialect).contains("t.renamed"),
            "{sql}"
        );

        let unqualified = transform_all(expression.clone(), &|node| {
            Ok(match node {
                Expression::Column(mut column) => {
                    column.table = None;
                    Expression::Column(column)
                }
                other => other,
            })
        })
        .unwrap();
        let qualified = qualify_columns(unqualified, "source");
        assert_eq!(qualified.find_all(|node| matches!(node, Expression::Column(c) if c.table.as_ref().is_some_and(|t| t.name == "source"))).len(), columns, "{sql}");
        let replaced = replace_nodes(
            expression,
            |node| matches!(node, Expression::Column(_)),
            Expression::column("replacement"),
        );
        assert_eq!(
            replaced
                .find_all(
                    |node| matches!(node, Expression::Column(c) if c.name.name == "replacement")
                )
                .len(),
            columns,
            "{sql}"
        );
    }
}

#[test]
fn complete_transform_visits_clauses_and_propagates_errors() {
    use polyglot_sql::transform_all;
    for sql in [
        "WITH x AS (SELECT c FROM t) SELECT c FROM x ORDER BY c",
        "SELECT FIRST_VALUE(c) OVER w FROM t WINDOW w AS (PARTITION BY d ORDER BY e ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        "SELECT SUM(c) FILTER (WHERE d > 0) FROM t",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY c) FROM t",
        "UPDATE t SET c = YEAR(d) WHERE e = 1 RETURNING c",
    ] {
        let original = parse_one(sql);
        let count = original.dfs().count();
        let visits = std::cell::Cell::new(0);
        let rewritten = transform_all(original.clone(), &|node| {
            visits.set(visits.get() + 1);
            Ok(node)
        }).unwrap();
        assert_eq!(visits.get(), count, "{sql}");
        assert_eq!(rewritten, original, "{sql}");
        let error = transform_all(original, &|node| match node {
            Expression::Column(_) => Err(polyglot_sql::Error::Internal("column failure".into())),
            other => Ok(other),
        }).unwrap_err();
        assert!(error.to_string().contains("column failure"), "{sql}: {error}");
    }
}

#[test]
fn complete_transform_is_bottom_up_and_does_not_revisit_replacements() {
    use polyglot_sql::{expressions::UnaryFunc, transform_all};
    let visits = RefCell::new(Vec::new());
    let expression = Expression::Year(Box::new(UnaryFunc::new(Expression::column("c"))));
    let result = transform_all(expression, &|node| {
        visits.borrow_mut().push(node.variant_name());
        Ok(match node {
            Expression::Column(_) => {
                Expression::Upper(Box::new(UnaryFunc::new(Expression::column("new"))))
            }
            Expression::Year(year) => {
                assert!(matches!(year.this, Expression::Upper(_)));
                Expression::Year(year)
            }
            other => other,
        })
    })
    .unwrap();
    assert_eq!(*visits.borrow(), vec!["column", "year"]);
    assert_eq!(result.dfs().count(), 3);
}

#[test]
fn ast_helpers_preserve_embedded_wrapper_callbacks() {
    let expr = parse_one("SELECT CAST(c AS INT) FROM a JOIN b ON a.id = b.id ORDER BY c");
    let transformed = replace_by_type(
        expr,
        |_| true,
        |node| match node {
            Expression::Join(mut join) => {
                join.kind = JoinKind::Left;
                Expression::Join(join)
            }
            Expression::Ordered(mut ordered) => {
                ordered.desc = true;
                Expression::Ordered(ordered)
            }
            Expression::DataType(DataType::Int { .. }) => {
                Expression::DataType(DataType::BigInt { length: None })
            }
            other => other,
        },
    );
    let sql = transformed.sql();
    assert!(sql.contains("LEFT JOIN"), "{sql}");
    assert!(sql.contains("BIGINT"), "{sql}");
    assert!(sql.contains("c DESC"), "{sql}");
}

#[test]
fn dialect_transforms_visit_interval_children_before_wrapper_477() {
    let expr = parse_one_dialect("SELECT INTERVAL ABS(n) MONTH", DialectType::BigQuery);
    let visits = RefCell::new(Vec::new());
    let result = transform_recursive(expr, &|node| {
        visits.borrow_mut().push(node.variant_name().to_string());
        Ok(match node {
            Expression::Column(mut column) if column.name.name == "n" => {
                column.name.name = "months".to_string();
                Expression::Column(column)
            }
            other => other,
        })
    })
    .unwrap();
    let visits = visits.into_inner();
    assert!(first_index(&visits, "column") < first_index(&visits, "interval"));
    assert_eq!(visits.iter().filter(|name| *name == "column").count(), 1);
    assert_eq!(
        generate_with_dialect(&result, DialectType::DuckDB),
        "SELECT INTERVAL (ABS(months)) MONTH"
    );
    // MySQL permits bare columns and calls in its interval expression syntax.
    assert_eq!(
        generate_with_dialect(&result, DialectType::MySQL),
        "SELECT INTERVAL ABS(months) MONTH"
    );
}
