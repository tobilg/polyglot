use polyglot_sql::dialects::{transform_recursive, Dialect, DialectType};
use polyglot_sql::expressions::{
    Array, Expression, Function, Identifier, Literal, Select, Subquery, TableRef, Union,
};
use polyglot_sql::{ComplexityGuardOptions, TranspileOptions};

fn select_star_from(source: Expression) -> Expression {
    Expression::Select(Box::new(
        Select::new().column(Expression::star()).from(source),
    ))
}

fn number(n: usize) -> Expression {
    Expression::Literal(Box::new(Literal::Number(n.to_string())))
}

fn build_deep_subquery_chain(depth: usize) -> Expression {
    let mut current = select_star_from(Expression::Table(Box::new(TableRef::new("base_table"))));

    for i in 0..depth {
        current = select_star_from(Expression::Subquery(Box::new(Subquery {
            this: current,
            alias: Some(Identifier::new(format!("s{i}"))),
            column_aliases: Vec::new(),
            alias_explicit_as: false,
            alias_keyword: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            lateral: false,
            modifiers_inside: false,
            trailing_comments: Vec::new(),
            inferred_type: None,
        })));
    }

    current
}

fn build_deep_and_chain(depth: usize) -> Expression {
    let mut current = Expression::Eq(Box::new(polyglot_sql::expressions::BinaryOp::new(
        Expression::column("c0"),
        number(0),
    )));

    for i in 1..depth {
        let next = Expression::Eq(Box::new(polyglot_sql::expressions::BinaryOp::new(
            Expression::column(format!("c{i}")),
            number(i),
        )));
        current = Expression::And(Box::new(polyglot_sql::expressions::BinaryOp::new(
            current, next,
        )));
    }

    current
}

fn build_deep_union_chain(depth: usize) -> Expression {
    let mut current = Expression::Select(Box::new(Select::new().column(number(0))));

    for i in 1..depth {
        current = Expression::Union(Box::new(Union {
            left: current,
            right: Expression::Select(Box::new(Select::new().column(number(i)))),
            all: true,
            distinct: false,
            with: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            by_name: false,
            side: None,
            kind: None,
            corresponding: false,
            strict: false,
            on_columns: Vec::new(),
        }));
    }

    current
}

fn build_deep_nested_array_materialize(depth: usize) -> Expression {
    let mut current = Expression::Literal(Box::new(Literal::String("Hello, world!".to_string())));

    for _ in 0..depth {
        current = Expression::Array(Box::new(Array {
            expressions: vec![current],
        }));
    }

    Expression::Function(Box::new(Function::new(
        "ARRAY_WITH_CONSTANT",
        vec![
            Expression::Literal(Box::new(Literal::Number("100000000".to_string()))),
            Expression::Function(Box::new(Function::new("MATERIALIZE", vec![current]))),
        ],
    )))
}

fn build_clickhouse_nested_array_sql(depth: usize) -> String {
    let mut literal = "'Hello world'".to_string();
    for _ in 0..depth {
        literal = format!("[{literal}]");
    }

    format!("SELECT length(arrayWithConstant(10000000, materialize({literal})))")
}

fn build_clickhouse_nested_function_sql(depth: usize) -> String {
    let mut expr = "b0".to_string();
    for i in 1..=depth {
        expr = format!("bitOr(bitShiftLeft({expr}, 1), b{i})");
    }
    format!("SELECT {expr} AS n_")
}

fn build_nested_unary_function_sql(depth: usize, name: &str) -> String {
    let mut expr = "id".to_string();
    for _ in 0..depth {
        expr = format!("{name}({expr})");
    }
    format!("SELECT {expr} FROM t")
}

const CLICKHOUSE_DEEP_TUPLE_SQL: &str = "SELECT * FROM ( SELECT 1 AS a GROUP BY GROUPING SETS ((tuple(toUInt128(67)))) UNION ALL SELECT materialize(2) ) WHERE a ORDER BY (75, ((tuple(((67, (67, (tuple((tuple(toLowCardinality(toLowCardinality(1))), 1)), toNullable(1))), (tuple(toUInt256(1)), 1)), 1)), 1), 1), toNullable(1)) ASC";

#[test]
fn transform_handles_deep_subquery_chain_without_large_stack() {
    let expr = build_deep_subquery_chain(2_000);
    let transformed =
        transform_recursive(expr, &|node| Ok(node)).expect("deep subquery chain should transform");
    assert!(matches!(transformed, Expression::Select(_)));
}

#[test]
fn transform_handles_deep_binary_chain_without_large_stack() {
    let expr = build_deep_and_chain(4_000);
    let transformed =
        transform_recursive(expr, &|node| Ok(node)).expect("deep binary chain should transform");
    assert!(matches!(
        transformed,
        Expression::And(_) | Expression::Eq(_)
    ));
}

#[test]
fn transform_handles_deep_union_chain_without_large_stack() {
    let expr = build_deep_union_chain(2_000);
    let transformed =
        transform_recursive(expr, &|node| Ok(node)).expect("deep union chain should transform");
    assert!(matches!(
        transformed,
        Expression::Union(_) | Expression::Select(_)
    ));
}

#[test]
fn transform_handles_deep_nested_array_function_chain_without_large_stack() {
    let expr = build_deep_nested_array_materialize(4_000);
    let transformed = transform_recursive(expr, &|node| Ok(node))
        .expect("deep nested array function chain should transform");
    assert!(matches!(transformed, Expression::Function(_)));
}

#[test]
fn clickhouse_parses_nested_array_function_sql_without_large_stack() {
    let sql = build_clickhouse_nested_array_sql(64);
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect
        .parse(&sql)
        .expect("clickhouse nested array sql should parse");
    assert_eq!(parsed.len(), 1);
}

#[test]
fn clickhouse_transforms_nested_array_function_sql_without_large_stack() {
    let sql = build_clickhouse_nested_array_sql(64);
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect
        .parse(&sql)
        .expect("clickhouse nested array sql should parse");
    let transformed = dialect
        .transform(parsed[0].clone())
        .expect("clickhouse nested array sql should transform");
    assert!(matches!(transformed, Expression::Select(_)));
}

#[test]
fn clickhouse_generates_nested_array_function_sql_without_large_stack() {
    let sql = build_clickhouse_nested_array_sql(64);
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect
        .parse(&sql)
        .expect("clickhouse nested array sql should parse");
    let generated = dialect
        .generate(&parsed[0])
        .expect("clickhouse nested array sql should generate");
    assert!(!generated.is_empty());
}

#[test]
fn clickhouse_parses_deep_tuple_order_by_sql_without_large_stack() {
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect
        .parse(CLICKHOUSE_DEEP_TUPLE_SQL)
        .expect("clickhouse deep tuple order by sql should parse");
    assert_eq!(parsed.len(), 1);
}

#[test]
fn clickhouse_transforms_deep_tuple_order_by_sql_without_large_stack() {
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect
        .parse(CLICKHOUSE_DEEP_TUPLE_SQL)
        .expect("clickhouse deep tuple order by sql should parse");
    let transformed = dialect
        .transform(parsed[0].clone())
        .expect("clickhouse deep tuple order by sql should transform");
    assert!(matches!(transformed, Expression::Select(_)));
}

#[test]
fn clickhouse_generates_deep_tuple_order_by_sql_without_large_stack() {
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect
        .parse(CLICKHOUSE_DEEP_TUPLE_SQL)
        .expect("clickhouse deep tuple order by sql should parse");
    let generated = dialect
        .generate(&parsed[0])
        .expect("clickhouse deep tuple order by sql should generate");
    assert!(!generated.is_empty());
}

#[test]
fn clickhouse_parses_deep_function_chain_sql_without_large_stack() {
    let sql = build_clickhouse_nested_function_sql(256);
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect
        .parse(&sql)
        .expect("clickhouse deep function chain sql should parse");
    assert_eq!(parsed.len(), 1);
}

#[test]
fn clickhouse_transforms_deep_function_chain_sql_without_large_stack() {
    let sql = build_clickhouse_nested_function_sql(256);
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect
        .parse(&sql)
        .expect("clickhouse deep function chain sql should parse");
    let transformed = dialect
        .transform(parsed[0].clone())
        .expect("clickhouse deep function chain sql should transform");
    assert!(matches!(transformed, Expression::Select(_)));
}

#[test]
fn clickhouse_generates_deep_function_chain_sql_without_large_stack() {
    let sql = build_clickhouse_nested_function_sql(256);
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect
        .parse(&sql)
        .expect("clickhouse deep function chain sql should parse");
    let generated = dialect
        .generate(&parsed[0])
        .expect("clickhouse deep function chain sql should generate");
    assert!(!generated.is_empty());
}

#[test]
fn postgres_parses_reasonable_nested_simple_unary_functions_without_large_stack() {
    let dialect = Dialect::get(DialectType::PostgreSQL);

    for name in ["abs", "sqrt", "upper", "lower"] {
        let sql = build_nested_unary_function_sql(20, name);
        let parsed = dialect
            .parse(&sql)
            .unwrap_or_else(|err| panic!("{name} nesting should parse: {err}"));
        assert_eq!(parsed.len(), 1);
    }
}

#[test]
fn postgres_rejects_excessive_function_call_nesting_before_parse_recursion() {
    let sql = build_nested_unary_function_sql(100, "abs");
    let dialect = Dialect::get(DialectType::PostgreSQL);
    let err = dialect
        .parse(&sql)
        .expect_err("excessive function nesting should return an error");

    assert!(
        err.to_string()
            .contains("E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_to_fabric_strict_rejects_excessive_function_call_nesting_without_abort() {
    let sql = build_nested_unary_function_sql(100, "abs");
    let postgres = Dialect::get(DialectType::PostgreSQL);
    let err = postgres
        .transpile_with(&sql, DialectType::Fabric, TranspileOptions::strict())
        .expect_err("excessive function nesting should return an error");

    assert!(
        err.to_string()
            .contains("E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED"),
        "unexpected error: {err}"
    );
}

#[test]
fn transpile_options_can_raise_function_call_nesting_budget() {
    let sql = build_nested_unary_function_sql(80, "abs");
    let postgres = Dialect::get(DialectType::PostgreSQL);
    let options = TranspileOptions::strict().with_complexity_guard(ComplexityGuardOptions {
        max_function_call_depth: Some(128),
        ..Default::default()
    });

    let transpiled = postgres
        .transpile_with(&sql, DialectType::Fabric, options)
        .expect("raised function nesting budget should allow this query");
    assert_eq!(transpiled.len(), 1);
}
