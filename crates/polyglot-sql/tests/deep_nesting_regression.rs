use polyglot_sql::dialects::{transform_recursive, Dialect, DialectType};
use polyglot_sql::expressions::{
    Array, Expression, Function, Identifier, Literal, Select, Subquery, TableRef, Union,
};
use polyglot_sql::{ComplexityGuardOptions, TranspileOptions};

#[test]
fn parse_and_validation_share_per_call_guard_options() {
    use polyglot_sql::{
        parse_one_with_options, parse_with_options, validate_with_options, ParseOptions,
        ValidationOptions,
    };
    let sql = |depth| {
        format!(
            "SELECT {}value{} FROM records",
            "COALESCE(".repeat(depth),
            ", 0)".repeat(depth)
        )
    };
    let dialect = DialectType::Snowflake;
    for depth in [64, 65] {
        let sql = sql(depth);
        assert_eq!(
            parse_with_options(&sql, dialect, &ParseOptions::default()).is_ok(),
            depth == 64
        );
        assert_eq!(
            validate_with_options(&sql, dialect, &ValidationOptions::default()).valid,
            depth == 64
        );
        for limit in [Some(128), None] {
            let guard = ComplexityGuardOptions {
                max_function_call_depth: limit,
                ..Default::default()
            };
            let options = ParseOptions {
                complexity_guard: Some(guard),
            };
            assert_eq!(
                parse_with_options(&sql, dialect, &options).unwrap().len(),
                1
            );
            assert!(parse_one_with_options(&sql, dialect, &options).is_ok());
            assert!(
                validate_with_options(
                    &sql,
                    dialect,
                    &ValidationOptions {
                        complexity_guard: Some(guard),
                        ..Default::default()
                    }
                )
                .valid
            );
        }
    }
    assert!(
        parse_with_options(&sql(65), DialectType::ClickHouse, &ParseOptions::default()).is_ok()
    );
    let limited = ParseOptions {
        complexity_guard: Some(ComplexityGuardOptions {
            max_function_call_depth: None,
            max_input_bytes: Some(1),
            ..Default::default()
        }),
    };
    assert!(parse_one_with_options(&sql(65), dialect, &limited)
        .unwrap_err()
        .to_string()
        .contains("E_GUARD_INPUT_TOO_LARGE"));
    assert!(
        parse_one_with_options("SELECT 1; SELECT 2", dialect, &ParseOptions::default()).is_err()
    );
    assert!(parse_one_with_options("SELECT 1", dialect, &ParseOptions::default()).is_ok());
}

#[test]
fn guard_json_rejects_invalid_limits_and_preserves_null() {
    for name in [
        "maxParserDepth",
        "maxInputBytes",
        "maxTokens",
        "maxAstNodes",
        "maxAstDepth",
        "maxParenthesisDepth",
        "maxFunctionCallDepth",
    ] {
        for value in ["-1", "true", "1.0", "\"128\"", "18446744073709551616"] {
            assert!(serde_json::from_str::<ComplexityGuardOptions>(&format!(
                "{{\"{name}\":{value}}}"
            ))
            .is_err());
        }
        for value in ["0", "128", "null"] {
            let options: ComplexityGuardOptions =
                serde_json::from_str(&format!("{{\"{name}\":{value}}}")).unwrap();
            assert_eq!(
                serde_json::to_value(options).unwrap()[name],
                serde_json::from_str::<serde_json::Value>(value).unwrap()
            );
        }
    }
    assert!(
        serde_json::from_str::<ComplexityGuardOptions>(r#"{"max_function_call_depth":128}"#)
            .is_err()
    );
}

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
            inferred_type: None,
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

#[test]
fn parser_depth_options_and_independent_guards() {
    use polyglot_sql::parser::{Parser, ParserConfig};
    use polyglot_sql::tokens::Tokenizer;
    let parse = |sql: &str, limit| {
        let tokens = Tokenizer::default().tokenize(sql).unwrap();
        Parser::with_config(
            tokens,
            ParserConfig {
                complexity_guard: ComplexityGuardOptions {
                    max_parser_depth: limit,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .parse()
    };
    assert!(parse("SELECT 1", Some(0))
        .unwrap_err()
        .to_string()
        .contains("E_GUARD_PARSER_DEPTH_EXCEEDED"));
    let sql = format!("SELECT {}1", "~ ".repeat(12));
    assert!(parse(&sql, Some(8))
        .unwrap_err()
        .to_string()
        .contains("E_GUARD_PARSER_DEPTH_EXCEEDED"));
    assert!(parse(&sql, Some(32)).is_ok());
    assert!(parse(&sql, None).is_ok());
    // Siblings and sequential statements release their depth allocations.
    assert!(parse("SELECT ~1, ~2, ~3; SELECT ~4", Some(8)).is_ok());
    assert!(parse("SELECT (1", None).is_err());
    // Disabling parser depth does not disable function-nesting protection.
    let nested = build_nested_unary_function_sql(100, "abs");
    assert!(parse(&nested, None)
        .unwrap_err()
        .to_string()
        .contains("E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED"));
}

#[test]
fn parser_depth_guard_covers_nested_types_and_procedural_children() {
    use polyglot_sql::parser::{Parser, ParserConfig};
    let dialect = Dialect::get(DialectType::TSQL);
    let mut sql = "SELECT 1".to_string();
    for _ in 0..12 {
        sql = format!("BEGIN TRY {sql} END TRY BEGIN CATCH SELECT 2 END CATCH");
    }
    let mut parser = Parser::with_config(
        dialect.tokenize(&sql).unwrap(),
        ParserConfig {
            dialect: Some(DialectType::TSQL),
            complexity_guard: ComplexityGuardOptions {
                max_parser_depth: Some(8),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    assert!(parser
        .parse()
        .unwrap_err()
        .to_string()
        .contains("E_GUARD_PARSER_DEPTH_EXCEEDED"));
    let dialect = Dialect::get(DialectType::DuckDB);
    let mut sql = "INT".to_string();
    for _ in 0..12 {
        sql = format!("STRUCT(x {sql})");
    }
    let mut parser = Parser::with_config(
        dialect.tokenize(&sql).unwrap(),
        ParserConfig {
            dialect: Some(DialectType::DuckDB),
            complexity_guard: ComplexityGuardOptions {
                max_parser_depth: Some(8),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    assert!(parser
        .parse_standalone_data_type()
        .unwrap_err()
        .to_string()
        .contains("E_GUARD_PARSER_DEPTH_EXCEEDED"));
}

#[test]
fn iterative_array_types_preserve_delimiters_and_suffixes() {
    use polyglot_sql::expressions::DataType;
    use polyglot_sql::parser::{Parser, ParserConfig};
    let dialect = Dialect::get(DialectType::Materialize);
    let parse = |sql: &str| {
        Parser::with_config(
            dialect.tokenize(sql).unwrap(),
            ParserConfig {
                dialect: Some(DialectType::Materialize),
                ..Default::default()
            },
        )
        .parse_standalone_data_type()
    };
    let array = |element, dimension| DataType::Array {
        element_type: Box::new(element),
        dimension,
    };
    let int = DataType::Int {
        length: None,
        integer_spelling: false,
    };
    for sql in [
        "ARRAY<ARRAY<INT>>",
        "ARRAY(ARRAY(INT))",
        "ARRAY<ARRAY(INT)>",
        "ARRAY<\"ARRAY\"<INT>>",
    ] {
        assert_eq!(parse(sql).unwrap(), array(array(int.clone(), None), None));
    }
    assert_eq!(
        parse("ARRAY<ARRAY<INT>[3]>[2]").unwrap(),
        array(
            array(array(array(int.clone(), None), Some(3)), None),
            Some(2)
        )
    );
    assert_eq!(
        parse("ARRAY<ARRAY<INT> LIST> LIST").unwrap(),
        DataType::List {
            element_type: Box::new(array(
                DataType::List {
                    element_type: Box::new(array(int, None)),
                },
                None,
            )),
        }
    );
    for sql in ["ARRAY<ARRAY<INT>", "ARRAY(ARRAY<INT))", "ARRAY<ARRAY(INT>>"] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn complete_transform_handles_deep_typed_function_chains() {
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(|| {
            use polyglot_sql::{expressions::UnaryFunc, transform_all};
            let mut expression = Expression::column("c");
            for _ in 0..20_000 {
                expression = Expression::Year(Box::new(UnaryFunc::new(expression)));
            }
            let visits = std::cell::Cell::new(0);
            // Collapse each parent after visiting its child, also avoiding recursive
            // destruction of the output tree on this deliberately small thread stack.
            let output = transform_all(expression, &|node| {
                visits.set(visits.get() + 1);
                Ok(match node {
                    Expression::Year(year) => year.this,
                    other => other,
                })
            })
            .unwrap();
            assert_eq!(visits.get(), 20_001);
            assert!(matches!(output, Expression::Column(_)));
        })
        .unwrap()
        .join()
        .unwrap();
}
