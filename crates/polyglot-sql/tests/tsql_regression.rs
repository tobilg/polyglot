//! Regression tests for T-SQL parsing/generation and PostgreSQL -> T-SQL transpilation.

use polyglot_sql::generator::{Generator, GeneratorConfig};
use polyglot_sql::{
    generate, generate_data_type, get_all_tables, parse, parse_data_type, transpile, validate,
    DataType, Dialect, DialectType, Expression, ExpressionWalk, Parser, TokenType,
    TranspileOptions,
};

fn pg_to_tsql(sql: &str) -> String {
    transpile(sql, DialectType::PostgreSQL, DialectType::TSQL)
        .unwrap_or_else(|e| panic!("transpile failed for {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("expected at least one statement")
}

fn generate_tsql(expr: &Expression) -> String {
    let config = GeneratorConfig {
        dialect: Some(DialectType::TSQL),
        ..Default::default()
    };
    let mut generator = Generator::with_config(config);
    generator
        .generate(expr)
        .expect("expression should generate as T-SQL")
}

const TRY_CATCH_SQL: &str = r#"BEGIN TRY
    INSERT INTO orders (id, amount) VALUES (1, 100.00);
    UPDATE inventory SET qty = qty - 1 WHERE product_id = 42;
END TRY
BEGIN CATCH
    INSERT INTO error_log (msg) VALUES (ERROR_MESSAGE());
END CATCH"#;

// ---------------------------------------------------------------------------
// T-SQL hex literals
// ---------------------------------------------------------------------------

#[test]
fn tsql_hex_literals_tokenize_as_hex_strings() {
    let dialect = Dialect::get(DialectType::TSQL);
    let tokens = dialect
        .tokenize("SELECT * FROM Employees WHERE EmployeeId = 0x1")
        .expect("T-SQL hex literal should tokenize");

    let hex = tokens
        .iter()
        .find(|token| token.token_type == TokenType::HexString)
        .expect("expected a HexString token for 0x1");

    assert_eq!(hex.text, "1");
}

#[test]
fn tsql_hex_literals_parse_and_roundtrip() {
    for sql in [
        "SELECT * FROM Employees WHERE EmployeeId = 0x1",
        "INSERT INTO t (bin) VALUES (0xFF)",
        "SELECT 0xCAFE + 1",
    ] {
        let ast = parse(sql, DialectType::TSQL)
            .unwrap_or_else(|error| panic!("T-SQL hex literal should parse for {sql:?}: {error}"));
        assert_eq!(ast.len(), 1);
        assert_eq!(generate_tsql(&ast[0]), sql);

        let roundtrip =
            transpile(sql, DialectType::TSQL, DialectType::TSQL).unwrap_or_else(|error| {
                panic!("T-SQL hex literal should transpile for {sql:?}: {error}")
            });
        assert_eq!(roundtrip, vec![sql.to_string()]);
    }
}

#[test]
fn tsql_max_length_types_parse_generate_and_transpile() {
    for type_name in ["VARBINARY(MAX)", "VARCHAR(MAX)", "NVARCHAR(MAX)"] {
        let data_type = parse_data_type(type_name, DialectType::TSQL).unwrap_or_else(|error| {
            panic!("{type_name} should parse as a standalone type: {error}")
        });
        assert_eq!(
            data_type,
            DataType::Custom {
                name: type_name.to_string()
            }
        );
        assert_eq!(
            generate_data_type(&data_type, DialectType::TSQL)
                .unwrap_or_else(|error| panic!("{type_name} should generate as T-SQL: {error}")),
            type_name
        );

        let create_sql = format!("CREATE TABLE t (c {type_name})");
        let ast = parse(&create_sql, DialectType::TSQL)
            .unwrap_or_else(|error| panic!("{create_sql} should parse: {error}"));
        let Expression::CreateTable(create) = &ast[0] else {
            panic!(
                "expected CreateTable for {create_sql}, got {}",
                ast[0].variant_name()
            );
        };
        assert_eq!(create.columns.len(), 1);
        assert_eq!(
            create.columns[0].data_type,
            DataType::Custom {
                name: type_name.to_string()
            }
        );
        assert_eq!(generate_tsql(&ast[0]), create_sql);
        assert_eq!(
            transpile(&create_sql, DialectType::TSQL, DialectType::TSQL)
                .unwrap_or_else(|error| panic!("{create_sql} should identity-transpile: {error}")),
            vec![create_sql]
        );

        let cast_sql = format!("SELECT CAST(x AS {type_name})");
        let ast = parse(&cast_sql, DialectType::TSQL)
            .unwrap_or_else(|error| panic!("{cast_sql} should parse: {error}"));
        assert_eq!(generate_tsql(&ast[0]), cast_sql);
        assert_eq!(
            transpile(&cast_sql, DialectType::TSQL, DialectType::TSQL)
                .unwrap_or_else(|error| panic!("{cast_sql} should identity-transpile: {error}")),
            vec![cast_sql]
        );
    }

    let postgres = transpile(
        "SELECT CAST(x AS VARBINARY(MAX))",
        DialectType::TSQL,
        DialectType::PostgreSQL,
    )
    .expect("VARBINARY(MAX) should no longer fail before target generation");
    assert_eq!(postgres.len(), 1);
    assert!(!postgres[0].is_empty());
}

#[test]
fn tsql_datepart_dayofweek_generates_valid_tsql_datepart() {
    let cases = [
        ("WEEKDAY", "WEEKDAY"),
        ("dw", "WEEKDAY"),
        ("DAYOFWEEK", "WEEKDAY"),
        ("isowk", "ISO_WEEK"),
        ("WEEKISO", "ISO_WEEK"),
        ("tz", "TZOFFSET"),
        ("TIMEZONE_MINUTE", "TZOFFSET"),
    ];

    for dialect in [DialectType::TSQL, DialectType::Fabric] {
        for (input, expected) in cases {
            let sql = format!("SELECT DATEPART({input}, o.order_date) AS dow FROM orders o");
            let ast = parse(&sql, dialect)
                .unwrap_or_else(|error| panic!("{dialect:?} DATEPART should parse: {error}"));
            assert_eq!(ast.len(), 1);

            let generated = generate(&ast[0], dialect)
                .unwrap_or_else(|error| panic!("{dialect:?} DATEPART should generate: {error}"));

            assert_eq!(
                generated,
                format!("SELECT DATEPART({expected}, o.order_date) AS dow FROM orders AS o"),
                "failed for {dialect:?} DATEPART({input}, ...)"
            );
        }
    }
}

#[test]
fn tsql_datepart_dayofweek_transpiles_to_valid_tsql_datepart() {
    let sql = "SELECT DATEPART(WEEKDAY, o.order_date) AS dow FROM orders o";

    assert_eq!(
        transpile(sql, DialectType::TSQL, DialectType::TSQL).expect("TSQL identity transpile"),
        vec!["SELECT DATEPART(WEEKDAY, o.order_date) AS dow FROM orders AS o"]
    );
    assert_eq!(
        transpile(sql, DialectType::TSQL, DialectType::Fabric).expect("TSQL to Fabric transpile"),
        vec!["SELECT DATEPART(WEEKDAY, o.order_date) AS dow FROM orders AS o"]
    );
}

// ---------------------------------------------------------------------------
// T-SQL SET options
// ---------------------------------------------------------------------------

#[test]
fn tsql_set_statistics_options_parse_as_commands() {
    for sql in [
        "SET STATISTICS TIME ON",
        "SET STATISTICS TIME OFF",
        "SET STATISTICS IO ON",
        "SET STATISTICS IO OFF",
        "SET STATISTICS XML ON",
        "SET STATISTICS XML OFF",
        "SET STATISTICS PROFILE ON",
        "SET STATISTICS PROFILE OFF",
        "SET STATISTICS IO, TIME ON",
    ] {
        let ast = parse(sql, DialectType::TSQL).expect("SET STATISTICS should parse");
        assert_eq!(ast.len(), 1);
        assert!(
            matches!(ast[0], Expression::Command(_)),
            "expected Command for {sql}, got {}",
            ast[0].variant_name()
        );
        assert_eq!(generate_tsql(&ast[0]), sql);
    }
}

#[test]
fn tsql_set_statistics_consumes_only_current_statement() {
    let ast =
        parse("SET STATISTICS TIME ON; SELECT 1", DialectType::TSQL).expect("batch should parse");

    assert_eq!(ast.len(), 2);
    assert!(matches!(ast[0], Expression::Command(_)));
    assert!(matches!(ast[1], Expression::Select(_)));
    assert_eq!(generate_tsql(&ast[0]), "SET STATISTICS TIME ON");
}

#[test]
fn tsql_simple_set_options_remain_structured() {
    for sql in ["SET NOCOUNT ON", "SET XACT_ABORT ON", "SET ANSI_NULLS OFF"] {
        let ast = parse(sql, DialectType::TSQL).expect("T-SQL SET option should parse");
        assert_eq!(ast.len(), 1);
        assert!(
            matches!(ast[0], Expression::SetStatement(_)),
            "expected SetStatement for {sql}, got {}",
            ast[0].variant_name()
        );
        assert_eq!(generate_tsql(&ast[0]), sql);
    }
}

#[test]
fn tsql_set_identity_insert_remains_structured() {
    for state in ["ON", "OFF"] {
        let sql = format!("SET IDENTITY_INSERT dbo.Employees {state}");
        let sql_with_semicolon = format!("{sql};");
        let ast = parse(&sql_with_semicolon, DialectType::TSQL)
            .expect("SET IDENTITY_INSERT should parse");
        assert_eq!(ast.len(), 1);

        let Expression::SetStatement(set) = &ast[0] else {
            panic!(
                "expected SetStatement for {sql}, got {}",
                ast[0].variant_name()
            );
        };

        assert_eq!(set.items.len(), 1);
        let item = &set.items[0];
        assert_eq!(item.name.to_string(), "IDENTITY_INSERT");
        assert!(item.no_equals);

        let Expression::Tuple(value) = &item.value else {
            panic!("expected tuple value for SET IDENTITY_INSERT");
        };
        assert_eq!(value.expressions.len(), 2);

        let Expression::Table(table) = &value.expressions[0] else {
            panic!("expected table target for SET IDENTITY_INSERT");
        };
        assert_eq!(
            table.schema.as_ref().map(|schema| schema.name.as_str()),
            Some("dbo")
        );
        assert_eq!(table.name.name, "Employees");
        assert_eq!(value.expressions[1].to_string(), state);

        assert_eq!(generate_tsql(&ast[0]), sql);
        assert_eq!(
            transpile(&sql_with_semicolon, DialectType::TSQL, DialectType::TSQL)
                .expect("transpile")[0],
            sql
        );
        assert!(validate(&sql_with_semicolon, DialectType::TSQL).valid);
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL LATERAL joins -> T-SQL APPLY
// ---------------------------------------------------------------------------

#[test]
fn postgres_lateral_joins_map_to_tsql_apply() {
    let lateral_subquery = "(SELECT v FROM lineitem WHERE l_orderkey = o.id)";
    let cross_apply =
        "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT v AS v FROM lineitem WHERE l_orderkey = o.id) AS t";
    let outer_apply =
        "SELECT o.id, t.v FROM orders AS o OUTER APPLY (SELECT v AS v FROM lineitem WHERE l_orderkey = o.id) AS t";

    let cases = [
        (
            format!("SELECT o.id, t.v FROM orders o CROSS JOIN LATERAL {lateral_subquery} t"),
            cross_apply,
        ),
        (
            format!("SELECT o.id, t.v FROM orders o JOIN LATERAL {lateral_subquery} t ON true"),
            cross_apply,
        ),
        (
            format!(
                "SELECT o.id, t.v FROM orders o INNER JOIN LATERAL {lateral_subquery} t ON true"
            ),
            cross_apply,
        ),
        (
            format!(
                "SELECT o.id, t.v FROM orders o LEFT JOIN LATERAL {lateral_subquery} t ON true"
            ),
            outer_apply,
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(&sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_parenthesized_lateral_joins_map_to_tsql_apply() {
    let cases = [
        (
            "SELECT * FROM (orders o JOIN LATERAL (SELECT 1 AS x) a ON true)",
            "SELECT * FROM (orders AS o CROSS APPLY (SELECT 1 AS x) AS a)",
        ),
        (
            "SELECT * FROM (orders o LEFT JOIN LATERAL (SELECT 1 AS x) a ON true)",
            "SELECT * FROM (orders AS o OUTER APPLY (SELECT 1 AS x) AS a)",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_comma_lateral_maps_to_tsql_cross_apply() {
    assert_eq!(
        pg_to_tsql("SELECT o.id, t.v FROM orders o, LATERAL (SELECT v FROM lineitem WHERE l_orderkey = o.id) t"),
        "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT v AS v FROM lineitem WHERE l_orderkey = o.id) AS t"
    );
}

#[test]
fn postgres_lateral_join_on_predicate_pushes_into_tsql_apply_rhs() {
    let cases = [
        (
            "SELECT o.id, t.v FROM orders o JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t ON t.v > 0",
            "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT * FROM (SELECT v AS v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source WHERE _polyglot_lateral_source.v > 0) AS t",
        ),
        (
            "SELECT o.id, t.v FROM orders o LEFT JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t ON t.v > 0",
            "SELECT o.id, t.v FROM orders AS o OUTER APPLY (SELECT * FROM (SELECT v AS v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source WHERE _polyglot_lateral_source.v > 0) AS t",
        ),
        (
            "SELECT o.id, t.x FROM orders o JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t(x) ON t.x > 0",
            "SELECT o.id, t.x FROM orders AS o CROSS APPLY (SELECT * FROM (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source(x) WHERE _polyglot_lateral_source.x > 0) AS t(x)",
        ),
        (
            "SELECT o.id, t.v FROM orders o JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t ON o.id > 0",
            "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT * FROM (SELECT v AS v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source WHERE o.id > 0) AS t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_lateral_using_inside_joined_table_fails_tsql_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT * FROM (orders o JOIN LATERAL (SELECT 1 AS id) a USING (id))",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject residual LATERAL");

    assert!(
        err.to_string().contains("LATERAL joins and subqueries"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_framed_named_window_inlines_frame_stripped_copy_for_tsql_ranking_function() {
    let out = pg_to_tsql(
        "SELECT row_number() OVER w AS rn, avg(o_totalprice) OVER w AS av \
         FROM orders \
         WINDOW w AS (PARTITION BY o_custkey ORDER BY o_orderdate NULLS FIRST \
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    );

    assert_eq!(
        out,
        "SELECT ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS rn, AVG(o_totalprice) OVER w AS av FROM orders WINDOW w AS (PARTITION BY o_custkey ORDER BY o_orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    );
}

#[test]
fn postgres_inline_window_frame_is_stripped_for_tsql_ranking_function() {
    let out = pg_to_tsql(
        "SELECT row_number() OVER (PARTITION BY o_custkey ORDER BY o_orderdate NULLS FIRST \
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn \
         FROM orders",
    );

    assert_eq!(
        out,
        "SELECT ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS rn FROM orders"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL NULLS FIRST/LAST -> T-SQL CASE sort key
// ---------------------------------------------------------------------------

#[test]
fn postgres_null_ordering_rewrites_for_tsql() {
    let cases = [
        (
            "SELECT id FROM t ORDER BY id ASC",
            "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END, id ASC",
        ),
        (
            "SELECT id FROM t ORDER BY id ASC NULLS LAST",
            "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END, id ASC",
        ),
        (
            "SELECT id FROM t ORDER BY id ASC NULLS FIRST",
            "SELECT id FROM t ORDER BY id ASC",
        ),
        (
            "SELECT id FROM t ORDER BY id DESC",
            "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END DESC, id DESC",
        ),
        (
            "SELECT id FROM t ORDER BY id DESC NULLS FIRST",
            "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END DESC, id DESC",
        ),
        (
            "SELECT id FROM t ORDER BY id DESC NULLS LAST",
            "SELECT id FROM t ORDER BY id DESC",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_random_ordering_does_not_add_null_sort_key_for_tsql() {
    let out = pg_to_tsql(r#"SELECT * FROM "test_table" ORDER BY RANDOM() LIMIT 5"#);
    assert_eq!(out, "SELECT TOP 5 * FROM [test_table] ORDER BY RAND()");
}

#[test]
fn postgres_set_operation_modifiers_wrap_for_tsql() {
    let cases = [
        (
            "SELECT c_custkey FROM customer EXCEPT SELECT o_custkey FROM orders ORDER BY c_custkey LIMIT 100",
            "SELECT TOP 100 * FROM (SELECT c_custkey FROM customer EXCEPT SELECT o_custkey FROM orders) AS _l_0 ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey",
        ),
        (
            "SELECT c_custkey FROM customer UNION ALL SELECT o_custkey FROM orders ORDER BY c_custkey LIMIT 100",
            "SELECT TOP 100 * FROM (SELECT c_custkey FROM customer UNION ALL SELECT o_custkey FROM orders) AS _l_0 ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey",
        ),
        (
            "SELECT c_custkey FROM customer INTERSECT SELECT o_custkey FROM orders ORDER BY c_custkey LIMIT 100",
            "SELECT TOP 100 * FROM (SELECT c_custkey FROM customer INTERSECT SELECT o_custkey FROM orders) AS _l_0 ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey",
        ),
        (
            "SELECT c_custkey FROM customer EXCEPT SELECT o_custkey FROM orders ORDER BY c_custkey LIMIT 100 OFFSET 2",
            "SELECT * FROM (SELECT c_custkey FROM customer EXCEPT SELECT o_custkey FROM orders) AS _l_0 ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey OFFSET 2 ROWS FETCH NEXT 100 ROWS ONLY",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL WITH RECURSIVE -> T-SQL WITH
// ---------------------------------------------------------------------------

#[test]
fn postgres_recursive_cte_omits_recursive_keyword_for_tsql() {
    let out = pg_to_tsql(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n FROM r",
    );
    assert_eq!(
        out,
        "WITH r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n FROM r"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL MOD function -> T-SQL modulo operator
// ---------------------------------------------------------------------------

#[test]
fn postgres_mod_function_maps_to_tsql_percent_operator() {
    let cases = [
        ("SELECT mod(a, 7) AS m FROM t", "SELECT a % 7 AS m FROM t"),
        (
            "SELECT mod(a + 1, b * 2) AS m FROM t",
            "SELECT (a + 1) % (b * 2) AS m FROM t",
        ),
        ("SELECT a % 7 AS m FROM t", "SELECT a % 7 AS m FROM t"),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL row-value IN subquery -> T-SQL correlated EXISTS
// ---------------------------------------------------------------------------

#[test]
fn postgres_row_value_in_subquery_maps_to_tsql_exists() {
    let out = pg_to_tsql("SELECT a FROM t WHERE (a, b) IN (SELECT x, y FROM u WHERE q < 10)");
    assert_eq!(
        out,
        "SELECT a FROM t WHERE EXISTS(SELECT 1 FROM u WHERE u.x = t.a AND u.y = t.b AND q < 10)"
    );
}

#[test]
fn postgres_row_value_not_in_subquery_is_not_rewritten_for_tsql() {
    let out = pg_to_tsql("SELECT a FROM t WHERE (a, b) NOT IN (SELECT x, y FROM u WHERE q < 10)");
    assert_eq!(
        out,
        "SELECT a FROM t WHERE NOT (a, b) IN (SELECT x, y FROM u WHERE q < 10)"
    );
}

#[test]
fn postgres_row_value_in_subquery_arity_mismatch_is_not_rewritten_for_tsql() {
    let out = pg_to_tsql("SELECT a FROM t WHERE (a, b) IN (SELECT x FROM u)");
    assert_eq!(out, "SELECT a FROM t WHERE (a, b) IN (SELECT x FROM u)");
}

// ---------------------------------------------------------------------------
// PostgreSQL statistical aggregates -> T-SQL names
// ---------------------------------------------------------------------------

#[test]
fn postgres_statistical_aggregates_map_to_tsql_names() {
    let cases = [
        (
            "SELECT stddev_samp(x) AS s FROM t",
            "SELECT STDEV(x) AS s FROM t",
        ),
        (
            "SELECT stddev_pop(x) AS s FROM t",
            "SELECT STDEVP(x) AS s FROM t",
        ),
        (
            "SELECT var_samp(x) AS s FROM t",
            "SELECT VAR(x) AS s FROM t",
        ),
        (
            "SELECT var_pop(x) AS s FROM t",
            "SELECT VARP(x) AS s FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL boolean aggregates -> T-SQL CASE aggregates
// ---------------------------------------------------------------------------

#[test]
fn postgres_boolean_aggregates_map_to_tsql_case_aggregates() {
    let cases = [
        (
            "SELECT bool_and(x > 0) AS s FROM t",
            "SELECT CAST(MIN(CASE WHEN x > 0 THEN 1 WHEN NOT x > 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
        (
            "SELECT bool_or(x > 0) AS s FROM t",
            "SELECT CAST(MAX(CASE WHEN x > 0 THEN 1 WHEN NOT x > 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
        (
            "SELECT every(x > 0) AS s FROM t",
            "SELECT CAST(MIN(CASE WHEN x > 0 THEN 1 WHEN NOT x > 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
        (
            "SELECT bool_and(x) AS s FROM t",
            "SELECT CAST(MIN(CASE WHEN x <> 0 THEN 1 WHEN NOT x <> 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
        (
            "SELECT bool_or(x > 0) FILTER (WHERE y > 0) AS s FROM t",
            "SELECT CAST(MAX(CASE WHEN y > 0 AND x > 0 THEN 1 WHEN y > 0 AND NOT x > 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_scalar_boolean_values_map_to_tsql_case_values() {
    let cases = [
        (
            "SELECT (l_quantity > 30) AS b FROM tpch.lineitem",
            "SELECT CASE WHEN (l_quantity > 30) THEN 1 WHEN NOT (l_quantity > 30) THEN 0 END AS b FROM tpch.lineitem",
        ),
        (
            "SELECT COUNT(*) AS c FROM tpch.lineitem GROUP BY (l_quantity > 30)",
            "SELECT COUNT_BIG(*) AS c FROM tpch.lineitem GROUP BY CASE WHEN (l_quantity > 30) THEN 1 WHEN NOT (l_quantity > 30) THEN 0 END",
        ),
        (
            "SELECT (l_quantity > 30) AS b, COUNT(*) AS c FROM tpch.lineitem WHERE l_orderkey < 1000 GROUP BY (l_quantity > 30) ORDER BY b",
            "SELECT CASE WHEN (l_quantity > 30) THEN 1 WHEN NOT (l_quantity > 30) THEN 0 END AS b, COUNT_BIG(*) AS c FROM tpch.lineitem WHERE l_orderkey < 1000 GROUP BY CASE WHEN (l_quantity > 30) THEN 1 WHEN NOT (l_quantity > 30) THEN 0 END ORDER BY CASE WHEN b IS NULL THEN 1 ELSE 0 END, b",
        ),
        (
            "SELECT l_quantity FROM tpch.lineitem ORDER BY (l_quantity > 30)",
            "SELECT l_quantity FROM tpch.lineitem ORDER BY CASE WHEN CASE WHEN (l_quantity > 30) THEN 1 WHEN NOT (l_quantity > 30) THEN 0 END IS NULL THEN 1 ELSE 0 END, CASE WHEN (l_quantity > 30) THEN 1 WHEN NOT (l_quantity > 30) THEN 0 END",
        ),
        (
            "SELECT COUNT(*) OVER (PARTITION BY (l_quantity > 30)) AS c FROM tpch.lineitem",
            "SELECT COUNT_BIG(*) OVER (PARTITION BY CASE WHEN (l_quantity > 30) THEN 1 WHEN NOT (l_quantity > 30) THEN 0 END) AS c FROM tpch.lineitem",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_predicate_boolean_contexts_stay_predicates_for_tsql() {
    let out = pg_to_tsql(
        "SELECT COUNT(*) AS c FROM tpch.lineitem WHERE l_quantity > 30 HAVING COUNT(*) > 0",
    );
    assert_eq!(
        out,
        "SELECT COUNT_BIG(*) AS c FROM tpch.lineitem WHERE l_quantity > 30 HAVING COUNT_BIG(*) > 0"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL aggregate FILTER clauses -> T-SQL conditional aggregates
// ---------------------------------------------------------------------------

#[test]
fn postgres_aggregate_filters_map_to_tsql_conditional_aggregates() {
    let cases = [
        (
            "SELECT count(*) FILTER (WHERE x > 5) AS c FROM t",
            "SELECT COUNT_BIG(CASE WHEN x > 5 THEN 1 END) AS c FROM t",
        ),
        (
            "SELECT count(value) FILTER (WHERE x > 5) AS c FROM t",
            "SELECT COUNT_BIG(CASE WHEN x > 5 THEN value END) AS c FROM t",
        ),
        (
            "SELECT count(DISTINCT value) FILTER (WHERE x > 5) AS c FROM t",
            "SELECT COUNT_BIG(DISTINCT CASE WHEN x > 5 THEN value END) AS c FROM t",
        ),
        (
            "SELECT sum(v) FILTER (WHERE x > 5) AS s FROM t",
            "SELECT SUM(CASE WHEN x > 5 THEN v END) AS s FROM t",
        ),
        (
            "SELECT avg(v) FILTER (WHERE x > 5) AS a FROM t",
            "SELECT AVG(CASE WHEN x > 5 THEN v END) AS a FROM t",
        ),
        (
            "SELECT count(*) FILTER (WHERE flag = 'R') OVER (PARTITION BY g) AS c FROM t",
            "SELECT COUNT_BIG(CASE WHEN flag = 'R' THEN 1 END) OVER (PARTITION BY g) AS c FROM t",
        ),
        (
            "SELECT sum(v) FILTER (WHERE x > 5) OVER (PARTITION BY g) AS s FROM t",
            "SELECT SUM(CASE WHEN x > 5 THEN v END) OVER (PARTITION BY g) AS s FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL STRING_AGG inline ORDER BY -> T-SQL WITHIN GROUP
// ---------------------------------------------------------------------------

#[test]
fn postgres_string_agg_order_by_maps_to_tsql_within_group() {
    let cases = [
        (
            "SELECT string_agg(name, ', ' ORDER BY name) AS s FROM t",
            "SELECT STRING_AGG(name, ', ') WITHIN GROUP (ORDER BY CASE WHEN name IS NULL THEN 1 ELSE 0 END, name) AS s FROM t",
        ),
        (
            "SELECT string_agg(name, ', ' ORDER BY name DESC) AS s FROM t",
            "SELECT STRING_AGG(name, ', ') WITHIN GROUP (ORDER BY CASE WHEN name IS NULL THEN 1 ELSE 0 END DESC, name DESC) AS s FROM t",
        ),
        (
            "SELECT string_agg(name, ', ') AS s FROM t",
            "SELECT STRING_AGG(name, ', ') AS s FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// T-SQL TRY/CATCH structured traversal
// ---------------------------------------------------------------------------

#[test]
fn try_catch_parses_structured_bodies_and_generates_sql() {
    let ast = Parser::parse_sql(TRY_CATCH_SQL).expect("TRY/CATCH should parse");
    assert_eq!(ast.len(), 1);

    let Expression::TryCatch(try_catch) = &ast[0] else {
        panic!("expected TRY/CATCH expression, got {:?}", ast[0]);
    };

    assert_eq!(try_catch.try_body.len(), 2);
    assert_eq!(try_catch.catch_body.as_ref().map(Vec::len), Some(1));

    let sql = Generator::sql(&ast[0]).expect("TRY/CATCH should generate");
    assert_eq!(
        sql,
        "BEGIN TRY INSERT INTO orders (id, amount) VALUES (1, 100.00); UPDATE inventory SET qty = qty - 1 WHERE product_id = 42; END TRY BEGIN CATCH INSERT INTO error_log (msg) VALUES (ERROR_MESSAGE()); END CATCH"
    );
}

#[test]
fn try_catch_children_include_inner_statements() {
    let ast = Parser::parse_sql(TRY_CATCH_SQL).expect("TRY/CATCH should parse");
    let children = ast[0].children();

    assert_eq!(children.len(), 3);
    assert!(matches!(children[0], Expression::Insert(_)));
    assert!(matches!(children[1], Expression::Update(_)));
    assert!(matches!(children[2], Expression::Insert(_)));
}

#[test]
fn try_catch_get_all_tables_finds_try_and_catch_tables() {
    let ast = Parser::parse_sql(TRY_CATCH_SQL).expect("TRY/CATCH should parse");
    let names: Vec<String> = get_all_tables(&ast[0])
        .into_iter()
        .filter_map(|table| match table {
            Expression::Table(table) => Some(table.name.name),
            _ => None,
        })
        .collect();

    assert_eq!(names, vec!["orders", "inventory", "error_log"]);
}

// ---------------------------------------------------------------------------
// DECLARE statement boundaries
// ---------------------------------------------------------------------------

#[test]
fn declare_table_variable_keeps_following_insert_as_second_statement() {
    let sql = "DECLARE @tmp TABLE (id INT, name VARCHAR(50)); \
               INSERT INTO @tmp SELECT id, name FROM employees;";
    let ast = parse(sql, DialectType::TSQL).expect("DECLARE TABLE batch should parse");

    assert_eq!(ast.len(), 2);
    assert!(matches!(ast[0], Expression::Declare(_)));
    assert!(matches!(ast[1], Expression::Insert(_)));
    assert_eq!(
        generate_tsql(&ast[0]),
        "DECLARE @tmp TABLE (id INTEGER, name VARCHAR(50))"
    );
    assert_eq!(
        generate_tsql(&ast[1]),
        "INSERT INTO @tmp SELECT id, name FROM employees"
    );

    let names: Vec<String> = get_all_tables(&ast[1])
        .into_iter()
        .filter_map(|table| match table {
            Expression::Table(table) => Some(table.name.name),
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["@tmp", "employees"]);
}

#[test]
fn declare_scalar_keeps_following_select_as_second_statement() {
    let ast = parse("DECLARE @x INT; SELECT @x;", DialectType::TSQL)
        .expect("DECLARE scalar batch should parse");

    assert_eq!(ast.len(), 2);
    assert!(matches!(ast[0], Expression::Declare(_)));
    assert!(matches!(ast[1], Expression::Select(_)));
    assert_eq!(generate_tsql(&ast[0]), "DECLARE @x INTEGER");
    assert_eq!(generate_tsql(&ast[1]), "SELECT @x");
}

// ---------------------------------------------------------------------------
// PostgreSQL function-style type casts -> T-SQL CAST
// ---------------------------------------------------------------------------

#[test]
fn postgres_type_function_casts_map_to_tsql_casts() {
    let cases = [
        (
            "SELECT s, sum(v) + numeric(sum(g)) AS mixed FROM t GROUP BY s",
            "SELECT s, SUM(v) + CAST(SUM(g) AS NUMERIC) AS mixed FROM t GROUP BY s",
        ),
        (
            "SELECT decimal(5) AS d, numeric(5) AS n",
            "SELECT CAST(5 AS NUMERIC) AS d, CAST(5 AS NUMERIC) AS n",
        ),
        (
            "SELECT int4(1.5) AS a, float8(4) AS b",
            "SELECT CAST(1.5 AS INTEGER) AS a, CAST(4 AS FLOAT) AS b",
        ),
        (
            "SELECT int8(1) AS a, float4(4) AS b, bool(1) AS c, text(1) AS d",
            "SELECT CAST(1 AS BIGINT) AS a, CAST(4 AS REAL) AS b, CAST(1 AS BIT) AS c, CAST(1 AS VARCHAR(MAX)) AS d",
        ),
        (
            "SELECT smallint(1) AS s, integer(1) AS i, varchar(1) AS v, uuid(id) AS u FROM t",
            "SELECT CAST(1 AS SMALLINT) AS s, CAST(1 AS INTEGER) AS i, CAST(1 AS VARCHAR) AS v, CAST(id AS UNIQUEIDENTIFIER) AS u FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_type_function_cast_with_unsafe_arity_fails_tsql_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT numeric(1, 2) AS n",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject residual PostgreSQL type-name function casts");

    assert!(
        err.to_string()
            .contains("PostgreSQL type-name function casts"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_qualified_or_quoted_type_named_functions_are_not_casts_for_tsql() {
    let out = pg_to_tsql(r#"SELECT public.numeric(1) AS q, "numeric"(1) AS quoted"#);
    assert_eq!(out, "SELECT public.numeric(1) AS q, NUMERIC(1) AS quoted");
}

// ---------------------------------------------------------------------------
// BPCHAR → CHAR normalisation
// ---------------------------------------------------------------------------

#[test]
fn bpchar_cast_no_length_maps_to_char() {
    let out = pg_to_tsql("SELECT CAST(x AS BPCHAR)");
    assert_eq!(out, "SELECT CAST(x AS CHAR)");
}

#[test]
fn bpchar_cast_with_length_maps_to_char() {
    let out = pg_to_tsql("SELECT CAST(x AS BPCHAR(3))");
    assert_eq!(out, "SELECT CAST(x AS CHAR(3))");
}

#[test]
fn bpchar_double_colon_no_length_maps_to_char() {
    let out = pg_to_tsql("SELECT x::bpchar");
    assert_eq!(out, "SELECT CAST(x AS CHAR)");
}

#[test]
fn bpchar_double_colon_with_length_maps_to_char() {
    let out = pg_to_tsql("SELECT x::bpchar(3)");
    assert_eq!(out, "SELECT CAST(x AS CHAR(3))");
}

#[test]
fn bpchar_ddl_column_no_length_maps_to_char() {
    let out = pg_to_tsql("CREATE TABLE t (x BPCHAR)");
    assert_eq!(out, "CREATE TABLE t (x CHAR)");
}

#[test]
fn bpchar_ddl_column_with_length_maps_to_char() {
    let out = pg_to_tsql("CREATE TABLE t (x BPCHAR(3))");
    assert_eq!(out, "CREATE TABLE t (x CHAR(3))");
}

// ---------------------------------------------------------------------------
// = ANY(ARRAY[...]) / = ANY((...)) → IN
// ---------------------------------------------------------------------------

#[test]
fn any_eq_array_brackets_rewrites_to_in() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY(ARRAY['a', 'b', 'c'])");
    assert_eq!(out, "SELECT * FROM t WHERE col IN ('a', 'b', 'c')");
}

#[test]
fn any_eq_tuple_rewrites_to_in() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY(('a', 'b', 'c'))");
    assert_eq!(out, "SELECT * FROM t WHERE col IN ('a', 'b', 'c')");
}

#[test]
fn any_eq_empty_array_rewrites_to_always_false() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY(ARRAY[])");
    assert_eq!(out, "SELECT * FROM t WHERE 1 = 0");
}

#[test]
fn any_eq_outer_array_cast_rewrites_to_in_with_element_casts() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY((ARRAY['a', 'b'])::text[])");
    assert_eq!(
        out,
        "SELECT * FROM t WHERE col IN (CAST('a' AS VARCHAR(MAX)), CAST('b' AS VARCHAR(MAX)))"
    );
}

#[test]
fn any_eq_outer_array_cast_empty_rewrites_to_always_false() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY((ARRAY[])::text[])");
    assert_eq!(out, "SELECT * FROM t WHERE 1 = 0");
}

#[test]
fn any_eq_ruleutils_outer_array_cast_rewrites_to_in() {
    let out = pg_to_tsql(
        "SELECT s FROM t WHERE ((s)::text = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[]))",
    );
    assert_eq!(
        out,
        "SELECT s FROM t WHERE (CAST((s) AS VARCHAR(MAX)) IN (CAST(CAST('a' AS VARCHAR) AS VARCHAR(MAX)), CAST(CAST('b' AS VARCHAR) AS VARCHAR(MAX))))"
    );
}

#[test]
fn any_neq_array_not_rewritten() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])");
    assert_eq!(out, "SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])");
}

#[test]
fn strict_any_neq_array_rejects_non_subquery_rhs() {
    let pg = Dialect::get(DialectType::PostgreSQL);
    let err = pg
        .transpile_with(
            "SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict T-SQL transpilation should reject ANY over array literals");

    assert!(err
        .to_string()
        .contains("ANY over non-subquery expressions"));
}

#[test]
fn any_eq_subquery_not_rewritten() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY(SELECT id FROM s)");
    assert_eq!(out, "SELECT * FROM t WHERE col = ANY (SELECT id FROM s)");
}
