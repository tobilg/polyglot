//! Regression tests for PostgreSQL → Fabric transpilation.

use polyglot_sql::{transpile, Dialect, DialectType, TranspileOptions};

fn pg_to_fabric(sql: &str) -> String {
    transpile(sql, DialectType::PostgreSQL, DialectType::Fabric)
        .unwrap_or_else(|e| panic!("transpile failed for {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("expected at least one statement")
}

fn tsql_to_fabric(sql: &str) -> String {
    transpile(sql, DialectType::TSQL, DialectType::Fabric)
        .unwrap_or_else(|e| panic!("transpile failed for {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("expected at least one statement")
}

#[test]
fn postgres_positional_parameters_render_as_tsql_style_placeholders_for_fabric() {
    assert_eq!(pg_to_fabric("SELECT $1, $2"), "SELECT @P1, @P2");
}

// ---------------------------------------------------------------------------
// PostgreSQL LATERAL joins -> Fabric APPLY
// ---------------------------------------------------------------------------

#[test]
fn postgres_lateral_joins_map_to_fabric_apply() {
    let lateral_subquery = "(SELECT v FROM lineitem WHERE l_orderkey = o.id)";
    let cross_apply =
        "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT v FROM lineitem WHERE l_orderkey = o.id) AS t";
    let outer_apply =
        "SELECT o.id, t.v FROM orders AS o OUTER APPLY (SELECT v FROM lineitem WHERE l_orderkey = o.id) AS t";

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
        assert_eq!(pg_to_fabric(&sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_parenthesized_lateral_joins_map_to_fabric_apply() {
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
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_comma_lateral_maps_to_fabric_cross_apply() {
    assert_eq!(
        pg_to_fabric("SELECT o.id, t.v FROM orders o, LATERAL (SELECT v FROM lineitem WHERE l_orderkey = o.id) t"),
        "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT v FROM lineitem WHERE l_orderkey = o.id) AS t"
    );
}

#[test]
fn postgres_lateral_join_on_predicate_pushes_into_fabric_apply_rhs() {
    let cases = [
        (
            "SELECT o.id, t.v FROM orders o JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t ON t.v > 0",
            "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT * FROM (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source WHERE _polyglot_lateral_source.v > 0) AS t",
        ),
        (
            "SELECT o.id, t.v FROM orders o LEFT JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t ON t.v > 0",
            "SELECT o.id, t.v FROM orders AS o OUTER APPLY (SELECT * FROM (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source WHERE _polyglot_lateral_source.v > 0) AS t",
        ),
        (
            "SELECT o.id, t.x FROM orders o JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t(x) ON t.x > 0",
            "SELECT o.id, t.x FROM orders AS o CROSS APPLY (SELECT * FROM (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source(x) WHERE _polyglot_lateral_source.x > 0) AS t(x)",
        ),
        (
            "SELECT o.id, t.v FROM orders o JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t ON o.id > 0",
            "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT * FROM (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source WHERE o.id > 0) AS t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_lateral_using_inside_joined_table_fails_fabric_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT * FROM (orders o JOIN LATERAL (SELECT 1 AS id) a USING (id))",
            DialectType::Fabric,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject residual LATERAL");

    assert!(
        err.to_string().contains("LATERAL joins and subqueries"),
        "unexpected error: {err}"
    );
}

#[test]
fn fabric_preserves_native_apply_generation() {
    let cases = [
        (
            "SELECT x.a, x.b, t.v FROM x CROSS APPLY (SELECT v FROM t) t",
            "SELECT x.a, x.b, t.v FROM x CROSS APPLY (SELECT v AS v FROM t) AS t",
        ),
        (
            "SELECT x.a, x.b, t.v FROM x OUTER APPLY (SELECT v FROM t) t",
            "SELECT x.a, x.b, t.v FROM x OUTER APPLY (SELECT v AS v FROM t) AS t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(tsql_to_fabric(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_framed_named_window_inlines_frame_stripped_copy_for_fabric_ranking_function() {
    let out = pg_to_fabric(
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
fn postgres_inline_window_frame_is_stripped_for_fabric_ranking_function() {
    let out = pg_to_fabric(
        "SELECT row_number() OVER (PARTITION BY o_custkey ORDER BY o_orderdate NULLS FIRST \
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn \
         FROM orders",
    );

    assert_eq!(
        out,
        "SELECT ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS rn FROM orders"
    );
}

#[test]
fn postgres_framed_named_window_inlines_for_all_fabric_frame_incompatible_functions() {
    let out = pg_to_fabric(
        "SELECT rank() OVER w AS r, dense_rank() OVER w AS dr, ntile(4) OVER w AS nt, \
                lead(o_totalprice) OVER w AS lead_price, lag(o_totalprice) OVER w AS lag_price, \
                percent_rank() OVER w AS pr, cume_dist() OVER w AS cd, \
                avg(o_totalprice) OVER w AS av \
         FROM orders \
         WINDOW w AS (PARTITION BY o_custkey ORDER BY o_orderdate NULLS FIRST \
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    );

    assert_eq!(
        out,
        "SELECT RANK() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS r, DENSE_RANK() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS dr, NTILE(4) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS nt, LEAD(o_totalprice) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS lead_price, LAG(o_totalprice) OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS lag_price, PERCENT_RANK() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS pr, CUME_DIST() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS cd, AVG(o_totalprice) OVER w AS av FROM orders WINDOW w AS (PARTITION BY o_custkey ORDER BY o_orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL NULLS FIRST/LAST -> Fabric CASE sort key
// ---------------------------------------------------------------------------

#[test]
fn postgres_null_ordering_rewrites_for_fabric() {
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
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_random_ordering_does_not_add_null_sort_key_for_fabric() {
    let out = pg_to_fabric(r#"SELECT * FROM "test_table" ORDER BY RANDOM() LIMIT 5"#);
    assert_eq!(out, "SELECT TOP 5 * FROM [test_table] ORDER BY RAND()");
}

#[test]
fn postgres_set_operation_modifiers_wrap_for_fabric() {
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
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL WITH RECURSIVE -> Fabric WITH
// ---------------------------------------------------------------------------

#[test]
fn postgres_recursive_cte_omits_recursive_keyword_for_fabric() {
    let out = pg_to_fabric(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n FROM r",
    );
    assert_eq!(
        out,
        "WITH r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n FROM r"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL MOD function -> Fabric modulo operator
// ---------------------------------------------------------------------------

#[test]
fn postgres_mod_function_maps_to_fabric_percent_operator() {
    let cases = [
        ("SELECT mod(a, 7) AS m FROM t", "SELECT a % 7 AS m FROM t"),
        (
            "SELECT mod(a + 1, b * 2) AS m FROM t",
            "SELECT (a + 1) % (b * 2) AS m FROM t",
        ),
        ("SELECT a % 7 AS m FROM t", "SELECT a % 7 AS m FROM t"),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL string position functions -> Fabric CHARINDEX
// ---------------------------------------------------------------------------

#[test]
fn postgres_string_position_maps_to_fabric_charindex() {
    let cases = [
        (
            "SELECT position('green' IN c) AS p FROM t",
            "SELECT CHARINDEX('green', c) AS p FROM t",
        ),
        (
            "SELECT position(x IN y) AS p FROM t",
            "SELECT CHARINDEX(x, y) AS p FROM t",
        ),
        (
            "SELECT strpos(c, 'green') AS p FROM t",
            "SELECT CHARINDEX('green', c) AS p FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL row-value IN subquery -> Fabric correlated EXISTS
// ---------------------------------------------------------------------------

#[test]
fn postgres_row_value_in_subquery_maps_to_fabric_exists() {
    let out = pg_to_fabric("SELECT a FROM t WHERE (a, b) IN (SELECT x, y FROM u WHERE q < 10)");
    assert_eq!(
        out,
        "SELECT a FROM t WHERE EXISTS(SELECT 1 FROM u WHERE u.x = t.a AND u.y = t.b AND q < 10)"
    );
}

#[test]
fn postgres_row_value_not_in_subquery_is_not_rewritten_for_fabric() {
    let out = pg_to_fabric("SELECT a FROM t WHERE (a, b) NOT IN (SELECT x, y FROM u WHERE q < 10)");
    assert_eq!(
        out,
        "SELECT a FROM t WHERE NOT (a, b) IN (SELECT x, y FROM u WHERE q < 10)"
    );
}

#[test]
fn postgres_row_value_in_subquery_arity_mismatch_is_not_rewritten_for_fabric() {
    let out = pg_to_fabric("SELECT a FROM t WHERE (a, b) IN (SELECT x FROM u)");
    assert_eq!(out, "SELECT a FROM t WHERE (a, b) IN (SELECT x FROM u)");
}

// ---------------------------------------------------------------------------
// PostgreSQL unqualified NUMERIC/DECIMAL casts -> Fabric scaled DECIMAL
// ---------------------------------------------------------------------------

#[test]
fn postgres_unqualified_numeric_cast_uses_scaled_fabric_decimal() {
    let out = pg_to_fabric("SELECT round(avg(p)::numeric, 2) AS r FROM t GROUP BY g");
    assert_eq!(
        out,
        "SELECT ROUND(CAST(AVG(p) AS DECIMAL(38, 10)), 2) AS r FROM t GROUP BY g"
    );
}

#[test]
fn postgres_plain_numeric_and_decimal_casts_use_scaled_fabric_decimal() {
    let cases = [
        (
            "SELECT p::numeric AS n FROM t",
            "SELECT CAST(p AS DECIMAL(38, 10)) AS n FROM t",
        ),
        (
            "SELECT p::decimal AS n FROM t",
            "SELECT CAST(p AS DECIMAL(38, 10)) AS n FROM t",
        ),
        (
            "SELECT CAST(p AS numeric) AS n FROM t",
            "SELECT CAST(p AS DECIMAL(38, 10)) AS n FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_explicit_numeric_cast_preserves_fabric_precision_and_scale() {
    let out = pg_to_fabric("SELECT p::numeric(38, 10) AS n FROM t");
    assert_eq!(out, "SELECT CAST(p AS DECIMAL(38, 10)) AS n FROM t");
}

#[test]
fn postgres_type_function_casts_map_to_fabric_casts() {
    let cases = [
        (
            "SELECT s, sum(v) + numeric(sum(g)) AS mixed FROM t GROUP BY s",
            "SELECT s, SUM(v) + CAST(SUM(g) AS DECIMAL(38, 10)) AS mixed FROM t GROUP BY s",
        ),
        (
            "SELECT decimal(5) AS d, numeric(5) AS n",
            "SELECT CAST(5 AS DECIMAL(38, 10)) AS d, CAST(5 AS DECIMAL(38, 10)) AS n",
        ),
        (
            "SELECT int4(1.5) AS a, float8(4) AS b",
            "SELECT CAST(1.5 AS INT) AS a, CAST(4 AS FLOAT) AS b",
        ),
        (
            "SELECT int8(1) AS a, float4(4) AS b, bool(1) AS c, text(1) AS d",
            "SELECT CAST(1 AS BIGINT) AS a, CAST(4 AS REAL) AS b, CAST(1 AS BIT) AS c, CAST(1 AS VARCHAR(MAX)) AS d",
        ),
        (
            "SELECT smallint(1) AS s, integer(1) AS i, varchar(1) AS v, uuid(id) AS u FROM t",
            "SELECT CAST(1 AS SMALLINT) AS s, CAST(1 AS INT) AS i, CAST(1 AS VARCHAR) AS v, CAST(id AS UNIQUEIDENTIFIER) AS u FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_type_function_cast_with_unsafe_arity_fails_fabric_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT numeric(1, 2) AS n",
            DialectType::Fabric,
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
fn postgres_qualified_or_quoted_type_named_functions_are_not_casts_for_fabric() {
    let out = pg_to_fabric(r#"SELECT public.numeric(1) AS q, "numeric"(1) AS quoted"#);
    assert_eq!(out, "SELECT public.numeric(1) AS q, NUMERIC(1) AS quoted");
}

// ---------------------------------------------------------------------------
// PostgreSQL statistical aggregates -> Fabric T-SQL names
// ---------------------------------------------------------------------------

#[test]
fn postgres_statistical_aggregates_map_to_fabric_names() {
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
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL boolean aggregates -> Fabric T-SQL CASE aggregates
// ---------------------------------------------------------------------------

#[test]
fn postgres_boolean_aggregates_map_to_fabric_case_aggregates() {
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
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_scalar_boolean_values_map_to_fabric_case_values() {
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
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_predicate_boolean_contexts_stay_predicates_for_fabric() {
    let out = pg_to_fabric(
        "SELECT COUNT(*) AS c FROM tpch.lineitem WHERE l_quantity > 30 HAVING COUNT(*) > 0",
    );
    assert_eq!(
        out,
        "SELECT COUNT_BIG(*) AS c FROM tpch.lineitem WHERE l_quantity > 30 HAVING COUNT_BIG(*) > 0"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL aggregate FILTER clauses -> Fabric conditional aggregates
// ---------------------------------------------------------------------------

#[test]
fn postgres_aggregate_filters_map_to_fabric_conditional_aggregates() {
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
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL STRING_AGG inline ORDER BY -> Fabric WITHIN GROUP
// ---------------------------------------------------------------------------

#[test]
fn postgres_string_agg_order_by_maps_to_fabric_within_group() {
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
        assert_eq!(pg_to_fabric(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL LIMIT/OFFSET -> Fabric TOP/OFFSET/FETCH
// ---------------------------------------------------------------------------

#[test]
fn limit_without_offset_uses_top() {
    let out = pg_to_fabric("SELECT id FROM t ORDER BY id LIMIT 5");
    assert_eq!(
        out,
        "SELECT TOP 5 id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END, id"
    );
}

#[test]
fn limit_with_offset_uses_offset_fetch() {
    let out = pg_to_fabric("SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 2");
    assert_eq!(
        out,
        "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END, id OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY"
    );
}

#[test]
fn offset_without_limit_keeps_rows_keyword() {
    let out = pg_to_fabric("SELECT id FROM t ORDER BY id OFFSET 2");
    assert_eq!(
        out,
        "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END, id OFFSET 2 ROWS"
    );
}

// ---------------------------------------------------------------------------
// BPCHAR → CHAR normalisation
// ---------------------------------------------------------------------------

#[test]
fn bpchar_cast_no_length_maps_to_char() {
    let out = pg_to_fabric("SELECT CAST(x AS BPCHAR)");
    assert_eq!(out, "SELECT CAST(x AS CHAR)");
}

#[test]
fn bpchar_cast_with_length_maps_to_char() {
    let out = pg_to_fabric("SELECT CAST(x AS BPCHAR(3))");
    assert_eq!(out, "SELECT CAST(x AS CHAR(3))");
}

#[test]
fn bpchar_double_colon_no_length_maps_to_char() {
    let out = pg_to_fabric("SELECT x::bpchar");
    assert_eq!(out, "SELECT CAST(x AS CHAR)");
}

#[test]
fn bpchar_double_colon_with_length_maps_to_char() {
    let out = pg_to_fabric("SELECT x::bpchar(3)");
    assert_eq!(out, "SELECT CAST(x AS CHAR(3))");
}

#[test]
fn bpchar_ddl_column_no_length_maps_to_char() {
    let out = pg_to_fabric("CREATE TABLE t (x BPCHAR)");
    assert_eq!(out, "CREATE TABLE t (x CHAR)");
}

#[test]
fn bpchar_ddl_column_with_length_maps_to_char() {
    let out = pg_to_fabric("CREATE TABLE t (x BPCHAR(3))");
    assert_eq!(out, "CREATE TABLE t (x CHAR(3))");
}

// ---------------------------------------------------------------------------
// = ANY(ARRAY[...]) / = ANY((...)) → IN
// ---------------------------------------------------------------------------

#[test]
fn any_eq_array_brackets_rewrites_to_in() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY(ARRAY['a', 'b', 'c'])");
    assert_eq!(out, "SELECT * FROM t WHERE col IN ('a', 'b', 'c')");
}

#[test]
fn any_eq_tuple_rewrites_to_in() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY(('a', 'b', 'c'))");
    assert_eq!(out, "SELECT * FROM t WHERE col IN ('a', 'b', 'c')");
}

#[test]
fn any_eq_empty_array_rewrites_to_always_false() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY(ARRAY[])");
    assert_eq!(out, "SELECT * FROM t WHERE 1 = 0");
}

#[test]
fn any_eq_outer_array_cast_rewrites_to_in_with_element_casts() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY((ARRAY['a', 'b'])::text[])");
    assert_eq!(
        out,
        "SELECT * FROM t WHERE col IN (CAST('a' AS VARCHAR(MAX)), CAST('b' AS VARCHAR(MAX)))"
    );
}

#[test]
fn any_eq_outer_array_cast_empty_rewrites_to_always_false() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY((ARRAY[])::text[])");
    assert_eq!(out, "SELECT * FROM t WHERE 1 = 0");
}

#[test]
fn any_eq_ruleutils_outer_array_cast_rewrites_to_in() {
    let out = pg_to_fabric(
        "SELECT s FROM t WHERE ((s)::text = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[]))",
    );
    assert_eq!(
        out,
        "SELECT s FROM t WHERE (CAST((s) AS VARCHAR(MAX)) IN (CAST(CAST('a' AS VARCHAR) AS VARCHAR(MAX)), CAST(CAST('b' AS VARCHAR) AS VARCHAR(MAX))))"
    );
}

#[test]
fn any_neq_array_not_rewritten() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])");
    assert_eq!(out, "SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])");
}

#[test]
fn strict_any_neq_array_rejects_non_subquery_rhs() {
    let pg = Dialect::get(DialectType::PostgreSQL);
    let err = pg
        .transpile_with(
            "SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])",
            DialectType::Fabric,
            TranspileOptions::strict(),
        )
        .expect_err("strict Fabric transpilation should reject ANY over array literals");

    assert!(err
        .to_string()
        .contains("ANY over non-subquery expressions"));
}

#[test]
fn any_eq_subquery_not_rewritten() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY(SELECT id FROM s)");
    assert_eq!(out, "SELECT * FROM t WHERE col = ANY (SELECT id FROM s)");
}

// ---------------------------------------------------------------------------
// PostgreSQL interval arithmetic -> Fabric DATEADD
// ---------------------------------------------------------------------------

#[test]
fn date_minus_interval_with_precision_rewrites_to_dateadd() {
    let out =
        pg_to_fabric("SELECT l_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY (3) FROM lineitem");
    assert_eq!(
        out,
        "SELECT CASE WHEN l_shipdate <= DATEADD(DAY, -3, CAST('1998-12-01' AS DATE)) THEN 1 WHEN NOT l_shipdate <= DATEADD(DAY, -3, CAST('1998-12-01' AS DATE)) THEN 0 END FROM lineitem"
    );
}

#[test]
fn date_minus_interval_placeholder_rewrites_to_unquoted_dateadd_amount() {
    let out = pg_to_fabric(
        "SELECT l_shipdate <= DATE '1998-12-01' - INTERVAL ':1' DAY (3) FROM lineitem",
    );
    assert_eq!(
        out,
        "SELECT CASE WHEN l_shipdate <= DATEADD(DAY, -:1, CAST('1998-12-01' AS DATE)) THEN 1 WHEN NOT l_shipdate <= DATEADD(DAY, -:1, CAST('1998-12-01' AS DATE)) THEN 0 END FROM lineitem"
    );
}

#[test]
fn date_minus_cast_interval_rewrites_to_dateadd() {
    let out = pg_to_fabric("SELECT shipdate - CAST('3 day' AS INTERVAL) FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(DAY, -3, shipdate) FROM lineitem");
}

#[test]
fn date_plus_cast_interval_rewrites_to_dateadd() {
    let out = pg_to_fabric("SELECT shipdate + CAST('3 day' AS INTERVAL) FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(DAY, 3, shipdate) FROM lineitem");
}

#[test]
fn date_plus_month_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT DATE '1993-07-01' + INTERVAL '3 months'");
    assert_eq!(out, "SELECT DATEADD(MONTH, 3, CAST('1993-07-01' AS DATE))");
}

#[test]
fn date_plus_mon_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT DATE '1993-07-01' + INTERVAL '3 mon'");
    assert_eq!(out, "SELECT DATEADD(MONTH, 3, CAST('1993-07-01' AS DATE))");
}

#[test]
fn date_plus_mons_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT DATE '1993-07-01' + INTERVAL '3 mons'");
    assert_eq!(out, "SELECT DATEADD(MONTH, 3, CAST('1993-07-01' AS DATE))");
}

#[test]
fn date_minus_mons_interval_rewrites_to_negative_month_dateadd() {
    let out = pg_to_fabric("SELECT DATE '1993-07-01' - INTERVAL '3 mons'");
    assert_eq!(out, "SELECT DATEADD(MONTH, -3, CAST('1993-07-01' AS DATE))");
}

#[test]
fn date_plus_cast_mons_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT shipdate + CAST('3 mons' AS INTERVAL) FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(MONTH, 3, shipdate) FROM lineitem");
}

#[test]
fn date_minus_double_colon_mons_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT shipdate - '3 mons'::INTERVAL FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(MONTH, -3, shipdate) FROM lineitem");
}

#[test]
fn date_minus_double_colon_interval_rewrites_to_dateadd() {
    let out = pg_to_fabric("SELECT shipdate - '3 day'::INTERVAL FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(DAY, -3, shipdate) FROM lineitem");
}
