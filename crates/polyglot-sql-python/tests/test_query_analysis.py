import pytest

import polyglot_sql


def test_analyze_query_returns_projection_facts():
    result = polyglot_sql.analyze_query("SELECT a FROM t")

    assert result["shape"] == "select"
    assert result["projections"][0]["name"] == "a"
    assert result["projections"][0]["transformKind"] == "direct"
    assert result["projections"][0]["upstream"][0]["column"] == "a"


def test_analyze_query_accepts_schema_options():
    schema = {
        "tables": [
            {
                "name": "orders",
                "columns": [
                    {"name": "total", "type": "INT"},
                    {"name": "user_id", "type": "INT"},
                ],
            }
        ]
    }
    result = polyglot_sql.analyze_query(
        "SELECT CAST(total AS TEXT) AS total_text FROM orders",
        {"schema": schema, "dialect": "generic"},
    )

    assert result["relations"][0]["name"] == "orders"
    assert "total" in result["relations"][0]["columns"]
    assert result["projections"][0]["transformKind"] == "cast"
    assert result["projections"][0]["castType"] == "TEXT"


def test_analyze_query_tolerates_partial_schema():
    schema = {
        "tables": [
            {
                "name": "t",
                "columns": [{"name": "amount", "type": "INT"}],
            }
        ]
    }
    result = polyglot_sql.analyze_query(
        "SELECT order_id, amount FROM t",
        {"schema": schema, "dialect": "duckdb"},
    )

    assert [projection["name"] for projection in result["projections"]] == [
        "order_id",
        "amount",
    ]
    assert any(
        reference["column"] == "order_id"
        and reference["table"] == "t"
        and reference["confidence"] == "resolved"
        for reference in result["projections"][0]["upstream"]
    )
    assert any(
        reference["column"] == "amount" and reference["table"] == "t"
        for reference in result["projections"][1]["upstream"]
    )


def test_analyze_query_reports_transform_function_arguments():
    schema = {
        "tables": [
            {
                "name": "events",
                "columns": [{"name": "created_at", "type": "TIMESTAMP"}],
            }
        ]
    }
    result = polyglot_sql.analyze_query(
        "SELECT DATE_TRUNC('month', created_at) AS bucket FROM events",
        {"schema": schema, "dialect": "duckdb"},
    )

    transform_function = result["projections"][0]["transformFunction"]
    assert transform_function["name"] == "DATE_TRUNC"
    assert transform_function["literalArgs"] == ["month"]
    assert transform_function["columnArgs"][0]["table"] == "events"
    assert transform_function["columnArgs"][0]["column"] == "created_at"


def test_analyze_query_reports_base_tables_aliases_aggregates_and_precise_types():
    schema = {
        "tables": [
            {
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "INT", "nullable": False},
                    {"name": "amount", "type": "DECIMAL(10,2)", "nullable": True},
                ],
            }
        ]
    }

    result = polyglot_sql.analyze_query(
        "SELECT o.id, SUM(o.amount) AS amount_sum FROM orders AS o GROUP BY o.id",
        {"schema": schema, "dialect": "generic"},
    )

    assert result["baseTables"][0]["name"] == "orders"
    assert result["baseTables"][0]["alias"] == "o"
    assert result["projections"][0]["upstream"][0]["table"] == "orders"
    assert result["projections"][0]["upstream"][0]["sourceAlias"] == "o"
    assert result["projections"][1]["transformKind"] == "aggregation"
    assert result["projections"][1]["typeHint"] == "DECIMAL(10, 2)"
    assert result["projections"][0]["nullability"] == "non_null"


def test_analyze_query_reports_structured_table_identity():
    result = polyglot_sql.analyze_query(
        'SELECT id FROM "my.catalog"."my.schema"."orders.table" AS o',
        dialect="duckdb",
    )

    base_table = result["baseTables"][0]
    assert base_table["name"] == "my.catalog.my.schema.orders.table"
    assert base_table["catalog"] == "my.catalog"
    assert base_table["schema"] == "my.schema"
    assert base_table["table"] == "orders.table"
    assert base_table["alias"] == "o"


def test_analyze_query_reports_cte_facts_and_star_projections():
    schema = {
        "tables": [
            {
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "INT", "nullable": False},
                    {"name": "amount", "type": "DECIMAL(10,2)", "nullable": True},
                ],
            }
        ]
    }

    result = polyglot_sql.analyze_query(
        "WITH base AS (SELECT id, amount FROM orders) SELECT * FROM base",
        {"schema": schema, "dialect": "generic"},
    )

    assert result["cteFacts"][0]["name"] == "base"
    assert result["cteFacts"][0]["bodySql"] == "SELECT id, amount FROM orders"
    assert result["cteFacts"][0]["outputColumns"] == ["id", "amount"]
    assert result["starProjections"][0]["index"] == 0
    assert result["starProjections"][0]["expandedColumns"] == ["id", "amount"]


def test_analyze_query_resolves_pivot_alias_columns():
    result = polyglot_sql.analyze_query(
        "SELECT region2, p1 FROM (SELECT region, q, amt FROM sales) "
        "PIVOT(SUM(amt) FOR q IN ('Q1')) AS p(region2, p1)",
        {"dialect": "duckdb"},
    )

    region = next(
        projection
        for projection in result["projections"]
        if projection["name"] == "region2"
    )
    assert any(
        reference["table"] == "sales" and reference["column"] == "region"
        for reference in region["upstream"]
    )

    pivot_value = next(
        projection for projection in result["projections"] if projection["name"] == "p1"
    )
    assert any(
        reference["table"] == "sales" and reference["column"] == "amt"
        for reference in pivot_value["upstream"]
    )


def test_analyze_query_resolves_nested_set_operation_with_schema():
    result = polyglot_sql.analyze_query(
        "SELECT v FROM ((SELECT v FROM t1 UNION ALL SELECT v FROM t2) "
        "UNION ALL SELECT v FROM t3) u",
        {
            "dialect": "duckdb",
            "schema": {
                "tables": [
                    {"name": "t1", "columns": [{"name": "v", "type": "INT"}]},
                    {"name": "t2", "columns": [{"name": "v", "type": "INT"}]},
                    {"name": "t3", "columns": [{"name": "v", "type": "INT"}]},
                ]
            },
        },
    )

    upstream = result["projections"][0]["upstream"]
    assert {reference["table"] for reference in upstream} == {"t1", "t2", "t3"}


def test_analyze_query_resolves_unnest_output_alias_with_schema():
    result = polyglot_sql.analyze_query(
        "SELECT i FROM t, UNNEST(t.arr) AS i",
        {
            "dialect": "duckdb",
            "schema": {
                "tables": [
                    {"name": "t", "columns": [{"name": "arr", "type": "INT"}]},
                ]
            },
        },
    )

    upstream = result["projections"][0]["upstream"]
    assert any(
        reference["table"] == "t" and reference["column"] == "arr"
        for reference in upstream
    )


def test_analyze_query_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.analyze_query("SELECT 1", dialect="not_a_dialect")


def test_analyze_query_rejects_invalid_options():
    with pytest.raises(ValueError):
        polyglot_sql.analyze_query("SELECT 1", "not an options object")
