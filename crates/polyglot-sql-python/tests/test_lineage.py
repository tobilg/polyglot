import pytest

import polyglot_sql


def test_lineage_returns_dict():
    sql = "SELECT o.total FROM orders o JOIN users u ON o.user_id = u.id"
    result = polyglot_sql.lineage("total", sql, dialect="postgres")
    assert isinstance(result, dict)
    assert "name" in result


def test_source_tables_returns_orders():
    sql = "SELECT o.total FROM orders o JOIN users u ON o.user_id = u.id"
    tables = polyglot_sql.source_tables("total", sql, dialect="postgres")
    assert isinstance(tables, list)
    assert "orders" in tables


def test_source_tables_nonexistent_column_is_graceful():
    sql = "SELECT o.total FROM orders o"
    try:
        result = polyglot_sql.source_tables("does_not_exist", sql, dialect="postgres")
        assert isinstance(result, list)
    except polyglot_sql.PolyglotError:
        # Accept explicit error behavior if lineage resolution rejects missing columns.
        pass


def test_lineage_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.lineage("a", "SELECT a FROM t", dialect="not_a_dialect")


def test_lineage_with_schema_resolves_ambiguous_column():
    schema = {
        "tables": [
            {
                "name": "users",
                "columns": [
                    {"name": "id", "type": "INT"},
                    {"name": "name", "type": "TEXT"},
                ],
            },
            {
                "name": "orders",
                "columns": [
                    {"name": "order_id", "type": "INT"},
                    {"name": "user_id", "type": "INT"},
                ],
            },
        ]
    }
    sql = "SELECT id FROM users u JOIN orders o ON u.id = o.user_id"
    result = polyglot_sql.lineage_with_schema("id", sql, schema, dialect="generic")

    def collect_names(node: dict) -> list[str]:
        names = [node.get("name", "")]
        for child in node.get("downstream", []):
            names.extend(collect_names(child))
        return names

    names = collect_names(result)
    assert any(name == "u.id" for name in names), f"expected u.id in lineage tree, got: {names}"
