import gc

import pytest
import polyglot_sql as sql


@pytest.mark.parametrize("order", ["dfs", "bfs"])
def test_walk_views_survive_root_collection_and_keep_types(order):
    root = sql.parse_one("SELECT (x + 1) AS a, y FROM t")
    nodes = root.walk(order)
    expected = [(type(node), node.to_dict()) for node in nodes]
    del root
    gc.collect()
    assert [(type(node), node.to_dict()) for node in nodes] == expected
    assert all(node.parent is None for node in nodes)
    addition = next(node for node in nodes if isinstance(node, sql.Add))
    assert addition.sql() == "x + 1"
    assert addition.this.parent is addition


def test_nested_views_and_builders_preserve_original_tree():
    root = sql.parse_one("SELECT x + 1 AS a FROM t")
    alias = root.expressions[0]
    addition = alias.this
    column = addition.this
    assert alias.parent is root
    assert addition.parent is alias
    assert column.root() is root
    assert column.depth == 3
    changed = addition + 5
    assert changed.sql() == "x + 1 + 5"
    assert addition.sql() == "x + 1"
    assert root.sql() == "SELECT x + 1 AS a FROM t"
    assert root.find(sql.Add).parent is None
    assert root.find_all(sql.Add)[0] == addition


def test_find_view_does_not_require_original_python_wrapper():
    child = sql.parse_one("SELECT (x + 1) AS a FROM t").find(sql.Add)
    gc.collect()
    assert child.this.name == "x"
    assert child.to_dict()["add"]["left"]["column"]["name"]["name"] == "x"
