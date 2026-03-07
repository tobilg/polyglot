import pytest

import polyglot_sql


def test_parse_one_expr_returns_expression_object():
    expr = polyglot_sql.parse_one_expr("SELECT a FROM t WHERE b = 1", dialect="postgres")

    assert isinstance(expr, polyglot_sql.Expression)
    assert expr.kind == "select"
    assert expr.tree_depth > 0


def test_expression_to_dict_and_sql_roundtrip():
    expr = polyglot_sql.parse_one_expr("SELECT a FROM t", dialect="postgres")

    assert expr.to_dict()["select"]["expressions"][0]["column"]["name"]["name"] == "a"
    assert expr.sql("postgres") == "SELECT a FROM t"


def test_expression_arg_returns_wrapped_children_when_possible():
    expr = polyglot_sql.parse_one_expr("SELECT a FROM t", dialect="postgres")

    from_clause = expr.arg("from")
    assert isinstance(from_clause, dict)
    assert from_clause["expressions"][0]["table"]["name"]["name"] == "t"

    expressions = expr.arg("expressions")
    assert isinstance(expressions, list)
    assert isinstance(expressions[0], polyglot_sql.Expression)


def test_expression_children_walk_and_find():
    expr = polyglot_sql.parse_one_expr("SELECT a, b FROM t WHERE c = 1", dialect="postgres")

    children = expr.children()
    assert children
    assert all(isinstance(child, polyglot_sql.Expression) for child in children)

    dfs_nodes = expr.walk()
    bfs_nodes = expr.walk("bfs")
    assert dfs_nodes[0].kind == "select"
    assert bfs_nodes[0].kind == "select"
    assert len(dfs_nodes) == len(bfs_nodes)

    first_column = expr.find("column")
    assert isinstance(first_column, polyglot_sql.Expression)
    assert first_column.kind == "column"

    columns = expr.find_all("column")
    assert len(columns) == 3
    assert all(node.kind == "column" for node in columns)


def test_expression_walk_rejects_unknown_order():
    expr = polyglot_sql.parse_one_expr("SELECT 1", dialect="postgres")

    with pytest.raises(ValueError):
        expr.walk("zigzag")


def test_generate_accepts_expression_objects():
    expr = polyglot_sql.parse_one_expr("SELECT a FROM t", dialect="postgres")

    assert polyglot_sql.generate(expr, dialect="postgres") == ["SELECT a FROM t"]
    assert polyglot_sql.generate([expr], dialect="postgres") == ["SELECT a FROM t"]


def test_parse_expr_returns_expression_list():
    expressions = polyglot_sql.parse_expr("SELECT 1; SELECT 2", dialect="postgres")

    assert len(expressions) == 2
    assert all(isinstance(expr, polyglot_sql.Expression) for expr in expressions)
