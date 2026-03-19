import pytest

import polyglot_sql


def test_parse_one_returns_expression_object():
    expr = polyglot_sql.parse_one("SELECT a FROM t WHERE b = 1", dialect="postgres")

    assert isinstance(expr, polyglot_sql.Expression)
    assert expr.kind == "select"
    assert expr.tree_depth > 0


def test_expression_to_dict_and_sql_roundtrip():
    expr = polyglot_sql.parse_one("SELECT a FROM t", dialect="postgres")

    assert expr.to_dict()["select"]["expressions"][0]["column"]["name"]["name"] == "a"
    assert expr.sql("postgres") == "SELECT a FROM t"


def test_expression_arg_returns_wrapped_children_when_possible():
    expr = polyglot_sql.parse_one("SELECT a FROM t", dialect="postgres")

    from_clause = expr.arg("from")
    assert isinstance(from_clause, dict)
    assert from_clause["expressions"][0]["table"]["name"]["name"] == "t"

    expressions = expr.arg("expressions")
    assert isinstance(expressions, list)
    assert isinstance(expressions[0], polyglot_sql.Expression)


def test_expression_children_walk_and_find():
    expr = polyglot_sql.parse_one("SELECT a, b FROM t WHERE c = 1", dialect="postgres")

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
    assert isinstance(first_column, polyglot_sql.Column)
    assert first_column.kind == "column"

    columns = expr.find_all("column")
    assert len(columns) == 3
    assert all(node.kind == "column" for node in columns)
    assert all(isinstance(node, polyglot_sql.Column) for node in columns)


def test_children_return_typed_subclasses():
    expr = polyglot_sql.parse_one("SELECT a FROM t", dialect="postgres")
    children = expr.children()
    for child in children:
        assert type(child).__name__ != "Expression"  # Should be a specific subclass
        assert isinstance(child, polyglot_sql.Expression)


def test_walk_returns_typed_subclasses():
    expr = polyglot_sql.parse_one("SELECT a FROM t", dialect="postgres")
    nodes = expr.walk()
    assert isinstance(nodes[0], polyglot_sql.Select)
    types = [type(n).__name__ for n in nodes]
    assert "Select" in types
    assert "Column" in types or "Identifier" in types


def test_find_returns_typed_subclass():
    expr = polyglot_sql.parse_one("SELECT a FROM t", dialect="postgres")
    col = expr.find("column")
    assert isinstance(col, polyglot_sql.Column)
    assert type(col).__name__ == "Column"


def test_find_all_returns_typed_subclasses():
    expr = polyglot_sql.parse_one("SELECT a, b FROM t", dialect="postgres")
    cols = expr.find_all("column")
    assert all(isinstance(c, polyglot_sql.Column) for c in cols)
    assert all(type(c).__name__ == "Column" for c in cols)


def test_expression_walk_rejects_unknown_order():
    expr = polyglot_sql.parse_one("SELECT 1", dialect="postgres")

    with pytest.raises(ValueError):
        expr.walk("zigzag")


def test_generate_accepts_expression_objects():
    expr = polyglot_sql.parse_one("SELECT a FROM t", dialect="postgres")

    assert polyglot_sql.generate(expr, dialect="postgres") == ["SELECT a FROM t"]
    assert polyglot_sql.generate([expr], dialect="postgres") == ["SELECT a FROM t"]


def test_expression_str_returns_sql():
    expr = polyglot_sql.parse_one("SELECT a FROM t WHERE b = 1", dialect="postgres")
    assert str(expr) == "SELECT a FROM t WHERE b = 1"


def test_expression_repr_shows_ast_tree():
    expr = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    r = repr(expr)
    assert r.startswith("Select(")
    assert "Literal" in r


# === New tests for properties and traversal ===


def test_this_property():
    expr = polyglot_sql.parse_one("NOT x")
    assert isinstance(expr.this, polyglot_sql.Column)


def test_this_property_binary_op():
    expr = polyglot_sql.parse_one("SELECT a + b").find(polyglot_sql.Add)
    assert expr is not None
    assert expr.this is not None  # left operand


def test_expression_property():
    expr = polyglot_sql.parse_one("SELECT a + b").find(polyglot_sql.Add)
    assert expr is not None
    assert expr.expression is not None  # right operand


def test_expressions_property():
    expr = polyglot_sql.parse_one("SELECT a, b, c")
    assert len(expr.expressions) == 3
    assert all(isinstance(e, polyglot_sql.Column) for e in expr.expressions)


def test_name_property():
    col = polyglot_sql.parse_one("SELECT foo").find(polyglot_sql.Column)
    assert col is not None
    assert col.name == "foo"


def test_name_property_table():
    expr = polyglot_sql.parse_one("SELECT 1 FROM my_table")
    tbl = expr.find(polyglot_sql.Table)
    assert tbl is not None
    assert tbl.name == "my_table"


def test_name_property_function():
    fn = polyglot_sql.parse_one("SELECT my_func(1)").find(polyglot_sql.Function)
    assert fn is not None
    assert fn.name.upper() == "MY_FUNC"  # may or may not be normalized


def test_name_property_literal():
    lit = polyglot_sql.parse_one("SELECT 42").find(polyglot_sql.Literal)
    assert lit is not None
    assert lit.name == "42"


def test_alias_property():
    alias_expr = polyglot_sql.parse_one("SELECT a AS b").find(polyglot_sql.Alias)
    assert alias_expr is not None
    assert alias_expr.alias == "b"


def test_alias_or_name_property():
    alias_expr = polyglot_sql.parse_one("SELECT a AS b").find(polyglot_sql.Alias)
    assert alias_expr is not None
    assert alias_expr.alias_or_name == "b"

    col = polyglot_sql.parse_one("SELECT a").find(polyglot_sql.Column)
    assert col is not None
    assert col.alias_or_name == "a"  # falls back to name


def test_output_name_property():
    expr = polyglot_sql.parse_one("SELECT a AS b")
    # The alias itself has output_name == "b"
    alias_expr = expr.find(polyglot_sql.Alias)
    assert alias_expr.output_name == "b"

    col = polyglot_sql.parse_one("SELECT c").find(polyglot_sql.Column)
    assert col.output_name == "c"


def test_key_property():
    expr = polyglot_sql.parse_one("SELECT 1")
    assert expr.key == "select"
    assert expr.key == expr.kind


def test_parent_property():
    expr = polyglot_sql.parse_one("SELECT a FROM t")
    # Root has no parent
    assert expr.parent is None

    # Children accessed via .this, .expression, .expressions have parent set
    for child in expr.expressions:
        assert child.parent is not None


def test_depth_property():
    expr = polyglot_sql.parse_one("SELECT a FROM t")
    assert expr.depth == 0

    col = expr.expressions[0]
    assert col.depth > 0


def test_root():
    expr = polyglot_sql.parse_one("SELECT a FROM t")
    col = expr.expressions[0]
    root = col.root()
    assert root.kind == "select"


def test_find_with_class():
    expr = polyglot_sql.parse_one("SELECT a, b FROM t")
    col = expr.find(polyglot_sql.Column)
    assert isinstance(col, polyglot_sql.Column)
    cols = expr.find_all(polyglot_sql.Column)
    assert len(cols) == 2


def test_find_with_multiple_types():
    expr = polyglot_sql.parse_one("SELECT a, 1 FROM t")
    result = expr.find(polyglot_sql.Column, polyglot_sql.Literal)
    assert result is not None
    assert isinstance(result, (polyglot_sql.Column, polyglot_sql.Literal))


def test_find_with_string():
    expr = polyglot_sql.parse_one("SELECT a FROM t")
    col = expr.find("column")
    assert isinstance(col, polyglot_sql.Column)


def test_unnest():
    expr = polyglot_sql.parse_one("SELECT (a)")
    paren = expr.find(polyglot_sql.Paren)
    assert paren is not None
    unnested = paren.unnest()
    assert isinstance(unnested, polyglot_sql.Column)


def test_unalias():
    expr = polyglot_sql.parse_one("SELECT a AS b")
    alias_expr = expr.find(polyglot_sql.Alias)
    assert alias_expr is not None
    unaliased = alias_expr.unalias()
    assert isinstance(unaliased, polyglot_sql.Column)

    # Non-alias returns self
    col = polyglot_sql.parse_one("SELECT a").find(polyglot_sql.Column)
    assert col.unalias() is col


def test_flatten():
    expr = polyglot_sql.parse_one("SELECT * WHERE a AND b AND c")
    and_node = expr.find(polyglot_sql.And)
    assert and_node is not None
    flat = and_node.flatten()
    assert len(flat) == 3
    assert all(isinstance(f, polyglot_sql.Column) for f in flat)


def test_is_string():
    lit_str = polyglot_sql.parse_one("SELECT 'hello'").find(polyglot_sql.Literal)
    assert lit_str.is_string

    lit_num = polyglot_sql.parse_one("SELECT 42").find(polyglot_sql.Literal)
    assert not lit_num.is_string


def test_is_number():
    lit_num = polyglot_sql.parse_one("SELECT 42").find(polyglot_sql.Literal)
    assert lit_num.is_number
    assert lit_num.is_int

    lit_float = polyglot_sql.parse_one("SELECT 3.14").find(polyglot_sql.Literal)
    assert lit_float.is_number
    assert not lit_float.is_int

    lit_str = polyglot_sql.parse_one("SELECT 'hello'").find(polyglot_sql.Literal)
    assert not lit_str.is_number


def test_is_star():
    star = polyglot_sql.parse_one("SELECT *").find(polyglot_sql.Star)
    assert star is not None
    assert star.is_star

    col = polyglot_sql.parse_one("SELECT a").find(polyglot_sql.Column)
    assert not col.is_star


def test_is_leaf():
    lit = polyglot_sql.parse_one("SELECT 1").find(polyglot_sql.Literal)
    assert lit.is_leaf()

    expr = polyglot_sql.parse_one("SELECT a FROM t")
    assert not expr.is_leaf()


def test_iter_expressions():
    expr = polyglot_sql.parse_one("SELECT a, b FROM t")
    children = expr.children()
    iter_exprs = expr.iter_expressions()
    assert len(children) == len(iter_exprs)


def test_text_method():
    expr = polyglot_sql.parse_one("SELECT a")
    col = expr.find(polyglot_sql.Column)
    assert col is not None
    # "name" arg is an identifier, text extracts the name
    name_text = col.text("name")
    assert name_text == "a"


def test_args_property():
    expr = polyglot_sql.parse_one("SELECT a FROM t")
    args = expr.args
    assert isinstance(args, dict)
    assert "expressions" in args


def test_parent_select():
    expr = polyglot_sql.parse_one("SELECT a FROM t")
    col = expr.expressions[0]
    ps = col.parent_select
    assert ps is not None
    assert ps.kind == "select"


def test_find_ancestor():
    expr = polyglot_sql.parse_one("SELECT a FROM t")
    col = expr.expressions[0]
    ancestor = col.find_ancestor(polyglot_sql.Select)
    assert ancestor is not None
    assert ancestor.kind == "select"


def test_comments_property():
    expr = polyglot_sql.parse_one("SELECT a FROM t")
    # Most expressions have empty comments
    assert isinstance(expr.comments, list)
