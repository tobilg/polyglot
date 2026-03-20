import pytest

import polyglot_sql


def test_parse_single_select_returns_list_of_expressions():
    ast_list = polyglot_sql.parse("SELECT 1", dialect="postgres")
    assert isinstance(ast_list, list)
    assert len(ast_list) == 1
    assert isinstance(ast_list[0], polyglot_sql.Expression)
    assert isinstance(ast_list[0], polyglot_sql.Select)
    assert ast_list[0].kind == "select"


def test_parse_one_returns_expression():
    ast = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    assert isinstance(ast, polyglot_sql.Expression)
    assert isinstance(ast, polyglot_sql.Select)
    assert ast.kind == "select"


def test_parse_one_returns_typed_subclass():
    ast = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    assert type(ast).__name__ == "Select"
    assert isinstance(ast, polyglot_sql.Select)
    assert isinstance(ast, polyglot_sql.Expression)
    # Not a different subclass
    assert not isinstance(ast, polyglot_sql.Insert)


def test_parse_returns_typed_subclasses():
    results = polyglot_sql.parse("SELECT 1; SELECT 2", dialect="postgres")
    assert all(isinstance(r, polyglot_sql.Select) for r in results)
    assert all(isinstance(r, polyglot_sql.Expression) for r in results)


def test_parse_multiple_statements_returns_n_entries():
    ast_list = polyglot_sql.parse("SELECT 1; SELECT 2", dialect="postgres")
    assert len(ast_list) == 2
    assert ast_list[0].kind == "select"
    assert ast_list[1].kind == "select"


def test_parse_one_multiple_statements_raises_parse_error():
    with pytest.raises(polyglot_sql.ParseError):
        polyglot_sql.parse_one("SELECT 1; SELECT 2", dialect="postgres")


def test_parse_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.parse("SELECT 1", dialect="not_a_dialect")


def test_parse_one_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.parse_one("SELECT 1", dialect="not_a_dialect")


def test_parse_invalid_sql_raises_parse_error():
    with pytest.raises(polyglot_sql.ParseError):
        polyglot_sql.parse("SELECT FROM", dialect="postgres")
