import pytest

import polyglot_sql


def test_parse_single_select_returns_list():
    ast_list = polyglot_sql.parse("SELECT 1", dialect="postgres")
    assert isinstance(ast_list, list)
    assert len(ast_list) == 1
    assert isinstance(ast_list[0], dict)
    assert "select" in ast_list[0]


def test_parse_one_returns_dict():
    ast = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    assert isinstance(ast, dict)
    assert "select" in ast


def test_parse_multiple_statements_returns_n_entries():
    ast_list = polyglot_sql.parse("SELECT 1; SELECT 2", dialect="postgres")
    assert len(ast_list) == 2
    assert "select" in ast_list[0]
    assert "select" in ast_list[1]


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
