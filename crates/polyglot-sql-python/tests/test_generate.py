import pytest

import polyglot_sql


def test_generate_roundtrip_from_parse_one():
    ast = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    out = polyglot_sql.generate(ast, dialect="postgres")
    assert isinstance(out, list)
    assert len(out) == 1
    assert "SELECT 1" in out[0]


def test_generate_invalid_ast_raises_generate_error():
    with pytest.raises(polyglot_sql.GenerateError):
        polyglot_sql.generate({"bad": "ast"}, dialect="postgres")


def test_generate_list_of_asts_returns_list():
    ast1 = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    ast2 = polyglot_sql.parse_one("SELECT 2", dialect="postgres")
    out = polyglot_sql.generate([ast1, ast2], dialect="postgres")
    assert len(out) == 2


def test_generate_with_different_target_dialect_transforms_output():
    ast = polyglot_sql.parse_one("SELECT x::TEXT FROM t", dialect="postgres")
    out = polyglot_sql.generate(ast, dialect="mysql")
    assert out == ["SELECT CAST(x AS CHAR) FROM t"]


def test_generate_pretty_contains_newlines():
    ast = polyglot_sql.parse_one("SELECT a,b FROM t WHERE x=1", dialect="postgres")
    out = polyglot_sql.generate(ast, dialect="postgres", pretty=True)
    assert len(out) == 1
    assert "\n" in out[0]


def test_generate_unknown_dialect_raises_value_error():
    ast = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    with pytest.raises(ValueError):
        polyglot_sql.generate(ast, dialect="not_a_dialect")
