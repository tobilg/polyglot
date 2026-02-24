import pytest

import polyglot_sql


def test_format_sql_contains_newlines():
    formatted = polyglot_sql.format_sql("SELECT a,b FROM t WHERE x=1", dialect="postgres")
    assert "\n" in formatted


def test_format_alias_is_available():
    formatted = polyglot_sql.format("SELECT a,b FROM t", dialect="postgres")
    assert isinstance(formatted, str)


def test_format_preserves_sql_semantics_for_simple_query():
    raw = "SELECT a,b FROM t WHERE x=1"
    formatted = polyglot_sql.format_sql(raw, dialect="postgres")
    assert polyglot_sql.parse_one(formatted, dialect="postgres") == polyglot_sql.parse_one(
        raw, dialect="postgres"
    )


def test_format_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.format_sql("SELECT 1", dialect="not_a_dialect")


def test_format_invalid_sql_raises_parse_error():
    with pytest.raises(polyglot_sql.ParseError):
        polyglot_sql.format_sql("SELECT FROM", dialect="postgres")
