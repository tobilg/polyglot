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


def test_format_with_options_guard_override_rejects_set_op_chain():
    sql = "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3"
    with pytest.raises(polyglot_sql.GenerateError) as excinfo:
        polyglot_sql.format_sql(sql, dialect="generic", max_set_op_chain=1)
    assert "E_GUARD_SET_OP_CHAIN_EXCEEDED" in str(excinfo.value)


def test_format_with_options_guard_override_rejects_input_bytes():
    with pytest.raises(polyglot_sql.GenerateError) as excinfo:
        polyglot_sql.format_sql("SELECT 1", dialect="generic", max_input_bytes=7)
    assert "E_GUARD_INPUT_TOO_LARGE" in str(excinfo.value)
