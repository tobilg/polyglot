import pytest

import polyglot_sql


def test_mysql_to_postgres_transpile():
    out = polyglot_sql.transpile(
        "SELECT IFNULL(a, b) FROM t",
        read="mysql",
        write="postgres",
    )
    assert len(out) == 1
    assert "COALESCE" in out[0]


def test_transpile_pretty_contains_newlines():
    out = polyglot_sql.transpile(
        "SELECT IFNULL(a,b) FROM t",
        read="mysql",
        write="postgres",
        pretty=True,
    )
    assert len(out) == 1
    assert "\n" in out[0]


def test_postgres_to_snowflake_cast_text():
    out = polyglot_sql.transpile(
        "SELECT CAST(x AS TEXT) FROM t",
        read="postgres",
        write="snowflake",
    )
    assert out == ["SELECT CAST(x AS VARCHAR) FROM t"]


def test_multi_statement_transpile_returns_multiple_entries():
    out = polyglot_sql.transpile(
        "SELECT 1; SELECT 2",
        read="postgres",
        write="postgres",
    )
    assert len(out) == 2
    assert out[0].startswith("SELECT 1")
    assert out[1].startswith("SELECT 2")


def test_identity_transpile_preserves_sql_shape():
    out = polyglot_sql.transpile(
        "SELECT a, b FROM t WHERE c = 1",
        read="postgres",
        write="postgres",
    )
    assert len(out) == 1
    assert "SELECT a, b FROM t WHERE c = 1" in out[0]


def test_invalid_sql_raises_parse_error():
    with pytest.raises(polyglot_sql.ParseError):
        polyglot_sql.transpile("SELECT FROM", read="postgres", write="mysql")


def test_unknown_read_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.transpile("SELECT 1", read="not_a_dialect", write="postgres")


def test_unknown_write_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.transpile("SELECT 1", read="postgres", write="not_a_dialect")


def test_empty_input_matches_core_behavior():
    try:
        out = polyglot_sql.transpile("", read="postgres", write="postgres")
        assert out == []
    except polyglot_sql.ParseError:
        # Accept parser-reject behavior as long as it is explicit and typed.
        pass
