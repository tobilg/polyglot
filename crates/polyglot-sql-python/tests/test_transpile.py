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


def test_tsql_identity_preserves_nvarchar():
    out = polyglot_sql.transpile(
        "SELECT CAST(x AS NVARCHAR(MAX))",
        read="tsql",
        write="tsql",
    )
    assert out == ["SELECT CAST(x AS NVARCHAR(MAX))"]


def test_tsql_to_fabric_maps_nvarchar_to_varchar():
    out = polyglot_sql.transpile(
        "SELECT CAST(x AS NVARCHAR(MAX))",
        read="tsql",
        write="fabric",
    )
    assert out == ["SELECT CAST(x AS VARCHAR(MAX))"]


def test_snowflake_timestamp_variant_names_match_sqlglot_aliases():
    out = polyglot_sql.transpile(
        "SELECT CURRENT_TIMESTAMP()::TIMESTAMPNTZ",
        read="snowflake",
        write="snowflake",
    )
    assert out == ["SELECT CAST(CURRENT_TIMESTAMP() AS TIMESTAMPNTZ)"]


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


def test_unsupported_level_raise_rejects_known_unsupported_transpile():
    with pytest.raises(polyglot_sql.TranspileError) as excinfo:
        polyglot_sql.transpile(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t",
            read="postgres",
            write="fabric",
            unsupported_level="raise",
        )

    assert "recursive CTEs" in str(excinfo.value)


def test_unsupported_level_warn_preserves_default_transpile_behavior():
    out = polyglot_sql.transpile(
        "SELECT ARRAY_AGG(x) FROM t",
        read="postgres",
        write="fabric",
        unsupported_level="warn",
    )

    assert out == ["SELECT ARRAY_AGG(x) FROM t"]


def test_invalid_unsupported_level_raises_value_error():
    with pytest.raises(ValueError, match="Unsupported unsupported_level"):
        polyglot_sql.transpile(
            "SELECT 1",
            read="postgres",
            write="fabric",
            unsupported_level="loud",
        )


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
