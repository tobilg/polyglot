import pytest

import polyglot_sql


def test_sqlglot_style_transpile_signature_pattern():
    out = polyglot_sql.transpile("SELECT 1", read="postgres", write="mysql")
    assert isinstance(out, list)


def test_sqlglot_style_parse_one_signature_pattern():
    ast = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    assert isinstance(ast, dict)


@pytest.mark.parametrize(
    ("sql", "read", "write", "expected"),
    [
        ("SELECT IFNULL(a, b) FROM t", "mysql", "postgres", "SELECT COALESCE(a, b) FROM t"),
        (
            "SELECT CAST(x AS TEXT) FROM t",
            "postgres",
            "snowflake",
            "SELECT CAST(x AS VARCHAR) FROM t",
        ),
        (
            "SELECT EPOCH_MS(1618088028295)",
            "duckdb",
            "hive",
            "SELECT EPOCH_MS(1618088028295)",
        ),
        ("SELECT 1", "postgres", "postgres", "SELECT 1"),
        ("SELECT CURRENT_TIMESTAMP", "postgres", "mysql", "SELECT CURRENT_TIMESTAMP()"),
        (
            "SELECT DATE_TRUNC('day', ts) FROM t",
            "postgres",
            "bigquery",
            "SELECT DATE_TRUNC('day', ts) FROM t",
        ),
        (
            "SELECT TRY_CAST(x AS INT) FROM t",
            "snowflake",
            "postgres",
            "SELECT CAST(x AS INT) FROM t",
        ),
        ("SELECT NVL(a, b) FROM t", "oracle", "postgres", "SELECT COALESCE(a, b) FROM t"),
        ("SELECT IFF(a > 1, 1, 0) FROM t", "snowflake", "postgres", "SELECT IF(a > 1, 1, 0) FROM t"),
        ("SELECT ARRAY_LENGTH(arr) FROM t", "bigquery", "postgres", "SELECT ARRAY_LENGTH(arr) FROM t"),
        ("SELECT x::TEXT FROM t", "postgres", "mysql", "SELECT CAST(x AS CHAR) FROM t"),
        ("SELECT TRUE", "postgres", "mysql", "SELECT TRUE"),
        ("SELECT FALSE", "postgres", "mysql", "SELECT FALSE"),
        ("SELECT 1 + 2 * 3", "postgres", "duckdb", "SELECT 1 + 2 * 3"),
        (
            "SELECT SUBSTRING(name, 1, 3) FROM t",
            "postgres",
            "mysql",
            "SELECT SUBSTRING(name, 1, 3) FROM t",
        ),
        (
            "SELECT COALESCE(a, b, c) FROM t",
            "postgres",
            "mysql",
            "SELECT COALESCE(a, b, c) FROM t",
        ),
        ("SELECT COUNT(*) FROM t", "postgres", "sqlite", "SELECT COUNT(*) FROM t"),
        (
            "SELECT CAST('2024-01-01' AS DATE)",
            "postgres",
            "duckdb",
            "SELECT CAST('2024-01-01' AS DATE)",
        ),
        ("SELECT NOW()", "postgres", "postgres", "SELECT CURRENT_TIMESTAMP"),
        ("SELECT DATEADD(day, 1, ts) FROM t", "tsql", "postgres", "SELECT ts + 1 FROM t"),
        ("SELECT a FROM t", "postgres", "snowflake", "SELECT a FROM t"),
        ("SELECT a FROM t", "snowflake", "postgres", "SELECT a FROM t"),
    ],
)
def test_curated_compat_transpilation_cases(sql: str, read: str, write: str, expected: str):
    out = polyglot_sql.transpile(sql, read=read, write=write)
    assert out == [expected]
