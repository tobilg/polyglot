import json
import os

import pytest

import polyglot_sql


def test_public_api_matches_capability_contract():
    path = os.environ.get("POLYGLOT_API_CONTRACT")
    if not path:
        pytest.skip("POLYGLOT_API_CONTRACT is not set")

    with open(path, encoding="utf-8") as contract_file:
        contract = json.load(contract_file)

    assert contract["schemaVersion"] == 1
    assert contract["layers"] == ["rust", "python", "ffi", "go", "wasm", "typescript"]
    seen = set()
    for capability in contract["capabilities"]:
        capability_id = capability["id"]
        assert capability_id not in seen
        seen.add(capability_id)

        entry = capability["layers"]["python"]
        assert entry["status"] in {"supported", "partial", "unavailable"}
        if entry["status"] != "supported":
            assert entry.get("notes")

        for symbol in entry["symbols"]:
            value = polyglot_sql
            exists = True
            for part in symbol.split("."):
                if not hasattr(value, part):
                    exists = False
                    break
                value = getattr(value, part)

            if entry["status"] == "unavailable":
                assert not exists, f"{capability_id}: {symbol} unexpectedly exists"
            else:
                assert exists, f"{capability_id}: {symbol} is missing"


def test_sqlglot_style_transpile_signature_pattern():
    out = polyglot_sql.transpile("SELECT 1", read="postgres", write="mysql")
    assert isinstance(out, list)


def test_sqlglot_style_parse_one_signature_pattern():
    ast = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    assert isinstance(ast, polyglot_sql.Expression)


def test_sqlglot_style_parse_read_alias_pattern():
    ast_list = polyglot_sql.parse("SELECT 1", read="postgres")
    assert isinstance(ast_list, list)
    assert isinstance(ast_list[0], polyglot_sql.Expression)


def test_sqlglot_style_transpile_identity_default_pattern():
    out = polyglot_sql.transpile("SELECT 1", read="postgres")
    assert out == ["SELECT 1"]


def test_sqlglot_style_optimize_read_alias_pattern():
    optimized = polyglot_sql.optimize("SELECT 1 + 2 * 3", read="postgres")
    assert isinstance(optimized, str)
    assert isinstance(polyglot_sql.parse_one(optimized, read="postgres"), polyglot_sql.Expression)


def test_sqlglot_style_error_level_keyword_is_accepted_for_valid_sql():
    ast = polyglot_sql.parse_one("SELECT 1", read="postgres", error_level="warn")
    assert isinstance(ast, polyglot_sql.Expression)
    out = polyglot_sql.transpile("SELECT 1", read="postgres", error_level="raise")
    assert out == ["SELECT 1"]


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
        ("SELECT ARRAY_LENGTH(arr) FROM t", "bigquery", "postgres", "SELECT ARRAY_LENGTH(arr, 1) FROM t"),
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
        ("SELECT DATEADD(day, 1, ts) FROM t", "tsql", "postgres", "SELECT ts + INTERVAL '1 DAY' FROM t"),
        ("SELECT a FROM t", "postgres", "snowflake", "SELECT a FROM t"),
        ("SELECT a FROM t", "snowflake", "postgres", "SELECT a FROM t"),
    ],
)
def test_curated_compat_transpilation_cases(sql: str, read: str, write: str, expected: str):
    out = polyglot_sql.transpile(sql, read=read, write=write)
    assert out == [expected]
