"""Optional engine comparisons for DuckDB -> DataFusion integer division.

Build the local Python extension with ``make develop-python``, then install
``duckdb`` and ``datafusion`` in that environment and run::

    cd crates/polyglot-sql-python
    uv run --no-sync pytest tests/test_duckdb_datafusion_execution.py

The module is skipped when either optional database engine is unavailable.
"""

import math

import pytest

import polyglot_sql


duckdb = pytest.importorskip("duckdb")
datafusion = pytest.importorskip("datafusion")


QUERIES = [
    "SELECT ABS(7 / 2) // 2 AS v",
    "SELECT COALESCE(7 / 2, 0) // 2 AS v",
    "SELECT (CASE WHEN TRUE THEN 7 / 2 ELSE 0 END) // 2 AS v",
    "SELECT CAST(7 / 2 AS DOUBLE) // 2 AS v",
    "SELECT (x / 2) // 2 AS v FROM (VALUES (7)) AS t(x)",
    "SELECT ABS(x / y) // 2 AS v FROM (VALUES (7, 2)) AS t(x, y)",
    "SELECT (CAST(1 AS DECIMAL(10, 1)) / 3) // 2 AS v",
    "SELECT (7 / NULLIF(2, 0)) // 2 AS v",
    "SELECT (7 / NULLIF(2, 0)) // CAST(2 AS DECIMAL(10, 1)) AS v",
    "SELECT ABS(7 // 2) // 2 AS v",
    "SELECT COALESCE(NULL, 7 // 2) // 2 AS v",
    "SELECT (7 // CAST(2 AS FLOAT)) // 2 AS v",
    "SELECT CAST(7 / 2 AS INTEGER) // 2 AS v",
    "SELECT TRY_CAST(-7 / 2 AS INTEGER) // 2 AS v",
    "SELECT CAST(CAST(7 AS DECIMAL(10, 1)) / 2 AS INTEGER) // 2 AS v",
    "SELECT 7 // 2 AS v",
    "SELECT -7 // 2 AS v",
    "SELECT 7 // -2 AS v",
    "SELECT -7 // -2 AS v",
    "SELECT 9007199254740995 // 2 AS v",
    "SELECT -9007199254740995 // 2 AS v",
    "SELECT 9223372036854775807 // 2 AS v",
    "SELECT -9223372036854775808 // 2 AS v",
    "SELECT 7 // 0 AS v",
    "SELECT 7 // (1 - 1) AS v",
    "SELECT 7 // CAST(0 AS DOUBLE) AS v",
    "SELECT CAST(x AS INTEGER) // CAST(y AS INTEGER) AS v FROM (VALUES (7, 2), (-7, 2), (7, 0), (NULL, 2)) AS t(x, y)",
    "SELECT CAST(x AS DECIMAL(10, 1)) // 3 AS v FROM (VALUES (1), (2), (NULL)) AS t(x)",
    "SELECT (CAST(x AS DECIMAL(10, 1)) // 3) * 3 = 1 AS v FROM (VALUES (1)) AS t(x)",
    "SELECT 7 // 2 // CAST(3 AS DECIMAL(10, 1)) AS v",
    "SELECT 7 // (2 // 1) AS v",
    "SELECT 7 // (2 // 0) AS v",
    "SELECT 7 // (4 / 2) AS v",
    "SELECT 7.5 / 2 // 2 AS v",
    "SELECT (7 / 0) // 2 AS v",
    "SELECT (0 / 0) // 2 AS v",
    "SELECT 7 // (0 / 0) AS v",
    "SELECT (CAST(7 AS FLOAT) / CAST(3 AS FLOAT)) // 2 AS v",
    "SELECT CAST(ROUND(7 // 2, 0) AS INTEGER) // 2 AS v",
    "SELECT CAST(2.5 AS INTEGER) // 2 AS v",
    "SELECT CAST(-2.5 AS INTEGER) // 2 AS v",
    "SELECT IF(TRUE, 7 / 2, 0) // 2 AS v",
]


@pytest.fixture(scope="module")
def engines():
    source = duckdb.connect()
    yield source, datafusion.SessionContext()
    source.close()


def same_value(left, right):
    return left == right or (
        isinstance(left, float)
        and isinstance(right, float)
        and math.isnan(left)
        and math.isnan(right)
    )


@pytest.mark.parametrize("unsupported_level", [None, "raise"])
@pytest.mark.parametrize("sql", QUERIES)
def test_division_matches_duckdb(engines, sql, unsupported_level):
    source, target = engines
    [generated] = polyglot_sql.transpile(
        sql, read="duckdb", write="datafusion", unsupported_level=unsupported_level
    )
    expected = source.sql(sql).fetchall()
    actual = [
        tuple(column[row].as_py() for column in batch.columns)
        for batch in target.sql(generated).collect()
        for row in range(batch.num_rows)
    ]
    assert len(actual) == len(expected), (sql, generated, expected, actual)
    assert all(
        len(a) == len(b) and all(same_value(x, y) for x, y in zip(a, b))
        for a, b in zip(actual, expected)
    ), (sql, generated, expected, actual)
