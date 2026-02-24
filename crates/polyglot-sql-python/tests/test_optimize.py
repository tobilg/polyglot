import pytest

import polyglot_sql


def test_optimize_returns_sql():
    optimized = polyglot_sql.optimize("SELECT a FROM t WHERE b = 1", dialect="postgres")
    assert isinstance(optimized, str)
    parsed = polyglot_sql.parse_one(optimized, dialect="postgres")
    assert isinstance(parsed, dict)


def test_optimized_sql_is_parseable():
    optimized = polyglot_sql.optimize("SELECT 1 + 2 * 3", dialect="postgres")
    assert isinstance(polyglot_sql.parse_one(optimized, dialect="postgres"), dict)


def test_optimize_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.optimize("SELECT 1", dialect="not_a_dialect")
