"""Optional execution checks against DuckDB 2.0.

Build the local extension, install duckdb>=2.0, and run this file with pytest.
"""

import pytest

import polyglot_sql


duckdb = pytest.importorskip("duckdb", minversion="2.0")


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT 'abc' ~ 'b'", [(True,)]),
        ("SELECT 'abc' !~ 'b'", [(False,)]),
        ("SELECT 'abc' ~ 'a.*'", [(True,)]),
        ("SELECT 'abC' ~ 'abc'", [(False,)]),
        ("SELECT NULL ~ 'b'", [(None,)]),
        (
            "SELECT value ~ pattern FROM (VALUES ('abc', 'b'), ('abc', 'abc')) AS samples(value, pattern)",
            [(True,), (True,)],
        ),
        (
            "SELECT regexp_full_match('abc', 'b'), regexp_matches('abc', 'b')",
            [(False, True)],
        ),
        ("SELECT regexp_matches('AbC', 'b', 'i')", [(True,)]),
    ],
)
def test_duckdb2_regex_preserves_executed_values(sql, expected):
    expression = polyglot_sql.parse_one(sql, dialect="duckdb")
    [generated] = polyglot_sql.generate(expression, dialect="duckdb")
    [transpiled] = polyglot_sql.transpile(sql, read="duckdb", write="duckdb")
    with duckdb.connect() as connection:
        assert connection.sql(sql).fetchall() == expected
        assert connection.sql(generated).fetchall() == expected
        assert connection.sql(transpiled).fetchall() == expected
