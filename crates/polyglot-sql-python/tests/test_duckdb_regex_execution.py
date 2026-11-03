"""Optional execution checks for DuckDB's version-dependent regex operators.

Build the local extension, install duckdb, and run this file with pytest.
The DuckDB dialect models the 1.x full-match operators, not 2.0's default
partial-match operators; explicit regex functions avoid that session dependency.
"""

import pytest

import polyglot_sql


duckdb = pytest.importorskip("duckdb")
DUCKDB_MAJOR = int(duckdb.__version__.split(".")[0])


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT 'abc' ~ 'b'", [(False,)]),
        ("SELECT 'abc' !~ 'b'", [(True,)]),
        ("SELECT 'abc' ~ 'a.*'", [(True,)]),
        ("SELECT NULL ~ 'b'", [(None,)]),
        (
            "SELECT value ~ pattern FROM (VALUES ('abc', 'b'), ('abc', 'abc')) AS samples(value, pattern)",
            [(False,), (True,)],
        ),
    ],
)
def test_legacy_full_match_operator_preserves_executed_values(sql, expected):
    expression = polyglot_sql.parse_one(sql, dialect="duckdb")
    [generated] = polyglot_sql.generate(expression, dialect="duckdb")
    with duckdb.connect() as connection:
        if DUCKDB_MAJOR >= 2:
            connection.execute("SET regex_match_operator_semantics='full'")
        assert connection.sql(sql).fetchall() == expected
        assert connection.sql(generated).fetchall() == expected


@pytest.mark.parametrize("semantics", ["full", "partial"])
def test_explicit_regex_functions_do_not_depend_on_operator_setting(semantics):
    sql = "SELECT regexp_full_match('abc', 'b'), regexp_matches('abc', 'b')"
    [generated] = polyglot_sql.generate(
        polyglot_sql.parse_one(sql, dialect="duckdb"), dialect="duckdb"
    )
    with duckdb.connect() as connection:
        if DUCKDB_MAJOR >= 2:
            connection.execute(f"SET regex_match_operator_semantics='{semantics}'")
        assert connection.sql(sql).fetchall() == [(False, True)]
        assert connection.sql(generated).fetchall() == [(False, True)]


@pytest.mark.skipif(DUCKDB_MAJOR < 2, reason="DuckDB 2.0 introduced partial-match operators")
def test_default_duckdb2_operator_is_outside_the_legacy_dialect_contract():
    sql = "SELECT 'abc' ~ 'b'"
    [generated] = polyglot_sql.generate(
        polyglot_sql.parse_one(sql, dialect="duckdb"), dialect="duckdb"
    )
    with duckdb.connect() as connection:
        connection.execute("SET regex_match_operator_semantics='partial'")
        assert connection.sql(sql).fetchall() == [(True,)]
        assert connection.sql(generated).fetchall() == [(False,)]
