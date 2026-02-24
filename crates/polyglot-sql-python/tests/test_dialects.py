import polyglot_sql


def test_dialects_contains_known_values():
    values = polyglot_sql.dialects()
    assert isinstance(values, list)
    assert len(values) >= 32
    assert "postgres" in values
    assert "mysql" in values
    assert "snowflake" in values
    assert "bigquery" in values
    assert "duckdb" in values


def test_version_is_exposed():
    assert isinstance(polyglot_sql.__version__, str)
    assert polyglot_sql.__version__


def test_dialects_are_strings():
    values = polyglot_sql.dialects()
    assert all(isinstance(v, str) for v in values)
