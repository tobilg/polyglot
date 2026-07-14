import polyglot_sql


EXPECTED_DIALECTS = {
    "athena",
    "bigquery",
    "clickhouse",
    "cockroachdb",
    "datafusion",
    "databricks",
    "doris",
    "dremio",
    "drill",
    "druid",
    "duckdb",
    "dune",
    "exasol",
    "fabric",
    "generic",
    "hive",
    "materialize",
    "mysql",
    "oracle",
    "postgres",
    "presto",
    "redshift",
    "risingwave",
    "singlestore",
    "snowflake",
    "solr",
    "spark",
    "sqlite",
    "starrocks",
    "tableau",
    "teradata",
    "tidb",
    "trino",
    "tsql",
}


def test_dialects_contains_known_values():
    values = polyglot_sql.dialects()
    assert isinstance(values, list)
    assert len(values) == len(EXPECTED_DIALECTS)
    assert len(values) == len(set(values))
    assert set(values) == EXPECTED_DIALECTS


def test_version_is_exposed():
    assert isinstance(polyglot_sql.__version__, str)
    assert polyglot_sql.__version__


def test_dialects_are_strings():
    values = polyglot_sql.dialects()
    assert all(isinstance(v, str) for v in values)
