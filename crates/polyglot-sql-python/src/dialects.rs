use pyo3::prelude::*;

const DIALECT_NAMES: &[&str] = &[
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
];

#[pyfunction]
pub fn dialects() -> Vec<String> {
    DIALECT_NAMES
        .iter()
        .map(|dialect| (*dialect).to_string())
        .collect()
}

#[pyfunction]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
