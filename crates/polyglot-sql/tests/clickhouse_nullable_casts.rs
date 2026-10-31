//! Cast target types from another dialect become `Nullable(T)` in ClickHouse,
//! as in Python sqlglot. ClickHouse types are not nullable unless wrapped, so
//! `CAST(NULL AS String)` fails with "Cannot convert NULL to a non-nullable
//! type". Types written in ClickHouse itself, and types that can't be
//! `Nullable` (`Array`, `Map`), are left alone.
use polyglot_sql::{transpile, DialectType};

fn to_clickhouse(sql: &str, source: DialectType) -> String {
    transpile(sql, source, DialectType::ClickHouse)
        .unwrap()
        .remove(0)
}

#[test]
fn scalar_casts_become_nullable_from_every_source() {
    let sql = "SELECT CAST(NULL AS VARCHAR) AS s, CAST(NULL AS CHAR(3)) AS c, \
               CAST(NULL AS DATE) AS d, CAST(NULL AS BOOLEAN) AS b, \
               CAST(NULL AS DECIMAL(10, 2)) AS n, CAST(NULL AS BIGINT) AS i";
    for source in [
        DialectType::Generic,
        DialectType::DuckDB,
        DialectType::PostgreSQL,
        DialectType::Trino,
        DialectType::Snowflake,
    ] {
        let out = to_clickhouse(sql, source);
        assert_eq!(
            out.matches("CAST(NULL AS Nullable(").count(),
            6,
            "{source:?}: {out}"
        );
    }
}

#[test]
fn varchar_and_char_map_to_string() {
    assert_eq!(
        to_clickhouse(
            "SELECT CAST(x AS VARCHAR(10)), CAST(x AS CHAR(3))",
            DialectType::Generic
        ),
        "SELECT CAST(x AS Nullable(String)), CAST(x AS Nullable(String))"
    );
}

#[test]
fn postgres_double_precision_becomes_nullable() {
    let out = to_clickhouse(
        "SELECT CAST(NULL AS DOUBLE PRECISION), CAST(NULL AS REAL)",
        DialectType::PostgreSQL,
    );
    assert_eq!(out.matches("Nullable(").count(), 2, "{out}");
}

#[test]
fn composite_and_already_nullable_types_are_not_wrapped() {
    let out = to_clickhouse(
        "SELECT CAST(x AS INT[]), CAST(x AS MAP(VARCHAR, INT))",
        DialectType::DuckDB,
    );
    assert!(!out.contains("Nullable(Array"), "{out}");
    assert!(!out.contains("Nullable(Map"), "{out}");
    assert!(!out.contains("Nullable(Nullable"), "{out}");
}

#[test]
fn clickhouse_source_types_are_unchanged() {
    let sql = "SELECT CAST(x AS String), CAST(x AS Bool), CAST(x AS Date), \
               CAST(x AS Decimal(10, 2)), CAST(x AS LowCardinality(String)), \
               CAST(x AS Nullable(Int32))";
    let out = to_clickhouse(sql, DialectType::ClickHouse);
    assert_eq!(out.matches("Nullable(").count(), 1, "{out}");
}
