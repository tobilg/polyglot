//! SQLite has no DATE/TIMESTAMP/TIME types: a CAST to them gets NUMERIC affinity and
//! returns the year, so they are written as the matching date/time functions.
use polyglot_sql::{transpile, DialectType};

#[test]
fn date_time_casts_to_sqlite_use_functions() {
    for read in [
        DialectType::PostgreSQL,
        DialectType::DuckDB,
        DialectType::Snowflake,
    ] {
        let out = transpile(
            "SELECT CAST(ts AS DATE), CAST(ts AS TIMESTAMP), CAST(ts AS TIME) FROM t",
            read,
            DialectType::SQLite,
        )
        .unwrap();
        assert_eq!(
            out,
            vec!["SELECT DATE(ts), DATETIME(ts), TIME(ts) FROM t"],
            "{read:?}"
        );
    }
}

#[test]
fn postgres_double_colon_date_to_sqlite() {
    let out = transpile(
        "SELECT id FROM t WHERE ts::date = DATE '2024-01-15'",
        DialectType::PostgreSQL,
        DialectType::SQLite,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT id FROM t WHERE DATE(ts) = '2024-01-15'"]);
}
