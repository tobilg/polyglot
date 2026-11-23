//! DATEDIFF(unit, start, end) from other dialects becomes JULIANDAY(end) - JULIANDAY(start)
//! in SQLite.
use polyglot_sql::{transpile, DialectType};

#[test]
fn datediff_to_sqlite_keeps_start_and_end() {
    for (read, sql) in [
        (DialectType::Snowflake, "SELECT DATEDIFF(day, a, b) FROM t"),
        (DialectType::TSQL, "SELECT DATEDIFF(day, a, b) FROM t"),
        (DialectType::DuckDB, "SELECT date_diff('day', a, b) FROM t"),
        (DialectType::BigQuery, "SELECT DATE_DIFF(b, a, DAY) FROM t"),
    ] {
        let out = transpile(sql, read, DialectType::SQLite).unwrap();
        assert_eq!(
            out,
            vec!["SELECT CAST((JULIANDAY(b) - JULIANDAY(a)) AS INTEGER) FROM t"],
            "{read:?}"
        );
    }
}

#[test]
fn sqlite_datediff_is_unchanged() {
    let out = transpile(
        "SELECT DATEDIFF(a, b, 'day') FROM t",
        DialectType::SQLite,
        DialectType::SQLite,
    )
    .unwrap();
    assert_eq!(
        out,
        vec!["SELECT CAST((JULIANDAY(a) - JULIANDAY(b)) AS INTEGER) FROM t"]
    );
}
