use polyglot_sql::{transpile, DialectType, Error};

fn pg_to_sqlite(sql: &str) -> String {
    transpile(sql, DialectType::PostgreSQL, DialectType::SQLite)
        .expect("PostgreSQL to SQLite transpilation should succeed")
        .join("; ")
}

#[test]
fn postgres_to_sqlite_rewrites_scalar_json_and_dates() {
    let cases = [
        ("SELECT LEAST(a, b) FROM t", "SELECT MIN(a, b) FROM t"),
        ("SELECT GREATEST(a, b) FROM t", "SELECT MAX(a, b) FROM t"),
        (
            "SELECT JSON_AGG(name) FROM t",
            "SELECT JSON_GROUP_ARRAY(name) FROM t",
        ),
        (
            "SELECT JSONB_AGG(name) FROM t",
            "SELECT JSON_GROUP_ARRAY(name) FROM t",
        ),
        (
            "SELECT JSON_OBJECT_AGG(k, v) FROM t",
            "SELECT JSON_GROUP_OBJECT(k, v) FROM t",
        ),
        (
            "SELECT JSON_BUILD_OBJECT('id', id, 'name', name) FROM t",
            "SELECT JSON_OBJECT('id', id, 'name', name) FROM t",
        ),
        (
            "SELECT JSON_BUILD_ARRAY(a, b, c) FROM t",
            "SELECT JSON_ARRAY(a, b, c) FROM t",
        ),
        (
            "SELECT DATE_PART('year', ts) FROM t",
            "SELECT CAST(STRFTIME('%Y', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT DATE_PART('hour', ts) FROM t",
            "SELECT CAST(STRFTIME('%H', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT DATE_PART('minute', ts) FROM t",
            "SELECT CAST(STRFTIME('%M', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT DATE_PART('second', ts) FROM t",
            "SELECT CAST(STRFTIME('%f', ts) AS REAL) FROM t",
        ),
        (
            "SELECT DATE_PART('dow', ts) FROM t",
            "SELECT CAST(STRFTIME('%w', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT DATE_PART('doy', ts) FROM t",
            "SELECT CAST(STRFTIME('%j', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT DATE_PART('epoch', ts) FROM t",
            "SELECT CAST(STRFTIME('%s', ts) AS REAL) FROM t",
        ),
        (
            "SELECT EXTRACT(YEAR FROM ts) FROM t",
            "SELECT CAST(STRFTIME('%Y', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT EXTRACT(HOUR FROM ts) FROM t",
            "SELECT CAST(STRFTIME('%H', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT EXTRACT(MINUTE FROM ts) FROM t",
            "SELECT CAST(STRFTIME('%M', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT EXTRACT(SECOND FROM ts) FROM t",
            "SELECT CAST(STRFTIME('%f', ts) AS REAL) FROM t",
        ),
        (
            "SELECT EXTRACT(DOW FROM ts) FROM t",
            "SELECT CAST(STRFTIME('%w', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT EXTRACT(DOY FROM ts) FROM t",
            "SELECT CAST(STRFTIME('%j', ts) AS INTEGER) FROM t",
        ),
        (
            "SELECT EXTRACT(EPOCH FROM ts) FROM t",
            "SELECT CAST(STRFTIME('%s', ts) AS REAL) FROM t",
        ),
        (
            "SELECT DATE_TRUNC('month', ts) FROM t",
            "SELECT STRFTIME('%Y-%m-01', ts) FROM t",
        ),
        (
            "SELECT DATE_TRUNC('hour', ts) FROM t",
            "SELECT STRFTIME('%Y-%m-%d %H:00:00', ts) FROM t",
        ),
        (
            "SELECT DATE_TRUNC('minute', ts) FROM t",
            "SELECT STRFTIME('%Y-%m-%d %H:%M:00', ts) FROM t",
        ),
        (
            "SELECT DATE_TRUNC('second', ts) FROM t",
            "SELECT STRFTIME('%Y-%m-%d %H:%M:%S', ts) FROM t",
        ),
        (
            "SELECT DATE_TRUNC('day', ts) FROM t",
            "SELECT DATE(ts) FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_sqlite(sql), expected, "input: {sql}");
    }
}

#[test]
fn postgres_to_sqlite_rewrites_common_syntax_and_types() {
    let cases = [
        (
            "SELECT SUBSTRING(str FROM 2 FOR 5) FROM t",
            "SELECT SUBSTRING(str, 2, 5) FROM t",
        ),
        (
            "SELECT TRIM(LEADING 'x' FROM str) FROM t",
            "SELECT LTRIM(str, 'x') FROM t",
        ),
        ("SELECT DATE '2023-01-01'", "SELECT '2023-01-01'"),
        (
            "CREATE TABLE public.t (id INT, name TEXT)",
            "CREATE TABLE t (id INTEGER, name TEXT)",
        ),
        (
            "CREATE TABLE t (flag BIT, label NVARCHAR(100), doc TSVECTOR)",
            "CREATE TABLE t (flag INTEGER, label TEXT, doc TEXT)",
        ),
        (
            "CREATE TABLE t (data JSONB, ts TIMESTAMPTZ)",
            "CREATE TABLE t (data JSON, ts TEXT)",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_sqlite(sql), expected, "input: {sql}");
    }
}

#[test]
fn postgres_to_sqlite_rejects_pgvector_distance_operators() {
    for sql in ["SELECT a <=> b FROM t", "SELECT a <~> b FROM t"] {
        let err = transpile(sql, DialectType::PostgreSQL, DialectType::SQLite)
            .expect_err("pgvector distance operators have no SQLite equivalent");
        assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    }
}
