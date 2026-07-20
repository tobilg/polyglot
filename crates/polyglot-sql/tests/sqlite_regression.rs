use polyglot_sql::{transpile, DialectType, Error};

fn identity(sql: &str, dialect: DialectType) -> String {
    transpile(sql, dialect, dialect)
        .expect("round-trip should succeed")
        .join("; ")
}

// SQLite/DuckDB allow string-literal aliases; normalized to quoted identifiers.
#[test]
fn normalizes_string_literal_aliases() {
    let cases = [
        // column alias
        ("SELECT a AS 'x' FROM t", r#"SELECT a AS "x" FROM t"#),
        (
            "SELECT a AS 'record id' FROM t",
            r#"SELECT a AS "record id" FROM t"#,
        ),
        // table alias
        ("SELECT a FROM t AS 'x'", r#"SELECT a FROM t AS "x""#),
        // CTE name
        (
            "WITH 'x' AS (SELECT 1) SELECT * FROM x",
            r#"WITH "x" AS (SELECT 1) SELECT * FROM x"#,
        ),
    ];
    for dialect in [DialectType::SQLite, DialectType::DuckDB] {
        for (input, expected) in cases {
            assert_eq!(identity(input, dialect), expected, "{dialect:?}: {input}");
        }
    }
}

// Only explicit AS/CTE positions accept string aliases, not `SELECT a 'x'`.
#[test]
fn implicit_string_alias_is_not_supported() {
    for dialect in [DialectType::SQLite, DialectType::DuckDB] {
        let err = transpile("SELECT a 'x' FROM t", dialect, dialect)
            .expect_err("implicit string aliases are not supported");
        assert!(matches!(err, Error::Parse { .. }), "{dialect:?}: {err}");
    }
}

// Other dialects treat single quotes as string literals and reject this.
#[test]
fn other_dialects_reject_string_literal_aliases() {
    for dialect in [DialectType::PostgreSQL, DialectType::Snowflake] {
        let err = transpile("SELECT a AS 'x' FROM t", dialect, dialect)
            .expect_err("string-literal aliases are not valid here");
        assert!(matches!(err, Error::Parse { .. }), "{dialect:?}: {err}");
    }
}
