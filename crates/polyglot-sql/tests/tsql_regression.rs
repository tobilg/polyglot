//! Regression tests for PostgreSQL → T-SQL transpilation.

use polyglot_sql::{transpile, DialectType};

fn pg_to_tsql(sql: &str) -> String {
    transpile(sql, DialectType::PostgreSQL, DialectType::TSQL)
        .unwrap_or_else(|e| panic!("transpile failed for {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("expected at least one statement")
}

// ---------------------------------------------------------------------------
// BPCHAR → CHAR normalisation
// ---------------------------------------------------------------------------

#[test]
fn bpchar_cast_no_length_maps_to_char() {
    let out = pg_to_tsql("SELECT CAST(x AS BPCHAR)");
    assert!(
        out.contains("CAST(x AS CHAR)") || out.contains("CAST([x] AS CHAR)"),
        "expected CAST(x AS CHAR), got: {out}"
    );
}

#[test]
fn bpchar_cast_with_length_maps_to_char() {
    let out = pg_to_tsql("SELECT CAST(x AS BPCHAR(3))");
    assert!(
        out.contains("CAST(x AS CHAR(3))") || out.contains("CAST([x] AS CHAR(3))"),
        "expected CAST(x AS CHAR(3)), got: {out}"
    );
}

#[test]
fn bpchar_double_colon_no_length_maps_to_char() {
    let out = pg_to_tsql("SELECT x::bpchar");
    assert!(
        out.to_uppercase().contains("AS CHAR"),
        "expected AS CHAR in output, got: {out}"
    );
}

#[test]
fn bpchar_double_colon_with_length_maps_to_char() {
    let out = pg_to_tsql("SELECT x::bpchar(3)");
    assert!(
        out.to_uppercase().contains("AS CHAR(3)"),
        "expected AS CHAR(3) in output, got: {out}"
    );
}

#[test]
fn bpchar_ddl_column_no_length_maps_to_char() {
    let out = pg_to_tsql("CREATE TABLE t (x BPCHAR)");
    assert!(
        out.to_uppercase().contains("CHAR"),
        "expected CHAR type in DDL, got: {out}"
    );
    assert!(
        !out.to_uppercase().contains("BPCHAR"),
        "BPCHAR should not appear in output, got: {out}"
    );
}

#[test]
fn bpchar_ddl_column_with_length_maps_to_char() {
    let out = pg_to_tsql("CREATE TABLE t (x BPCHAR(3))");
    assert!(
        out.to_uppercase().contains("CHAR(3)"),
        "expected CHAR(3) in DDL, got: {out}"
    );
}
