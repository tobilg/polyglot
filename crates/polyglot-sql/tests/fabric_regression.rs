//! Regression tests for PostgreSQL → Fabric transpilation.

use polyglot_sql::{transpile, DialectType};

fn pg_to_fabric(sql: &str) -> String {
    transpile(sql, DialectType::PostgreSQL, DialectType::Fabric)
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
    let out = pg_to_fabric("SELECT CAST(x AS BPCHAR)");
    assert_eq!(out, "SELECT CAST(x AS CHAR)");
}

#[test]
fn bpchar_cast_with_length_maps_to_char() {
    let out = pg_to_fabric("SELECT CAST(x AS BPCHAR(3))");
    assert_eq!(out, "SELECT CAST(x AS CHAR(3))");
}

#[test]
fn bpchar_double_colon_no_length_maps_to_char() {
    let out = pg_to_fabric("SELECT x::bpchar");
    assert_eq!(out, "SELECT CAST(x AS CHAR)");
}

#[test]
fn bpchar_double_colon_with_length_maps_to_char() {
    let out = pg_to_fabric("SELECT x::bpchar(3)");
    assert_eq!(out, "SELECT CAST(x AS CHAR(3))");
}

#[test]
fn bpchar_ddl_column_no_length_maps_to_char() {
    let out = pg_to_fabric("CREATE TABLE t (x BPCHAR)");
    assert_eq!(out, "CREATE TABLE t (x CHAR)");
}

#[test]
fn bpchar_ddl_column_with_length_maps_to_char() {
    let out = pg_to_fabric("CREATE TABLE t (x BPCHAR(3))");
    assert_eq!(out, "CREATE TABLE t (x CHAR(3))");
}
