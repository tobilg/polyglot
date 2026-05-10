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

// ---------------------------------------------------------------------------
// = ANY(ARRAY[...]) / = ANY((...)) → IN
// ---------------------------------------------------------------------------

#[test]
fn any_eq_array_brackets_rewrites_to_in() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY(ARRAY['a', 'b', 'c'])");
    assert_eq!(out, "SELECT * FROM t WHERE col IN ('a', 'b', 'c')");
}

#[test]
fn any_eq_tuple_rewrites_to_in() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY(('a', 'b', 'c'))");
    assert_eq!(out, "SELECT * FROM t WHERE col IN ('a', 'b', 'c')");
}

#[test]
fn any_eq_empty_array_rewrites_to_always_false() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY(ARRAY[])");
    assert_eq!(out, "SELECT * FROM t WHERE 1 = 0");
}

#[test]
fn any_neq_array_not_rewritten() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])");
    assert_eq!(out, "SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])");
}

#[test]
fn any_eq_subquery_not_rewritten() {
    let out = pg_to_fabric("SELECT * FROM t WHERE col = ANY(SELECT id FROM s)");
    assert_eq!(out, "SELECT * FROM t WHERE col = ANY (SELECT id FROM s)");
}

// ---------------------------------------------------------------------------
// PostgreSQL interval arithmetic -> Fabric DATEADD
// ---------------------------------------------------------------------------

#[test]
fn date_minus_interval_with_precision_rewrites_to_dateadd() {
    let out =
        pg_to_fabric("SELECT l_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY (3) FROM lineitem");
    assert_eq!(
        out,
        "SELECT l_shipdate <= DATEADD(DAY, -3, CAST('1998-12-01' AS DATE)) FROM lineitem"
    );
}

#[test]
fn date_minus_interval_placeholder_rewrites_to_unquoted_dateadd_amount() {
    let out = pg_to_fabric(
        "SELECT l_shipdate <= DATE '1998-12-01' - INTERVAL ':1' DAY (3) FROM lineitem",
    );
    assert_eq!(
        out,
        "SELECT l_shipdate <= DATEADD(DAY, -:1, CAST('1998-12-01' AS DATE)) FROM lineitem"
    );
}

#[test]
fn date_minus_cast_interval_rewrites_to_dateadd() {
    let out = pg_to_fabric("SELECT shipdate - CAST('3 day' AS INTERVAL) FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(DAY, -3, shipdate) FROM lineitem");
}

#[test]
fn date_plus_cast_interval_rewrites_to_dateadd() {
    let out = pg_to_fabric("SELECT shipdate + CAST('3 day' AS INTERVAL) FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(DAY, 3, shipdate) FROM lineitem");
}

#[test]
fn date_plus_month_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT DATE '1993-07-01' + INTERVAL '3 months'");
    assert_eq!(out, "SELECT DATEADD(MONTH, 3, CAST('1993-07-01' AS DATE))");
}

#[test]
fn date_plus_mon_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT DATE '1993-07-01' + INTERVAL '3 mon'");
    assert_eq!(out, "SELECT DATEADD(MONTH, 3, CAST('1993-07-01' AS DATE))");
}

#[test]
fn date_plus_mons_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT DATE '1993-07-01' + INTERVAL '3 mons'");
    assert_eq!(out, "SELECT DATEADD(MONTH, 3, CAST('1993-07-01' AS DATE))");
}

#[test]
fn date_minus_mons_interval_rewrites_to_negative_month_dateadd() {
    let out = pg_to_fabric("SELECT DATE '1993-07-01' - INTERVAL '3 mons'");
    assert_eq!(out, "SELECT DATEADD(MONTH, -3, CAST('1993-07-01' AS DATE))");
}

#[test]
fn date_plus_cast_mons_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT shipdate + CAST('3 mons' AS INTERVAL) FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(MONTH, 3, shipdate) FROM lineitem");
}

#[test]
fn date_minus_double_colon_mons_interval_rewrites_to_month_dateadd() {
    let out = pg_to_fabric("SELECT shipdate - '3 mons'::INTERVAL FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(MONTH, -3, shipdate) FROM lineitem");
}

#[test]
fn date_minus_double_colon_interval_rewrites_to_dateadd() {
    let out = pg_to_fabric("SELECT shipdate - '3 day'::INTERVAL FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(DAY, -3, shipdate) FROM lineitem");
}
