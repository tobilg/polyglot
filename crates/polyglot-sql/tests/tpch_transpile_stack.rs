//! Regression tests for transpiling TPC-H queries.
//!
//! Customer-reported case: TPC-H Query 2 (a 5-table cross join with a
//! correlated scalar subquery in the WHERE clause) overflowed the thread
//! stack when transpiled from PostgreSQL to Fabric.  The root cause was
//! the unboxed `Star` variant inflating `Expression` to 232 bytes —
//! since Rust (in debug) allocates stack space for ALL match-arm locals
//! simultaneously, functions like `cross_dialect_normalize` (which has
//! dozens of Expression-typed temporaries across its ~60 match arms)
//! had a 22 KB frame.  Boxing `Star` dropped `Expression` to 96 bytes,
//! shrinking `cross_dialect_normalize` from 22 KB to 14 KB and
//! `transform_recursive_inner` from 5.7 KB to 4.2 KB per frame.
//!
//! Source of the SQL:
//! https://github.com/lushl9301/TPCH-in-English/blob/master/tpch-query2.md

use polyglot_sql::{transpile, DialectType};

/// TPC-H Query 2 in PostgreSQL dialect, copied verbatim from the upstream
/// "TPCH-in-English" repo (comments stripped — the parser would accept
/// them, but they aren't relevant to the regression we're guarding).
const TPCH_QUERY_2: &str = r#"
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
        )
order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey
limit 100
"#;

/// Baseline: TPC-H Q2 must transpile PostgreSQL -> Fabric and produce a
/// single output statement that preserves the query's structure (FROM
/// list, WHERE conjuncts, the correlated MIN(...) subquery, ORDER BY and
/// LIMIT). Runs on whatever stack `cargo test` provides — exists to lock
/// in correctness independently of the stack-size regression.
#[test]
fn tpch_query2_transpile_postgres_to_fabric() {
    let stmts = transpile(TPCH_QUERY_2, DialectType::PostgreSQL, DialectType::Fabric)
        .expect("TPC-H Q2 should transpile cleanly");

    assert_eq!(stmts.len(), 1, "expected a single output statement");
    let sql = &stmts[0];

    // Outer query shape.
    assert!(sql.starts_with("SELECT "), "got: {sql}");
    assert!(sql.contains(" FROM part, supplier, partsupp, nation, region "));
    assert!(sql.contains(" WHERE p_partkey = ps_partkey "));
    assert!(sql.contains(" AND p_type LIKE '%BRASS' "));
    assert!(sql.contains(" AND r_name = 'EUROPE' "));

    // Correlated scalar subquery (the part of the AST that drives the
    // recursion that used to blow the stack).
    assert!(
        sql.contains(" AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM "),
        "missing correlated MIN subquery; got: {sql}"
    );

    // Trailing clauses.
    assert!(sql.contains(" ORDER BY s_acctbal DESC"));
    assert!(sql.trim_end().ends_with(" LIMIT 100"));
}

/// Regression: the same transpile must succeed on a thread with a 4 MB
/// stack.  Before the `Expression::Star` boxing fix, `Expression` was
/// 232 bytes (due to the unboxed `Star` variant), which inflated every
/// stack frame enough to overflow even an 8 MB stack on TPC-H Q2 in
/// debug builds.  After boxing, `Expression` dropped to 96 bytes and
/// this query completes comfortably within 4 MB.
///
/// The 4 MB limit is chosen because:
///   - `cargo test` runs debug (unoptimized) builds where frames are
///     largest; 4 MB is well under the pre-fix overflow threshold of
///     ~6–8 MB and proves the fix is effective.
///   - Release builds need only ~512 KB for this query, so 4 MB gives
///     ample headroom across both profiles.
///
/// Note: this test does NOT require the `stacker` cargo feature. The
/// `stacker` feature guards against *deeply nested* recursion (hundreds
/// of subquery levels); TPC-H Q2 has only one subquery level, so the
/// Star boxing alone is the fix.
#[test]
fn tpch_query2_transpile_succeeds_on_constrained_stack() {
    let handle = std::thread::Builder::new()
        .name("tpch-q2-constrained".to_string())
        .stack_size(4 * 1024 * 1024)
        .spawn(|| {
            transpile(TPCH_QUERY_2, DialectType::PostgreSQL, DialectType::Fabric).map(|v| v.len())
        })
        .expect("spawn 4 MB thread");

    let stmt_count = handle
        .join()
        .expect("transpile thread must not panic / overflow")
        .expect("transpile must return Ok");
    assert_eq!(stmt_count, 1);
}

/// Build a deeply nested `SELECT * FROM (SELECT * FROM (... SELECT 1 ...))` query.
fn nested_select_star(depth: usize) -> String {
    let mut sql = "SELECT 1".to_string();
    for _ in 0..depth {
        sql = format!("SELECT * FROM ({sql}) t");
    }
    sql
}

/// Regression: deeply nested subqueries (200 levels) must not overflow
/// when the `stacker` feature is enabled.  Without stacker, this level
/// of nesting exceeds any reasonable thread stack; with stacker, the
/// `maybe_grow` guards in `parse_statement`, `transform_recursive`, and
/// `generate_expression` allocate fresh stack segments on demand.
///
/// 200 levels is chosen as a reasonable stress test — well beyond what
/// production SQL typically contains, but a realistic fuzzing / adversarial
/// input scenario.  On the default 8 MB stack without stacker, overflow
/// occurs around 100 levels in release builds and ~50 in debug builds.
///
/// The 4 MB thread gives enough room for the non-guarded setup code
/// (tokenisation, transpile pipeline orchestration) while relying on
/// stacker for the recursive descent through the AST.
#[cfg(feature = "stacker")]
#[test]
fn deeply_nested_200_levels_with_stacker() {
    let sql = nested_select_star(200);
    let handle = std::thread::Builder::new()
        .name("nested-200".to_string())
        .stack_size(4 * 1024 * 1024)
        .spawn(move || {
            transpile(&sql, DialectType::PostgreSQL, DialectType::Fabric).map(|v| v.len())
        })
        .expect("spawn 4 MB thread");

    let stmt_count = handle
        .join()
        .expect("deeply nested transpile must not overflow with stacker")
        .expect("transpile must return Ok");
    assert_eq!(stmt_count, 1);
}
