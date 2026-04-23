//! Regression tests for transpiling TPC-H queries.
//!
//! Customer-reported case: TPC-H Query 2 (a 5-table cross join with a
//! correlated scalar subquery in the WHERE clause) overflows the thread
//! stack when transpiled from PostgreSQL to Fabric on constrained-stack
//! runtimes (FFI worker threads, async runtime workers, wasm, debug
//! builds, etc.).
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

/// Regression: the same transpile must also succeed on a thread with a
/// 1 MB stack, which mirrors the customer's runtime (FFI / tokio worker
/// / wasm). This is the actual stack-overflow guard.
///
/// Gated behind the `stacker` cargo feature because that is the
/// documented mitigation for deep-recursion SQL: without it,
/// `transpile` is free to overflow on inputs like this one. Run with
/// `cargo test -p polyglot-sql --features stacker` to exercise.
#[cfg(feature = "stacker")]
#[test]
fn tpch_query2_transpile_succeeds_on_constrained_stack() {
    // 1 MB is well below `transpile`'s natural appetite for this query
    // (which exceeds 4 MB in debug builds), so this thread WILL abort
    // the test process via "fatal runtime error: stack overflow" if the
    // `stacker::maybe_grow` guards in the parser and dialect transformer
    // ever stop covering this case.
    let handle = std::thread::Builder::new()
        .name("tpch-q2-constrained".to_string())
        .stack_size(1024 * 1024)
        .spawn(|| {
            transpile(TPCH_QUERY_2, DialectType::PostgreSQL, DialectType::Fabric)
                .map(|v| v.len())
        })
        .expect("spawn 1 MB thread");

    let stmt_count = handle
        .join()
        .expect("transpile thread must not panic / overflow")
        .expect("transpile must return Ok");
    assert_eq!(stmt_count, 1);
}
