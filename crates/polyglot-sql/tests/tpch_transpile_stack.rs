use polyglot_sql::{transpile, DialectType};

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

fn nested_select_star(depth: usize) -> String {
    let mut sql = "SELECT 1".to_string();
    for _ in 0..depth {
        sql = format!("SELECT * FROM ({sql}) t");
    }
    sql
}

#[test]
fn tpch_query2_transpile_postgres_to_fabric() {
    let stmts = transpile(TPCH_QUERY_2, DialectType::PostgreSQL, DialectType::Fabric)
        .expect("TPC-H Q2 should transpile cleanly");

    assert_eq!(stmts.len(), 1, "expected a single output statement");
    let sql = &stmts[0];

    assert!(sql.starts_with("SELECT TOP 100 "), "got: {sql}");
    assert!(sql.contains(" FROM part, supplier, partsupp, nation, region "));
    assert!(sql.contains(" WHERE p_partkey = ps_partkey "));
    assert!(sql.contains(" AND p_type LIKE '%BRASS' "));
    assert!(sql.contains(" AND r_name = 'EUROPE' "));
    assert!(
        sql.contains(" AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM "),
        "missing correlated MIN subquery; got: {sql}"
    );
    assert!(sql.contains(" ORDER BY s_acctbal DESC"));
    assert!(
        !sql.contains(" LIMIT "),
        "Fabric output should use TOP instead of LIMIT; got: {sql}"
    );
}

#[test]
fn tpch_query2_transpile_succeeds_on_4mb_thread() {
    let handle = std::thread::Builder::new()
        .name("tpch-q2-constrained".to_string())
        .stack_size(4 * 1024 * 1024)
        .spawn(|| transpile(TPCH_QUERY_2, DialectType::PostgreSQL, DialectType::Fabric))
        .expect("spawn 4 MB thread");

    let stmts = handle
        .join()
        .expect("transpile thread must not panic / overflow")
        .expect("transpile must return Ok");
    assert_eq!(stmts.len(), 1);
}

#[cfg(feature = "stacker")]
#[test]
fn deeply_nested_200_levels_transpile_succeeds_on_4mb_thread() {
    let sql = nested_select_star(200);
    let handle = std::thread::Builder::new()
        .name("nested-200".to_string())
        .stack_size(4 * 1024 * 1024)
        .spawn(move || transpile(&sql, DialectType::PostgreSQL, DialectType::Fabric))
        .expect("spawn 4 MB thread");

    let stmts = handle
        .join()
        .expect("deeply nested transpile must not overflow with stacker")
        .expect("transpile must return Ok");
    assert_eq!(stmts.len(), 1);
}
