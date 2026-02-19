use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(150)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(150)], e),
    }
}

fn main() {
    // Projection WITH SETTINGS in CREATE TABLE column list
    test("CREATE TABLE t(x UInt64, y String, PROJECTION p1 (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 2)) ENGINE = MergeTree() ORDER BY x");

    // DROP TEMPORARY VIEW
    test("DROP TEMPORARY VIEW IF EXISTS tview_basic");

    // CREATE TEMPORARY VIEW INNER ENGINE (intentional error test - should tolerate it)
    test("CREATE TEMPORARY VIEW tv_inner INNER ENGINE = Memory AS SELECT 1");

    // Negative index on column
    test("SELECT _partition_value.-1 FROM a1");

    // 02681 - UNDROP ON CLUSTER
    test("undrop table t1 uuid '1234-5678' on cluster test_shard_localhost");

    // 01556 - test multiple EXPLAIN lines
    test("EXPLAIN SELECT 1 UNION ALL SELECT 1");
    test("EXPLAIN (SELECT 1 UNION ALL SELECT 1) UNION ALL SELECT 1");
    test("EXPLAIN SELECT 1 UNION (SELECT 1 UNION ALL SELECT 1)");

    // 01604 EXPLAIN AST non-SELECT
    test("explain ast alter table t1 delete where date = today()");
    test("explain ast create function double AS (n) -> 2*n");

    // 02339 DESCRIBE (SELECT)
    test("DESCRIBE (SELECT *)");
    test("DESCRIBE TABLE t");

    // 02343 CREATE TABLE EMPTY
    test("create table t engine=Memory empty");
    test("create table t engine=Memory empty as select 1");

    // Check the 03789 RMV
    test("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 MONTH APPEND TO target AS WITH (SELECT 1) AS x, (SELECT 2) AS y SELECT * FROM t");

    // 03567 comparison in function
    test("SELECT if(length(v) as bs < 1000, 'ok', toString(bs))");

    // 03595 IS NOT NULL with star
    test("SELECT *, * IS NOT NULL FROM t");
}
