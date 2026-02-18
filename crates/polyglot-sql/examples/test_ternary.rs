use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // PASTE JOIN
    test("SELECT 1 FROM t0 PASTE JOIN (SELECT 1 c0) tx PASTE JOIN t0 t1 GROUP BY tx.c0");

    // WITH in subquery of CREATE VIEW
    test("CREATE VIEW v AS (WITH RECURSIVE 42 as ttt SELECT ttt)");
    test("CREATE MATERIALIZED VIEW v TO dst AS (WITH (SELECT 1) AS x SELECT x)");

    // SHOW CREATE ROLE/POLICY/QUOTA with multiple names / ON clause
    test("SHOW CREATE ROLE r1, r2");
    test("SHOW CREATE QUOTA q1, q2");
    test("SHOW CREATE SETTINGS PROFILE s1, s2");
    test("SHOW CREATE ROW POLICY p1 ON db.table");
    test("SHOW CREATE POLICY p1 ON db.table");

    // SET DEFAULT ROLE
    test("SET DEFAULT ROLE ALL EXCEPT r1 TO u1");

    // grouping as identifier
    test("SELECT grouping, item FROM (SELECT number % 6 AS grouping, number AS item FROM system.numbers LIMIT 30)");

    // ALTER TABLE multi-action
    test("ALTER TABLE t ADD COLUMN c Int64, MODIFY SETTING check_delay=5");

    // ALTER TABLE STATISTICS
    test("ALTER TABLE tab ADD STATISTICS f64, f32 TYPE tdigest, uniq");
    test("ALTER TABLE tab DROP STATISTICS f64, f32");

    // sum(ALL number)
    test("SELECT sum(ALL number) FROM numbers(10)");
    test("SELECT repeat(ALL, 5) FROM (SELECT 'a' AS ALL)");
}
