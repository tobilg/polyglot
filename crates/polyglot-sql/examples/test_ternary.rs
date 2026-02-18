use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // Double semicolons
    test("SELECT 1;; -- comment");

    // INDEX in MV schema
    test("CREATE MATERIALIZED VIEW mv (key String, INDEX idx key TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY key AS SELECT * FROM data");

    // PROJECTION in schema
    test("CREATE MATERIALIZED VIEW mv (key String, PROJECTION p (SELECT uniqCombined(key))) ENGINE = MergeTree ORDER BY key AS SELECT * FROM data");

    // PROJECTION with INDEX in CREATE TABLE (03460)
    test("CREATE TABLE t (region String, INDEX i1 region TYPE bloom_filter, PROJECTION region_proj (SELECT region ORDER BY region)) ENGINE = MergeTree ORDER BY region");

    // Grouping still works
    test("SELECT 1 FROM t GROUP BY grouping, item ORDER BY grouping");
    test("SELECT 1 FROM t GROUP BY GROUPING SETS ((a), (b))");
}
