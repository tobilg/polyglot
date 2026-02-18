use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // GROUP BY grouping (grouping as column name)
    test("SELECT 1 FROM t GROUP BY grouping");
    test("SELECT grouping FROM t GROUP BY grouping, item ORDER BY grouping, item");

    // GROUPING SETS should still work
    test("SELECT 1 FROM t GROUP BY GROUPING SETS ((a), (b))");

    // ntile with multiple args
    test("SELECT ntile(3, 2) OVER (ORDER BY a)");

    // EXTRACT with comma-separated args
    test("SELECT extract(year, date_col) FROM t");

    // * IS NOT NULL
    test("SELECT * IS NOT NULL FROM t");

    // INDEX with expression in CREATE TABLE
    test("CREATE TABLE t (c0 Int, PROJECTION p (SELECT c0 ORDER BY c0), INDEX idx c0 TYPE bloom_filter) ENGINE = MergeTree ORDER BY c0");
}
