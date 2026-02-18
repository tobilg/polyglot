use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // INSERT ... FORMAT with inline data (should parse INSERT and stop before data)
    test("INSERT INTO t FORMAT JSONEachRow {\"x\":1}");
    test("INSERT INTO t FORMAT CSV 1,2,3");

    // Empty IN()
    test("SELECT * FROM t WHERE k2 IN ()");

    // Empty VALUES()
    test("INSERT INTO t VALUES ()");

    // values as CTE name
    test("WITH values AS (SELECT 1) SELECT * FROM values");

    // grouping as identifier (not GROUPING() function)
    test("SELECT grouping, item FROM t");

    // floor with 3 args (ClickHouse allows arbitrary args)
    test("SELECT floor(1, floor(NULL), 257)");

    // DISTINCT in second chained function call: func(5, 11111)(DISTINCT subdomain)
    test("SELECT groupArraySample(5, 11111)(DISTINCT x) FROM t");

    // Backtick identifier without AS: SELECT 1 `DIV`
    test("SELECT 1 `DIV`");
}
