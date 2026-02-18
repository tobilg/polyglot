use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_) => println!("OK: {}", sql),
        Err(e) => println!("ERR: {} -> {}", sql, e),
    }
}

fn main() {
    // WITH TOTALS without GROUP BY
    test("SELECT count() FROM t WITH TOTALS");
    test("SELECT 1 GROUP BY 1 WITH TOTALS");

    // Trailing comma in tuples
    test("SELECT (1,)");
    test("SELECT toTypeName((1,)), (1,)");

    // AS alias inside function args
    test("SELECT lower('aaa' as str) = str");
    test("SELECT position('' as h, '' as n)");

    // AS alias inside array literals
    test("SELECT has([0 as x], x)");

    // CREATE TABLE AS SELECT
    test("CREATE TABLE t (x String) ENGINE = MergeTree ORDER BY x AS SELECT 'Hello'");

    // Existing working features
    test("SELECT 1");
    test("SELECT 1 ? 2 : 3");
}
