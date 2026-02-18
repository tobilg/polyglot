use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // grouping as identifier
    test("SELECT grouping, item, runningAccumulate(state, grouping) FROM (SELECT 1 AS grouping, 2 AS item, sumState(3) AS state)");

    // overlay as regular function
    test("SELECT overlay('Spark SQL', '_', 6)");
    test("SELECT overlay('hello', 'world', 2, 3, 'extra')");

    // ORDER BY () empty tuple
    test("CREATE TABLE t (c0 String) ENGINE = MergeTree() ORDER BY ()");

    // key as identifier in INDEX
    test("CREATE TABLE data (key String, INDEX idx key TYPE bloom_filter) ENGINE = MergeTree ORDER BY key");

    // CAST(expr, 'Type') form
    test("SELECT CAST(123, 'String')");
    test("SELECT CAST(123, 'Str' || 'ing')");

    // EXPLAIN (parenthesized query)
    test("EXPLAIN (SELECT 1 UNION ALL SELECT 2)");

    // FORMAT data after INSERT (should consume the rest as raw data)
    test("INSERT INTO test FORMAT JSONEachRow {\"x\": 1}");

    // hex float literals
    test("SELECT 0x1p10");
    test("SELECT 0x1.fp10");

    // negative tuple index
    test("SELECT (1, 2, 3).-1");

    // FROM SELECT syntax
    test("FROM numbers(1) SELECT number");
}
