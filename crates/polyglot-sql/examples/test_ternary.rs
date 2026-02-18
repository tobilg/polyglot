use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_) => println!("OK: {}", sql),
        Err(e) => println!("ERR: {} -> {}", sql, e),
    }
}

fn main() {
    // LIMIT BY inside various subquery contexts
    test("SELECT * FROM (SELECT * FROM t ORDER BY x LIMIT 1 BY y LIMIT 10) AS sub");
    test("SELECT (SELECT x FROM t LIMIT 1 BY y)");
    test("WITH t AS (SELECT * FROM s LIMIT 2 BY x LIMIT 5) SELECT * FROM t");

    // LIMIT BY followed by OFFSET
    test("SELECT * FROM t LIMIT 10 BY x LIMIT 5 OFFSET 3");

    // ORDER BY ... LIMIT BY
    test("SELECT * FROM t ORDER BY a LIMIT 1 BY b LIMIT 10 OFFSET 0");

    // aggregate function with * as arg
    test("SELECT ignore(*, col1, col2)");

    // CAST(tuple AS type)
    test("SELECT CAST((1, 'Hello', toDate('2016-01-01')) AS Tuple(Int32, String, Date))");
    test("SELECT CAST((1, 2) AS String)");

    // ClickHouse: EXPLAIN with options
    test("EXPLAIN SYNTAX SELECT 1");
    test("EXPLAIN AST SELECT 1");
    test("EXPLAIN PIPELINE SELECT 1");

    // Ternary inside CAST
    test("CAST(number = 999999 ? NULL : number AS Nullable(UInt64))");
}
