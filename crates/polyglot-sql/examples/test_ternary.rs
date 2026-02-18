use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // from as column name
    test("CREATE TABLE t (from String, val Date32) Engine=Memory");
    test("SELECT from, val FROM t");

    // INT() empty parens
    test("CREATE TEMPORARY TABLE t6 (x INT())");
    test("CREATE TEMPORARY TABLE t7 (x INT() DEFAULT 1)");

    // Time('UTC') with string arg
    test("CREATE TABLE test_time (t Time('UTC')) engine=MergeTree ORDER BY tuple()");
    test("CREATE TABLE test_time64 (t Time64(3, 'UTC')) engine=MergeTree ORDER BY tuple()");

    // JOIN with UUID-like backtick alias
    test("SELECT * FROM (SELECT 1 as a) t JOIN (SELECT 2 as a) `89467d35-77c2-4f82-ae7a-f093ff40f4cd` ON t.a = `89467d35-77c2-4f82-ae7a-f093ff40f4cd`.a");

    // UNDROP TABLE
    test("UNDROP TABLE test_table");
}
