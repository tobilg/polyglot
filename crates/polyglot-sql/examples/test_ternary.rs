use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // timestamp in ORDER BY with fill
    test("select * from ts order by sensor_id, timestamp with fill from 6 to 10");

    // using ts as column name instead
    test("select * from ts order by sensor_id, ts with fill from 6 to 10");

    // using just timestamp
    test("select * from ts order by timestamp with fill from 6 to 10");

    // fix replace.from
    test("INSERT INTO t (tag_id, replace.from) SELECT 1, 2");
}
