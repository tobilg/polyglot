use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(200)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(200)], e),
    }
}

fn main() {
    test("SELECT 1 FROM t0 JOIN t0 USING *");
    test("SELECT from + 1 FROM numbers(1)");
    test("SELECT from, 1 FROM numbers(1)");
}
