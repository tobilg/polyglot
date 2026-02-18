use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // except as column name
    test("SELECT source, except, arrayExcept(source, except) FROM t");

    // ALIAS column modifier
    test("CREATE VIEW v (dummy Int, n ALIAS dummy) AS SELECT * FROM system.one");

    // Trailing comma in VALUES
    test("INSERT INTO t VALUES (1, 2),");
}
