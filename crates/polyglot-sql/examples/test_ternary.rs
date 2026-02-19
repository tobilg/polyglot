use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(200)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(200)], e),
    }
}

fn main() {
    // Lambda without parens - single param (pseudocolumn issue)
    test("SELECT level -> least(1.0, greatest(-1.0, level))");
    test("WITH level -> least(1.0, greatest(-1.0, level)) AS clamp SELECT clamp(0.5)");
    test("WITH 1 AS master_volume, level -> least(1.0, greatest(-1.0, level)) AS clamp SELECT clamp(0.5)");

    // INSERT INTO t VALUES without parens
    test("INSERT INTO t VALUES 1");
    test("INSERT INTO FUNCTION s3('url') VALUES 1");
    // Regular INSERT should still work
    test("INSERT INTO t VALUES (1, 2)");
    test("INSERT INTO t VALUES (1), (2), (3)");
}
