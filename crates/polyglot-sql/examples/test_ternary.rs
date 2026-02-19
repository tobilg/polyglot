use polyglot_sql::{parse, DialectType};

fn test(label: &str, sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(exprs) => println!("OK: {} ({} stmts)", label, exprs.len()),
        Err(e) => println!("ERR: {} -> {}", label, e),
    }
}

fn main() {
    test("format_json", r#"insert into t select * from input() format JSONEachRow {"x" : 1, "y" : "s1"}, {"y" : "s2", "x" : 2}"#);
    test("format_csv", "insert into t select x, y from input() format CSV 1,2");
    test("normal_format", "SELECT 1 FORMAT TabSeparated");
    test("format_values", "INSERT INTO t FORMAT Values (1, 'a'), (2, 'b')");
}
