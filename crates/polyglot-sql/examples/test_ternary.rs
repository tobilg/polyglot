use polyglot_sql::{parse, DialectType};

fn test(label: &str, sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(exprs) => println!("OK: {} ({} stmts)", label, exprs.len()),
        Err(e) => println!("ERR: {} -> {}", label, e),
    }
}

fn main() {
    // Normal EXTRACT
    test("e1", "SELECT EXTRACT(DAY FROM toDate('2019-05-05'))");
    test("e2", "SELECT EXTRACT(YEAR FROM now())");
    // ClickHouse function-style extract
    test("e3", "SELECT extract('1234', '123')");
    test("e4", "SELECT extract('1234' arg_1, '123' arg_2), arg_1, arg_2");
    // Normal CAST
    test("c1", "SELECT cast('1234' AS UInt32)");
    test("c2", "SELECT cast(x AS DateTime('UTC'))");
    // Normal SUBSTRING
    test("s1", "SELECT substring('hello' FROM 2 FOR 3)");
    test("s2", "SELECT substring('hello', 2, 3)");
    // Normal TRIM
    test("t1", "SELECT trim(BOTH ' ' FROM '  hello  ')");
    test("t2", "SELECT trim('  hello  ')");
    // Normal DATEADD/DATEDIFF
    test("d1", "SELECT dateAdd(DAY, 1, now())");
    test("d2", "SELECT dateDiff(DAY, now(), now())");
}
