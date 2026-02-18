use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_) => println!("OK: {}", sql),
        Err(e) => println!("ERR: {} -> {}", sql, e),
    }
}

fn main() {
    // Test individual parts
    test("select minus(1, 2) from t");
    test("select minus(c1 = 1, c1 = 2) from t");
    test("select minus(c1 = 1 or c1 = 2, c1 = 5) from t");
    test("select minus(c1 = 1 or c1=2 or c1 =3, c1=5) from orin_test");

    // VALUES with no comma between tuples (user error)
    test("INSERT INTO t VALUES (1), (1), (2), (2), (2), (2) (3), (3)");

    // The error #8 test case
    test("insert into orin_test values(1), (100)");
}
