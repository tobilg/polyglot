use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(200)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(200)], e),
    }
}

fn main() {
    // Star APPLY/EXCEPT/REPLACE
    test("SELECT * APPLY(toDate) EXCEPT(i, j) APPLY(any) from t");
    test("SELECT a.* APPLY(toDate) EXCEPT(i, j) APPLY(any) from t a");
    test("SELECT * EXCEPT(id) REPLACE(5 AS value) FROM t");
    test("SELECT a.* EXCEPT(id) FROM t a");
    test("SELECT * APPLY(toString) FROM t");
    test("SELECT a.* APPLY(toString) FROM t a");

    // COLUMNS function with transformers
    test("SELECT COLUMNS(id, value) REPLACE (5 AS id) FROM t");
    test("SELECT COLUMNS(id) REPLACE (5 AS id) FROM t");
    test("SELECT COLUMNS('pattern') EXCEPT (col1) FROM t");
    test("SELECT COLUMNS(id, value) APPLY(toString) FROM t");

    // Basic queries should still work
    test("SELECT 1");
    test("SELECT a, b FROM t");
    test("SELECT a.b FROM t");
    test("SELECT * FROM t");
}
