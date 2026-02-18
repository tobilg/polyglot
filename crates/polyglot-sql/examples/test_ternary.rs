use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // DIV keyword
    test("SELECT number DIV 2, number FROM numbers(3)");
    test("SELECT number MOD 2, number FROM numbers(3)");

    // DESC format()
    test("DESC format(Values, '(123)')");
    test("DESCRIBE format(CSV, '1,2,3')");

    // INSERT VALUES without commas between tuples
    test("INSERT INTO t VALUES (1), (2) (3), (4)");
    test("INSERT INTO t VALUES (1, 2, 3) (4, 5, 6)");

    // INSERT FORMAT - raw data should be skipped
    test("INSERT INTO t FORMAT JSONEachRow");

    // STALENESS in WITH FILL
    test("SELECT a FROM t ORDER BY a WITH FILL STALENESS 3");
    test("SELECT a FROM t ORDER BY a WITH FILL STALENESS INTERVAL 2 SECOND, b WITH FILL");

    // EXCEPT STRICT
    test("SELECT * EXCEPT STRICT i, j FROM t");

    // table.* APPLY
    test("SELECT t.* APPLY toString FROM t");

    // LIMIT offset, count after LIMIT BY
    test("SELECT * FROM t LIMIT 1 BY number LIMIT 5, 5");
}
