use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_) => println!("OK: {}", sql),
        Err(e) => println!("ERR: {} -> {}", sql, e),
    }
}

fn main() {
    // ALTER TABLE MODIFY SETTING
    test("ALTER TABLE t MODIFY SETTING aaa=123");
    test("ALTER TABLE t RESET SETTING aaa");

    // ARRAY JOIN with semicolon (empty)
    test("SELECT x FROM t ARRAY JOIN arr AS a");

    // union as table name (keywords as identifiers)
    test("DROP TABLE IF EXISTS union");
    test("SELECT * FROM union ORDER BY test");

    // EXPRESSION in dictionary CREATE
    test("CREATE DICTIONARY dict (key UInt64, val String EXPRESSION toString(key)) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'tab')) LAYOUT(FLAT()) LIFETIME(0)");

    // insert into with SELECT without parens
    test("insert into t values(1), (100)");

    // SELECT with modulo
    test("SELECT number FROM numbers(10) LIMIT (number % 2)");

    // WITH FILL
    test("SELECT * FROM t ORDER BY x WITH FILL FROM 0 TO 10 STEP 1");

    // REFRESH MATERIALIZED VIEW
    test("CREATE MATERIALIZED VIEW v0 REFRESH AFTER 1 SECOND APPEND TO t0 AS SELECT 1");

    // DIV operator
    test("SELECT 10 DIV 3");

    // LARGE OBJECT type
    test("SELECT CAST(x AS CHARACTER LARGE OBJECT)");

    // EXCEPT/INTERSECT after subquery
    test("SELECT 1 EXCEPT SELECT 2");
    test("SELECT 1 INTERSECT SELECT 2");

    // Double-colon cast
    test("SELECT x::UInt64 FROM t");
}
