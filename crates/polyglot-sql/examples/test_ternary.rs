use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // REGEXP/RLIKE as column name
    test("CREATE TABLE t (id UInt64, regexp String) ENGINE=TinyLog");
    test("SELECT regexp FROM t");

    // EXISTS as column name in UPDATE SET
    test("UPDATE t SET exists = 1 WHERE 1");

    // TABLE as identifier after dot in INSERT INTO
    test("INSERT INTO test_01676.table (x) VALUES (2)");

    // ALTER TABLE multi-action with MODIFY SETTING commas
    test("ALTER TABLE t ADD COLUMN Data2 UInt64, MODIFY SETTING check_delay_period=5, check_delay_period=10, check_delay_period=15");

    // ALTER TABLE ADD COLUMN and then ADD INDEX
    test("ALTER TABLE t ADD COLUMN c Int64, ADD INDEX idx c TYPE minmax GRANULARITY 1");

    // MODIFY STATISTICS
    test("ALTER TABLE tab MODIFY STATISTICS f64, f32 TYPE tdigest, uniq");
    test("ALTER TABLE tab CLEAR STATISTICS f64, f32");
    test("ALTER TABLE tab MATERIALIZE STATISTICS f64, f32");
}
