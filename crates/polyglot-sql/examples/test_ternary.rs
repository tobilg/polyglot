use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_) => println!("OK: {}", sql),
        Err(e) => println!("ERR: {} -> {}", sql, e),
    }
}

fn main() {
    // CHECK constraint without parens
    test("CREATE TABLE t (a UInt32, b UInt32, CONSTRAINT a_constraint CHECK a < 10) ENGINE = Memory");
    test("CREATE TABLE t (URL String, CONSTRAINT is_censor CHECK domainWithoutWWW(URL) = 'censor.net') ENGINE = Null");

    // EXTRACT as regular function
    test("SELECT extract(toString(number), '10000000') FROM system.numbers");

    // Column defaults with == and complex expressions
    test("CREATE TABLE t (a UInt64, test1 ALIAS zoneId == 1, test2 DEFAULT zoneId * 3, test3 MATERIALIZED zoneId * 5) ENGINE = MergeTree ORDER BY a");

    // Enum with negative values
    test("CREATE TABLE t (x Enum8('a' = -1000, 'b' = 0)) ENGINE = Memory");

    // Decimal with negative scale
    test("CREATE TABLE t (x DECIMAL(10, -2)) ENGINE = Memory");

    // DROP PARTITION
    test("DROP PARTITION 201901");
}
