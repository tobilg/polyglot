use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // DESC/DESCRIBE ... SETTINGS key=val, key=val
    test("desc format(CSV, '1,\"String\"') settings schema_inference_hints='x UInt8', column_names_for_schema_inference='x, y'");

    // ALTER TABLE ... SETTINGS key=val, key=val
    test("alter table t add column c Int64 settings mutations_sync=2, alter_sync=2");

    // Keywords as identifiers: FROM
    test("WITH 1 as from SELECT from, from + from");
    test("SELECT from, val FROM test_date32_casts");

    // Keywords as identifiers: GROUPING
    test("SELECT grouping, item, runningAccumulate(state, grouping) FROM t");

    // Keywords as identifiers: DISTINCT
    test("SELECT repeat(DISTINCT, 5) FROM (SELECT 'a' AS DISTINCT)");

    // Keywords as identifiers: DIV, MOD
    test("SELECT DIV FROM (SELECT 1 AS DIV)");

    // Keywords as identifiers: EXCEPT in table name
    test("CREATE TABLE array_except1 (a Array(Int32)) ENGINE=Memory");

    // Ternary operator
    test("SELECT empty(x) ? 'yes' : 'no' FROM t");

    // UNDROP TABLE
    test("UNDROP TABLE t");

    // Tuple element access with number: t.1
    test("SELECT toDateTime(time.1) FROM t");

    // COLUMNS transformer
    test("SELECT * APPLY(toDate) FROM t");
    test("SELECT COLUMNS('id|value') EXCEPT (id) FROM t");
}
