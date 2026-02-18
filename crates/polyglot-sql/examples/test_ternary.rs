use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // LIMIT in subquery with modulo
    test("SELECT count() FROM (SELECT number FROM numbers(10) LIMIT randConstant() % 2)");
    test("SELECT count() FROM (SELECT number FROM numbers(10) LIMIT 1 % 2)");

    // PRIMARY KEY key in schema
    test("CREATE MATERIALIZED VIEW mv (key String, PRIMARY KEY key) ENGINE = MergeTree ORDER BY key AS SELECT * FROM data");

    // PROJECTION INDEX syntax
    test("CREATE TABLE t (id UInt64, PROJECTION region_proj INDEX region TYPE basic) ENGINE = MergeTree ORDER BY id");

    // PRIMARY KEY (t.a)
    test("CREATE TABLE test (t Tuple(a Int32)) ENGINE = EmbeddedRocksDB() PRIMARY KEY (t.a)");

    // INDEX with comparison
    test("CREATE TABLE t0 (c0 String, INDEX i0 c0 < (SELECT _table) TYPE minmax) ENGINE = MergeTree() ORDER BY tuple()");

    // CONSTRAINT CHECK (SELECT)
    test("ALTER TABLE t ADD CONSTRAINT c0 CHECK (SELECT 1)");

    // ARRAY JOIN no args
    test("SELECT x, a FROM (SELECT 1 AS x, [1] AS arr) ARRAY JOIN");

    // EXECUTE AS
    test("EXECUTE AS test_user ALTER TABLE normal UPDATE s = 'x' WHERE n=1");

    // Inline alias in function args (not yet fixed)
    test("SELECT countIf(toDate('2000-12-05') + number as d, toDayOfYear(d) % 2) FROM numbers(100)");

    // SELECT * AND(16) in subquery (not yet fixed)
    test("SELECT not((SELECT * AND(16)) AND 1)");

    // DuckDB LIMIT PERCENT should still work
    test("SELECT * FROM t LIMIT 50 PERCENT");
}
