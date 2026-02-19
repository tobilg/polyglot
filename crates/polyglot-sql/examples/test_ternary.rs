use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(200)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(200)], e),
    }
}

fn main() {
    // WITH tuple CTE
    test("WITH ((SELECT 1) AS x, (SELECT 2) AS y) SELECT x, y");
    test("WITH ((SELECT query_start_time_microseconds FROM system.query_log) AS t1, (SELECT query_start_time FROM system.query_log) AS t2) SELECT t1, t2");

    // Simple WITH
    test("WITH 1 AS n SELECT n");
    test("WITH (SELECT 1) AS n SELECT n");

    // AND as function
    test("SELECT NOT ((SELECT * AND(16)) AND 1)");
    test("SELECT * FROM AND(16)");

    // * in JOIN ON
    test("SELECT 1 FROM t0 JOIN t0 ON *");

    // * IS NOT NULL
    test("SELECT *, * IS NOT NULL FROM t");

    // Trailing comma in Tuple type
    test("SELECT (1, 'foo')::Tuple(a Int, b String,)");
    test("SELECT (1, (2,'foo'))::Tuple(Int, Tuple(Int, String,),)");

    // Trailing comma in SELECT
    test("SELECT 1,");
    test("SELECT 1, FROM numbers(1)");
}
