use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // SETTINGS in function calls
    test("DESC format(JSONEachRow, '{}' SETTINGS schema_inference_hints='age UInt8')");
    test("SELECT * FROM mysql('host', 'db', 'table', 'user', 'pass' SETTINGS connect_timeout=10)");

    // IGNORE NULLS postfix
    test("SELECT count(NULL) IGNORE NULLS");
    test("SELECT any(x) RESPECT NULLS FROM t");

    // Tuple index access
    test("SELECT row.1, row.2 FROM t");
    test("WITH (1,2) AS t SELECT t.1");

    // DROP WORKLOAD/PROFILE
    test("DROP WORKLOAD IF EXISTS production");
    test("DROP PROFILE IF EXISTS s1");

    // Qualified star with EXCEPT
    test("SELECT system.detached_parts.* EXCEPT (bytes_on_disk, path) FROM system.detached_parts");
    test("SELECT t.COLUMNS('^c') EXCEPT (col1, col2) FROM t");

    // UNION ALL with WITH CTE
    test("SELECT 1 UNION ALL WITH 2 AS x SELECT x");

    // FLOAT(precision, scale) cast
    test("SELECT inf::FLOAT(15,22)");
}
