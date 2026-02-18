use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // AS alias inside function args
    test("SELECT format('CSV' AS format, '1,2,3' AS format_value)");
    test("SELECT arrayMap((x -> toString(x)) as lambda, [1,2,3])");
    test("SELECT toTypeName(quantilesExactWeightedState(0.2, 0.4)(number + 1, 1) AS x)");

    // SETTINGS in table function args
    test("SELECT * FROM executable('', 'JSON', 'data String', SETTINGS max_command_execution_time=100)");
    test("SELECT * FROM mysql('127.0.0.1:9004', 'default', 'atable', 'default', '', SETTINGS connect_timeout = 100)");

    // ON CLUSTER
    test("CREATE DATABASE IF NOT EXISTS test ON CLUSTER test_shard_localhost");

    // USING (col AS alias)
    test("SELECT * FROM system.one l INNER JOIN numbers(1) r USING (dummy AS number)");
}
