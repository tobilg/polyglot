use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // 02354: // comments
    test("SELECT parseTimeDelta('1 min 35 sec'); // no-sanitize-coverage");

    // 00834: LIMIT with expressions
    test("SELECT number FROM numbers(10) LIMIT 0 + 1");
    test("SELECT number FROM numbers(10) LIMIT 2 - 1");

    // 02287: EPHEMERAL without expression followed by type
    test("CREATE TABLE test(a UInt8, b EPHEMERAL 'a' String) Engine=MergeTree ORDER BY tuple()");

    // 02560: ntile window
    test("select a, b, ntile(3) over (partition by a order by b rows between unbounded preceding and unbounded following) from(select 1 as a, 2 as b)");

    // 01902: REGEXP as function
    test("CREATE TABLE t1 as t2 ENGINE=Merge(REGEXP('^db'), '^t')");

    // 02493: numeric underscore in various places
    test("SELECT 1_234 + 5_678");
    test("SELECT 1_000.500_000");

    // 03601: SHOW TEMPORARY VIEWS
    test("SHOW TEMPORARY VIEWS");

    // RLIKE as identifier (dictionary name)
    test("SELECT dictGet('test_dict_01902', 'val', toUInt64(1))");

    // 02730: number in FROM position after SETTINGS
    test("select * from numbers(10) settings max_threads=1");

    // 02841: lambda in function arg
    test("SELECT transform(x, (val -> val * 2)) FROM t");
}
