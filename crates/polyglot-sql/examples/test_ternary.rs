use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(200)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(200)], e),
    }
}

fn main() {
    // Lambda in WITH with keyword as parameter
    test("WITH time -> sin(time * 2) AS sine_wave SELECT sine_wave");
    test("WITH 1 AS master_volume, level -> least(1.0, greatest(-1.0, level)) AS clamp, time -> sin(time * 2 * 3.14159) AS sine_wave SELECT sine_wave");

    // Lambda with various keyword params
    test("WITH x -> (x, x) AS mono SELECT mono(1)");
    test("WITH (from, to, wave, time) -> from + ((wave(time) + 1) / 2) * (to - from) AS lfo SELECT lfo(1,2,3,4)");

    // Lambda inside parentheses
    test("SELECT f((time -> sine_wave(time * 50)))");

    // Standard CTE should still work
    test("WITH t AS (SELECT 1) SELECT * FROM t");
    test("WITH 42 AS n SELECT n");
}
