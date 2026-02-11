#![no_main]

use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;
use polyglot_sql::dialects::DialectType;
use polyglot_sql::transpile;

#[derive(Arbitrary, Debug)]
struct TranspileInput {
    sql: String,
    source_dialect: u8,
    target_dialect: u8,
}

fn get_dialect(index: u8) -> DialectType {
    match index % 6 {
        0 => DialectType::Generic,
        1 => DialectType::PostgreSQL,
        2 => DialectType::MySQL,
        3 => DialectType::BigQuery,
        4 => DialectType::Snowflake,
        _ => DialectType::DuckDB,
    }
}

fuzz_target!(|input: TranspileInput| {
    let source = get_dialect(input.source_dialect);
    let target = get_dialect(input.target_dialect);

    // The transpile function should never panic, regardless of input
    let _ = transpile(&input.sql, source, target);
});
