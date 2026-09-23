use polyglot_sql::dialects::{Dialect, DialectType};
use stats_alloc::{Region, Stats, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;
use std::hint::black_box;

mod common;

use common::{
    large_strings, large_token_list, many_columns, many_numbers, nested_functions,
    COMMENT_AND_STRING_HEAVY, SHORT_ASCII, UNICODE,
};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn measure<T>(operation: impl FnOnce() -> T) -> Stats {
    let region = Region::new(GLOBAL);
    black_box(operation());
    region.change()
}

fn print_stats(case: &str, operation: &str, input_bytes: usize, stats: Stats) {
    println!(
        "{{\"case\":\"{case}\",\"operation\":\"{operation}\",\"input_bytes\":{input_bytes},\"allocations\":{},\"deallocations\":{},\"bytes_allocated\":{},\"bytes_deallocated\":{}}}",
        stats.allocations,
        stats.deallocations,
        stats.bytes_allocated,
        stats.bytes_deallocated,
    );
}

fn main() {
    let dialect = Dialect::get(DialectType::PostgreSQL);
    let large = large_token_list();
    let columns = many_columns();
    let functions = nested_functions();
    let strings = large_strings();
    let numbers = many_numbers();
    let generic_functions = format!(
        "SELECT {} FROM t",
        (0..1_000)
            .map(|i| format!("my_udf(c{i}) AS v{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let inputs = [
        ("short_ascii", SHORT_ASCII),
        ("unicode", UNICODE),
        ("comments_strings", COMMENT_AND_STRING_HEAVY),
        ("large_tokens", large.as_str()),
        ("many_columns", columns.as_str()),
        ("nested_functions", functions.as_str()),
        ("large_strings", strings.as_str()),
        ("many_numbers", numbers.as_str()),
        ("generic_functions", generic_functions.as_str()),
    ];

    black_box(dialect.tokenize(SHORT_ASCII).unwrap());
    black_box(dialect.parse(SHORT_ASCII).unwrap());
    black_box(Dialect::get(DialectType::PostgreSQL));

    for (case, sql) in inputs {
        print_stats(
            case,
            "tokenize",
            sql.len(),
            measure(|| dialect.tokenize(sql).unwrap()),
        );
        print_stats(
            case,
            "parse",
            sql.len(),
            measure(|| dialect.parse(sql).unwrap()),
        );
    }

    print_stats(
        "postgresql",
        "dialect_get",
        0,
        measure(|| Dialect::get(DialectType::PostgreSQL)),
    );

    for depth in [10, 20, 40, 80] {
        let mut sql = "SELECT 1".to_owned();
        for _ in 0..depth {
            sql = format!("SELECT ({sql})");
        }
        for kind in [DialectType::PostgreSQL, DialectType::HANA] {
            let dialect = Dialect::get(kind);
            black_box(dialect.parse(&sql).unwrap());
            print_stats(
                &format!("nested_{depth}_{kind}"),
                "parse",
                sql.len(),
                measure(|| dialect.parse(&sql).unwrap()),
            );
        }
    }

    #[cfg(feature = "transpile")]
    {
        let options = polyglot_sql::TranspileOptions::strict();
        for (name, sql) in common::transpilation_queries() {
            let run =
                || polyglot_sql::transpile_with_by_name(&sql, "hana", "duckdb", &options).unwrap();
            black_box(run());
            print_stats(
                &format!("hana_{name}"),
                "transpile",
                sql.len(),
                measure(run),
            );
        }
    }
}
