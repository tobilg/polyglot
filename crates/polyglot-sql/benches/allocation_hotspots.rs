use polyglot_sql::dialects::{Dialect, DialectType};
use stats_alloc::{Region, Stats, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;
use std::hint::black_box;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const SHORT_ASCII: &str =
    "SELECT a, b, SUM(c) AS total FROM events WHERE created_at >= '2025-01-01' GROUP BY a, b";
const UNICODE: &str =
    "SELECT \"Kundennummer\", 'Gr\u{00fc}\u{00df}e aus Z\u{00fc}rich' AS \"Mitteilung\" FROM \"Bestellungen\" WHERE \"Stadt\" = 'M\u{00fc}nchen'";
const COMMENT_AND_STRING_HEAVY: &str = r#"
-- leading comment
SELECT 'alpha''beta' AS value, "quoted name", E'line\nvalue'
FROM events /* source comment */
WHERE payload = '{"key":"value"}' -- trailing comment
"#;

fn large_token_list() -> String {
    let values = (0..20_000)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT * FROM events WHERE event_id IN ({values})")
}

fn many_columns() -> String {
    format!(
        "SELECT {} FROM t",
        (0..1_000)
            .map(|index| format!("c{index}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn nested_functions() -> String {
    format!(
        "SELECT {}x{} FROM t",
        "COALESCE(".repeat(20),
        ", NULL)".repeat(20)
    )
}

fn large_strings() -> String {
    format!(
        "SELECT {} FROM t",
        vec![format!("'{}'", "x".repeat(100)); 500].join(", ")
    )
}

fn many_numbers() -> String {
    format!(
        "SELECT {} FROM t",
        (0..10_000)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

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
    let inputs = [
        ("short_ascii", SHORT_ASCII),
        ("unicode", UNICODE),
        ("comments_strings", COMMENT_AND_STRING_HEAVY),
        ("large_tokens", large.as_str()),
        ("many_columns", columns.as_str()),
        ("nested_functions", functions.as_str()),
        ("large_strings", strings.as_str()),
        ("many_numbers", numbers.as_str()),
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
}
