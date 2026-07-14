use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use polyglot_sql::dialects::{Dialect, DialectType};
use polyglot_sql::parser::{Parser, ParserConfig};
use polyglot_sql::ComplexityGuardOptions;
use std::time::Duration;

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

const TPCH_STYLE: &str = r#"
WITH regional_sales AS (
    SELECT n.region_id, SUM(o.total_price) AS revenue
    FROM orders o
    JOIN customers c ON c.id = o.customer_id
    JOIN nations n ON n.id = c.nation_id
    WHERE o.created_at >= DATE '2024-01-01'
    GROUP BY n.region_id
), ranked AS (
    SELECT region_id, revenue, RANK() OVER (ORDER BY revenue DESC) AS position
    FROM regional_sales
)
SELECT r.name, ranked.revenue
FROM ranked
JOIN regions r ON r.id = ranked.region_id
WHERE ranked.position <= 10
ORDER BY ranked.revenue DESC
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

fn disabled_guards() -> ComplexityGuardOptions {
    ComplexityGuardOptions {
        max_input_bytes: None,
        max_tokens: None,
        max_ast_nodes: None,
        max_ast_depth: None,
        max_parenthesis_depth: None,
        max_function_call_depth: None,
    }
}

fn bench_dialect_construction(c: &mut Criterion) {
    let dialects = [
        ("generic", DialectType::Generic),
        ("postgresql", DialectType::PostgreSQL),
        ("bigquery", DialectType::BigQuery),
        ("clickhouse", DialectType::ClickHouse),
    ];

    for (_, dialect) in dialects {
        black_box(Dialect::get(dialect));
    }

    let mut group = c.benchmark_group("dialect_get_warm");
    for (name, dialect) in dialects {
        group.bench_with_input(BenchmarkId::from_parameter(name), &dialect, |b, dialect| {
            b.iter(|| black_box(Dialect::get(black_box(*dialect))))
        });
    }
    group.bench_function("representative_set", |b| {
        b.iter(|| {
            for (_, dialect) in dialects {
                black_box(Dialect::get(black_box(dialect)));
            }
        })
    });
    group.finish();
}

fn bench_fresh_vs_reused_dialect(c: &mut Criterion) {
    let reused = Dialect::get(DialectType::PostgreSQL);
    let mut group = c.benchmark_group("parse_dialect_lifecycle");
    group.bench_function("reused", |b| {
        b.iter(|| reused.parse(black_box(SHORT_ASCII)).unwrap())
    });
    group.bench_function("fresh", |b| {
        b.iter(|| {
            Dialect::get(black_box(DialectType::PostgreSQL))
                .parse(black_box(SHORT_ASCII))
                .unwrap()
        })
    });
    group.finish();
}

fn bench_tokenize_and_parse(c: &mut Criterion) {
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
        ("tpch_style", TPCH_STYLE),
        ("large_tokens", large.as_str()),
        ("many_columns", columns.as_str()),
        ("nested_functions", functions.as_str()),
        ("large_strings", strings.as_str()),
        ("many_numbers", numbers.as_str()),
    ];

    let mut tokenize = c.benchmark_group("tokenize_hotspots");
    tokenize.sample_size(20);
    tokenize.measurement_time(Duration::from_secs(4));
    for (name, sql) in inputs {
        tokenize.bench_with_input(BenchmarkId::from_parameter(name), &sql, |b, sql| {
            b.iter(|| dialect.tokenize(black_box(sql)).unwrap())
        });
    }
    tokenize.finish();

    let mut parse = c.benchmark_group("parse_hotspots");
    parse.sample_size(20);
    parse.measurement_time(Duration::from_secs(4));
    for (name, sql) in inputs {
        parse.bench_with_input(BenchmarkId::from_parameter(name), &sql, |b, sql| {
            b.iter(|| dialect.parse(black_box(sql)).unwrap())
        });
    }
    parse.finish();

    let tokens = dialect.tokenize(numbers.as_str()).unwrap();
    let mut guards = c.benchmark_group("parse_guard_overhead");
    guards.sample_size(20);
    guards.measurement_time(Duration::from_secs(4));
    guards.bench_function("default", |b| {
        b.iter(|| {
            Parser::with_source(
                black_box(tokens.clone()),
                ParserConfig {
                    dialect: Some(DialectType::PostgreSQL),
                    ..Default::default()
                },
                numbers.clone(),
            )
            .parse()
            .unwrap()
        })
    });
    guards.bench_function("disabled", |b| {
        b.iter(|| {
            Parser::with_source(
                black_box(tokens.clone()),
                ParserConfig {
                    dialect: Some(DialectType::PostgreSQL),
                    complexity_guard: disabled_guards(),
                    ..Default::default()
                },
                numbers.clone(),
            )
            .parse()
            .unwrap()
        })
    });
    guards.finish();
}

criterion_group!(
    benches,
    bench_dialect_construction,
    bench_fresh_vs_reused_dialect,
    bench_tokenize_and_parse
);
criterion_main!(benches);
