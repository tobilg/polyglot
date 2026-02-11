//! Benchmark that outputs JSON for comparison with Python sqlglot.
//!
//! Run with: cargo run --example bench_json -p polyglot-sql --release

use polyglot_sql::dialects::{Dialect, DialectType};
use polyglot_sql::transpile;
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

const SIMPLE_SELECT: &str = "SELECT a, b, c FROM table1";

const MEDIUM_SELECT: &str = "\
SELECT \
u.id, \
u.name, \
u.email, \
COUNT(o.id) AS order_count, \
SUM(o.total) AS total_spent \
FROM users AS u \
LEFT JOIN orders AS o ON u.id = o.user_id \
WHERE u.created_at > '2024-01-01' AND u.status = 'active' \
GROUP BY u.id, u.name, u.email \
HAVING COUNT(o.id) > 5 \
ORDER BY total_spent DESC \
LIMIT 100";

const COMPLEX_SELECT: &str = "\
WITH active_users AS (\
SELECT u.id, u.name, u.email, u.created_at \
FROM users AS u \
WHERE u.status = 'active' AND u.last_login > CURRENT_DATE - INTERVAL '30 days'\
), \
user_orders AS (\
SELECT o.user_id, COUNT(*) AS order_count, SUM(o.total) AS total_spent, \
AVG(o.total) AS avg_order_value, MAX(o.created_at) AS last_order_date \
FROM orders AS o \
WHERE o.status = 'completed' \
GROUP BY o.user_id\
), \
product_categories AS (\
SELECT DISTINCT p.category_id, c.name AS category_name \
FROM products AS p \
JOIN categories AS c ON p.category_id = c.id \
WHERE p.is_active = TRUE\
) \
SELECT au.id AS user_id, au.name AS user_name, au.email, \
COALESCE(uo.order_count, 0) AS total_orders, \
COALESCE(uo.total_spent, 0) AS lifetime_value, \
COALESCE(uo.avg_order_value, 0) AS average_order, \
uo.last_order_date, \
CASE WHEN uo.total_spent > 10000 THEN 'VIP' \
WHEN uo.total_spent > 1000 THEN 'Premium' \
WHEN uo.total_spent > 100 THEN 'Regular' \
ELSE 'New' END AS customer_tier, \
(SELECT STRING_AGG(pc.category_name, ', ') \
FROM user_orders AS uo2 \
JOIN order_items AS oi ON uo2.user_id = oi.order_id \
JOIN products AS p ON oi.product_id = p.id \
JOIN product_categories AS pc ON p.category_id = pc.category_id \
WHERE uo2.user_id = au.id) AS preferred_categories \
FROM active_users AS au \
LEFT JOIN user_orders AS uo ON au.id = uo.user_id \
WHERE uo.order_count IS NULL OR uo.order_count < 100 \
ORDER BY uo.total_spent DESC NULLS LAST, au.created_at \
LIMIT 1000 OFFSET 0";

const WARMUP: usize = 5;

struct BenchResult {
    operation: &'static str,
    query_size: &'static str,
    read_dialect: &'static str,
    write_dialect: Option<&'static str>,
    iterations: usize,
    total_us: f64,
    mean_us: f64,
    min_us: f64,
    max_us: f64,
}

fn dialect_name(dt: DialectType) -> &'static str {
    match dt {
        DialectType::Generic => "generic",
        DialectType::PostgreSQL => "postgresql",
        DialectType::MySQL => "mysql",
        DialectType::BigQuery => "bigquery",
        DialectType::Snowflake => "snowflake",
        DialectType::DuckDB => "duckdb",
        _ => "other",
    }
}

fn bench_parse(sql: &str, dialect_type: DialectType, iterations: usize) -> (f64, f64, f64) {
    let dialect = Dialect::get(dialect_type);

    // Warmup
    for _ in 0..WARMUP {
        let _ = black_box(dialect.parse(black_box(sql)));
    }

    let mut total = 0.0_f64;
    let mut min = f64::MAX;
    let mut max = 0.0_f64;

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = black_box(dialect.parse(black_box(sql)));
        let elapsed = start.elapsed().as_secs_f64() * 1_000_000.0;
        total += elapsed;
        if elapsed < min {
            min = elapsed;
        }
        if elapsed > max {
            max = elapsed;
        }
    }

    (total, min, max)
}

fn bench_generate(sql: &str, dialect_type: DialectType, iterations: usize) -> (f64, f64, f64) {
    let dialect = Dialect::get(dialect_type);
    let ast = dialect.parse(sql).expect("parse failed");

    // Warmup
    for _ in 0..WARMUP {
        for expr in &ast {
            let _ = black_box(dialect.generate(black_box(expr)));
        }
    }

    let mut total = 0.0_f64;
    let mut min = f64::MAX;
    let mut max = 0.0_f64;

    for _ in 0..iterations {
        let start = Instant::now();
        for expr in &ast {
            let _ = black_box(dialect.generate(black_box(expr)));
        }
        let elapsed = start.elapsed().as_secs_f64() * 1_000_000.0;
        total += elapsed;
        if elapsed < min {
            min = elapsed;
        }
        if elapsed > max {
            max = elapsed;
        }
    }

    (total, min, max)
}

fn bench_roundtrip(sql: &str, dialect_type: DialectType, iterations: usize) -> (f64, f64, f64) {
    let dialect = Dialect::get(dialect_type);

    // Warmup
    for _ in 0..WARMUP {
        let ast = dialect.parse(black_box(sql)).unwrap();
        for expr in &ast {
            let gen = dialect.generate(black_box(expr)).unwrap();
            let _ = black_box(dialect.parse(black_box(&gen)));
        }
    }

    let mut total = 0.0_f64;
    let mut min = f64::MAX;
    let mut max = 0.0_f64;

    for _ in 0..iterations {
        let start = Instant::now();
        let ast = dialect.parse(black_box(sql)).unwrap();
        for expr in &ast {
            let gen = dialect.generate(black_box(expr)).unwrap();
            let _ = black_box(dialect.parse(black_box(&gen)));
        }
        let elapsed = start.elapsed().as_secs_f64() * 1_000_000.0;
        total += elapsed;
        if elapsed < min {
            min = elapsed;
        }
        if elapsed > max {
            max = elapsed;
        }
    }

    (total, min, max)
}

fn bench_transpile(
    sql: &str,
    read: DialectType,
    write: DialectType,
    iterations: usize,
) -> (f64, f64, f64) {
    // Warmup
    for _ in 0..WARMUP {
        let _ = black_box(transpile(black_box(sql), read, write));
    }

    let mut total = 0.0_f64;
    let mut min = f64::MAX;
    let mut max = 0.0_f64;

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = black_box(transpile(black_box(sql), read, write));
        let elapsed = start.elapsed().as_secs_f64() * 1_000_000.0;
        total += elapsed;
        if elapsed < min {
            min = elapsed;
        }
        if elapsed > max {
            max = elapsed;
        }
    }

    (total, min, max)
}

fn main() {
    let queries: Vec<(&str, &str, usize)> = vec![
        ("simple", SIMPLE_SELECT, 1000),
        ("medium", MEDIUM_SELECT, 500),
        ("complex", COMPLEX_SELECT, 100),
    ];

    let dialect_pairs: Vec<(&str, DialectType, DialectType)> = vec![
        ("pg_to_mysql", DialectType::PostgreSQL, DialectType::MySQL),
        (
            "pg_to_bigquery",
            DialectType::PostgreSQL,
            DialectType::BigQuery,
        ),
        (
            "mysql_to_pg",
            DialectType::MySQL,
            DialectType::PostgreSQL,
        ),
        (
            "bq_to_snowflake",
            DialectType::BigQuery,
            DialectType::Snowflake,
        ),
        (
            "sf_to_duckdb",
            DialectType::Snowflake,
            DialectType::DuckDB,
        ),
        (
            "generic_to_pg",
            DialectType::Generic,
            DialectType::PostgreSQL,
        ),
    ];

    let mut results: Vec<BenchResult> = Vec::new();

    // Parse benchmarks
    for &(size, sql, iters) in &queries {
        let (total, min, max) = bench_parse(sql, DialectType::Generic, iters);
        results.push(BenchResult {
            operation: "parse",
            query_size: size,
            read_dialect: "generic",
            write_dialect: None,
            iterations: iters,
            total_us: total,
            mean_us: total / iters as f64,
            min_us: min,
            max_us: max,
        });
    }

    // Generate benchmarks
    for &(size, sql, iters) in &queries {
        let (total, min, max) = bench_generate(sql, DialectType::Generic, iters);
        results.push(BenchResult {
            operation: "generate",
            query_size: size,
            read_dialect: "generic",
            write_dialect: None,
            iterations: iters,
            total_us: total,
            mean_us: total / iters as f64,
            min_us: min,
            max_us: max,
        });
    }

    // Roundtrip benchmarks
    for &(size, sql, iters) in &queries {
        let (total, min, max) = bench_roundtrip(sql, DialectType::Generic, iters);
        results.push(BenchResult {
            operation: "roundtrip",
            query_size: size,
            read_dialect: "generic",
            write_dialect: None,
            iterations: iters,
            total_us: total,
            mean_us: total / iters as f64,
            min_us: min,
            max_us: max,
        });
    }

    // Transpile benchmarks
    for &(size, sql, iters) in &queries {
        for &(_, read, write) in &dialect_pairs {
            let (total, min, max) = bench_transpile(sql, read, write, iters);
            results.push(BenchResult {
                operation: "transpile",
                query_size: size,
                read_dialect: dialect_name(read),
                write_dialect: Some(dialect_name(write)),
                iterations: iters,
                total_us: total,
                mean_us: total / iters as f64,
                min_us: min,
                max_us: max,
            });
        }
    }

    // Output JSON
    let benchmarks: Vec<serde_json::Value> = results
        .iter()
        .map(|r| {
            json!({
                "operation": r.operation,
                "query_size": r.query_size,
                "read_dialect": r.read_dialect,
                "write_dialect": r.write_dialect,
                "iterations": r.iterations,
                "total_us": (r.total_us * 100.0).round() / 100.0,
                "mean_us": (r.mean_us * 100.0).round() / 100.0,
                "min_us": (r.min_us * 100.0).round() / 100.0,
                "max_us": (r.max_us * 100.0).round() / 100.0,
            })
        })
        .collect();

    let output = json!({
        "engine": "polyglot-sql",
        "version": env!("CARGO_PKG_VERSION"),
        "benchmarks": benchmarks,
    });

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}
