use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use polyglot_sql::dialects::DialectType;
use polyglot_sql::transpile;

const SIMPLE_SELECT: &str = "SELECT a, b, c FROM table1";

const MEDIUM_SELECT: &str = r#"
SELECT
    u.id,
    u.name,
    u.email,
    COUNT(o.id) as order_count,
    SUM(o.total) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at > '2024-01-01'
    AND u.status = 'active'
GROUP BY u.id, u.name, u.email
HAVING COUNT(o.id) > 5
ORDER BY total_spent DESC
LIMIT 100
"#;

const COMPLEX_SELECT: &str = r#"
WITH
    active_users AS (
        SELECT
            u.id,
            u.name,
            u.email,
            u.created_at
        FROM users u
        WHERE u.status = 'active'
            AND u.last_login > CURRENT_DATE - INTERVAL '30 days'
    ),
    user_orders AS (
        SELECT
            o.user_id,
            COUNT(*) as order_count,
            SUM(o.total) as total_spent,
            AVG(o.total) as avg_order_value,
            MAX(o.created_at) as last_order_date
        FROM orders o
        WHERE o.status = 'completed'
        GROUP BY o.user_id
    )
SELECT
    au.id as user_id,
    au.name as user_name,
    au.email,
    COALESCE(uo.order_count, 0) as total_orders,
    COALESCE(uo.total_spent, 0) as lifetime_value,
    COALESCE(uo.avg_order_value, 0) as average_order,
    uo.last_order_date,
    CASE
        WHEN uo.total_spent > 10000 THEN 'VIP'
        WHEN uo.total_spent > 1000 THEN 'Premium'
        ELSE 'Regular'
    END as customer_tier
FROM active_users au
LEFT JOIN user_orders uo ON au.id = uo.user_id
ORDER BY uo.total_spent DESC NULLS LAST
LIMIT 1000
"#;

fn bench_transpile_by_query_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("transpile_by_size");

    // PostgreSQL to MySQL transpilation
    group.bench_function("simple", |b| {
        b.iter(|| {
            transpile(
                black_box(SIMPLE_SELECT),
                DialectType::PostgreSQL,
                DialectType::MySQL,
            )
        })
    });

    group.bench_function("medium", |b| {
        b.iter(|| {
            transpile(
                black_box(MEDIUM_SELECT),
                DialectType::PostgreSQL,
                DialectType::MySQL,
            )
        })
    });

    group.bench_function("complex", |b| {
        b.iter(|| {
            transpile(
                black_box(COMPLEX_SELECT),
                DialectType::PostgreSQL,
                DialectType::MySQL,
            )
        })
    });

    group.finish();
}

fn bench_transpile_dialect_pairs(c: &mut Criterion) {
    let mut group = c.benchmark_group("transpile_dialect_pairs");

    let dialect_pairs = [
        (
            "PostgreSQL_to_MySQL",
            DialectType::PostgreSQL,
            DialectType::MySQL,
        ),
        (
            "PostgreSQL_to_BigQuery",
            DialectType::PostgreSQL,
            DialectType::BigQuery,
        ),
        (
            "MySQL_to_PostgreSQL",
            DialectType::MySQL,
            DialectType::PostgreSQL,
        ),
        (
            "BigQuery_to_Snowflake",
            DialectType::BigQuery,
            DialectType::Snowflake,
        ),
        (
            "Snowflake_to_DuckDB",
            DialectType::Snowflake,
            DialectType::DuckDB,
        ),
        (
            "Generic_to_PostgreSQL",
            DialectType::Generic,
            DialectType::PostgreSQL,
        ),
    ];

    for (name, source, target) in dialect_pairs {
        group.bench_with_input(
            BenchmarkId::new("medium_query", name),
            &MEDIUM_SELECT,
            |b, sql| b.iter(|| transpile(black_box(sql), source, target)),
        );
    }

    group.finish();
}

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("roundtrip");
    use polyglot_sql::dialects::Dialect;

    let dialect = Dialect::get(DialectType::PostgreSQL);

    group.bench_function("medium_query", |b| {
        b.iter(|| {
            // Parse
            let ast = dialect.parse(black_box(MEDIUM_SELECT)).unwrap();
            // Generate
            for expr in &ast {
                let _ = dialect.generate(black_box(expr));
            }
        })
    });

    group.finish();
}

fn bench_compound_row_limits(c: &mut Criterion) {
    use polyglot_sql::dialects::Dialect;
    use std::time::Duration;

    let mut group = c.benchmark_group("compound_row_limits");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    for branches in [1, 10, 100] {
        let body = vec!["SELECT a FROM t"; branches].join(" UNION ALL ");
        for (name, source, target, tail) in [
            (
                "count",
                DialectType::PostgreSQL,
                DialectType::Oracle,
                "LIMIT 5 OFFSET 2",
            ),
            (
                "percent",
                DialectType::DuckDB,
                DialectType::Oracle,
                "LIMIT 10 PERCENT",
            ),
            (
                "fetch",
                DialectType::Oracle,
                DialectType::DuckDB,
                "OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY",
            ),
        ] {
            let sql = format!("{body} ORDER BY a {tail}");
            let dialect = Dialect::get(source);
            group.bench_function(format!("parse/{name}/{branches}"), |b| {
                b.iter(|| dialect.parse(black_box(&sql)).unwrap())
            });
            group.bench_function(format!("transpile/{name}/{branches}"), |b| {
                b.iter(|| transpile(black_box(&sql), source, target).unwrap())
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_transpile_by_query_size,
    bench_transpile_dialect_pairs,
    bench_roundtrip,
    bench_compound_row_limits
);
criterion_main!(benches);
