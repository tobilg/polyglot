use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use polyglot_sql::dialects::{Dialect, DialectType};

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
    ),
    product_categories AS (
        SELECT DISTINCT
            p.category_id,
            c.name as category_name
        FROM products p
        JOIN categories c ON p.category_id = c.id
        WHERE p.is_active = true
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
        WHEN uo.total_spent > 100 THEN 'Regular'
        ELSE 'New'
    END as customer_tier,
    (
        SELECT STRING_AGG(pc.category_name, ', ')
        FROM user_orders uo2
        JOIN order_items oi ON uo2.user_id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        JOIN product_categories pc ON p.category_id = pc.category_id
        WHERE uo2.user_id = au.id
    ) as preferred_categories
FROM active_users au
LEFT JOIN user_orders uo ON au.id = uo.user_id
WHERE (uo.order_count IS NULL OR uo.order_count < 100)
ORDER BY uo.total_spent DESC NULLS LAST, au.created_at
LIMIT 1000 OFFSET 0
"#;

fn bench_parsing_by_query_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("parsing_by_size");
    let dialect = Dialect::get(DialectType::Generic);

    group.bench_function("simple", |b| {
        b.iter(|| dialect.parse(black_box(SIMPLE_SELECT)))
    });

    group.bench_function("medium", |b| {
        b.iter(|| dialect.parse(black_box(MEDIUM_SELECT)))
    });

    group.bench_function("complex", |b| {
        b.iter(|| dialect.parse(black_box(COMPLEX_SELECT)))
    });

    group.finish();
}

fn bench_parsing_by_dialect(c: &mut Criterion) {
    let mut group = c.benchmark_group("parsing_by_dialect");

    let dialects = [
        ("Generic", DialectType::Generic),
        ("PostgreSQL", DialectType::PostgreSQL),
        ("MySQL", DialectType::MySQL),
        ("BigQuery", DialectType::BigQuery),
        ("Snowflake", DialectType::Snowflake),
        ("DuckDB", DialectType::DuckDB),
    ];

    for (name, dialect_type) in dialects {
        let dialect = Dialect::get(dialect_type);
        group.bench_with_input(
            BenchmarkId::new("medium_query", name),
            &MEDIUM_SELECT,
            |b, sql| b.iter(|| dialect.parse(black_box(sql))),
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parsing_by_query_size,
    bench_parsing_by_dialect
);
criterion_main!(benches);
