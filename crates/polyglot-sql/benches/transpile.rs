use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
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
        ("PostgreSQL_to_MySQL", DialectType::PostgreSQL, DialectType::MySQL),
        ("PostgreSQL_to_BigQuery", DialectType::PostgreSQL, DialectType::BigQuery),
        ("MySQL_to_PostgreSQL", DialectType::MySQL, DialectType::PostgreSQL),
        ("BigQuery_to_Snowflake", DialectType::BigQuery, DialectType::Snowflake),
        ("Snowflake_to_DuckDB", DialectType::Snowflake, DialectType::DuckDB),
        ("Generic_to_PostgreSQL", DialectType::Generic, DialectType::PostgreSQL),
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

criterion_group!(benches, bench_transpile_by_query_size, bench_transpile_dialect_pairs, bench_roundtrip);
criterion_main!(benches);
