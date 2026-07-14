use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use polyglot_sql::dialects::transform_recursive;
use polyglot_sql::{ExpressionWalk, Parser};

const COMPLEX_QUERY: &str = r#"
WITH active_users AS (
    SELECT id, name FROM users WHERE active = TRUE
), order_totals AS (
    SELECT user_id, SUM(amount) AS total
    FROM orders
    GROUP BY user_id
)
SELECT u.id, u.name, COALESCE(o.total, 0) AS total
FROM active_users AS u
LEFT JOIN order_totals AS o ON u.id = o.user_id
WHERE o.total > 100 OR o.total IS NULL
ORDER BY total DESC
LIMIT 100
"#;

fn benchmark_ast_traversal(c: &mut Criterion) {
    let expression = Parser::parse_sql(COMPLEX_QUERY)
        .expect("benchmark query should parse")
        .remove(0);

    let mut group = c.benchmark_group("ast_traversal");
    group.bench_function("dfs", |b| b.iter(|| black_box(&expression).dfs().count()));
    group.bench_function("bfs", |b| b.iter(|| black_box(&expression).bfs().count()));
    group.bench_function("direct_children", |b| {
        b.iter(|| black_box(&expression).children())
    });
    group.bench_function("noop_transform", |b| {
        b.iter_batched(
            || expression.clone(),
            |expression| transform_recursive(expression, &|node| Ok(node)).unwrap(),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, benchmark_ast_traversal);
criterion_main!(benches);
