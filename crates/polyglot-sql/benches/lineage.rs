use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use polyglot_sql::dialects::DialectType;
use polyglot_sql::lineage::lineage;
use polyglot_sql::parse_one;
use std::fmt::Write as _;
use std::hint::black_box;
use std::time::Duration;

const CTE_COUNTS: [usize; 4] = [4, 8, 16, 24];
const CTE_REFERENCE_COUNTS: [usize; 4] = [2, 4, 8, 16];

fn build_cte_chain(n: usize) -> String {
    let mut sql = String::from(
        "WITH\n  c0 AS (\n    SELECT x AS col, y FROM base_table WHERE y > 0\n    \
         UNION ALL\n    SELECT x AS col, y FROM base_table WHERE y <= 0\n  )",
    );
    for i in 1..n {
        let _ = write!(
            &mut sql,
            ",\n  c{i} AS (\n    SELECT c{prev}.col AS col, c{prev}.y AS y FROM c{prev}\n    \
             UNION ALL\n    SELECT x AS col, y FROM base_table\n  )",
            i = i,
            prev = i - 1,
        );
    }
    let _ = write!(&mut sql, "\nSELECT col FROM c{}", n - 1);
    sql
}

fn build_nested_cte_chain(n: usize) -> String {
    let mut sql = "SELECT x AS col FROM base_table".to_string();
    for i in 0..n {
        sql = format!("WITH c{i} AS ({sql}) SELECT col FROM c{i}");
    }
    sql
}

fn build_reused_nested_cte(reference_count: usize) -> String {
    let projection = (0..reference_count)
        .map(|i| format!("s{i}.col"))
        .collect::<Vec<_>>()
        .join(" + ");
    let sources = (0..reference_count)
        .map(|i| format!("shared AS s{i}"))
        .collect::<Vec<_>>()
        .join(" CROSS JOIN ");

    format!(
        "WITH shared AS (\
         WITH nested AS (SELECT x AS col FROM base_table) \
         SELECT col FROM nested\
         ) SELECT {projection} AS col FROM {sources}"
    )
}

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
}

fn bench_lineage(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    n: usize,
    sql: &str,
) {
    let expr = parse_one(sql, DialectType::Generic).expect("benchmark query should parse");

    assert!(
        lineage("col", &expr, Some(DialectType::Generic), false).is_ok(),
        "lineage should resolve for n={n} (did we exceed MAX_LINEAGE_DEPTH?)",
    );

    group.bench_with_input(BenchmarkId::from_parameter(n), &expr, |b, expr| {
        b.iter(|| {
            black_box(lineage(
                black_box("col"),
                black_box(expr),
                Some(DialectType::Generic),
                false,
            ))
        });
    });
}

fn bench_cte_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("lineage_cte_chain");
    configure_group(&mut group);

    for n in CTE_COUNTS {
        let sql = build_cte_chain(n);
        bench_lineage(&mut group, n, &sql);
    }

    group.finish();
}

fn bench_nested_cte_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("lineage_nested_cte_chain");
    configure_group(&mut group);

    for n in CTE_COUNTS {
        let sql = build_nested_cte_chain(n);
        bench_lineage(&mut group, n, &sql);
    }

    group.finish();
}

fn bench_reused_nested_cte(c: &mut Criterion) {
    let mut group = c.benchmark_group("lineage_reused_nested_cte");
    configure_group(&mut group);

    for n in CTE_REFERENCE_COUNTS {
        let sql = build_reused_nested_cte(n);
        bench_lineage(&mut group, n, &sql);
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_cte_chain,
    bench_nested_cte_chain,
    bench_reused_nested_cte
);
criterion_main!(benches);
