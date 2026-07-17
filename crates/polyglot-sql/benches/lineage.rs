use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use polyglot_sql::dialects::DialectType;
use polyglot_sql::lineage::lineage;
use polyglot_sql::parse_one;
use std::fmt::Write as _;
use std::time::Duration;

const CTE_COUNTS: [usize; 4] = [4, 8, 16, 24];

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

fn bench_cte_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("lineage_cte_chain");
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));

    for n in CTE_COUNTS {
        let sql = build_cte_chain(n);
        let expr = parse_one(&sql, DialectType::Generic).expect("benchmark query should parse");

        assert!(
            lineage("col", &expr, Some(DialectType::Generic), false).is_ok(),
            "lineage should resolve for n={n} CTEs (did we exceed MAX_LINEAGE_DEPTH?)",
        );

        group.bench_with_input(BenchmarkId::from_parameter(n), &expr, |b, expr| {
            b.iter(|| {
                let node = lineage(
                    black_box("col"),
                    black_box(expr),
                    Some(DialectType::Generic),
                    false,
                );
                debug_assert!(node.is_ok());
                black_box(node.ok());
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_cte_chain);
criterion_main!(benches);
