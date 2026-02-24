use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use polyglot_sql::dialects::{Dialect, DialectType};
use std::fmt::Write as _;
use std::time::Duration;

const LIST_SIZES: [usize; 3] = [10_000, 100_000, 1_000_000];

#[derive(Clone, Copy)]
enum LiteralKind {
    Numeric,
    String,
}

impl LiteralKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Numeric => "numeric",
            Self::String => "string",
        }
    }
}

fn build_in_query(size: usize, kind: LiteralKind) -> String {
    let approx_per_item = match kind {
        LiteralKind::Numeric => 8,
        LiteralKind::String => 10,
    };
    let mut sql = String::with_capacity(32 + size * approx_per_item);
    sql.push_str("SELECT * FROM t WHERE x IN (");

    for i in 0..size {
        if i > 0 {
            sql.push_str(", ");
        }
        match kind {
            LiteralKind::Numeric => {
                let _ = write!(&mut sql, "{i}");
            }
            LiteralKind::String => {
                sql.push('\'');
                let _ = write!(&mut sql, "{i}");
                sql.push('\'');
            }
        }
    }

    sql.push(')');
    sql
}

fn bench_in_list_kind(c: &mut Criterion, kind: LiteralKind) {
    let dialect = Dialect::get(DialectType::Generic);

    for size in LIST_SIZES {
        let sql = build_in_query(size, kind);
        let mut group = c.benchmark_group(format!("parse_in_list_{}", kind.as_str()));

        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Bytes(sql.len() as u64));
        group.warm_up_time(Duration::from_secs(1));

        if size >= 1_000_000 {
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(12));
        } else if size >= 100_000 {
            group.sample_size(15);
            group.measurement_time(Duration::from_secs(8));
        } else {
            group.sample_size(25);
            group.measurement_time(Duration::from_secs(6));
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), &sql, |b, sql| {
            b.iter(|| {
                let parsed = dialect.parse(black_box(sql));
                assert!(parsed.is_ok(), "benchmark input should parse successfully");
            });
        });

        group.finish();
    }
}

fn bench_in_list_numeric(c: &mut Criterion) {
    bench_in_list_kind(c, LiteralKind::Numeric);
}

fn bench_in_list_string(c: &mut Criterion) {
    bench_in_list_kind(c, LiteralKind::String);
}

criterion_group!(benches, bench_in_list_numeric, bench_in_list_string);
criterion_main!(benches);
