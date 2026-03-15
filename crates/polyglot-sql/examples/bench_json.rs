//! Benchmark that outputs JSON for comparison with Python sqlglot.
//!
//! Run with: cargo run --example bench_json -p polyglot-sql --release

use polyglot_sql::dialects::{Dialect, DialectType};
use polyglot_sql::transpile;
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

// -- Original polyglot queries --

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

// -- SQLGlot benchmark queries (from sqlglot/benchmarks/parse.py) --

const SQLGLOT_SHORT: &str =
    "SELECT 1 AS a, CASE WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END AS b, c FROM x";

const SQLGLOT_TPCH: &str = r#"
WITH "_e_0" AS (
  SELECT
    "partsupp"."ps_partkey" AS "ps_partkey",
    "partsupp"."ps_suppkey" AS "ps_suppkey",
    "partsupp"."ps_supplycost" AS "ps_supplycost"
  FROM "partsupp" AS "partsupp"
), "_e_1" AS (
  SELECT
    "region"."r_regionkey" AS "r_regionkey",
    "region"."r_name" AS "r_name"
  FROM "region" AS "region"
  WHERE
    "region"."r_name" = 'EUROPE'
)
SELECT
  "supplier"."s_acctbal" AS "s_acctbal",
  "supplier"."s_name" AS "s_name",
  "nation"."n_name" AS "n_name",
  "part"."p_partkey" AS "p_partkey",
  "part"."p_mfgr" AS "p_mfgr",
  "supplier"."s_address" AS "s_address",
  "supplier"."s_phone" AS "s_phone",
  "supplier"."s_comment" AS "s_comment"
FROM (
  SELECT
    "part"."p_partkey" AS "p_partkey",
    "part"."p_mfgr" AS "p_mfgr",
    "part"."p_type" AS "p_type",
    "part"."p_size" AS "p_size"
  FROM "part" AS "part"
  WHERE
    "part"."p_size" = 15
    AND "part"."p_type" LIKE '%BRASS'
) AS "part"
LEFT JOIN (
  SELECT
    MIN("partsupp"."ps_supplycost") AS "_col_0",
    "partsupp"."ps_partkey" AS "_u_1"
  FROM "_e_0" AS "partsupp"
  CROSS JOIN "_e_1" AS "region"
  JOIN (
    SELECT
      "nation"."n_nationkey" AS "n_nationkey",
      "nation"."n_regionkey" AS "n_regionkey"
    FROM "nation" AS "nation"
  ) AS "nation"
    ON "nation"."n_regionkey" = "region"."r_regionkey"
  JOIN (
    SELECT
      "supplier"."s_suppkey" AS "s_suppkey",
      "supplier"."s_nationkey" AS "s_nationkey"
    FROM "supplier" AS "supplier"
  ) AS "supplier"
    ON "supplier"."s_nationkey" = "nation"."n_nationkey"
    AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
  GROUP BY
    "partsupp"."ps_partkey"
) AS "_u_0"
  ON "part"."p_partkey" = "_u_0"."_u_1"
CROSS JOIN "_e_1" AS "region"
JOIN (
  SELECT
    "nation"."n_nationkey" AS "n_nationkey",
    "nation"."n_name" AS "n_name",
    "nation"."n_regionkey" AS "n_regionkey"
  FROM "nation" AS "nation"
) AS "nation"
  ON "nation"."n_regionkey" = "region"."r_regionkey"
JOIN "_e_0" AS "partsupp"
  ON "part"."p_partkey" = "partsupp"."ps_partkey"
JOIN (
  SELECT
    "supplier"."s_suppkey" AS "s_suppkey",
    "supplier"."s_name" AS "s_name",
    "supplier"."s_address" AS "s_address",
    "supplier"."s_nationkey" AS "s_nationkey",
    "supplier"."s_phone" AS "s_phone",
    "supplier"."s_acctbal" AS "s_acctbal",
    "supplier"."s_comment" AS "s_comment"
  FROM "supplier" AS "supplier"
) AS "supplier"
  ON "supplier"."s_nationkey" = "nation"."n_nationkey"
  AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
WHERE
  "partsupp"."ps_supplycost" = "_u_0"."_col_0"
  AND NOT "_u_0"."_u_1" IS NULL
ORDER BY
  "supplier"."s_acctbal" DESC,
  "nation"."n_name",
  "supplier"."s_name",
  "part"."p_partkey"
LIMIT 100
"#;

/// Build the "deep_arithmetic" query: 500 chained additions + 500 chained multiplications.
fn build_deep_arithmetic() -> String {
    let nums: Vec<String> = (0..500).map(|i| i.to_string()).collect();
    format!(
        "SELECT 1+{} AS a, 2*{} AS b FROM x",
        nums.join("+"),
        nums.join("*"),
    )
}

/// Build the "large_in" query: 20k string IN + 20k numeric IN.
fn build_large_in() -> String {
    let str_items: Vec<String> = (0..20000).map(|i| format!("'s{i}'")).collect();
    let num_items: Vec<String> = (0..20000).map(|i| i.to_string()).collect();
    format!(
        "SELECT * FROM t WHERE x IN ({}) OR y IN ({})",
        str_items.join(", "),
        num_items.join(", "),
    )
}

/// Build the "values" query: INSERT with 2000 rows x 20 columns.
fn build_values() -> String {
    let rows: Vec<String> = (0..2000)
        .map(|i| {
            let cols: Vec<String> = (0..20)
                .map(|j| {
                    if j % 2 != 0 {
                        format!("'s{i}_{j}'")
                    } else {
                        (i * 20 + j).to_string()
                    }
                })
                .collect();
            format!("({})", cols.join(", "))
        })
        .collect();
    format!("INSERT INTO t VALUES {}", rows.join(", "))
}

/// Build the "many_joins" query: 200 JOINs.
fn build_many_joins() -> String {
    let joins: Vec<String> = (1..200)
        .map(|i| format!("\nJOIN t{i} ON t{i}.id = t{}.id", i - 1))
        .collect();
    format!("SELECT * FROM t0{}", joins.join(""))
}

/// Build the "many_unions" query: 500 UNION ALL.
fn build_many_unions() -> String {
    let selects: Vec<String> = (0..500)
        .map(|i| format!("SELECT {i} AS a, 's{i}' AS b FROM t{i}"))
        .collect();
    selects.join("\nUNION ALL\n")
}

/// Build the "nested_subqueries" query: 20 levels of nested subqueries.
fn build_nested_subqueries() -> String {
    let open = "(SELECT * FROM ".repeat(20);
    let close = ")".repeat(20);
    format!("SELECT * FROM {open}t{close}")
}

/// Build the "many_columns" query: 1000 columns.
fn build_many_columns() -> String {
    let cols: Vec<String> = (0..1000).map(|i| format!("c{i}")).collect();
    format!("SELECT {} FROM t", cols.join(", "))
}

/// Build the "large_case" query: 1000 WHEN clauses.
fn build_large_case() -> String {
    let whens: Vec<String> = (0..1000)
        .map(|i| format!("WHEN x = {i} THEN {i}"))
        .collect();
    format!("SELECT CASE {} ELSE -1 END FROM t", whens.join(" "))
}

/// Build the "complex_where" query: 200 complex conditions.
fn build_complex_where() -> String {
    let conds: Vec<String> = (0..200)
        .map(|i| {
            format!(
                "(c{i} > {i} OR c{i} LIKE '%s{i}%' OR c{i} BETWEEN {i} AND {} OR c{i} IS NULL)",
                i + 10
            )
        })
        .collect();
    format!("SELECT * FROM t WHERE {}", conds.join(" AND "))
}

/// Build the "many_ctes" query: 200 CTEs.
fn build_many_ctes() -> String {
    let ctes: Vec<String> = (0..200)
        .map(|i| {
            let from = if i == 0 {
                "tbase".to_string()
            } else {
                format!("t{}", i - 1)
            };
            format!("t{i} AS (SELECT {i} AS a FROM {from})")
        })
        .collect();
    format!("WITH {} SELECT * FROM t199", ctes.join(", "))
}

/// Build the "many_windows" query: 200 window functions.
fn build_many_windows() -> String {
    let cols: Vec<String> = (0..200)
        .map(|i| {
            format!(
                "SUM(c{i}) OVER (PARTITION BY p{} ORDER BY o{}) AS w{i}",
                i % 10,
                i % 5
            )
        })
        .collect();
    format!("SELECT {} FROM t", cols.join(", "))
}

/// Build the "nested_functions" query: 20 levels of nested COALESCE.
fn build_nested_functions() -> String {
    let open = "COALESCE(".repeat(20);
    let close = ", NULL)".repeat(20);
    format!("SELECT {open}x{close} FROM t")
}

/// Build the "large_strings" query: 500 large string literals.
fn build_large_strings() -> String {
    let x100 = "x".repeat(100);
    let cols: Vec<String> = (0..500).map(|_| format!("'{x100}'")).collect();
    format!("SELECT {} FROM t", cols.join(", "))
}

/// Build the "many_numbers" query: 10000 number literals.
fn build_many_numbers() -> String {
    let nums: Vec<String> = (0..10000).map(|i| i.to_string()).collect();
    format!("SELECT {} FROM t", nums.join(", "))
}

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
    // The sg_deep_arithmetic query (500 chained operators) needs deep recursion
    // in the recursive-descent parser. Spawn on a thread with a large stack.
    let child = std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(run_benchmarks)
        .expect("failed to spawn benchmark thread");
    child.join().unwrap();
}

fn run_benchmarks() {
    let deep_arithmetic = build_deep_arithmetic();
    let large_in = build_large_in();
    let values = build_values();
    let many_joins = build_many_joins();
    let many_unions = build_many_unions();
    let nested_subqueries = build_nested_subqueries();
    let many_columns = build_many_columns();
    let large_case = build_large_case();
    let complex_where = build_complex_where();
    let many_ctes = build_many_ctes();
    let many_windows = build_many_windows();
    let nested_functions = build_nested_functions();
    let large_strings = build_large_strings();
    let many_numbers = build_many_numbers();

    let queries: Vec<(&str, &str, usize)> = vec![
        ("simple", SIMPLE_SELECT, 1000),
        ("medium", MEDIUM_SELECT, 500),
        ("complex", COMPLEX_SELECT, 100),
        ("sg_short", SQLGLOT_SHORT, 1000),
        ("sg_tpch", SQLGLOT_TPCH, 100),
        ("sg_deep_arithmetic", &deep_arithmetic, 50),
        ("sg_large_in", &large_in, 10),
        ("sg_values", &values, 10),
        ("sg_many_joins", &many_joins, 50),
        ("sg_many_unions", &many_unions, 20),
        ("sg_nested_subqueries", &nested_subqueries, 500),
        ("sg_many_columns", &many_columns, 100),
        ("sg_large_case", &large_case, 20),
        ("sg_complex_where", &complex_where, 20),
        ("sg_many_ctes", &many_ctes, 50),
        ("sg_many_windows", &many_windows, 50),
        ("sg_nested_functions", &nested_functions, 500),
        ("sg_large_strings", &large_strings, 50),
        ("sg_many_numbers", &many_numbers, 20),
    ];

    let dialect_pairs: Vec<(&str, DialectType, DialectType)> = vec![
        ("pg_to_mysql", DialectType::PostgreSQL, DialectType::MySQL),
        (
            "pg_to_bigquery",
            DialectType::PostgreSQL,
            DialectType::BigQuery,
        ),
        ("mysql_to_pg", DialectType::MySQL, DialectType::PostgreSQL),
        (
            "bq_to_snowflake",
            DialectType::BigQuery,
            DialectType::Snowflake,
        ),
        ("sf_to_duckdb", DialectType::Snowflake, DialectType::DuckDB),
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
