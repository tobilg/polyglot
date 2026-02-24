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

const SQLGLOT_LONG: &str = r#"
SELECT
  "e"."employee_id" AS "Employee #",
  "e"."first_name" || ' ' || "e"."last_name" AS "Name",
  "e"."email" AS "Email",
  "e"."phone_number" AS "Phone",
  TO_CHAR("e"."hire_date", 'MM/DD/YYYY') AS "Hire Date",
  TO_CHAR("e"."salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS "Salary",
  "e"."commission_pct" AS "Commission %",
  'works as ' || "j"."job_title" || ' in ' || "d"."department_name" || ' department (manager: ' || "dm"."first_name" || ' ' || "dm"."last_name" || ') and immediate supervisor: ' || "m"."first_name" || ' ' || "m"."last_name" AS "Current Job",
  TO_CHAR("j"."min_salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') || ' - ' || TO_CHAR("j"."max_salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS "Current Salary",
  "l"."street_address" || ', ' || "l"."postal_code" || ', ' || "l"."city" || ', ' || "l"."state_province" || ', ' || "c"."country_name" || ' (' || "r"."region_name" || ')' AS "Location",
  "jh"."job_id" AS "History Job ID",
  'worked from ' || TO_CHAR("jh"."start_date", 'MM/DD/YYYY') || ' to ' || TO_CHAR("jh"."end_date", 'MM/DD/YYYY') || ' as ' || "jj"."job_title" || ' in ' || "dd"."department_name" || ' department' AS "History Job Title",
  case when 1 then 1 when 2 then 2 when 3 then 3 when 4 then 4 when 5 then 5 else a(b(c + 1 * 3 % 4)) end
FROM "employees" AS e
JOIN "jobs" AS j
  ON "e"."job_id" = "j"."job_id"
LEFT JOIN "employees" AS m
  ON "e"."manager_id" = "m"."employee_id"
LEFT JOIN "departments" AS d
  ON "d"."department_id" = "e"."department_id"
LEFT JOIN "employees" AS dm
  ON "d"."manager_id" = "dm"."employee_id"
LEFT JOIN "locations" AS l
  ON "d"."location_id" = "l"."location_id"
LEFT JOIN "countries" AS c
  ON "l"."country_id" = "c"."country_id"
LEFT JOIN "regions" AS r
  ON "c"."region_id" = "r"."region_id"
LEFT JOIN "job_history" AS jh
  ON "e"."employee_id" = "jh"."employee_id"
LEFT JOIN "jobs" AS jj
  ON "jj"."job_id" = "jh"."job_id"
LEFT JOIN "departments" AS dd
  ON "dd"."department_id" = "jh"."department_id"
ORDER BY
  "e"."employee_id"
"#;

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

/// Build the "crazy" query from sqlglot benchmarks: 500 chained additions + 500 chained multiplications.
fn build_crazy_query() -> String {
    let nums: Vec<String> = (0..500).map(|i| i.to_string()).collect();
    format!(
        "SELECT 1+{} AS a, 2*{} AS b FROM x",
        nums.join("+"),
        nums.join("*"),
    )
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
    // The sg_crazy query (500 chained operators) needs deep recursion in the
    // recursive-descent parser. Spawn on a thread with an 8 MB stack.
    let child = std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(run_benchmarks)
        .expect("failed to spawn benchmark thread");
    child.join().unwrap();
}

fn run_benchmarks() {
    let crazy = build_crazy_query();
    let queries: Vec<(&str, &str, usize)> = vec![
        ("simple", SIMPLE_SELECT, 1000),
        ("medium", MEDIUM_SELECT, 500),
        ("complex", COMPLEX_SELECT, 100),
        ("sg_short", SQLGLOT_SHORT, 1000),
        ("sg_long", SQLGLOT_LONG, 500),
        ("sg_tpch", SQLGLOT_TPCH, 100),
        ("sg_crazy", &crazy, 50),
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
