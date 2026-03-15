#!/usr/bin/env python3
"""Benchmark sqlglot operations and output JSON for comparison with polyglot-sql.

Run with: uv run --with sqlglot[c] python3 tools/bench-compare/bench_sqlglot.py
"""

import json
import time

import sqlglot

# -- Original polyglot queries --

SIMPLE_SELECT = "SELECT a, b, c FROM table1"

MEDIUM_SELECT = (
    "SELECT "
    "u.id, "
    "u.name, "
    "u.email, "
    "COUNT(o.id) AS order_count, "
    "SUM(o.total) AS total_spent "
    "FROM users AS u "
    "LEFT JOIN orders AS o ON u.id = o.user_id "
    "WHERE u.created_at > '2024-01-01' AND u.status = 'active' "
    "GROUP BY u.id, u.name, u.email "
    "HAVING COUNT(o.id) > 5 "
    "ORDER BY total_spent DESC "
    "LIMIT 100"
)

COMPLEX_SELECT = (
    "WITH active_users AS ("
    "SELECT u.id, u.name, u.email, u.created_at "
    "FROM users AS u "
    "WHERE u.status = 'active' AND u.last_login > CURRENT_DATE - INTERVAL '30 days'"
    "), "
    "user_orders AS ("
    "SELECT o.user_id, COUNT(*) AS order_count, SUM(o.total) AS total_spent, "
    "AVG(o.total) AS avg_order_value, MAX(o.created_at) AS last_order_date "
    "FROM orders AS o "
    "WHERE o.status = 'completed' "
    "GROUP BY o.user_id"
    "), "
    "product_categories AS ("
    "SELECT DISTINCT p.category_id, c.name AS category_name "
    "FROM products AS p "
    "JOIN categories AS c ON p.category_id = c.id "
    "WHERE p.is_active = TRUE"
    ") "
    "SELECT au.id AS user_id, au.name AS user_name, au.email, "
    "COALESCE(uo.order_count, 0) AS total_orders, "
    "COALESCE(uo.total_spent, 0) AS lifetime_value, "
    "COALESCE(uo.avg_order_value, 0) AS average_order, "
    "uo.last_order_date, "
    "CASE WHEN uo.total_spent > 10000 THEN 'VIP' "
    "WHEN uo.total_spent > 1000 THEN 'Premium' "
    "WHEN uo.total_spent > 100 THEN 'Regular' "
    "ELSE 'New' END AS customer_tier, "
    "(SELECT STRING_AGG(pc.category_name, ', ') "
    "FROM user_orders AS uo2 "
    "JOIN order_items AS oi ON uo2.user_id = oi.order_id "
    "JOIN products AS p ON oi.product_id = p.id "
    "JOIN product_categories AS pc ON p.category_id = pc.category_id "
    "WHERE uo2.user_id = au.id) AS preferred_categories "
    "FROM active_users AS au "
    "LEFT JOIN user_orders AS uo ON au.id = uo.user_id "
    "WHERE uo.order_count IS NULL OR uo.order_count < 100 "
    "ORDER BY uo.total_spent DESC NULLS LAST, au.created_at "
    "LIMIT 1000 OFFSET 0"
)

# -- SQLGlot benchmark queries (from sqlglot/benchmarks/parse.py) --

SQLGLOT_SHORT = "SELECT 1 AS a, CASE WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END AS b, c FROM x"

SQLGLOT_DEEP_ARITHMETIC = "SELECT 1+"
SQLGLOT_DEEP_ARITHMETIC += "+".join(str(i) for i in range(500))
SQLGLOT_DEEP_ARITHMETIC += " AS a, 2*"
SQLGLOT_DEEP_ARITHMETIC += "*".join(str(i) for i in range(500))
SQLGLOT_DEEP_ARITHMETIC += " AS b FROM x"

SQLGLOT_LARGE_IN = (
    "SELECT * FROM t WHERE x IN (" + ", ".join(f"'s{i}'" for i in range(20000)) + ")"
    " OR y IN (" + ", ".join(str(i) for i in range(20000)) + ")"
)

SQLGLOT_VALUES = "INSERT INTO t VALUES " + ", ".join(
    "(" + ", ".join(f"'s{i}_{j}'" if j % 2 else str(i * 20 + j) for j in range(20)) + ")"
    for i in range(2000)
)

SQLGLOT_MANY_JOINS = "SELECT * FROM t0" + "".join(
    f"\nJOIN t{i} ON t{i}.id = t{i - 1}.id" for i in range(1, 200)
)

SQLGLOT_MANY_UNIONS = "\nUNION ALL\n".join(f"SELECT {i} AS a, 's{i}' AS b FROM t{i}" for i in range(500))

SQLGLOT_NESTED_SUBQUERIES = (
    "SELECT * FROM " + "".join("(SELECT * FROM " for _ in range(20)) + "t" + ")" * 20
)

SQLGLOT_MANY_COLUMNS = "SELECT " + ", ".join(f"c{i}" for i in range(1000)) + " FROM t"

SQLGLOT_LARGE_CASE = (
    "SELECT CASE " + " ".join(f"WHEN x = {i} THEN {i}" for i in range(1000)) + " ELSE -1 END FROM t"
)

SQLGLOT_COMPLEX_WHERE = "SELECT * FROM t WHERE " + " AND ".join(
    f"(c{i} > {i} OR c{i} LIKE '%s{i}%' OR c{i} BETWEEN {i} AND {i+10} OR c{i} IS NULL)"
    for i in range(200)
)

SQLGLOT_MANY_CTES = (
    "WITH "
    + ", ".join(f"t{i} AS (SELECT {i} AS a FROM t{i-1 if i else 'base'})" for i in range(200))
    + " SELECT * FROM t199"
)

SQLGLOT_MANY_WINDOWS = (
    "SELECT "
    + ", ".join(
        f"SUM(c{i}) OVER (PARTITION BY p{i % 10} ORDER BY o{i % 5}) AS w{i}" for i in range(200)
    )
    + " FROM t"
)

SQLGLOT_NESTED_FUNCTIONS = "SELECT " + "COALESCE(" * 20 + "x" + ", NULL)" * 20 + " FROM t"

SQLGLOT_LARGE_STRINGS = "SELECT " + ", ".join(f"'{'x' * 100}'" for i in range(500)) + " FROM t"

SQLGLOT_MANY_NUMBERS = "SELECT " + ", ".join(str(i) for i in range(10000)) + " FROM t"

SQLGLOT_TPCH = """
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
"""

WARMUP = 5

# Mapping from polyglot-sql dialect names to sqlglot dialect names
DIALECT_MAP = {
    "generic": None,
    "postgresql": "postgres",
    "mysql": "mysql",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "duckdb": "duckdb",
}


def bench_parse(sql, read_dialect, iterations):
    sg_dialect = DIALECT_MAP[read_dialect]

    # Warmup
    for _ in range(WARMUP):
        sqlglot.parse(sql, dialect=sg_dialect)

    total = 0.0
    min_t = float("inf")
    max_t = 0.0

    for _ in range(iterations):
        start = time.perf_counter_ns()
        sqlglot.parse(sql, dialect=sg_dialect)
        elapsed = (time.perf_counter_ns() - start) / 1000.0  # ns -> us
        total += elapsed
        min_t = min(min_t, elapsed)
        max_t = max(max_t, elapsed)

    return total, min_t, max_t


def bench_generate(sql, read_dialect, iterations):
    sg_dialect = DIALECT_MAP[read_dialect]
    ast = sqlglot.parse(sql, dialect=sg_dialect)

    # Warmup
    for _ in range(WARMUP):
        for expr in ast:
            expr.sql(dialect=sg_dialect)

    total = 0.0
    min_t = float("inf")
    max_t = 0.0

    for _ in range(iterations):
        start = time.perf_counter_ns()
        for expr in ast:
            expr.sql(dialect=sg_dialect)
        elapsed = (time.perf_counter_ns() - start) / 1000.0
        total += elapsed
        min_t = min(min_t, elapsed)
        max_t = max(max_t, elapsed)

    return total, min_t, max_t


def bench_roundtrip(sql, read_dialect, iterations):
    sg_dialect = DIALECT_MAP[read_dialect]

    # Warmup
    for _ in range(WARMUP):
        ast = sqlglot.parse(sql, dialect=sg_dialect)
        for expr in ast:
            gen = expr.sql(dialect=sg_dialect)
            sqlglot.parse(gen, dialect=sg_dialect)

    total = 0.0
    min_t = float("inf")
    max_t = 0.0

    for _ in range(iterations):
        start = time.perf_counter_ns()
        ast = sqlglot.parse(sql, dialect=sg_dialect)
        for expr in ast:
            gen = expr.sql(dialect=sg_dialect)
            sqlglot.parse(gen, dialect=sg_dialect)
        elapsed = (time.perf_counter_ns() - start) / 1000.0
        total += elapsed
        min_t = min(min_t, elapsed)
        max_t = max(max_t, elapsed)

    return total, min_t, max_t


def bench_transpile(sql, read_dialect, write_dialect, iterations):
    sg_read = DIALECT_MAP[read_dialect]
    sg_write = DIALECT_MAP[write_dialect]

    # Warmup
    for _ in range(WARMUP):
        sqlglot.transpile(sql, read=sg_read, write=sg_write)

    total = 0.0
    min_t = float("inf")
    max_t = 0.0

    for _ in range(iterations):
        start = time.perf_counter_ns()
        sqlglot.transpile(sql, read=sg_read, write=sg_write)
        elapsed = (time.perf_counter_ns() - start) / 1000.0
        total += elapsed
        min_t = min(min_t, elapsed)
        max_t = max(max_t, elapsed)

    return total, min_t, max_t


def main():
    queries = [
        ("simple", SIMPLE_SELECT, 1000),
        ("medium", MEDIUM_SELECT, 500),
        ("complex", COMPLEX_SELECT, 100),
        ("sg_short", SQLGLOT_SHORT, 1000),
        ("sg_tpch", SQLGLOT_TPCH, 100),
        ("sg_deep_arithmetic", SQLGLOT_DEEP_ARITHMETIC, 50),
        ("sg_large_in", SQLGLOT_LARGE_IN, 10),
        ("sg_values", SQLGLOT_VALUES, 10),
        ("sg_many_joins", SQLGLOT_MANY_JOINS, 50),
        ("sg_many_unions", SQLGLOT_MANY_UNIONS, 20),
        ("sg_nested_subqueries", SQLGLOT_NESTED_SUBQUERIES, 500),
        ("sg_many_columns", SQLGLOT_MANY_COLUMNS, 100),
        ("sg_large_case", SQLGLOT_LARGE_CASE, 20),
        ("sg_complex_where", SQLGLOT_COMPLEX_WHERE, 20),
        ("sg_many_ctes", SQLGLOT_MANY_CTES, 50),
        ("sg_many_windows", SQLGLOT_MANY_WINDOWS, 50),
        ("sg_nested_functions", SQLGLOT_NESTED_FUNCTIONS, 500),
        ("sg_large_strings", SQLGLOT_LARGE_STRINGS, 50),
        ("sg_many_numbers", SQLGLOT_MANY_NUMBERS, 20),
    ]

    dialect_pairs = [
        ("postgresql", "mysql"),
        ("postgresql", "bigquery"),
        ("mysql", "postgresql"),
        ("bigquery", "snowflake"),
        ("snowflake", "duckdb"),
        ("generic", "postgresql"),
    ]

    results = []

    # Parse benchmarks
    for size, sql, iters in queries:
        total, min_t, max_t = bench_parse(sql, "generic", iters)
        results.append({
            "operation": "parse",
            "query_size": size,
            "read_dialect": "generic",
            "write_dialect": None,
            "iterations": iters,
            "total_us": round(total, 2),
            "mean_us": round(total / iters, 2),
            "min_us": round(min_t, 2),
            "max_us": round(max_t, 2),
        })

    # Generate benchmarks
    for size, sql, iters in queries:
        total, min_t, max_t = bench_generate(sql, "generic", iters)
        results.append({
            "operation": "generate",
            "query_size": size,
            "read_dialect": "generic",
            "write_dialect": None,
            "iterations": iters,
            "total_us": round(total, 2),
            "mean_us": round(total / iters, 2),
            "min_us": round(min_t, 2),
            "max_us": round(max_t, 2),
        })

    # Roundtrip benchmarks
    for size, sql, iters in queries:
        total, min_t, max_t = bench_roundtrip(sql, "generic", iters)
        results.append({
            "operation": "roundtrip",
            "query_size": size,
            "read_dialect": "generic",
            "write_dialect": None,
            "iterations": iters,
            "total_us": round(total, 2),
            "mean_us": round(total / iters, 2),
            "min_us": round(min_t, 2),
            "max_us": round(max_t, 2),
        })

    # Transpile benchmarks
    for size, sql, iters in queries:
        for read_d, write_d in dialect_pairs:
            total, min_t, max_t = bench_transpile(sql, read_d, write_d, iters)
            results.append({
                "operation": "transpile",
                "query_size": size,
                "read_dialect": read_d,
                "write_dialect": write_d,
                "iterations": iters,
                "total_us": round(total, 2),
                "mean_us": round(total / iters, 2),
                "min_us": round(min_t, 2),
                "max_us": round(max_t, 2),
            })

    output = {
        "engine": "sqlglot",
        "version": sqlglot.__version__,
        "benchmarks": results,
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
