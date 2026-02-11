#!/usr/bin/env python3
"""Benchmark sqlglot operations and output JSON for comparison with polyglot-core.

Run with: uv run --with sqlglot python3 tools/bench-compare/bench_sqlglot.py
"""

import json
import time

import sqlglot

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

WARMUP = 5

# Mapping from polyglot-core dialect names to sqlglot dialect names
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
