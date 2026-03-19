#!/usr/bin/env python3
"""Transpilation benchmark comparing polyglot-sql (Rust/PyO3) vs sqlglot[c] (Python + C tokenizer).

Covers 8 queries of varying complexity and a 6-pair dialect matrix.

Run via make from the project root:

    make bench-transpile
    make bench-transpile-quick

Or directly:

    uv sync --project tools/bench-compare --reinstall-package polyglot-sql
    uv run --project tools/bench-compare python3 tools/bench-compare/bench_transpile.py --quiet
    uv run --project tools/bench-compare python3 tools/bench-compare/bench_transpile.py --quiet --quick
"""

import argparse
import os
import sys
from math import exp, log

import pyperf

import polyglot_sql
import sqlglot

# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

simple_select = (
    "SELECT id, name::TEXT, active = TRUE AS is_active "
    "FROM users WHERE created_at > '2024-01-01' LIMIT 100"
)

aggregation = (
    "SELECT department, COUNT(*) AS cnt, SUM(salary) AS total_sal, "
    "AVG(COALESCE(bonus, 0)) AS avg_bonus, "
    "EXTRACT(EPOCH FROM MAX(updated_at) - MIN(created_at)) AS span_seconds "
    "FROM employees "
    "WHERE hire_date > CURRENT_DATE - INTERVAL '30 days' "
    "GROUP BY department HAVING COUNT(*) > 5"
)

cte_join = (
    "WITH active_users AS ("
    "SELECT id, name, email FROM users WHERE status = 'active'), "
    "user_orders AS ("
    "SELECT user_id, COUNT(*) AS order_count, SUM(amount) AS total_amount "
    "FROM orders GROUP BY user_id) "
    "SELECT a.name, a.email, IFNULL(u.order_count, 0) AS orders, "
    "CASE WHEN u.total_amount > 1000 THEN 'vip' ELSE 'regular' END AS tier, "
    "GROUP_CONCAT(u.order_count ORDER BY u.order_count SEPARATOR ', ') AS order_list "
    "FROM active_users a LEFT JOIN user_orders u ON a.id = u.user_id"
)

window_qualify = (
    "SELECT *, "
    "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn, "
    "LAG(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) AS prev_salary, "
    "IFF(salary > 100000, 'high', 'low') AS band, "
    "DATEADD(month, 6, hire_date) AS review_date, "
    "NVL(bonus, 0) AS safe_bonus "
    "FROM employees "
    "QUALIFY ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) <= 3"
)

dml_upsert = (
    "INSERT INTO inventory (product_id, warehouse_id, quantity, updated_at) "
    "VALUES (1, 10, 100, NOW()), (2, 10, 200, NOW()), (3, 20, 50, NOW()) "
    "ON CONFLICT (product_id, warehouse_id) DO UPDATE SET "
    "quantity = EXCLUDED.quantity, updated_at = EXCLUDED.updated_at"
)

type_heavy = (
    "SELECT SAFE_CAST(id AS STRING) AS id_str, "
    "TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_secs, "
    "FORMAT_DATE('%Y-%m', created_date) AS month, "
    "STARTS_WITH(name, 'test') AS is_test, "
    "CAST(price AS FLOAT64) AS price_f, "
    "ARRAY_LENGTH(tags) AS tag_count "
    "FROM events WHERE event_date > '2024-01-01'"
)

subquery_complex = (
    "SELECT d.name AS department, d.budget, "
    "(SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS emp_count, "
    "(SELECT AVG(salary) FROM employees e WHERE e.dept_id = d.id) AS avg_salary "
    "FROM departments d "
    "WHERE d.budget > (SELECT AVG(budget) FROM departments) "
    "ORDER BY d.budget DESC"
)

# TPC-H Q2 — unquoted identifiers so all dialect parsers accept it
tpch_q2 = """\
WITH _e_0 AS (
  SELECT partsupp.ps_partkey AS ps_partkey, partsupp.ps_suppkey AS ps_suppkey, partsupp.ps_supplycost AS ps_supplycost
  FROM partsupp AS partsupp
), _e_1 AS (
  SELECT region.r_regionkey AS r_regionkey, region.r_name AS r_name
  FROM region AS region
  WHERE region.r_name = 'EUROPE'
)
SELECT supplier.s_acctbal AS s_acctbal, supplier.s_name AS s_name, nation.n_name AS n_name, part.p_partkey AS p_partkey
FROM (SELECT part.p_partkey AS p_partkey, part.p_mfgr AS p_mfgr FROM part AS part WHERE part.p_size = 15 AND part.p_type LIKE '%BRASS') AS part
JOIN _e_0 AS partsupp ON part.p_partkey = partsupp.ps_partkey
JOIN (SELECT supplier.s_suppkey AS s_suppkey, supplier.s_name AS s_name, supplier.s_nationkey AS s_nationkey, supplier.s_acctbal AS s_acctbal FROM supplier AS supplier) AS supplier ON supplier.s_suppkey = partsupp.ps_suppkey
CROSS JOIN _e_1 AS region
JOIN (SELECT nation.n_nationkey AS n_nationkey, nation.n_name AS n_name, nation.n_regionkey AS n_regionkey FROM nation AS nation) AS nation ON nation.n_regionkey = region.r_regionkey AND supplier.s_nationkey = nation.n_nationkey
ORDER BY supplier.s_acctbal DESC, nation.n_name, supplier.s_name, part.p_partkey
LIMIT 100"""

# Dialect-neutral CTE+JOIN for the matrix (no dialect-specific functions)
matrix_medium = """\
WITH active_users AS (
  SELECT id, name, email, created_at
  FROM users
  WHERE status = 'active' AND created_at > '2024-01-01'
),
user_orders AS (
  SELECT user_id, COUNT(*) AS order_count, SUM(amount) AS total_amount
  FROM orders
  GROUP BY user_id
  HAVING COUNT(*) > 0
)
SELECT
  a.name,
  a.email,
  COALESCE(u.order_count, 0) AS orders,
  COALESCE(u.total_amount, 0) AS total,
  CASE WHEN u.total_amount > 1000 THEN 'vip' ELSE 'regular' END AS tier
FROM active_users a
LEFT JOIN user_orders u ON a.id = u.user_id
ORDER BY total DESC
LIMIT 50"""

# ---------------------------------------------------------------------------
# Query registry
# ---------------------------------------------------------------------------

# Individual benchmarks: (query_name, sql, read_dialect, write_dialect)
INDIVIDUAL_QUERIES = [
    ("simple_select", simple_select, "postgres", "mysql"),
    ("aggregation", aggregation, "postgres", "bigquery"),
    ("cte_join", cte_join, "mysql", "snowflake"),
    ("window_qualify", window_qualify, "snowflake", "postgres"),
    ("dml_upsert", dml_upsert, "postgres", "mysql"),
    ("type_heavy", type_heavy, "bigquery", "duckdb"),
    ("subquery_complex", subquery_complex, "duckdb", "postgres"),
    ("tpch_q2", tpch_q2, "postgres", "snowflake"),
]

# Dialect matrix: run both matrix_medium and tpch_q2 across these pairs
MATRIX_PAIRS = [
    ("postgres", "mysql"),
    ("postgres", "bigquery"),
    ("mysql", "postgres"),
    ("bigquery", "snowflake"),
    ("snowflake", "duckdb"),
    ("duckdb", "postgres"),
]

MATRIX_QUERIES = [
    ("medium", matrix_medium),
    ("tpch_q2", tpch_q2),
]

# ---------------------------------------------------------------------------
# Transpile functions
# ---------------------------------------------------------------------------


def polyglot_transpile(sql, read, write):
    polyglot_sql.transpile(sql, read=read, write=write)


def sqlglot_transpile(sql, read, write):
    sqlglot.transpile(sql, read=read, write=write)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def format_time(seconds):
    us = seconds * 1_000_000
    if us >= 1_000:
        return f"{us / 1_000:.2f} ms"
    return f"{us:.1f} us"


def format_speedup(ratio):
    if ratio >= 100:
        return f"{ratio:.0f}x"
    if ratio >= 10:
        return f"{ratio:.1f}x"
    return f"{ratio:.2f}x"


def print_comparison(results, supported, skipped):
    """Print a comparison table from collected benchmark results."""
    print(file=sys.stderr)
    print(
        f"Transpile Benchmark: polyglot-sql v{polyglot_sql.__version__} vs "
        f"sqlglot v{sqlglot.__version__}",
        file=sys.stderr,
    )
    print("=" * 80, file=sys.stderr)
    print(file=sys.stderr)

    # --- Individual queries ---
    name_w = 20
    time_w = 14
    header = f"{'Query':<{name_w}} {'polyglot':>{time_w}} {'sqlglot':>{time_w}} {'speedup':>10}"
    print(header, file=sys.stderr)
    print("-" * len(header), file=sys.stderr)

    speedups = []
    for query_name, _, _, _ in INDIVIDUAL_QUERIES:
        p_bench = results.get(f"transpile_polyglot_{query_name}")
        s_bench = results.get(f"transpile_sqlglot_{query_name}")
        p_str = format_time(p_bench.mean()) if p_bench else "n/a"
        s_str = format_time(s_bench.mean()) if s_bench else "n/a"
        if p_bench and s_bench:
            ratio = s_bench.mean() / p_bench.mean()
            speedups.append(ratio)
            ratio_str = format_speedup(ratio)
        else:
            ratio_str = "n/a"
        print(
            f"{query_name:<{name_w}} {p_str:>{time_w}} {s_str:>{time_w}} {ratio_str:>10}",
            file=sys.stderr,
        )

    if speedups:
        geo = exp(sum(log(s) for s in speedups) / len(speedups))
        print(file=sys.stderr)
        print(
            f"  Geometric mean speedup (individual): {format_speedup(geo)}",
            file=sys.stderr,
        )

    # --- Matrix ---
    print(file=sys.stderr)
    print("Dialect Matrix", file=sys.stderr)
    print("-" * 80, file=sys.stderr)

    matrix_speedups = []
    pair_w = 22
    header2 = f"{'Pair':<{pair_w}} {'Query':<10} {'polyglot':>{time_w}} {'sqlglot':>{time_w}} {'speedup':>10}"
    print(header2, file=sys.stderr)
    print("-" * len(header2), file=sys.stderr)

    for query_name, _ in MATRIX_QUERIES:
        for read_d, write_d in MATRIX_PAIRS:
            pair_label = f"{read_d}->{write_d}"
            tag = f"{query_name}_{read_d}_{write_d}"
            p_bench = results.get(f"transpile_polyglot_matrix_{tag}")
            s_bench = results.get(f"transpile_sqlglot_matrix_{tag}")
            p_str = format_time(p_bench.mean()) if p_bench else "n/a"
            s_str = format_time(s_bench.mean()) if s_bench else "n/a"
            if p_bench and s_bench:
                ratio = s_bench.mean() / p_bench.mean()
                matrix_speedups.append(ratio)
                ratio_str = format_speedup(ratio)
            else:
                ratio_str = "n/a"
            print(
                f"{pair_label:<{pair_w}} {query_name:<10} {p_str:>{time_w}} {s_str:>{time_w}} {ratio_str:>10}",
                file=sys.stderr,
            )

    if matrix_speedups:
        geo = exp(sum(log(s) for s in matrix_speedups) / len(matrix_speedups))
        print(file=sys.stderr)
        print(
            f"  Geometric mean speedup (matrix): {format_speedup(geo)}",
            file=sys.stderr,
        )

    # Combined geometric mean
    all_speedups = speedups + matrix_speedups
    if all_speedups:
        geo = exp(sum(log(s) for s in all_speedups) / len(all_speedups))
        print(file=sys.stderr)
        print(
            f"  Geometric mean speedup (overall): {format_speedup(geo)}",
            file=sys.stderr,
        )

    if skipped:
        print(file=sys.stderr)
        print("  Skipped (unsupported):", file=sys.stderr)
        for lib, name, reason in skipped:
            print(f"    - {lib}:{name}: {reason}", file=sys.stderr)

    print(file=sys.stderr)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--core-only",
        action="store_true",
        help="Only benchmark polyglot and sqlglot (no-op, kept for consistency).",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quicker/less stable timings (maps to pyperf --fast).",
    )
    args, remaining = parser.parse_known_args()
    if args.quick and "--fast" not in remaining and "-f" not in remaining:
        remaining = ["--fast", *remaining]
    sys.argv = [sys.argv[0], *remaining]
    return args


def run_benchmarks():
    runner = pyperf.Runner()
    results = {}
    supported = []
    skipped = []

    libs = [
        ("polyglot", polyglot_transpile),
        ("sqlglot", sqlglot_transpile),
    ]

    # --- Individual queries ---
    for lib_name, func in libs:
        for query_name, sql, read_d, write_d in INDIVIDUAL_QUERIES:
            try:
                func(sql, read_d, write_d)
            except Exception as exc:
                skipped.append(
                    (lib_name, query_name, f"{type(exc).__name__}: {exc}")
                )
                continue

            supported.append((lib_name, query_name))
            bench_name = f"transpile_{lib_name}_{query_name}"
            bench = runner.bench_func(bench_name, func, sql, read_d, write_d)
            if bench is not None:
                results[bench_name] = bench

    # --- Dialect matrix ---
    for lib_name, func in libs:
        for query_name, sql in MATRIX_QUERIES:
            for read_d, write_d in MATRIX_PAIRS:
                tag = f"{query_name}_{read_d}_{write_d}"
                try:
                    func(sql, read_d, write_d)
                except Exception as exc:
                    skipped.append(
                        (lib_name, f"matrix_{tag}", f"{type(exc).__name__}: {exc}")
                    )
                    continue

                supported.append((lib_name, f"matrix_{tag}"))
                bench_name = f"transpile_{lib_name}_matrix_{tag}"
                bench = runner.bench_func(bench_name, func, sql, read_d, write_d)
                if bench is not None:
                    results[bench_name] = bench

    # Print comparison table only in the main process (all results collected)
    expected = len(supported)
    if len(results) == expected:
        print_comparison(results, supported, skipped)


if __name__ == "__main__":
    parse_args()
    run_benchmarks()
