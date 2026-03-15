#!/usr/bin/env python3
"""Simple parse benchmark comparing polyglot-sql vs sqlglot and other parsers.

Uses the same timing approach as sqlglot/benchmarks/parse.py: median-of-N,
no subprocesses, no pyperf.

Run from the repository root:

    uv sync --project tools/bench-compare
    uv run --project tools/bench-compare python3 tools/bench-compare/bench_simple.py
    uv run --project tools/bench-compare python3 tools/bench-compare/bench_simple.py --core-only
"""

import argparse
import inspect
import os
import subprocess
import sys
import tempfile
import time
from math import exp, log

import polyglot_sql
import sqlglot

# ---------------------------------------------------------------------------
# Queries (identical to sqlglot/benchmarks/parse.py)
# ---------------------------------------------------------------------------

large_in = (
    "SELECT * FROM t WHERE x IN (" + ", ".join(f"'s{i}'" for i in range(20000)) + ")"
    " OR y IN (" + ", ".join(str(i) for i in range(20000)) + ")"
)

values = "INSERT INTO t VALUES " + ", ".join(
    "(" + ", ".join(f"'s{i}_{j}'" if j % 2 else str(i * 20 + j) for j in range(20)) + ")"
    for i in range(2000)
)

many_joins = "SELECT * FROM t0" + "".join(
    f"\nJOIN t{i} ON t{i}.id = t{i - 1}.id" for i in range(1, 200)
)

many_unions = "\nUNION ALL\n".join(f"SELECT {i} AS a, 's{i}' AS b FROM t{i}" for i in range(500))

short = "SELECT 1 AS a, CASE WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END AS b, c FROM x"

deep_arithmetic = "SELECT 1+"
deep_arithmetic += "+".join(str(i) for i in range(500))
deep_arithmetic += " AS a, 2*"
deep_arithmetic += "*".join(str(i) for i in range(500))
deep_arithmetic += " AS b FROM x"

nested_subqueries = (
    "SELECT * FROM " + "".join("(SELECT * FROM " for _ in range(20)) + "t" + ")" * 20
)

many_columns = "SELECT " + ", ".join(f"c{i}" for i in range(1000)) + " FROM t"

large_case = (
    "SELECT CASE " + " ".join(f"WHEN x = {i} THEN {i}" for i in range(1000)) + " ELSE -1 END FROM t"
)

complex_where = "SELECT * FROM t WHERE " + " AND ".join(
    f"(c{i} > {i} OR c{i} LIKE '%s{i}%' OR c{i} BETWEEN {i} AND {i+10} OR c{i} IS NULL)"
    for i in range(200)
)

many_ctes = (
    "WITH "
    + ", ".join(f"t{i} AS (SELECT {i} AS a FROM t{i-1 if i else 'base'})" for i in range(200))
    + " SELECT * FROM t199"
)

many_windows = (
    "SELECT "
    + ", ".join(
        f"SUM(c{i}) OVER (PARTITION BY p{i % 10} ORDER BY o{i % 5}) AS w{i}" for i in range(200)
    )
    + " FROM t"
)

nested_functions = "SELECT " + "COALESCE(" * 20 + "x" + ", NULL)" * 20 + " FROM t"

large_strings = "SELECT " + ", ".join(f"'{'x' * 100}'" for i in range(500)) + " FROM t"

many_numbers = "SELECT " + ", ".join(str(i) for i in range(10000)) + " FROM t"

tpch = """
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

QUERIES = {
    "tpch": tpch,
    "short": short,
    "deep_arithmetic": deep_arithmetic,
    "large_in": large_in,
    "values": values,
    "many_joins": many_joins,
    "many_unions": many_unions,
    "nested_subqueries": nested_subqueries,
    "many_columns": many_columns,
    "large_case": large_case,
    "complex_where": complex_where,
    "many_ctes": many_ctes,
    "many_windows": many_windows,
    "nested_functions": nested_functions,
    "large_strings": large_strings,
    "many_numbers": many_numbers,
}

# ---------------------------------------------------------------------------
# Parser definitions
# ---------------------------------------------------------------------------


def polyglot_sql_parse(sql):
    polyglot_sql.parse_one(sql)


def sqlglot_parse(sql):
    sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)


DISPLAY_NAMES = {
    "polyglot_sql": "polyglot-sql",
    "sqlglot": "sqlglot",
    "sqloxide": "sqloxide",
    "sqlparse": "sqlparse",
    "sqlfluff": "sqlfluff",
    "sqltree": "sqltree",
    "moz_sql_parser": "moz_sql_parser",
}


# ---------------------------------------------------------------------------
# Third-party parser discovery (from sqlglot/benchmarks/parse.py)
# ---------------------------------------------------------------------------


def _make_third_party_parsers():
    parsers = {}

    def sqloxide_parse(sql):
        import sqloxide
        sqloxide.parse_sql(sql, dialect="ansi")

    def sqlparse_parse(sql):
        import sqlparse
        sqlparse.parse(sql)

    def sqlfluff_parse(sql):
        import sqlfluff
        sqlfluff.parse(sql)

    def sqltree_parse(sql):
        import sqltree
        sqltree.api.sqltree(sql.replace('"', "`").replace("''", '"'))

    def moz_sql_parser_parse(sql):
        import moz_sql_parser
        moz_sql_parser.parse(sql)

    parsers["sqloxide"] = sqloxide_parse
    parsers["sqlparse"] = sqlparse_parse
    parsers["sqlfluff"] = sqlfluff_parse
    parsers["sqltree"] = sqltree_parse
    parsers["moz_sql_parser"] = moz_sql_parser_parse
    return parsers


def _check_parser(parse_fn, queries):
    """Check which queries a parser can handle (one subprocess per query to isolate segfaults)."""
    fn_name = parse_fn.__name__
    source = inspect.getsource(parse_fn)
    supported = set()
    installed = None

    for name, sql in queries.items():
        code = f"""import signal

def _timeout(signum, frame):
    raise TimeoutError()

signal.signal(signal.SIGALRM, _timeout)
signal.alarm(5)

{source}

{fn_name}({repr(sql)})
"""
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", suffix=".py", delete=True) as f:
            f.write(code)
            f.flush()
            try:
                result = subprocess.run([sys.executable, f.name], capture_output=True, timeout=10)
            except subprocess.TimeoutExpired:
                installed = True
                continue
            if b"ModuleNotFoundError" in result.stderr:
                return None
            installed = True
            if result.returncode == 0:
                supported.add(name)

    return supported if installed else None


def _discover_parsers():
    """Discover available third-party parsers and which queries they support."""
    third_party = _make_third_party_parsers()
    valid_pairs = set()
    available = {}
    for parser_name, parse_fn in third_party.items():
        supported = _check_parser(parse_fn, QUERIES)
        if supported is None:
            continue
        for query_name in supported:
            valid_pairs.add((parser_name, query_name))
        available[parser_name] = parse_fn
    return available, valid_pairs


# ---------------------------------------------------------------------------
# Benchmarking (same approach as sqlglot/benchmarks/parse.py)
# ---------------------------------------------------------------------------


def _bench(name, fn, *args, iterations=5):
    """Benchmark fn(*args) and return the median time in seconds."""
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn(*args)
        times.append(time.perf_counter() - t0)
        if times[-1] > 1:
            break
    times.sort()
    median = times[len(times) // 2]
    print(f"  {name}: {_fmt_time(median)}")
    return median


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _fmt_ratio(ratio):
    return f"{ratio:.2f}"


def _fmt_time(seconds):
    if seconds >= 1:
        return f"{seconds:.2f} sec"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.2f} ms"
    return f"{seconds * 1e6:.1f} us"


def _fmt_speedup(ratio):
    if ratio >= 100:
        return f"{ratio:.0f}x"
    if ratio >= 10:
        return f"{ratio:.1f}x"
    return f"{ratio:.2f}x"


# ---------------------------------------------------------------------------
# Table printing
# ---------------------------------------------------------------------------


def _print_table(all_parsers, results, valid_pairs):
    base = "polyglot_sql"
    query_width = max(len(q) for q in QUERIES)
    query_width = max(query_width, len("Query"))

    # Pre-compute all cells
    cells = {}
    for query_name in QUERIES:
        base_time = results.get(f"{base}:{query_name}")
        for p in all_parsers:
            t = results.get(f"{p}:{query_name}")
            if t is not None and base_time:
                ratio = t / base_time
                cells[(p, query_name)] = f"{_fmt_time(t)} ({_fmt_ratio(ratio)})"
            elif t is not None:
                cells[(p, query_name)] = _fmt_time(t)
            else:
                cells[(p, query_name)] = "N/A"

    col_widths = {}
    for p in all_parsers:
        name = DISPLAY_NAMES.get(p, p)
        w = len(name)
        for query_name in QUERIES:
            w = max(w, len(cells.get((p, query_name), "")))
        col_widths[p] = w

    print()
    print(
        f"Parse Benchmark: polyglot-sql v{polyglot_sql.__version__} "
        f"vs sqlglot v{sqlglot.__version__}"
    )
    print("=" * 72)
    print()

    header = f"| {'Query':>{query_width}} |"
    sep = f"| {'-' * query_width} |"
    for p in all_parsers:
        name = DISPLAY_NAMES.get(p, p)
        header += f" {name:>{col_widths[p]}} |"
        sep += f" {'-' * col_widths[p]} |"

    print(header)
    print(sep)

    for query_name in QUERIES:
        row = f"| {query_name:>{query_width}} |"
        for p in all_parsers:
            row += f" {cells.get((p, query_name), 'N/A'):>{col_widths[p]}} |"
        print(row)

    # Geometric mean speedup per parser vs polyglot-sql
    print()
    for p in all_parsers:
        if p == base:
            continue
        ratios = []
        for query_name in QUERIES:
            base_time = results.get(f"{base}:{query_name}")
            other_time = results.get(f"{p}:{query_name}")
            if base_time and other_time:
                ratios.append(other_time / base_time)
        if ratios:
            geo_mean = exp(sum(log(r) for r in ratios) / len(ratios))
            print(
                f"  Geometric mean ({DISPLAY_NAMES.get(p, p)} / polyglot-sql): "
                f"{_fmt_speedup(geo_mean)}"
            )

    # Skipped pairs
    skipped = []
    for p in all_parsers:
        for q in QUERIES:
            if (p, q) not in valid_pairs and p != base:
                skipped.append((p, q))
    if skipped:
        print()
        print("  Skipped (unsupported):")
        for p, q in skipped:
            print(f"    {DISPLAY_NAMES.get(p, p)}:{q}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Simple parse benchmark")
    parser.add_argument(
        "--core-only",
        action="store_true",
        help="Only benchmark polyglot-sql and sqlglot.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of iterations per benchmark (default: 5).",
    )
    args = parser.parse_args()

    results = {}
    valid_pairs = set()

    # Core parsers
    parsers = {
        "polyglot_sql": polyglot_sql_parse,
        "sqlglot": sqlglot_parse,
    }

    # All core pairs are valid
    for p in parsers:
        for q in QUERIES:
            valid_pairs.add((p, q))

    # Third-party parsers
    if not args.core_only:
        print("Discovering third-party parsers...", flush=True)
        available, tp_valid = _discover_parsers()
        parsers.update(available)
        valid_pairs |= tp_valid

    # Run benchmarks
    for parser_name, parse_fn in parsers.items():
        print(f"\n=== {DISPLAY_NAMES.get(parser_name, parser_name)} ===", flush=True)
        for query_name, sql in QUERIES.items():
            if (parser_name, query_name) not in valid_pairs:
                continue
            key = f"{parser_name}:{query_name}"
            results[key] = _bench(key, parse_fn, sql, iterations=args.iterations)

    _print_table(list(parsers.keys()), results, valid_pairs)


if __name__ == "__main__":
    main()
