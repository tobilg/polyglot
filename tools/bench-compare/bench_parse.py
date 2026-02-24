#!/usr/bin/env python3
"""Parse benchmark comparing polyglot-sql (Rust) vs sqlglot (Python/C).

Mirrors sqlglot's benchmarks/parse.py with the same queries, adding polyglot-sql
as an additional parser.

Run via make from the project root:

    make bench-parse

Or directly from the repository root:

    uv sync --project tools/bench-compare
    uv run --project tools/bench-compare python3 tools/bench-compare/bench_parse.py --quiet
    uv run --project tools/bench-compare python3 tools/bench-compare/bench_parse.py --quiet --core-only
    uv run --project tools/bench-compare python3 tools/bench-compare/bench_parse.py --quiet --core-only --quick
"""

import argparse
import collections
import collections.abc
import os
import sys
from math import exp, log

import pyperf

import polyglot_sql
import sqlglot

CORE_ONLY_BOOT = (
    "--core-only" in sys.argv
    or os.environ.get("POLYGLOT_BENCH_CORE_ONLY") == "1"
)

if not CORE_ONLY_BOOT:
    try:
        import sqlparse
    except ImportError:
        sqlparse = None

    try:
        import sqlfluff
    except ImportError:
        sqlfluff = None

    try:
        import sqloxide
    except ImportError:
        sqloxide = None

    try:
        import sqltree
    except ImportError:
        sqltree = None

    try:
        # moz_sql_parser uses collections.Iterable on older releases.
        collections.Iterable = collections.abc.Iterable
        import moz_sql_parser
    except ImportError:
        moz_sql_parser = None
else:
    sqlparse = None
    sqlfluff = None
    sqloxide = None
    sqltree = None
    moz_sql_parser = None

# ---------------------------------------------------------------------------
# Queries (identical to sqlglot/benchmarks/parse.py)
# ---------------------------------------------------------------------------

long = """
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
"""

short = "SELECT 1 AS a, CASE WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END AS b, c FROM x"

crazy = "SELECT 1+"
crazy += "+".join(str(i) for i in range(500))
crazy += " AS a, 2*"
crazy += "*".join(str(i) for i in range(500))
crazy += " AS b FROM x"

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

QUERY_NAMES = ["short", "long", "tpch", "crazy"]
QUERIES = {"tpch": tpch, "short": short, "long": long, "crazy": crazy}

# ---------------------------------------------------------------------------
# Parse functions
# ---------------------------------------------------------------------------


def _set_sqlglot_rs_tokenizer(enabled):
    tokens = getattr(sqlglot, "tokens", None)
    if tokens is not None and hasattr(tokens, "USE_RS_TOKENIZER"):
        tokens.USE_RS_TOKENIZER = enabled


def sqlglot_parse(sql):
    """Parse with sqlglot (v29) using the C tokenizer implementation."""
    _set_sqlglot_rs_tokenizer(True)
    sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)


def polyglot_parse(sql):
    """Parse with polyglot-sql (Rust native extension via PyO3)."""
    polyglot_sql.parse_one(sql)


def sqlparse_parse(sql):
    """Parse with sqlparse."""
    sqlparse.parse(sql)


def sqlfluff_parse(sql):
    """Parse with sqlfluff."""
    sqlfluff.parse(sql)


def sqloxide_parse(sql):
    """Parse with sqloxide."""
    sqloxide.parse_sql(sql, dialect="ansi")


def sqltree_parse(sql):
    """Parse with sqltree."""
    sqltree.api.sqltree(sql.replace('"', "`").replace("''", '"'))


def moz_sql_parser_parse(sql):
    """Parse with moz_sql_parser."""
    moz_sql_parser.parse(sql)


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


def _format_name(lib_name):
    return {
        "polyglot": "polyglot",
        "sqlglot": "sqlglot",
        "sqlparse": "sqlparse",
        "sqlfluff": "sqlfluff",
        "sqloxide": "sqloxide",
        "sqltree": "sqltree",
        "moz_sql_parser": "moz-sql-parser",
    }.get(lib_name, lib_name)


def print_comparison(results, libs, supported_pairs, skipped_pairs):
    """Print a comparison table from collected benchmark results."""
    print(file=sys.stderr)
    print(
        f"Parse Benchmark: polyglot-sql v{polyglot_sql.__version__} vs other parsers",
        file=sys.stderr,
    )
    print("=" * 72, file=sys.stderr)
    versions = [f"sqlglot={sqlglot.__version__}"]
    if sqlparse is not None:
        versions.append(f"sqlparse={getattr(sqlparse, '__version__', 'unknown')}")
    if sqlfluff is not None:
        versions.append(f"sqlfluff={getattr(sqlfluff, '__version__', 'unknown')}")
    if sqloxide is not None:
        versions.append(f"sqloxide={getattr(sqloxide, '__version__', 'unknown')}")
    if sqltree is not None:
        versions.append(f"sqltree={getattr(sqltree, '__version__', 'unknown')}")
    if moz_sql_parser is not None:
        versions.append(f"moz_sql_parser={getattr(moz_sql_parser, '__version__', 'unknown')}")
    print("  " + ", ".join(versions), file=sys.stderr)
    print(file=sys.stderr)

    name_w = 10
    time_w = 12
    active_libs = [name for name in libs.keys() if any((name, q) in supported_pairs for q in QUERY_NAMES)]
    header = f"{'Query':<{name_w}} " + " ".join(
        f"{_format_name(name):>{time_w}}" for name in active_libs
    )
    print(header, file=sys.stderr)
    print("-" * len(header), file=sys.stderr)

    speedups_by_lib = {}
    for query_name in QUERY_NAMES:
        row_parts = [f"{query_name:<{name_w}}"]
        baseline = results.get(f"parse_polyglot_{query_name}")
        baseline_mean = baseline.mean() if baseline is not None else None

        for lib_name in active_libs:
            bench = results.get(f"parse_{lib_name}_{query_name}")
            if bench is None:
                row_parts.append(f"{'n/a':>{time_w}}")
                continue
            mean = bench.mean()
            row_parts.append(f"{format_time(mean):>{time_w}}")

            if baseline_mean and lib_name != "polyglot":
                speedup = mean / baseline_mean if baseline_mean > 0 else float("inf")
                speedups_by_lib.setdefault(lib_name, []).append(speedup)

        row = " ".join(row_parts)
        print(row, file=sys.stderr)

    print(file=sys.stderr)
    for lib_name in active_libs:
        if lib_name == "polyglot":
            continue
        speedups = speedups_by_lib.get(lib_name, [])
        if not speedups:
            continue
        geo_mean = exp(sum(log(s) for s in speedups) / len(speedups))
        print(file=sys.stderr)
        print(
            f"  Geometric mean speedup vs polyglot ({_format_name(lib_name)} / polyglot): "
            f"{format_speedup(geo_mean)}",
            file=sys.stderr,
        )

    if skipped_pairs:
        print(file=sys.stderr)
        print("  Skipped parser/query pairs (unsupported):", file=sys.stderr)
        for lib_name, query_name, reason in skipped_pairs:
            print(f"  - {_format_name(lib_name)}:{query_name}: {reason}", file=sys.stderr)
    print(file=sys.stderr)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

BASE_LIBS = {
    "polyglot": polyglot_parse,
    "sqlglot": sqlglot_parse,
}

OPTIONAL_LIBS = {}
if sqlparse is not None:
    OPTIONAL_LIBS["sqlparse"] = sqlparse_parse
if sqlfluff is not None:
    OPTIONAL_LIBS["sqlfluff"] = sqlfluff_parse
if sqloxide is not None:
    OPTIONAL_LIBS["sqloxide"] = sqloxide_parse
if sqltree is not None:
    OPTIONAL_LIBS["sqltree"] = sqltree_parse
if moz_sql_parser is not None:
    OPTIONAL_LIBS["moz_sql_parser"] = moz_sql_parser_parse


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--core-only",
        action="store_true",
        help="Only benchmark polyglot and sqlglot.",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quicker/less stable timings (maps to pyperf --fast).",
    )
    args, remaining = parser.parse_known_args()
    if args.core_only:
        # Ensure pyperf worker subprocesses inherit the same mode.
        os.environ["POLYGLOT_BENCH_CORE_ONLY"] = "1"
    if args.quick and "--fast" not in remaining and "-f" not in remaining:
        # pyperf fast mode: fewer samples/warmups, much faster but noisier.
        remaining = ["--fast", *remaining]
    # Keep pyperf CLI flags functional by passing unknown args back.
    sys.argv = [sys.argv[0], *remaining]
    return args


def run_benchmarks(core_only=False):
    libs = dict(BASE_LIBS)
    if not core_only:
        libs.update(OPTIONAL_LIBS)

    runner = pyperf.Runner()
    results = {}
    supported_pairs = []
    skipped_pairs = []

    for lib_name, parse_func in libs.items():
        for query_name, sql in QUERIES.items():
            try:
                parse_func(sql)
            except Exception as exc:
                skipped_pairs.append((lib_name, query_name, f"{type(exc).__name__}: {exc}"))
                continue

            supported_pairs.append((lib_name, query_name))
            bench_name = f"parse_{lib_name}_{query_name}"
            bench = runner.bench_func(bench_name, parse_func, sql)
            if bench is not None:
                results[bench_name] = bench

    # Print comparison table only when all benchmarks have completed (main process)
    expected = len(supported_pairs)
    if len(results) == expected:
        print_comparison(results, libs, set(supported_pairs), skipped_pairs)


if __name__ == "__main__":
    args = parse_args()
    run_benchmarks(core_only=args.core_only)
