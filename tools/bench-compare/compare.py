#!/usr/bin/env python3
"""Run polyglot-core and sqlglot benchmarks, then print a comparison table.

Run with: uv run python3 tools/bench-compare/compare.py
"""

import json
import os
import subprocess
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def run_rust_bench():
    print("Running polyglot-core benchmarks (release build)...", file=sys.stderr)
    result = subprocess.run(
        ["cargo", "run", "--example", "bench_json", "-p", "polyglot-core", "--release"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print("Rust benchmark failed:", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return json.loads(result.stdout)


def run_python_bench():
    print("Running sqlglot benchmarks...", file=sys.stderr)
    result = subprocess.run(
        ["uv", "run", "--with", "sqlglot", "python3",
         os.path.join(PROJECT_ROOT, "tools", "bench-compare", "bench_sqlglot.py")],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print("Python benchmark failed:", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return json.loads(result.stdout)


def make_key(b):
    return (b["operation"], b["query_size"], b["read_dialect"], b.get("write_dialect"))


def format_us(us):
    if us >= 1_000_000:
        return f"{us / 1_000_000:.2f}s"
    if us >= 1_000:
        return f"{us / 1_000:.2f}ms"
    return f"{us:.1f}us"


def format_speedup(ratio):
    if ratio >= 100:
        return f"{ratio:.0f}x"
    if ratio >= 10:
        return f"{ratio:.1f}x"
    return f"{ratio:.2f}x"


def describe_bench(b):
    op = b["operation"]
    size = b["query_size"]
    read_d = b["read_dialect"]
    write_d = b.get("write_dialect")
    if op == "transpile" and write_d:
        return f"{op}/{size} ({read_d}->{write_d})"
    return f"{op}/{size}"


def print_table(rust_data, python_data):
    rust_map = {make_key(b): b for b in rust_data["benchmarks"]}
    python_map = {make_key(b): b for b in python_data["benchmarks"]}

    print()
    print(f"Polyglot-Core vs SQLGlot Performance Comparison")
    print(f"================================================")
    print(f"  polyglot-core: v{rust_data['version']}")
    print(f"  sqlglot:       v{python_data['version']}")
    print()

    # Column widths
    desc_w = 42
    mean_w = 12
    speedup_w = 10

    header = (
        f"{'Benchmark':<{desc_w}} "
        f"{'Rust (mean)':>{mean_w}} "
        f"{'Python (mean)':>{mean_w}} "
        f"{'Speedup':>{speedup_w}}"
    )
    print(header)
    print("-" * len(header))

    current_op = None
    speedups = []

    for key in rust_map:
        rust_b = rust_map[key]
        python_b = python_map.get(key)
        if python_b is None:
            continue

        op = rust_b["operation"]
        if op != current_op:
            if current_op is not None:
                print()
            current_op = op

        desc = describe_bench(rust_b)
        rust_mean = rust_b["mean_us"]
        python_mean = python_b["mean_us"]

        if rust_mean > 0:
            speedup = python_mean / rust_mean
        else:
            speedup = float("inf")

        speedups.append(speedup)

        row = (
            f"{desc:<{desc_w}} "
            f"{format_us(rust_mean):>{mean_w}} "
            f"{format_us(python_mean):>{mean_w}} "
            f"{format_speedup(speedup):>{speedup_w}}"
        )
        print(row)

    print()
    if speedups:
        avg_speedup = sum(speedups) / len(speedups)
        min_speedup = min(speedups)
        max_speedup = max(speedups)
        # Geometric mean for ratios
        from math import log, exp
        geo_mean = exp(sum(log(s) for s in speedups) / len(speedups))
        print(f"  Geometric mean speedup: {format_speedup(geo_mean)}")
        print(f"  Arithmetic mean:        {format_speedup(avg_speedup)}")
        print(f"  Range:                  {format_speedup(min_speedup)} - {format_speedup(max_speedup)}")
    print()


def main():
    rust_data = run_rust_bench()
    python_data = run_python_bench()
    print_table(rust_data, python_data)


if __name__ == "__main__":
    main()
