#!/usr/bin/env python3
"""Render the current benchmark report's six charts from recorded JSON timings.

uv run --no-project --with matplotlib python tools/bench-compare/plot_current_benchmarks.py \
    --results tools/bench-compare/results/2026-11-02
"""

import argparse
import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FixedLocator, NullLocator, ScalarFormatter


QUERIES = {
    "tpch": "TPC-H",
    "short": "Short",
    "deep_arithmetic": "Deep arithmetic",
    "large_in": "Large IN",
    "values": "Values",
    "many_joins": "Many joins",
    "many_unions": "Many unions",
    "nested_subqueries": "Nested subqueries",
    "many_columns": "Many columns",
    "large_case": "Large CASE",
    "complex_where": "Complex WHERE",
    "many_ctes": "Many CTEs",
    "many_windows": "Many windows",
    "nested_functions": "Nested functions",
    "large_strings": "Large strings",
    "many_numbers": "Many numbers",
}
PARSERS = {
    "sqlglot": ("SQLGlot", "sqlglot", "#4b6cb7"),
    "sqlglotc": ("SQLGlot[c]", "sqlglotc", "#f4b42e"),
    "sqltree": ("SQLTree", "sqltree", "#8963ae"),
    "sqlparse": ("SQLParse", "sqlparse", "#cf5d54"),
    "sqlfluff": ("SQLFluff", "sqlfluff", "#829647"),
    "moz_sql_parser": ("moz-sql-parser", "moz-sql-parser", "#b47b54"),
    "sqloxide": ("SQLOxide", "sqloxide", "#46a4b4"),
    "polyglot_sql": ("Polyglot", "polyglot-sql", "#138a77"),
}


def plot(results, metadata, parsers, mode, hybrid, output):
    versions = metadata["versions"]
    fig, ax = plt.subplots(figsize=(19, 8.5), layout="constrained")
    width = 0.82 / len(parsers)
    x = np.arange(len(QUERIES))
    measured = []
    for index, parser in enumerate(parsers):
        label, package, color = PARSERS[parser]
        times = [results.get(f"{parser}:{query}", math.nan) * 1000 for query in QUERIES]
        measured.extend(value for value in times if math.isfinite(value))
        ax.bar(
            x + (index - (len(parsers) - 1) / 2) * width,
            times,
            width=width * 0.94,
            color=color,
            label=f"{label} {versions[package]}",
            zorder=3,
        )
    if hybrid:
        ax.set_yscale("symlog", base=2, linthresh=5, linscale=1)
        top = max(10, 5 * 2 ** math.ceil(math.log2(max(measured) / 5)))
        ticks = [0, 1, 2, 3, 4, 5]
        ticks.extend(5 * 2**power for power in range(1, int(math.log2(top / 5)) + 1))
        ax.yaxis.set_major_locator(FixedLocator(ticks))
        ax.yaxis.set_major_formatter(ScalarFormatter())
        ax.yaxis.set_minor_locator(NullLocator())
        ax.set_ylim(0, top * 1.15)
        ax.axhline(5, linestyle="--", color="#566476", linewidth=1.1, zorder=4)
        scale = "linear 0–5 ms, then logarithmic base 2"
    else:
        ax.set_yscale("log")
        ax.set_ylim(min(measured) * 0.65, max(measured) * 2)
        scale = "logarithmic scale"
    ax.set_xticks(x, QUERIES.values(), rotation=34, ha="right", fontsize=10)
    ax.set_ylabel(f"{mode} time (milliseconds; {scale})", fontsize=11)
    ax.grid(axis="y", which="both", color="#e1e6ed", linewidth=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.margins(x=0.015)
    comparison = "all parsers" if len(parsers) > 2 else "SQLGlot[c] vs Polyglot"
    fig.suptitle(f"{mode} benchmark: {comparison}", fontsize=19, fontweight="bold")
    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, 1.10 if len(parsers) > 2 else 1.07),
        ncol=4 if len(parsers) > 2 else 2,
        frameon=False,
        fontsize=10,
    )
    note = "Missing bars: capability check failed or timed out.  " if len(parsers) > 2 else ""
    fig.supxlabel(
        f"{note}Lower is better | Fastest of up to five calls | "
        f"{metadata['processor']} | Python {metadata['python']} | "
        f"{metadata['recorded_at'][:10]}",
        fontsize=9,
        color="#596474",
    )
    fig.savefig(output, dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path(__file__).resolve().parents[2] / "docs/images",
    )
    args = parser.parse_args()
    metadata = json.loads((args.results / "metadata.json").read_text())
    parse = json.loads((args.results / "parse.json").read_text())
    transpile = json.loads((args.results / "transpile.json").read_text())
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for name, results, parsers, mode in [
        ("core", parse, ["sqlglotc", "polyglot_sql"], "Parsing"),
        ("all", parse, list(PARSERS), "Parsing"),
        ("transpile", transpile, ["sqlglotc", "polyglot_sql"], "Transpilation"),
    ]:
        for hybrid in [False, True]:
            suffix = "-hybrid" if hybrid else ""
            output = args.output_dir / f"current-benchmarks-{name}{suffix}.png"
            plot(results, metadata, parsers, mode, hybrid, output)
            print(output)


if __name__ == "__main__":
    main()
