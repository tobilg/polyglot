#!/usr/bin/env python3
"""Render Criterion benchmark estimates as a Markdown table."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_GROUP = "rust_parse_quick_equivalent/parse_one"
DEFAULT_QUERIES = ["short", "long", "tpch", "crazy"]


def format_ns(value_ns: float) -> str:
    if value_ns >= 1_000_000_000:
        return f"{value_ns / 1_000_000_000:.2f} s"
    if value_ns >= 1_000_000:
        return f"{value_ns / 1_000_000:.2f} ms"
    if value_ns >= 1_000:
        return f"{value_ns / 1_000:.2f} us"
    return f"{value_ns:.2f} ns"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_query_metrics(root: Path, group: str, query: str) -> tuple[str, str, str, str]:
    new_estimates = load_json(root / group / query / "new" / "estimates.json")

    mean = float(new_estimates["mean"]["point_estimate"])
    std_dev = float(new_estimates["std_dev"]["point_estimate"])
    ci_low = float(new_estimates["mean"]["confidence_interval"]["lower_bound"])
    ci_high = float(new_estimates["mean"]["confidence_interval"]["upper_bound"])

    change_path = root / group / query / "change" / "estimates.json"
    change = "n/a"
    if change_path.exists():
        change_estimates = load_json(change_path)
        delta = float(change_estimates["mean"]["point_estimate"]) * 100.0
        sign = "+" if delta >= 0 else ""
        change = f"{sign}{delta:.2f}%"

    return (
        format_ns(mean),
        format_ns(std_dev),
        f"{format_ns(ci_low)} - {format_ns(ci_high)}",
        change,
    )


def render_report(criterion_dir: Path, group: str, queries: list[str], title: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        f"# {title}",
        "",
        f"- Generated: {timestamp}",
        f"- Source: `{criterion_dir / group}`",
        "",
        "| Query | Mean | Std Dev | 95% CI (mean) | Change vs baseline |",
        "|---|---:|---:|---:|---:|",
    ]

    for query in queries:
        mean, std_dev, ci, change = read_query_metrics(criterion_dir, group, query)
        lines.append(f"| {query} | {mean} | {std_dev} | {ci} | {change} |")

    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--criterion-dir",
        default="target/criterion",
        help="Criterion output directory.",
    )
    parser.add_argument(
        "--group",
        default=DEFAULT_GROUP,
        help="Criterion group path under criterion-dir.",
    )
    parser.add_argument(
        "--queries",
        default=",".join(DEFAULT_QUERIES),
        help="Comma-separated query names in output order.",
    )
    parser.add_argument(
        "--title",
        default="Rust Parsing Benchmark Report",
        help="Markdown report title.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Write report to this file path. If omitted, print to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    criterion_dir = Path(args.criterion_dir)
    queries = [q.strip() for q in args.queries.split(",") if q.strip()]
    report = render_report(criterion_dir, args.group, queries, args.title)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report, encoding="utf-8")
    else:
        print(report, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
