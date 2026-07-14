#!/usr/bin/env python3
"""Measure Polyglot Python parse/transpile scaling across caller threads."""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polyglot_sql

SHORT = "SELECT a, b, SUM(c) AS total FROM events GROUP BY a, b"
SUBSTANTIAL = " UNION ALL ".join(
    f"SELECT {value} AS id, 'value_{value}' AS value FROM events_{value}"
    for value in range(150)
)


def parse(sql: str) -> object:
    return polyglot_sql.parse(sql, dialect="postgres")


def transpile(sql: str) -> object:
    return polyglot_sql.transpile(sql, read="postgres", write="duckdb")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * fraction))
    return ordered[index]


def run_case(
    operation_name: str,
    operation,
    sql: str,
    concurrency: int,
    requests: int,
    rounds: int,
) -> dict[str, object]:
    for _ in range(max(2, concurrency)):
        operation(sql)

    throughputs: list[float] = []
    latencies: list[float] = []
    failures: list[str] = []

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for _ in range(rounds):
            def invoke() -> float:
                started = time.perf_counter_ns()
                operation(sql)
                return (time.perf_counter_ns() - started) / 1_000_000

            started = time.perf_counter()
            futures = [executor.submit(invoke) for _ in range(requests)]
            for future in as_completed(futures):
                try:
                    latencies.append(future.result())
                except Exception as error:  # pragma: no cover - diagnostic path
                    failures.append(f"{type(error).__name__}: {error}")
            elapsed = time.perf_counter() - started
            throughputs.append(requests / elapsed)

    return {
        "operation": operation_name,
        "concurrency": concurrency,
        "requests_per_round": requests,
        "rounds": rounds,
        "throughput_ops_s": statistics.median(throughputs),
        "latency_p50_ms": statistics.median(latencies),
        "latency_p95_ms": percentile(latencies, 0.95),
        "failures": failures,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    requests = 8 if args.quick else 40
    rounds = 2 if args.quick else 5
    results = []
    for name, operation in (("parse", parse), ("transpile", transpile)):
        for size, sql in (("short", SHORT), ("substantial", SUBSTANTIAL)):
            for concurrency in (1, 2, 4, 8):
                result = run_case(
                    f"{name}_{size}", operation, sql, concurrency, requests, rounds
                )
                results.append(result)
                print(json.dumps(result, sort_keys=True))

    report = {
        "metadata": {
            "python": sys.version,
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "polyglot": getattr(polyglot_sql, "__version__", "unknown"),
        },
        "results": results,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
