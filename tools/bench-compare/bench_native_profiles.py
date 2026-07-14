#!/usr/bin/env python3
"""Compare size- and speed-optimized native FFI builds."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import platform
import re
import statistics
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_RE = re.compile(
    r"^(BenchmarkNativeProfile(?:Parse|Transpile)(?:/[^\s-]+)?)-\d+\s+\d+\s+(\d+) ns/op",
    re.MULTILINE,
)
MATRIX = [
    ("z-fat", "z", "fat"),
    ("s-fat", "s", "fat"),
    ("1-fat", "1", "fat"),
    ("2-fat", "2", "fat"),
    ("2-thin", "2", "thin"),
    ("3-fat", "3", "fat"),
]


def library_name() -> str:
    if os.name == "nt":
        return "polyglot_sql_ffi.dll"
    if platform.system() == "Darwin":
        return "libpolyglot_sql_ffi.dylib"
    return "libpolyglot_sql_ffi.so"


def measure_variant(
    name: str,
    opt_level: str,
    lto: str,
    target_dir: Path,
    build_seconds: float | None,
) -> dict[str, object]:
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target_dir)
    env["CARGO_PROFILE_FFI_RELEASE_OPT_LEVEL"] = opt_level
    env["CARGO_PROFILE_FFI_RELEASE_LTO"] = lto

    library = target_dir / "ffi_release" / library_name()
    payload = library.read_bytes()

    bench_env = env.copy()
    bench_env["POLYGLOT_SQL_FFI_PATH"] = str(library)
    benchmark = subprocess.run(
        [
            "go",
            "test",
            "-run",
            "^$",
            "-bench",
            "BenchmarkNativeProfile",
            "-benchmem",
            "-count",
            "3",
        ],
        cwd=ROOT / "packages" / "go",
        env=bench_env,
        check=True,
        capture_output=True,
        text=True,
    )

    timings: dict[str, list[int]] = {}
    for benchmark_name, nanoseconds in BENCHMARK_RE.findall(benchmark.stdout):
        operation = benchmark_name.removeprefix("BenchmarkNativeProfile").lower()
        timings.setdefault(operation, []).append(int(nanoseconds))

    return {
        "name": name,
        "opt_level": opt_level,
        "lto": lto,
        "build_seconds": build_seconds,
        "library_bytes": len(payload),
        "library_gzip_bytes": len(gzip.compress(payload, compresslevel=9)),
        "median_ns": {
            operation: statistics.median(values)
            for operation, values in timings.items()
        },
        "go_output": benchmark.stdout,
    }


def build_and_measure_variant(
    name: str, opt_level: str, lto: str, target_dir: Path
) -> dict[str, object]:
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target_dir)
    env["CARGO_PROFILE_FFI_RELEASE_OPT_LEVEL"] = opt_level
    env["CARGO_PROFILE_FFI_RELEASE_LTO"] = lto

    started = time.perf_counter()
    subprocess.run(
        [
            "cargo",
            "build",
            "-p",
            "polyglot-sql-ffi",
            "--profile",
            "ffi_release",
        ],
        cwd=ROOT,
        env=env,
        check=True,
    )
    return measure_variant(
        name,
        opt_level,
        lto,
        target_dir,
        time.perf_counter() - started,
    )


def balanced_decision(variants: list[dict[str, object]]) -> dict[str, object]:
    baseline = next(variant for variant in variants if variant["name"] == "z-fat")
    baseline_times = baseline["median_ns"]
    comparisons = []
    for variant in variants:
        variant_times = variant["median_ns"]
        operations = sorted(set(baseline_times) & set(variant_times))
        speedups = [baseline_times[name] / variant_times[name] for name in operations]
        geometric_speedup = statistics.geometric_mean(speedups)
        raw_growth = variant["library_bytes"] / baseline["library_bytes"] - 1
        gzip_growth = (
            variant["library_gzip_bytes"] / baseline["library_gzip_bytes"] - 1
        )
        qualified = variant is baseline or (
            geometric_speedup >= 1.05 and max(raw_growth, gzip_growth) <= 0.25
        ) or (geometric_speedup >= 1.15 and max(raw_growth, gzip_growth) <= 0.50)
        comparisons.append(
            {
                "name": variant["name"],
                "geometric_speedup": geometric_speedup,
                "raw_size_growth": raw_growth,
                "gzip_size_growth": gzip_growth,
                "qualified": qualified,
            }
        )

    qualified = [comparison for comparison in comparisons if comparison["qualified"]]
    selected = max(qualified, key=lambda comparison: comparison["geometric_speedup"])
    return {
        "selected": selected["name"],
        "comparisons": comparisons,
    }


def write_report(output: Path, variants: list[dict[str, object]]) -> None:
    order = {name: index for index, (name, _, _) in enumerate(MATRIX)}
    variants.sort(key=lambda variant: order[variant["name"]])
    report = {
        "metadata": {
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "variants": variants,
        "decision": (
            balanced_decision(variants)
            if any(variant["name"] == "z-fat" for variant in variants)
            else None
        ),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def clean_profile_target(target_dir: Path) -> None:
    subprocess.run(
        ["cargo", "clean", "--target-dir", str(target_dir)],
        cwd=ROOT,
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--only", action="append", choices=[name for name, _, _ in MATRIX])
    parser.add_argument("--measure-existing", action="store_true")
    args = parser.parse_args()

    target_dir = ROOT / "target" / "performance" / "native-profile-build"
    selected_names = set(args.only or [name for name, _, _ in MATRIX])
    if args.measure_existing and len(selected_names) != 1:
        parser.error("--measure-existing requires exactly one --only variant")

    variants = []
    if args.output.exists():
        variants = json.loads(args.output.read_text(encoding="utf-8"))["variants"]
    completed = {variant["name"] for variant in variants}

    for name, opt_level, lto in MATRIX:
        if name not in selected_names or name in completed:
            continue
        try:
            if args.measure_existing:
                variant = measure_variant(name, opt_level, lto, target_dir, None)
            else:
                variant = build_and_measure_variant(name, opt_level, lto, target_dir)
            variants.append(variant)
            write_report(args.output, variants)
        finally:
            clean_profile_target(target_dir)

    write_report(args.output, variants)
    report = json.loads(args.output.read_text(encoding="utf-8"))
    print(json.dumps(report["decision"], sort_keys=True))


if __name__ == "__main__":
    main()
