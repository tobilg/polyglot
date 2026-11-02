# Polyglot vs SQLGlot Parsing Benchmark

**Recorded:** 2026-11-02

**Engines:** locally built polyglot-sql 0.14.0 and SQLGlot 30.27.0
with the `sqlglot[c]` 30.27.0 mypyc-compiled native extensions verified active.

**Environment:** Apple M2, 8 cores, 16 GB RAM,
arm64, macOS 27.0.1 (build 26A434), CPython 3.13.9.

**Method:** median of five Python API calls per parser/query using the same
16-query corpus as SQLGlot v30.27.0's upstream benchmark. Polyglot was
built from commit `31b0fc6a050ad5aa80e0f9c912ee8e04f0602dd5` using the production
`python_release` profile (`opt-level=2`, thin LTO, one codegen unit). SQLGlot uses
`parse_one(..., error_level=IGNORE)`; Polyglot uses `parse_one`. Results are
created and discarded without AST traversal or serialization.

| Query | Polyglot | SQLGlot[c] | SQLGlot[c] / Polyglot |
|---|---:|---:|---:|
| TPC-H | 95.2 us | 358.7 us | 3.77x |
| Short | 9.1 us | 26.0 us | 2.85x |
| Deep arithmetic | 359.2 us | 1.464 ms | 4.07x |
| Large `IN` | 20.645 ms | 64.594 ms | 3.13x |
| Values | 21.032 ms | 69.282 ms | 3.29x |
| Many joins | 357.6 us | 1.850 ms | 5.17x |
| Many unions | 2.546 ms | 5.032 ms | 1.98x |
| Nested subqueries | 28.0 us | 113.5 us | 4.05x |
| Many columns | 643.9 us | 1.874 ms | 2.91x |
| Large `CASE` | 1.499 ms | 5.162 ms | 3.44x |
| Complex `WHERE` | 1.150 ms | 3.884 ms | 3.38x |
| Many CTEs | 472.5 us | 2.108 ms | 4.46x |
| Many windows | 795.3 us | 2.872 ms | 3.61x |
| Nested functions | 35.7 us | 78.8 us | 2.20x |
| Large strings | 267.3 us | 759.3 us | 2.84x |
| Many numbers | 4.588 ms | 15.637 ms | 3.41x |

The geometric mean is **3.32x in Polyglot's favor**. Polyglot was
faster on **16/16 queries**.
This is a measurement of these Python API calls on this corpus and machine;
query shapes, build settings, parser semantics, and background activity affect
results. Five calls do not establish confidence intervals.

## Reproduce

```bash
make bench-simple-quick
```

To preserve exact timings as JSON, after building the locked benchmark
environment with the command above:

```bash
uv run --project tools/bench-compare --locked python tools/bench-compare/bench_simple.py \
  --core-only --iterations 5 --json target/parse-median.json
```

Recorded [raw median timings](../tools/bench-compare/results/2026-11-02/parse-median.json)
are in seconds; the `sqlglot` keys refer to the compiled SQLGlot[c] installation.
See the [environment and build metadata](../tools/bench-compare/results/2026-11-02/metadata.json)
for package versions and source hashes.

The [full comparison](current-benchmarks.md) includes all parsers, pure-Python
SQLGlot, transpilation, and charts. It reports fastest-of-up-to-five timings,
so its values and aggregate ratios differ from this median-of-five report.
Focused Rust timing, allocation, Python concurrency, and native-profile results
remain documented in [performance-benchmarks.md](performance-benchmarks.md).
