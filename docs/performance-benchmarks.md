# Performance Hotspot Benchmark Report

Measured on 2026-07-13 at commit `00e20d6` plus the changes described here.

## Environment

- Apple M2, arm64, macOS 26.5.2
- Rust 1.96.0
- Python 3.13.9
- Polyglot 0.5.16
- SQLGlot 30.12.0 installed with the `sqlglot[c]` mypyc-compiled native extensions
- Criterion results use 95% confidence intervals; allocation results use `stats_alloc`

Generated benchmark data is written to `target/performance` and is not committed. Run
`make bench-performance-all` to reproduce the focused benchmark suite.

## Dialect Caching

Built-in tokenizer configurations now use shared immutable `Arc<TokenizerConfig>` values.
The cache is initialized before measurement, so these are steady-state construction costs.

| Case | Before | After | Change |
|---|---:|---:|---:|
| Generic `Dialect::get` | 4.662 us | 9.87 ns | -99.79% |
| PostgreSQL `Dialect::get` | 4.791 us | 9.88 ns | -99.79% |
| BigQuery `Dialect::get` | 4.650 us | 9.87 ns | -99.79% |
| ClickHouse `Dialect::get` | 4.608 us | 9.89 ns | -99.79% |
| Short parse with fresh PostgreSQL dialect | 11.593 us | 6.561 us | -43.36% |

A warmed PostgreSQL construction fell from 264 allocations and 19,975 allocated bytes to
zero allocations. Custom dialect modifiers still receive an owned mutable copy before the
registered runtime configuration is shared.

## Tokenizer Allocation

ASCII input now uses a byte cursor over the source. Unicode input keeps the character-buffer
implementation. Both cursors monomorphize the common scanner, avoiding a per-character mode
branch. The internal parser path stores unchanged token text as ranges into one shared `Arc<str>`;
the public `Token` API remains owned and unchanged. Token-budget and nesting statistics are also
collected during tokenization, avoiding a second full token pass before parsing.

| Tokenize case | Before | After | Change |
|---|---:|---:|---:|
| Short ASCII | 1.770 us | 1.628 us | -8.2% |
| Comments and strings | 1.773 us | 1.478 us | -16.7% |
| TPC-H style | 8.509 us | 7.949 us | -6.6% |
| 20,000-value token list | 2.022 ms | 1.896 ms | -6.1% |
| Unicode | 1.238 us | 1.233 us | no significant change |

| Allocation case | Before | After | Change |
|---|---:|---:|---:|
| Short ASCII tokenize bytes | 4,028 | 3,660 | -9.1% |
| Comment/string tokenize bytes | 3,068 | 2,380 | -22.4% |
| Large tokenize bytes | 7,964,683 | 7,448,955 | -6.5% |
| Large parse bytes | 18,560,265 | 18,044,537 | -2.8% |

End-to-end parsing improved by 1.9% for short ASCII, 6.4% for comments/strings, 5.8% for
TPC-H style SQL, and 7.1% for the large token list. Unicode parsing did not regress.

## Python Concurrency

The previous global 64 MB worker serialized native work. Python now calls the core directly
inside `py.detach`; the core's `stacker` guards remain responsible for recursive entry points.

The strongest isolated comparison is substantial transpilation, where single-caller throughput
was unchanged while parallel scaling was restored:

| Callers | Global worker | Direct detached | Improvement |
|---:|---:|---:|---:|
| 1 | 319 ops/s | 318 ops/s | unchanged |
| 2 | 323 ops/s | 534 ops/s | 1.65x |
| 4 | 323 ops/s | 1,028 ops/s | 3.18x |
| 8 | 325 ops/s | 1,162 ops/s | 3.58x |

The longer five-round final run reached 1,115 ops/s at four callers and 1,430 ops/s at eight.
Parallel parse/transpile correctness and concurrent error tests are part of the Python suite.

## Native Release Profiles

Six FFI candidates were measured across five parse shapes and transpilation. Timings include
PureGo and FFI overhead and are medians of three Go benchmark runs. Speedups are geometric means
relative to the existing `z`/fat-LTO FFI profile.

| Profile | Speedup | Shared library | Gzip | Clean build |
|---|---:|---:|---:|---:|
| `z` / fat | 1.00x | 13.95 MB | 6.47 MB | 5m 34s |
| `s` / fat | 1.93x | 33.86 MB | 9.69 MB | 8m 10s |
| `1` / fat | 1.92x | 96.09 MB | 15.79 MB | 12m 53s |
| `2` / fat | 2.44x | 39.41 MB | 13.56 MB | 11m 00s |
| `2` / thin | 2.48x | 39.41 MB | 13.53 MB | 11m 25s |
| `3` / fat | 2.46x | 41.74 MB | 14.45 MB | 11m 35s |

Every faster FFI candidate exceeds the balanced artifact-growth threshold, so production FFI
remains on `z`/fat LTO. The `native_release` profile remains available for explicit local builds,
and `make bench-native-profiles` reproduces the matrix.

## Python Release Profile

Python was measured separately through the actual `polyglot_sql.parse_one` PyO3 path. The chosen
`python_release` profile inherits all existing release settings but uses `opt-level=2` and thin
LTO. Editable installs continue to use Cargo's `dev` profile, while Maturin wheel builds and all
SQLGlot comparison targets explicitly select `python_release`.

| Metric | `z` / fat | `2` / thin | Change |
|---|---:|---:|---:|
| 16-case Python parse geometric mean | 1.00x | 3.03x | 3.03x faster |
| Native extension | 8.89 MB | 28.85 MB | +224.6% |
| Gzip-compressed extension | 4.97 MB | 9.66 MB | +94.5% |
| Clean extension build | 6m 17s | 10m 10s | +62.1% |

The measured macOS CPython 3.13 wheel is 9.7 MB. This speed/size tradeoff applies only to Python;
the size-oriented global release profile, FFI artifacts, and WASM artifacts are unchanged.

## Current SQLGlot Comparison

Using the production `python_release` profile, the current Python API benchmark gives a geometric
mean of 3.39x in Polyglot's favor across 16 parser stress cases. All comparison targets that build
Polyglot now use the same profile as published Python wheels. See
[`benchmark.md`](benchmark.md) for the complete table and methodology.
