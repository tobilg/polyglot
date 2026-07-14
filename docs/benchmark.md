# Polyglot vs SQLGlot Parsing Benchmark

**Engines:** polyglot-sql 0.5.16 and SQLGlot 30.12.0 installed with the
`sqlglot[c]` mypyc-compiled native extensions

**Environment:** Apple M2, arm64, macOS 26.5.2, Python 3.13.9

**Method:** median-of-five Python API calls using the shared SQLGlot parser corpus.
Polyglot was built with the production `python_release` profile (`opt-level=2`, thin LTO).

| Query | Polyglot | SQLGlot | SQLGlot / Polyglot |
|---|---:|---:|---:|
| TPC-H | 103.5 us | 389.3 us | 3.76x |
| Short | 9.3 us | 28.0 us | 3.00x |
| Deep arithmetic | 377.7 us | 1.29 ms | 3.43x |
| Large `IN` | 17.76 ms | 57.77 ms | 3.25x |
| Values | 19.14 ms | 62.33 ms | 3.26x |
| Many joins | 374.0 us | 1.59 ms | 4.26x |
| Many unions | 2.29 ms | 5.12 ms | 2.23x |
| Nested subqueries | 25.8 us | 111.5 us | 4.32x |
| Many columns | 531.7 us | 1.80 ms | 3.39x |
| Large `CASE` | 1.38 ms | 5.25 ms | 3.80x |
| Complex `WHERE` | 1.00 ms | 3.82 ms | 3.81x |
| Many CTEs | 452.5 us | 1.97 ms | 4.35x |
| Many windows | 800.1 us | 2.74 ms | 3.43x |
| Nested functions | 35.1 us | 75.9 us | 2.16x |
| Large strings | 232.8 us | 756.5 us | 3.25x |
| Many numbers | 3.97 ms | 13.99 ms | 3.52x |

The geometric mean is **3.39x in Polyglot's favor** for this corpus and environment. This is
not a universal speed claim; results depend on query shapes, hardware, versions, and build
profiles.

## Reproduce

```bash
make bench-simple-quick
```

Focused Rust timing, allocation, Python concurrency, and native-profile results are documented
in [`performance-benchmarks.md`](performance-benchmarks.md).
