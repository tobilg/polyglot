# Polyglot-Core vs SQLGlot Performance Report

**Engines:** polyglot-sql v0.1.2 (Rust/WASM) vs sqlglot v28.10.1 (Python)
**Overall geometric mean speedup: 8.70x**

## Parse (SQL string → AST)

| Query | Rust | Python | Speedup |
|---|---|---|---|
| simple (SELECT a, b, c) | 7.8us | 100.6us | **12.9x** |
| medium (5 cols, JOIN, GROUP BY) | 53.4us | 592.2us | **11.1x** |
| complex (3 CTEs, subquery) | 221.2us | 2.11ms | **9.54x** |

Parsing shows a **consistent ~10-13x speedup** across all query sizes. The linear scaling in both engines suggests neither has algorithmic bottlenecks — the difference is purely language-level overhead (Rust native vs CPython interpreter).

## Generate (AST → SQL string)

| Query | Rust | Python | Speedup |
|---|---|---|---|
| simple | 0.5us | 51.3us | **101x** |
| medium | 3.6us | 293.5us | **81.8x** |
| complex | 13.4us | 1.04ms | **77.1x** |

Generation is the **biggest win at ~86x average**. This makes sense — SQL generation is primarily string concatenation and tree traversal, both areas where Rust's zero-cost abstractions (pre-allocated `String` buffers, stack-based dispatch via `match`) vastly outperform Python's dynamic dispatch and repeated small allocations. The simple query generates in 500 nanoseconds — essentially a few cache lines of work.

## Roundtrip (parse → generate → re-parse)

| Query | Rust | Python | Speedup |
|---|---|---|---|
| simple | 15.3us | 236.4us | **15.4x** |
| medium | 98.8us | 1.39ms | **14.0x** |
| complex | 374.2us | 4.90ms | **13.1x** |

Roundtrip is **~13-15x faster**, which is a weighted blend of parsing (10-13x) and generation (77-101x). Since parsing dominates the total time (it's much slower than generation), the combined ratio skews closer to the parse speedup. This metric matters for validation workflows where SQL is parsed, normalized, and re-emitted.

## Transpile (parse with read dialect → transform → generate with write dialect)

### Simple queries (1000 iterations)

| Dialect Pair | Rust | Python | Speedup |
|---|---|---|---|
| PostgreSQL → MySQL | 76.7us | 137.4us | **1.79x** |
| PostgreSQL → BigQuery | 77.8us | 201.8us | **2.59x** |
| MySQL → PostgreSQL | 78.3us | 135.3us | **1.73x** |
| BigQuery → Snowflake | 78.5us | 275.9us | **3.51x** |
| Snowflake → DuckDB | 78.7us | 133.9us | **1.70x** |
| Generic → PostgreSQL | 78.6us | 127.5us | **1.62x** |

### Medium queries (500 iterations)

| Dialect Pair | Rust | Python | Speedup |
|---|---|---|---|
| PostgreSQL → MySQL | 131.6us | 743.1us | **5.65x** |
| PostgreSQL → BigQuery | 137.1us | 976.8us | **7.13x** |
| MySQL → PostgreSQL | 128.8us | 682.0us | **5.29x** |
| BigQuery → Snowflake | 125.9us | 1.41ms | **11.2x** |
| Snowflake → DuckDB | 126.4us | 733.0us | **5.80x** |
| Generic → PostgreSQL | 131.9us | 694.3us | **5.26x** |

### Complex queries (100 iterations)

| Dialect Pair | Rust | Python | Speedup |
|---|---|---|---|
| PostgreSQL → MySQL | 307.5us | 3.39ms | **11.0x** |
| PostgreSQL → BigQuery | 305.2us | 4.24ms | **13.9x** |
| MySQL → PostgreSQL | 303.5us | 2.54ms | **8.38x** |
| BigQuery → Snowflake | 303.9us | 5.65ms | **18.6x** |
| Snowflake → DuckDB | 307.5us | 2.59ms | **8.42x** |
| Generic → PostgreSQL | 299.8us | 2.51ms | **8.38x** |

Transpilation speedups **vary significantly by query complexity and dialect pair**:

- **Simple queries (1.6-3.5x):** The Rust overhead from dialect initialization (~77us baseline) dominates the small query, narrowing the gap. Python's per-call overhead is lower for tiny inputs because sqlglot caches dialect instances.
- **Medium queries (5-11x):** The ratio climbs as actual parsing/generation work dominates fixed overhead.
- **Complex queries (8-19x):** The biggest win is **BigQuery → Snowflake at 18.6x** — both are complex dialects with heavy AST transformations. The more transformations needed, the more Rust's advantage shows.

Notable dialect patterns:

- BigQuery as source or target consistently shows higher speedups (it has the most complex transformation rules)
- Snowflake → DuckDB is the lowest speedup pair (1.70x simple, 5.80x medium) — likely simpler transformations between these similar analytical dialects

## Key Takeaways

1. **Generation is the killer feature** — at ~86x, if your use case is primarily re-emitting SQL (formatting, normalization), polyglot-sql is nearly two orders of magnitude faster
2. **Parsing is a solid 10-13x** — consistent regardless of query complexity, which means predictable performance for any workload
3. **Transpilation scales with complexity** — the more SQL you throw at it, the better the Rust advantage (1.6x → 19x as queries grow)
4. **The ~77us Rust baseline for transpile** suggests dialect initialization has a fixed cost that could potentially be amortized if dialect instances were cached across calls

## Reproducing

```bash
# Full comparison (runs both engines, prints table)
make bench-compare

# Run engines independently (JSON output)
make bench-rust
make bench-python
```
