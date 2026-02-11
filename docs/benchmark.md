# Polyglot-Core vs SQLGlot Performance Report

**Engines:** polyglot-sql v0.1.0 (Rust/WASM) vs sqlglot v28.10.1 (Python)
**Overall geometric mean speedup: 10.4x**

## Parse (SQL string → AST)

| Query | Rust | Python | Speedup |
|---|---|---|---|
| simple (SELECT a, b, c) | 7.7us | 108.9us | **14.1x** |
| medium (5 cols, JOIN, GROUP BY) | 47.5us | 691.1us | **14.6x** |
| complex (3 CTEs, subquery) | 184.6us | 2.58ms | **14.0x** |

Parsing shows a remarkably **consistent ~14x speedup** across all query sizes. The linear scaling in both engines suggests neither has algorithmic bottlenecks — the difference is purely language-level overhead (Rust native vs CPython interpreter). This is one of the most predictable categories.

## Generate (AST → SQL string)

| Query | Rust | Python | Speedup |
|---|---|---|---|
| simple | 0.5us | 42.9us | **91.2x** |
| medium | 3.3us | 291.1us | **88.2x** |
| complex | 11.8us | 1.04ms | **87.6x** |

Generation is the **biggest win at ~89x average**. This makes sense — SQL generation is primarily string concatenation and tree traversal, both areas where Rust's zero-cost abstractions (pre-allocated `String` buffers, stack-based dispatch via `match`) vastly outperform Python's dynamic dispatch and repeated small allocations. The simple query generates in 500 nanoseconds — essentially a few cache lines of work.

## Roundtrip (parse → generate → re-parse)

| Query | Rust | Python | Speedup |
|---|---|---|---|
| simple | 14.5us | 272.3us | **18.8x** |
| medium | 98.9us | 1.70ms | **17.2x** |
| complex | 369.9us | 6.32ms | **17.1x** |

Roundtrip is **~17-19x faster**, which is a weighted blend of parsing (14x) and generation (89x). Since parsing dominates the total time (it's much slower than generation), the combined ratio skews closer to the parse speedup. This metric matters for validation workflows where SQL is parsed, normalized, and re-emitted.

## Transpile (parse with read dialect → transform → generate with write dialect)

### Simple queries (1000 iterations)

| Dialect Pair | Rust | Python | Speedup |
|---|---|---|---|
| PostgreSQL → MySQL | 73.5us | 156.8us | **2.13x** |
| PostgreSQL → BigQuery | 74.6us | 211.7us | **2.84x** |
| MySQL → PostgreSQL | 74.5us | 157.4us | **2.11x** |
| BigQuery → Snowflake | 75.0us | 295.2us | **3.94x** |
| Snowflake → DuckDB | 78.7us | 142.3us | **1.81x** |
| Generic → PostgreSQL | 74.7us | 159.1us | **2.13x** |

### Medium queries (500 iterations)

| Dialect Pair | Rust | Python | Speedup |
|---|---|---|---|
| PostgreSQL → MySQL | 122.0us | 890.1us | **7.30x** |
| PostgreSQL → BigQuery | 122.7us | 1.43ms | **11.7x** |
| MySQL → PostgreSQL | 122.4us | 874.9us | **7.15x** |
| BigQuery → Snowflake | 123.9us | 1.50ms | **12.1x** |
| Snowflake → DuckDB | 122.8us | 854.1us | **6.95x** |
| Generic → PostgreSQL | 120.5us | 839.1us | **6.96x** |

### Complex queries (100 iterations)

| Dialect Pair | Rust | Python | Speedup |
|---|---|---|---|
| PostgreSQL → MySQL | 283.6us | 3.31ms | **11.7x** |
| PostgreSQL → BigQuery | 283.9us | 4.25ms | **15.0x** |
| MySQL → PostgreSQL | 373.0us | 3.12ms | **8.35x** |
| BigQuery → Snowflake | 295.8us | 6.10ms | **20.6x** |
| Snowflake → DuckDB | 295.4us | 3.20ms | **10.8x** |
| Generic → PostgreSQL | 291.1us | 3.12ms | **10.7x** |

Transpilation speedups **vary significantly by query complexity and dialect pair**:

- **Simple queries (2-4x):** The Rust overhead from dialect initialization (~70us baseline) dominates the small query, narrowing the gap. Python's per-call overhead is lower for tiny inputs because sqlglot caches dialect instances.
- **Medium queries (7-12x):** The ratio climbs as actual parsing/generation work dominates fixed overhead.
- **Complex queries (8-21x):** The biggest win is **BigQuery → Snowflake at 20.6x** — both are complex dialects with heavy AST transformations. The more transformations needed, the more Rust's advantage shows.

Notable dialect patterns:

- BigQuery as source or target consistently shows higher speedups (it has the most complex transformation rules)
- Snowflake → DuckDB is the lowest speedup pair (1.81x simple, 6.95x medium) — likely simpler transformations between these similar analytical dialects

## Key Takeaways

1. **Generation is the killer feature** — at ~89x, if your use case is primarily re-emitting SQL (formatting, normalization), polyglot-sql is nearly two orders of magnitude faster
2. **Parsing is a solid 14x** — consistent regardless of query complexity, which means predictable performance for any workload
3. **Transpilation scales with complexity** — the more SQL you throw at it, the better the Rust advantage (2x → 21x as queries grow)
4. **The ~70us Rust baseline for transpile** suggests dialect initialization has a fixed cost that could potentially be amortized if dialect instances were cached across calls

## Reproducing

```bash
# Full comparison (runs both engines, prints table)
make bench-compare

# Run engines independently (JSON output)
make bench-rust
make bench-python
```
