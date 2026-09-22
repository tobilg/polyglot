# Vertica coverage

The `vertica` dialect is available through Rust, Python, FFI/Go, and WASM/TypeScript.
Its implementation follows the [Vertica SQL reference](https://docs.vertica.com/26.2.x/en/sql-reference/).
The [SQLGlot Vertica plugin](https://github.com/luisdelatorre012/vertica-sqlglot-dialect)
was also checked, including its adjacent PostgreSQL and DuckDB conversions.
Reference-plugin output is not treated as a semantic specification.

The following matrix describes the forms reviewed in
[PR #467](https://github.com/tobilg/polyglot/pull/467), not every option in the
Vertica language. “Native” means parsing, structured AST/JSON, traversal, and
Vertica generation. Unsupported foreign conversions fail even at `Ignore`
and `Warn`; those settings do not authorize these semantic losses.

| Feature | Native | PostgreSQL / DuckDB conversion |
|---|---|---|
| `LISTAGG` parameters and integer expressions | Yes | Error: default/explicit byte limits and overflow behavior differ |
| Approximate distinct count | Yes, with aggregate metadata | Incoming filters become `CASE` inside the argument; unsupported modifiers error |
| `::!` safe casts | Yes | Error; incoming `TRY_CAST`/`SAFE_CAST` also error because constant-failure behavior differs |
| `DATEDIFF` | Yes | Boundary counts for known DATE/TIMESTAMP WITHOUT TIME ZONE operands and year, quarter, month, day, hour, minute, second, millisecond, microsecond units |
| Week, untyped, time-zone, TIME or INTERVAL date differences | Yes | Error; week cutoff requires engine verification |
| Binary `B'...'` / `X'...'` literals | Yes, byte value retained | PostgreSQL `DECODE(..., 'hex')`; DuckDB `UNHEX(...)` |
| Array element access | Yes, zero-based | Adjusts literal/dynamic indices; negative/out-of-range accesses return NULL; nested access supported |
| Array slices | Yes, exclusive end | Omitted or non-negative constant bounds supported; dynamic/negative bounds error |
| ARRAY/SET declarations, bounds, byte sizes and casts | Yes | Error: bounds, casts and set semantics are not equivalent |
| ROW fields and outer field aliases | Yes | Error without a verified type/field mapping |
| LONG VARBINARY size | Yes | Error rather than silently dropping capacity constraints |
| INTERVAL precision and INTERVALYM | Yes | Error rather than dropping precision/qualifiers |
| `USING PARAMETERS`, parameterized aggregates, `EXPLODE`, `PARTITION BEST` | Yes | Error without a verified parameter/result-shape mapping |
| Analytic and ordered-aggregate null ordering | Yes, including `NULLS AUTO` | Defined defaults retained; `AUTO` errors |
| Top-level null ordering | Type-dependent defaults; explicit placement uses CASE keys | Source defaults require a known key type; unknown types error |
| Historical `AT EPOCH` / `AT TIME` | Yes | Error |
| `FOR UPDATE [OF ...]` | Yes | Error: Vertica table locks differ from PostgreSQL row locks |
| SELECT optimizer hints | Yes | Error rather than discarding the hint |
| Partitioned `LIMIT ... OVER` | Yes | ROW_NUMBER rewrite for named, unambiguous outputs and resolvable keys; wildcard, OFFSET, locking or event-series combinations error |
| TIMESERIES, MATCH and INTERPOLATE | Yes | Error rather than treating them as ordinary queries/joins |
| CREATE PROJECTION, ENCODING, SEGMENTED/UNSEGMENTED and KSAFE | Yes | Error |
| CREATE FLEX/FLEXIBLE TABLE | Yes | Error |
| COPY FROM LOCAL and parser call arguments | Yes | Error |
| EXPORT TO PARQUET options and OVER | Yes | Error |
| NULLIFZERO | Yes | NULLIF(value, 0) |
| TIME_SLICE, MATCH_COLUMNS, conditional events, approximate percentiles, REGEXP_SUBSTR | Native calls retained | Error without a verified mapping |

Additional restrictions:

- Explicit top-level null placement uses an extra CASE sort key. Keys that might
  be volatile are rejected rather than evaluated twice. Projection aliases and
  ordinals are resolved before constructing the CASE expression.
- Partitioned LIMIT preserves output names and hides its helper columns. Internal
  aliases avoid names already present in the query. Ambiguous output names and
  non-projected keys in grouped/distinct queries are rejected conservatively.
- Array access assumes the target column represents a normal one-based array.
  Migrating PostgreSQL columns with custom array lower bounds needs an explicit
  data migration policy. Incoming array subscripts targeting Vertica are rejected.
- Generic native function acceptance is not a claim that every Vertica function
  has a foreign mapping. The reviewed dialect-specific functions are guarded.
- Parsing does not replace database validation of catalog objects, permissions,
  all parameter ranges, or table constraints.

## Verification

Regression cases live in the existing `custom_dialect_tests.rs`, Vertica custom
fixtures, and existing Python, Go, and TypeScript test files. No imported SQLGlot
fixture was excluded for this change. Corrections to PR-authored custom fixtures
replace unsafe expected translations with explicit error regressions.

Run the complete Rust gate with `make test-rust-verify`. To additionally execute
the value regressions against an installed DuckDB CLI:

```sh
POLYGLOT_DUCKDB=/path/to/duckdb cargo test -p polyglot-sql --test custom_dialect_tests
```

Native Vertica cases are checked against documentation and structured round trips;
these tests do not require or claim execution against a live Vertica server.

Key semantic references:

- [Array indexing, slices, bounds and casts](https://docs.vertica.com/26.2.x/en/sql-reference/data-types/complex-types/array/)
- [DATEDIFF boundaries](https://docs.vertica.com/24.2.x/en/sql-reference/functions/data-type-specific-functions/datetime-functions/datediff/)
- [SELECT and locking](https://docs.vertica.com/26.2.x/en/sql-reference/statements/select/)
- [Partitioned LIMIT](https://docs.vertica.com/26.2.x/en/sql-reference/statements/select/limit-clause/)
- [MATCH](https://docs.vertica.com/26.2.x/en/sql-reference/statements/select/match-clause/)
- [DuckDB date differences](https://duckdb.org/docs/lts/sql/functions/timestamp)
