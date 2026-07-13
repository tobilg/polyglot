## Context

Phase 1 (`add-sap-hana-dialect`) established the HANA dialect with tokenizer config, generator config, and identity round-trip. The `HanaDialect` struct currently has no `transform_expr` override — it uses the default no-op pass-through.

Phase 2 adds the transpilation layer: ~40 function transforms, a date format converter, and generator type mappings. The primary target is Trino, which is already well-supported in polyglot with an existing `TrinoDialect` that maps many generic functions to Trino equivalents.

The transpilation pipeline is: HANA SQL → parse (generic AST) → HANA `transform_expr` (normalizes HANA functions to generic forms) → Trino `transform_expr` (adapts to Trino) → Trino generator (emits SQL). Much of the pipeline already works because the existing Trino dialect handles many generic→Trino mappings (SUBSTR→SUBSTRING, NVL→COALESCE, NOW→CURRENT_TIMESTAMP). The HANA dialect only needs to normalize HANA-proprietary functions that no other dialect recognizes.

## Goals / Non-Goals

**Goals:**
- Normalize ~40 HANA-specific functions to generic AST forms during `transform_expr`
- Convert HANA Oracle-style date format strings to Java SimpleDateFormat for Trino compatibility
- Map HANA-specific data types to Trino-compatible types in the generator
- Achieve correct HANA→Trino transpilation for common analytical SQL queries
- Comprehensive transpilation test fixtures with edge cases (negative values, nested functions, WHERE-clause usage, single-arg vs two-arg variants, NULL args, mixed-case names)
- All existing Phase 1 identity tests continue to pass

**Non-Goals:**
- SQLScript procedural language
- WITH HINT, CONTAINS/FUZZY, graph, spatial methods
- Bi-directional Trino→HANA transpilation (only HANA→Trino is verified)
- Performance optimization

## Decisions

### D1: Function normalization in HANA dialect's transform_expr

**Decision:** HANA-specific function transforms live in `HanaDialect::transform_expr`, normalizing to generic AST forms. The existing Trino dialect and generator then handle rendering.

**Rationale:** The polyglot architecture places function normalization in the source dialect's `transform_expr`. For example, HANA `ADD_DAYS(d, n)` becomes `Function("DATE_ADD", [Literal("day"), n, d])` in the generic AST, which the Trino generator renders as `DATE_ADD('day', n, d)`. Many HANA functions (SUBSTR, NVL, NOW) are already handled by the existing Trino dialect, so no new code is needed for those — they pass through and Trino's existing transforms handle them.

### D2: HANA date functions → DATE_ADD/DATE_DIFF with string unit argument

**Decision:** Map HANA's typed date arithmetic functions to generic `DATE_ADD`/`DATE_DIFF` with a string literal unit:
- `ADD_DAYS(d, n)` → `Function("DATE_ADD", [Literal("day"), n, d])`
- `DAYS_BETWEEN(d1, d2)` → `Function("DATE_DIFF", [Literal("day"), d1, d2])`

**Rationale:** This matches Trino's `DATE_ADD`/`DATE_DIFF` signature (first arg is unit string). The Trino generator already handles these natively. The arg order follows Trino convention: `DATE_ADD(unit, value, date)`.

### D3: Date format string conversion — HANA Oracle-style → Java SimpleDateFormat

**Decision:** Implement `convert_hana_to_java_format()` in `hana.rs` with token-by-token conversion: YYYY→yyyy, MM→MM, DD→dd, HH24→HH, MI→mm, SS→ss, FF3→SSS, DAY→EEEE, MON→MMM, etc. Applied during `transform_expr` when processing TO_VARCHAR/TO_DATE/TO_TIMESTAMP with format arguments.

**Rationale:** Trino's DATE_FORMAT and DATE_PARSE use Java SimpleDateFormat. HANA uses Oracle-style tokens. Unknown tokens pass through unchanged (best-effort).

### D4: Data type mapping in generator's DataType::Custom branch

**Decision:** HANA-specific types (SMALLDECIMAL, SECONDDATE, ALPHANUM, etc.) that parse as `DataType::Custom` get mapped in the generator's existing `DataType::Custom` match arm, following the same pattern as ClickHouse and Dremio custom types.

**Rationale:** The generator already has a well-structured per-dialect type mapping branch. Adding HANA there follows the established pattern. Precision/length args are preserved (e.g., NVARCHAR(255) → VARCHAR(255)).

### D5: LOCATE argument swap

**Decision:** HANA's `LOCATE(substr, str)` has reversed argument order compared to Trino's `STRPOS(str, substr)`. The HANA transform swaps the args.

**Rationale:** HANA LOCATE takes substring first, string second. Trino STRPOS takes string first, substring second. Without swapping, transpilation would produce incorrect results. This is a semantic difference, not just a naming difference.

## Risks / Trade-offs

- **[HANA function semantics differ from Trino in edge cases]** → Mitigation: edge case fixtures (negative values, NULL args, nested calls); document assumptions in fixture descriptions
- **[SMALLDECIMAL precision loss]** → HANA SMALLDECIMAL has 38 significant digits with flexible decimal places; Trino DECIMAL has fixed precision. Mitigation: map to DECIMAL as best-effort; document limitation
- **[Date format conversion edge cases]** → Unknown HANA tokens pass through. Mitigation: comprehensive format fixture covering all documented HANA tokens plus unknown-token edge case
- **[LOCATE with 3 args]** → HANA LOCATE supports a start position (third arg); Trino STRPOS does not. Mitigation: map to SUBSTRING + STRPOS + offset arithmetic, or pass through if too complex for Phase 2
- **[TRUNC overload]** → HANA TRUNC serves both numeric truncation (TRUNC(x, n)) and date truncation (TRUNC(d)). Mitigation: detect context (numeric vs temporal arg) and map accordingly
