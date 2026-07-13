## Context

Polyglot has 33 dialects following a uniform pattern: each dialect implements `DialectImpl` with `tokenizer_config`, `generator_config`, and optionally `transform_expr`. This is **Phase 1** of a two-phase effort to add SAP HANA Cloud SQL support.

Phase 1 delivers the dialect scaffold: registration, tokenizer config, generator config, and identity round-trip tests. The HANA dialect will parse and regenerate HANA SQL correctly but will not yet transpile to other dialects (no function transforms or type mappings). Phase 2 (`add-sap-hana-transpilation`) adds the ~40 function transforms, date format conversion, and generator type mappings needed for HANA→Trino transpilation.

The split exists because:
1. Phase 1 is low-risk, mechanical work (proven pattern across 33 dialects) with immediate value (HANA parsing)
2. Phase 2 contains novel logic (date format conversion, ~40 transforms) that benefits from isolated review attention
3. Each phase has a clean RED→GREEN TDD cycle where all tests pass at the end

HANA's tokenizer/generator config is modeled after Oracle (double-quote identifiers, uppercase folding, no nested comments) and verified against the SAP HANA Cloud SQL Reference Guide.

## Goals / Non-Goals

**Goals:**
- Register `HANA` as a `DialectType` with feature gate `dialect-hana`
- Configure tokenizer and generator for HANA Cloud SQL conventions
- Parse standard HANA Cloud SQL (SELECT, INSERT, UPDATE, DELETE, DDL) correctly
- Round-trip HANA SQL as identity (HANA→HANA) for all supported standard SQL constructs
- Expose HANA in all language bindings (FFI, TypeScript SDK)
- Comprehensive identity test fixtures including edge cases

**Non-Goals:**
- Function transforms (ADD_DAYS, NVL, TO_VARCHAR, etc.) — Phase 2
- Date format string conversion — Phase 2
- Generator type mapping for HANA-specific types — Phase 2
- HANA→Trino transpilation — Phase 2
- SQLScript, WITH HINT, CONTAINS/FUZZY, spatial methods

## Decisions

### D1: HANA tokenizer modeled after Oracle

**Decision:** Configure tokenizer with double-quote identifiers, no nested comments, standard SQL string escapes (`''`).

**Rationale:** HANA shares Oracle's identifier quoting and comment conventions. This is a proven config in the existing Oracle dialect.

### D2: HANA generator modeled after Oracle

**Decision:** Configure generator with double-quote identifier quoting, uppercase keywords, and HANA-compatible null ordering/limit/interval settings.

**Rationale:** HANA generates SQL in uppercase with double-quoted identifiers, matching Oracle conventions. Specific config flags (null ordering, limit style) are verified against HANA Cloud docs.

### D3: No transform_expr in Phase 1

**Decision:** `HanaDialect` uses the default `transform_expr` (no-op pass-through) in Phase 1.

**Rationale:** Identity round-trip requires no transforms — the parser produces a generic AST and the HANA generator renders it back. Transforms are only needed for cross-dialect transpilation, which is Phase 2. This keeps Phase 1 focused and low-risk.

### D4: Four fixture files in Phase 1 (identity, select, ddl, dml)

**Decision:** Create only identity/select/ddl/dml fixture files in Phase 1. Transpilation/types/functions fixtures come in Phase 2.

**Rationale:** Identity tests are the only tests that pass without transforms. Including transpilation fixtures that fail would violate the principle that each phase's tests all pass.

### D5: Minimal HANA arms in shared generator.rs

**Decision:** Add two HANA-specific match arms in `generator.rs`: canonicalize `INT` to `INTEGER`, and preserve `NVARCHAR` instead of the default fold to `VARCHAR`.

**Rationale:** These are required for identity round-trip, not cross-dialect type mapping (which remains Phase 2). Without the `NVARCHAR` arm, `NVARCHAR(100)` in HANA DDL would regenerate as `VARCHAR(100)`. Both arms follow the existing per-dialect pattern in these match blocks and only affect output when `dialect == Some(HANA)`.

### D6: normalize_functions = None in Phase 1

**Decision:** Set `normalize_functions: NormalizeFunctions::None` (default is `Upper`) so mixed-case function names round-trip as-is.

**Rationale:** Identity fidelity in Phase 1. Note for Phase 2: function transforms often key off normalized names, so this setting may need to be revisited (or transforms must match case-insensitively) when transforms are added.

### D7: HANA aliases in FromStr — "hana", "saphana", "sap_hana"

**Decision:** Accept all three as aliases for `DialectType::HANA`.

**Rationale:** Following the precedent of TSQL accepting "mssql"/"sqlserver" and CockroachDB accepting "cockroach".

## Risks / Trade-offs

- **[Generator config flags may need tuning]** → Some HANA-specific generator settings (null ordering, interval syntax) may not be exactly right without testing against real HANA. Mitigation: identity fixtures will catch obvious mismatches; fine-tuning can happen in Phase 2 when transpilation tests provide cross-dialect validation.
- **[No transpilation value yet]** → Phase 1 alone doesn't enable HANA→Trino. Mitigation: Phase 1 delivers HANA parsing/identity which is useful for tooling; Phase 2 follows quickly.
