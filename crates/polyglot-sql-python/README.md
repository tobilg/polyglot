# polyglot-sql (Python)

Rust-powered SQL transpiler for more than 30 SQL dialects.

The `polyglot-sql` Python package exposes an API backed by the Rust `polyglot-sql` engine for fast parse/transpile/generate/format/validate workflows.

## Installation

```bash
pip install polyglot-sql
```

## Quick Start

```python
import polyglot_sql

polyglot_sql.transpile(
    "SELECT IFNULL(a, b) FROM t",
    read="mysql",
    write="postgres",
)
# ["SELECT COALESCE(a, b) FROM t"]
```

```python
ast = polyglot_sql.parse_one("SELECT 1 + 2", dialect="postgres")
polyglot_sql.generate(ast, dialect="mysql")
```

```python
data_type = polyglot_sql.parse_data_type("DECIMAL(10, 2)", dialect="duckdb")
data_type.sql("postgres")
# "DECIMAL(10, 2)"

# SQLGlot-compatible narrow form for data types only:
polyglot_sql.parse_one("VARCHAR(255)", dialect="duckdb", into=polyglot_sql.DataType)
```

```python
polyglot_sql.format_sql("SELECT a,b FROM t WHERE x=1", dialect="postgres")
```

### SQLGlot-Compatible Builders

The common SQLGlot builder surface is available directly from
`polyglot_sql`. Builders return normal Polyglot expression objects and are
immutable: each chained call returns a new expression.

```python
query = (
    polyglot_sql.select("customer_id", "COUNT(*) AS orders")
    .from_("orders")
    .where("status = 'complete'")
    .group_by("customer_id")
    .order_by("orders DESC")
    .limit(10)
)

query.sql("postgres")
# "SELECT customer_id, COUNT(*) AS orders FROM orders WHERE status = 'complete' GROUP BY customer_id ORDER BY orders DESC LIMIT 10"

active = polyglot_sql.column("status").eq("active")
active.sql()
# "status = 'active'"
```

The shared builder feature set also includes named aggregate/string/math/date
helpers, all join and set-operation variants, named windows, lateral views,
hints, row locks, CTAS, `CASE`, `INSERT`, `UPDATE`, `DELETE`, and conditional
`MERGE` actions. Repeated clauses append by default; pass `append=False` to
replace one. Advanced parser options, mutable `copy=False` behavior, and the
complete SQLGlot expression catalog are not included. Polyglot expressions
remain the native AST type; SQLGlot is not a runtime dependency.

```python
ast = polyglot_sql.parse_one("SELECT id FROM a UNION ALL SELECT id FROM b")
order_expr = polyglot_sql.parse_one("SELECT id").args["expressions"][0]
ast = polyglot_sql.set_limit(ast, 100)
ast = polyglot_sql.set_offset(ast, 10)
ast = polyglot_sql.set_order_by(ast, order_expr)
polyglot_sql.generate(ast)
# ["SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id LIMIT 100 OFFSET 10"]
```

### Complexity Guard Options

`parse`, `parse_one` (including `into=DataType`), `parse_data_type`, `validate`,
`validate_with_schema`, `analyze_query`, and `transpile` accept the keyword-only
`complexity_guard`, a `ComplexityGuardOptions` typed dictionary
using the shared camelCase keys: `maxParserDepth`, `maxInputBytes`, `maxTokens`,
`maxAstNodes`, `maxAstDepth`, `maxParenthesisDepth`, and `maxFunctionCallDepth`.
Omit the argument, pass `None`, or omit a key to use its default. A field-level `None`
disables that check; a nonnegative integer overrides it. For example:

```python
polyglot_sql.transpile(sql, complexity_guard={"maxParserDepth": 128})
polyglot_sql.validate(sql, dialect="snowflake", complexity_guard={"maxFunctionCallDepth": 128})
polyglot_sql.parse_one(sql, dialect="snowflake", complexity_guard={"maxFunctionCallDepth": None})
```

`analyze_query` also accepts `options={"complexityGuard": {...}}`; do not supply
both forms in one call. Limits must be nonnegative integers or `None`; booleans,
floats, out-of-range integers, and unknown guard keys are rejected.
Omitting the entire guard preserves dialect-specific defaults (including
ClickHouse's higher function-nesting limit). A supplied dictionary uses the
shared Rust defaults for omitted fields. Validation reports guard exhaustion
as diagnostics; parsing and analysis raise `ParseError`.

Parser depth defaults to 1024 logical levels on native targets (32 on WASM) and is
checked during parsing, before an AST exists. Zero rejects parsing descents.
Other checks remain independent.
Raising or disabling limits can permit stack exhaustion and process termination,
even for trusted generated SQL. Increasing a limit does not increase stack space;
these limits are not general time/memory budgets. Application owners should control
overrides. Other SQL-consuming helpers, including lineage and optimization,
continue to inherit default protection.

### Format Guard Behavior

`format_sql` uses Rust core formatting guards with default limits:
- input bytes: `16 * 1024 * 1024`
- tokens: `1_000_000`
- AST nodes: `1_000_000`
- set-op chain: `256`

```python
import polyglot_sql

try:
    pretty = polyglot_sql.format_sql("SELECT 1", dialect="generic")
except polyglot_sql.GenerateError as exc:
    # Guard failures contain E_GUARD_* codes in the message.
    print(str(exc))
```

Per-call guard overrides:

```python
pretty = polyglot_sql.format_sql(
    "SELECT 1 UNION ALL SELECT 2",
    dialect="generic",
    max_set_op_chain=1024,
    max_input_bytes=32 * 1024 * 1024,
)
```

```python
result = polyglot_sql.validate(
    "SELECT * FROM users LIMIT 10",
    dialect="postgres",
    strict_syntax=True,
    semantic=True,
)
if result:
    print("valid")
```

Schema-aware validation uses the same Rust validator as the TypeScript SDK:

```python
sql = "SELECT o.order_id FROM orders o WHERE o.missing_column = TRUE"
schema = {"tables": [{"name": "orders", "columns": [{"name": "order_id", "type": "INT"}]}]}
result = polyglot_sql.validate_with_schema(
    sql, schema, dialect="snowflake", check_types=True, check_references=True,
)
for error in result.errors:
    print(error.code, error.message)
    if error.start is not None and error.end is not None:
        print(sql[error.start:error.end])
```

Unknown tables, columns and aliases are checked by default. `check_references`
also checks ambiguous columns and foreign-key metadata; `check_types` enables
type checks. `strict` overrides the schema's `strict` value, which defaults to
`True`; `strict=False` reports reference/type findings as warnings. An empty
column list or a `*` column denotes an open schema, so unknown columns are not
rejected solely because their names are absent. Nonempty lists without `*`
are treated as complete. Options use snake_case keyword arguments, not an
`options` dictionary. Invalid schemas and unknown dialects raise `ValueError`.

```python
options = {
    "producer": "https://github.com/tobilg/polyglot",
    "datasetNamespace": "postgres://warehouse",
    "outputDataset": {
        "namespace": "postgres://warehouse",
        "name": "analytics.revenue",
    },
}

payload = polyglot_sql.openlineage_column_lineage(
    "SELECT order_id, amount * 100 AS amount_cents FROM raw.orders",
    options,
)
print(payload["facet"]["fields"])
```

OpenLineage helpers only produce compatible payloads. Transport and client
emission are intentionally out of scope.

```python
analysis = polyglot_sql.analyze_query(
    "WITH base AS (SELECT id, amount FROM orders) SELECT * FROM base",
    {
        "dialect": "generic",
        "schema": {
            "tables": [
                {
                    "name": "orders",
                    "columns": [
                        {"name": "id", "type": "INT", "nullable": False},
                        {"name": "amount", "type": "DECIMAL(10,2)", "nullable": True},
                    ],
                }
            ]
        },
    },
)
print(analysis["cteFacts"][0]["bodySql"])           # "SELECT id, amount FROM orders"
print(analysis["starProjections"][0]["expandedColumns"])  # ["id", "amount"]
print(analysis["projections"][0]["nullability"])    # "non_null"
print(analysis["baseTables"][0]["name"])            # "orders"
print(analysis["baseTables"][0]["table"])           # "orders"
```

Non-projection uses are available through the same shared Rust analysis:

```python
analysis = polyglot_sql.analyze_query(
    "SELECT o.id FROM orders o WHERE o.amount > 0", dialect="duckdb"
)
use = analysis["columnUses"][0]
print(use["context"])                          # "filter"
print(use["references"][0]["column"])          # "amount"
print(use["scopePath"])                        # "root"
```

`columnUses` groups references by clause expression without changing projection
lineage. It covers joins, filters, grouping, HAVING/QUALIFY, window keys/frames,
ordering and set-operation filter inputs. `scopePath`/`expressionPath` identify
the scope and expression; `expressionSql` is dialect-rendered SQL. Optional
`span` objects use half-open Unicode-character offsets in the original input.
Reference spans locate uses, not upstream definitions. Unknown or ambiguous
sources remain conservative; whole-expression spans are omitted when unavailable.

`analysis["relations"]` reports sources visible in the analyzed scope.
`analysis["baseTables"]` reports deduplicated physical table dependencies across
nested CTEs, derived tables, subqueries, and set-operation branches. For
physical relation facts, `name` remains the qualified display name while
`catalog`, `schema`, and `table` expose parsed identifier parts. Validation
uses broad type families, while query analysis preserves parseable detailed
schema type strings for projection `typeHint` values. `analysis["cteFacts"]`
reports top-level CTE definitions, `analysis["starProjections"]` records the
original star projections and schema-expanded columns, and each projection has
conservative `nullability`: `"non_null"`, `"nullable"`, or `"unknown"`.
Function-like projections may include `transformFunction` with the function
name, literal arguments, and column arguments, for example for
`DATE_TRUNC('month', created_at)`.

Each `analysis["setOperations"][...]["branches"]` entry includes a `role` of
`"value"` or `"filter"`. Lineage results attach optional `set_branch` metadata
to immediate set-operation branch roots with the `operator`, original
zero-based `ordinal`, and `all` flag; omitted branches do not renumber the
surviving nodes. In OpenLineage output, `EXCEPT` and `INTERSECT` right-hand
inputs are emitted as indirect `FILTER` dependencies.

Validation schema dictionaries use:

```python
schema = {
    "strict": True,
    "tables": [
        {
            "name": "orders",
            "schema": "analytics",
            "aliases": ["o"],
            "primaryKey": ["id"],
            "uniqueKeys": [["external_id"]],
            "foreignKeys": [
                {
                    "columns": ["customer_id"],
                    "references": {"table": "customers", "columns": ["id"]},
                }
            ],
            "columns": [
                {"name": "id", "type": "INT", "nullable": False, "primaryKey": True},
                {"name": "amount", "type": "DECIMAL(10,2)", "nullable": True},
            ],
        }
    ],
}
```

Use the `type` key for column types. `dataType` / `data_type` are not accepted
aliases in this payload.

## API Reference

All functions are exported from `polyglot_sql`.

- `transpile(sql: str, read: str = "generic", write: str = "generic", *, pretty: bool = False) -> list[str]`
- `parse(sql: str, dialect: str = "generic") -> list[dict]`
- `parse_one(sql: str, dialect: str = "generic") -> dict`
- `parse_one(sql: str, dialect: str = "generic", *, into=polyglot_sql.DataType) -> DataType` (only `DataType` is supported for `into`)
- `parse_data_type(sql: str, dialect: str = "generic") -> DataType`
- `generate(ast: dict | list[dict], dialect: str = "generic", *, pretty: bool = False) -> list[str]`
- `format_sql(sql: str, dialect: str = "generic", *, max_input_bytes: int | None = None, max_tokens: int | None = None, max_ast_nodes: int | None = None, max_set_op_chain: int | None = None) -> str`
- `format(sql: str, dialect: str = "generic", *, max_input_bytes: int | None = None, max_tokens: int | None = None, max_ast_nodes: int | None = None, max_set_op_chain: int | None = None) -> str` (alias of `format_sql`)
- `validate(sql: str, dialect: str = "generic", *, strict_syntax: bool = False, semantic: bool = False) -> ValidationResult`
- `validate_with_schema(sql: str, schema: dict, dialect: str = "generic", *, check_types: bool = False, check_references: bool = False, strict: bool | None = None, semantic: bool = False, strict_syntax: bool = False) -> ValidationResult`
- `optimize(sql: str, dialect: str = "generic") -> str`
- `lineage(column: str, sql: str, dialect: str = "generic") -> dict`
- `lineage_at(ordinal: int, sql: str, dialect: str = "generic") -> dict`
- `lineage_at_with_schema(ordinal: int, sql: str, schema: dict, dialect: str = "generic") -> dict`
- `lineage_with_schema(column: str, sql: str, schema: dict, dialect: str = "generic") -> dict`
- `output_columns(sql: str, dialect: str = "generic") -> dict`
- `output_columns_with_schema(sql: str, schema: dict, dialect: str = "generic") -> dict`
- `source_tables(column: str, sql: str, dialect: str = "generic") -> list[str]`
- `analyze_query(sql: str, options: dict | None = None, dialect: str = "generic") -> dict`
- `openlineage_column_lineage(sql: str, options: dict) -> dict`
- `openlineage_job_event(sql: str, options: dict) -> dict`
- `openlineage_run_event(sql: str, options: dict) -> dict`
- `diff(sql1: str, sql2: str, dialect: str = "generic") -> list[dict]`
- `dialects() -> list[str]`
- `__version__: str`

## Supported Dialects

Current dialect names returned by `polyglot_sql.dialects()`:

`athena`, `bigquery`, `clickhouse`, `cockroachdb`, `datafusion`, `databricks`, `doris`, `dremio`, `drill`, `druid`, `duckdb`, `dune`, `exasol`, `fabric`, `generic`, `hana`, `hive`, `materialize`, `mysql`, `oracle`, `postgres`, `presto`, `redshift`, `risingwave`, `singlestore`, `snowflake`, `solr`, `spark`, `sqlite`, `starrocks`, `tableau`, `teradata`, `tidb`, `trino`, `tsql`.

## Error Handling

Exception hierarchy:

- `PolyglotError`
- `ParseError`
- `GenerateError`
- `TranspileError`
- `ValidationError`
- `ColumnResolutionError` (`reason`, `column`, and `ordinal` attributes)

Unknown dialect names raise built-in `ValueError`.

`validate(...)` and `validate_with_schema(...)` return `ValidationResult`:

- `result.valid: bool`
- `result.errors: list[ValidationErrorInfo]`
- `bool(result)` works (`True` when valid)

`strict_syntax=True` rejects compatibility forms such as trailing commas before
clause boundaries. `semantic=True` checks every query scope and reports errors
for invalid grouping (`E230`), aggregate placement/nesting (`E231`), and window
placement/nesting (`E232`). These errors make the result invalid, including with
`strict=False`. Quality hints remain warnings: `SELECT *` (`W001`), uncertain
grouping (`W002`), `DISTINCT` with `ORDER BY` (`W003`), and unordered `LIMIT`
(`W004`). Default validation remains syntax-only.

Schema validation always checks DML targets and references, independently of
`check_types`. Type checks use lexical query scopes and name-aligned set-operation
outputs. An empty column list or `*` denotes an open schema; validation is not a
database execution check and cannot prove runtime/session-dependent behavior.

Analysis options and schemas reject unknown keys, including nested metadata.
Public `TypedDict` models such as `AnalyzeQueryOptions`, `ValidationSchema`,
`QueryAnalysis`, and `FunctionCatalogSpec` describe their dictionary payloads.
Analysis retains best-effort references for missing columns but marks them
`unknown`, not `resolved`. Lambda-local parameters are not physical dependencies.

Python also accepts a declarative function catalog (Rust offers
`FunctionCatalogSpec::build` and the existing `FunctionCatalog` trait):

```python
catalog: polyglot_sql.FunctionCatalogSpec = {
    "functions": [{
        "name": "my_udf",
        "signatures": [{"minArity": 1, "maxArity": 2}],
    }],
}
result = polyglot_sql.validate_with_schema(
    "SELECT my_udf(1)", {"tables": []}, check_types=True,
    function_catalog=catalog,
)
```

The catalog **replaces** the embedded function name/arity catalog; it does not
activate `check_types` automatically. Overloads are supported; omitted/null
`maxArity` means variadic. `nameCase` is `insensitive` by default, or `sensitive`,
and can be overridden per function. Native typed-function checks remain active.
Blank names, empty signature lists, negative/noninteger arities, reversed bounds,
conflicting case overrides, and unknown fields are rejected. Other SDKs do not
expose this catalog option.

Each `ValidationErrorInfo` has:

- `message: str`
- `line: int`
- `col: int`
- `code: str`
- `severity: str`
- `start: int | None` (zero-based Unicode character offset)
- `end: int | None` (exclusive Unicode character offset)

Source ranges refer to the original SQL and support Python string slicing.
Reference diagnostics point to the offending identifier when available;
synthetic or schema-only findings have no source range. Existing `line` and
`col` fields remain integers and use `0` when unavailable.

## Performance Note

The package uses Rust internals directly via PyO3 and has zero runtime Python dependencies for SQL processing.
Published wheels use the dedicated Cargo `python_release` profile with
`opt-level=2` and thin LTO. This favors Python query throughput without changing
the size-oriented release profile used by WASM. FFI/Go artifacts use their own
native throughput profile. Editable development installs continue to use
Cargo's `dev` profile.

## Development

```bash
cd crates/polyglot-sql-python
uv sync --group dev
uv run maturin develop
uv run pytest
uv run pyright python/polyglot_sql/
uv run maturin build --profile python_release
uv run --with mkdocs mkdocs build --strict --clean --config-file mkdocs.yml --site-dir ../../packages/python-docs/dist
```

## Links

- Repository: https://github.com/tobilg/polyglot
- Issues: https://github.com/tobilg/polyglot/issues
- Python API Docs: https://polyglot-sql-python-api.pages.dev
- TypeScript API Docs: https://polyglot.gh.tobilg.com
- Playground: https://polyglot-playground.gh.tobilg.com/
