# polyglot-sql (Python)

Rust-powered SQL transpiler for 33+ dialects.

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

```python
ast = polyglot_sql.parse_one("SELECT id FROM a UNION ALL SELECT id FROM b")
order_expr = polyglot_sql.parse_one("SELECT id").args["expressions"][0]
ast = polyglot_sql.set_limit(ast, 100)
ast = polyglot_sql.set_offset(ast, 10)
ast = polyglot_sql.set_order_by(ast, order_expr)
polyglot_sql.generate(ast)
# ["SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id LIMIT 100 OFFSET 10"]
```

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
result = polyglot_sql.validate("SELECT 1", dialect="postgres")
if result:
    print("valid")
```

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
- `validate(sql: str, dialect: str = "generic") -> ValidationResult`
- `optimize(sql: str, dialect: str = "generic") -> str`
- `lineage(column: str, sql: str, dialect: str = "generic") -> dict`
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

Unknown dialect names raise built-in `ValueError`.

`validate(...)` returns `ValidationResult`:
- `result.valid: bool`
- `result.errors: list[ValidationErrorInfo]`
- `bool(result)` works (`True` when valid)

Each `ValidationErrorInfo` has:
- `message: str`
- `line: int`
- `col: int`
- `code: str`
- `severity: str`

## Performance Note

The package uses Rust internals directly via PyO3 and has zero runtime Python dependencies for SQL processing.

## Development

```bash
cd crates/polyglot-sql-python
uv sync --group dev
uv run maturin develop
uv run pytest
uv run pyright python/polyglot_sql/
uv run maturin build --release
uv run --with mkdocs mkdocs build --strict --clean --config-file mkdocs.yml --site-dir ../../packages/python-docs/dist
```

## Links

- Repository: https://github.com/tobilg/polyglot
- Issues: https://github.com/tobilg/polyglot/issues
- Python API Docs: https://polyglot-sql-python-api.pages.dev
- TypeScript API Docs: https://polyglot.gh.tobilg.com
- Playground: https://polyglot-playground.gh.tobilg.com/
