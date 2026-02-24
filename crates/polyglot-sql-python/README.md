# polyglot-sql (Python)

Rust-powered SQL transpiler for 30+ dialects.

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
polyglot_sql.format_sql("SELECT a,b FROM t WHERE x=1", dialect="postgres")
```

```python
result = polyglot_sql.validate("SELECT 1", dialect="postgres")
if result:
    print("valid")
```

## API Reference

All functions are exported from `polyglot_sql`.

- `transpile(sql: str, read: str = "generic", write: str = "generic", *, pretty: bool = False) -> list[str]`
- `parse(sql: str, dialect: str = "generic") -> list[dict]`
- `parse_one(sql: str, dialect: str = "generic") -> dict`
- `generate(ast: dict | list[dict], dialect: str = "generic", *, pretty: bool = False) -> list[str]`
- `format_sql(sql: str, dialect: str = "generic") -> str`
- `format(sql: str, dialect: str = "generic") -> str` (alias of `format_sql`)
- `validate(sql: str, dialect: str = "generic") -> ValidationResult`
- `optimize(sql: str, dialect: str = "generic") -> str`
- `lineage(column: str, sql: str, dialect: str = "generic") -> dict`
- `source_tables(column: str, sql: str, dialect: str = "generic") -> list[str]`
- `diff(sql1: str, sql2: str, dialect: str = "generic") -> list[dict]`
- `dialects() -> list[str]`
- `__version__: str`

## Supported Dialects

Current dialect names returned by `polyglot_sql.dialects()`:

`athena`, `bigquery`, `clickhouse`, `cockroachdb`, `datafusion`, `databricks`, `doris`, `dremio`, `drill`, `druid`, `duckdb`, `dune`, `exasol`, `fabric`, `generic`, `hive`, `materialize`, `mysql`, `oracle`, `postgres`, `presto`, `redshift`, `risingwave`, `singlestore`, `snowflake`, `solr`, `spark`, `sqlite`, `starrocks`, `tableau`, `teradata`, `tidb`, `trino`, `tsql`.

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
```

## Links

- Repository: https://github.com/tobilg/polyglot
- Issues: https://github.com/tobilg/polyglot/issues
- Docs: https://polyglot.gh.tobilg.com
- Playground: https://polyglot-playground.gh.tobilg.com/
