# API Reference

All APIs are exported from `polyglot_sql`.

## Core Functions

```python
transpile(sql: str, read: str = "generic", write: str = "generic", *, pretty: bool = False) -> list[str]
parse(sql: str, dialect: str = "generic") -> list[dict[str, Any]]
parse_one(sql: str, dialect: str = "generic") -> dict[str, Any]
generate(ast: dict[str, Any] | list[dict[str, Any]], dialect: str = "generic", *, pretty: bool = False) -> list[str]
format_sql(
    sql: str,
    dialect: str = "generic",
    *,
    max_input_bytes: int | None = None,
    max_tokens: int | None = None,
    max_ast_nodes: int | None = None,
    max_set_op_chain: int | None = None,
) -> str
format(...) -> str
validate(sql: str, dialect: str = "generic") -> ValidationResult
optimize(sql: str, dialect: str = "generic") -> str
lineage(column: str, sql: str, dialect: str = "generic") -> dict[str, Any]
source_tables(column: str, sql: str, dialect: str = "generic") -> list[str]
diff(sql1: str, sql2: str, dialect: str = "generic") -> list[dict[str, Any]]
dialects() -> list[str]
```

`format` is an alias of `format_sql`.

## Errors

- `PolyglotError`
- `ParseError`
- `GenerateError`
- `TranspileError`
- `ValidationError`

Unknown dialect names raise Python `ValueError`.

## Validation Result Types

`validate(...)` returns `ValidationResult`:

- `valid: bool`
- `errors: list[ValidationErrorInfo]`
- `bool(result)` is `True` when `valid` is `True`

`ValidationErrorInfo` fields:

- `message: str`
- `line: int`
- `col: int`
- `code: str`
- `severity: str`
