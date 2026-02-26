# Polyglot SQL Python API

`polyglot-sql` exposes the Rust `polyglot-sql` engine to Python via PyO3.

## Install

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

## Formatting Guard Overrides

`format_sql` enforces parser/AST limits by default and supports per-call overrides.

```python
import polyglot_sql

sql = polyglot_sql.format_sql(
    "SELECT 1 UNION ALL SELECT 2",
    dialect="generic",
    max_set_op_chain=1024,
    max_input_bytes=32 * 1024 * 1024,
)
```

## Dialects

Use `polyglot_sql.dialects()` to retrieve supported dialect names at runtime.
