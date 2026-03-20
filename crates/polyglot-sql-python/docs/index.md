# Polyglot SQL Python API

`polyglot-sql` exposes the Rust `polyglot-sql` engine to Python via PyO3.

## Install

```bash
pip install polyglot-sql
```

## Quick Start

### Transpile SQL between dialects

```python
import polyglot_sql

polyglot_sql.transpile(
    "SELECT IFNULL(a, b) FROM t",
    read="mysql",
    write="postgres",
)
# ["SELECT COALESCE(a, b) FROM t"]
```

### Parse, inspect, and generate

```python
ast = polyglot_sql.parse_one("SELECT a AS x, b FROM t WHERE c > 1", dialect="postgres")

# Type dispatch
isinstance(ast, polyglot_sql.Select)  # True

# Property access
ast.expressions          # [Alias(Column(a) AS x), Column(b)]
ast.find(polyglot_sql.Column).name  # "a"

# Generate SQL for a different dialect
ast.sql("mysql")         # "SELECT a AS x, b FROM t WHERE c > 1"
```

### Traverse the AST

```python
ast = polyglot_sql.parse_one("SELECT a + b AS total, c FROM t")

# Find specific node types
columns = ast.find_all(polyglot_sql.Column)
for col in columns:
    print(col.name)  # "a", "b", "c"

# Walk the entire tree
for node in ast.walk():
    print(node.kind, node.name)

# Access parent chain
col = ast.expressions[0].this.this  # Column inside Add inside Alias
col.parent_select.kind  # "select"
```

### Flatten conditions

```python
ast = polyglot_sql.parse_one("SELECT * FROM t WHERE a AND b AND c")
and_node = ast.find(polyglot_sql.And)
conditions = and_node.flatten()
# [Column(a), Column(b), Column(c)]
```

## Format SQL

```python
polyglot_sql.format_sql(
    "SELECT a,b,c FROM t WHERE x>1 AND y<2",
    dialect="postgres",
)
# "SELECT\n  a,\n  b,\n  c\nFROM t\nWHERE\n  x > 1\n  AND y < 2"
```

## Formatting Guard Overrides

`format_sql` enforces parser/AST limits by default and supports per-call overrides.

```python
sql = polyglot_sql.format_sql(
    "SELECT 1 UNION ALL SELECT 2",
    dialect="generic",
    max_set_op_chain=1024,
    max_input_bytes=32 * 1024 * 1024,
)
```

## Validate SQL

```python
result = polyglot_sql.validate("SELCT 1", dialect="postgres")
result.valid   # False
for err in result.errors:
    print(f"Line {err.line}, Col {err.col}: {err.message}")
```

## Dialects

Use `polyglot_sql.dialects()` to retrieve supported dialect names at runtime.

```python
polyglot_sql.dialects()
# ["athena", "bigquery", "clickhouse", "databricks", "doris", "drill",
#  "duckdb", "generic", "hive", "materialize", "mysql", "oracle",
#  "postgres", "presto", "redshift", "snowflake", "spark", "sqlite",
#  "starrocks", "tableau", "teradata", "trino", "tsql", ...]
```
