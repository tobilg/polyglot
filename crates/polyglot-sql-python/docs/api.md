# API Reference

All APIs are exported from `polyglot_sql`.

## Core Functions

### `transpile`

```python
transpile(
    sql: str,
    read: str | None = None,
    write: str | None = None,
    *,
    identity: bool = True,
    error_level: str | None = None,
    pretty: bool = False,
) -> list[str]
```

Transpile SQL from one dialect to another.

```python
polyglot_sql.transpile(
    "SELECT IFNULL(a, b) FROM t",
    read="mysql",
    write="postgres",
)
# ["SELECT COALESCE(a, b) FROM t"]
```

### `parse`

```python
parse(
    sql: str,
    read: str | None = None,
    dialect: str | None = None,
    *,
    error_level: str | None = None,
) -> list[Expression]
```

Parse SQL into a list of typed `Expression` AST nodes.

```python
stmts = polyglot_sql.parse("SELECT 1; SELECT 2", dialect="postgres")
len(stmts)  # 2
isinstance(stmts[0], polyglot_sql.Select)  # True
```

### `parse_one`

```python
parse_one(
    sql: str,
    read: str | None = None,
    dialect: str | None = None,
    *,
    into: Any | None = None,
    error_level: str | None = None,
) -> Expression
```

Parse a single SQL statement into an `Expression` AST node. Raises `ParseError` if the input contains zero or multiple statements.

```python
ast = polyglot_sql.parse_one("SELECT a, b FROM t", dialect="postgres")
isinstance(ast, polyglot_sql.Select)  # True
```

### `generate`

```python
generate(
    ast: Expression | dict | list[Expression] | list[dict],
    dialect: str = "generic",
    *,
    pretty: bool = False,
) -> list[str]
```

Generate SQL strings from AST nodes. Accepts `Expression` objects or their `dict` equivalents (as returned by `to_dict()`).

```python
ast = polyglot_sql.parse_one("SELECT 1 + 2")
polyglot_sql.generate(ast, dialect="mysql")
# ["SELECT 1 + 2"]
```

### `format_sql` / `format`

```python
format_sql(
    sql: str,
    dialect: str = "generic",
    *,
    max_input_bytes: int | None = None,
    max_tokens: int | None = None,
    max_ast_nodes: int | None = None,
    max_set_op_chain: int | None = None,
) -> str
```

Parse and pretty-print SQL. `format` is an alias for `format_sql`.

```python
polyglot_sql.format_sql("SELECT a,b FROM t WHERE c>1", dialect="postgres")
# "SELECT\n  a,\n  b\nFROM t\nWHERE\n  c > 1"
```

### `validate`

```python
validate(sql: str, dialect: str = "generic") -> ValidationResult
```

Validate SQL syntax. Does **not** raise on invalid SQL — check `result.valid` instead.

```python
result = polyglot_sql.validate("SELCT 1")
result.valid   # False
result.errors  # [ValidationErrorInfo(...)]
```

### `optimize`

```python
optimize(sql: str, dialect: str | None = None, *, read: str | None = None) -> str
```

Apply basic SQL optimizations (predicate simplification, etc.).

### `lineage` / `lineage_with_schema` / `source_tables`

```python
lineage(column: str, sql: str, dialect: str = "generic") -> dict
lineage_with_schema(column: str, sql: str, schema: dict, dialect: str = "generic") -> dict
source_tables(column: str, sql: str, dialect: str = "generic") -> list[str]
```

Column lineage analysis. `source_tables` returns a flat list of table names contributing to a column.

### `diff`

```python
diff(sql1: str, sql2: str, dialect: str = "generic") -> list[dict]
```

Compute a structural diff between two SQL statements.

### `dialects`

```python
dialects() -> list[str]
```

Returns the list of supported dialect names (e.g. `["athena", "bigquery", "clickhouse", ...]`).

---

## Expression

All parsed SQL is represented as typed `Expression` subclasses. The base `Expression` class provides a rich API for inspection and traversal.

### Creating Expressions

```python
# Parse SQL into an AST
ast = polyglot_sql.parse_one("SELECT a AS x, b FROM t WHERE c > 1")
isinstance(ast, polyglot_sql.Select)  # True
```

### Type Dispatch with `isinstance`

Every AST node is an instance of a specific subclass — `Select`, `Column`, `Literal`, `Add`, etc. — enabling idiomatic Python `isinstance` checks:

```python
col = ast.find(polyglot_sql.Column)
isinstance(col, polyglot_sql.Column)     # True
isinstance(col, polyglot_sql.Expression) # True (all subclass Expression)
type(col).__name__                       # "Column"
```

### Core Identifiers

| Property | Type | Description |
|----------|------|-------------|
| `kind` | `str` | Snake-case variant name: `"select"`, `"column"`, `"add"`, etc. |
| `key` | `str` | Alias for `kind` (sqlglot compatibility). |
| `tree_depth` | `int` | Maximum depth of the sub-tree (0 for leaves). |

### SQL Generation

```python
ast.sql()                          # "SELECT a AS x, b FROM t WHERE c > 1"
ast.sql("mysql")                   # MySQL-specific output
ast.sql("postgres", pretty=True)   # Formatted PostgreSQL output
str(ast)                           # Same as ast.sql()
```

### Child Accessors

These properties provide fast, no-serialization access to child nodes:

| Property | Type | Description |
|----------|------|-------------|
| `this` | `Expression \| None` | Primary child: operand for unary ops, left for binary ops, aliased expr for `Alias`, predicate for `Where`/`Having`. |
| `expression` | `Expression \| None` | Secondary child: right operand for binary ops, second arg for binary functions. |
| `expressions` | `list[Expression]` | List children: columns in `Select`, args in `Function`, tables in `From`, etc. |
| `args` | `dict` | All fields as a dict (uses serialization). |

```python
ast = polyglot_sql.parse_one("SELECT a, b, c FROM t")
ast.expressions                    # [Column(a), Column(b), Column(c)]

binop = polyglot_sql.parse_one("SELECT 1 + 2").find(polyglot_sql.Add)
binop.this                         # Literal(1)  — left operand
binop.expression                   # Literal(2)  — right operand
```

### Name & Alias Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | `str` | Short name: column name, table name, function name, literal value, `"*"` for Star. |
| `alias` | `str` | Alias identifier if present (from `Alias`, `Table`, `Subquery`). |
| `alias_or_name` | `str` | Alias if non-empty, otherwise name. |
| `output_name` | `str` | Name this expression produces in a result set. |

```python
ast = polyglot_sql.parse_one("SELECT a AS x FROM my_table")
alias_node = ast.find(polyglot_sql.Alias)
alias_node.name         # "a"  (delegates to aliased expression)
alias_node.alias        # "x"
alias_node.alias_or_name  # "x"
alias_node.output_name  # "x"

tbl = ast.find(polyglot_sql.Table)
tbl.name                # "my_table"
```

### Type Predicates

| Property / Method | Type | Description |
|-------------------|------|-------------|
| `is_string` | `bool` | True if this is a string literal. |
| `is_number` | `bool` | True if this is a numeric literal (or negated). |
| `is_int` | `bool` | True if this is an integer literal (or negated). |
| `is_star` | `bool` | True if this is a `*` wildcard. |
| `is_leaf()` | `bool` | True if this node has no children. |

```python
lit = polyglot_sql.parse_one("SELECT 'hello'").find(polyglot_sql.Literal)
lit.is_string  # True
lit.is_number  # False

num = polyglot_sql.parse_one("SELECT 42").find(polyglot_sql.Literal)
num.is_number  # True
num.is_int     # True
```

### Comments

```python
ast.comments  # list[str] — SQL comments attached to this node
```

### Parent Tracking

Parent references are set lazily when you access children via `.this`, `.expression`, `.expressions`, or `.children()`:

| Property / Method | Type | Description |
|-------------------|------|-------------|
| `parent` | `Expression \| None` | Parent node, or `None` for root. |
| `depth` | `int` | Number of hops to root (0 for root). |
| `root()` | `Expression` | Walk parent chain to root. |
| `find_ancestor(*types)` | `Expression \| None` | First ancestor matching any given type. |
| `parent_select` | `Expression \| None` | Shorthand for `find_ancestor(Select)`. |

```python
ast = polyglot_sql.parse_one("SELECT a FROM t")
col = ast.expressions[0]     # Column(a) — parent is set
col.parent.kind              # "select"
col.depth                    # 1
col.root().kind              # "select"
col.parent_select.kind       # "select"
```

### Traversal

| Method | Returns | Description |
|--------|---------|-------------|
| `children()` | `list[Expression]` | Immediate children (with parent refs). |
| `walk(order="dfs")` | `list[Expression]` | All nodes in DFS or BFS order (including self). |
| `find(*types)` | `Expression \| None` | First descendant matching any type (DFS, skips self). |
| `find_all(*types)` | `list[Expression]` | All descendants matching any type (DFS, skips self). |
| `iter_expressions()` | `list[Expression]` | Alias for `children()`. |

`find()` and `find_all()` accept **class objects** or **strings**:

```python
# Using class objects (recommended)
ast.find(polyglot_sql.Column)
ast.find_all(polyglot_sql.Column, polyglot_sql.Literal)

# Using strings
ast.find("column")
ast.find_all("column", "literal")
```

### Unwrapping

| Method | Returns | Description |
|--------|---------|-------------|
| `unnest()` | `Expression` | Recursively unwrap `Paren(...)` wrappers. |
| `unalias()` | `Expression` | Unwrap one `Alias` layer. |
| `flatten()` | `list[Expression]` | Flatten same-type chains, e.g. `And(And(a,b),c)` → `[a, b, c]`. |

```python
# Flatten chained AND conditions
where = polyglot_sql.parse_one("SELECT * WHERE a AND b AND c")
and_node = where.find(polyglot_sql.And)
conditions = and_node.flatten()  # [Column(a), Column(b), Column(c)]
```

### Other Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `to_dict()` | `dict` | Full serialization to nested dict. |
| `arg(name)` | `Any` | Single field by name from serialized payload. |
| `text(key)` | `str` | Extract a field value as a plain string. |
| `sql(dialect, pretty)` | `str` | Generate SQL for this node. |

### String Representations

```python
str(ast)    # SQL string: "SELECT a FROM t"
repr(ast)   # Tree repr: "Select(expressions=[Column(this=Identifier(...))])"
```

---

## Expression Subclasses

Every AST node type has a corresponding Python class that inherits from `Expression`. There are 919 subclasses covering all SQL constructs. Here are the most commonly used ones:

### Query Structure

`Select`, `Union`, `Intersect`, `Except`, `Subquery`, `Values`, `With`, `Cte`

### DML

`Insert`, `Update`, `Delete`, `Merge`

### DDL

`CreateTable`, `DropTable`, `AlterTable`, `CreateView`, `DropView`, `CreateIndex`, `DropIndex`, `CreateFunction`, `DropFunction`

### Clauses

`From`, `Join`, `Where`, `GroupBy`, `Having`, `OrderBy`, `Limit`, `Offset`, `Qualify`, `Window`, `Over`

### Expressions

`Column`, `Table`, `Identifier`, `Literal`, `Star`, `Alias`, `Cast`, `Case`, `Paren`, `DataType`, `Interval`, `Boolean`, `Null`

### Operators

`And`, `Or`, `Not`, `Add`, `Sub`, `Mul`, `Div`, `Eq`, `Neq`, `Lt`, `Lte`, `Gt`, `Gte`, `Like`, `ILike`, `In`, `Between`, `IsNull`, `Exists`, `Concat`

### Functions

`Function`, `AggregateFunction`, `WindowFunction`, `Count`, `Sum`, `Avg`, `Min`, `Max`, `Coalesce`, `Upper`, `Lower`, `Substring`, `Cast`, `TryCast`, `SafeCast`

### Window Functions

`RowNumber`, `Rank`, `DenseRank`, `Lead`, `Lag`, `FirstValue`, `LastValue`, `NthValue`, `PercentRank`, `CumeDist`

All subclasses inherit every property and method from `Expression`.

---

## Errors

| Exception | Description |
|-----------|-------------|
| `PolyglotError` | Base exception. |
| `ParseError` | SQL parsing failed. |
| `GenerateError` | SQL generation from AST failed. |
| `TranspileError` | SQL transpilation failed. |
| `ValidationError` | Fatal validation error. |

Unknown dialect names raise Python `ValueError`.

## Validation Result Types

`validate(...)` returns `ValidationResult`:

- `valid: bool` — `True` when the SQL is syntactically valid
- `errors: list[ValidationErrorInfo]` — list of findings (may be empty when valid)
- `bool(result)` — allows `if validate(...):` usage

`ValidationErrorInfo` fields:

- `message: str` — human-readable description
- `line: int` — 1-based line number
- `col: int` — 1-based column number
- `code: str` — machine-readable error code
- `severity: str` — `"error"` or `"warning"`
