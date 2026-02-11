# SQLGlot Architecture Report

Based on my analysis of the codebase at `external-projects/sqlglot`, here's a comprehensive report on how SQLGlot works.

---

## 1. Project Overview

SQLGlot is a **pure Python SQL parser, transpiler, optimizer, and executor** supporting 34+ dialects. It converts SQL between database systems with full semantic understanding.

### Key Statistics

| Component | Files | Lines | Purpose |
|-----------|-------|-------|---------|
| Tokenizer | 1 | 1,592 | Lexical analysis |
| Expressions/AST | 1 | 10,327 | 202 expression types |
| Parser | 1 | 9,159 | Recursive descent parsing |
| Generator | 1 | 5,563 | AST → SQL conversion |
| Dialects | 34 | 23,577 | Dialect implementations |
| Optimizer | 20 | 6,121 | Query optimization |
| **Total** | 87+ | 65,319 | Complete transpiler |

---

## 2. Core Data Flow

```
SQL String (source dialect)
        │
        ▼
┌───────────────────┐
│    Tokenizer      │  → Token Stream
│  (tokens.py)      │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│     Parser        │  → AST (Expression Tree)
│  (parser.py)      │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│   Optimizer       │  → Optimized AST (optional)
│  (optimizer/)     │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│    Generator      │  → SQL String (target dialect)
│  (generator.py)   │
└───────────────────┘
```

---

## 3. Parser Architecture

### Methodology: Recursive Descent with Precedence Climbing

SQLGlot uses a **recursive descent parser** where operator precedence is encoded through method chaining. Each parsing method calls the next level down in the precedence hierarchy.

### Precedence Hierarchy (High to Low)

```python
_parse_assignment()      # :=
    ↓
_parse_disjunction()     # OR
    ↓
_parse_conjunction()     # AND
    ↓
_parse_equality()        # =, !=, <=>
    ↓
_parse_comparison()      # <, >, <=, >=
    ↓
_parse_range()           # BETWEEN, IN, LIKE, IS
    ↓
_parse_bitwise()         # &, |, ^, <<, >>
    ↓
_parse_term()            # +, -, %, COLLATE
    ↓
_parse_factor()          # *, /, DIV
    ↓
_parse_exponent()        # ^, **
    ↓
_parse_unary()           # NOT, ~, -, +
    ↓
_parse_type()            # CAST, INTERVAL
    ↓
_parse_primary()         # literals, parens, identifiers
```

### Token Management

The parser maintains three pointers for lookahead:
- `_curr`: Current token
- `_next`: Next token (1-lookahead)
- `_prev`: Previous token (for comment attachment)

```python
def _advance(self, times: int = 1) -> None:
    self._index += times
    self._curr = seq_get(self._tokens, self._index)
    self._next = seq_get(self._tokens, self._index + 1)
```

### Dispatch Tables

The parser uses configuration dictionaries for extensibility:

| Table | Purpose | Example |
|-------|---------|---------|
| `FUNCTIONS` | Function name → builder | `"LOG": build_logarithm` |
| `STATEMENT_PARSERS` | Statement dispatch | `SELECT → _parse_select()` |
| `RANGE_PARSERS` | Binary predicates | `BETWEEN, IN, LIKE, EXISTS` |
| `TYPE_TOKENS` | 80+ SQL type keywords | `BIGINT, VARCHAR, ARRAY` |
| `NO_PAREN_FUNCTIONS` | Zero-arg functions | `CURRENT_DATE, CURRENT_USER` |

### Example: SELECT Parsing

```python
def _parse_select_query(self, ...):
    # 1. Parse WITH clause (CTEs)
    cte = self._parse_with()

    # 2. Parse SELECT and projections
    hint = self._parse_hint()
    distinct = self._match_set(self.DISTINCT_TOKENS)
    projections = self._parse_projections()

    this = exp.Select(expressions=projections, distinct=distinct, ...)

    # 3. Parse FROM
    this.set("from_", self._parse_from())

    # 4. Parse modifiers (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT)
    this = self._parse_query_modifiers(this)

    # 5. Parse set operations (UNION, INTERSECT, EXCEPT)
    return self._parse_set_operations(this)
```

### Backtracking Support

The parser supports speculative parsing with automatic rollback:

```python
def _try_parse(self, parse_method, retreat=False):
    index = self._index
    try:
        return parse_method()
    except ParseError:
        self._retreat(index)  # Backtrack on failure
        return None
```

---

## 4. Transpiler (Generator) Architecture

### Design Pattern: Visitor with Triple Dispatch

The generator converts AST back to SQL via three dispatch mechanisms:

1. **TRANSFORMS dictionary** (fastest) - direct lambda mapping
2. **Named methods** - `<expression_type>_sql()` methods
3. **Fallback handlers** - generic handling for functions/properties

```python
def sql(self, expression, key=None, comment=True):
    # 1. Try TRANSFORMS lookup
    transform = self.TRANSFORMS.get(expression.__class__)
    if callable(transform):
        sql = transform(self, expression)

    # 2. Try named method
    elif hasattr(self, f"{expression.key}_sql"):
        sql = getattr(self, f"{expression.key}_sql")(expression)

    # 3. Fallback handlers
    elif isinstance(expression, exp.Func):
        sql = self.function_fallback_sql(expression)

    return sql
```

### TRANSFORMS Dictionary

Maps 400+ expression types to SQL generation:

```python
TRANSFORMS = {
    # Simple binary operations
    exp.BitwiseAnd: lambda self, e: self.binary(e, "&"),
    exp.Or: lambda self, e: self.binary(e, "OR"),

    # Function renames
    exp.Explode: rename_func("UNNEST"),

    # Complex transformations
    exp.DateDiff: _date_diff_sql,
    exp.Select: lambda self, e: self.select_sql(e),
}
```

### Dialect Customization

Dialects override TRANSFORMS for different syntax:

```python
# PostgreSQL: || is string concat
exp.Concat: lambda self, e: self.expressions(e, sep=" || ")

# MySQL: Use CONCAT function
exp.Concat: lambda self, e: self.func("CONCAT", *e.expressions)
```

### Pretty-Printing

The generator supports intelligent formatting:

```python
def sep(self, sep=" "):
    return f"{sep.strip()}\n" if self.pretty else sep

def indent(self, sql, level=0):
    if not self.pretty:
        return sql
    return "\n".join(f"{' ' * level * self._indent}{line}" for line in sql.split("\n"))
```

### Identifier Quoting

Per-dialect quote character selection:

| Dialect | Identifier Quote | Example |
|---------|-----------------|---------|
| PostgreSQL | `"` | `"column"` |
| MySQL | `` ` `` | `` `column` `` |
| BigQuery | `` ` `` | `` `column` `` |
| TSQL | `[` `]` | `[column]` |

---

## 5. Dialect System

### Three-Level Inheritance

Each dialect defines inner classes for each component:

```python
class Postgres(Dialect):
    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {...}

    class Parser(parser.Parser):
        FUNCTIONS = {...}
        RANGE_PARSERS = {...}

    class Generator(generator.Generator):
        TYPE_MAPPING = {...}
        TRANSFORMS = {...}
```

### Key Dialect Differences

| Feature | PostgreSQL | MySQL | BigQuery |
|---------|------------|-------|----------|
| Identifier Quote | `"` | `` ` `` | `` ` `` |
| String Concat | `\|\|` | `CONCAT()` | `CONCAT()` |
| Cast Syntax | `::` | `CAST()` | `CAST()` |
| Division | Typed | Safe | Safe |
| QUALIFY Clause | No | No | Yes |
| ILIKE | Yes | No | No |
| Array Syntax | `ARRAY[...]` | N/A | `[...]` |

### Configuration Flags

60+ boolean flags control behavior:

```python
# PostgreSQL
TYPED_DIVISION = True          # int / int = int
DPIPE_IS_STRING_CONCAT = True  # || means concat

# MySQL
SAFE_DIVISION = True           # div by zero = NULL
DPIPE_IS_STRING_CONCAT = False # || means OR

# BigQuery
SUPPORTS_STRUCT_STAR_EXPANSION = True
QUALIFY_CLAUSE_SUPPORTED = True
```

---

## 6. Expression System (AST)

### 202 Expression Types

Organized hierarchically with `arg_types` defining children:

```python
class Select(Expression):
    arg_types = {
        "expressions": True,   # required - SELECT columns
        "from": False,         # optional - FROM clause
        "where": False,        # optional - WHERE clause
        "group": False,        # optional - GROUP BY
        "having": False,       # optional - HAVING
        "order": False,        # optional - ORDER BY
        "limit": False,        # optional - LIMIT
        # ...
    }
```

### Expression Categories

- **Query**: Select, Union, Intersect, CTE, Subquery
- **DML**: Insert, Update, Delete, Merge
- **DDL**: Create, Alter, Drop (Table, View, Index)
- **Clauses**: Where, GroupBy, OrderBy, Having, Join, Window
- **Functions**: 100+ (Aggregate, Math, String, Date, JSON)
- **Operators**: Binary, Unary, Comparison, Logical
- **Literals**: String, Number, Boolean, Null, Interval
- **Types**: DataType with nested Array, Map, Struct

---

## 7. Key Design Patterns

1. **Metaclass Pattern**: Auto-generates expression keys and validates arg_types
2. **Visitor Pattern**: Generator dispatches to specific handlers via TRANSFORMS
3. **Factory Pattern**: `Dialect.get_or_raise()` for lazy dialect instantiation
4. **Trie Data Structure**: Efficient multi-character keyword matching in tokenizer
5. **Strategy Pattern**: Dialect-specific implementations swap parsing/generation behavior
6. **Builder Pattern**: Helper functions construct complex expressions

---

## 8. Testing Strategy

The test suite (`tests/`) contains 57 files with 44,543 lines covering:

1. **Round-trip tests**: `parse(generate(parse(sql))) == parse(sql)`
2. **Dialect pair tests**: Transpile between all combinations
3. **Edge cases**: Complex nesting, operator precedence, special syntax
4. **Real-world queries**: Production query examples

---

This architecture enables SQLGlot to support 34+ dialects with clean separation between tokenization, parsing, AST representation, and code generation, while allowing extensive per-dialect customization at each layer.
