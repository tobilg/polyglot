# Polyglot - Claude Code Context

## Project Overview

Polyglot is a WASM-based SQL dialect translator inspired by Python's sqlglot library. It enables SQL transpilation between 30+ database dialects in the browser via a TypeScript SDK.

## SQLGlot Source Analysis

The reference implementation in `external-projects/sqlglot` provides the authoritative source for porting SQL parsing and generation logic to Rust.

### Key Architecture Patterns

#### 1. Expression System (expressions.py)
- **202 Expression subclasses** organized hierarchically
- Each expression defines `arg_types` dict specifying child nodes and required status
- Key pattern: `this` for primary argument, `expression` for secondary, `expressions` for lists
- Expressions carry metadata: `type`, `meta`, `parent`, `comments`

**Rust Mapping:**
```rust
// Python: class Select(Expression): arg_types = {"expressions": True, "from": False, ...}
// Rust equivalent:
pub enum Expression {
    Select(SelectExpr),
    // ...
}

pub struct SelectExpr {
    pub expressions: Vec<Expression>,  // required
    pub from: Option<Box<Expression>>, // optional
    // ...
}
```

#### 2. Token System (tokens.py)
- **200+ TokenType enum values** covering all SQL syntax
- Token struct carries: `token_type`, `text`, `line`, `col`, `start`, `end`, `comments`
- Tokenizer uses Trie data structure for multi-character keyword matching
- Comments are preserved and attached to tokens

**Rust Mapping:**
- TokenType enum already implemented in `crates/polyglot-sql/src/tokens.rs`
- Consider using `logos` crate or hand-written scanner (current approach)

#### 3. Parser (parser.py)
- **Recursive descent parser** with operator precedence climbing
- 500+ parsing methods of form `_parse_<construct>()`
- Expression builders: `FUNCTIONS` dict maps function names to builder callables
- Configuration maps: `NO_PAREN_FUNCTIONS`, `TYPE_TOKENS`, `SUBQUERY_PREDICATES`

**Key Parser Maps to Port:**
- `TYPE_TOKENS`: 60+ type keywords (BIGINT, VARCHAR, TIMESTAMP, etc.)
- `NESTED_TYPE_TOKENS`: Parametric types (ARRAY, MAP, STRUCT)
- `NO_PAREN_FUNCTIONS`: Zero-arg functions (CURRENT_DATE, etc.)
- `RESERVED_TOKENS`: Cannot be identifiers without quoting

#### 4. Generator (generator.py)
- **Visitor pattern** with TRANSFORMS dispatch table
- 200+ expression type → SQL generation function mappings
- Handles pretty printing, identifier quoting, case normalization
- Dialect subclasses override TRANSFORMS for syntax differences

**Rust Mapping:**
```rust
// Python: TRANSFORMS = {exp.Select: lambda self, e: self.select_sql(e), ...}
// Rust equivalent:
impl Generator {
    fn generate(&self, expr: &Expression) -> String {
        match expr {
            Expression::Select(s) => self.generate_select(s),
            // ...
        }
    }
}
```

#### 5. Dialect System (dialects/)
- **36 supported dialects** with inheritance hierarchy
- Each dialect subclasses Tokenizer, Parser, Generator components
- Configuration via class attributes: `QUOTE_START`, `IDENTIFIER_START`, `NORMALIZE_FUNCTIONS`
- Function/type mappings vary per dialect

**Priority Dialects:**
1. PostgreSQL - most comprehensive feature set
2. MySQL - widely used, different syntax
3. BigQuery - cloud analytics, unique syntax
4. Snowflake - cloud DW, QUALIFY clause
5. DuckDB - modern analytics
6. TSQL - SQL Server syntax

### Porting Guidelines

#### Expression Types to Prioritize

Based on sqlglot's expressions.py, these are the most important categories:

1. **Query Structure**: Select, Union, Intersect, Except, Subquery, CTE
2. **DML**: Insert, Update, Delete, Merge
3. **DDL**: Create, Alter, Drop (Table, View, Index)
4. **Clauses**: Where, GroupBy, OrderBy, Having, Limit, Join, Window
5. **Functions**: 100+ function types organized by category
6. **Operators**: Binary, Unary, Comparison, Logical, Bitwise
7. **Literals**: String, Number, Boolean, Null, Interval
8. **Types**: DataType with nested Array, Map, Struct support

#### Parser Complexity Areas

1. **Operator Precedence**: `_parse_or()` → ... → `_parse_primary()` chain
2. **Function Parsing**: FUNCTIONS dict with special builders
3. **Type Parsing**: Nested parametric types like `ARRAY<MAP<STRING, INT>>`
4. **Window Functions**: Frame specifications with ROWS/RANGE/GROUPS
5. **Subqueries**: Correlated detection, lateral joins


### Testing Strategy

1. **Round-trip tests**: `parse(generate(parse(sql))) == parse(sql)`
2. **Dialect pair tests**: Transpile between all dialect combinations
3. **Port sqlglot tests**: `external-projects/sqlglot/tests/` contains comprehensive test suite
4. **Real-world queries**: Test with production query examples

### Performance Considerations

1. **Avoid allocations**: Use `Cow<str>` for string sharing
2. **Arena allocation**: Consider typed-arena for AST nodes
3. **Lazy dialect loading**: Don't initialize unused dialects
4. **WASM size**: Target <2MB bundle, currently ~1.1MB

## Project Structure

```
polyglot/
├── crates/
│   ├── polyglot-sql/         # Core Rust library
│   │   └── src/
│   │       ├── lib.rs        # Main API
│   │       ├── tokens.rs     # Tokenizer
│   │       ├── expressions.rs # AST types
│   │       ├── parser.rs     # Parser
│   │       ├── generator.rs  # Generator
│   │       └── dialects/     # Dialect implementations
│   └── polyglot-sql-wasm/    # WASM bindings
├── packages/
│   └── sdk/                  # TypeScript SDK
├── external-projects/
│   └── sqlglot/              # Reference Python implementation
└── plans/                    # Implementation plans
```

## Commands

```bash
# Build Rust core
cargo build -p polyglot-sql

# Build WASM
cd crates/polyglot-sql-wasm && wasm-pack build --target web

# Build TypeScript SDK
cd packages/sdk && npm run build

# Build everything
make build-all
```

## Testing

```bash
# Setup fixtures (required once)
make setup-fixtures

# Run all Rust tests
make test-rust

# SQLGlot Fixture Tests (6,284 tests)
make test-rust-all           # All fixture tests
make test-rust-identity      # 955 generic identity tests
make test-rust-dialect       # 3,489 dialect identity tests
make test-rust-transpile     # 1,816 transpilation tests
make test-rust-pretty        # 24 pretty-print tests

# Additional Tests
make test-rust-roundtrip     # 131 organized roundtrip unit tests
make test-rust-matrix        # Dialect matrix transpilation tests
make test-rust-compat        # SQLGlot compatibility tests
make test-rust-errors        # Error handling tests
make test-rust-functions     # Function normalization tests

# TypeScript SDK tests
cd packages/sdk && npm test

# Full comparison against Python SQLGlot (slow)
make test-compare
```

## Key Files for Reference

When implementing new features, consult these sqlglot sources:

- **Tokenizer**: `external-projects/sqlglot/sqlglot/tokens.py`
- **Expressions**: `external-projects/sqlglot/sqlglot/expressions.py`
- **Parser**: `external-projects/sqlglot/sqlglot/parser.py`
- **Generator**: `external-projects/sqlglot/sqlglot/generator.py`
- **Dialects**: `external-projects/sqlglot/sqlglot/dialects/`
- **Tests**: `external-projects/sqlglot/tests/`


## Temporary files
**IMPORTANT**
When you create temporary files, DO NOT heredoc. Write temporary files to the `temporary-files/` directory in the project root instead of `/tmp`.

## Running Python scripts
**IMPORTANT**
Always use `uv run` to run Python scripts, and not python3 directly.
