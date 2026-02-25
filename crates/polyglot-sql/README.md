# polyglot-sql

Core SQL parsing and dialect translation library for Rust. Parses, generates, transpiles, and formats SQL across 32 database dialects.

Part of the [Polyglot](https://github.com/tobilg/polyglot) project.

## Features

- **Parse** SQL into a fully-typed AST with 200+ expression types
- **Generate** SQL from AST nodes for any target dialect
- **Transpile** between any pair of 32 dialects in one call
- **Format** / pretty-print SQL
- **Fluent builder API** for constructing queries programmatically
- **AST traversal** utilities (DFS/BFS iterators, transform, walk)
- **Validation** with syntax checking and error location reporting
- **Schema** module for column resolution and type annotation

## Usage

### Transpile

```rust
use polyglot_sql::{transpile, DialectType};

let result = transpile(
    "SELECT IFNULL(a, b) FROM t",
    DialectType::MySQL,
    DialectType::Postgres,
).unwrap();
assert_eq!(result[0], "SELECT COALESCE(a, b) FROM t");
```

### Parse + Generate

```rust
use polyglot_sql::{parse, generate, DialectType};

let ast = parse("SELECT 1 + 2", DialectType::Generic).unwrap();
let sql = generate(&ast[0], DialectType::Postgres).unwrap();
assert_eq!(sql, "SELECT 1 + 2");
```

### Format With Guard Options

Formatting is protected by guard limits by default:
- `max_input_bytes`: `16 * 1024 * 1024`
- `max_tokens`: `1_000_000`
- `max_ast_nodes`: `1_000_000`
- `max_set_op_chain`: `256`

You can override these limits per call:

```rust
use polyglot_sql::{format_with_options, DialectType, FormatGuardOptions};

let options = FormatGuardOptions {
    max_input_bytes: Some(2 * 1024 * 1024),
    max_tokens: Some(250_000),
    max_ast_nodes: Some(250_000),
    max_set_op_chain: Some(128),
};

let formatted = format_with_options("SELECT a,b FROM t", DialectType::Postgres, &options).unwrap();
assert!(formatted[0].contains("SELECT"));
```

Guard failures include stable codes in the error message:
- `E_GUARD_INPUT_TOO_LARGE`
- `E_GUARD_TOKEN_BUDGET_EXCEEDED`
- `E_GUARD_AST_BUDGET_EXCEEDED`
- `E_GUARD_SET_OP_CHAIN_EXCEEDED`

### Fluent Builder

```rust
use polyglot_sql::builder::*;

// SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10
let expr = select(["id", "name"])
    .from("users")
    .where_(col("age").gt(lit(18)))
    .order_by(["name"])
    .limit(10)
    .build();
```

#### Expression Helpers

```rust
use polyglot_sql::builder::*;

// Column references (supports dotted names)
let c = col("users.id");

// Literals
let s = lit("hello");   // 'hello'
let n = lit(42);         // 42
let f = lit(3.14);       // 3.14
let b = lit(true);       // TRUE

// Operators
let cond = col("age").gte(lit(18)).and(col("status").eq(lit("active")));

// Functions
let f = func("COALESCE", [col("a"), col("b"), null()]);
```

#### CASE Expressions

```rust
use polyglot_sql::builder::*;

let expr = case()
    .when(col("x").gt(lit(0)), lit("positive"))
    .when(col("x").eq(lit(0)), lit("zero"))
    .else_(lit("negative"))
    .build();
```

#### Set Operations

```rust
use polyglot_sql::builder::*;

let expr = union_all(
    select(["id"]).from("a"),
    select(["id"]).from("b"),
)
.order_by(["id"])
.limit(5)
.build();
```

#### INSERT, UPDATE, DELETE

```rust
use polyglot_sql::builder::*;

// INSERT INTO users (id, name) VALUES (1, 'Alice')
let ins = insert_into("users")
    .columns(["id", "name"])
    .values([lit(1), lit("Alice")])
    .build();

// UPDATE users SET name = 'Bob' WHERE id = 1
let upd = update("users")
    .set("name", lit("Bob"))
    .where_(col("id").eq(lit(1)))
    .build();

// DELETE FROM users WHERE id = 1
let del = delete("users")
    .where_(col("id").eq(lit(1)))
    .build();
```

### AST Traversal

```rust
use polyglot_sql::{parse, DialectType, traversal::*};

let ast = parse("SELECT a, b FROM t WHERE x > 1", DialectType::Generic).unwrap();
let columns = get_columns(&ast[0]);
let tables = get_tables(&ast[0]);
```

### Validation

```rust
use polyglot_sql::{validate, DialectType};

let result = validate("SELECT * FORM users", DialectType::Generic);
// result contains error with line/column location
```

```rust
use polyglot_sql::{
    validate_with_schema, DialectType, SchemaColumn, SchemaTable, SchemaValidationOptions,
    ValidationSchema,
};

let schema = ValidationSchema {
    strict: Some(true),
    tables: vec![
        SchemaTable {
            name: "users".into(),
            schema: None,
            columns: vec![
                SchemaColumn {
                    name: "id".into(),
                    data_type: "integer".into(),
                    nullable: Some(false),
                    primary_key: true,
                    unique: false,
                    references: None,
                },
                SchemaColumn {
                    name: "email".into(),
                    data_type: "varchar".into(),
                    nullable: Some(false),
                    primary_key: false,
                    unique: true,
                    references: None,
                },
            ],
            aliases: vec![],
            primary_key: vec!["id".into()],
            unique_keys: vec![vec!["email".into()]],
            foreign_keys: vec![],
        },
    ],
};

let opts = SchemaValidationOptions {
    check_types: true,
    check_references: true,
    strict: None,
    semantic: true,
};

let result = validate_with_schema(
    "SELECT id FROM users WHERE email = 1",
    DialectType::Generic,
    &schema,
    &opts,
);
assert!(!result.valid);
```

Schema-aware validation emits stable codes such as:
- `E200`/`E201` for unknown tables/columns
- `E210-E217` and `W210-W216` for type checks
- `E220`, `E221`, `W220`, `W221`, `W222` for reference/FK checks

### Error Reporting

Parse and tokenize errors include source position information (line and column numbers), making it straightforward to provide precise error feedback.

```rust
use polyglot_sql::{parse, DialectType};

let result = parse("SELECT 1 +", DialectType::Generic);
if let Err(e) = result {
    println!("{}", e);          // "Parse error at line 1, column 11: ..."
    println!("{:?}", e.line());   // Some(1)
    println!("{:?}", e.column()); // Some(11)
}
```

The `Error` enum provides `line()` and `column()` accessors that return `Option<usize>` for `Parse`, `Tokenize`, and `Syntax` error variants:

```rust
use polyglot_sql::error::Error;

let err = Error::parse("Unexpected token", 3, 15);
assert_eq!(err.line(), Some(3));
assert_eq!(err.column(), Some(15));

// Generation errors don't carry position info
let err = Error::generate("unsupported expression");
assert_eq!(err.line(), None);
```

## Supported Dialects

Athena, BigQuery, ClickHouse, CockroachDB, Databricks, Doris, Dremio, Drill, Druid, DuckDB, Dune, Exasol, Fabric, Hive, Materialize, MySQL, Oracle, PostgreSQL, Presto, Redshift, RisingWave, SingleStore, Snowflake, Solr, Spark, SQLite, StarRocks, Tableau, Teradata, TiDB, Trino, TSQL

## Feature Flags

| Flag | Description |
|------|-------------|
| `bindings` | Enable `ts-rs` TypeScript type generation |

## License

[MIT](../../LICENSE)
