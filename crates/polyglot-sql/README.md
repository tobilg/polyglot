# polyglot-sql

Core SQL parsing and dialect translation library for Rust. Parses, generates, transpiles, and formats SQL across 32+ database dialects.

Part of the [Polyglot](https://github.com/tobilg/polyglot) project.

## Features

- **Parse** SQL into a fully-typed AST with 200+ expression types
- **Generate** SQL from AST nodes for any target dialect
- **Transpile** between any pair of 32+ dialects in one call
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

## Supported Dialects

Athena, BigQuery, ClickHouse, CockroachDB, Databricks, Doris, Dremio, Drill, Druid, DuckDB, Dune, Exasol, Fabric, Hive, Materialize, MySQL, Oracle, PostgreSQL, Presto, Redshift, RisingWave, SingleStore, Snowflake, Solr, Spark, SQLite, StarRocks, Tableau, Teradata, TiDB, Trino, TSQL

## Feature Flags

| Flag | Description |
|------|-------------|
| `bindings` | Enable `ts-rs` TypeScript type generation |

## License

[MIT](../../LICENSE)
