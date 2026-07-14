# polyglot-sql

Core SQL parsing and dialect translation library for Rust. Parses, generates, transpiles, and formats SQL across more than 30 SQL dialects.

Part of the [Polyglot](https://github.com/tobilg/polyglot) project.

## Features

- **Parse** SQL into a fully-typed AST with 200+ expression types
- **Parse standalone data types** such as `DECIMAL(10, 2)` without a statement wrapper
- **Generate** SQL from AST nodes for any target dialect
- **Transpile** between any pair of more than 30 SQL dialects in one call
- **Format** / pretty-print SQL
- **Fluent builder API** for constructing queries programmatically
- **AST traversal** utilities (DFS/BFS iterators, transform, walk)
- **Validation** with syntax checking and error location reporting
- **Schema** module for column resolution and type annotation
- **Compact query analysis** for projection, relation, CTE, set-operation, and upstream-reference facts

## Usage

### Cargo Features

By default, `polyglot-sql` enables the full public API. Parser-only consumers can
disable default features and opt into only the dialect parsers they need:

```toml
polyglot-sql = { version = "0.6.0", default-features = false }
```

```toml
polyglot-sql = {
    version = "0.5",
    default-features = false,
    features = ["dialect-clickhouse"],
}
```

Optional capability features include `generate`, `transpile`, `builder`,
`ast-tools`, `semantic`, `openlineage`, `diff`, `planner`, and `time`.

Examples:

```toml
# Parse and generate SQL for one dialect.
polyglot-sql = {
    version = "0.5",
    default-features = false,
    features = ["generate", "dialect-clickhouse"],
}

# Cross-dialect transpilation.
polyglot-sql = {
    version = "0.5",
    default-features = false,
    features = ["transpile", "dialect-clickhouse", "dialect-postgresql"],
}
```

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

You can also transpile through a `Dialect` handle directly — useful when you
already hold one (e.g., for custom dialects) or need pretty-printed output:

```rust
use polyglot_sql::{Dialect, DialectType, TranspileOptions};

let mysql = Dialect::get(DialectType::MySQL);

// Built-in target via DialectType
let plain = mysql.transpile("SELECT IFNULL(a, b) FROM t", DialectType::Postgres).unwrap();

// Pretty-printed output via TranspileOptions
let pretty = mysql
    .transpile_with(
        "SELECT IFNULL(a, b) FROM t",
        DialectType::Postgres,
        TranspileOptions::pretty(),
    )
    .unwrap();

// Target a custom (or built-in) Dialect handle directly
let pg = Dialect::get(DialectType::Postgres);
let via_handle = mysql.transpile("SELECT IFNULL(a, b) FROM t", &pg).unwrap();
```

### Parse + Generate

```rust
use polyglot_sql::{parse, generate, DialectType};

let ast = parse("SELECT 1 + 2", DialectType::Generic).unwrap();
let sql = generate(&ast[0], DialectType::Postgres).unwrap();
assert_eq!(sql, "SELECT 1 + 2");
```

### Standalone Data Types

```rust
use polyglot_sql::{generate_data_type, parse_data_type, DialectType};

let data_type = parse_data_type("DECIMAL(10, 2)", DialectType::DuckDB).unwrap();
let sql = generate_data_type(&data_type, DialectType::Postgres).unwrap();
assert_eq!(sql, "DECIMAL(10, 2)");
```

`parse_data_type` parses exactly one type string and rejects trailing SQL. Type
rendering requires the `generate` feature, which is enabled by default.

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
use polyglot_sql::{validate_with_options, DialectType, ValidationOptions};

let result = validate_with_options(
    "SELECT * FROM users LIMIT 10",
    DialectType::Generic,
    &ValidationOptions {
        strict_syntax: false,
        semantic: true,
    },
);
// W001 and W004 are warnings, so result.valid remains true.
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

### Compact Query Analysis

Use `analyze_query` when you need high-level facts without consuming the full AST
or full lineage graph. `relations` reports sources visible in the analyzed
scope, while `base_tables` reports deduplicated physical table dependencies
across CTEs, derived tables, subqueries, and set-operation branches. When a
`ValidationSchema` is supplied, detailed type strings such as `DECIMAL(10,2)`
are preserved in projection `type_hint` values when parseable.
For physical table relations, `name` remains the qualified display name and
`catalog`, `schema`, and `table` expose parsed identifier parts.
`cte_facts` reports top-level CTE names, declared columns, original CTE body SQL,
and CTE output columns. `star_projections` reports the original top-level star
projection index, optional table qualifier, and schema-expanded columns when
known. Each projection also includes conservative `nullability`:
`non_null`, `nullable`, or `unknown`.

```rust
use polyglot_sql::{
    analyze_query, AnalyzeQueryOptions, DialectType, ProjectionNullability, QueryShape,
};

let schema: polyglot_sql::ValidationSchema = serde_json::from_value(serde_json::json!({
    "tables": [{
        "name": "orders",
        "columns": [
            {"name": "id", "type": "INT", "nullable": false},
            {"name": "amount", "type": "DECIMAL(10,2)", "nullable": true}
        ]
    }]
})).unwrap();

let analysis = analyze_query(
    "WITH base AS (SELECT id, amount FROM orders) SELECT * FROM base",
    AnalyzeQueryOptions {
        dialect: DialectType::Generic,
        schema: Some(schema),
    },
).unwrap();

assert_eq!(analysis.shape, QueryShape::Select);
assert_eq!(analysis.cte_facts[0].name, "base");
assert_eq!(analysis.cte_facts[0].body_sql, "SELECT id, amount FROM orders");
assert_eq!(analysis.star_projections[0].expanded_columns, vec!["id", "amount"]);
assert_eq!(analysis.projections[0].nullability, ProjectionNullability::NonNull);
assert_eq!(analysis.base_tables[0].name, "orders");
assert_eq!(analysis.base_tables[0].table.as_deref(), Some("orders"));
```

External JSON schemas use this shape:

```json
{
  "strict": true,
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
          "references": { "table": "customers", "columns": ["id"] }
        }
      ],
      "columns": [
        { "name": "id", "type": "INT", "nullable": false, "primaryKey": true },
        { "name": "amount", "type": "DECIMAL(10,2)", "nullable": true }
      ]
    }
  ]
}
```

Use the `type` key for column types in JSON. `dataType` / `data_type` are not
accepted aliases.

### Tokenize

Access the raw token stream with full source position spans. Each token carries a `Span` with byte offsets and line/column numbers.

```rust
use polyglot_sql::{DialectType, Dialect};

let dialect = Dialect::new(DialectType::Generic);
let tokens = dialect.tokenize("SELECT a, b FROM t").unwrap();

for token in &tokens {
    println!("{:?} {:?} {:?}", token.token_type, token.text, token.span);
    // Select "SELECT" Span { start: 0, end: 6, line: 1, column: 1 }
    // Var    "a"      Span { start: 7, end: 8, line: 1, column: 8 }
    // ...
}
```

The `Span` struct provides:

| Field | Type | Description |
|-------|------|-------------|
| `start` | `usize` | Start byte offset (0-based) |
| `end` | `usize` | End byte offset (exclusive) |
| `line` | `usize` | Line number (1-based) |
| `column` | `usize` | Column number (1-based) |

### Error Reporting

Parse and tokenize errors include source position information with line/column numbers and byte offset ranges, making it straightforward to provide precise error feedback.

```rust
use polyglot_sql::{parse, DialectType};

let result = parse("SELECT 1 +", DialectType::Generic);
if let Err(e) = result {
    println!("{}", e);            // "Parse error at line 1, column 11: ..."
    println!("{:?}", e.line());   // Some(1)
    println!("{:?}", e.column()); // Some(11)
    println!("{:?}", e.start());  // Some(10) — byte offset
    println!("{:?}", e.end());    // Some(11) — byte offset (exclusive)
}
```

The `Error` enum provides `line()`, `column()`, `start()`, and `end()` accessors that return `Option<usize>` for `Parse`, `Tokenize`, and `Syntax` error variants:

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

Athena, BigQuery, ClickHouse, CockroachDB, DataFusion, Databricks, Doris, Dremio, Drill, Druid, DuckDB, Dune, Exasol, Fabric, Generic SQL, Hive, Materialize, MySQL, Oracle, PostgreSQL, Presto, Redshift, RisingWave, SingleStore, Snowflake, Solr, Spark, SQLite, StarRocks, Tableau, Teradata, TiDB, Trino, TSQL

## Feature Flags

| Flag | Description |
|------|-------------|
| `generate` | Enable SQL generation and formatting from AST nodes |
| `transpile` | Enable cross-dialect transpilation; implies `generate` |
| `builder` | Enable the fluent query builder API; implies `generate` |
| `ast-tools` | Enable AST inspection and transform helper APIs |
| `semantic` | Enable schema, resolver, lineage, optimizer, and validation APIs |
| `openlineage` | Enable OpenLineage payload generation; implies `semantic` |
| `diff` | Enable AST diff support; implies `generate` |
| `planner` | Enable logical planning helpers |
| `time` | Enable time-format conversion helpers |
| `bindings` | Enable `ts-rs` TypeScript type generation |

## License

[MIT](../../LICENSE)
