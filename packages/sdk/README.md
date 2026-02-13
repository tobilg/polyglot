# @polyglot-sql/sdk

Rust/WASM-powered SQL transpiler for TypeScript. Parse, generate, transpile, format, and build SQL across 32 database dialects.

Part of the [Polyglot](https://github.com/tobilg/polyglot) project.

## Installation

```bash
npm install @polyglot-sql/sdk
```

## Quick Start

### Transpile

```typescript
import { transpile, Dialect } from '@polyglot-sql/sdk';

const result = transpile(
  'SELECT IFNULL(a, b) FROM t',
  Dialect.MySQL,
  Dialect.PostgreSQL,
);
console.log(result.sql[0]); // SELECT COALESCE(a, b) FROM t
```

### Parse + Generate

```typescript
import { parse, generate, Dialect } from '@polyglot-sql/sdk';

const { ast } = parse('SELECT 1 + 2', Dialect.Generic);
const { sql } = generate(ast, Dialect.PostgreSQL);
console.log(sql[0]); // SELECT 1 + 2
```

### Format

```typescript
import { format, Dialect } from '@polyglot-sql/sdk';

const { sql } = format('SELECT a,b FROM t WHERE x=1', Dialect.PostgreSQL);
console.log(sql[0]);
// SELECT
//   a,
//   b
// FROM t
// WHERE
//   x = 1
```

## Fluent Query Builder

Build SQL queries programmatically with full type safety. All builder operations are backed by the Rust engine via WASM.

### SELECT

```typescript
import { select, col, lit } from '@polyglot-sql/sdk';

const sql = select('id', 'name')
  .from('users')
  .where(col('age').gt(lit(18)))
  .orderBy(col('name').asc())
  .limit(10)
  .toSql('postgresql');

// SELECT id, name FROM users WHERE age > 18 ORDER BY name ASC LIMIT 10
```

#### Joins

```typescript
const sql = select('u.id', 'o.total')
  .from('users')
  .join('orders', col('u.id').eq(col('o.user_id')))
  .leftJoin('addresses', col('u.id').eq(col('a.user_id')))
  .toSql();
```

#### GROUP BY / HAVING

```typescript
const sql = select(col('dept'), count())
  .from('employees')
  .groupBy('dept')
  .having(count().gt(lit(5)))
  .toSql();
```

#### DISTINCT / QUALIFY

```typescript
const sql = select('id', 'name')
  .from('users')
  .distinct()
  .qualify(col('rn').eq(lit(1)))
  .toSql('snowflake');
```

### Expression Helpers

```typescript
import { col, lit, star, sqlNull, boolean, table, sqlExpr, func } from '@polyglot-sql/sdk';

col('users.id');         // Column reference (supports dotted names)
lit('hello');            // String literal
lit(42);                 // Numeric literal
star();                  // *
sqlNull();               // NULL
boolean(true);           // TRUE
table('schema.users');   // Table reference
sqlExpr('x + 1');        // Raw SQL fragment
func('MY_FUNC', col('a'), lit(1));  // Function call
```

### Expression Operators

```typescript
import { col, lit } from '@polyglot-sql/sdk';

// Comparison
col('age').eq(lit(18));
col('age').neq(lit(0));
col('age').gt(lit(18));
col('age').gte(lit(18));
col('age').lt(lit(100));
col('age').lte(lit(100));

// Logical
col('a').and(col('b'));
col('a').or(col('b'));
col('a').not();
col('a').xor(col('b'));

// Arithmetic
col('price').mul(lit(1.1));
col('a').add(col('b'));

// Pattern matching
col('name').like(lit('%Alice%'));
col('name').ilike(lit('%alice%'));

// Predicates
col('email').isNull();
col('email').isNotNull();
col('age').between(lit(18), lit(65));
col('status').inList(lit('active'), lit('pending'));

// Transform
col('total').alias('grand_total');  // or .as('grand_total')
col('id').cast('TEXT');
col('name').asc();
col('name').desc();
```

### Convenience Functions

```typescript
import {
  // Aggregate
  count, countDistinct, sum, avg, min, max,
  // String
  upper, lower, length, trim, ltrim, rtrim, reverse, initcap,
  substring, replace, concatWs,
  // Null handling
  coalesce, nullIf, ifNull,
  // Math
  abs, round, floor, ceil, power, sqrt, ln, exp, sign, greatest, least,
  // Date/time
  currentDate, currentTime, currentTimestamp, extract,
  // Window
  rowNumber, rank, denseRank,
  // Logical
  and, or, not, cast, alias,
} from '@polyglot-sql/sdk';

// Examples
count();                          // COUNT(*)
countDistinct(col('id'));         // COUNT(DISTINCT id)
coalesce(col('a'), col('b'));     // COALESCE(a, b)
upper(col('name'));               // UPPER(name)
round(col('price'), lit(2));      // ROUND(price, 2)
extract('YEAR', col('created'));  // EXTRACT(YEAR FROM created)
```

### CASE Expressions

```typescript
import { caseWhen, caseOf, col, lit } from '@polyglot-sql/sdk';

// Searched CASE
const expr = caseWhen()
  .when(col('x').gt(lit(0)), lit('positive'))
  .when(col('x').eq(lit(0)), lit('zero'))
  .else_(lit('negative'))
  .build();

// Simple CASE
const expr2 = caseOf(col('status'))
  .when(lit('A'), lit('Active'))
  .when(lit('I'), lit('Inactive'))
  .else_(lit('Unknown'))
  .build();
```

### INSERT

```typescript
import { insertInto, lit, select } from '@polyglot-sql/sdk';

// INSERT ... VALUES
const sql = insertInto('users')
  .columns('id', 'name')
  .values(lit(1), lit('Alice'))
  .toSql();

// INSERT ... SELECT
const sql2 = insertInto('archive')
  .query(select('*').from('users').where(col('active').eq(lit(false))))
  .toSql();
```

### UPDATE

```typescript
import { update, col, lit } from '@polyglot-sql/sdk';

const sql = update('users')
  .set('name', lit('Bob'))
  .set('updated_at', sqlExpr('NOW()'))
  .where(col('id').eq(lit(1)))
  .toSql();
```

### DELETE

```typescript
import { deleteFrom, col, lit } from '@polyglot-sql/sdk';

const sql = deleteFrom('users')
  .where(col('id').eq(lit(1)))
  .toSql();
```

### MERGE

```typescript
import { mergeInto, col } from '@polyglot-sql/sdk';

const sql = mergeInto('target')
  .using('source', col('target.id').eq(col('source.id')))
  .whenMatchedUpdate({ name: col('source.name') })
  .whenNotMatchedInsert(
    ['id', 'name'],
    [col('source.id'), col('source.name')]
  )
  .toSql();
```

### Set Operations

```typescript
import { select, union, unionAll, intersect, except } from '@polyglot-sql/sdk';

const q1 = select('id').from('a');
const q2 = select('id').from('b');

union(q1, q2).toSql();
unionAll(q1, q2).orderBy(col('id').asc()).limit(10).toSql();
intersect(q1, q2).toSql();
except(q1, q2).toSql();
```

## AST Visitor

Walk and transform parsed AST nodes.

```typescript
import { parse, Dialect, walk, transform, findAll, getColumns, renameColumns } from '@polyglot-sql/sdk';

const { ast } = parse('SELECT a, b FROM t WHERE x > 1', Dialect.Generic);

// Walk all nodes
walk(ast, (node) => console.log(node));

// Find all nodes matching a predicate
const columns = getColumns(ast);

// Transform AST nodes
const transformed = transform(ast, (node) => {
  // Return modified node or undefined to keep original
  return node;
});

// Rename columns
const renamed = renameColumns(ast, { a: 'alpha', b: 'beta' });
```

## Validation

### Syntax Validation

```typescript
import { validate } from '@polyglot-sql/sdk';

const result = validate('SELECT * FROM users', 'postgresql');
if (!result.valid) {
  for (const err of result.errors) {
    console.log(`${err.code}: ${err.message} (line ${err.line}, col ${err.column})`);
  }
}
```

### Semantic Validation

```typescript
const result = validate('SELECT * FROM users', 'postgresql', { semantic: true });
// May also report warnings like "SELECT * is discouraged"
```

### Schema Validation

```typescript
import { validateWithSchema } from '@polyglot-sql/sdk';

const schema = {
  tables: [{
    name: 'users',
    columns: [
      { name: 'id', type: 'integer', primaryKey: true },
      { name: 'name', type: 'varchar' },
      { name: 'email', type: 'varchar' },
    ],
  }],
};

const result = validateWithSchema(
  'SELECT id, name, unknown_col FROM users',
  schema,
  'postgresql'
);
// result.errors will contain an error for unknown_col
```

## Class-Based API

For an object-oriented style, use the singleton `Polyglot` class:

```typescript
import { Polyglot, Dialect } from '@polyglot-sql/sdk';

const pg = Polyglot.getInstance();
const result = pg.transpile('SELECT 1', Dialect.MySQL, Dialect.PostgreSQL);
const formatted = pg.format('SELECT a,b FROM t');
```

## API Reference

### Core Functions

| Function | Description |
|----------|-------------|
| `transpile(sql, read, write)` | Transpile SQL between dialects |
| `parse(sql, dialect?)` | Parse SQL into AST |
| `generate(ast, dialect?)` | Generate SQL from AST |
| `format(sql, dialect?)` | Pretty-print SQL |
| `validate(sql, dialect?, options?)` | Validate SQL syntax/semantics |
| `validateWithSchema(sql, schema, dialect?, options?)` | Validate against a database schema |
| `getDialects()` | List supported dialect names |
| `getVersion()` | Get library version |

### Expression Helpers

| Function | Description |
|----------|-------------|
| `col(name)` | Column reference |
| `lit(value)` | Literal value (string, number, boolean, null) |
| `star()` | Star (`*`) expression |
| `sqlNull()` | NULL literal |
| `boolean(value)` | Boolean literal |
| `table(name)` | Table reference |
| `sqlExpr(sql)` | Parse raw SQL fragment |
| `condition(sql)` | Alias for `sqlExpr` |
| `func(name, ...args)` | Function call |
| `not(expr)` | NOT expression |
| `cast(expr, type)` | CAST expression |
| `alias(expr, name)` | Alias expression |
| `and(...conditions)` | Chain with AND |
| `or(...conditions)` | Chain with OR |

### Query Builders

| Builder | Constructor | Description |
|---------|------------|-------------|
| `SelectBuilder` | `select(...cols)` | SELECT queries |
| `InsertBuilder` | `insertInto(table)` / `insert(table)` | INSERT statements |
| `UpdateBuilder` | `update(table)` | UPDATE statements |
| `DeleteBuilder` | `deleteFrom(table)` / `del(table)` | DELETE statements |
| `MergeBuilder` | `mergeInto(table)` | MERGE statements |
| `CaseBuilder` | `caseWhen()` / `caseOf(expr)` | CASE expressions |
| `SetOpBuilder` | `union()` / `unionAll()` / `intersect()` / `except()` | Set operations |

### AST Visitor

| Function | Description |
|----------|-------------|
| `walk(node, callback)` | Walk all AST nodes |
| `findAll(node, predicate)` | Find nodes matching a predicate |
| `transform(node, callback)` | Transform AST nodes |
| `renameColumns(node, mapping)` | Rename columns in AST |
| `renameTables(node, mapping)` | Rename tables in AST |
| `getColumns(node)` | Get all column references |
| `getTables(node)` | Get all table references |
| `qualifyColumns(node, table)` | Add table qualifier to columns |
| `addWhere(node, condition)` | Add WHERE clause |
| `clone(node)` | Deep clone AST |

## Supported Dialects

| Dialect | Enum Value |
|---------|-----------|
| Athena | `Dialect.Athena` |
| BigQuery | `Dialect.BigQuery` |
| ClickHouse | `Dialect.ClickHouse` |
| CockroachDB | `Dialect.CockroachDB` |
| Databricks | `Dialect.Databricks` |
| Doris | `Dialect.Doris` |
| Dremio | `Dialect.Dremio` |
| Drill | `Dialect.Drill` |
| Druid | `Dialect.Druid` |
| DuckDB | `Dialect.DuckDB` |
| Dune | `Dialect.Dune` |
| Exasol | `Dialect.Exasol` |
| Fabric | `Dialect.Fabric` |
| Hive | `Dialect.Hive` |
| Materialize | `Dialect.Materialize` |
| MySQL | `Dialect.MySQL` |
| Oracle | `Dialect.Oracle` |
| PostgreSQL | `Dialect.PostgreSQL` |
| Presto | `Dialect.Presto` |
| Redshift | `Dialect.Redshift` |
| RisingWave | `Dialect.RisingWave` |
| SingleStore | `Dialect.SingleStore` |
| Snowflake | `Dialect.Snowflake` |
| Solr | `Dialect.Solr` |
| Spark | `Dialect.Spark` |
| SQLite | `Dialect.SQLite` |
| StarRocks | `Dialect.StarRocks` |
| Tableau | `Dialect.Tableau` |
| Teradata | `Dialect.Teradata` |
| TiDB | `Dialect.TiDB` |
| Trino | `Dialect.Trino` |
| TSQL | `Dialect.TSQL` |

## CDN Usage

For browser use without a bundler:

```html
<script type="module">
  import polyglot from 'https://unpkg.com/@polyglot-sql/sdk/dist/cdn/polyglot.esm.js';
  // or: https://cdn.jsdelivr.net/npm/@polyglot-sql/sdk/dist/cdn/polyglot.esm.js

  const { transpile, Dialect } = polyglot;
  const result = transpile('SELECT 1', Dialect.MySQL, Dialect.PostgreSQL);
  console.log(result.sql);
</script>
```

## License

[MIT](../../LICENSE)
