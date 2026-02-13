# Polyglot SQL

Rust/WASM-powered SQL transpiler for TypeScript. Parse, generate, transpile, format, and build SQL across 32 database dialects.

## Packages

| Package | Description |
|---------|-------------|
| [@polyglot-sql/sdk](https://www.npmjs.com/package/@polyglot-sql/sdk) | TypeScript SDK with WASM-powered SQL transpilation |

## Quick Start

### Installation

```bash
npm install @polyglot-sql/sdk
```

### Transpile between dialects

```typescript
import { transpile, Dialect } from '@polyglot-sql/sdk';

const result = transpile(
  'SELECT IFNULL(a, b) FROM t',
  Dialect.MySQL,
  Dialect.PostgreSQL,
);
console.log(result.sql[0]); // SELECT COALESCE(a, b) FROM t
```

### Parse and generate

```typescript
import { parse, generate, Dialect } from '@polyglot-sql/sdk';

const { ast } = parse('SELECT 1 + 2', Dialect.Generic);
const { sql } = generate(ast, Dialect.PostgreSQL);
```

### Fluent query builder

```typescript
import { select, col, lit } from '@polyglot-sql/sdk';

const sql = select('id', 'name')
  .from('users')
  .where(col('age').gt(lit(18)))
  .orderBy(col('name').asc())
  .limit(10)
  .toSql('postgresql');
```

### Format SQL

```typescript
import { format, Dialect } from '@polyglot-sql/sdk';

const { sql } = format('SELECT a,b FROM t WHERE x=1', Dialect.PostgreSQL);
```

## Supported Dialects

Athena, BigQuery, ClickHouse, CockroachDB, Databricks, Dremio, Drill, Druid, DuckDB, Dune, Exasol, Fabric, Hive, Materialize, MySQL, Oracle, PostgreSQL, Presto, Redshift, RisingWave, SingleStore, Snowflake, Solr, Spark, SQLite, StarRocks, Tableau, Teradata, TiDB, Trino, TSQL (SQL Server), and Doris.

## Links

- [GitHub](https://github.com/tobilg/polyglot)
- [npm](https://www.npmjs.com/package/@polyglot-sql/sdk)
- [Rust crate](https://crates.io/crates/polyglot-sql)
