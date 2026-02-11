/**
 * Polyglot SQL Dialect Translator
 *
 * A WebAssembly-powered SQL dialect translator that can convert SQL
 * between different database dialects (PostgreSQL, MySQL, BigQuery, etc.)
 */

// Import the WASM module - synchronously initialized on import via bundler target
import {
  transpile as wasmTranspile,
  parse as wasmParse,
  generate as wasmGenerate,
  format_sql as wasmFormatSql,
  get_dialects as wasmGetDialects,
  version as wasmVersion,
} from '../wasm/polyglot_sql_wasm.js';

/**
 * Supported SQL dialects
 */
export enum Dialect {
  Generic = 'generic',
  PostgreSQL = 'postgresql',
  MySQL = 'mysql',
  BigQuery = 'bigquery',
  Snowflake = 'snowflake',
  DuckDB = 'duckdb',
  SQLite = 'sqlite',
  Hive = 'hive',
  Spark = 'spark',
  Trino = 'trino',
  Presto = 'presto',
  Redshift = 'redshift',
  TSQL = 'tsql',
  Oracle = 'oracle',
  ClickHouse = 'clickhouse',
  Databricks = 'databricks',
  Athena = 'athena',
  Teradata = 'teradata',
  Doris = 'doris',
  StarRocks = 'starrocks',
  Materialize = 'materialize',
  RisingWave = 'risingwave',
  SingleStore = 'singlestore',
  CockroachDB = 'cockroachdb',
  TiDB = 'tidb',
  Druid = 'druid',
  Solr = 'solr',
  Tableau = 'tableau',
  Dune = 'dune',
  Fabric = 'fabric',
  Drill = 'drill',
  Dremio = 'dremio',
  Exasol = 'exasol',
}

/**
 * Transpilation options
 */
export interface TranspileOptions {
  /** Source dialect to parse the SQL with */
  read: Dialect;
  /** Target dialect to generate SQL for */
  write: Dialect;
}

/**
 * Result of a transpilation operation
 */
export interface TranspileResult {
  success: boolean;
  sql?: string[];
  error?: string;
}

/**
 * Result of a parse operation
 */
export interface ParseResult {
  success: boolean;
  ast?: any;
  error?: string;
}

/**
 * Initialize the WASM module.
 * With the bundler target, the WASM module is synchronously initialized
 * on import. This function is kept for backwards compatibility.
 */
export async function init(): Promise<void> {
  return Promise.resolve();
}

/**
 * Check if the WASM module is initialized.
 */
export function isInitialized(): boolean {
  return true;
}

/**
 * Transpile SQL from one dialect to another.
 *
 * @param sql - The SQL string to transpile
 * @param options - Transpilation options specifying source and target dialects
 * @returns The transpiled SQL statements
 *
 * @example
 * ```typescript
 * const result = transpile(
 *   "SELECT IFNULL(a, b)",
 *   { read: Dialect.MySQL, write: Dialect.PostgreSQL }
 * );
 * // result.sql[0] = "SELECT COALESCE(a, b)"
 * ```
 */
export function transpile(sql: string, options: TranspileOptions): TranspileResult {
  const resultJson = wasmTranspile(sql, options.read, options.write);
  return JSON.parse(resultJson) as TranspileResult;
}

/**
 * Parse SQL into an Abstract Syntax Tree (AST).
 *
 * @param sql - The SQL string to parse
 * @param dialect - The dialect to use for parsing
 * @returns The parsed AST
 *
 * @example
 * ```typescript
 * const result = parse("SELECT a, b FROM t", Dialect.PostgreSQL);
 * console.log(result.ast);
 * ```
 */
export function parse(sql: string, dialect: Dialect = Dialect.Generic): ParseResult {
  const resultJson = wasmParse(sql, dialect);
  const result = JSON.parse(resultJson);

  // Parse the nested AST JSON if present
  if (result.success && result.ast) {
    result.ast = JSON.parse(result.ast);
  }

  return result as ParseResult;
}

/**
 * Generate SQL from an AST.
 *
 * @param ast - The AST to generate SQL from
 * @param dialect - The target dialect
 * @returns The generated SQL
 */
export function generate(ast: any, dialect: Dialect = Dialect.Generic): TranspileResult {
  const astJson = JSON.stringify(ast);
  const resultJson = wasmGenerate(astJson, dialect);
  return JSON.parse(resultJson) as TranspileResult;
}

/**
 * Format/pretty-print SQL.
 *
 * @param sql - The SQL string to format
 * @param dialect - The dialect to use
 * @returns The formatted SQL
 *
 * @example
 * ```typescript
 * const result = format("SELECT a,b FROM t WHERE x=1", Dialect.PostgreSQL);
 * // result.sql[0] = "SELECT\n  a,\n  b\nFROM t\nWHERE x = 1"
 * ```
 */
export function format(sql: string, dialect: Dialect = Dialect.Generic): TranspileResult {
  const resultJson = wasmFormatSql(sql, dialect);
  return JSON.parse(resultJson) as TranspileResult;
}

/**
 * Get list of all supported dialects.
 *
 * @returns Array of dialect names
 */
export function getDialects(): string[] {
  return JSON.parse(wasmGetDialects());
}

/**
 * Get the version of the Polyglot library.
 *
 * @returns Version string
 */
export function getVersion(): string {
  return wasmVersion();
}

/**
 * Main Polyglot class for object-oriented usage.
 */
export class Polyglot {
  private static instance: Polyglot | null = null;

  private constructor() {}

  /**
   * Get or create the Polyglot instance.
   * The WASM module is automatically initialized on import.
   */
  static getInstance(): Polyglot {
    if (!Polyglot.instance) {
      Polyglot.instance = new Polyglot();
    }
    return Polyglot.instance;
  }

  /**
   * Transpile SQL from one dialect to another.
   */
  transpile(sql: string, options: TranspileOptions): TranspileResult {
    return transpile(sql, options);
  }

  /**
   * Parse SQL into an AST.
   */
  parse(sql: string, dialect: Dialect = Dialect.Generic): ParseResult {
    return parse(sql, dialect);
  }

  /**
   * Generate SQL from an AST.
   */
  generate(ast: any, dialect: Dialect = Dialect.Generic): TranspileResult {
    return generate(ast, dialect);
  }

  /**
   * Format SQL.
   */
  format(sql: string, dialect: Dialect = Dialect.Generic): TranspileResult {
    return format(sql, dialect);
  }

  /**
   * Get supported dialects.
   */
  getDialects(): string[] {
    return getDialects();
  }

  /**
   * Get library version.
   */
  getVersion(): string {
    return getVersion();
  }
}

// Re-export AST module (types, guards, helpers, visitor — excluding old builders)
export * as ast from './ast';

// Re-export WASM-backed builders at top level
export {
  // Expression class & types
  Expr,
  type ExprInput,
  // Expression helpers
  col,
  lit,
  star,
  sqlNull,
  boolean,
  table,
  sqlExpr,
  condition,
  func,
  not,
  cast,
  alias,
  and,
  or,
  // Convenience functions
  count,
  countDistinct,
  sum,
  avg,
  min,
  max,
  upper,
  lower,
  length,
  trim,
  ltrim,
  rtrim,
  reverse,
  initcap,
  substring,
  replace,
  concatWs,
  coalesce,
  nullIf,
  ifNull,
  abs,
  round,
  floor,
  ceil,
  power,
  sqrt,
  ln,
  exp,
  sign,
  greatest,
  least,
  currentDate,
  currentTime,
  currentTimestamp,
  extract,
  rowNumber,
  rank,
  denseRank,
  // Query builders
  SelectBuilder,
  select,
  InsertBuilder,
  insert,
  insertInto,
  UpdateBuilder,
  update,
  DeleteBuilder,
  deleteFrom,
  del,
  MergeBuilder,
  mergeInto,
  CaseBuilder,
  caseWhen,
  caseOf,
  SetOpBuilder,
  union,
  unionAll,
  intersect,
  except,
} from './builders';

// Also export commonly used AST items at top level for convenience
export {
  // Type guards
  isSelect,
  isColumn,
  isLiteral,
  isFunction,
  // Visitor utilities
  walk,
  findAll,
  getColumns,
  transform,
  renameColumns,
} from './ast';

// Re-export validation module
export { validate, ValidationSeverity } from './validation';
export type {
  ValidationResult,
  ValidationError,
  ValidationOptions,
} from './validation';

export { validateWithSchema } from './validation/schema-validator';
export type {
  Schema,
  TableSchema,
  ColumnSchema,
  SchemaValidationOptions,
} from './validation/schema-validator';

// Default export
export default {
  init,
  isInitialized,
  transpile,
  parse,
  generate,
  format,
  getDialects,
  getVersion,
  Dialect,
  Polyglot,
};
