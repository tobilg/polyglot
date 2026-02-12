/**
 * Polyglot SQL Dialect Translator
 *
 * A WebAssembly-powered SQL dialect translator that can convert SQL
 * between different database dialects (PostgreSQL, MySQL, BigQuery, etc.)
 */

// Import the WASM module - synchronously initialized on import via bundler target
import {
  format_sql as wasmFormatSql,
  generate as wasmGenerate,
  get_dialects as wasmGetDialects,
  parse as wasmParse,
  transpile as wasmTranspile,
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
 * @param read - Source dialect to parse the SQL with
 * @param write - Target dialect to generate SQL for
 * @returns The transpiled SQL statements
 *
 * @example
 * ```typescript
 * const result = transpile(
 *   "SELECT IFNULL(a, b)",
 *   Dialect.MySQL,
 *   Dialect.PostgreSQL,
 * );
 * // result.sql[0] = "SELECT COALESCE(a, b)"
 * ```
 */
export function transpile(
  sql: string,
  read: Dialect,
  write: Dialect,
): TranspileResult {
  const resultJson = wasmTranspile(sql, read, write);
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
export function parse(
  sql: string,
  dialect: Dialect = Dialect.Generic,
): ParseResult {
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
export function generate(
  ast: any,
  dialect: Dialect = Dialect.Generic,
): TranspileResult {
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
export function format(
  sql: string,
  dialect: Dialect = Dialect.Generic,
): TranspileResult {
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
  transpile(sql: string, read: Dialect, write: Dialect): TranspileResult {
    return transpile(sql, read, write);
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
// Also export commonly used AST items at top level for convenience
export {
  findAll,
  getColumns,
  isColumn,
  isFunction,
  isLiteral,
  // Type guards
  isSelect,
  renameColumns,
  transform,
  // Visitor utilities
  walk,
} from './ast';
// Re-export WASM-backed builders at top level
export {
  abs,
  alias,
  and,
  avg,
  boolean,
  CaseBuilder,
  caseOf,
  caseWhen,
  cast,
  ceil,
  coalesce,
  // Expression helpers
  col,
  concatWs,
  condition,
  // Convenience functions
  count,
  countDistinct,
  currentDate,
  currentTime,
  currentTimestamp,
  DeleteBuilder,
  del,
  deleteFrom,
  denseRank,
  // Expression class & types
  Expr,
  type ExprInput,
  except,
  exp,
  extract,
  floor,
  func,
  greatest,
  InsertBuilder,
  ifNull,
  initcap,
  insert,
  insertInto,
  intersect,
  least,
  length,
  lit,
  ln,
  lower,
  ltrim,
  MergeBuilder,
  max,
  mergeInto,
  min,
  not,
  nullIf,
  or,
  power,
  rank,
  replace,
  reverse,
  round,
  rowNumber,
  rtrim,
  // Query builders
  SelectBuilder,
  SetOpBuilder,
  select,
  sign,
  sqlExpr,
  sqlNull,
  sqrt,
  star,
  substring,
  sum,
  table,
  trim,
  UpdateBuilder,
  union,
  unionAll,
  update,
  upper,
} from './builders';
export type {
  ValidationError,
  ValidationOptions,
  ValidationResult,
} from './validation';
// Re-export validation module
export { ValidationSeverity, validate } from './validation';
export type {
  ColumnSchema,
  Schema,
  SchemaValidationOptions,
  TableSchema,
} from './validation/schema-validator';
export { validateWithSchema } from './validation/schema-validator';

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
