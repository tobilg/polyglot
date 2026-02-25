/**
 * Polyglot SQL Dialect Translator
 *
 * A WebAssembly-powered SQL dialect translator that can convert SQL
 * between different database dialects (PostgreSQL, MySQL, BigQuery, etc.)
 */

// Import the WASM module - synchronously initialized on import via bundler target
import * as wasmModule from '../wasm/polyglot_sql_wasm.js';

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
  DataFusion = 'datafusion',
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
  /** 1-based line number where the error occurred */
  errorLine?: number;
  /** 1-based column number where the error occurred */
  errorColumn?: number;
}

/**
 * Result of a parse operation
 */
export interface ParseResult {
  success: boolean;
  ast?: any;
  error?: string;
  /** 1-based line number where the error occurred */
  errorLine?: number;
  /** 1-based column number where the error occurred */
  errorColumn?: number;
}

/**
 * Guard options for formatting very large/complex SQL safely.
 */
export interface FormatOptions {
  /** Maximum SQL input size in bytes */
  maxInputBytes?: number;
  /** Maximum token count after tokenization */
  maxTokens?: number;
  /** Maximum AST node count after parsing */
  maxAstNodes?: number;
  /** Maximum set-operation count (UNION/INTERSECT/EXCEPT) before parse */
  maxSetOpChain?: number;
}

type WasmBindings = typeof wasmModule & {
  transpile_value?: (sql: string, read: string, write: string) => unknown;
  parse_value?: (sql: string, dialect: string) => unknown;
  generate_value?: (ast: unknown, dialect: string) => unknown;
  format_sql_value?: (sql: string, dialect: string) => unknown;
  format_sql_with_options?: (
    sql: string,
    dialect: string,
    options_json: string,
  ) => string;
  format_sql_with_options_value?: (
    sql: string,
    dialect: string,
    options: FormatOptions,
  ) => unknown;
  get_dialects_value?: () => unknown;
};

const wasm = wasmModule as WasmBindings;

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return String(error);
}

function transpileFailure(context: string, error: unknown): TranspileResult {
  return {
    success: false,
    sql: undefined,
    error: `WASM ${context} failed: ${errorMessage(error)}`,
    errorLine: undefined,
    errorColumn: undefined,
  };
}

function parseFailure(context: string, error: unknown): ParseResult {
  return {
    success: false,
    ast: undefined,
    error: `WASM ${context} failed: ${errorMessage(error)}`,
    errorLine: undefined,
    errorColumn: undefined,
  };
}

function decodeWasmPayload<T>(payload: unknown): T {
  if (typeof payload === 'string') {
    return JSON.parse(payload) as T;
  }
  return payload as T;
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
 * @remarks
 * **Per-dialect builds:** When using a per-dialect sub-path import
 * (e.g., `@polyglot-sql/sdk/clickhouse`), only same-dialect transpilation
 * and conversion to/from {@link Dialect.Generic} are supported.
 * Cross-dialect transpilation (e.g., ClickHouse → PostgreSQL) will return
 * `{ success: false, error: "Cross-dialect transpilation not available in this build" }`.
 * Use {@link getDialects} to check which dialects are available at runtime.
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
  try {
    if (typeof wasm.transpile_value === 'function') {
      return decodeWasmPayload<TranspileResult>(
        wasm.transpile_value(sql, read, write),
      );
    }

    return JSON.parse(wasm.transpile(sql, read, write)) as TranspileResult;
  } catch (error) {
    return transpileFailure('transpile', error);
  }
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
  try {
    if (typeof wasm.parse_value === 'function') {
      return decodeWasmPayload<ParseResult>(wasm.parse_value(sql, dialect));
    }

    const result = JSON.parse(wasm.parse(sql, dialect)) as ParseResult;
    if (result.success && typeof result.ast === 'string') {
      result.ast = JSON.parse(result.ast);
    }
    return result;
  } catch (error) {
    return parseFailure('parse', error);
  }
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
  try {
    // Valid parse output is an array of Expression nodes.
    // Keep legacy string path as fallback for non-array inputs
    // (e.g. cyclic objects) to preserve error behavior.
    if (typeof wasm.generate_value === 'function' && Array.isArray(ast)) {
      return decodeWasmPayload<TranspileResult>(
        wasm.generate_value(ast, dialect),
      );
    }

    const astJson = JSON.stringify(ast);
    return JSON.parse(wasm.generate(astJson, dialect)) as TranspileResult;
  } catch (error) {
    return transpileFailure('generate', error);
  }
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
  return formatWithOptions(sql, dialect, {});
}

/**
 * Format/pretty-print SQL with explicit guard limits.
 *
 * This can be used to tune handling for very large SQL inputs.
 */
export function formatWithOptions(
  sql: string,
  dialect: Dialect = Dialect.Generic,
  options: FormatOptions = {},
): TranspileResult {
  try {
    if (typeof wasm.format_sql_with_options_value === 'function') {
      return decodeWasmPayload<TranspileResult>(
        wasm.format_sql_with_options_value(sql, dialect, options),
      );
    }

    if (typeof wasm.format_sql_with_options === 'function') {
      return JSON.parse(
        wasm.format_sql_with_options(sql, dialect, JSON.stringify(options)),
      ) as TranspileResult;
    }

    if (typeof wasm.format_sql_value === 'function') {
      return decodeWasmPayload<TranspileResult>(
        wasm.format_sql_value(sql, dialect),
      );
    }

    return JSON.parse(wasm.format_sql(sql, dialect)) as TranspileResult;
  } catch (error) {
    return transpileFailure('format', error);
  }
}

/**
 * Get list of supported dialects in this build.
 *
 * The full build (`@polyglot-sql/sdk`) includes all 34 dialects.
 * Per-dialect builds (e.g., `@polyglot-sql/sdk/clickhouse`) include only
 * `"generic"` and the selected dialect.
 *
 * Use this function to check dialect availability before calling {@link transpile}.
 *
 * @returns Array of dialect name strings available in this build
 *
 * @example
 * ```typescript
 * const dialects = getDialects();
 * // Full build: ["generic", "postgresql", "mysql", "bigquery", ...]
 * // Per-dialect: ["generic", "clickhouse"]
 *
 * if (dialects.includes("postgresql")) {
 *   // Safe to transpile to PostgreSQL
 * }
 * ```
 */
export function getDialects(): string[] {
  if (typeof wasm.get_dialects_value === 'function') {
    return decodeWasmPayload<string[]>(wasm.get_dialects_value());
  }
  return JSON.parse(wasm.get_dialects());
}

/**
 * Get the version of the Polyglot library.
 *
 * @returns Version string
 */
export function getVersion(): string {
  return wasm.version();
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
   *
   * @remarks
   * Per-dialect builds only support same-dialect and to/from Generic transpilation.
   * Use {@link Polyglot.getDialects} to check available dialects.
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
   * Format SQL with explicit guard limits.
   */
  formatWithOptions(
    sql: string,
    dialect: Dialect = Dialect.Generic,
    options: FormatOptions = {},
  ): TranspileResult {
    return formatWithOptions(sql, dialect, options);
  }

  /**
   * Get supported dialects in this build.
   * Per-dialect builds return only `"generic"` and the selected dialect.
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
  WindowDefBuilder,
  select,
  sign,
  sqlExpr,
  sqlNull,
  sqrt,
  star,
  subquery,
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
// Re-export lineage module
export { getSourceTables, lineage } from './lineage';
export type { LineageNode, LineageResult, SourceTablesResult } from './lineage';
// Re-export diff module
export { changesOnly, diff, hasChanges } from './diff';
export type { DiffEdit, DiffOptions, DiffResult, EditType } from './diff';
// Re-export planner module
export { plan } from './planner';
export type {
  JoinType as PlanJoinType,
  PlanResult,
  PlanStep,
  QueryPlan,
  SetOperationType,
  StepKind,
} from './planner';
export type {
  ColumnSchema,
  Schema,
  SchemaValidationOptions,
  TableSchema,
} from './validation/schema-validator';
export { validateWithSchema } from './validation/schema-validator';

// Import new modules for default export
import { lineage, getSourceTables } from './lineage';
import { diff, hasChanges, changesOnly } from './diff';
import { plan } from './planner';

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
  lineage,
  getSourceTables,
  diff,
  hasChanges,
  changesOnly,
  plan,
  Dialect,
  Polyglot,
};
