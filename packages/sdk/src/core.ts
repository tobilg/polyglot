/**
 * Polyglot SDK core - shared by both ESM and CJS entry points.
 * ESM index.ts eagerly loads WASM before re-exporting; CJS uses this directly.
 */

import { getWasmSync, isWasmLoaded, loadWasm } from './wasm-loader';

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

export interface TranspileResult {
  success: boolean;
  sql?: string[];
  error?: string;
  errorLine?: number;
  errorColumn?: number;
}

export interface ParseResult {
  success: boolean;
  ast?: any;
  error?: string;
  errorLine?: number;
  errorColumn?: number;
}

export interface FormatOptions {
  maxInputBytes?: number;
  maxTokens?: number;
  maxAstNodes?: number;
  maxSetOpChain?: number;
}

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

export async function init(): Promise<void> {
  await loadWasm();
}

export function isInitialized(): boolean {
  return isWasmLoaded();
}

export function transpile(
  sql: string,
  read: Dialect,
  write: Dialect,
): TranspileResult {
  try {
    const wasm = getWasmSync();
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

export function parse(
  sql: string,
  dialect: Dialect = Dialect.Generic,
): ParseResult {
  try {
    const wasm = getWasmSync();
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

export function generate(
  ast: any,
  dialect: Dialect = Dialect.Generic,
): TranspileResult {
  try {
    const wasm = getWasmSync();
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

export function format(
  sql: string,
  dialect: Dialect = Dialect.Generic,
): TranspileResult {
  return formatWithOptions(sql, dialect, {});
}

export function formatWithOptions(
  sql: string,
  dialect: Dialect = Dialect.Generic,
  options: FormatOptions = {},
): TranspileResult {
  try {
    const wasm = getWasmSync();
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

export function getDialects(): string[] {
  const wasm = getWasmSync();
  if (typeof wasm.get_dialects_value === 'function') {
    return decodeWasmPayload<string[]>(wasm.get_dialects_value());
  }
  return JSON.parse(wasm.get_dialects());
}

export function getVersion(): string {
  return getWasmSync().version();
}

export class Polyglot {
  private static instance: Polyglot | null = null;

  private constructor() {}

  static getInstance(): Polyglot {
    if (!Polyglot.instance) {
      Polyglot.instance = new Polyglot();
    }
    return Polyglot.instance;
  }

  transpile(sql: string, read: Dialect, write: Dialect): TranspileResult {
    return transpile(sql, read, write);
  }

  parse(sql: string, dialect: Dialect = Dialect.Generic): ParseResult {
    return parse(sql, dialect);
  }

  generate(ast: any, dialect: Dialect = Dialect.Generic): TranspileResult {
    return generate(ast, dialect);
  }

  format(sql: string, dialect: Dialect = Dialect.Generic): TranspileResult {
    return format(sql, dialect);
  }

  formatWithOptions(
    sql: string,
    dialect: Dialect = Dialect.Generic,
    options: FormatOptions = {},
  ): TranspileResult {
    return formatWithOptions(sql, dialect, options);
  }

  getDialects(): string[] {
    return getDialects();
  }

  getVersion(): string {
    return getVersion();
  }
}

export * as ast from './ast';
export {
  findAll,
  getColumns,
  isColumn,
  isFunction,
  isLiteral,
  isSelect,
  renameColumns,
  transform,
  walk,
} from './ast';
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
  col,
  concatWs,
  condition,
  count,
  countDistinct,
  currentDate,
  currentTime,
  currentTimestamp,
  DeleteBuilder,
  del,
  deleteFrom,
  denseRank,
  Expr,
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
export type { ExprInput } from './builders';
export type {
  ValidationError,
  ValidationOptions,
  ValidationResult,
} from './validation';
export { ValidationSeverity, validate } from './validation';
export { getSourceTables, lineage } from './lineage';
export type { LineageNode, LineageResult, SourceTablesResult } from './lineage';
export { changesOnly, diff, hasChanges } from './diff';
export type { DiffEdit, DiffOptions, DiffResult, EditType } from './diff';
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

import { lineage, getSourceTables } from './lineage';
import { diff, hasChanges, changesOnly } from './diff';
import { plan } from './planner';

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
