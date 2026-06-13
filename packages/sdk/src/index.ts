/**
 * Polyglot SQL Dialect Translator
 *
 * A WebAssembly-powered SQL dialect translator that can convert SQL
 * between different database dialects (PostgreSQL, MySQL, BigQuery, etc.)
 */

// Import the WASM module - synchronously initialized on import via bundler target
import * as wasmModule from '../wasm/polyglot_sql_wasm.js';
import type { DataType } from './generated/DataType';
import type { Expression } from './generated/Expression';
import type { Schema as ValidationSchema } from './validation/schema-validator';

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
export type UnsupportedLevel = 'ignore' | 'warn' | 'raise' | 'immediate';

export interface TranspileOptions {
  /** Pretty-print the generated SQL */
  pretty?: boolean;
  /** How unsupported target-dialect constructs should be handled */
  unsupportedLevel?: UnsupportedLevel;
  /** Maximum number of unsupported diagnostics to include in raised errors */
  maxUnsupported?: number;
}

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
  /** Start byte offset of the error range */
  errorStart?: number;
  /** End byte offset of the error range (exclusive) */
  errorEnd?: number;
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
  /** Start byte offset of the error range */
  errorStart?: number;
  /** End byte offset of the error range (exclusive) */
  errorEnd?: number;
}

/**
 * Result of a standalone data type parse operation
 */
export interface DataTypeResult {
  success: boolean;
  dataType?: DataType;
  error?: string;
  /** 1-based line number where the error occurred */
  errorLine?: number;
  /** 1-based column number where the error occurred */
  errorColumn?: number;
  /** Start byte offset of the error range */
  errorStart?: number;
  /** End byte offset of the error range (exclusive) */
  errorEnd?: number;
}

/**
 * Result of a standalone data type generation operation
 */
export interface GenerateDataTypeResult {
  success: boolean;
  sql?: string;
  error?: string;
  /** 1-based line number where the error occurred */
  errorLine?: number;
  /** 1-based column number where the error occurred */
  errorColumn?: number;
  /** Start byte offset of the error range */
  errorStart?: number;
  /** End byte offset of the error range (exclusive) */
  errorEnd?: number;
}

/**
 * Span information for a token, indicating its position in the source SQL.
 */
export interface SpanInfo {
  start: number;
  end: number;
  line: number;
  column: number;
}

/**
 * A single token from the SQL token stream.
 */
export interface TokenInfo {
  tokenType: string;
  text: string;
  span: SpanInfo;
  comments: string[];
  trailingComments: string[];
}

/**
 * Result of a tokenize operation
 */
export interface TokenizeResult {
  success: boolean;
  tokens?: TokenInfo[];
  error?: string;
  /** 1-based line number where the error occurred */
  errorLine?: number;
  /** 1-based column number where the error occurred */
  errorColumn?: number;
  /** Start byte offset of the error range */
  errorStart?: number;
  /** End byte offset of the error range (exclusive) */
  errorEnd?: number;
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

export interface AnalyzeQueryOptions {
  /** Dialect used for parsing and dialect-aware rendering */
  dialect?: Dialect | string;
  /** Optional schema used for qualification and type annotation */
  schema?: ValidationSchema;
}

export type QueryShape = 'select' | 'set_operation';
export type QueryAnalysisSourceKind =
  | 'root'
  | 'table'
  | 'derived_table'
  | 'cte'
  | 'virtual'
  | 'unknown';
export type TransformKind =
  | 'direct'
  | 'cast'
  | 'aggregation'
  | 'constant'
  | 'expression'
  | 'star';
export type ReferenceConfidence = 'resolved' | 'ambiguous' | 'unknown';
export type ProjectionNullability = 'non_null' | 'nullable' | 'unknown';

export interface ColumnReferenceFact {
  sourceName?: string;
  sourceAlias?: string;
  sourceKind: QueryAnalysisSourceKind;
  table?: string;
  column: string;
  unqualified: boolean;
  confidence: ReferenceConfidence;
}

export interface TransformFunctionFact {
  name: string;
  literalArgs: string[];
  columnArgs: ColumnReferenceFact[];
}

export interface ProjectionFact {
  index: number;
  name?: string;
  isStar: boolean;
  starTable?: string;
  transformKind: TransformKind;
  transformFunction?: TransformFunctionFact;
  castType?: string;
  typeHint?: string;
  nullability: ProjectionNullability;
  upstream: ColumnReferenceFact[];
}

export interface CteFact {
  name: string;
  columns: string[];
  bodySql: string;
  outputColumns: string[];
}

export interface StarProjectionFact {
  index: number;
  table?: string;
  expandedColumns: string[];
}

export interface RelationFact {
  name: string;
  alias?: string | null;
  kind: QueryAnalysisSourceKind;
  columns: string[];
  catalog?: string | null;
  schema?: string | null;
  table?: string | null;
}

export interface SetOperationBranchFact {
  index: number;
  projections: ProjectionFact[];
}

export interface SetOperationFact {
  kind: 'union' | 'intersect' | 'except' | string;
  all: boolean;
  distinct: boolean;
  outputColumns: string[];
  branches: SetOperationBranchFact[];
}

export interface QueryAnalysis {
  shape: QueryShape;
  ctes: string[];
  cteFacts: CteFact[];
  projections: ProjectionFact[];
  relations: RelationFact[];
  baseTables: RelationFact[];
  starProjections: StarProjectionFact[];
  setOperations: SetOperationFact[];
}

export interface QueryAnalysisResult {
  success: boolean;
  analysis?: QueryAnalysis;
  error?: string;
}

type WasmBindings = typeof wasmModule & {
  transpile_value?: (sql: string, read: string, write: string) => unknown;
  transpile_with_options?: (
    sql: string,
    read: string,
    write: string,
    options_json: string,
  ) => string;
  transpile_with_options_value?: (
    sql: string,
    read: string,
    write: string,
    options: TranspileOptions,
  ) => unknown;
  parse_value?: (sql: string, dialect: string) => unknown;
  parse_data_type?: (sql: string, dialect: string) => string;
  parse_data_type_value?: (sql: string, dialect: string) => unknown;
  generate_value?: (ast: unknown, dialect: string) => unknown;
  generate_data_type?: (dataTypeJson: string, dialect: string) => string;
  generate_data_type_value?: (dataType: unknown, dialect: string) => unknown;
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
  tokenize_value?: (sql: string, dialect: string) => unknown;
  annotate_types?: (
    sql: string,
    dialect: string,
    schema_json: string,
  ) => string;
  annotate_types_value?: (
    sql: string,
    dialect: string,
    schema_json: string,
  ) => unknown;
  analyze_query?: (sql: string, options_json: string) => string;
  analyze_query_value?: (sql: string, options: AnalyzeQueryOptions) => unknown;
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

function dataTypeFailure(context: string, error: unknown): DataTypeResult {
  return {
    success: false,
    dataType: undefined,
    error: `WASM ${context} failed: ${errorMessage(error)}`,
    errorLine: undefined,
    errorColumn: undefined,
  };
}

function generateDataTypeFailure(
  context: string,
  error: unknown,
): GenerateDataTypeResult {
  return {
    success: false,
    sql: undefined,
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

function queryAnalysisFailure(
  context: string,
  error: unknown,
): QueryAnalysisResult {
  return {
    success: false,
    analysis: undefined,
    error: `WASM ${context} failed: ${errorMessage(error)}`,
  };
}

type AnalyzeQueryInput = AnalyzeQueryOptions | Dialect | string;

function normalizeAnalyzeQueryOptions(
  optionsOrDialect: AnalyzeQueryInput = {},
): AnalyzeQueryOptions {
  if (typeof optionsOrDialect === 'string') {
    return { dialect: optionsOrDialect };
  }

  return {
    dialect: Dialect.Generic,
    ...optionsOrDialect,
  };
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
  options?: TranspileOptions,
): TranspileResult {
  try {
    if (options && Object.keys(options).length > 0) {
      if (typeof wasm.transpile_with_options_value === 'function') {
        return decodeWasmPayload<TranspileResult>(
          wasm.transpile_with_options_value(sql, read, write, options),
        );
      }
      if (typeof wasm.transpile_with_options === 'function') {
        return JSON.parse(
          wasm.transpile_with_options(sql, read, write, JSON.stringify(options)),
        ) as TranspileResult;
      }
      return {
        success: false,
        sql: undefined,
        error: 'WASM transpile options are not available in this build',
      };
    }

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
 * Parse a standalone SQL data type.
 *
 * @param sql - The data type string to parse
 * @param dialect - The dialect to use
 * @returns The parsed DataType AST node
 *
 * @example
 * ```typescript
 * const result = parseDataType("DECIMAL(10, 2)", Dialect.DuckDB);
 * console.log(result.dataType);
 * ```
 */
export function parseDataType(
  sql: string,
  dialect: Dialect = Dialect.Generic,
): DataTypeResult {
  try {
    if (typeof wasm.parse_data_type_value === 'function') {
      return decodeWasmPayload<DataTypeResult>(
        wasm.parse_data_type_value(sql, dialect),
      );
    }

    const result = JSON.parse(
      wasm.parse_data_type(sql, dialect),
    ) as DataTypeResult & { dataType?: string | DataType };
    if (result.success && typeof result.dataType === 'string') {
      result.dataType = JSON.parse(result.dataType) as DataType;
    }
    return result as DataTypeResult;
  } catch (error) {
    return dataTypeFailure('parseDataType', error);
  }
}

/**
 * Tokenize SQL into a token stream.
 *
 * @param sql - The SQL string to tokenize
 * @param dialect - The dialect to use for tokenization
 * @returns The token stream
 *
 * @example
 * ```typescript
 * const result = tokenize("SELECT a, b FROM t", Dialect.PostgreSQL);
 * if (result.success) {
 *   for (const token of result.tokens!) {
 *     console.log(token.tokenType, token.text, token.span);
 *   }
 * }
 * ```
 */
export function tokenize(
  sql: string,
  dialect: Dialect = Dialect.Generic,
): TokenizeResult {
  try {
    if (typeof wasm.tokenize_value === 'function') {
      return decodeWasmPayload<TokenizeResult>(
        wasm.tokenize_value(sql, dialect),
      );
    }
    return JSON.parse(wasm.tokenize(sql, dialect)) as TokenizeResult;
  } catch (error) {
    return {
      success: false,
      tokens: undefined,
      error: `WASM tokenize failed: ${errorMessage(error)}`,
    };
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
 * Generate SQL from a standalone DataType AST node.
 *
 * @param dataType - The DataType object to render
 * @param dialect - The target dialect
 * @returns The generated data type SQL
 *
 * @example
 * ```typescript
 * const parsed = parseDataType("VARCHAR(255)", Dialect.DuckDB);
 * const rendered = generateDataType(parsed.dataType!, Dialect.PostgreSQL);
 * // rendered.sql = "VARCHAR(255)"
 * ```
 */
export function generateDataType(
  dataType: DataType,
  dialect: Dialect = Dialect.Generic,
): GenerateDataTypeResult {
  try {
    if (typeof wasm.generate_data_type_value === 'function') {
      return decodeWasmPayload<GenerateDataTypeResult>(
        wasm.generate_data_type_value(dataType, dialect),
      );
    }

    const dataTypeJson = JSON.stringify(dataType);
    return JSON.parse(
      wasm.generate_data_type(dataTypeJson, dialect),
    ) as GenerateDataTypeResult;
  } catch (error) {
    return generateDataTypeFailure('generateDataType', error);
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
 * Return compact query analysis facts for a SELECT or set operation.
 */
export function analyzeQuery(
  sql: string,
  dialect: Dialect | string,
): QueryAnalysisResult;
export function analyzeQuery(
  sql: string,
  options?: AnalyzeQueryOptions,
): QueryAnalysisResult;
export function analyzeQuery(
  sql: string,
  optionsOrDialect: AnalyzeQueryInput = {},
): QueryAnalysisResult {
  try {
    const normalized = normalizeAnalyzeQueryOptions(optionsOrDialect);

    if (typeof wasm.analyze_query_value === 'function') {
      return decodeWasmPayload<QueryAnalysisResult>(
        wasm.analyze_query_value(sql, normalized),
      );
    }

    if (typeof wasm.analyze_query === 'function') {
      return JSON.parse(
        wasm.analyze_query(sql, JSON.stringify(normalized)),
      ) as QueryAnalysisResult;
    }

    return {
      success: false,
      error: 'analyze_query not available in this WASM build',
    };
  } catch (error) {
    return queryAnalysisFailure('analyzeQuery', error);
  }
}

/**
 * Result of an annotateTypes operation
 */
export interface AnnotateTypesResult {
  success: boolean;
  /** The AST with `inferred_type` fields populated on value-producing nodes */
  ast?: Expression[];
  error?: string;
}

/**
 * Parse SQL and annotate the AST with inferred type information.
 *
 * Parses the given SQL and runs bottom-up type inference, populating the
 * `inferred_type` field on value-producing AST nodes (columns, operators,
 * functions, casts, etc.). Use `ast.getInferredType(expr)` to read the
 * inferred type from any expression node.
 *
 * @param sql - The SQL string to parse and annotate
 * @param dialect - The dialect to use for parsing (default: Generic)
 * @param schema - Optional schema for column type resolution
 *
 * @example
 * ```typescript
 * import { annotateTypes, ast, Dialect } from '@polyglot-sql/sdk';
 *
 * const result = annotateTypes(
 *   "SELECT a + b FROM t",
 *   Dialect.PostgreSQL,
 *   { tables: { t: { a: "INT", b: "DOUBLE" } } }
 * );
 *
 * if (result.success) {
 *   // Walk the AST and inspect inferred types
 *   ast.walk(result.ast![0], (node) => {
 *     const dt = ast.getInferredType(node);
 *     if (dt) {
 *       console.log(ast.getExprType(node), "=>", dt);
 *     }
 *   });
 * }
 * ```
 */
export function annotateTypes(
  sql: string,
  dialect: Dialect = Dialect.Generic,
  schema?: ValidationSchema,
): AnnotateTypesResult {
  try {
    const schemaJson = schema ? JSON.stringify(schema) : '';

    if (typeof wasm.annotate_types_value === 'function') {
      return decodeWasmPayload<AnnotateTypesResult>(
        wasm.annotate_types_value(sql, dialect, schemaJson),
      );
    }

    if (typeof wasm.annotate_types === 'function') {
      return JSON.parse(
        wasm.annotate_types(sql, dialect, schemaJson),
      ) as AnnotateTypesResult;
    }

    return {
      success: false,
      error: 'annotate_types not available in this WASM build',
    };
  } catch (error) {
    return {
      success: false,
      error: `WASM annotate_types failed: ${errorMessage(error)}`,
    };
  }
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
  transpile(
    sql: string,
    read: Dialect,
    write: Dialect,
    options?: TranspileOptions,
  ): TranspileResult {
    return transpile(sql, read, write, options);
  }

  /**
   * Parse SQL into an AST.
   */
  parse(sql: string, dialect: Dialect = Dialect.Generic): ParseResult {
    return parse(sql, dialect);
  }

  /**
   * Parse a standalone SQL data type.
   */
  parseDataType(
    sql: string,
    dialect: Dialect = Dialect.Generic,
  ): DataTypeResult {
    return parseDataType(sql, dialect);
  }

  /**
   * Tokenize SQL into a token stream.
   */
  tokenize(sql: string, dialect: Dialect = Dialect.Generic): TokenizeResult {
    return tokenize(sql, dialect);
  }

  /**
   * Generate SQL from an AST.
   */
  generate(ast: any, dialect: Dialect = Dialect.Generic): TranspileResult {
    return generate(ast, dialect);
  }

  /**
   * Generate SQL from a standalone DataType AST node.
   */
  generateDataType(
    dataType: DataType,
    dialect: Dialect = Dialect.Generic,
  ): GenerateDataTypeResult {
    return generateDataType(dataType, dialect);
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

  /**
   * Parse SQL and annotate the AST with inferred type information.
   */
  annotateTypes(
    sql: string,
    dialect: Dialect = Dialect.Generic,
    schema?: ValidationSchema,
  ): AnnotateTypesResult {
    return annotateTypes(sql, dialect, schema);
  }

  /**
   * Return compact query analysis facts for a SELECT or set operation.
   */
  analyzeQuery(sql: string, dialect: Dialect | string): QueryAnalysisResult;
  analyzeQuery(sql: string, options?: AnalyzeQueryOptions): QueryAnalysisResult;
  analyzeQuery(
    sql: string,
    optionsOrDialect: AnalyzeQueryInput = {},
  ): QueryAnalysisResult {
    return analyzeQuery(sql, normalizeAnalyzeQueryOptions(optionsOrDialect));
  }
}

// Re-export AST module (types, guards, helpers, visitor — excluding old builders)
export * as ast from './ast';
// Also export commonly used AST items at top level for convenience
export {
  findAll,
  getColumns,
  getInferredType,
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
  WindowDefBuilder,
} from './builders';
export type { DiffEdit, DiffOptions, DiffResult, EditType } from './diff';
// Re-export diff module
export { changesOnly, diff, hasChanges } from './diff';
export type {
  LineageNode,
  LineageResult,
  LineageSourceKind,
  SourceTablesResult,
} from './lineage';
// Re-export lineage module
export { getSourceTables, lineage, lineageWithSchema } from './lineage';
export type {
  OpenLineageColumnLineageFacet,
  OpenLineageColumnLineageField,
  OpenLineageColumnLineageResult,
  OpenLineageDataset,
  OpenLineageDatasetId,
  OpenLineageEventResult,
  OpenLineageInputField,
  OpenLineageOptions,
  OpenLineageRunEventType,
  OpenLineageTransformation,
  OpenLineageWarning,
} from './openlineage';
// Re-export OpenLineage module
export {
  openLineageColumnLineage,
  openLineageJobEvent,
  openLineageRunEvent,
} from './openlineage';
export type {
  JoinType as PlanJoinType,
  PlanResult,
  PlanStep,
  QueryPlan,
  SetOperationType,
  StepKind,
} from './planner';
export type { DataType } from './generated/DataType';
// Re-export planner module
export { plan } from './planner';
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

import { changesOnly, diff, hasChanges } from './diff';
// Import new modules for default export
import { getSourceTables, lineage, lineageWithSchema } from './lineage';
import {
  openLineageColumnLineage,
  openLineageJobEvent,
  openLineageRunEvent,
} from './openlineage';
import { plan } from './planner';

// Default export
export default {
  init,
  isInitialized,
  transpile,
  parse,
  parseDataType,
  tokenize,
  generate,
  generateDataType,
  format,
  annotateTypes,
  analyzeQuery,
  getDialects,
  getVersion,
  lineage,
  lineageWithSchema,
  getSourceTables,
  openLineageColumnLineage,
  openLineageJobEvent,
  openLineageRunEvent,
  diff,
  hasChanges,
  changesOnly,
  plan,
  Dialect,
  Polyglot,
};
