/**
 * SQL Validation Module
 *
 * Provides syntax and semantic validation for SQL queries.
 */

import * as wasmModule from '../../wasm/polyglot_sql_wasm.js';
import type {
  ValidationError,
  ValidationOptions,
  ValidationResult,
} from './types';

export type { ValidationResult, ValidationError, ValidationOptions };
export type {
  ColumnSchema,
  Schema,
  SchemaValidationOptions,
  TableForeignKey,
  TableSchema,
} from './schema-validator';

// Re-export schema validation
export { validateWithSchema } from './schema-validator';
export { ValidationSeverity } from './types';

const wasm = wasmModule;

/**
 * Dialect type for validation (matches the main Dialect enum)
 */
export type ValidateDialect =
  | 'generic'
  | 'postgresql'
  | 'mysql'
  | 'bigquery'
  | 'snowflake'
  | 'duckdb'
  | 'sqlite'
  | 'hive'
  | 'spark'
  | 'trino'
  | 'presto'
  | 'redshift'
  | 'tsql'
  | 'oracle'
  | 'clickhouse'
  | 'databricks';

/**
 * Validate SQL syntax and optionally semantics.
 *
 * @param sql - The SQL string to validate
 * @param dialect - The dialect to use for validation
 * @param options - Validation options
 * @returns Validation result with errors and warnings
 *
 * @example
 * ```typescript
 * // Basic syntax validation
 * const result = validate('SELECT * FROM users', 'postgresql');
 * if (!result.valid) {
 *   console.log('Errors:', result.errors);
 * }
 *
 * // With semantic validation
 * const result = validate('SELECT * FROM users', 'postgresql', { semantic: true });
 * // This may also report warnings like "W001: SELECT * is discouraged"
 * ```
 */
export function validate(
  sql: string,
  dialect: ValidateDialect | string = 'generic',
  options: ValidationOptions = {},
): ValidationResult {
  const resultJson =
    options.strictSyntax || options.semantic
      ? wasm.validate_with_options(
          sql,
          dialect,
          JSON.stringify({
            strictSyntax: options.strictSyntax ?? false,
            semantic: options.semantic ?? false,
          }),
        )
      : wasm.validate(sql, dialect);
  return JSON.parse(resultJson) as ValidationResult;
}
