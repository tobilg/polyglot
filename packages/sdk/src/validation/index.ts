/**
 * SQL Validation Module
 *
 * Provides syntax and semantic validation for SQL queries.
 */

import {
  parse as wasmParse,
  validate as wasmValidate,
} from '../../wasm/polyglot_sql_wasm.js';
import type { Expression } from '../generated/Expression';
import { validateSemantics } from './rules';
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
  TableSchema,
} from './schema-validator';

// Re-export schema validation
export { validateWithSchema } from './schema-validator';
export { ValidationSeverity } from './types';

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
  // Step 1: Syntax validation via WASM
  const resultJson = wasmValidate(sql, dialect);
  const syntaxResult: ValidationResult = JSON.parse(resultJson);

  // If syntax validation failed or semantic validation not requested, return
  if (!syntaxResult.valid || !options.semantic) {
    return syntaxResult;
  }

  // Step 2: Parse the SQL to get AST for semantic validation
  const parseResultJson = wasmParse(sql, dialect);
  const parseResult = JSON.parse(parseResultJson);

  if (!parseResult.success || !parseResult.ast) {
    // Parse should succeed if syntax validation passed, but handle edge cases
    return syntaxResult;
  }

  // Parse the AST JSON
  let ast: Expression[];
  try {
    ast = JSON.parse(parseResult.ast);
  } catch {
    return syntaxResult;
  }

  // Step 3: Run semantic validation on each statement
  const allErrors: ValidationError[] = [...syntaxResult.errors];

  for (const stmt of ast) {
    const semanticErrors = validateSemantics(stmt, options);
    allErrors.push(...semanticErrors);
  }

  // Determine if still valid (only errors affect validity, not warnings)
  const hasErrors = allErrors.some((e) => e.severity === 'error');

  return {
    valid: !hasErrors,
    errors: allErrors,
  };
}
